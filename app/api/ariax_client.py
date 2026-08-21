"""API layer (#08): resilient AriaX REST client.

Design goals:

* **Retry with exponential backoff + jitter** on timeouts/network errors
  (never on 4xx business rejections).
* **Tolerant response parsing** — the AriaX testnet API has evolved its
  payload shapes across versions; every parser accepts both the ``{ok,
  data: ...}`` envelope and flat forms.
* **Bounded sessions** — one reusable ``aiohttp.ClientSession`` with hard
  timeouts, closed cleanly on shutdown.
* **Never logs secrets** — bodies are logged through the redaction layer.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any, Dict, List, Optional

import aiohttp

from app.api.signing import RequestSigner
from app.config import Settings, TF_V5, V5_SYMBOL
from app.errors import AriaXAPIError
from app.models import Candle, MarketMeta

log = logging.getLogger("quant.api")


class AriaXClient:
    """Async REST client for the AriaX testnet exchange."""

    def __init__(self, settings: Settings, signer: RequestSigner):
        self._settings = settings
        self._signer = signer
        self._base = settings.arlax_base.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self._timeout = aiohttp.ClientTimeout(
            total=settings.request_timeout_s, connect=settings.connect_timeout_s
        )
        self.symbol_meta: Dict[str, MarketMeta] = {}

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    # ------------------------------------------------------------------
    # Core request primitive
    # ------------------------------------------------------------------
    async def request(self, method: str, path: str,
                      json_body: Optional[Dict[str, Any]] = None) -> Any:
        """Send a signed request with bounded retries.

        Raises:
            AriaXAPIError: after exhausting retries, or on 4xx rejection.
        """
        body = ""
        if json_body is not None:
            body = RequestSigner.json_body(json_body)

        attempts = max(1, self._settings.max_retries)
        for attempt in range(attempts):
            session = await self._get_session()
            url = f"{self._base}{path}"
            headers = self._signer.headers(method, path, body)
            try:
                async with session.request(
                    method, url, headers=headers,
                    data=body.encode("utf-8") if body else None,
                ) as resp:
                    text = await resp.text()
                    data = self._decode(text, resp.status)
                    if resp.status >= 400:
                        raise AriaXAPIError(
                            f"HTTP {resp.status} on {path}", status=resp.status, raw=data
                        )
                    return data
            except AriaXAPIError:
                # Business-level rejection: do NOT retry; surface immediately.
                raise
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                if attempt == attempts - 1:
                    raise AriaXAPIError(f"Network failure on {path}: {exc}") from exc
                wait = self._settings.retry_base_s * (2 ** attempt) + random.uniform(0, 1)
                log.warning("retry %s %s in %.1fs (%s)", method, path, wait,
                            exc.__class__.__name__)
                await asyncio.sleep(wait)
        raise AriaXAPIError(f"Exhausted retries for {method} {path}")

    @staticmethod
    def _decode(text: str, status: int) -> Any:
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"raw": text[:300], "status": status}

    # ------------------------------------------------------------------
    # Endpoints
    # ------------------------------------------------------------------
    async def get_markets(self) -> Any:
        return await self.request("GET", "/api/markets")

    async def get_wallet(self) -> Any:
        return await self.request("GET", "/api/wallet")

    async def get_positions(self) -> Any:
        return await self.request("GET", "/api/positions")

    async def get_orders(self) -> Any:
        return await self.request("GET", "/api/orders")

    async def get_fills(self) -> Any:
        return await self.request("GET", "/api/fills")

    async def get_performance(self) -> Any:
        return await self.request("GET", "/api/performance")

    async def get_config(self) -> Dict[str, MarketMeta]:
        """Fetch and cache per-symbol qty rules (``minq``/``step``)."""
        try:
            data = await self.request("GET", "/api/config")
        except AriaXAPIError as exc:  # config is non-critical
            log.warning("config fetch failed: %s", exc)
            return {}
        meta_src = data.get("data") or data.get("symbols") or data.get("markets") or data
        meta: Dict[str, MarketMeta] = {}
        if isinstance(meta_src, dict):
            for key, value in meta_src.items():
                if isinstance(value, dict) and ("minq" in value or "step" in value):
                    meta[str(key).upper()] = MarketMeta(
                        min_qty=float(value.get("minq") or value.get("minQty")
                                      or value.get("min") or 0),
                        step=float(value.get("step") or value.get("qtyStep")
                                   or value.get("stepSize") or 0),
                        price_step=float(value.get("priceStep") or value.get("tick")
                                         or value.get("priceTick") or 0),
                    )
        self.symbol_meta = meta
        log.info("config: %d symbols with qty rules", len(meta))
        return meta

    async def transfer_to_futures(self, amount: float) -> Any:
        """Move ``amount`` USDT from the spot wallet to the futures wallet."""
        return await self.request("POST", "/api/transfer", json_body={
            "from": "spot", "to": "futures", "amount": round(float(amount), 2),
        })

    async def place_order(self, symbol: str, side: str, qty: float, lev: int,
                          order_type: str = "market", price: Optional[float] = None,
                          strategy: str = "", client_oid: str = "") -> Any:
        """Submit an order. Returns the raw exchange response."""
        body: Dict[str, Any] = {
            "symbol": symbol,
            "side": side.lower(),
            "type": order_type.lower(),
            "qty": float(qty),
            "lev": int(lev),
        }
        if strategy:
            body["strategy"] = str(strategy)[:40]
        if order_type.lower() == "limit" and price is not None:
            body["price"] = float(price)
        if client_oid:
            body["clientOid"] = client_oid
        log.info("ORDER %s %s qty=%s", side.upper(), symbol, qty)
        return await self.request("POST", "/api/order", json_body=body)

    async def cancel_order(self, order_id: str) -> Any:
        return await self.request("POST", "/api/cancel", json_body={"id": order_id})

    # ------------------------------------------------------------------
    # Public candles served by the exchange itself (no auth dependency)
    # ------------------------------------------------------------------
    async def fetch_klines(self, arlax_sym: str, timeframe: str,
                           limit: int = 100) -> List[Candle]:
        """Fetch candles from AriaX's public ``/v5/market/kline`` endpoint.

        Returns candles ordered oldest -> newest.  Empty list on any failure.
        """
        v5 = V5_SYMBOL.get(arlax_sym)
        interval = TF_V5.get(timeframe)
        if not v5 or not interval:
            return []
        path = (f"/v5/market/kline?category=linear&symbol={v5}"
                f"&interval={interval}&limit={min(limit, 1000)}")
        try:
            data = await self.request("GET", path)
        except AriaXAPIError as exc:
            log.warning("ariax klines %s: %s", arlax_sym, exc)
            return []
        rows = ((data or {}).get("result") or {}).get("list") or []
        if not isinstance(rows, list):
            return []
        # Bybit v5 returns newest-first rows [ts, o, h, l, c, volume, ...].
        candles: List[Candle] = []
        for row in reversed(rows):
            try:
                candles.append(Candle.from_row([row[0], row[1], row[2], row[3],
                                                row[4], row[5]]))
            except (ValueError, TypeError, IndexError):
                continue
        return candles

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------
    async def health(self) -> bool:
        """Cheap liveness probe: fetch markets and confirm the ok flag."""
        try:
            data = await self.get_markets()
            return isinstance(data, dict) and bool(data.get("ok"))
        except AriaXAPIError:
            return False


# ---------------------------------------------------------------------------
# Shared market-payload helpers (used by engine, executor, feed)
# ---------------------------------------------------------------------------


def find_market_item(data: Any, symbol: str) -> Any:
    """Locate a symbol's market entry in any envelope shape the API returns."""
    if not isinstance(data, dict):
        return None
    if symbol in data:
        return data[symbol]
    for key in ("markets", "data", "prices", "items", "symbols"):
        bucket = data.get(key)
        if isinstance(bucket, dict) and symbol in bucket:
            return bucket[symbol]
        if isinstance(bucket, list):
            for item in bucket:
                if isinstance(item, dict) and str(item.get("symbol", "")).upper() == symbol:
                    return item
    return None


def market_price(item: Any) -> float:
    """Extract the last trade price from a market entry (0.0 if unknown)."""
    if item is None:
        return 0.0
    if isinstance(item, (int, float)):
        return float(item)
    if isinstance(item, dict):
        for key in ("price", "last", "mark", "close", "lastPrice", "last_price"):
            value = item.get(key)
            try:
                if value is not None and float(value) > 0:
                    return float(value)
            except (TypeError, ValueError):
                continue
    return 0.0
