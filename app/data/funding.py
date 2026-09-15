"""Current perpetual funding rate per symbol (v24 sprint 8).

The Donchian funding gate ("don't join the crowded funding side") needs one
number at signal time: the symbol's current perpetual funding rate. Source:
OKX's public funding-rate endpoint (no auth, no geo issues). The backtest
that validated the gate (analysis/runs/v24_sprint8.json) used Binance 8h
funding events 2024-03 -> 2026-08; cross-venue funding is arb-aligned, and
the gate threshold sits on a validated plateau (0.010%-0.020%/8h) so minor
venue basis does not flip decisions.

Design rules:
  * fail-open — any fetch error returns the last cached value or None;
    a None rate leaves the gate inert (never blocks trades on data loss)
  * TTL-cached (funding changes every 8h; 30min cache is plenty)
  * injectable fetcher for unit tests
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable, Dict, Optional

import aiohttp

log = logging.getLogger("quant.funding")

_OKX_URL = "https://www.okx.com/api/v5/public/funding-rate"


def okx_inst_id(arlax_symbol: str) -> Optional[str]:
    """ETHUSD -> ETH-USDT-SWAP (USDT-margined perpetual)."""
    base = arlax_symbol[:-3] if arlax_symbol.endswith("USD") else ""
    return f"{base}-USDT-SWAP" if base else None


class FundingFeed:
    """Per-symbol current funding rate, TTL-cached, fail-open."""

    def __init__(self, ttl_sec: float = 1800.0,
                 fetcher: Optional[Callable[[str], Awaitable[Optional[float]]]] = None,
                 timeout_s: float = 8.0) -> None:
        self._ttl = ttl_sec
        self._timeout = timeout_s
        self._fetcher = fetcher or self._fetch_okx
        self._cache: Dict[str, tuple] = {}          # sym -> (monotonic_ts, rate)
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    async def get(self, symbol: str) -> Optional[float]:
        """Current funding rate for an AriaX symbol, or None (fail-open)."""
        now = time.monotonic()
        hit = self._cache.get(symbol)
        if hit is not None and now - hit[0] < self._ttl:
            return hit[1]
        try:
            rate = await self._fetcher(symbol)
        except Exception as exc:  # noqa: BLE001 - never raise into the scan
            log.warning("funding fetch %s failed: %s", symbol, exc)
            rate = None
        if rate is not None:
            self._cache[symbol] = (now, rate)
            return rate
        # Fail-open: serve a stale value if we have one, else None.
        return hit[1] if hit is not None else None

    # ------------------------------------------------------------------
    def snapshot(self) -> Dict[str, Optional[float]]:
        """Cache contents for observability (dashboard/status)."""
        return {s: v[1] for s, v in sorted(self._cache.items())}

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    async def _fetch_okx(self, symbol: str) -> Optional[float]:
        inst = okx_inst_id(symbol)
        if not inst:
            return None
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        async with self._session.get(
                _OKX_URL, params={"instId": inst},
                timeout=aiohttp.ClientTimeout(total=self._timeout)) as resp:
            data = await resp.json()
        if data.get("code") != "0" or not data.get("data"):
            return None
        return float(data["data"][0]["fundingRate"])
