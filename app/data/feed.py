"""Data finder (#06): redundant candle feed.

Source priority for every symbol/timeframe:

    1. AriaX public ``/v5/market/kline`` (same exchange data, no ban risk)
    2. Bybit spot (via ccxt, optional dependency)
    3. OKX (via ccxt)
    4. Binance (via ccxt — often IP-banned on cloud hosts, kept as last resort)

The feed tracks per-source health, applies a per-symbol failure cooldown,
and exposes the underlying candles for price discovery.  All sources return
the same :class:`Candle` shape so the strategy layer is source-agnostic.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Dict, List

from app.api.ariax_client import AriaXClient
from app.config import Settings, SYMBOL_MAP
from app.errors import DataUnavailableError
from app.models import Candle
from app.state import EngineState

log = logging.getLogger("quant.feed")


class CandleFeed:
    """Fetches OHLCV with automatic source fallback and health tracking."""

    def __init__(self, settings: Settings, arlax: AriaXClient,
                 state: EngineState) -> None:
        self._settings = settings
        self._arlax = arlax
        self._state = state
        self._fallback: List[object] = []
        self._symbol_cooldown: Dict[str, float] = {}
        self._init_fallback_sources()

    # ------------------------------------------------------------------
    def _init_fallback_sources(self) -> None:
        """Build optional ccxt-based fallback sources (guarded import)."""
        try:
            import ccxt.async_support as ccxt  # type: ignore
        except Exception:  # pragma: no cover - ccxt is optional
            log.info("ccxt not installed; fallback chain limited to AriaX")
            return
        for name, opts in (
            ("bybit", {"enableRateLimit": True, "options": {"defaultType": "spot"}}),
            ("okx", {"enableRateLimit": True}),
            ("binance", {"enableRateLimit": True, "options": {"defaultType": "spot"}}),
        ):
            try:
                self._fallback.append(getattr(ccxt, name)(opts))
                log.info("fallback source ready: %s", name)
            except Exception as exc:  # pragma: no cover
                log.warning("could not init fallback %s: %s", name, exc)

    async def close(self) -> None:
        for ex in self._fallback:
            try:
                await ex.close()  # type: ignore[attr-defined]
            except Exception:
                pass

    # ------------------------------------------------------------------
    async def fetch(self, arlax_sym: str, timeframe: str, limit: int) -> List[Candle]:
        """Fetch ``limit`` candles, falling back across sources.

        Raises:
            DataUnavailableError: if no source succeeds.
        """
        if time.time() < self._symbol_cooldown.get(arlax_sym, 0.0):
            raise DataUnavailableError(f"{arlax_sym} in failure cooldown")

        # 1) Primary: the exchange's own public kline endpoint.
        try:
            candles = await self._arlax.fetch_klines(arlax_sym, timeframe, limit)
            if self._good(candles, limit):
                self._state.record_fetch(arlax_sym, timeframe, True)
                return candles
        except Exception as exc:  # noqa: BLE001 - degrade gracefully
            log.warning("ariax klines %s %s: %s", arlax_sym, timeframe, exc)

        # 2) Fallbacks.
        pair = SYMBOL_MAP.get(arlax_sym)
        last_error = "no fallback sources available"
        if pair:
            for ex in self._fallback:
                try:
                    rows = await ex.fetch_ohlcv(pair, timeframe=timeframe, limit=limit)
                    await asyncio.sleep(self._settings.ohlcv_pause_s)
                    candles = [Candle.from_row(r) for r in rows]
                    if self._good(candles, limit):
                        self._state.record_fetch(arlax_sym, timeframe, True)
                        return candles
                except Exception as exc:  # noqa: BLE001
                    last_error = f"{getattr(ex, 'id', 'ccxt')}: {exc}"
                    log.warning("ohlcv fallback %s %s: %s", arlax_sym, timeframe,
                                last_error)
                    await asyncio.sleep(1.5)

        self._state.record_fetch(arlax_sym, timeframe, False)
        self._symbol_cooldown[arlax_sym] = time.time() + 60.0
        raise DataUnavailableError(f"{arlax_sym} {timeframe}: {last_error}")

    # ------------------------------------------------------------------
    @staticmethod
    def _good(candles: List[Candle], limit: int) -> bool:
        """Accept only sufficiently complete, ordered, sane candle batches.

        The former 30-bar threshold accepted AriaX's truncated 100-bar reply
        for a 300-bar request. StrategyEngine then received fewer bars than
        its warm-up requirement and permanently returned ``insufficient
        data`` instead of trying a complete fallback source.
        """
        required = max(30, math.ceil(limit * 0.80))
        if not candles or len(candles) < required:
            return False
        tail = candles[-min(len(candles), 10):]
        if any(c.ts <= 0 or c.c <= 0 or c.h < c.l or c.l < 0 for c in tail):
            return False
        return all(a.ts < b.ts for a, b in zip(tail, tail[1:]))

    # ------------------------------------------------------------------
    def last_price_hint(self) -> Dict[str, float]:
        """Return cached last closes keyed by AriaX symbol (0 if unknown)."""
        out: Dict[str, float] = {}
        for sym in self._settings.symbols:
            out[sym] = 0.0
        return out
