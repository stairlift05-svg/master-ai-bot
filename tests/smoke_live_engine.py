#!/usr/bin/env python3
"""End-to-end smoke test of the live engine against a mocked exchange.

Boots the real QuantEngine (all modules wired), lets the supervision loops
run a few seconds against a deterministic fake AriaX endpoint, then shuts
down cleanly.  Asserts that prices/balance/decisions propagate through the
full pipeline without exceptions.

Run:  python tests/smoke_live_engine.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.backtest.synthetic import generate_5m, resample_1h  # noqa: E402
from app.config import Settings  # noqa: E402
from app.core.engine import QuantEngine  # noqa: E402
from app.models import Candle  # noqa: E402
from app.state import EngineState  # noqa: E402

_SYMBOLS = ("ETHUSD", "SOLUSD")
_PRICES = {"ETHUSD": 1800.0, "SOLUSD": 120.0}


class FakeAriaX:
    """Deterministic stand-in for AriaXClient (same public interface)."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.symbol_meta: Dict[str, Any] = {
            "ETHUSD": {"minq": 0.001, "step": 0.001},
            "SOLUSD": {"minq": 0.01, "step": 0.01},
        }
        self.orders_placed: List[Dict[str, Any]] = []
        self._cache5: Dict[str, List[Candle]] = {}
        self._cache1: Dict[str, List[Candle]] = {}

    def _candles5(self, sym: str) -> List[Candle]:
        if sym not in self._cache5:
            self._cache5[sym] = generate_5m(30, base_price=_PRICES[sym], seed=9)
        return self._cache5[sym]

    def _candles1(self, sym: str) -> List[Candle]:
        if sym not in self._cache1:
            self._cache1[sym] = resample_1h(self._candles5(sym))
        return self._cache1[sym]

    # -- engine-facing API -------------------------------------------------
    async def health(self) -> bool:
        return True

    async def get_markets(self) -> Dict[str, Any]:
        return {"ok": True, "data": {
            sym: {"price": _PRICES[sym], "last": _PRICES[sym], "funding": 0.0}
            for sym in _SYMBOLS
        }}

    async def get_wallet(self) -> Dict[str, Any]:
        return {
            "ok": True, "equity": 500.0, "free_margin": 120.0,
            "balances": {"USDT": 380.0}, "locks": {"USDT": 0.0},
            "futures": {"balances": {"USDT": 120.0}, "locks": {"USDT": 0.0}},
        }

    async def get_config(self) -> Dict[str, Any]:
        return {"ok": True, "data": self.symbol_meta}

    async def get_positions(self) -> Dict[str, Any]:
        return {"ok": True, "data": []}

    async def fetch_klines(self, sym: str, timeframe: str,
                           limit: int = 100) -> List[Candle]:
        if timeframe == "1h":
            src = self._candles1(sym)
        elif timeframe == "15m":
            from app.backtest.synthetic import resample
            src = resample(self._candles5(sym), 3)
        else:
            src = self._candles5(sym)
        return src[-limit:]

    async def place_order(self, symbol: str, side: str, qty: float, lev: int,
                          order_type: str = "market", price: Optional[float] = None,
                          strategy: str = "", client_oid: str = "") -> Dict[str, Any]:
        self.orders_placed.append({"symbol": symbol, "side": side, "qty": qty})
        return {"ok": True, "orderId": f"ord_{len(self.orders_placed)}",
                "avgPrice": _PRICES[symbol], "qty": qty}

    async def transfer_to_futures(self, amount: float) -> Dict[str, Any]:
        return {"ok": True}

    async def close(self) -> None:
        return None


def _settings() -> Settings:
    return Settings(
        arlax_key="fake-key", arlax_secret="fake-secret",
        symbols=_SYMBOLS,
        scan_interval_s=1.0, price_interval_s=1.0, sync_interval_s=2.0,
        entry_cooldown_s=0.0, post_close_cooldown_s=0.0,
        error_cooldown_base_s=1.0,
        min_order_usd=5.0, max_notional_usd=80.0, min_free_margin=5.0,
    )


async def _main() -> int:
    tmp = tempfile.mkdtemp(prefix="quant_smoke_")
    db_path = os.path.join(tmp, "smoke.db")
    settings = _settings()
    state = EngineState()
    engine = QuantEngine(settings, state, db_path=db_path, client=FakeAriaX(settings))

    await engine.start()
    # Let the loops run: ~8 simulated loop iterations with fake data.
    await asyncio.sleep(8.0)

    snap = state.snapshot()
    decisions = await engine.db.get_recent_decisions(5)
    prices = engine.prices

    await engine.shutdown()
    print(f"balance={snap['balance']:.2f} free={snap['free_balance']:.2f}")
    print(f"prices={ {k: round(v, 2) for k, v in prices.items()} }")
    print(f"decisions logged={len(decisions)} (last={decisions[0]['reason'][:40] if decisions else 'n/a'})")
    print(f"last_scan={snap['last_scan']} last_sync={snap['last_sync']}")

    ok = True
    if snap["balance"] != 500.0:
        print("FAIL: balance did not propagate"); ok = False
    if not all(prices.get(s, 0) > 0 for s in _SYMBOLS):
        print("FAIL: prices missing"); ok = False
    if not decisions:
        print("FAIL: no decisions logged"); ok = False
    if snap["last_scan"] == "Never":
        print("FAIL: scan loop never ran"); ok = False
    print("SMOKE TEST", "PASSED ✅" if ok else "FAILED ❌")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
