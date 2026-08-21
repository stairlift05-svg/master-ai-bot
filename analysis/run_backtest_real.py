#!/usr/bin/env python3
"""Run the real-market backtest once and cache the raw output for analysis.

Caches trades/equity to JSON so the think-tank analysis can iterate quickly
without re-running the (≈65s) simulation.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.backtest.backtester import Backtester
from app.backtest.synthetic import resample_1h
from app.config import Settings
from app.models import Candle
from simulate import load_csv_market


def main() -> int:
    data_dir = "analysis/data"
    symbols = ["ETHUSD", "SOLUSD", "XRPUSD", "AVAXUSD",
               "DOTUSD", "LINKUSD", "ADAUSD", "DOGEUSD"]
    market = load_csv_market(data_dir, symbols)
    if not market:
        print("no data"); return 2

    settings = Settings()
    bt = Backtester(settings, initial_balance=1000.0, slippage_bps=2.0)
    report = bt.run(market, {s: resample_1h(market[s]) for s in market})

    out = {
        "meta": {
            "source": "OKX spot, real 5m",
            "period": "2026-06-22 -> 2026-08-21",
            "symbols": symbols,
            "initial_balance": bt.initial_balance,
            "slippage_bps": 2.0,
            "taker_fee": settings.taker_fee,
            "fee_buffer": settings.fee_buffer,
        },
        "summary": report.to_dict(),
        "trades": [
            {
                "symbol": t.symbol, "side": t.side, "strategy": t.strategy,
                "entry": round(t.entry, 6), "exit": round(t.exit_price, 6),
                "qty": round(t.qty, 6), "pnl": round(t.pnl, 4),
                "fees": round(t.fees, 4), "hold_bars": t.hold_bars,
                "reason": t.reason, "opened_bar": t.opened_bar,
                "closed_bar": t.closed_bar,
            }
            for t in report.trades
        ],
        "equity": bt.equity_history,
        "per_strategy": report.per_strategy,
    }
    os.makedirs("analysis/runs", exist_ok=True)
    with open("analysis/runs/real_60d.json", "w") as fh:
        json.dump(out, fh, indent=1)
    print(f"cached: {len(out['trades'])} realizations, "
          f"equity points={len(out['equity'])}, net=${report.net_pnl():+.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
