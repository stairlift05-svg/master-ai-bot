#!/usr/bin/env python3
"""v22 evidence — Donchian_Trend long-side distance gate sweep.

Question: does requiring long breakouts to clear the slow EMA by
N x ATR improve out-of-sample performance, using the SAME two-window
protocol as validate_v21_final.py?

  A) analysis/data_1h     — 14 months Binance (2024-03 -> 2025-04)
  B) analysis/data_1h_oos — 16 months OKX   (2025-05 -> 2026-08, unseen)

Decision rule (agreed by the review board before running):
  ship a non-zero default ONLY if it improves NET on window B (OOS)
  without reducing NET on window A, and without cutting trade count
  to a statistically meaningless sample (<60 trades in B).

Usage: python analysis/sweep_long_gate_v22.py
"""
from __future__ import annotations

import csv
import dataclasses
import json
import os
import sys
import time
from typing import Dict, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.backtest.backtester import Backtester  # noqa: E402
from app.config import Settings  # noqa: E402
from app.models import Candle  # noqa: E402

DIR_A = os.path.join(ROOT, "analysis", "data_1h")
DIR_B = os.path.join(ROOT, "analysis", "data_1h_oos")
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD", "BNBUSD", "DOGEUSD"]
BALANCE = 1000.0
GATES = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]
BASE_PARAMS = {"entry_len": 40, "sl_m": 2.5, "tp_m": 20.0, "break_atr": 1.5}


def load(directory: str) -> Dict[str, List[Candle]]:
    market: Dict[str, List[Candle]] = {}
    for sym in SYMBOLS:
        rows: List[Candle] = []
        with open(os.path.join(directory, f"{sym}.csv"), newline="",
                  encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                try:
                    rows.append(Candle(int(float(row["ts"])), float(row["o"]),
                                       float(row["h"]), float(row["l"]),
                                       float(row["c"]), float(row["v"])))
                except (KeyError, ValueError):
                    continue
        rows.sort(key=lambda c: c.ts)
        market[sym] = rows
    return market


def settings_for(gate: float) -> Settings:
    params = {k: dict(v) for k, v in Settings().strategy_params.items()}
    params["Donchian_Trend"] = {**BASE_PARAMS, "long_dist_atr": gate}
    return dataclasses.replace(Settings(), enabled_strategies=("Donchian_Trend",),
                               sides="both", timeframe="1h",
                               mid_timeframe="4h", htf_timeframe="4h",
                               strategy_params=params)


def run(market, gate: float, bar0: int = 0, bar1: int = None) -> Dict:
    bt = Backtester(settings_for(gate), initial_balance=BALANCE, base_tf="1h")
    bt.run({s: market[s][bar0:bar1] for s in market})
    closed = bt.closed
    wins = [t for t in closed if t.pnl > 0]
    losses = [t for t in closed if t.pnl < 0]
    gw = sum(t.pnl for t in wins)
    gl = abs(sum(t.pnl for t in losses))
    final = bt.equity_history[-1] if bt.equity_history else BALANCE
    peak, mdd = BALANCE, 0.0
    for eq in bt.equity_history:
        peak = max(peak, eq)
        if peak > 0:
            mdd = max(mdd, (peak - eq) / peak * 100.0)
    longs = [t for t in closed if t.side == "buy"]
    shorts = [t for t in closed if t.side == "sell"]
    return {
        "gate": gate, "n": len(closed),
        "n_long": len(longs), "n_short": len(shorts),
        "ret_pct": round((final / BALANCE - 1) * 100, 2),
        "wr": round(100 * len(wins) / len(closed), 1) if closed else 0.0,
        "pf": round(gw / gl, 2) if gl > 0 else None,
        "max_dd": round(mdd, 2),
        "net": round(sum(t.pnl for t in closed), 2),
        "long": round(sum(t.pnl for t in longs), 2),
        "short": round(sum(t.pnl for t in shorts), 2),
    }


def main() -> int:
    t0 = time.time()
    A, B = load(DIR_A), load(DIR_B)
    nb = min(len(B[s]) for s in B)
    half = nb // 2
    out = {"A_full": [], "B_full": [], "B_second_half": []}
    for g in GATES:
        out["A_full"].append(run(A, g))
        out["B_full"].append(run(B, g))
        out["B_second_half"].append(run(B, g, half, None))
        print(f"gate={g:<4} A net={out['A_full'][-1]['net']:>7} "
              f"(long {out['A_full'][-1]['long']:>6}) | "
              f"B net={out['B_full'][-1]['net']:>7} "
              f"(long {out['B_full'][-1]['long']:>6}) | "
              f"B2h net={out['B_second_half'][-1]['net']:>7}  "
              f"[{time.time()-t0:.0f}s]", flush=True)
    dest = os.path.join(ROOT, "analysis", "runs", "sweep_long_gate_v22.json")
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=1)
    print("saved:", dest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
