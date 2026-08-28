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




def run_window(which: str, gate: float) -> Dict:
    """which: A_full | B_full | B_second_half"""
    market = load(DIR_A if which.startswith("A") else DIR_B)
    if which == "B_second_half":
        half = min(len(market[s]) for s in market) // 2
        return run(market, gate, half, None)
    return run(market, gate)
