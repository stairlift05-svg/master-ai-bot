#!/usr/bin/env python3
"""v21 final validation — the decisive run.

Runs the DONCHIAN_TREND strategy with the v21-corrected harness
(per-bar signal cadence for 1h, fee model identical to live, SL/TP
re-anchored to the actual fill) over two independent windows:

  A) analysis/data_1h      — the 14 months used for the v20.6 validation
     (2024-03 -> 2025-04, Binance spot, committed to the repo)
  B) analysis/data_1h_oos  — 16 months the strategy has NEVER seen
     (2025-05 -> 2026-08, OKX spot, fetched 2026-08-28)

Plus: per-symbol market returns in each window (regime context), a
half-split of the OOS window, and the shipped default vs long-only /
short-only. Deterministic, no credentials.

Usage:  python analysis/validate_v21_final.py
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


def load(directory: str) -> Dict[str, List[Candle]]:
    market: Dict[str, List[Candle]] = {}
    for sym in SYMBOLS:
        path = os.path.join(directory, f"{sym}.csv")
        if not os.path.exists(path):
            print(f"missing {path}", file=sys.stderr)
            sys.exit(2)
        rows: List[Candle] = []
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                try:
                    rows.append(Candle(int(float(row["ts"])),
                                       float(row["o"]), float(row["h"]),
                                       float(row["l"]), float(row["c"]),
                                       float(row["v"])))
                except (KeyError, ValueError):
                    continue
        rows.sort(key=lambda c: c.ts)
        market[sym] = rows
    return market


def settings_for(**ov) -> Settings:
    base = dict(enabled_strategies=("Donchian_Trend",), sides="both",
                timeframe="1h", mid_timeframe="4h", htf_timeframe="4h")
    base.update(ov)
    return dataclasses.replace(Settings(), **base)


def run(market: Dict[str, List[Candle]], bar0: int = 0, bar1: int = None,
        **ov) -> Backtester:
    bt = Backtester(settings_for(**ov), initial_balance=BALANCE,
                    base_tf="1h")  # v21 defaults: per-bar cadence, live fees
    bt.run({s: market[s][bar0:bar1] for s in market})
    return bt


def summ(bt: Backtester) -> Dict:
    n = len(bt.closed)
    wins = [t.pnl for t in bt.closed if t.pnl > 0]
    losses = [t.pnl for t in bt.closed if t.pnl < 0]
    gw, gl = sum(wins), abs(sum(losses))
    final = bt.equity_history[-1] if bt.equity_history else BALANCE
    peak, mdd = bt.initial_balance, 0.0
    for eq in bt.equity_history:
        peak = max(peak, eq)
        mdd = max(mdd, (peak - eq) / peak * 100.0 if peak > 0 else 0.0)
    return {
        "ret_pct": round((final / BALANCE - 1.0) * 100.0, 2),
        "n": n,
        "wr": round(100.0 * len(wins) / n, 1) if n else 0.0,
        "pf": round(gw / gl, 2) if gl > 0 else None,
        "max_dd": round(mdd, 2),
        "net": round(sum(t.pnl for t in bt.closed), 2),
        "long": round(sum(t.pnl for t in bt.closed if t.side == "buy"), 2),
        "short": round(sum(t.pnl for t in bt.closed if t.side == "sell"), 2),
        "exits": {},
    }


def market_returns(market: Dict[str, List[Candle]]) -> Dict[str, float]:
    out = {}
    for s, rows in market.items():
        out[s] = round((rows[-1].c / rows[0].c - 1.0) * 100.0, 1)
    return out


def main() -> int:
    t0 = time.time()
    A = load(DIR_A)
    B = load(DIR_B)
    na = min(len(A[s]) for s in A)
    nb = min(len(B[s]) for s in B)
    import datetime as dt
    fa = dt.datetime.fromtimestamp(A["BTCUSD"][0].ts / 1000, dt.UTC)
    fb = dt.datetime.fromtimestamp(B["BTCUSD"][-1].ts / 1000, dt.UTC)
    print(f"A: {na} bars  {fa:%Y-%m-%d} -> {dt.datetime.fromtimestamp(A['BTCUSD'][-1].ts/1000, dt.UTC):%Y-%m-%d} (Binance)")
    print(f"B: {nb} bars  {dt.datetime.fromtimestamp(B['BTCUSD'][0].ts/1000, dt.UTC):%Y-%m-%d} -> {fb:%Y-%m-%d} (OKX, unseen)")
    print(f"market returns A: {market_returns(A)}")
    print(f"market returns B: {market_returns(B)}\n")

    out: Dict = {}

    def report(label: str, bt: Backtester) -> None:
        s = summ(bt)
        exits: Dict[str, int] = {}
        for t in bt.closed:
            exits[t.reason] = exits.get(t.reason, 0) + 1
        s["exits"] = exits
        out[label] = s
        print(f"{label:<26} ret={s['ret_pct']:+7.2f}% n={s['n']:>4} "
              f"WR={s['wr']:>5}% PF={s['pf']} maxDD={s['max_dd']:>5}% "
              f"net=${s['net']:+8.2f} L/S=({s['long']:+.1f}/{s['short']:+.1f}) "
              f"[{time.time() - t0:.0f}s]", flush=True)

    # A) original 14 months, v21 harness
    report("A_full_v21", run(A))
    split_a = int(na * 0.6)
    report("A_train60", run(A, 0, split_a))
    report("A_test40", run(A, split_a))
    report("A_longonly", run(A, sides="long"))
    report("A_shortonly", run(A, sides="short"))
    # B) 16 months of truly unseen data, v21 harness
    report("B_full_v21", run(B))
    split_b = int(nb * 0.5)
    report("B_first_half", run(B, 0, split_b))
    report("B_second_half", run(B, split_b))
    report("B_longonly", run(B, sides="long"))
    report("B_shortonly", run(B, sides="short"))

    with open(os.path.join(ROOT, "analysis", "runs", "v21_final_validation.json"),
              "w") as fh:
        json.dump({"market_returns_A": market_returns(A),
                   "market_returns_B": market_returns(B), **out}, fh, indent=1)
    print(f"\nsaved analysis/runs/v21_final_validation.json "
          f"({time.time() - t0:.0f}s)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
