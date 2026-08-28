#!/usr/bin/env python3
"""Reproducible validation of Donchian_Trend (v20.6) on committed 1h data.

The repo documents v20.6 headline numbers (analysis/STRATEGY_v20.6.md) but
ships no script that produced them. This script is that missing pipeline:

    data       analysis/data_1h/{SYM}.csv  (real Binance 1h, 2024-03-01
              -> 2025-04-30, 5 symbols, 10,224 bars each)
    engine     Backtester(base_tf="1h") — same strategy/risk code as live
    costs      taker 0.05%/side + slippage 2bps/side (production defaults)

Reports:
  * full sample, train (first 60%) / test (last 40%)
  * walk-forward: 4 sequential quarters
  * signal cadence sensitivity: every 3 bars (committed default) vs every
    1 bar (what live does — scans every ~70s after each 1h close)
  * parameter plateau around the shipped centre

Deterministic: no RNG in the backtester.

Usage:  python analysis/validate_donchian_1h.py [--json out.json]
"""
from __future__ import annotations

import argparse
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

DATA_DIR = os.path.join(ROOT, "analysis", "data_1h")
SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD", "BNBUSD", "DOGEUSD"]
BALANCE = 1000.0


def load_csv(directory: str, symbols: List[str]) -> Dict[str, List[Candle]]:
    market: Dict[str, List[Candle]] = {}
    for sym in symbols:
        path = os.path.join(directory, f"{sym}.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        rows: List[Candle] = []
        with open(path, newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                try:
                    rows.append(Candle(
                        ts=int(float(row["ts"])), o=float(row["o"]),
                        h=float(row["h"]), l=float(row["l"]),
                        c=float(row["c"]), v=float(row["v"]),
                    ))
                except (KeyError, ValueError):
                    continue
        rows.sort(key=lambda c: c.ts)
        market[sym] = rows
    return market


def make_settings(**overrides) -> Settings:
    base = dict(
        enabled_strategies=("Donchian_Trend",),
        sides="both",
        timeframe="1h",
        mid_timeframe="4h",
        htf_timeframe="4h",
    )
    base.update(overrides)
    return dataclasses.replace(Settings(), **base)


def run_bt(market: Dict[str, List[Candle]], bar0: int = 0, bar1: int | None = None,
          signal_every_n: int = 3, **overrides) -> Backtester:
    settings = make_settings(**overrides)
    bt = Backtester(settings, initial_balance=BALANCE, base_tf="1h",
                    signal_every_n=signal_every_n)
    sub = {s: market[s][bar0:bar1] for s in market}
    bt.run(sub)
    return bt


def summarize(bt: Backtester) -> Dict:
    d = bt.report_dict if hasattr(bt, "report_dict") else None
    n = len(bt.closed)
    wins = [t.pnl for t in bt.closed if t.pnl > 0]
    losses = [t.pnl for t in bt.closed if t.pnl < 0]
    gross_w, gross_l = sum(wins), abs(sum(losses))
    final = bt.equity_history[-1] if bt.equity_history else BALANCE
    peak, max_dd = bt.initial_balance, 0.0
    for eq in bt.equity_history:
        peak = max(peak, eq)
        max_dd = max(max_dd, (peak - eq) / peak * 100.0 if peak > 0 else 0.0)
    return {
        "ret_pct": round((final / BALANCE - 1.0) * 100.0, 2),
        "n": n,
        "wr": round(100.0 * len(wins) / n, 1) if n else 0.0,
        "pf": round(gross_w / gross_l, 2) if gross_l > 0 else None,
        "max_dd": round(max_dd, 2),
        "net": round(sum(t.pnl for t in bt.closed), 2),
        "by_side": {
            "long": round(sum(t.pnl for t in bt.closed if t.side == "buy"), 2),
            "short": round(sum(t.pnl for t in bt.closed if t.side == "sell"), 2),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default="")
    args = parser.parse_args()

    t0 = time.time()
    market = load_csv(DATA_DIR, SYMBOLS)
    n_bars = min(len(market[s]) for s in market)
    first = market["BTCUSD"][0].ts // 1000
    last = market["BTCUSD"][-1].ts // 1000
    import datetime as _dt
    print(f"data: {n_bars} x 1h bars  "
          f"{_dt.datetime.utcfromtimestamp(first):%Y-%m-%d} -> "
          f"{_dt.datetime.utcfromtimestamp(last):%Y-%m-%d}  "
          f"({n_bars / 24 / 30.44:.1f} months)")
    print(f"loaded in {time.time() - t0:.1f}s\n")

    out: Dict = {"n_bars": n_bars}

    # 1) headline, committed cadence (every 3 bars) and live cadence (every bar)
    for label, cadence in (("cadence=3h (committed default)", 3),
                           ("cadence=1h (live-equivalent)", 1)):
        bt = run_bt(market, signal_every_n=cadence)
        s = summarize(bt)
        out[label] = s
        print(f"{label}: ret={s['ret_pct']:+.2f}% n={s['n']} "
              f"WR={s['wr']}% PF={s['pf']} maxDD={s['max_dd']}% "
              f"net=${s['net']:+.2f} sides={s['by_side']}")

    # 2) train/test 60/40 split at committed cadence
    split = int(n_bars * 0.6)
    bt_tr = run_bt(market, bar0=0, bar1=split)
    bt_te = run_bt(market, bar0=split)
    tr, te = summarize(bt_tr), summarize(bt_te)
    out["train_60pct"], out["test_40pct"] = tr, te
    print(f"\ntrain (60%): ret={tr['ret_pct']:+.2f}% n={tr['n']} PF={tr['pf']} maxDD={tr['max_dd']}%")
    print(f"test  (40%): ret={te['ret_pct']:+.2f}% n={te['n']} PF={te['pf']} maxDD={te['max_dd']}%")

    # 3) walk-forward: 4 sequential quarters (committed cadence)
    q = n_bars // 4
    wf = []
    for i in range(4):
        b0, b1 = i * q, (i + 1) * q if i < 3 else n_bars
        s = summarize(run_bt(market, bar0=b0, bar1=b1))
        wf.append(s["ret_pct"])
    out["walk_forward_quarters"] = wf
    print(f"walk-forward quarters: {wf}")

    # 4) parameter plateau (full sample, committed cadence)
    plateau: List[Dict] = []
    for entry_len in (30, 40, 50, 60):
        for sl_m in (2.0, 2.5, 3.0):
            for break_atr in (1.4, 1.5, 1.6, 2.0):
                s = summarize(run_bt(
                    market,
                    strategy_params={"Donchian_Trend": {
                        "entry_len": entry_len, "sl_m": sl_m, "tp_m": 20.0,
                        "break_atr": break_atr}}))
                plateau.append({"entry_len": entry_len, "sl_m": sl_m,
                                "break_atr": break_atr, **s})
    pos = [p for p in plateau if p["ret_pct"] > 0]
    out["plateau"] = plateau
    print(f"\nplateau: {len(pos)}/{len(plateau)} combos positive "
          f"(ret {min(p['ret_pct'] for p in pos):+.2f}% .. "
          f"{max(p['ret_pct'] for p in pos):+.2f}%)")

    # 5) warm-up sensitivity (claimed 120 vs 210 bars)
    for mb in (120, 210, 260):
        settings = make_settings()
        bt = Backtester(settings, initial_balance=BALANCE, base_tf="1h",
                        min_bars=mb, signal_every_n=3)
        bt.run(market)
        s = summarize(bt)
        out[f"warmup_{mb}"] = s
        print(f"warmup={mb} bars: ret={s['ret_pct']:+.2f}% n={s['n']} PF={s['pf']}")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(out, fh, indent=1)
        print(f"\nsaved {args.json}")
    print(f"\ntotal runtime {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
