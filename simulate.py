#!/usr/bin/env python3
"""Offline simulation harness.

Runs the baseline backtest plus every market-shock stress scenario against
the *same* strategy, risk and portfolio code used live, and writes a text
report to ``reports/backtest_report.txt`` (plus optional JSON).

Example::

    python simulate.py --days 45 --seed 7 --balance 500 --symbols ETHUSD,SOLUSD,XRPUSD
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from typing import Dict, List

# Make the package importable when run from any directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.backtest.stress import run_stress  # noqa: E402
from app.backtest.backtester import Backtester  # noqa: E402
from app.backtest.synthetic import resample_1h  # noqa: E402
from app.config import Settings  # noqa: E402
from app.models import Candle  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s | %(message)s")

_DEFAULT_SYMBOLS = ["ETHUSD", "SOLUSD", "XRPUSD", "AVAXUSD"]
_BASE_PRICES = {
    "ETHUSD": 1800.0, "SOLUSD": 120.0, "XRPUSD": 0.55, "AVAXUSD": 25.0,
    "DOTUSD": 5.0, "LINKUSD": 12.0, "ADAUSD": 0.45, "DOGEUSD": 0.08,
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quant v20 offline simulation")
    parser.add_argument("--days", type=int, default=40,
                        help="simulated trading days (default 40)")
    parser.add_argument("--seed", type=int, default=42,
                        help="RNG seed for the synthetic market (default 42)")
    parser.add_argument("--balance", type=float, default=500.0,
                        help="starting balance in USDT (default 500)")
    parser.add_argument("--symbols", type=str,
                        default=",".join(_DEFAULT_SYMBOLS),
                        help="comma-separated AriaX symbols (default 4 symbols)")
    parser.add_argument("--json", action="store_true",
                        help="also write reports/stress_report.json")
    parser.add_argument(
        "--csv", type=str, default="",
        help="directory of real CSV files ({SYMBOL}.csv with columns "
             "ts,o,h,l,c,v) to backtest instead of synthetic data",
    )
    parser.add_argument(
        "--tf", type=str, default="",
        help="base timeframe of the CSV data: '5m' or '1h' "
             "(default: auto-detected from the bar spacing; v21 review F-04 "
             "— 1h data previously ran under the 5m preset, mis-scaling "
             "cooldowns, daily rolls and context resamples by 12x)",
    )
    return parser.parse_args()


def load_csv_market(directory: str, symbols: List[str]) -> Dict[str, List[Candle]]:
    """Load 5m candles from per-symbol CSV files (ts,o,h,l,c,v)."""
    market: Dict[str, List[Candle]] = {}
    for sym in symbols:
        path = os.path.join(directory, f"{sym}.csv")
        if not os.path.exists(path):
            print(f"⚠️  missing {path} — skipping {sym}")
            continue
        rows: List[Candle] = []
        with open(path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                try:
                    rows.append(Candle(
                        ts=int(float(row["ts"])),
                        o=float(row["o"]), h=float(row["h"]),
                        l=float(row["l"]), c=float(row["c"]), v=float(row["v"]),
                    ))
                except (KeyError, ValueError):
                    continue
        rows.sort(key=lambda c: c.ts)
        market[sym] = rows
        print(f"   loaded {sym}: {len(rows)} candles")
    return market


def main() -> int:
    args = _parse_args()
    symbols: List[str] = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    settings = Settings()  # defaults; env vars are not required offline

    if args.csv:
        print(f"=== Quant v20 backtest on REAL data ({args.csv}) ===")
        market = load_csv_market(args.csv, symbols)
        if not market:
            print("❌ no CSV data loaded")
            return 2
        base_tf = args.tf
        if not base_tf:
            first = next(iter(market.values()))
            dt = (first[1].ts - first[0].ts) / 1000.0 if len(first) > 1 else 300
            base_tf = "1h" if dt >= 1800 else "5m"
            print(f"   auto-detected base timeframe: {base_tf} "
                  f"(bar spacing {dt:.0f}s)")
        bt = Backtester(settings, initial_balance=args.balance,
                        base_tf=base_tf)
        if base_tf == "1h":
            # HTF context must be 4h (4 base bars) to match the live engine
            # (htf_timeframe=4h); the 5m path keeps 1h context as before.
            from app.backtest.synthetic import resample
            htf = {s: resample(market[s], 4) for s in market}
        else:
            htf = {s: resample_1h(market[s]) for s in market}
        report = bt.run(market, htf)
        lines = ["=" * 78,
                 f"QUANT v20 — REAL-DATA BACKTEST ({list(market.keys())})",
                 "=" * 78]
        lines.append(json.dumps(report.to_dict(), indent=2))
        lines.append("Per-strategy:")
        for name, bucket in report.per_strategy.items():
            lines.append(f"  {name:<22} {bucket}")
        lines.append("NOTE: past performance is not a promise of future results.")
        text = "\n".join(lines)
        print("\n" + text)
        os.makedirs("reports", exist_ok=True)
        with open("reports/backtest_report.txt", "w", encoding="utf-8") as handle:
            handle.write(text)
        print("\nReport written to reports/backtest_report.txt")
        return 0

    print(f"=== Quant v20 simulation | {args.days}d | seed={args.seed} "
          f"| balance=${args.balance:.0f} | symbols={symbols} ===")

    report = run_stress(settings, days=args.days, seed=args.seed,
                        symbols=symbols, balance=args.balance)

    lines = ["=" * 78,
             f"QUANT v20 — BACKTEST & STRESS REPORT ({args.days} days, "
             f"seed {args.seed}, start ${args.balance:.0f})",
             "=" * 78]
    lines += report.summary_lines()
    lines.append("-" * 70)
    lines.append("Logic assertions (pure risk/security checks):")
    for name, passed in report.assertions.items():
        lines.append(f"  [{'PASS' if passed else 'FAIL'}] {name}")
    lines.append("=" * 78)
    lines.append(f"Baseline detail: {json.dumps(report.baseline.to_dict(), indent=2)}")
    lines.append("Per-strategy (baseline):")
    for name, bucket in report.baseline.per_strategy.items():
        lines.append(f"  {name:<22} {bucket}")
    lines.append("=" * 78)
    lines.append("NOTE: synthetic data only. Backtests are NOT a promise of live"
                 " results — validate on the testnet before risking capital.")

    text = "\n".join(lines)
    print("\n" + text)
    os.makedirs("reports", exist_ok=True)
    with open("reports/backtest_report.txt", "w", encoding="utf-8") as handle:
        handle.write(text)
    print(f"\nReport written to reports/backtest_report.txt")

    if args.json:
        payload = {
            "baseline": report.baseline.to_dict(),
            "scenarios": {k: v.to_dict() for k, v in report.scenarios.items()},
            "assertions": report.assertions,
        }
        with open("reports/stress_report.json", "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        print("JSON written to reports/stress_report.json")

    # Non-zero exit if any assertion fails or any scenario wipes the account.
    failures = [n for n, ok in report.assertions.items() if not ok]
    wiped = [n for n, rep in report.scenarios.items() if rep.final_equity <= 0]
    if failures or wiped:
        print("\n⚠️  STRESS TEST WARNINGS:",
              f"failed assertions={failures}", f"wiped scenarios={wiped}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
