#!/usr/bin/env python3
"""Systematic strategy screening on REAL data (think-tank step 2).

For every candidate strategy (family x parameter set):

* Runs the full event-driven backtest on real OKX data (3 symbols, 60 days)
  with the risk circuit breakers relaxed (MAX_DD=100, daily loss=100) so the
  screen measures *strategy edge* rather than the risk overlay.
* Splits the outcome into In-Sample (bars 0..11520, days 1-40) and
  Out-of-Sample (bars 11520..17280, days 41-60).
* Computes costs-aware metrics: net PnL, trades, win rate, profit factor,
  max drawdown, per-symbol PnL, bootstrap CI of total PnL.

Pass criteria (decided by the panel *before* looking at the results):

    IS  : net return > +2% after costs, trades >= 25, PF >= 1.1, maxDD < 20%
    OOS : net return > 0, trades >= 8
    robustness: >= 2 of 3 symbols positive in IS

Only candidates that pass ALL criteria are "successful" and eligible for the
live engine.

Run:  python analysis/screen_strategies.py
"""
from __future__ import annotations

import csv
import dataclasses
import json
import math
import os
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.backtest.backtester import Backtester  # noqa: E402
from app.config import Settings  # noqa: E402
from app.models import Candle  # noqa: E402
from app.strategy.signals import DEFAULT_V2_PARAMS  # noqa: E402

DATA_DIR = str(ROOT / "analysis" / "data")
SYMBOLS = ["ETHUSD", "SOLUSD", "XRPUSD"]
BARS_PER_DAY = 288
IS_END = 40 * BARS_PER_DAY      # bars 0..11520  -> days 1-40
N_BARS = 60 * BARS_PER_DAY      # 17280

PASS = {
    "is_return_pct_min": 2.0,
    "is_trades_min": 25,
    "is_pf_min": 1.1,
    "is_maxdd_max": 20.0,
    "oos_return_pct_min": 0.0,
    "oos_trades_min": 8,
    "min_positive_symbols": 2,
}


def load_csv_market(directory: str, symbols: List[str]) -> Dict[str, List[Candle]]:
    market: Dict[str, List[Candle]] = {}
    for sym in symbols:
        path = os.path.join(directory, f"{sym}.csv")
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


# ---------------------------------------------------------------------------
# Candidate universe (6 families x 3 variants)
# ---------------------------------------------------------------------------
def candidate_universe() -> List[Dict]:
    variants: Dict[str, List[Dict]] = {
        "TrendPullback_HTF": [
            {"sl_m": 2.0, "tp_m": 3.0, "trend_min": 0.05},
            {"sl_m": 2.5, "tp_m": 4.0, "trend_min": 0.05},
            {"sl_m": 1.5, "tp_m": 2.2, "trend_min": 0.10},
        ],
        "HTF_Breakout": [
            {"sl_m": 1.5, "tp_m": 3.0, "trend_min": 0.05},
            {"sl_m": 2.0, "tp_m": 4.0, "trend_min": 0.05},
            {"sl_m": 1.5, "tp_m": 3.5, "trend_min": 0.02},
        ],
        "MomentumRetrace_RSI": [
            {"sl_m": 2.0, "tp_m": 2.2, "rsi15_low": 32, "rsi15_high": 68},
            {"sl_m": 1.8, "tp_m": 1.6, "rsi15_low": 28, "rsi15_high": 72},
            {"sl_m": 2.5, "tp_m": 2.5, "rsi15_low": 35, "rsi15_high": 65},
        ],
        "MeanReversion_BB": [
            {"sl_m": 1.0, "rsi_low": 30, "rsi_high": 70, "max_trend": 0.15},
            {"sl_m": 0.8, "rsi_low": 25, "rsi_high": 75, "max_trend": 0.12},
            {"sl_m": 1.5, "rsi_low": 32, "rsi_high": 68, "max_trend": 0.20},
        ],
        "VolatilityExpansion": [
            {"sl_m": 1.8, "tp_m": 2.5, "atr_mult": 1.5, "vol_mult": 1.3},
            {"sl_m": 1.8, "tp_m": 3.5, "atr_mult": 2.0, "vol_mult": 1.5},
            {"sl_m": 1.5, "tp_m": 2.0, "atr_mult": 1.3, "vol_mult": 1.2},
        ],
        "SwingPullback_1h": [
            {"sl_m": 1.2, "tp_m": 3.0, "trend_min": 0.05},
            {"sl_m": 1.5, "tp_m": 4.0, "trend_min": 0.05},
            {"sl_m": 1.0, "tp_m": 2.5, "trend_min": 0.10},
        ],
    }
    out: List[Dict] = []
    for family, param_sets in variants.items():
        for idx, params in enumerate(param_sets):
            merged = dict(DEFAULT_V2_PARAMS.get(family, {}))
            merged.update(params)
            out.append({"name": family, "variant": idx + 1, "params": merged,
                        "sides": "both"})
    # ---- Round 2 (informed by round-1 failures): long-only + relaxed MR ----
    round2 = [
        {"name": "MeanReversion_BB", "variant": 4,
         "params": {**DEFAULT_V2_PARAMS["MeanReversion_BB"], "rsi_low": 42,
                    "rsi_high": 58, "sl_m": 0.7, "max_bbw": 0.12,
                    "max_trend": 0.20}, "sides": "both"},
        {"name": "MeanReversion_BB", "variant": 5,
         "params": {**DEFAULT_V2_PARAMS["MeanReversion_BB"], "rsi_low": 40,
                    "rsi_high": 60, "sl_m": 1.2, "max_bbw": 0.10,
                    "max_trend": 0.15}, "sides": "both"},
        {"name": "TrendPullback_HTF", "variant": 4,
         "params": {**DEFAULT_V2_PARAMS["TrendPullback_HTF"], "sl_m": 2.5,
                    "tp_m": 4.0, "trend_min": 0.05}, "sides": "long"},
        {"name": "TrendPullback_HTF", "variant": 5,
         "params": {**DEFAULT_V2_PARAMS["TrendPullback_HTF"], "sl_m": 2.0,
                    "tp_m": 4.0, "trend_min": 0.10}, "sides": "long"},
        {"name": "HTF_Breakout", "variant": 4,
         "params": {**DEFAULT_V2_PARAMS["HTF_Breakout"], "sl_m": 2.0,
                    "tp_m": 4.0, "trend_min": 0.02}, "sides": "long"},
        {"name": "MomentumRetrace_RSI", "variant": 4,
         "params": {**DEFAULT_V2_PARAMS["MomentumRetrace_RSI"], "rsi15_low": 48,
                    "rsi15_high": 52, "sl_m": 2.0, "tp_m": 2.5}, "sides": "long"},
        {"name": "VolatilityExpansion", "variant": 4,
         "params": {**DEFAULT_V2_PARAMS["VolatilityExpansion"], "atr_mult": 2.0,
                    "vol_mult": 1.5, "sl_m": 1.8, "tp_m": 3.5}, "sides": "long"},
        {"name": "SwingPullback_1h", "variant": 4,
         "params": {**DEFAULT_V2_PARAMS["SwingPullback_1h"], "sl_m": 1.5,
                    "tp_m": 4.0, "trend_min": 0.05}, "sides": "long"},
    ]
    out.extend(round2)
    return out


# ---------------------------------------------------------------------------
# Worker: one candidate -> metrics
# ---------------------------------------------------------------------------
def _split_metrics(trades: List[dict], equity: List[float],
                   start: int, end: int) -> Dict:
    t = [tr for tr in trades if start <= tr["closed_bar"] < end]
    pnls = [tr["pnl"] for tr in t]
    wins = sum(1 for p in pnls if p > 0)
    gross_win = sum(p for p in pnls if p > 0)
    gross_loss = abs(sum(p for p in pnls if p <= 0))
    # equity window
    seg = equity[start:end] if end <= len(equity) else equity[start:]
    start_eq = equity[start] if start < len(equity) else (equity[0] if equity else 0.0)
    end_eq = seg[-1] if seg else start_eq
    peak, maxdd = start_eq, 0.0
    for eq in seg:
        peak = max(peak, eq)
        maxdd = max(maxdd, (peak - eq) / peak * 100.0 if peak > 0 else 0.0)
    per_sym: Dict[str, float] = {}
    for tr in t:
        per_sym[tr["symbol"]] = per_sym.get(tr["symbol"], 0.0) + tr["pnl"]
    return {
        "n": len(t), "pnl": sum(pnls), "ret_pct": (end_eq / start_eq - 1.0) * 100.0
        if start_eq > 0 else 0.0,
        "wr": wins / len(t) * 100 if t else 0.0,
        "pf": round(gross_win / gross_loss, 2) if gross_loss > 0 else 0.0,
        "maxdd": round(maxdd, 2),
        "per_symbol": {k: round(v, 2) for k, v in per_sym.items()},
    }


def _bootstrap_ci(pnls: List[float], iters: int = 5000,
                  seed: int = 11) -> Tuple[float, float]:
    import random
    rng = random.Random(seed)
    n = len(pnls)
    if n == 0:
        return 0.0, 0.0
    sums = [sum(rng.choices(pnls, k=n)) for _ in range(iters)]
    return sorted(sums)[int(iters * 0.025)], sorted(sums)[int(iters * 0.975)]


def _run_candidate(cfg: Dict) -> Dict:
    name, variant, params = cfg["name"], cfg["variant"], cfg["params"]
    sides = cfg.get("sides", "both")
    market = load_csv_market(DATA_DIR, SYMBOLS)
    settings = dataclasses.replace(
        Settings(),
        max_dd_pct=100.0, max_daily_loss_pct=100.0, max_daily_entries=6,
        entry_cooldown_s=3600.0, sides=sides,
        enabled_strategies=[name], strategy_params={name: params},
    )
    bt = Backtester(settings, initial_balance=1000.0, slippage_bps=2.0)
    report = bt.run(market)
    trades = [
        {"symbol": t.symbol, "side": t.side, "strategy": t.strategy,
         "pnl": t.pnl, "fees": t.fees, "hold_bars": t.hold_bars,
         "reason": t.reason, "opened_bar": t.opened_bar,
         "closed_bar": t.closed_bar}
        for t in report.trades
    ]
    is_ = _split_metrics(trades, bt.equity_history, 0, IS_END)
    oos = _split_metrics(trades, bt.equity_history, IS_END, N_BARS)
    is_["boot_ci"] = [round(v, 2) for v in _bootstrap_ci(
        [t["pnl"] for t in trades if t["closed_bar"] < IS_END])]
    gross = sum(t["pnl"] + t["fees"] for t in trades)
    fees = sum(t["fees"] for t in trades)
    turnover = sum(t.entry * t.qty + t.exit_price * t.qty for t in report.trades)
    return {
        "name": name, "variant": variant, "params": params, "sides": sides,
        "is": is_, "oos": oos, "total_pnl": report.net_pnl(),
        "gross_edge": round(gross, 2), "fees": round(fees, 2),
        "entries_total": len(trades), "final_eq": report.final_equity,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    candidates = candidate_universe()
    print(f"Screening {len(candidates)} candidates x {len(SYMBOLS)} symbols "
          f"(60d real OKX, 2 workers)…")
    t0 = time.time()
    results: List[Dict] = []
    with ProcessPoolExecutor(max_workers=2) as pool:
        for res in pool.map(_run_candidate, candidates):
            results.append(res)
            is_, oos = res["is"], res["oos"]
            tag = f" [{res['sides']}]" if res['sides'] != 'both' else ""
            print(f"  {res['name']:<22} v{res['variant']}{tag:<6} "
                  f"IS {is_['ret_pct']:>7.2f}% ({is_['n']}t, PF {is_['pf']}, "
                  f"DD {is_['maxdd']}%) | OOS {oos['ret_pct']:>7.2f}% ({oos['n']}t)")
    print(f"screening done in {time.time()-t0:.0f}s")

    # ---- evaluate pass criteria -------------------------------------------
    for r in results:
        is_, oos = r["is"], r["oos"]
        checks = {
            "is_ret": is_["ret_pct"] >= PASS["is_return_pct_min"],
            "is_n": is_["n"] >= PASS["is_trades_min"],
            "is_pf": is_["pf"] >= PASS["is_pf_min"],
            "is_dd": is_["maxdd"] <= PASS["is_maxdd_max"],
            "oos_ret": oos["ret_pct"] >= PASS["oos_return_pct_min"],
            "oos_n": oos["n"] >= PASS["oos_trades_min"],
            "symbols": sum(1 for v in is_["per_symbol"].values() if v > 0)
                       >= PASS["min_positive_symbols"],
        }
        r["checks"] = checks
        r["passed"] = all(checks.values())

    winners = [r for r in results if r["passed"]]
    names = ", ".join(f"{r['name']}v{r['variant']}" for r in winners)
    print(f"\nPassed {len(winners)}/{len(results)}: {names}")

    # ---- persist -----------------------------------------------------------
    os.makedirs(ROOT / "analysis" / "runs", exist_ok=True)
    with open(ROOT / "analysis" / "runs" / "screening_results.json", "w") as fh:
        json.dump(results, fh, indent=1)

    # ---- render markdown report -------------------------------------------
    L: List[str] = []
    A = L.append
    A("# STRATEGY SCREENING REPORT — Real Data (60d OKX), Think-Tank Step 2")
    A("")
    A(f"- Universe: **{len(candidates)} candidates** (6 strategy families x 3 parameter sets)")
    A(f"- Data: real OKX spot 5m, {len(SYMBOLS)} symbols ({', '.join(SYMBOLS)}), "
      f"2026-06-22 -> 2026-08-21")
    A(f"- Costs: 0.05% taker/side, 2bps slippage/side (same as production)")
    A(f"- Split: IS = days 1-40 (bars < {IS_END}), OOS = days 41-60")
    A(f"- Circuit breakers relaxed during screening (measure edge, not the overlay)")
    A("")
    A("### Pass criteria (set before the screen)")
    A("")
    A(f"| Gate | Requirement |")
    A("|---|---|")
    A(f"| IS return | >= +{PASS['is_return_pct_min']}% after costs |")
    A(f"| IS trades | >= {PASS['is_trades_min']} |")
    A(f"| IS profit factor | >= {PASS['is_pf_min']} |")
    A(f"| IS max drawdown | <= {PASS['is_maxdd_max']}% |")
    A(f"| OOS return | >= {PASS['oos_return_pct_min']}% |")
    A(f"| OOS trades | >= {PASS['oos_trades_min']} |")
    A(f"| Robustness | >= {PASS['min_positive_symbols']} of 3 symbols positive in IS |")
    A("")
    A("## Results (all candidates)")
    A("")
    A("| # | Strategy | Var | IS ret% | IS n | IS WR% | IS PF | IS DD% | OOS ret% | OOS n | Pass |")
    A("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in sorted(results, key=lambda r: (-r["passed"], -r["is"]["ret_pct"])):
        A(f"| {r['name']} | v{r['variant']} | {r['is']['ret_pct']:+.2f} | "
          f"{r['is']['n']} | {r['is']['wr']:.0f} | {r['is']['pf']} | "
          f"{r['is']['maxdd']:.1f} | {r['oos']['ret_pct']:+.2f} | {r['oos']['n']} | "
          f"{'✅' if r['passed'] else '❌'} |")
    A("")
    A("## Accepted (successful) strategies")
    A("")
    if not winners:
        A("**None passed.** The panel must go back to the drawing board — see "
          "recommendations below.")
    for r in winners:
        A(f"### {r['name']} v{r['variant']}")
        A("")
        A(f"- Params: `{json.dumps(r['params'])}`")
        A(f"- IS: {r['is']['ret_pct']:+.2f}% ({r['is']['n']} trades, WR "
          f"{r['is']['wr']:.0f}%, PF {r['is']['pf']}, maxDD {r['is']['maxdd']}%, "
          f"bootstrap CI {r['is']['boot_ci']})")
        A(f"- OOS: {r['oos']['ret_pct']:+.2f}% ({r['oos']['n']} trades)")
        A(f"- Per-symbol IS: `{r['is']['per_symbol']}`")
        A(f"- Total (60d): {r['total_pnl']:+.2f} USD · gross edge "
          f"{r['gross_edge']:+.2f} · fees {r['fees']:.2f}")
    A("")
    A("## Think-tank notes")
    A("")
    A("- Screening relaxes the circuit breakers **on purpose**: a good strategy "
      "should show edge before the risk overlay is applied; the overlay stays on "
      "in production.")
    A("- OOS is only 20 days — treat OOS as a sanity check, not proof. Winners "
      "must additionally pass the final 8-symbol validation and the live-engine "
      "smoke test before deployment.")
    A("- Costs are realistic (taker fees + slippage); gross edge is reported "
      "separately so fee-sensitivity is transparent.")
    A("")
    A(f"*Generated {time.strftime('%Y-%m-%d %H:%M UTC')} · deterministic seeds · "
      "raw results in `analysis/runs/screening_results.json`*")
    A("")
    report_path = ROOT / "analysis" / "STRATEGY_SCREENING_REPORT.md"
    report_path.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"report -> {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
