#!/usr/bin/env python3
"""Final validation of the top candidates under REAL production settings.

Runs the 8-symbol, 60-day backtest with the production risk overlay ON
(drawdown halt 10%, daily-loss halt 5%, daily budget, cooldowns) for:

    A) HTF_Breakout long-only (best OOS candidate)
    B) MeanReversion_BB v3    (best IS candidate)
    C) A + B combined
    D) All v2 defaults        (reference)

Every number feeds the panel's final accept/reject decision.
"""
from __future__ import annotations

import dataclasses
import json
import sys
import time
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from analysis.screen_strategies import load_csv_market, _split_metrics, _bootstrap_ci  # noqa: E402
from app.backtest.backtester import Backtester  # noqa: E402
from app.config import Settings  # noqa: E402

DATA_DIR = str(ROOT / "analysis" / "data")
SYMBOLS = ["ETHUSD", "SOLUSD", "XRPUSD", "AVAXUSD",
           "DOTUSD", "LINKUSD", "ADAUSD", "DOGEUSD"]
IS_END = 40 * 288
N_BARS = 60 * 288

CONFIGS = {
    "A_HTFBreakout_long": {
        "enabled_strategies": ["HTF_Breakout"],
        "strategy_params": {"HTF_Breakout": {"sl_m": 2.0, "tp_m": 4.0,
                                             "trend_min": 0.02}},
        "sides": "long",
    },
    "B_MeanRev_v3": {
        "enabled_strategies": ["MeanReversion_BB"],
        "strategy_params": {"MeanReversion_BB": {"sl_m": 1.0, "rsi_low": 30,
                                                 "rsi_high": 70,
                                                 "max_trend": 0.15,
                                                 "max_bbw": 0.05}},
        "sides": "both",
    },
    "C_Combined": {
        "enabled_strategies": ["HTF_Breakout", "MeanReversion_BB"],
        "strategy_params": {
            "HTF_Breakout": {"sl_m": 2.0, "tp_m": 4.0, "trend_min": 0.02},
            "MeanReversion_BB": {"sl_m": 1.0, "rsi_low": 30, "rsi_high": 70,
                                 "max_trend": 0.15, "max_bbw": 0.05}},
        "sides": "both",
    },
    "D_AllDefaults": {
        "enabled_strategies": None, "strategy_params": None, "sides": "both",
    },
}


def run_config(name: str, cfg: Dict, market: Dict) -> Dict:
    settings = dataclasses.replace(
        Settings(),
        enabled_strategies=cfg["enabled_strategies"],
        strategy_params=cfg["strategy_params"],
        sides=cfg["sides"],
    )  # production halts/limits remain default (10% DD, 5% daily, cap 8)
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
    return {
        "name": name, "net": report.net_pnl(), "final_eq": report.final_equity,
        "ret_pct": (report.final_equity / 1000.0 - 1.0) * 100.0,
        "max_dd": report.max_dd_pct, "n": len(trades),
        "wr": report.win_rate(), "pf": report.profit_factor(),
        "halted_frac": report.halted_bars / N_BARS * 100.0,
        "is": is_, "oos": oos,
        "per_symbol": report.per_strategy,
    }


def main() -> int:
    market = load_csv_market(DATA_DIR, SYMBOLS)
    results = []
    for name, cfg in CONFIGS.items():
        t0 = time.time()
        r = run_config(name, cfg, market)
        r["secs"] = round(time.time() - t0)
        results.append(r)
        print(f"{name:<20} net={r['net']:>+8.2f} ret={r['ret_pct']:>+7.2f}% "
              f"DD={r['max_dd']:>5.2f}% n={r['n']:>4d} WR={r['wr']:>5.1f}% "
              f"PF={r['pf']:>4.2f} halt={r['halted_frac']:.0f}% | "
              f"IS {r['is']['ret_pct']:+.2f}% OOS {r['oos']['ret_pct']:+.2f}% "
              f"({r['secs']}s)")

    Path(ROOT / "analysis" / "runs").mkdir(exist_ok=True)
    with open(ROOT / "analysis" / "runs" / "final_validation.json", "w") as fh:
        json.dump(results, fh, indent=1)
    print("saved analysis/runs/final_validation.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
