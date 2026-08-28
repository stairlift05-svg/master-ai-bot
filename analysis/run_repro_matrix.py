#!/usr/bin/env python3
"""Fast validation matrix for Donchian_Trend v20.6 (see validate_donchian_1h.py).

Trims the plateau to the TEST half only so the full matrix finishes quickly.
Writes JSON + prints a compact table.
"""
from __future__ import annotations

import json
import sys
import time

sys.path.insert(0, ".")
from analysis.validate_donchian_1h import (  # noqa: E402
    DATA_DIR, SYMBOLS, load_csv, run_bt, summarize,
)

out = {}
t_start = time.time()
market = load_csv(DATA_DIR, SYMBOLS)
n = min(len(market[s]) for s in market)
split = int(n * 0.6)
q = n // 4
print(f"n={n} split={split} q={q}", flush=True)


def tag(label, s):
    out[label] = s
    print(f"{label:<34} ret={s['ret_pct']:+7.2f}% n={s['n']:>4} "
          f"WR={s['wr']:>5}% PF={s['pf']} maxDD={s['max_dd']:>5}% "
          f"sides=({s['by_side']['long']:+.1f}/{s['by_side']['short']:+.1f}) "
          f"[{time.time() - t_start:.0f}s]", flush=True)


# headline both cadences, full sample
tag("full_cad3", summarize(run_bt(market, signal_every_n=3)))
tag("full_cad1", summarize(run_bt(market, signal_every_n=1)))
# train / test at committed cadence
tag("train60_cad3", summarize(run_bt(market, bar0=0, bar1=split)))
tag("test40_cad3", summarize(run_bt(market, bar0=split)))
tag("test40_cad1", summarize(run_bt(market, bar0=split, signal_every_n=1)))
# walk-forward quarters (cadence 3)
wf = []
for i in range(4):
    b0, b1 = i * q, (i + 1) * q if i < 3 else n
    s = summarize(run_bt(market, bar0=b0, bar1=b1))
    wf.append(s["ret_pct"])
    tag(f"wf_q{i + 1}", s)
out["walk_forward"] = wf
# warm-up sensitivity
for mb in (120, 210, 260):
    import dataclasses
    from app.backtest.backtester import Backtester
    from app.config import Settings
    settings = dataclasses.replace(
        Settings(), enabled_strategies=("Donchian_Trend",), sides="both",
        timeframe="1h", mid_timeframe="4h", htf_timeframe="4h")
    bt = Backtester(settings, initial_balance=1000.0, base_tf="1h",
                    min_bars=mb, signal_every_n=3)
    bt.run(market)
    tag(f"warmup_{mb}", summarize(bt))
# plateau: TEST half only, entry_len x sl_m x break_atr
plateau = []
for entry_len in (30, 40, 50, 60):
    for sl_m in (2.0, 2.5, 3.0):
        for br in (1.4, 1.5, 1.6, 2.0):
            s = summarize(run_bt(
                market, bar0=split,
                strategy_params={"Donchian_Trend": {
                    "entry_len": entry_len, "sl_m": sl_m, "tp_m": 20.0,
                    "break_atr": br}}))
            row = {"entry_len": entry_len, "sl_m": sl_m, "break_atr": br,
                   "ret": s["ret_pct"], "n": s["n"], "pf": s["pf"]}
            plateau.append(row)
pos = [p for p in plateau if p["ret"] > 0]
out["plateau_test_half"] = plateau
print(f"plateau: {len(pos)}/{len(plateau)} positive on test half "
      f"[{time.time() - t_start:.0f}s]", flush=True)

with open("analysis/runs/donchian_v206_repro.json", "w") as fh:
    json.dump(out, fh, indent=1)
print(f"DONE in {time.time() - t_start:.0f}s -> analysis/runs/donchian_v206_repro.json",
      flush=True)
