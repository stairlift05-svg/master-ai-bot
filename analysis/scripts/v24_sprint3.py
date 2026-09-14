#!/usr/bin/env python3
"""v24 sprint 3 — portfolio risk control + parameter plateau check.

Owner delegated the decision to the working-group chair (2026-09-14).

Study A — same-direction exposure cap (lesson of the live incident: six
  correlated shorts into a bullish grind). Cap concurrent same-side
  positions at K in {2,3,4} vs baseline (no cap), Donchian on both windows.
  Ship rule: both windows keep >=95% of baseline net AND max DD improves
  >=15%, or nets improve. Otherwise veto (trend-following may legitimately
  cluster same-side winners).

Study B — Donchian parameter plateau (anti-overfit check): one-factor
  sweeps around the shipped centre (entry_len 30/40/50/60, break_atr
  1.0/1.5/2.0, sl_m 2.0/2.5/3.0, tp_m 15/20/25). Ship a change only if a
  neighbour beats the centre on BOTH windows by >10% net without worse DD;
  otherwise the centre stays (plateau discipline).
"""
import importlib.util as il
import json, sys

sys.path.insert(0, "/home/user/master-ai-bot")
_sp = il.spec_from_file_location(
    "m", "/home/user/master-ai-bot/analysis/scripts/ict_validation.py")
m = il.module_from_spec(_sp); _sp.loader.exec_module(m)
_s2p = "/home/user/master-ai-bot/analysis/scripts/v24_sprint2.py"
_s2s = il.spec_from_file_location("s2", _s2p)
s2 = il.module_from_spec(_s2s); _s2s.loader.exec_module(s2)

BASE = dict(entry_len=40, sl_m=2.5, tp_m=20.0, break_atr=1.5, long_dist_atr=1.0)


def run(bars, params=None, cap=0):
    sigs = {s: s2.gen_signals("Donchian_Trend", bars[s], params) for s in m.SYMBOLS}
    return m._walk(bars, sigs, "x", max_same_side=cap)


def row(r):
    return (r["n"], r["wr"], r["pf"], r["net"], r["max_dd"], r["peak_same_side"])


def main():
    barsA = {s: m.load(m.DATA_A, s) for s in m.SYMBOLS}
    barsB = {s: m.load(m.DATA_B, s) for s in m.SYMBOLS}
    out = {}

    print("=== A) سقف هم‌جهتی ===")
    bA = run(barsA); bB = run(barsB)
    print(f"baseline     A: n={bA['n']:>3} pf={bA['pf']} net={bA['net']:>7} dd={bA['max_dd']}% peak_same={bA['peak_same_side']}")
    print(f"             B: n={bB['n']:>3} pf={bB['pf']} net={bB['net']:>7} dd={bB['max_dd']}% peak_same={bB['peak_same_side']}")
    out["cap_study"] = {"baseline": {"A": dict(bA), "B": dict(bB)}, "caps": {}}
    for k in (2, 3, 4):
        rA, rB = run(barsA, cap=k), run(barsB, cap=k)
        keepA, keepB = rA["net"] >= 0.95 * bA["net"], rB["net"] >= 0.95 * bB["net"]
        ddA, ddB = rA["max_dd"] <= 0.85 * bA["max_dd"], rB["max_dd"] <= 0.85 * bB["max_dd"]
        ship = (keepA and keepB) and (ddA or ddB or
                (rA["net"] > bA["net"] and rB["net"] > bB["net"]))
        out["cap_study"]["caps"][k] = {"A": dict(rA), "B": dict(rB), "ship": bool(ship)}
        print(f"cap={k}        A: n={rA['n']:>3} net={rA['net']:>7} dd={rA['max_dd']}% | "
              f"B: n={rB['n']:>3} net={rB['net']:>7} dd={rB['max_dd']}% | "
              f"{'SHIP' if ship else 'veto'} (keepA={keepA} keepB={keepB} ddA={ddA} ddB={ddB})")

    print("\n=== B) فلات پارامتری دونچین ===")
    out["plateau"] = {}
    grids = {
        "entry_len": [30, 50, 60],
        "break_atr": [1.0, 2.0],
        "sl_m": [2.0, 3.0],
        "tp_m": [15.0, 25.0],
    }
    for axis, vals in grids.items():
        out["plateau"][axis] = {}
        for v in vals:
            p = dict(BASE); p[axis] = v
            rA, rB = run(barsA, p), run(barsB, p)
            better = (rA["net"] > 1.10 * bA["net"] and rB["net"] > 1.10 * bB["net"]
                      and rA["max_dd"] <= bA["max_dd"] and rB["max_dd"] <= bB["max_dd"])
            out["plateau"][axis][v] = {"A": dict(rA), "B": dict(rB), "beats_base_both": bool(better)}
            print(f"{axis}={v:<5}  A: n={rA['n']:>3} pf={rA['pf']} net={rA['net']:>7} dd={rA['max_dd']}% | "
                  f"B: n={rB['n']:>3} pf={rB['pf']} net={rB['net']:>7} dd={rB['max_dd']}% | "
                  f"{'BEATS BASE' if better else 'centre holds'}")

    p = "/home/user/master-ai-bot/analysis/runs/v24_sprint3.json"
    json.dump(out, open(p, "w"), indent=1, default=str)
    print("\nsaved ->", p)


if __name__ == "__main__":
    main()
