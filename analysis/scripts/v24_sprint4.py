#!/usr/bin/env python3
"""v24 sprint 4 — council of trading masters (owner directive 2026-09-15).

Owner complaint: the bot makes no trades. Diagnosis first, then three
classical master techniques not yet tested, through the two-window rule:

  Study A — expected frequency: Donchian's validated trade rate vs the
            current flat gap (is 18h without a trade normal?).
  Study B — Turtle-style pullback-retest entry: after a breakout, wait for
            a retest of the broken channel edge (better price, tighter risk).
  Study C — volatility-regime filter: only take breakouts when ATR sits in
            the upper X percentile of its trailing distribution (vol
            expansion precedes trends — the classic trend-following gate).
  Study D — per-side evidence: long vs short contribution per window.
"""
import importlib.util as il
import json, sys
from datetime import datetime, timezone

sys.path.insert(0, "/home/user/master-ai-bot")
_m = il.spec_from_file_location("m", "/home/user/master-ai-bot/analysis/scripts/ict_validation.py")
m = il.module_from_spec(_m); _m.loader.exec_module(m)
from app.strategy import indicators as ind  # noqa: E402
from app.models import Signal  # noqa: E402

BASE = dict(entry_len=40, sl_m=2.5, tp_m=20.0, break_atr=1.5, long_dist_atr=1.0)
W = 300


def donchian_breakouts(series):
    """Raw breakout events (bar, side, channel edge) with the shipped logic."""
    closes = [b[4] for b in series]
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    e200 = ind.ema(closes, 200)
    L = int(BASE["entry_len"])
    out = []
    for i in range(205, len(series)):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i]:
            continue
        ph = max(highs[i - L - 1:i])
        pl = min(lows[i - L - 1:i])
        price = closes[i]
        if price > ph + BASE["break_atr"] * atr and price > e200[i] \
                and (price - e200[i]) >= BASE["long_dist_atr"] * atr:
            out.append((i, "buy", ph, atr))
        elif price < pl - BASE["break_atr"] * atr and price < e200[i]:
            out.append((i, "sell", pl, atr))
    return out, closes, highs, lows, atr_l, e200


def signals_retest(series, wait=12, band=0.25):
    """Study B: enter on the first retest of the broken edge within `wait` bars."""
    evs, closes, highs, lows, atr_l, e200 = donchian_breakouts(series)
    sigs = {}
    for i, side, edge, atr0 in evs:
        for j in range(i + 1, min(i + 1 + wait, len(series) - 1)):
            atr = atr_l[j] or 0.0
            if atr <= 0 or not e200[j]:
                continue
            if side == "buy":
                if lows[j] <= edge + band * atr and closes[j] > edge and closes[j] > e200[j]:
                    sigs[j] = Signal(side="buy", strategy="Donchian_Retest",
                                     reason="40-bar breakout retest", entry=closes[j],
                                     sl=closes[j] - BASE["sl_m"] * atr, tp1=None,
                                     tp=closes[j] + BASE["tp_m"] * atr, rsi=50.0,
                                     atr=atr, htf="bullish")
                    break
            else:
                if highs[j] >= edge - band * atr and closes[j] < edge and closes[j] < e200[j]:
                    sigs[j] = Signal(side="sell", strategy="Donchian_Retest",
                                     reason="40-bar breakdown retest", entry=closes[j],
                                     sl=closes[j] + BASE["sl_m"] * atr, tp1=None,
                                     tp=closes[j] - BASE["tp_m"] * atr, rsi=50.0,
                                     atr=atr, htf="bearish")
                    break
    return sigs


def signals_volgate(series, pct_thr):
    """Study C: baseline Donchian signals gated by ATR percentile."""
    evs, closes, highs, lows, atr_l, e200 = donchian_breakouts(series)
    sigs = {}
    n = len(closes)
    for i, side, edge, atr0 in evs:
        lo = max(0, i - 400)
        window = [a for a in atr_l[lo:i + 1] if a]
        if not window:
            continue
        rank = sum(1 for a in window if a <= atr0) / len(window)
        if rank < pct_thr:
            continue
        atr = atr_l[i]
        if side == "buy":
            sigs[i] = Signal(side="buy", strategy="Donchian_VolGate",
                             reason="breakout + vol regime", entry=closes[i],
                             sl=closes[i] - BASE["sl_m"] * atr, tp1=None,
                             tp=closes[i] + BASE["tp_m"] * atr, rsi=50.0,
                             atr=atr, htf="bullish")
        else:
            sigs[i] = Signal(side="sell", strategy="Donchian_VolGate",
                             reason="breakdown + vol regime", entry=closes[i],
                             sl=closes[i] + BASE["sl_m"] * atr, tp1=None,
                             tp=closes[i] - BASE["tp_m"] * atr, rsi=50.0,
                             atr=atr, htf="bearish")
    return sigs


def gen_baseline(series):
    evs, closes, highs, lows, atr_l, e200 = donchian_breakouts(series)
    sigs = {}
    for i, side, edge, atr0 in evs:
        if side == "buy":
            sigs[i] = Signal(side="buy", strategy="Donchian_Trend",
                             reason="40-bar channel breakout", entry=closes[i],
                             sl=closes[i] - BASE["sl_m"] * atr0, tp1=None,
                             tp=closes[i] + BASE["tp_m"] * atr0, rsi=50.0,
                             atr=atr0, htf="bullish")
        else:
            sigs[i] = Signal(side="sell", strategy="Donchian_Trend",
                             reason="40-bar channel breakdown", entry=closes[i],
                             sl=closes[i] + BASE["sl_m"] * atr0, tp1=None,
                             tp=closes[i] - BASE["tp_m"] * atr0, rsi=50.0,
                             atr=atr0, htf="bearish")
    return sigs


def main():
    barsA = {s: m.load(m.DATA_A, s) for s in m.SYMBOLS}
    barsB = {s: m.load(m.DATA_B, s) for s in m.SYMBOLS}
    out = {}

    # ---- baseline ------------------------------------------------------
    sigA = {s: gen_baseline(barsA[s]) for s in m.SYMBOLS}
    sigB = {s: gen_baseline(barsB[s]) for s in m.SYMBOLS}
    bA = m._walk(barsA, sigA, "A")
    bB = m._walk(barsB, sigB, "B")
    print(f"[BASE] A: n={bA['n']} net={bA['net']} pf={bA['pf']} | B: n={bB['n']} net={bB['net']} pf={bB['pf']}")

    # ---- A) frequency --------------------------------------------------
    daysA = (barsA["ETHUSD"][-1][0] - barsA["ETHUSD"][0][0]) / 86400000
    daysB = (barsB["ETHUSD"][-1][0] - barsB["ETHUSD"][0][0]) / 86400000
    rateA, rateB = bA["n"] / daysA, bB["n"] / daysB
    rate = (rateA + rateB) / 2
    import math
    p18 = math.exp(-rate * 0.75)   # P(no trade in 18h)
    print(f"[FREQ] A: {rateA:.2f}/day | B: {rateB:.2f}/day | P(no trade in 18h) = {p18:.0%}")
    out["frequency"] = {"A_per_day": round(rateA, 2), "B_per_day": round(rateB, 2),
                        "P_no_trade_18h": round(p18, 2)}

    # ---- D) per-side ----------------------------------------------------
    for tag, r in (("A", bA), ("B", bB)):
        lng = sum(t["net"] for t in r["trades"] if t["side"] == "buy")
        sht = sum(t["net"] for t in r["trades"] if t["side"] == "sell")
        nl = sum(1 for t in r["trades"] if t["side"] == "buy")
        print(f"[SIDE {tag}] long: n={nl} net={lng:+.1f} | short: n={r['n']-nl} net={sht:+.1f}")
        out.setdefault("per_side", {})[tag] = {"long_n": nl, "long_net": round(lng, 2),
                                               "short_n": r["n"] - nl, "short_net": round(sht, 2)}

    # ---- B) retest ------------------------------------------------------
    rA = m._walk(barsA, {s: signals_retest(barsA[s]) for s in m.SYMBOLS}, "A")
    rB = m._walk(barsB, {s: signals_retest(barsB[s]) for s in m.SYMBOLS}, "B")
    ok = rA["net"] > bA["net"] and rB["net"] > bB["net"]
    out["retest"] = {"A": dict(rA), "B": dict(rB), "beats_base_both": bool(ok)}
    print(f"[RETEST] A: n={rA['n']} net={rA['net']} pf={rA['pf']} | B: n={rB['n']} net={rB['net']} pf={rB['pf']} | {'BEATS BASE' if ok else 'no'}")

    # ---- C) vol gate -----------------------------------------------------
    out["vol_gate"] = {}
    for thr in (0.4, 0.5, 0.6):
        vA = m._walk(barsA, {s: signals_volgate(barsA[s], thr) for s in m.SYMBOLS}, "A")
        vB = m._walk(barsB, {s: signals_volgate(barsB[s], thr) for s in m.SYMBOLS}, "B")
        ok = vA["net"] > bA["net"] and vB["net"] > bB["net"]
        out["vol_gate"][thr] = {"A": dict(vA), "B": dict(vB), "beats_base_both": bool(ok)}
        print(f"[VOL≥{thr:.0%}] A: n={vA['n']} net={vA['net']} pf={vA['pf']} | B: n={vB['n']} net={vB['net']} pf={vB['pf']} | {'BEATS BASE' if ok else 'no'}")

    json.dump(out, open("/home/user/master-ai-bot/analysis/runs/v24_sprint4.json", "w"),
              indent=1, default=str)
    print("saved -> analysis/runs/v24_sprint4.json")


if __name__ == "__main__":
    main()
