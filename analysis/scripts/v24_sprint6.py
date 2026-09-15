#!/usr/bin/env python3
"""v24 sprint 6 — the masters invent/discover (owner directive 2026-09-15).

Six NEW styles, none ever tested in this repo, each through the two-window
live-equivalent harness (fees, slippage, risk sizing, engine exits):

  M1 Turtle (Dennis/Eckhardt)  — dual Donchian 20/55 breakout, opposite
     10-bar channel as the target, EMA200 regime agreement.
  M2 Connors RSI-2 pullback    — classic counter-trend: RSI(2) extreme
     against an EMA200 regime, fast target (approximated 2xATR).
  M3 TTM squeeze breakout      — Bollinger width in the lowest quintile
     (squeeze), then break with the regime.
  M4 Session levels (Dow/ICT)  — prior UTC-day high/low breakout + regime.
  M5 Chandelier exit (LeBeau)  — 50-bar breakout entry, NO fixed target:
     wide 3xATR chandelier stop, engine trail does the exiting. Isolates
     the "exit style" variable the council keeps debating.
  M6 RSI divergence (Dow)      — pivot RSI divergence with confirmation.

Ship rule unchanged: positive on BOTH windows solo, or improves BOTH vs
the Donchian baseline (A +160.47 / B +126.97).
"""
import importlib.util as il
import json, sys

sys.path.insert(0, "/home/user/master-ai-bot")
sys.path.insert(0, "/home/user/master-ai-bot/analysis/scripts")
_m = il.spec_from_file_location("m", "/home/user/master-ai-bot/analysis/scripts/ict_validation.py")
m = il.module_from_spec(_m); _m.loader.exec_module(m)
from app.strategy import indicators as ind  # noqa: E402
from app.models import Signal  # noqa: E402


def base_arrays(series):
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    closes = [b[4] for b in series]
    return highs, lows, closes, ind.atr_wilder(highs, lows, closes, 14), ind.ema(closes, 200)


def mk(side, strat, reason, entry, sl, tp, atr):
    return Signal(side=side, strategy=strat, reason=reason, entry=entry,
                  sl=sl, tp1=None, tp=tp, rsi=50.0, atr=atr,
                  htf="bullish" if side == "buy" else "bearish")


def m1_turtle(series, ch_len=20, exit_len=10, use_ema=True):
    highs, lows, closes, atr_l, e200 = base_arrays(series)
    sigs = {}
    for i in range(205, len(closes) - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i]:
            continue
        ph = max(highs[i - ch_len - 1:i])
        pl = min(lows[i - ch_len - 1:i])
        # opposite 10-bar channel = target
        tgt_up = max(highs[i - exit_len - 1:i])
        tgt_dn = min(lows[i - exit_len - 1:i])
        px = closes[i]
        if px > ph and (not use_ema or px > e200[i]):
            tp = max(tgt_up, px + 3 * atr)
            sigs[i] = mk("buy", "Turtle", "20-bar breakout", px,
                         px - 2 * atr, tp, atr)
        elif px < pl and (not use_ema or px < e200[i]):
            tp = min(tgt_dn, px - 3 * atr)
            sigs[i] = mk("sell", "Turtle", "20-bar breakdown", px,
                         px + 2 * atr, tp, atr)
    return sigs


def m2_connors(series, rsi_lo=10, rsi_hi=90):
    highs, lows, closes, atr_l, e200 = base_arrays(series)
    rsi2 = ind.rsi_wilder(closes, 2)
    sigs = {}
    for i in range(205, len(closes) - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i] or rsi2[i] is None:
            continue
        px = closes[i]
        if px > e200[i] and rsi2[i] < rsi_lo:
            sigs[i] = mk("buy", "ConnorsRSI2", "RSI2 dip in uptrend", px,
                         px - 3 * atr, px + 2 * atr, atr)
        elif px < e200[i] and rsi2[i] > rsi_hi:
            sigs[i] = mk("sell", "ConnorsRSI2", "RSI2 spike in downtrend", px,
                         px + 3 * atr, px - 2 * atr, atr)
    return sigs


def m3_squeeze(series, pct=0.2):
    highs, lows, closes, atr_l, e200 = base_arrays(series)
    mid_l, up_l, lo_l = ind.bollinger(closes, 20, 2.0)
    sigs = {}
    for i in range(205, len(closes) - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i] or not mid_l[i] or not up_l[i]:
            continue
        wnow = (up_l[i] - lo_l[i]) / (mid_l[i] + 1e-12)
        window = [(up_l[j] - lo_l[j]) / (mid_l[j] + 1e-12)
                  for j in range(max(0, i - 100), i + 1)
                  if up_l[j] and mid_l[j]]
        if not window or wnow > sorted(window)[int(pct * len(window))]:
            continue   # no squeeze
        px = closes[i]
        if px > up_l[i] and px > e200[i]:
            sigs[i] = mk("buy", "Squeeze", "BB squeeze release up", px,
                         px - 2.5 * atr, px + 20 * atr, atr)
        elif px < lo_l[i] and px < e200[i]:
            sigs[i] = mk("sell", "Squeeze", "BB squeeze release down", px,
                         px + 2.5 * atr, px - 20 * atr, atr)
    return sigs


def m4_daybreak(series, margin_atr=0.25):
    highs, lows, closes, atr_l, e200 = base_arrays(series)
    ts = [b[0] for b in series]
    sigs = {}
    prev_h = prev_l = None
    day = None
    cur_h = cur_l = None
    for i in range(1, len(closes)):
        d = ts[i] // 86400000
        if d != day:
            prev_h, prev_l = cur_h, cur_l
            day, cur_h, cur_l = d, highs[i], lows[i]
        else:
            cur_h = max(cur_h, highs[i])
            cur_l = min(cur_l, lows[i])
        if i < 205 or prev_h is None:
            continue
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i]:
            continue
        px = closes[i]
        if px > prev_h + margin_atr * atr and px > e200[i]:
            sigs[i] = mk("buy", "DayBreak", "prior-day high break", px,
                         px - 2.5 * atr, px + 20 * atr, atr)
        elif px < prev_l - margin_atr * atr and px < e200[i]:
            sigs[i] = mk("sell", "DayBreak", "prior-day low break", px,
                         px + 2.5 * atr, px - 20 * atr, atr)
    return sigs


def m5_chandelier(series, entry_len=50, atr_mult=3.0):
    highs, lows, closes, atr_l, e200 = base_arrays(series)
    sigs = {}
    for i in range(205, len(closes) - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i]:
            continue
        ph = max(highs[i - entry_len - 1:i])
        pl = min(lows[i - entry_len - 1:i])
        px = closes[i]
        if px > ph and px > e200[i]:
            sigs[i] = mk("buy", "Chandelier", "50-bar break, trail-only", px,
                         px - atr_mult * atr, px + 30 * atr, atr)
        elif px < pl and px < e200[i]:
            sigs[i] = mk("sell", "Chandelier", "50-bar break, trail-only", px,
                         px + atr_mult * atr, px - 30 * atr, atr)
    return sigs


def m6_divergence(series, plen=5):
    highs, lows, closes, atr_l, e200 = base_arrays(series)
    rsi14 = ind.rsi_wilder(closes, 14)
    n = len(closes)
    sigs = {}

    def pivots(arr, is_high):
        out = []
        for j in range(plen, n - plen):
            seg = arr[j - plen:j + plen + 1]
            if is_high and arr[j] == max(seg):
                out.append(j)
            if not is_high and arr[j] == min(seg):
                out.append(j)
        return out

    ph_idx = pivots(highs, True)
    pl_idx = pivots(lows, False)
    for i in range(205, n - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not rsi14[i]:
            continue
        px = closes[i]
        # bearish: last two price HH with RSI LH
        if len(ph_idx) >= 2 and ph_idx[-1] < i:
            a, b = ph_idx[-2], ph_idx[-1]
            if b - a > plen and highs[b] > highs[a] and \
                    rsi14[b] is not None and rsi14[a] is not None and \
                    rsi14[b] < rsi14[a] and px < lows[b]:
                if e200[i] and px < e200[i]:
                    tgt = min(lows[max(0, b - 15):b + 1]) if b > 15 else px - 5 * atr
                    sigs[i] = mk("sell", "RsiDiv", "bearish divergence", px,
                                 highs[b] + 0.5 * atr,
                                 min(tgt, px - 2 * atr), atr)
        # bullish: last two price LL with RSI HL
        if len(pl_idx) >= 2 and pl_idx[-1] < i:
            a, b = pl_idx[-2], pl_idx[-1]
            if b - a > plen and lows[b] < lows[a] and \
                    rsi14[b] is not None and rsi14[a] is not None and \
                    rsi14[b] > rsi14[a] and px > highs[b]:
                if e200[i] and px > e200[i]:
                    tgt = max(highs[max(0, b - 15):b + 1]) if b > 15 else px + 5 * atr
                    sigs[i] = mk("buy", "RsiDiv", "bullish divergence", px,
                                 lows[b] - 0.5 * atr,
                                 max(tgt, px + 2 * atr), atr)
    return sigs


def main():
    import v24_sprint4 as s4
    barsA = {s: m.load(m.DATA_A, s) for s in m.SYMBOLS}
    barsB = {s: m.load(m.DATA_B, s) for s in m.SYMBOLS}
    bA = m._walk(barsA, {s: s4.gen_baseline(barsA[s]) for s in m.SYMBOLS}, "A")
    bB = m._walk(barsB, {s: s4.gen_baseline(barsB[s]) for s in m.SYMBOLS}, "B")
    print(f"[BASE Donchian] A net={bA['net']} pf={bA['pf']} | B net={bB['net']} pf={bB['pf']}")

    studies = {
        "M1 Turtle 20/10": m1_turtle,
        "M2 Connors RSI-2": m2_connors,
        "M3 TTM squeeze": m3_squeeze,
        "M4 prior-day break": m4_daybreak,
        "M5 chandelier trail-only": m5_chandelier,
        "M6 RSI divergence": m6_divergence,
    }
    out = {}
    for name, fn in studies.items():
        ra = m._walk(barsA, {s: fn(barsA[s]) for s in m.SYMBOLS}, "A")
        rb = m._walk(barsB, {s: fn(barsB[s]) for s in m.SYMBOLS}, "B")
        solo = ra["net"] > 0 and rb["net"] > 0
        beats = ra["net"] > bA["net"] and rb["net"] > bB["net"]
        out[name] = {"A": {k: ra[k] for k in ("n", "wr", "pf", "net", "max_dd")},
                     "B": {k: rb[k] for k in ("n", "wr", "pf", "net", "max_dd")},
                     "solo_both_positive": bool(solo),
                     "beats_donchian_both": bool(beats)}
        print(f"[{name:<24}] A: n={ra['n']:>4} wr={ra['wr']:>5} pf={ra['pf']:>5} net={ra['net']:>8} | "
              f"B: n={rb['n']:>4} wr={rb['wr']:>5} pf={rb['pf']:>5} net={rb['net']:>8} | "
              f"solo:{'✓' if solo else '✗'} beats:{'✓' if beats else '✗'}")
        if solo:
            ca, cb = {}, {}
            for s in m.SYMBOLS:
                da, sa2 = s4.gen_baseline(barsA[s]), fn(barsA[s])
                db, sb2 = s4.gen_baseline(barsB[s]), fn(barsB[s])
                ca[s] = {i: (da.get(i) or sa2.get(i)) for i in set(list(da) + list(sa2))}
                cb[s] = {i: (db.get(i) or sb2.get(i)) for i in set(list(db) + list(sb2))}
            rca = m._walk(barsA, ca, "A")
            rcb = m._walk(barsB, cb, "B")
            imp = rca["net"] > bA["net"] and rcb["net"] > bB["net"]
            out[name]["combo"] = {"A": round(rca["net"], 2), "B": round(rcb["net"], 2),
                                  "improves_both": bool(imp)}
            print(f"    COMBO: A net={rca['net']} | B net={rcb['net']} | improves-both:{'✓' if imp else '✗'}")

    json.dump(out, open("/home/user/master-ai-bot/analysis/runs/v24_sprint6.json", "w"),
              indent=1, default=str)
    print("saved -> analysis/runs/v24_sprint6.json")


if __name__ == "__main__":
    main()
