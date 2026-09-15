#!/usr/bin/env python3
"""v24 sprint 5 — "SMC in combination" (owner directive 2026-09-15).

External consensus on how profitable SMC traders actually use it:
zones as CONFLUENCE (not standalone), HTF-trend agreement, liquidity
targets, session timing, stacked setups. Four fusion candidates, all
through the two-window live-equivalent harness:

  F1  model2022      — sweep -> displacement FVG -> retest entry, stop
                       beyond the sweep extreme, target = opposing swing
                       (the classical ICT 2022 model, never tested here).
  F2  star_trend_3R  — 5-star OB touches (the only gross-positive ICT
                       cell, window B) + EMA200 trend agreement + minimum
                       3R target (fixes the fee-vs-target-size problem).
  F3  kz_star        — 5-star OB touches gated to London/NY-AM killzones
                       (the pure session-timing claim).
  F4  donchian+SMC   — our validated Donchian breakouts filtered by SMC
                       confluence: entry only if an unmitigated same-
                       direction OB (score>=6) sits within 3 ATR.

Ship rule: solo positive on BOTH windows, or improves BOTH vs the
Donchian baseline (A +160.5 / B +127.0).
"""
import importlib.util as il
import json, sys

sys.path.insert(0, "/home/user/master-ai-bot")
_m = il.spec_from_file_location("m", "/home/user/master-ai-bot/analysis/scripts/ict_validation.py")
m = il.module_from_spec(_m); _m.loader.exec_module(m)
from app.strategy import indicators as ind  # noqa: E402
from app.models import Signal  # noqa: E402

L = 10  # swing length


def core_state(series):
    """Shared SMC state machine: swings, OBs (with EMA-based scoring), FVGs."""
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    closes = [b[4] for b in series]
    opens = [b[1] for b in series]
    ts = [b[0] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    e200 = ind.ema(closes, 200)
    n = len(closes)
    swings = []          # (idx, price, isHigh)
    lastH = lastL = None
    lastH_broken = lastL_broken = False
    obs = []             # dicts: idx, top, bottom, isBullish, score, mitigated
    fvgs = []            # dicts: idx, top, bottom, isBullish, mitigated
    bull_near = [False] * n   # unmitigated bull OB within 3 ATR below price
    bear_near = [False] * n
    for i in range(1, n):
        # pivots
        j = i - L
        if j >= L:
            seg = slice(j - L, j + L + 1)
            ph, pl = max(highs[seg]), min(lows[seg])
            if highs[j] == ph and sum(1 for x in highs[seg] if x == ph) == 1:
                swings.append((j, ph, True)); lastH, lastH_broken = ph, False
            if lows[j] == pl and sum(1 for x in lows[seg] if x == pl) == 1:
                swings.append((j, pl, False)); lastL, lastL_broken = pl, False
        # FVG (displacement >= 1 ATR already enforced)
        if i >= 2 and atr_l[i - 1]:
            mid = abs(closes[i - 1] - opens[i - 1])
            disp = mid >= atr_l[i - 1]
            if disp and closes[i - 1] > opens[i - 1] and lows[i] > highs[i - 2]:
                fvgs.append(dict(idx=i, top=lows[i], bottom=highs[i - 2],
                                 isBullish=True, mitigated=False))
            if disp and closes[i - 1] < opens[i - 1] and highs[i] < lows[i - 2]:
                fvgs.append(dict(idx=i, top=lows[i - 2], bottom=highs[i],
                                 isBullish=False, mitigated=False))
        for f in fvgs:
            if not f["mitigated"]:
                if f["isBullish"] and closes[i] < f["bottom"]:
                    f["mitigated"] = True
                elif not f["isBullish"] and closes[i] > f["top"]:
                    f["mitigated"] = True
        # order blocks on fresh pivots (EMA-based scoring)
        for (sidx, sp, isHigh) in swings[-2:]:
            if sidx != i - L:
                continue
            off = i - sidx
            if not (0 < off < 500):
                continue
            if not isHigh:
                obH, obL = highs[sidx], lows[sidx]
                if closes[sidx] < opens[sidx]:
                    swept = any((not h) and si < sidx and obL < sp2
                                for (si, sp2, h) in swings[-10:])
                    disp2 = off >= 3 and any(
                        lows[sidx + k + 2] > highs[sidx + k]
                        for k in range(0, min(off - 2, 5) + 1))
                    eq = (lastH + lastL) / 2 if (lastH and lastL) else None
                    disc = eq is not None and obL < eq
                    ema_al = e200[i] is not None and closes[i] > e200[i]
                    score = (2 * swept + 2 * disp2 + (1 if m.in_killzone(ts[i]) else 0)
                             + (1 if disc else 0) + (2 if ema_al else 0))
                    if score >= 3:
                        obs.append(dict(idx=sidx, top=obH, bottom=obL,
                                        isBullish=True, score=score,
                                        mitigated=False))
            else:
                obH, obL = highs[sidx], lows[sidx]
                if closes[sidx] > opens[sidx]:
                    swept = any(h and si < sidx and obH > sp2
                                for (si, sp2, h) in swings[-10:])
                    disp2 = off >= 3 and any(
                        highs[sidx + k + 2] < lows[sidx + k]
                        for k in range(0, min(off - 2, 5) + 1))
                    eq = (lastH + lastL) / 2 if (lastH and lastL) else None
                    prem = eq is not None and obH > eq
                    ema_al = e200[i] is not None and closes[i] < e200[i]
                    score = (2 * swept + 2 * disp2 + (1 if m.in_killzone(ts[i]) else 0)
                             + (1 if prem else 0) + (2 if ema_al else 0))
                    if score >= 3:
                        obs.append(dict(idx=sidx, top=obH, bottom=obL,
                                        isBullish=False, score=score,
                                        mitigated=False))
        # mitigation + confluence flags
        for ob in obs:
            if not ob["mitigated"]:
                if ob["isBullish"] and closes[i] < ob["bottom"]:
                    ob["mitigated"] = True
                elif not ob["isBullish"] and closes[i] > ob["top"]:
                    ob["mitigated"] = True
        atr = atr_l[i] or closes[i] * 0.01
        bull_near[i] = any(
            not ob["mitigated"] and ob["isBullish"] and ob["score"] >= 6
            and ob["top"] <= closes[i] <= ob["top"] + 3 * atr
            for ob in obs[-15:])
        bear_near[i] = any(
            not ob["mitigated"] and not ob["isBullish"] and ob["score"] >= 6
            and ob["bottom"] >= closes[i] >= ob["bottom"] - 3 * atr
            for ob in obs[-15:])
        if len(obs) > 40:
            obs[:15] = []
        if len(fvgs) > 40:
            fvgs[:15] = []
    return dict(highs=highs, lows=lows, closes=closes, opens=opens, ts=ts,
                atr_l=atr_l, e200=e200, swings=swings, obs=obs, fvgs=fvgs,
                bull_near=bull_near, bear_near=bear_near)


def nearest_swing_target(swings, isHigh, ref, better):
    """Nearest opposite-side swing beyond ref (liquidity target)."""
    best = None
    for (si, sp, h) in swings[-25:]:
        if h == isHigh:
            if (better(sp, ref)) and (best is None or better(best, sp) is False):
                best = sp
            if better(sp, ref) and (best is None or abs(sp - ref) < abs(best - ref)):
                best = sp
    return best


def f1_model2022(series, use_ema=True):
    st = core_state(series)
    closes, lows, highs, opens, ts = st["closes"], st["lows"], st["highs"], st["opens"], st["ts"]
    atr_l, e200, swings, fvgs = st["atr_l"], st["e200"], st["swings"], st["fvgs"]
    sigs = {}
    last_long = last_short = None
    act_long = act_short = None   # {sweep_i, sweep_px, fvg_top, fvg_bottom, armed}
    for i in range(205, len(closes) - 1):
        atr = atr_l[i] or closes[i] * 0.01
        # ---- sweep detection ----
        if act_long is None:
            for (si, sp, h) in swings[-10:]:
                if not h and si < i - 1 and lows[i] < sp and closes[i] > sp:
                    if not use_ema or (e200[i] and closes[i] > e200[i]):
                        act_long = dict(sweep_i=i, sweep_px=lows[i],
                                        fvg_top=None, fvg_bottom=None)
                    break
        else:
            # displacement FVG arming
            if act_long["fvg_top"] is None:
                for f in fvgs:
                    if (f["isBullish"] and not f["mitigated"]
                            and act_long["sweep_i"] < f["idx"] <= i
                            and f["bottom"] > act_long["sweep_px"]):
                        act_long["fvg_top"], act_long["fvg_bottom"] = f["top"], f["bottom"]
                        break
            else:
                # retest entry
                if lows[i] <= act_long["fvg_top"] and closes[i] > act_long["fvg_bottom"]:
                    if last_long is None or i - last_long >= 10:
                        sl = act_long["sweep_px"]
                        R = closes[i] - sl
                        if R > 0:
                            tp = None
                            for (si, sp, h) in reversed(swings[-25:]):
                                if h and sp > closes[i]:
                                    tp = sp if tp is None else min(tp, sp)
                            if tp is None or tp < closes[i] + 1.5 * R:
                                tp = closes[i] + 2.0 * R
                            sigs[i] = Signal(side="buy", strategy="Model2022",
                                             reason="sweep+FVG retest", entry=closes[i],
                                             sl=sl, tp1=None, tp=tp, rsi=50.0,
                                             atr=atr, htf="bullish")
                            last_long = i
                    act_long = None
                # invalidation / expiry
                if act_long and closes[i] < act_long["sweep_px"]:
                    act_long = None
                if act_long and i - act_long["sweep_i"] > 40:
                    act_long = None
        # ---- short side (symmetric) ----
        if act_short is None:
            for (si, sp, h) in swings[-10:]:
                if h and si < i - 1 and highs[i] > sp and closes[i] < sp:
                    if not use_ema or (e200[i] and closes[i] < e200[i]):
                        act_short = dict(sweep_i=i, sweep_px=highs[i],
                                         fvg_top=None, fvg_bottom=None)
                    break
        else:
            if act_short["fvg_top"] is None:
                for f in fvgs:
                    if (not f["isBullish"] and not f["mitigated"]
                            and act_short["sweep_i"] < f["idx"] <= i
                            and f["top"] < act_short["sweep_px"]):
                        act_short["fvg_top"], act_short["fvg_bottom"] = f["top"], f["bottom"]
                        break
            else:
                if highs[i] >= act_short["fvg_bottom"] and closes[i] < act_short["fvg_top"]:
                    if last_short is None or i - last_short >= 10:
                        sl = act_short["sweep_px"]
                        R = sl - closes[i]
                        if R > 0:
                            tp = None
                            for (si, sp, h) in reversed(swings[-25:]):
                                if not h and sp < closes[i]:
                                    tp = sp if tp is None else max(tp, sp)
                            if tp is None or tp > closes[i] - 1.5 * R:
                                tp = closes[i] - 2.0 * R
                            sigs[i] = Signal(side="sell", strategy="Model2022",
                                             reason="sweep+FVG retest", entry=closes[i],
                                             sl=sl, tp1=None, tp=tp, rsi=50.0,
                                             atr=atr, htf="bearish")
                            last_short = i
                    act_short = None
                if act_short and closes[i] > act_short["sweep_px"]:
                    act_short = None
                if act_short and i - act_short["sweep_i"] > 40:
                    act_short = None
    return sigs


def f2_star_trend_3r(series, min_star=7, min_rr=3.0):
    st = core_state(series)
    closes, lows, highs, ts = st["closes"], st["lows"], st["highs"], st["ts"]
    atr_l, e200, obs, swings = st["atr_l"], st["e200"], st["obs"], st["swings"]
    sigs = {}
    last_long = last_short = None
    for i in range(205, len(closes) - 1):
        atr = atr_l[i] or closes[i] * 0.01
        if last_long is None or i - last_long >= 10:
            for ob in reversed(obs[-12:]):
                if (not ob["mitigated"] and ob["isBullish"] and ob["score"] >= min_star
                        and lows[i] <= ob["top"] and closes[i] > ob["bottom"]
                        and e200[i] and closes[i] > e200[i]):
                    sl = ob["bottom"]
                    R = closes[i] - sl
                    if R > 0:
                        tp = None
                        for (si, sp, h) in reversed(swings[-25:]):
                            if h and sp > closes[i]:
                                tp = sp if tp is None else min(tp, sp)
                        if tp is None or tp < closes[i] + min_rr * R:
                            tp = closes[i] + min_rr * R
                        sigs[i] = Signal(side="buy", strategy="StarTrend3R",
                                         reason=f"{min_star}-star OB + trend", entry=closes[i],
                                         sl=sl, tp1=None, tp=tp, rsi=50.0,
                                         atr=atr, htf="bullish")
                        last_long = i
                    break
        if last_short is None or i - last_short >= 10:
            for ob in reversed(obs[-12:]):
                if (not ob["mitigated"] and not ob["isBullish"] and ob["score"] >= min_star
                        and highs[i] >= ob["bottom"] and closes[i] < ob["top"]
                        and e200[i] and closes[i] < e200[i]):
                    sl = ob["top"]
                    R = sl - closes[i]
                    if R > 0:
                        tp = None
                        for (si, sp, h) in reversed(swings[-25:]):
                            if not h and sp < closes[i]:
                                tp = sp if tp is None else max(tp, sp)
                        if tp is None or tp > closes[i] - min_rr * R:
                            tp = closes[i] - min_rr * R
                        sigs[i] = Signal(side="sell", strategy="StarTrend3R",
                                         reason=f"{min_star}-star OB + trend", entry=closes[i],
                                         sl=sl, tp1=None, tp=tp, rsi=50.0,
                                         atr=atr, htf="bearish")
                        last_short = i
                    break
    return sigs


def f3_kz_star(series):
    st = core_state(series)
    closes, lows, highs, ts = st["closes"], st["lows"], st["highs"], st["ts"]
    atr_l, obs, swings = st["atr_l"], st["obs"], st["swings"]
    sigs = {}
    last_long = last_short = None
    for i in range(205, len(closes) - 1):
        if not m.in_killzone(ts[i]):
            continue
        atr = atr_l[i] or closes[i] * 0.01
        if last_long is None or i - last_long >= 10:
            for ob in reversed(obs[-12:]):
                if (not ob["mitigated"] and ob["isBullish"] and ob["score"] >= 7
                        and lows[i] <= ob["top"] and closes[i] > ob["bottom"]):
                    tp = None
                    for (si, sp, h) in reversed(swings[-25:]):
                        if h and sp > closes[i]:
                            tp = sp if tp is None else min(tp, sp)
                    if tp and tp > closes[i]:
                        sigs[i] = Signal(side="buy", strategy="KZ_Star",
                                         reason="killzone 5-star OB", entry=closes[i],
                                         sl=ob["bottom"], tp1=None, tp=tp, rsi=50.0,
                                         atr=atr, htf="bullish")
                        last_long = i
                    break
        if last_short is None or i - last_short >= 10:
            for ob in reversed(obs[-12:]):
                if (not ob["mitigated"] and not ob["isBullish"] and ob["score"] >= 7
                        and highs[i] >= ob["bottom"] and closes[i] < ob["top"]):
                    tp = None
                    for (si, sp, h) in reversed(swings[-25:]):
                        if not h and sp < closes[i]:
                            tp = sp if tp is None else max(tp, sp)
                    if tp and tp < closes[i]:
                        sigs[i] = Signal(side="sell", strategy="KZ_Star",
                                         reason="killzone 5-star OB", entry=closes[i],
                                         sl=ob["top"], tp1=None, tp=tp, rsi=50.0,
                                         atr=atr, htf="bearish")
                        last_short = i
                    break
    return sigs


def f4_donchian_smc(series):
    """Donchian baseline filtered by SMC confluence (same-direction OB near)."""
    st = core_state(series)
    closes, highs, lows = st["closes"], st["highs"], st["lows"]
    atr_l, e200 = st["atr_l"], st["e200"]
    bull_near, bear_near = st["bull_near"], st["bear_near"]
    ENTRY, BREAK, SLM, TPM, LG = 40, 1.5, 2.5, 20.0, 1.0
    sigs = {}
    for i in range(205, len(closes) - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i]:
            continue
        ph = max(highs[i - ENTRY - 1:i])
        pl = min(lows[i - ENTRY - 1:i])
        px = closes[i]
        if (px > ph + BREAK * atr and px > e200[i]
                and (px - e200[i]) >= LG * atr and bull_near[i]):
            sigs[i] = Signal(side="buy", strategy="Donchian_SMC",
                             reason="breakout + bull OB confluence", entry=px,
                             sl=px - SLM * atr, tp1=None, tp=px + TPM * atr,
                             rsi=50.0, atr=atr, htf="bullish")
        elif (px < pl - BREAK * atr and px < e200[i] and bear_near[i]):
            sigs[i] = Signal(side="sell", strategy="Donchian_SMC",
                             reason="breakdown + bear OB confluence", entry=px,
                             sl=px + SLM * atr, tp1=None, tp=px - TPM * atr,
                             rsi=50.0, atr=atr, htf="bearish")
    return sigs


def main():
    barsA = {s: m.load(m.DATA_A, s) for s in m.SYMBOLS}
    barsB = {s: m.load(m.DATA_B, s) for s in m.SYMBOLS}
    out = {}

    # baseline (from sprint4 generator, inline here for independence)
    import v24_sprint4 as s4
    bA = m._walk(barsA, {s: s4.gen_baseline(barsA[s]) for s in m.SYMBOLS}, "A")
    bB = m._walk(barsB, {s: s4.gen_baseline(barsB[s]) for s in m.SYMBOLS}, "B")
    print(f"[BASE Donchian] A: n={bA['n']} net={bA['net']} pf={bA['pf']} | "
          f"B: n={bB['n']} net={bB['net']} pf={bB['pf']}")

    studies = {
        "F1 model2022 (sweep→FVG retest)": f1_model2022,
        "F2 5★+trend+3R": f2_star_trend_3r,
        "F3 killzone 5★": f3_kz_star,
        "F4 donchian×SMC-filter": f4_donchian_smc,
    }
    for name, fn in studies.items():
        sa = {s: fn(barsA[s]) for s in m.SYMBOLS}
        sb = {s: fn(barsB[s]) for s in m.SYMBOLS}
        ra = m._walk(barsA, sa, "A")
        rb = m._walk(barsB, sb, "B")
        solo_ok = ra["net"] > 0 and rb["net"] > 0
        beats = ra["net"] > bA["net"] and rb["net"] > bB["net"]
        out[name] = {"A": {k: ra[k] for k in ("n", "wr", "pf", "net", "max_dd")},
                     "B": {k: rb[k] for k in ("n", "wr", "pf", "net", "max_dd")},
                     "solo_both_positive": bool(solo_ok),
                     "beats_donchian_both": bool(beats)}
        print(f"[{name:<34}] A: n={ra['n']:>4} wr={ra['wr']:>5} pf={ra['pf']:>5} net={ra['net']:>8} | "
              f"B: n={rb['n']:>4} wr={rb['wr']:>5} pf={rb['pf']:>5} net={rb['net']:>8} | "
              f"solo:{'✓' if solo_ok else '✗'} beats-base:{'✓' if beats else '✗'}")

    json.dump(out, open("/home/user/master-ai-bot/analysis/runs/v24_sprint5.json", "w"),
              indent=1, default=str)
    print("saved -> analysis/runs/v24_sprint5.json")


if __name__ == "__main__":
    main()
