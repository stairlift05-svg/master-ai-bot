#!/usr/bin/env python3
"""v24 sprint 2 — strategy screening for addition to the bot.

Owner directive (2026-09-14): review OTHER strategies that could be added.

Candidates (all through the live-equivalent two-window harness):
  * the six legacy v2 families (failed the old v20.5 audit — re-tested here
    on BOTH current windows with the calibrated engine)
  * EmaCross_Trend (v23.3, registered/disabled)
  * Imba_Fib tp4=6 (reference row — live-rejected 2026-09-14)
  * NEW archetypes: SuperTrend flip (3 x ATR-10) and Keltner-EMA breakout

Protocol: solo on window A (14mo Binance) and window B (16mo unseen OKX);
candidates positive on BOTH windows proceed to the combined test with
Donchian_Trend (incumbent priority). Context builder mirrors the engine's
_tf_context (trend from EMA50/200, Bollinger 20/2, prior-bar hh/ll).

Limitation (documented): tf15/tf1 slots use the same 1h series (the live
engine feeds 4h); uniform across all candidates incl. the reference rows.
"""
import importlib.util as il
import json, os, sys

sys.path.insert(0, "/home/user/master-ai-bot")
_sp = il.spec_from_file_location(
    "base", "/home/user/master-ai-bot/analysis/scripts/ict_validation.py")
m = il.module_from_spec(_sp); _sp.loader.exec_module(m)

from app.strategy import indicators as ind  # noqa: E402
from app.strategy.signals import build_strategy  # noqa: E402
from app.models import Signal  # noqa: E402

W = 300


class RichCtx:
    """Engine-faithful TFContext for one bar (all tf slots share it)."""
    def __init__(self, closes, highs, lows, vols, atr, rsi, ema20, ema50,
                 ema200, mid, upper, lower, hh, ll):
        self.label = "1h"
        self.closes, self.highs, self.lows = closes, highs, lows
        self.volumes = vols
        self.atr, self.rsi = atr, rsi
        self.ema20, self.ema50, self.ema200 = ema20, ema50, ema200
        self.hh, self.ll = hh, ll
        self.mid, self.bb_upper, self.bb_lower = mid, upper, lower
        # engine trend logic
        if ema200 is not None and ema200 > 0:
            self.strength = abs(ema50 - ema200) / abs(ema200) * 100.0
            if closes[-1] > ema50 * 0.995 and ema50 >= ema200 * 0.995:
                self.trend = "bullish"
            elif closes[-1] < ema50 * 1.005 and ema50 <= ema200 * 1.005:
                self.trend = "bearish"
            else:
                self.trend = "sideways"
        else:
            self.strength = abs(ema50 - closes[-1]) / (abs(closes[-1]) + 1e-9) * 100
            self.trend = ("bullish" if closes[-1] > ema50 * 1.002 else
                          "bearish" if closes[-1] < ema50 * 0.998 else "sideways")

    @property
    def bb_width(self):
        return ((self.bb_upper - self.bb_lower) / (self.mid + 1e-12)
                if self.mid and self.mid > 0 else 0.0)


class Htf:
    def __init__(self, price, tf):
        self.symbol, self.price = "X", price
        self.tf5 = self.tf15 = self.tf1 = tf
        self.candle_bull_5m = True
        self.candle_bear_5m = True
        self.min_stop_pct = m.MIN_STOP_PCT
        self.round_trip_cost_pct = 2 * (m.TAKER * m.FEE_BUF) + 2 * m.SLIP
        self.min_edge_ratio = m.MIN_EDGE


def gen_signals(name, series, params=None):
    """Signals from a registered repo strategy via build_strategy()."""
    strat = build_strategy(name, params)
    closes = [b[4] for b in series]
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    vols = [b[5] if len(b) > 5 else 0.0 for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    rsi_l = ind.rsi_wilder(closes, 14)
    e20, e50, e200 = ind.ema(closes, 20), ind.ema(closes, 50), ind.ema(closes, 200)
    mid_l, up_l, lo_l = ind.bollinger(closes, 20, 2.0)
    sigs = {}
    for i in range(205, len(series) - 1):
        lo = max(0, i - W + 1)
        ctx = Htf(closes[i], RichCtx(
            closes[lo:i + 1], highs[lo:i + 1], lows[lo:i + 1], vols[lo:i + 1],
            atr_l[i] or 0.0,
            rsi_l[i] if rsi_l[i] is not None else 50.0,
            e20[i] or closes[i], e50[i] or closes[i], e200[i],
            mid_l[i] or closes[i], up_l[i] or closes[i], lo_l[i] or closes[i],
            max(highs[lo:i]), min(lows[lo:i])))
        s = strat.propose(ctx)
        if s is not None:
            sigs[i] = s
    return sigs


def supertrend_signals(series, mult=3.0, period=10, use_ema_filter=True):
    closes = [b[4] for b in series]
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, period)
    e200 = ind.ema(closes, 200)
    n = len(closes)
    fub = [None] * n
    flb = [None] * n
    direction = [1] * n
    sigs = {}
    for i in range(1, n):
        hl2 = (highs[i] + lows[i]) / 2
        atr = atr_l[i] or 0.0
        bub = hl2 + mult * atr
        blb = hl2 - mult * atr
        pfub = fub[i - 1] if fub[i - 1] is not None else bub
        flb_ = flb[i - 1] if flb[i - 1] is not None else blb
        fub[i] = bub if (bub < pfub or closes[i - 1] > pfub) else pfub
        flb[i] = blb if (blb > flb_ or closes[i - 1] < flb_) else flb_
        prev = direction[i - 1]
        d = 1 if closes[i] > flb[i] else (-1 if closes[i] < fub[i] else prev)
        direction[i] = d
        if i < 205 or d == prev:
            continue
        atr14 = atr_l[i] or closes[i] * 0.01
        ema_ok = True
        if use_ema_filter and e200[i]:
            ema_ok = (closes[i] > e200[i]) if d == 1 else (closes[i] < e200[i])
        if not ema_ok:
            continue
        if d == 1:
            sl, tp = flb[i], closes[i] + 20 * atr14
            sigs[i] = Signal(side="buy", strategy="SuperTrend", entry=closes[i], reason="SuperTrend flip up",
                             sl=sl, tp1=None, tp=tp, rsi=50.0, atr=atr14,
                             htf="bullish")
        else:
            sl, tp = fub[i], closes[i] - 20 * atr14
            sigs[i] = Signal(side="sell", strategy="SuperTrend", entry=closes[i], reason="SuperTrend flip down",
                             sl=sl, tp1=None, tp=tp, rsi=50.0, atr=atr14,
                             htf="bearish")
    return sigs


def keltner_signals(series, mult=2.0, use_ema_filter=True):
    closes = [b[4] for b in series]
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    e20 = ind.ema(closes, 20)
    e200 = ind.ema(closes, 200)
    sigs = {}
    for i in range(205, len(series) - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e20[i]:
            continue
        upper = e20[i] + mult * atr
        lower = e20[i] - mult * atr
        ema_ok = True
        if use_ema_filter and e200[i]:
            ema_ok = (closes[i] > e200[i]) if closes[i] > upper else (closes[i] < e200[i])
        if not ema_ok:
            continue
        if closes[i] > upper:
            sigs[i] = Signal(side="buy", strategy="Keltner", entry=closes[i], reason="Keltner upper break",
                             sl=closes[i] - 2.5 * atr, tp1=None,
                             tp=closes[i] + 20 * atr, rsi=50.0, atr=atr,
                             htf="bullish")
        elif closes[i] < lower:
            sigs[i] = Signal(side="sell", strategy="Keltner", entry=closes[i], reason="Keltner lower break",
                             sl=closes[i] + 2.5 * atr, tp1=None,
                             tp=closes[i] - 20 * atr, rsi=50.0, atr=atr,
                             htf="bearish")
    return sigs


def donchian_signals(series):
    return gen_signals("Donchian_Trend", series)


def combo(bars, incumbent, candidate):
    """Incumbent (Donchian) priority per bar."""
    out = {}
    for sym in bars:
        a, b = incumbent.get(sym, {}), candidate.get(sym, {})
        d = {}
        for i in sorted(set(list(a.keys()) + list(b.keys()))):
            d[i] = a.get(i) or b.get(i)
        out[sym] = d
    return out


def main():
    barsA = {s: m.load(m.DATA_A, s) for s in m.SYMBOLS}
    barsB = {s: m.load(m.DATA_B, s) for s in m.SYMBOLS}
    refA = {s: donchian_signals(barsA[s]) for s in m.SYMBOLS}
    refB = {s: donchian_signals(barsB[s]) for s in m.SYMBOLS}
    out = {"reference": {}}

    rA = m._walk(barsA, refA, "A")
    rB = m._walk(barsB, refB, "B")
    out["reference"]["Donchian_Trend (incumbent)"] = {
        "A": {k: rA[k] for k in ("n", "wr", "pf", "net", "max_dd")},
        "B": {k: rB[k] for k in ("n", "wr", "pf", "net", "max_dd")}}
    print(f"[REF] Donchian: A n={rA['n']} net={rA['net']} pf={rA['pf']} | "
          f"B n={rB['n']} net={rB['net']} pf={rB['pf']}")

    candidates = {
        "TrendPullback_HTF": lambda ser: gen_signals("TrendPullback_HTF", ser),
        "HTF_Breakout": lambda ser: gen_signals("HTF_Breakout", ser),
        "MomentumRetrace_RSI": lambda ser: gen_signals("MomentumRetrace_RSI", ser),
        "MeanReversion_BB": lambda ser: gen_signals("MeanReversion_BB", ser),
        "VolatilityExpansion": lambda ser: gen_signals("VolatilityExpansion", ser),
        "SwingPullback_1h": lambda ser: gen_signals("SwingPullback_1h", ser),
        "EmaCross_Trend": lambda ser: gen_signals("EmaCross_Trend", ser),
        "Imba_Fib tp4=6 (ref)": lambda ser: gen_signals(
            "Imba_Fib", ser, {"tp4": 6.0}),
        "SuperTrend 3xATR10": lambda ser: supertrend_signals(ser),
        "Keltner 2xATR": lambda ser: keltner_signals(ser),
    }
    out["solo"] = {}
    passed = []
    for name, fn in candidates.items():
        sa = {s: fn(barsA[s]) for s in m.SYMBOLS}
        sb = {s: fn(barsB[s]) for s in m.SYMBOLS}
        ra = m._walk(barsA, sa, "A")
        rb = m._walk(barsB, sb, "B")
        ok = ra["net"] > 0 and rb["net"] > 0
        out["solo"][name] = {
            "A": {k: ra[k] for k in ("n", "wr", "pf", "net", "max_dd")},
            "B": {k: rb[k] for k in ("n", "wr", "pf", "net", "max_dd")},
            "solo_both_windows": ok}
        if ok and name != "Imba_Fib tp4=6 (ref)":
            passed.append((name, fn))
        print(f"[SOLO] {name:<24} A: n={ra['n']:>4} pf={ra['pf']:>5} net={ra['net']:>8} | "
              f"B: n={rb['n']:>4} pf={rb['pf']:>5} net={rb['net']:>8} | "
              f"{'PASS' if ok else 'fail'}")

    print("\n[COMBO with Donchian — incumbent priority]")
    out["combo_with_donchian"] = {}
    for name, fn in passed:
        ca = combo(barsA, refA, {s: fn(barsA[s]) for s in m.SYMBOLS})
        cb = combo(barsB, refB, {s: fn(barsB[s]) for s in m.SYMBOLS})
        rca = m._walk(barsA, ca, "A")
        rcb = m._walk(barsB, cb, "B")
        n_add_a = sum(1 for t in rca["trades"] if t["strat"] != "Donchian_Trend")
        n_add_b = sum(1 for t in rcb["trades"] if t["strat"] != "Donchian_Trend")
        pnl_add_a = round(sum(t["net"] for t in rca["trades"]
                              if t["strat"] != "Donchian_Trend"), 2)
        pnl_add_b = round(sum(t["net"] for t in rcb["trades"]
                              if t["strat"] != "Donchian_Trend"), 2)
        improves = rca["net"] > rA["net"] and rcb["net"] > rB["net"]
        out["combo_with_donchian"][name] = {
            "A": {k: rca[k] for k in ("n", "wr", "pf", "net", "max_dd")},
            "B": {k: rcb[k] for k in ("n", "wr", "pf", "net", "max_dd")},
            "added_leg": {"A": {"n": n_add_a, "pnl": pnl_add_a},
                          "B": {"n": n_add_b, "pnl": pnl_add_b}},
            "improves_both": improves}
        print(f"[COMBO] {name:<24} A: net={rca['net']:>8} (leg {pnl_add_a:+}) | "
              f"B: net={rcb['net']:>8} (leg {pnl_add_b:+}) | "
              f"{'IMPROVES BOTH' if improves else 'no'}")

    p = "/home/user/master-ai-bot/analysis/runs/v24_sprint2.json"
    json.dump(out, open(p, "w"), indent=1)
    print("\nsaved ->", p)


if __name__ == "__main__":
    main()
