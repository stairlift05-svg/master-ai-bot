#!/usr/bin/env python3
"""v24 sprint 8 — the reopening path: NEW DATA (volume + funding).

Sprint 7 proved every existing strategy is a slow mirror of the Donchian
edge (free trades negative). The documented reopening condition: a
genuinely different edge needs different DATA. This sprint adds two new
data dimensions that were already on disk or freely available:

  * VOLUME   — the v column of both window CSVs (Binance A / OKX B),
               present since the original fetches, never used by any
               strategy in repo history.
  * FUNDING  — Binance USDT-M perpetual funding rates, 8h events,
               2024-03-01 -> 2026-08-31 (2742/symbol), fetched from the
               public data.binance.vision archive. Proxy for window B's
               OKX prices (funding is arb-aligned across venues; OKX's
               own history only reaches back ~2 months — documented).

Battery (all through the live-equivalent harness, cap=4):
  Gates on the incumbent Donchian_Trend (ship: beats baseline on BOTH
  windows without collapsing PF/DD):
    V1..V4  volume-confirmation gates at the breakout bar
    FR1..3  funding-crowding gates (rolling 90-day percentiles, no lookahead)
  Standalone new-edge candidates (ship: solo-positive BOTH windows AND
    free trades vs Donchian positive BOTH windows — the sprint-7 rule):
    SV1     volume-shock momentum (regime-aligned)
    SF1     trapped-traders: extreme funding crowding + channel break
"""
import bisect
import csv
import importlib.util as il
import json
import sys
from collections import deque

sys.path.insert(0, "/home/user/master-ai-bot")
sys.path.insert(0, "/home/user/master-ai-bot/analysis/scripts")
_m = il.spec_from_file_location("m", "/home/user/master-ai-bot/analysis/scripts/ict_validation.py")
m = il.module_from_spec(_m); _m.loader.exec_module(m)
import v24_sprint4 as s4   # noqa: E402  (BASE params)
import v24_sprint7 as s7   # noqa: E402  (walk_indep for free-trade test)
from app.strategy import indicators as ind   # noqa: E402
from app.models import Signal   # noqa: E402

ROOT = "/home/user/master-ai-bot"
FUND_DIR = f"{ROOT}/analysis/data_funding"
FUND_SYM = {"ETHUSD": "ETHUSDT", "BTCUSD": "BTCUSDT", "SOLUSD": "SOLUSDT",
            "BNBUSD": "BNBUSDT", "DOGEUSD": "DOGEUSDT"}
OUT = f"{ROOT}/analysis/runs/v24_sprint8.json"
CAP = 4
BASE = s4.BASE


# ------------------------------------------------------------------ loaders
def load_vol(d, sym):
    rows = []
    with open(f"{d}/{sym}.csv", newline="") as fh:
        for r in csv.DictReader(fh):
            rows.append((int(r["ts"]), float(r["o"]), float(r["h"]),
                         float(r["l"]), float(r["c"]), float(r["v"])))
    rows.sort()
    return rows


def load_funding(sym):
    ev = []
    with open(f"{FUND_DIR}/{FUND_SYM[sym]}.csv") as fh:
        for line in fh.read().strip().split("\n"):
            p = line.split(",")
            ev.append((int(p[0]), float(p[2])))
    ev.sort()
    return ev


def funding_pct_series(events, roll=270):
    """Per-event rolling percentile of the rate within the prior `roll`
    events (no lookahead). Returns [(event_ts, rate, pct)]."""
    out, window_sorted, order = [], [], deque()
    for ts, rate in events:
        if len(window_sorted) >= roll:
            pct = bisect.bisect_left(window_sorted, rate) / len(window_sorted) * 100.0
        else:
            pct = 50.0
        out.append((ts, rate, pct))
        bisect.insort(window_sorted, rate)
        order.append(rate)
        if len(order) > roll:
            old = order.popleft()
            window_sorted.pop(bisect.bisect_left(window_sorted, old))
    return out


def align_funding(events_pct, series):
    """Map event percentiles onto bars: each bar gets the latest event <= ts."""
    ts_ev = [e[0] for e in events_pct]
    rate_l, pct_l = [None] * len(series), [None] * len(series)
    cur_r, cur_p = None, None
    for i, b in enumerate(series):
        j = bisect.bisect_right(ts_ev, b[0]) - 1
        if j >= 0:
            cur_r, cur_p = events_pct[j][1], events_pct[j][2]
        rate_l[i], pct_l[i] = cur_r, cur_p
    return rate_l, pct_l


def vol_stats(vols):
    """Rolling median100, p75(100), mean20 per bar."""
    n = len(vols)
    med = [None] * n
    p75 = [None] * n
    mean20 = [None] * n
    win = []
    d20 = deque()
    s20 = 0.0
    for i, v in enumerate(vols):
        bisect.insort(win, v)
        if len(win) > 100:
            win.pop(bisect.bisect_left(win, vols[i - 100]))
        if len(win) == 100:
            med[i] = win[50]
            p75[i] = win[75]
        d20.append(v)
        s20 += v
        if len(d20) > 20:
            s20 -= d20.popleft()
        if len(d20) == 20:
            mean20[i] = s20 / 20.0
    return med, p75, mean20


# ------------------------------------------------------- gated Donchian gen
def gen_donchian(series, rate_l, pct_l, vol_gate=None, fund_gate=None):
    """Shipped Donchian logic + optional gates. vol_gate(vols, i, stats)
    -> bool; fund_gate(pct, rate, side) -> bool."""
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    closes = [b[4] for b in series]
    vols = [b[5] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    e200 = ind.ema(closes, 200)
    med, p75, mean20 = vol_stats(vols)
    L = int(BASE["entry_len"])
    stats = {"med": med, "p75": p75, "mean20": mean20}
    sigs = {}
    for i in range(205, len(series)):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i]:
            continue
        ph = max(highs[i - L - 1:i])
        pl = min(lows[i - L - 1:i])
        price = closes[i]
        side = None
        if price > ph + BASE["break_atr"] * atr and price > e200[i] \
                and (price - e200[i]) >= BASE["long_dist_atr"] * atr:
            side = "buy"
        elif price < pl - BASE["break_atr"] * atr and price < e200[i]:
            side = "sell"
        if side is None:
            continue
        if vol_gate is not None and not vol_gate(vols, i, stats):
            continue
        if fund_gate is not None and not fund_gate(pct_l[i], rate_l[i], side):
            continue
        if side == "buy":
            sigs[i] = Signal(side="buy", strategy="Donchian_Trend",
                             reason="40-bar channel breakout", entry=price,
                             sl=price - BASE["sl_m"] * atr, tp1=None,
                             tp=price + BASE["tp_m"] * atr, rsi=50.0,
                             atr=atr, htf="bullish")
        else:
            sigs[i] = Signal(side="sell", strategy="Donchian_Trend",
                             reason="40-bar channel breakdown", entry=price,
                             sl=price + BASE["sl_m"] * atr, tp1=None,
                             tp=price - BASE["tp_m"] * atr, rsi=50.0,
                             atr=atr, htf="bearish")
    return sigs


# ------------------------------------------------------ standalone signals
def gen_volshock(series):
    """SV1 — volume-shock momentum: 3x avg volume bar with a >=1 ATR move,
    regime-aligned (EMA200). Different information source (volume)."""
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    closes = [b[4] for b in series]
    vols = [b[5] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    e200 = ind.ema(closes, 200)
    _, _, mean20 = vol_stats(vols)
    sigs = {}
    for i in range(205, len(series) - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i] or not mean20[i]:
            continue
        if vols[i] < 3.0 * mean20[i]:
            continue
        px, prev = closes[i], closes[i - 1]
        if px > e200[i] and (px - prev) >= 1.0 * atr:
            sigs[i] = Signal(side="buy", strategy="VolShock",
                             reason="3x volume thrust up", entry=px,
                             sl=px - 2.5 * atr, tp1=None, tp=px + 10 * atr,
                             rsi=50.0, atr=atr, htf="bullish")
        elif px < e200[i] and (prev - px) >= 1.0 * atr:
            sigs[i] = Signal(side="sell", strategy="VolShock",
                             reason="3x volume thrust down", entry=px,
                             sl=px + 2.5 * atr, tp1=None, tp=px - 10 * atr,
                             rsi=50.0, atr=atr, htf="bearish")
    return sigs


def gen_fundtrap(series, rate_l, pct_l, hi=97.0, lo=3.0, ch_len=20):
    """SF1 — trapped traders: funding crowding extreme (rolling pct) plus a
    break of the 20-bar channel AGAINST the crowded side. Longs crowded
    (funding pct >= hi) + price breaks 20-bar low -> trapped longs -> short.
    Mirror for squeezed shorts."""
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    closes = [b[4] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    sigs = {}
    for i in range(205, len(series) - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or pct_l[i] is None:
            continue
        px = closes[i]
        pl20 = min(lows[i - ch_len - 1:i])
        ph20 = max(highs[i - ch_len - 1:i])
        if pct_l[i] >= hi and px < pl20:
            sigs[i] = Signal(side="sell", strategy="FundTrap",
                             reason=f"crowded longs (funding pct {pct_l[i]:.0f}) + 20-bar breakdown",
                             entry=px, sl=px + 2.5 * atr, tp1=None,
                             tp=px - 8 * atr, rsi=50.0, atr=atr, htf="bearish")
        elif pct_l[i] <= lo and px > ph20:
            sigs[i] = Signal(side="buy", strategy="FundTrap",
                             reason=f"squeezed shorts (funding pct {pct_l[i]:.0f}) + 20-bar breakout",
                             entry=px, sl=px - 2.5 * atr, tp1=None,
                             tp=px + 8 * atr, rsi=50.0, atr=atr, htf="bullish")
    return sigs


# ------------------------------------------------------------- decomposition
def decompose_vs_donchian(bars, dsig, xsig, xname):
    """Classify x's trades (arch-A run with D) into same-bar / d-held / free."""
    rD = s7.walk_indep(bars, {"D": dsig}, "w")
    rX = s7.walk_indep(bars, {"D": dsig, "X": xsig}, "w")
    d_sig_ts = {s: {bars[s][i][0]: sg.side for i, sg in dsig[s].items()}
                for s in bars}
    cls = {"same-bar": [0, 0.0], "d-held": [0, 0.0], "free": [0, 0.0]}
    for t in rX["trades"]:
        if t["strat"] != xname:
            continue
        same_bar = d_sig_ts.get(t["sym"], {}).get(t["open_ts"]) == t["side"]
        d_held = any(d["sym"] == t["sym"] and d["open_ts"] <= t["open_ts"] <= d["ts"]
                     for d in rD["trades"])
        c = "same-bar" if same_bar else ("d-held" if d_held else "free")
        cls[c][0] += 1
        cls[c][1] += t["net"]
    return {k: {"n": v[0], "net": round(v[1], 2)} for k, v in cls.items()}


def brief(r):
    return {k: r[k] for k in ("n", "wr", "pf", "net", "max_dd", "peak_same_side")}


# ------------------------------------------------------------------- main
def main():
    barsA = {s: load_vol(f"{ROOT}/analysis/data_1h", s) for s in m.SYMBOLS}
    barsB = {s: load_vol(f"{ROOT}/analysis/data_1h_oos", s) for s in m.SYMBOLS}
    fund = {s: funding_pct_series(load_funding(s)) for s in m.SYMBOLS}
    rateA, pctA, rateB, pctB = {}, {}, {}, {}
    for s in m.SYMBOLS:
        rateA[s], pctA[s] = align_funding(fund[s], barsA[s])
        rateB[s], pctB[s] = align_funding(fund[s], barsB[s])

    out = {"data": {"volume": "v column of existing window CSVs (first use in repo history)",
                    "funding": "Binance USDT-M 8h, 2024-03-01..2026-08-31, data.binance.vision; "
                               "proxy for window B (OKX own history ~2 months only)",
                    "funding_events_per_symbol": len(fund["ETHUSD"])}}

    # ---- validation: ungated reproduction ----
    dA = {s: gen_donchian(barsA[s], rateA[s], pctA[s]) for s in m.SYMBOLS}
    dB = {s: gen_donchian(barsB[s], rateB[s], pctB[s]) for s in m.SYMBOLS}
    bA = m._walk(barsA, dA, "A", max_same_side=CAP)
    bB = m._walk(barsB, dB, "B", max_same_side=CAP)
    print(f"[VALIDATE] gated-generator no-gate run: A {bA['net']} (expect 163.56) | "
          f"B {bB['net']} (expect 143.06)")
    if abs(bA["net"] - 163.56) > 0.5 or abs(bB["net"] - 143.06) > 0.5:
        print("!! BASELINE MISMATCH — aborting")
        return
    baseA_net, baseB_net = bA["net"], bB["net"]
    out["baseline"] = {"A": brief(bA), "B": brief(bB)}

    # ---- gates battery ----
    gates = {
        "V1 vol>med100": lambda v, i, st: st["med"][i] is not None and v[i] > st["med"][i],
        "V2 vol>p75(100)": lambda v, i, st: st["p75"][i] is not None and v[i] > st["p75"][i],
        "V3 vol>1.5xmean20": lambda v, i, st: st["mean20"][i] is not None and v[i] > 1.5 * st["mean20"][i],
        "V4 vol<med100 (quiet)": lambda v, i, st: st["med"][i] is not None and v[i] < st["med"][i],
    }
    fund_gates = {
        "FR1 skip crowded side p75/25": lambda pct, rate, side:
            None if pct is None else (pct <= 75 if side == "buy" else pct >= 25),
        "FR2 skip crowded side p90/10": lambda pct, rate, side:
            None if pct is None else (pct <= 90 if side == "buy" else pct >= 10),
        "FR3 aligned (receive funding)": lambda pct, rate, side:
            None if rate is None else (rate <= 0 if side == "buy" else rate >= 0),
    }
    out["gates"] = {}
    print(f"\n== GATES vs baseline A {baseA_net} / B {baseB_net} ==")
    for name, vg in gates.items():
        ra = m._walk(barsA, {s: gen_donchian(barsA[s], rateA[s], pctA[s], vol_gate=vg) for s in m.SYMBOLS}, "A", max_same_side=CAP)
        rb = m._walk(barsB, {s: gen_donchian(barsB[s], rateB[s], pctB[s], vol_gate=vg) for s in m.SYMBOLS}, "B", max_same_side=CAP)
        ok = ra["net"] > baseA_net and rb["net"] > baseB_net
        out["gates"][name] = {"A": brief(ra), "B": brief(rb), "beats_both": ok}
        print(f"[{name:<28}] A n={ra['n']:>4} pf={ra['pf']:>5} net={ra['net']:>8} | "
              f"B n={rb['n']:>4} pf={rb['pf']:>5} net={rb['net']:>8} | {'PASS' if ok else '✗'}")
    for name, fg in fund_gates.items():
        ra = m._walk(barsA, {s: gen_donchian(barsA[s], rateA[s], pctA[s], fund_gate=fg) for s in m.SYMBOLS}, "A", max_same_side=CAP)
        rb = m._walk(barsB, {s: gen_donchian(barsB[s], rateB[s], pctB[s], fund_gate=fg) for s in m.SYMBOLS}, "B", max_same_side=CAP)
        ok = ra["net"] > baseA_net and rb["net"] > baseB_net
        out["gates"]["FUND " + name] = {"A": brief(ra), "B": brief(rb), "beats_both": ok}
        print(f"[{'FUND ' + name:<28}] A n={ra['n']:>4} pf={ra['pf']:>5} net={ra['net']:>8} | "
              f"B n={rb['n']:>4} pf={rb['pf']:>5} net={rb['net']:>8} | {'PASS' if ok else '✗'}")

    # ---- standalones (sprint-7 reopening rule) ----
    out["standalone"] = {}
    print("\n== STANDALONES (rule: solo+ BOTH windows AND free trades + BOTH) ==")
    for key, name, genA, genB in (
        ("SV1", "VolShock 3x", lambda s: gen_volshock(barsA[s]),
         lambda s: gen_volshock(barsB[s])),
        ("SF1", "FundTrap 97/3", lambda s: gen_fundtrap(barsA[s], rateA[s], pctA[s]),
         lambda s: gen_fundtrap(barsB[s], rateB[s], pctB[s])),
    ):
        sa = {s: genA(s) for s in m.SYMBOLS}
        sb = {s: genB(s) for s in m.SYMBOLS}
        ra = m._walk(barsA, sa, "A", max_same_side=CAP)
        rb = m._walk(barsB, sb, "B", max_same_side=CAP)
        solo = ra["net"] > 0 and rb["net"] > 0
        da = decompose_vs_donchian(barsA, dA, sa, name.split()[0])
        db = decompose_vs_donchian(barsB, dB, sb, name.split()[0])
        free_ok = da["free"]["net"] > 0 and db["free"]["net"] > 0
        out["standalone"][key] = {"name": name, "solo": {"A": brief(ra), "B": brief(rb),
                                                         "both_positive": solo},
                                  "decomposition": {"A": da, "B": db},
                                  "free_both_positive": free_ok,
                                  "qualifies": bool(solo and free_ok)}
        print(f"[{key} {name:<14}] solo A n={ra['n']:>4} pf={ra['pf']:>5} net={ra['net']:>8} | "
              f"B n={rb['n']:>4} pf={rb['pf']:>5} net={rb['net']:>8} | solo:{'✓' if solo else '✗'}")
        print(f"    free-vs-D  A {da['free']['n']} trades {da['free']['net']} | "
              f"B {db['free']['n']} trades {db['free']['net']} | free:{'✓' if free_ok else '✗'}"
              f"  => {'QUALIFIES' if (solo and free_ok) else '✗'}")

    json.dump(out, open(OUT, "w"), indent=1, default=str)
    print(f"\nsaved -> {OUT}")


if __name__ == "__main__":
    main()
