#!/usr/bin/env python3
"""v24 sprint 7 — the independent-portfolio review (owner directive 2026-09-15).

Owner: "Review the previous and new strategies together; any that is
profitable alone or alongside ONE other should enter trades independently,
not everything fused into one signal."

Two independent-operation architectures, both live-faithful (shared balance,
risk sizing 0.4%, cap $80, taker fee 0.06%/side + slippage, engine exits,
global MAX_SAME_SIDE=4):

  ARCH-B (waterfall):   one position per symbol; on each bar the enabled
                        strategies are tried in priority order, first signal
                        wins. This is what the live engine does TODAY with
                        ENABLED_STRATEGIES=(a, b) — sprint-6 "combo".
  ARCH-A (indep books): one position per (strategy, symbol); two strategies
                        may BOTH hold the same symbol at once; global
                        same-side cap 4 shared. The owner's literal proposal.

Candidates (all dual-window solo-positive, or the live incumbent):
  D  Donchian_Trend    — live incumbent, params frozen (v20.6/v22)
  I  Imba_Fib tp4=6    — the only other repo strategy ever dual-positive
                         (A +$11.63 / B +$50.79, ict_validation stage 1);
                         a DIFFERENT edge family (fib pullback vs breakout)
  T  Turtle+1ATR       — bench #1 (exact reconstruction verified vs record)
  C  Chandelier 4xATR  — bench #2 (literal el=50 am=4 tp=30; fresh dual+)
  S  Squeeze+range1.25 — bench #3 representative (exact confirm rule lost
                         in /tmp; rng>=1.25xATR reading, fresh dual+)

Vetoed-on-record and NOT re-run (permanent rule): EmaCross_Trend (A -$21 /
B -$58), the six legacy families (v20.5 real-data OOS failure), all SMC
variants (v23.7/8 + sprint 5).

Ship rule: a pair/architecture is activatable only if it beats the Donchian
solo baseline on BOTH windows (net) without collapsing PF or maxDD.
"""
import importlib.util as il
import json
import pickle
import sys

sys.path.insert(0, "/home/user/master-ai-bot")
sys.path.insert(0, "/home/user/master-ai-bot/analysis/scripts")
_m = il.spec_from_file_location("m", "/home/user/master-ai-bot/analysis/scripts/ict_validation.py")
m = il.module_from_spec(_m); _m.loader.exec_module(m)
import v24_sprint4 as s4
import v24_sprint6 as s6
from app.strategy import indicators as ind
from app.models import Signal

CAP = 4                      # live MAX_SAME_SIDE
PICKLE = "/tmp/s7_signals.pkl"
OUT = "/home/user/master-ai-bot/analysis/runs/v24_sprint7.json"


# ------------------------------------------------------------------
# candidate generators
# ------------------------------------------------------------------
def gen_turtle1atr(series):
    """Bench #1 — Turtle 20/10 + 1xATR entry margin, EMA200 agreement.

    Exact reconstruction: reproduces the recorded sprint-6 round-2 numbers
    bit-for-bit (A n=664 wr=43.2 pf=1.08 net=26.5 | B n=740 wr=44.9
    pf=1.14 net=53.49).
    """
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    closes = [b[4] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    e200 = ind.ema(closes, 200)
    sigs = {}
    for i in range(205, len(closes) - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i]:
            continue
        ph = max(highs[i - 21:i])
        pl = min(lows[i - 21:i])
        tgt_up = max(highs[i - 11:i])
        tgt_dn = min(lows[i - 11:i])
        px = closes[i]
        if px > ph + 1.0 * atr and px > e200[i]:
            tp = max(tgt_up, px + 3 * atr)
            sigs[i] = Signal(side="buy", strategy="Turtle1ATR", reason="20+1ATR breakout",
                             entry=px, sl=px - 2 * atr, tp1=None, tp=tp, rsi=50.0,
                             atr=atr, htf="bullish")
        elif px < pl - 1.0 * atr and px < e200[i]:
            tp = min(tgt_dn, px - 3 * atr)
            sigs[i] = Signal(side="sell", strategy="Turtle1ATR", reason="20+1ATR breakdown",
                             entry=px, sl=px + 2 * atr, tp1=None, tp=tp, rsi=50.0,
                             atr=atr, htf="bearish")
    return sigs


def gen_chandelier4(series):
    """Bench #2 — 50-bar breakout, 4xATR stop, far target (engine trail exits)."""
    return s6.m5_chandelier(series, entry_len=50, atr_mult=4.0)


def gen_squeeze125(series):
    """Bench #3 representative — BB-squeeze release + range >= 1.25xATR confirm.

    Caveat: the exact sprint-6 round-2 confirm rule was lost with /tmp; this
    reading is freshly dual-positive on its own (A +12.71 / B +8.76).
    """
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    closes = [b[4] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    e200 = ind.ema(closes, 200)
    mid_l, up_l, lo_l = ind.bollinger(closes, 20, 2.0)
    sigs = {}
    for i in range(205, len(closes) - 1):
        atr = atr_l[i] or 0.0
        if atr <= 0 or not e200[i] or not mid_l[i] or not up_l[i]:
            continue
        wnow = (up_l[i] - lo_l[i]) / (mid_l[i] + 1e-12)
        window = [(up_l[j] - lo_l[j]) / (mid_l[j] + 1e-12)
                  for j in range(max(0, i - 100), i + 1) if up_l[j] and mid_l[j]]
        if not window or wnow > sorted(window)[int(0.2 * len(window))]:
            continue
        rng = highs[i] - lows[i]
        if rng < 1.25 * (atr_l[i - 1] or atr):
            continue
        px = closes[i]
        if px > up_l[i] and px > e200[i]:
            sigs[i] = Signal(side="buy", strategy="Squeeze125", reason="squeeze release up",
                             entry=px, sl=px - 2.5 * atr, tp1=None, tp=px + 20 * atr,
                             rsi=50.0, atr=atr, htf="bullish")
        elif px < lo_l[i] and px < e200[i]:
            sigs[i] = Signal(side="sell", strategy="Squeeze125", reason="squeeze release down",
                             entry=px, sl=px + 2.5 * atr, tp1=None, tp=px - 20 * atr,
                             rsi=50.0, atr=atr, htf="bearish")
    return sigs


CANDS = {
    "D": ("Donchian_Trend", lambda ser: s4.gen_baseline(ser)),
    "I": ("Imba_Fib_tp4_6", lambda ser: m.imba_signals(ser, 6)),
    "T": ("Turtle_1ATR", gen_turtle1atr),
    "C": ("Chandelier_4ATR", gen_chandelier4),
    "S": ("Squeeze_rng125", gen_squeeze125),
}


# ------------------------------------------------------------------
# ARCH-A walk: independent books, one position per (strategy, symbol),
# global same-side cap shared across strategies. Exits identical to _walk.
# ------------------------------------------------------------------
def walk_indep(bars, sig_maps, label, cap=CAP):
    """sig_maps: ordered dict {strat_key: {symbol: {bar_i: Signal}}}."""
    balance = m.START_BALANCE
    positions = {}                       # (strat, sym) -> SimPos
    closed = []
    timelines = {s: {b[0]: k for k, b in enumerate(series)}
                 for s, series in bars.items()}
    peak_same = 0
    all_ts = sorted({b[0] for series in bars.values() for b in series})
    for ts in all_ts:
        # ---- exits first (per position) ----
        for key in list(positions):
            strat, sym = key
            if ts not in timelines[sym]:
                continue
            k = timelines[sym][ts]
            bar = bars[sym][k]
            p = positions[key]
            o, h, l, c = bar[1], bar[2], bar[3], bar[4]
            exit_px = reason = None
            if p.side == "buy":
                sl_hit, tp_hit = l <= p.sl, h >= p.tp
            else:
                sl_hit, tp_hit = h >= p.sl, l <= p.tp
            if sl_hit and tp_hit:
                exit_px, reason = p.sl, ("TrailStop" if p.trailed else "SL")
            elif sl_hit:
                exit_px, reason = p.sl, ("TrailStop" if p.trailed else "SL")
            elif tp_hit:
                exit_px, reason = p.tp, "TP"
            elif k - p.open_i >= m.MAX_HOLD_BARS:
                exit_px, reason = c, "MaxHold"
            if exit_px is not None:
                gross = ((exit_px - p.entry) if p.side == "buy"
                         else (p.entry - exit_px)) * p.qty
                fees = exit_px * p.qty * m.FEE
                balance += gross - fees
                closed.append(dict(sym=sym, side=p.side, strat=p.strategy,
                                   entry=p.entry, exit=exit_px, qty=p.qty,
                                   net=gross - fees, reason=reason, ts=ts,
                                   open_ts=p.open_ts))
                del positions[key]
                continue
            pc = ((c - p.entry) / p.entry * 100 if p.side == "buy"
                  else (p.entry - c) / p.entry * 100)
            if pc > m.TRAIL_ACT and pc > p.highest_pnl_pct:
                p.highest_pnl_pct = pc
                dist = max(c * m.TRAIL_STEP / 100.0, p.atr * m.ATR_TRAIL_MULT)
                cand = c - dist if p.side == "buy" else c + dist
                if (p.side == "buy" and cand > p.sl) or \
                        (p.side == "sell" and cand < p.sl):
                    p.sl = cand
                    p.trailed = True
        # ---- entries (strategies in priority order) ----
        for strat, sym_maps in sig_maps.items():
            for sym, sigs in sym_maps.items():
                key = (strat, sym)
                if key in positions or ts not in timelines[sym]:
                    continue
                k = timelines[sym][ts]
                sig = sigs.get(k)
                if sig is None or k + 1 >= len(bars[sym]):
                    continue
                same = sum(1 for p in positions.values() if p.side == sig.side)
                peak_same = max(peak_same, same + 1 if same < cap else same)
                if same >= cap:
                    continue
                nxt = bars[sym][k + 1]
                fill = nxt[1] * ((1 + m.SLIP) if sig.side == "buy" else (1 - m.SLIP))
                sl_d = abs(sig.entry - sig.sl)
                tp_d = abs(sig.tp - sig.entry)
                cost_d = fill * (2 * (m.TAKER * m.FEE_BUF) + 2 * m.SLIP)
                if tp_d < cost_d * m.MIN_EDGE:
                    if cost_d * m.MIN_EDGE > tp_d * 2.0:
                        continue
                    tp_d = cost_d * m.MIN_EDGE
                sl_d = max(sl_d, fill * m.MIN_STOP_PCT)
                risk = balance * m.RISK_PCT
                qty = min(risk / sl_d, m.MAX_NOTIONAL / fill)
                if qty * fill < 1.0:
                    continue
                balance -= fill * qty * m.FEE
                positions[key] = m.SimPos(
                    symbol=sym, side=sig.side, strategy=sig.strategy,
                    entry=fill, qty=qty,
                    sl=(fill - sl_d) if sig.side == "buy" else (fill + sl_d),
                    tp=(fill + tp_d) if sig.side == "buy" else (fill - tp_d),
                    atr=sig.atr if sig.atr > 0 else fill * 0.01,
                    open_i=k + 1)
                positions[key].open_ts = ts
    for key, p in positions.items():
        series = bars[p.symbol]
        c = series[-1][4]
        gross = ((c - p.entry) if p.side == "buy" else (p.entry - c)) * p.qty
        fees = c * p.qty * m.FEE
        balance += gross - fees
        closed.append(dict(sym=p.symbol, side=p.side, strat=p.strategy,
                           entry=p.entry, exit=c, qty=p.qty, net=gross - fees,
                           reason="EndOfTest", ts=series[-1][0],
                           open_ts=p.open_ts))
    r = m.summarize(closed, balance, label)
    r["peak_same_side"] = peak_same
    return r


def brief(r):
    return {k: r[k] for k in ("n", "wr", "pf", "net", "max_dd", "peak_same_side")}


# ------------------------------------------------------------------
def main():
    barsA = {s: m.load(m.DATA_A, s) for s in m.SYMBOLS}
    barsB = {s: m.load(m.DATA_B, s) for s in m.SYMBOLS}

    # ---- generate all signal maps once (Imba is the slow one) ----
    try:
        sig = pickle.load(open(PICKLE, "rb"))
        print("[signals] loaded from cache")
    except Exception:
        sig = {}
        for key, (name, fn) in CANDS.items():
            sig[key] = {"A": {s: fn(barsA[s]) for s in m.SYMBOLS},
                        "B": {s: fn(barsB[s]) for s in m.SYMBOLS}}
            print(f"[signals] {name} done")
        pickle.dump(sig, open(PICKLE, "wb"))

    out = {"candidates": {k: v[0] for k, v in CANDS.items()},
           "cap": CAP,
           "reconstruction_notes": {
               "T": "exact vs sprint-6 record (A 664/43.2/1.08/26.5, B 740/44.9/1.14/53.49)",
               "C": "literal el=50 am=4 tp=30; fresh dual-positive, differs from lost round-2 exits",
               "S": "confirm rule lost; rng>=1.25xATR representative, fresh dual-positive",
           }}

    # ---- phase 1: solo (both cap settings for the incumbent) ----
    solo = {}
    print("\n== SOLO (cap=4 live config; baseline also shown uncapped) ==")
    for key, (name, _) in CANDS.items():
        ra = m._walk(barsA, sig[key]["A"], "A", max_same_side=CAP)
        rb = m._walk(barsB, sig[key]["B"], "B", max_same_side=CAP)
        solo[key] = {"A": brief(ra), "B": brief(rb)}
        print(f"[{key}] {name:<16} A n={ra['n']:>4} wr={ra['wr']:>5} pf={ra['pf']:>5} "
              f"net={ra['net']:>8} dd={ra['max_dd']:>5} | "
              f"B n={rb['n']:>4} wr={rb['wr']:>5} pf={rb['pf']:>5} net={rb['net']:>8} dd={rb['max_dd']:>5}")
    bA0 = m._walk(barsA, sig["D"]["A"], "A", max_same_side=0)
    bB0 = m._walk(barsB, sig["D"]["B"], "B", max_same_side=0)
    print(f"[D-uncapped]              A net={bA0['net']} | B net={bB0['net']}")
    out["solo_cap4"] = solo
    out["baseline_uncapped"] = {"A": brief(bA0), "B": brief(bB0)}
    baseA, baseB = solo["D"]["A"]["net"], solo["D"]["B"]["net"]

    # ---- phase 2: all pairs, both architectures ----
    keys = list(CANDS.keys())
    pairs = {}
    print("\n== PAIRS (cap=4) — baseline D-solo: "
          f"A {baseA} / B {baseB} ==")
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            a, b = keys[i], keys[j]
            tag = f"{a}+{b}"
            # ARCH-B: waterfall, first-listed has priority, one book/symbol
            mB_A = {s: {k: (sig[a]["A"][s].get(k) or sig[b]["A"][s].get(k))
                        for k in set(list(sig[a]["A"][s]) + list(sig[b]["A"][s]))}
                    for s in m.SYMBOLS}
            mB_B = {s: {k: (sig[a]["B"][s].get(k) or sig[b]["B"][s].get(k))
                        for k in set(list(sig[a]["B"][s]) + list(sig[b]["B"][s]))}
                    for s in m.SYMBOLS}
            rBA = m._walk(barsA, mB_A, "A", max_same_side=CAP)
            rBB = m._walk(barsB, mB_B, "B", max_same_side=CAP)
            # ARCH-A: independent books
            rAA = walk_indep(barsA, {a: sig[a]["A"], b: sig[b]["A"]}, "A")
            rAB = walk_indep(barsB, {a: sig[a]["B"], b: sig[b]["B"]}, "B")
            okB = rBA["net"] > baseA and rBB["net"] > baseB
            okA = rAA["net"] > baseA and rAB["net"] > baseB
            pairs[tag] = {
                "archB_waterfall": {"A": brief(rBA), "B": brief(rBB), "beats_both": okB},
                "archA_indep_books": {"A": brief(rAA), "B": brief(rAB), "beats_both": okA},
            }
            print(f"[{tag}] B: A {rBA['net']:>8} / B {rBB['net']:>8} {'✓' if okB else '✗'}"
                  f"  (n {rBA['n']}/{rBB['n']} pf {rBA['pf']}/{rBB['pf']})   "
                  f"A: A {rAA['net']:>8} / B {rAB['net']:>8} {'✓' if okA else '✗'}"
                  f"  (n {rAA['n']}/{rAB['n']} pf {rAA['pf']}/{rAB['pf']} "
                  f"dd {rAA['max_dd']}/{rAB['max_dd']} peak {rAA['peak_same_side']}/{rAB['peak_same_side']})")
    out["pairs"] = pairs

    # ---- phase 3: marginal decomposition of the partner's trades (ARCH-A,
    #      partner vs D) — is the partner adding anything D doesn't hold? ----
    print("\n== DECOMPOSITION (arch-A partner trades vs D activity) ==")
    decomp = {}
    for key in ("I", "T", "C", "S"):
        name = CANDS[key][0]
        pname = sig_name(key)
        row = {}
        for win, bars in (("A", barsA), ("B", barsB)):
            rD = walk_indep(bars, {"D": sig["D"][win]}, win)   # D trades w/ open_ts
            rP = walk_indep(bars, {"D": sig["D"][win], key: sig[key][win]}, win)
            cls = {"same-bar": [0, 0.0], "d-held": [0, 0.0], "free": [0, 0.0]}
            d_sig_ts = {s: {bars[s][i][0]: sg.side
                            for i, sg in sig["D"][win][s].items()} for s in m.SYMBOLS}
            for t in rP["trades"]:
                if t["strat"] != pname:
                    continue
                sym = t["sym"]
                same_bar = d_sig_ts.get(sym, {}).get(t["open_ts"]) == t["side"]
                d_held = any(d["sym"] == sym and d["open_ts"] <= t["open_ts"] <= d["ts"]
                             for d in rD["trades"])
                c = "same-bar" if same_bar else ("d-held" if d_held else "free")
                cls[c][0] += 1
                cls[c][1] += t["net"]
            row[win] = {k: {"n": v[0], "net": round(v[1], 2)} for k, v in cls.items()}
        decomp[key] = row
        print(f"[{key}] {name:<16} " + " | ".join(
            f"{w}: same-bar {row[w]['same-bar']['n']}({row[w]['same-bar']['net']}) "
            f"d-held {row[w]['d-held']['n']}({row[w]['d-held']['net']}) "
            f"free {row[w]['free']['n']}({row[w]['free']['net']})"
            for w in ("A", "B")))
    out["decomposition_partner_vs_D"] = decomp

    json.dump(out, open(OUT, "w"), indent=1, default=str)
    print(f"\nsaved -> {OUT}")


def sig_name(key):
    return {"D": "Donchian_Trend", "I": "Imba_Fib", "T": "Turtle1ATR",
            "C": "Chandelier", "S": "Squeeze125"}[key]


if __name__ == "__main__":
    main()
