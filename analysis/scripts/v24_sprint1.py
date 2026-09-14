#!/usr/bin/env python3
"""v24 sprint 1 — specialist working-group study.

A) Per-symbol Donchian evidence for the 8 never-validated symbols
   (each split into two independent 8-month halves).
B) Maker/limit-entry cost study on the 5 core validated symbols
   (existing windows A & B): fill at signal close if next bar touches it,
   maker fee 0.02% / 0.00% variants vs taker baseline.
"""
import importlib.util as il
import json, os, sys

sys.path.insert(0, "/home/user/master-ai-bot")
_sp = il.spec_from_file_location(
    "base", "/home/user/master-ai-bot/analysis/scripts/ict_validation.py")
m = il.module_from_spec(_sp); _sp.loader.exec_module(m)

from app.strategy import indicators as ind  # noqa: E402
from app.strategy.signals import DonchianTrend  # noqa: E402

EXTRA = "/home/user/master-ai-bot/analysis/data_1h_okx_extra"
PARAMS = dict(entry_len=40, sl_m=2.5, tp_m=20.0, break_atr=1.5, long_dist_atr=1.0)
W = 300


def donchian_signals(series):
    strat = DonchianTrend(dict(PARAMS))
    closes = [b[4] for b in series]
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    ema_l = ind.ema(closes, 200)
    sigs = {}
    for i in range(205, len(series) - 1):
        lo = max(0, i - W + 1)
        tf5 = m._Ctx("1h", closes[lo:i + 1], highs[lo:i + 1], lows[lo:i + 1],
                     atr_l[i] or 0.0, 50.0, ema_l[i])
        ctx = m._Htf(closes[i], tf5)
        s = strat.propose(ctx)
        if s is not None:
            sigs[i] = s
    return sigs


def walk_maker(bars, signals_by_bar, label, maker_fee, maker=True):
    """_walk variant with maker/limit entries (no slippage on entry)."""
    balance, positions, closed = m.START_BALANCE, {}, []
    timelines = {s: {b[0]: k for k, b in enumerate(series)}
                 for s, series in bars.items()}
    all_ts = sorted({b[0] for series in bars.values() for b in series})
    fee_in = maker_fee if maker else m.FEE
    slip_in = 0.0 if maker else m.SLIP
    for ts in all_ts:
        for sym in list(positions):
            if ts not in timelines[sym]:
                continue
            k = timelines[sym][ts]; p = positions[sym]
            bar = bars[sym][k]
            o, h, l, c = bar[1], bar[2], bar[3], bar[4]
            if p.side == "buy":
                sl_hit, tp_hit = l <= p.sl, h >= p.tp
            else:
                sl_hit, tp_hit = h >= p.sl, l <= p.tp
            exit_px = reason = None
            if sl_hit:
                exit_px, reason = p.sl, ("TrailStop" if p.trailed else "SL")
            elif tp_hit:
                exit_px, reason = p.tp, "TP"
            elif k - p.open_i >= m.MAX_HOLD_BARS:
                exit_px, reason = c, "MaxHold"
            if exit_px is not None:
                gross = ((exit_px - p.entry) if p.side == "buy"
                         else (p.entry - exit_px)) * p.qty
                tn = gross - exit_px * p.qty * m.FEE - getattr(p, "entry_fee_paid", 0.0)
                balance += tn
                closed.append(dict(sym=sym, strat=p.strategy, net=tn,
                                   reason=reason, entry=p.entry, exit=exit_px, qty=p.qty))
                del positions[sym]
                continue
            pc = ((c - p.entry) / p.entry * 100 if p.side == "buy"
                  else (p.entry - c) / p.entry * 100)
            if pc > m.TRAIL_ACT and pc > p.highest_pnl_pct:
                p.highest_pnl_pct = pc
                dist = max(c * m.TRAIL_STEP / 100.0, p.atr * m.ATR_TRAIL_MULT)
                cand = c - dist if p.side == "buy" else c + dist
                if (p.side == "buy" and cand > p.sl) or (p.side == "sell" and cand < p.sl):
                    p.sl = cand; p.trailed = True
        for sym, sigs in signals_by_bar.items():
            if sym in positions or ts not in timelines[sym]:
                continue
            k = timelines[sym][ts]
            sig = sigs.get(k)
            if sig is None or k + 1 >= len(bars[sym]):
                continue
            nxt = bars[sym][k + 1]
            if maker:
                # limit at signal close; filled only if next bar retraces to it
                if sig.side == "buy" and nxt[3] > sig.entry:
                    continue                      # low never reached the limit
                if sig.side == "sell" and nxt[2] < sig.entry:
                    continue                      # high never reached the limit
                fill = sig.entry
            else:
                fill = nxt[1] * ((1 + m.SLIP) if sig.side == "buy" else (1 - m.SLIP))
            sl_d = abs(sig.entry - sig.sl); tp_d = abs(sig.tp - sig.entry)
            cost_d = fill * m.round_trip_cost(m.TAKER, m.FEE_BUF, m.SLIP) if False else fill * (2 * (m.TAKER * m.FEE_BUF) + 2 * m.SLIP)
            if tp_d < cost_d * m.MIN_EDGE:
                if cost_d * m.MIN_EDGE > tp_d * 2.0:
                    continue
                tp_d = cost_d * m.MIN_EDGE
            sl_d = max(sl_d, fill * m.MIN_STOP_PCT)
            qty = min(balance * m.RISK_PCT / sl_d, m.MAX_NOTIONAL / fill)
            if qty * fill < 1.0:
                continue
            entry_fee = fill * qty * fee_in
            balance -= entry_fee
            pp = m.SimPos(sym, sig.side, sig.strategy, fill, qty,
                          (fill - sl_d) if sig.side == "buy" else (fill + sl_d),
                          (fill + tp_d) if sig.side == "buy" else (fill - tp_d),
                          sig.atr if sig.atr > 0 else fill * 0.01, k + 1)
            pp.entry_fee_paid = entry_fee
            positions[sym] = pp
    for sym, p in positions.items():
        c = bars[sym][-1][4]
        net = ((c - p.entry) if p.side == "buy" else (p.entry - c)) * p.qty - c * p.qty * m.FEE - getattr(p, "entry_fee_paid", 0.0)
        closed.append(dict(sym=sym, strat=p.strategy, net=net, reason="EndOfTest",
                           entry=p.entry, exit=c, qty=p.qty))
    wins = [t for t in closed if t["net"] > 0]
    gw = sum(t["net"] for t in wins)
    gl = abs(sum(t["net"] for t in closed if t["net"] <= 0))
    return dict(window=label, n=len(closed),
                wr=round(100 * len(wins) / max(1, len(closed)), 1),
                pf=round(gw / gl, 2) if gl > 0 else 99.0,
                net=round(sum(t["net"] for t in closed), 2))


def main():
    out = {}
    # ---- A) per-symbol Donchian on the 8 unvalidated symbols -------------
    print("=== A) Donchian solo — نمادهای بدون اعتبارسنجی (دو نیمه‌ی مستقل) ===")
    symA = {}
    for sym in ["XRP", "AVAX", "DOT", "LINK", "ADA", "BCH", "LTC", "TRX"]:
        series = m.load(EXTRA, sym + "USD")
        half = len(series) // 2
        r1 = m._walk({sym: series[:half]}, {sym: donchian_signals(series[:half])}, sym + "-H1")
        r2 = m._walk({sym: series[half:]}, {sym: donchian_signals(series[half:])}, sym + "-H2")
        verdict = "PASS" if (r1["net"] > 0 and r2["net"] > 0) else (
            "MIXED" if (r1["net"] + r2["net"]) > 0 else "FAIL")
        symA[sym] = {"H1": {k: r1[k] for k in ("n", "wr", "pf", "net")},
                     "H2": {k: r2[k] for k in ("n", "wr", "pf", "net")},
                     "verdict": verdict}
        print(f"  {sym:<5} H1: n={r1['n']:>3} pf={r1['pf']:>5} net={r1['net']:>7} | "
              f"H2: n={r2['n']:>3} pf={r2['pf']:>5} net={r2['net']:>7} | {verdict}")
    out["donchian_per_symbol"] = symA

    # reference: core 5 on existing windows
    print("\n=== مرجع: ۵ نماد اصلی روی پنجره‌های A/B موجود ===")
    ref = {}
    for tag, data in (("A", m.DATA_A), ("B", m.DATA_B)):
        bars = {s: m.load(data, s) for s in m.SYMBOLS}
        sigs = {s: donchian_signals(bars[s]) for s in m.SYMBOLS}
        r = m._walk(bars, sigs, tag)
        ref[tag] = {k: r[k] for k in ("n", "wr", "pf", "net", "max_dd")}
        print(f"  {tag}: n={r['n']} wr={r['wr']} pf={r['pf']} net={r['net']} dd={r['max_dd']}")
    out["donchian_core5_reference"] = ref

    # ---- B) maker-entry study on core 5 -----------------------------------
    print("\n=== B) ورود maker/limit (۵ نماد اصلی، هر دو پنجره) ===")
    mk = {}
    for tag, data in (("A", m.DATA_A), ("B", m.DATA_B)):
        bars = {s: m.load(data, s) for s in m.SYMBOLS}
        sigs = {s: donchian_signals(bars[s]) for s in m.SYMBOLS}
        base = walk_maker(bars, sigs, tag, m.FEE, maker=False)
        v002 = walk_maker(bars, sigs, tag, 0.0002 * m.FEE_BUF, maker=True)
        v000 = walk_maker(bars, sigs, tag, 0.0, maker=True)
        mk[tag] = {"taker_baseline": base, "maker_0.02%": v002, "maker_0%": v000}
        print(f"  {tag}: taker net={base['net']:>7} (n={base['n']}) | "
              f"maker 0.02% net={v002['net']:>7} (n={v002['n']}) | "
              f"maker 0% net={v000['net']:>7} (n={v000['n']})")
    out["maker_entry_study"] = mk
    p = "/home/user/master-ai-bot/analysis/runs/v24_sprint1.json"
    json.dump(out, open(p, "w"), indent=1)
    print("\nsaved ->", p)


if __name__ == "__main__":
    main()
