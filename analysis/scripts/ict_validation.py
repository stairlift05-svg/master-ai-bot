#!/usr/bin/env python3
"""v23.7 committee validation — 'ICT Validated SMC v1.8' (Pine v6) as a bot strategy.

Protocol (identical to imba_v23 / v23.4 / v23.5 harness):
  Window A = analysis/data_1h      (14 months, Binance, in-sample era)
  Window B = analysis/data_1h_oos  (16 months, OKX, unseen)
  Live-equivalent engine: per-bar 1h cadence, next-bar-open fills with
  slippage against us, SL/TP re-anchored to fill, live fee model
  (taker 0.05% x fee_buffer 1.2 + slippage 0.02% per side), risk-based
  sizing (0.4% of balance, notional cap $80), engine exits (SL/TP intrabar
  with SL priority, ATR trailing act 4% dist max(1%price, 6xATR), MaxHold
  400h, EndOfTest).

Stage 1: calibrate the harness on Imba_Fib (tp4=4 -> A +32.17 / B -5.23,
         tp4=6 -> A +11.63 / B +50.79 must reproduce).
Stage 2: ICT SMC v1.8 solo (both windows).
Stage 3: combined Imba_Fib(tp4=6) + ICT (one position per symbol).
"""
import csv, json, math, os, sys, time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from app.strategy import indicators as ind  # noqa: E402
from app.strategy.signals import ImbaFib  # noqa: E402
from app.models import Signal  # noqa: E402

DATA_A = os.path.join(ROOT, "analysis", "data_1h")
DATA_B = os.path.join(ROOT, "analysis", "data_1h_oos")
SYMBOLS = ["BNBUSD", "BTCUSD", "DOGEUSD", "ETHUSD", "SOLUSD"]

# ---- live cost model (app/config.py defaults) --------------------------
TAKER = 0.0005
FEE_BUF = 1.2
SLIP = 0.0002
FEE = TAKER * FEE_BUF                      # per side, fraction of notional
MIN_EDGE = 3.0
MIN_STOP_PCT = 0.003
RISK_PCT = 0.004
MAX_NOTIONAL = 80.0
START_BALANCE = 1000.0
TRAIL_ACT = 4.0
TRAIL_STEP = 1.0
ATR_TRAIL_MULT = 6.0
MAX_HOLD_BARS = 400


def load(d, sym):
    rows = []
    with open(os.path.join(d, sym + ".csv"), newline="") as fh:
        for r in csv.DictReader(fh):
            rows.append((int(r["ts"]), float(r["o"]), float(r["h"]),
                         float(r["l"]), float(r["c"])))
    rows.sort()
    return rows


# ------------------------------------------------------------------
# Live-equivalent engine (portfolio: one position per symbol)
# ------------------------------------------------------------------
@dataclass
class SimPos:
    symbol: str
    side: str            # "buy" | "sell"
    strategy: str
    entry: float
    qty: float
    sl: float
    tp: float
    atr: float
    open_i: int
    highest_pnl_pct: float = 0.0
    trailed: bool = False


def _walk(bars, signals_by_bar, label):
    balance = START_BALANCE
    positions = {}
    closed = []
    # global timeline: map ts -> symbol bars
    timelines = {s: {b[0]: k for k, b in enumerate(series)} for s, series in bars.items()}
    all_ts = sorted({b[0] for series in bars.values() for b in series})
    for ts in all_ts:
        # ---- exits first ----
        for sym in list(positions):
            if ts not in timelines[sym]:
                continue
            k = timelines[sym][ts]
            bar = bars[sym][k]
            p = positions[sym]
            o, h, l, c = bar[1], bar[2], bar[3], bar[4]
            pnl_pct = ((o - p.entry) / p.entry * 100 if p.side == "buy"
                       else (p.entry - o) / p.entry * 100)
            # trail on bar open state (uses close of prev bar effectively via o)
            # engine checks each scan; hourly approx: evaluate at bar close
            # -> do trail update AFTER exit checks, on close
            exit_px = None
            reason = None
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
            elif k - p.open_i >= MAX_HOLD_BARS:
                exit_px, reason = c, "MaxHold"
            if exit_px is not None:
                gross = ((exit_px - p.entry) if p.side == "buy"
                         else (p.entry - exit_px)) * p.qty
                fees = exit_px * p.qty * FEE   # entry leg booked at open
                net = gross - fees
                balance += net
                closed.append(dict(sym=sym, side=p.side, strat=p.strategy,
                                   entry=p.entry, exit=exit_px, qty=p.qty,
                                   net=net, reason=reason, ts=ts))
                del positions[sym]
                continue
            # trail update on close
            pc = ((c - p.entry) / p.entry * 100 if p.side == "buy"
                  else (p.entry - c) / p.entry * 100)
            if pc > TRAIL_ACT and pc > p.highest_pnl_pct:
                p.highest_pnl_pct = pc
                dist = max(c * TRAIL_STEP / 100.0, p.atr * ATR_TRAIL_MULT)
                cand = c - dist if p.side == "buy" else c + dist
                if (p.side == "buy" and cand > p.sl) or \
                   (p.side == "sell" and cand < p.sl):
                    p.sl = cand
                    p.trailed = True
        # ---- entries ----
        for sym, sigs in signals_by_bar.items():
            if sym in positions or ts not in timelines[sym]:
                continue
            k = timelines[sym][ts]
            sig = sigs.get(k)
            if sig is None or k + 1 >= len(bars[sym]):
                continue
            nxt = bars[sym][k + 1]
            fill = nxt[1] * ((1 + SLIP) if sig.side == "buy" else (1 - SLIP))
            sl_d = abs(sig.entry - sig.sl)
            tp_d = abs(sig.tp - sig.entry)
            # cost gate (same as BaseStrategyV2._build)
            cost_d = fill * (2 * (TAKER * FEE_BUF) + 2 * SLIP)
            if tp_d < cost_d * MIN_EDGE:
                if cost_d * MIN_EDGE > tp_d * 2.0:
                    continue
                tp_d = cost_d * MIN_EDGE
            sl_d = max(sl_d, fill * MIN_STOP_PCT)
            risk = balance * RISK_PCT
            qty = min(risk / sl_d, MAX_NOTIONAL / fill)
            if qty * fill < 1.0:
                continue
            entry_fee = fill * qty * FEE
            balance -= entry_fee  # entry leg fee booked immediately
            positions[sym] = SimPos(
                symbol=sym, side=sig.side, strategy=sig.strategy,
                entry=fill, qty=qty,
                sl=(fill - sl_d) if sig.side == "buy" else (fill + sl_d),
                tp=(fill + tp_d) if sig.side == "buy" else (fill - tp_d),
                atr=sig.atr if sig.atr > 0 else fill * 0.01,
                open_i=k + 1)
            # note: entry fee double-count guard -> pnl math below adds exit
            # fee only from now on; store fee_paid marker via strategy name
            positions[sym].highest_pnl_pct = 0.0
            positions[sym].trailed = False
    # close remainder at last close
    for sym, p in positions.items():
        series = bars[sym]
        c = series[-1][4]
        gross = ((c - p.entry) if p.side == "buy" else (p.entry - c)) * p.qty
        fees = c * p.qty * FEE
        net = gross - fees
        balance += net
        closed.append(dict(sym=sym, side=p.side, strat=p.strategy,
                           entry=p.entry, exit=c, qty=p.qty, net=net,
                           reason="EndOfTest", ts=series[-1][0]))
    return summarize(closed, balance, label)


def summarize(closed, balance, label):
    wins = [t for t in closed if t["net"] > 0]
    losses = [t for t in closed if t["net"] <= 0]
    gross_w = sum(t["net"] for t in wins)
    gross_l = abs(sum(t["net"] for t in losses))
    peak, dd = balance, 0.0
    curve = START_BALANCE
    for t in closed:
        curve += t["net"]
        peak = max(peak, curve)
        dd = max(dd, (peak - curve) / peak * 100)
    reasons = {}
    for t in closed:
        reasons[t["reason"]] = reasons.get(t["reason"], 0) + 1
    return dict(
        window=label, n=len(closed),
        n_long=sum(1 for t in closed if t["side"] == "buy"),
        n_short=sum(1 for t in closed if t["side"] == "sell"),
        wr=round(100 * len(wins) / len(closed), 1) if closed else 0.0,
        pf=round(gross_w / gross_l, 2) if gross_l > 0 else float("inf"),
        net=round(balance - START_BALANCE, 2),
        max_dd=round(dd, 2),
        avg=round((balance - START_BALANCE) / len(closed), 3) if closed else 0,
        exits=reasons, trades=closed)


# ------------------------------------------------------------------
# Strategy 1 — Imba_Fib via the repo class (harness calibration)
# ------------------------------------------------------------------
class _Ctx:
    """Duck-typed TFContext with a tail window + precomputed scalars."""
    def __init__(self, label, closes, highs, lows, atr, rsi, ema200):
        self.label = label
        self.closes, self.highs, self.lows = closes, highs, lows
        self.volumes = [0.0] * len(closes)
        self.atr, self.rsi, self.ema200 = atr, rsi, ema200
        self.ema50 = ema200 or 0.0


class _Htf:
    def __init__(self, price, tf5):
        self.symbol = "X"
        self.price = price
        self.tf5 = tf5
        self.tf15 = tf5
        self.tf1 = tf5
        self.candle_bull_5m = True
        self.candle_bear_5m = True
        self.min_stop_pct = MIN_STOP_PCT
        self.round_trip_cost_pct = 2 * (TAKER * FEE_BUF) + 2 * SLIP
        self.min_edge_ratio = MIN_EDGE


def imba_signals(series, tp4):
    params = dict(sensitivity=18, use_filters=1, ema_len=200,
                  rsi_long_guard=72, rsi_short_guard=28,
                  tp1=1, tp2=2, tp3=3, tp4=tp4)
    strat = ImbaFib(params)
    closes = [b[4] for b in series]
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    rsi_l = ind.rsi_wilder(closes, 14)
    ema_l = ind.ema(closes, 200)
    W = 300
    sigs = {}
    for i in range(205, len(series) - 1):
        lo = max(0, i - W + 1)
        tf5 = _Ctx("1h", closes[lo:i + 1], highs[lo:i + 1], lows[lo:i + 1],
                   atr_l[i] or 0.0, rsi_l[i] or 50.0, ema_l[i])
        ctx = _Htf(closes[i], tf5)
        s = strat.propose(ctx)
        if s is not None:
            sigs[i] = s
    return sigs


# ------------------------------------------------------------------
# Strategy 2 — ICT Validated SMC v1.8 (signal engine port)
# ------------------------------------------------------------------
NY = ZoneInfo("America/New_York")


def _sess_active(ts_ms, start_hm, end_hm):
    t = datetime.fromtimestamp(ts_ms / 1000, NY)
    minutes = t.hour * 60 + t.minute
    sh, sm = start_hm
    eh, em = end_hm
    a, b = sh * 60 + sm, eh * 60 + em
    return a <= minutes < b or (a > b and (minutes >= a or minutes < b))


def in_killzone(ts_ms):
    return (_sess_active(ts_ms, (2, 0), (5, 0)) or      # London
            _sess_active(ts_ms, (8, 30), (11, 0)))      # NY AM


@dataclass
class _Swing:
    idx: int
    price: float
    isHigh: bool


@dataclass
class _OB:
    idx: int
    top: float
    bottom: float
    isBullish: bool
    score: int
    mitigated: bool = False


@dataclass
class _FVG:
    idx: int
    top: float
    bottom: float
    isBullish: bool
    mitigated: bool = False


@dataclass
class _Breaker:
    created: int
    top: float
    bottom: float
    isBullish: bool
    retested: bool = False
    mitigated: bool = False


@dataclass
class _OTE:
    idx: int
    legHigh: float
    legLow: float
    zoneTop: float
    zoneBottom: float
    isBullish: bool
    invalidated: bool = False


class _Htf4h:
    def __init__(self, swing_len):
        self.L = swing_len
        self.reset()

    def reset(self):
        self.buf = []          # 4h candles [ts4, o, h, l, c]
        self.pivH = []         # (idx4, price)
        self.pivL = []
        self.lastH = None
        self.lastL = None
        self.hBroken = False
        self.lBroken = False
        self.bull = True

    def step(self, ts, h, l, c):
        ts4 = ts // 14400000 * 14400000
        if not self.buf or self.buf[-1][0] != ts4:
            self.buf.append([ts4, c, h, l, c])
        else:
            b = self.buf[-1]
            b[2] = max(b[2], h)
            b[3] = min(b[3], l)
            b[4] = c
        i4 = len(self.buf) - 1
        L = self.L
        j = i4 - L
        if j >= L:
            seg = self.buf[j - L:j + L + 1]
            ph = max(s[2] for s in seg)
            pl = min(s[3] for s in seg)
            if self.buf[j][2] == ph and sum(1 for s in seg if s[2] == ph) == 1:
                if not self.pivH or self.pivH[-1][0] != j:
                    self.pivH.append((j, ph))
                    self.lastH, self.hBroken = ph, False
            if self.buf[j][3] == pl and sum(1 for s in seg if s[3] == pl) == 1:
                if not self.pivL or self.pivL[-1][0] != j:
                    self.pivL.append((j, pl))
                    self.lastL, self.lBroken = pl, False
        # real-time wick break on the forming 4h candle
        hh, ll = self.buf[-1][2], self.buf[-1][3]
        if self.lastH is not None and not self.hBroken and hh > self.lastH:
            self.bull = True
            self.hBroken = True
        if self.lastL is not None and not self.lBroken and ll < self.lastL:
            self.bull = False
            self.lBroken = True
        if len(self.buf) > 400:
            pass


def ict_signals(series, **kw):
    """ Stateless wrapper: runs the sequential machine over the whole
    series, emitting signals exactly as the live 1h cadence would."""
    p = dict(swing_len=10, htf_swing_len=10, min_sig_score=4, cooldown=10,
             fvg_min_atr=1.0, require_htf=True, require_sweep=True,
             require_disp=True, min_score=3, max_sl_atr=0.0)
    p.update(kw)
    L = p["swing_len"]
    highs = [b[2] for b in series]
    lows = [b[3] for b in series]
    closes = [b[4] for b in series]
    opens = [b[1] for b in series]
    atr_l = ind.atr_wilder(highs, lows, closes, 14)
    sigs = {}
    # state
    swings = []
    lastH = lastL = None
    lastH_idx = lastL_idx = None
    lastH_broken = lastL_broken = False
    bull = True
    obs, fvgs, brks, otes = [], [], [], []
    htf = _Htf4h(p["htf_swing_len"])
    lastBearOpen = lastBullOpen = None
    lastLongBar = lastShortBar = None
    warm = max(L * 2 + 1, 205)

    def add_pivot(i):
        nonlocal lastH, lastL, lastH_idx, lastL_idx, lastH_broken, lastL_broken
        j = i - L
        if j < L:
            return
        seg = slice(j - L, j + L + 1)
        ph = max(highs[seg])
        pl = min(lows[seg])
        if highs[j] == ph and sum(1 for x in highs[seg] if x == ph) == 1:
            swings.append(_Swing(j, ph, True))
            lastH, lastH_idx, lastH_broken = ph, j, False
        if lows[j] == pl and sum(1 for x in lows[seg] if x == pl) == 1:
            swings.append(_Swing(j, pl, False))
            lastL, lastL_idx, lastL_broken = pl, j, False

    for i in range(1, len(series) - 1):
        ts, o, h, l, c = series[i][0], opens[i], highs[i], lows[i], closes[i]
        atr_prev = atr_l[i - 1] or 0.0
        add_pivot(i)
        htf.step(ts, h, l, c)

        # ---- structure break (wick based, both-sides resolved by color) --
        brokeH = lastH is not None and not lastH_broken and h > lastH
        brokeL = lastL is not None and not lastL_broken and l < lastL
        if brokeH and brokeL:
            if c >= o:
                brokeL = False
            else:
                brokeH = False
        if brokeH:
            bull = True
            lastH_broken = True
            legH, legL = h, lastL
            if legL is not None and legH > legL:
                rng = legH - legL
                otes.append(_OTE(i, legH, legL, legH - rng * 0.618,
                                 legH - rng * 0.786, True))
        if brokeL:
            bull = False
            lastL_broken = True
            legH, legL = lastH, l
            if legH is not None and legH > legL:
                rng = legH - legL
                otes.append(_OTE(i, legH, legL, legL + rng * 0.786,
                                 legL + rng * 0.618, False))
        if len(otes) > 12:
            otes[:1] = []

        # ---- FVG with displacement --------------------------------------
        if i >= 2:
            mid = abs(closes[i - 1] - opens[i - 1])
            disp = atr_prev > 0 and mid >= atr_prev * p["fvg_min_atr"]
            if disp and closes[i - 1] > opens[i - 1] and lows[i] > highs[i - 2]:
                fvgs.append(_FVG(i, lows[i], highs[i - 2], True))
            if disp and closes[i - 1] < opens[i - 1] and highs[i] < lows[i - 2]:
                fvgs.append(_FVG(i, lows[i - 2], highs[i], False))
        for f in list(fvgs):
            if not f.mitigated:
                if f.isBullish and c < f.bottom:
                    f.mitigated = True
                elif not f.isBullish and c > f.top:
                    f.mitigated = True
        if len(fvgs) > 30:
            fvgs[:10] = []

        # ---- order blocks on fresh pivots --------------------------------
        for sw in swings[-2:]:
            if sw.idx != i - L:
                continue
            off = i - sw.idx
            if not (0 < off < 500):
                continue
            if not sw.isHigh:
                obH, obL = highs[sw.idx], lows[sw.idx]
                bear_candle = closes[sw.idx] < opens[sw.idx]
                swept = any(not s.isHigh and s.idx < sw.idx and obL < s.price
                            for s in swings[-10:])
                disp2 = False
                if off >= 3:
                    for jj in range(0, min(off - 2, 5) + 1):
                        if lows[sw.idx + jj + 2] > highs[sw.idx + jj]:
                            disp2 = True
                            break
                eq = (lastH + lastL) / 2 if (lastH and lastL) else None
                disc = eq is not None and obL < eq
                ok_sweep = (not p["require_sweep"]) or swept
                ok_disp = (not p["require_disp"]) or disp2
                if bear_candle and ok_sweep and ok_disp:
                    score = (2 * swept + 2 * disp2 + (1 if in_killzone(ts) else 0)
                             + (1 if disc else 0)
                             + (2 if (not p["require_htf"] or htf.bull) else 0))
                    if score >= p["min_score"]:
                        obs.append(_OB(sw.idx, obH, obL, True, score))
            else:
                obH, obL = highs[sw.idx], lows[sw.idx]
                bull_candle = closes[sw.idx] > opens[sw.idx]
                swept = any(s.isHigh and s.idx < sw.idx and obH > s.price
                            for s in swings[-10:])
                disp2 = False
                if off >= 3:
                    for jj in range(0, min(off - 2, 5) + 1):
                        if highs[sw.idx + jj + 2] < lows[sw.idx + jj]:
                            disp2 = True
                            break
                eq = (lastH + lastL) / 2 if (lastH and lastL) else None
                prem = eq is not None and obH > eq
                ok_sweep = (not p["require_sweep"]) or swept
                ok_disp = (not p["require_disp"]) or disp2
                if bull_candle and ok_sweep and ok_disp:
                    score = (2 * swept + 2 * disp2 + (1 if in_killzone(ts) else 0)
                             + (1 if prem else 0)
                             + (2 if (not p["require_htf"] or not htf.bull) else 0))
                    if score >= p["min_score"]:
                        obs.append(_OB(sw.idx, obH, obL, False, score))
        if len(obs) > 30:
            obs[:10] = []

        # ---- OB mitigation -> breakers ------------------------------------
        for ob in obs:
            if not ob.mitigated:
                if ob.isBullish and c < ob.bottom:
                    ob.mitigated = True
                    brks.append(_Breaker(i, ob.top, ob.bottom, False))
                elif not ob.isBullish and c > ob.top:
                    ob.mitigated = True
                    brks.append(_Breaker(i, ob.top, ob.bottom, True))
        for bk in brks:
            if not bk.mitigated:
                touch = h >= bk.bottom and l <= bk.top
                if touch and not bk.retested and i > bk.created:
                    bk.retested = True
                elif bk.retested:
                    if bk.isBullish and c < bk.bottom:
                        bk.mitigated = True
                    elif not bk.isBullish and c > bk.top:
                        bk.mitigated = True
        if len(brks) > 20:
            brks[:8] = []
        for z in otes:
            if not z.invalidated:
                if z.isBullish and c < z.legLow:
                    z.invalidated = True
                elif not z.isBullish and c > z.legHigh:
                    z.invalidated = True

        if i < warm:
            continue

        # ---- CISD ---------------------------------------------------------
        if c < o:
            lastBearOpen = o
        if c > o:
            lastBullOpen = o
        bull_cisd = lastBearOpen is not None and c > lastBearOpen
        bear_cisd = lastBullOpen is not None and c < lastBullOpen
        kz = in_killzone(ts)
        eq = (lastH + lastL) / 2 if (lastH and lastL) else None

        # ---- LONG ----------------------------------------------------------
        def zone_touch_long():
            sl = None
            sc = 0
            for ob in reversed(obs[-10:]):
                if not ob.mitigated and ob.isBullish and l <= ob.top and c > ob.bottom:
                    sc += 2
                    sl = ob.bottom if sl is None else min(sl, ob.bottom)
                    break
            for f in reversed(fvgs[-10:]):
                if not f.mitigated and f.isBullish and l <= f.top and c > f.bottom:
                    sc += 1
                    if sl is None:
                        sl = f.bottom
                    break
            for z in reversed(otes[-5:]):
                if not z.invalidated and z.isBullish and l <= z.zoneTop and c > z.zoneBottom:
                    sc += 2
                    if sl is None:
                        sl = z.legLow
                    break
            for bk in reversed(brks[-5:]):
                if not bk.mitigated and bk.isBullish and not bk.retested and l <= bk.top and c > bk.bottom:
                    sc += 1
                    if sl is None:
                        sl = bk.bottom
                    break
            return sc, sl

        def zone_touch_short():
            sl = None
            sc = 0
            for ob in reversed(obs[-10:]):
                if not ob.mitigated and not ob.isBullish and h >= ob.bottom and c < ob.top:
                    sc += 2
                    sl = ob.top if sl is None else max(sl, ob.top)
                    break
            for f in reversed(fvgs[-10:]):
                if not f.mitigated and not f.isBullish and h >= f.bottom and c < f.top:
                    sc += 1
                    if sl is None:
                        sl = f.top
                    break
            for z in reversed(otes[-5:]):
                if not z.invalidated and not z.isBullish and h >= z.zoneBottom and c < z.zoneTop:
                    sc += 2
                    if sl is None:
                        sl = z.legHigh
                    break
            for bk in reversed(brks[-5:]):
                if not bk.mitigated and not bk.isBullish and not bk.retested and h >= bk.bottom and c < bk.top:
                    sc += 1
                    if sl is None:
                        sl = bk.top
                    break
            return sc, sl

        # cooldown check first
        long_cd = lastLongBar is None or (i - lastLongBar) >= p["cooldown"]
        short_cd = lastShortBar is None or (i - lastShortBar) >= p["cooldown"]
        htf_bull = htf.bull

        sig = None
        sc_l, sl_l = zone_touch_long()
        if sig is None and long_cd and sc_l > 0:
            score = sc_l
            if (not p["require_htf"]) or htf_bull:
                score += 1
            if kz:
                score += 1
            if eq is not None and c < eq:
                score += 1
            if bull:
                score += 1
            if bull_cisd:
                score += 1
            if score >= p["min_sig_score"] and sl_l is not None and sl_l < c:
                tp = None
                for s in reversed(swings[-20:]):
                    if s.isHigh and s.price > c and (tp is None or s.price < tp):
                        tp = s.price
                if tp is not None and tp > c:
                    sig = Signal(side="buy", strategy="Ict_Smc",
                                 reason=f"ICT long score={score}", entry=c,
                                 sl=sl_l, tp1=None, tp=tp, rsi=50.0,
                                 atr=(atr_l[i] or c * 0.01), htf="bullish")
                    lastLongBar = i
        sc_s, sl_s = zone_touch_short()
        if sig is None and short_cd and sc_s > 0:
            score = sc_s
            if (not p["require_htf"]) or not htf_bull:
                score += 1
            if kz:
                score += 1
            if eq is not None and c > eq:
                score += 1
            if not bull:
                score += 1
            if bear_cisd:
                score += 1
            if score >= p["min_sig_score"] and sl_s is not None and sl_s > c:
                tp = None
                for s in reversed(swings[-20:]):
                    if not s.isHigh and s.price < c and (tp is None or s.price > tp):
                        tp = s.price
                if tp is not None and tp < c:
                    sig = Signal(side="sell", strategy="Ict_Smc",
                                 reason=f"ICT short score={score}", entry=c,
                                 sl=sl_s, tp1=None, tp=tp, rsi=50.0,
                                 atr=(atr_l[i] or c * 0.01), htf="bearish")
                    lastShortBar = i
        if sig is not None:
            sigs[i] = sig
    return sigs


# ------------------------------------------------------------------
def combo_signals(bars, sig_ict, sig_imba):
    """First signal per bar wins; ICT checked first (priority trial)."""
    out = {}
    for sym in bars:
        si, sm = sig_ict.get(sym, {}), sig_imba.get(sym, {})
        for i in range(max(len(si), len(sm)) if (si or sm) else 0):
            if i in si:
                out.setdefault(sym, {})[i] = si[i]
            elif i in sm:
                out.setdefault(sym, {})[i] = sm[i]
    return out


def main():
    stage = sys.argv[1] if len(sys.argv) > 1 else "cal"
    res = {}
    barsA = {s: load(DATA_A, s) for s in SYMBOLS}
    barsB = {s: load(DATA_B, s) for s in SYMBOLS}
    if stage in ("cal", "all"):
        for tag, tp4, expA, expB in (
                ("imba_tp4=4", 4, 32.17, -5.23),):
            sa = {s: imba_signals(barsA[s], tp4) for s in SYMBOLS}
            sb = {s: imba_signals(barsB[s], tp4) for s in SYMBOLS}
            ra = _walk(barsA, sa, "A")
            rb = _walk(barsB, sb, "B")
            print(f"[cal] {tag}: A net={ra['net']} (n={ra['n']}, pf={ra['pf']}) "
                  f"vs {expA} | B net={rb['net']} (n={rb['n']}, pf={rb['pf']}) vs {expB}")
            res[f"cal_{tag}"] = {"A": {k: v for k, v in ra.items() if k != 'trades'},
                                 "B": {k: v for k, v in rb.items() if k != 'trades'}}
        sa = {s: imba_signals(barsA[s], 6) for s in SYMBOLS}
        sb = {s: imba_signals(barsB[s], 6) for s in SYMBOLS}
        ra = _walk(barsA, sa, "A"); rb = _walk(barsB, sb, "B")
        print(f"[cal] tp4=6: A net={ra['net']} (n={ra['n']}) vs 11.63 | "
              f"B net={rb['net']} (n={rb['n']}) vs 50.79")
        res["cal_tp4=6"] = {"A": {k: v for k, v in ra.items() if k != 'trades'},
                            "B": {k: v for k, v in rb.items() if k != 'trades'}}
    if stage in ("ict", "all"):
        t0 = time.time()
        sa = {s: ict_signals(barsA[s]) for s in SYMBOLS}
        sb = {s: ict_signals(barsB[s]) for s in SYMBOLS}
        ra = _walk(barsA, sa, "A"); rb = _walk(barsB, sb, "B")
        print(f"[ict solo] A: n={ra['n']} wr={ra['wr']} pf={ra['pf']} "
              f"net={ra['net']} dd={ra['max_dd']} exits={ra['exits']}")
        print(f"[ict solo] B: n={rb['n']} wr={rb['wr']} pf={rb['pf']} "
              f"net={rb['net']} dd={rb['max_dd']} exits={rb['exits']}")
        res["ict_solo"] = {"A": {k: v for k, v in ra.items() if k != 'trades'},
                           "B": {k: v for k, v in rb.items() if k != 'trades'}}
        # combined with shipped imba tp4=6
        ia = {s: imba_signals(barsA[s], 6) for s in SYMBOLS}
        ib = {s: imba_signals(barsB[s], 6) for s in SYMBOLS}
        ca = combo_signals(barsA, sa, ia); cb = combo_signals(barsB, sb, ib)
        rca = _walk(barsA, ca, "A"); rcb = _walk(barsB, cb, "B")
        print(f"[ict+imba] A: n={rca['n']} net={rca['net']} pf={rca['pf']} dd={rca['max_dd']}")
        print(f"[ict+imba] B: n={rcb['n']} net={rcb['net']} pf={rcb['pf']} dd={rcb['max_dd']}")
        n_ict_a = sum(1 for t in rca['trades'] if t['strat'] == 'Ict_Smc')
        n_ict_b = sum(1 for t in rcb['trades'] if t['strat'] == 'Ict_Smc')
        pnl_ict_a = round(sum(t['net'] for t in rca['trades'] if t['strat'] == 'Ict_Smc'), 2)
        pnl_ict_b = round(sum(t['net'] for t in rcb['trades'] if t['strat'] == 'Ict_Smc'), 2)
        print(f"[ict+imba] ICT contribution: A n={n_ict_a} pnl={pnl_ict_a} | "
              f"B n={n_ict_b} pnl={pnl_ict_b}")
        res["ict_combo"] = {"A": {k: v for k, v in rca.items() if k != 'trades'},
                            "B": {k: v for k, v in rcb.items() if k != 'trades'},
                            "ict_contrib": {"A": {"n": n_ict_a, "pnl": pnl_ict_a},
                                            "B": {"n": n_ict_b, "pnl": pnl_ict_b}}}
        print(f"(elapsed {time.time()-t0:.0f}s)")
    out = os.path.join(ROOT, "analysis", "runs", "ict_v237_validation.json")
    if os.path.exists(out):
        old = json.load(open(out))
        old.update(res)
        res = old
    with open(out, "w") as fh:
        json.dump(res, fh, indent=1)
    print("saved ->", out)


if __name__ == "__main__":
    main()
