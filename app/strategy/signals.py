"""Signal generation (v20.1): high-timeframe strategy family.

Rewrite of the strategy layer following the think-tank review of the real
60-day backtest (see ``analysis/THINK_TANK_REPORT.md``).  The v1 (v20.0)
strategies — 5m-only signals — were rejected by the panel because their edge
per trade (+$0.02) was smaller than their cost per trade (~$0.14), producing
a statistically significant loss in a bull market.

Design principles of v20.1:

* **Multi-timeframe confluence** — every strategy reads a 5m, 15m and 1h
  context (indicators precomputed once per scan in :class:`HtfContext`).
* **Wider risk framework** — stops/targets are anchored to the 15m/1h ATR,
  not the 5m ATR, so stop distance exceeds intraday noise.
* **Fewer, higher-quality trades** — HTF confirmation gates naturally cut
  churn (v1 averaged ~13 entries/day; the panel target is ≤ 4–6/day).
* **Regime-aware** — momentum families only trade trending HTF; mean
  reversion only trades sideways HTF.

Each family is the responsibility of one think-tank specialist agent:

    Agent A — TrendPullback_HTF    (1h trend + 15m pullback)
    Agent B — HTFBreakout          (1h Donchian breakout + trend)
    Agent C — MomentumRetrace_RSI  (1h trend + RSI(1h) healthy + 15m extreme)
    Agent D — MeanReversion_BB     (sideways 1h + 15m Bollinger touch)
    Agent E — VolatilityExpansion  (ATR expansion + range break with trend)
    Agent F — SwingPullback_1h     (pullback to 1h EMA50 in 1h trend)
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from app.models import Signal
from app.strategy import indicators as ind


# ---------------------------------------------------------------------------
# Contexts (precomputed once per scan)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class TFContext:
    """Precomputed features for one timeframe."""

    label: str
    closes: List[float]
    highs: List[float]
    lows: List[float]
    volumes: List[float]
    atr: float = 0.0
    rsi: float = 50.0
    ema20: float = 0.0
    ema50: float = 0.0
    ema200: Optional[float] = None
    hh: float = 0.0          # highest high over lookback
    ll: float = 0.0          # lowest low over lookback
    trend: str = "sideways"  # bullish / bearish / sideways
    strength: float = 0.0    # |ema50 - ema200| / ema200, percent
    mid: float = 0.0         # Bollinger mid
    bb_upper: float = 0.0
    bb_lower: float = 0.0

    @property
    def bb_width(self) -> float:
        return (self.bb_upper - self.bb_lower) / (self.mid + 1e-12) if self.mid > 0 else 0.0


@dataclass(frozen=True)
class HtfContext:
    """Everything a v2 strategy needs: three timeframes + entry price."""

    symbol: str
    price: float            # current 5m close
    tf5: TFContext
    tf15: TFContext
    tf1: TFContext
    candle_bull_5m: bool
    candle_bear_5m: bool
    min_stop_pct: float = 0.003
    # Round-trip friction as a fraction of price (2 x taker fee + 2 x
    # slippage, scaled by the fee buffer). Signals whose take-profit target
    # does not clear friction by ``min_edge_ratio`` are refused outright —
    # the single defect the think-tank blamed for every losing version:
    # edge per trade (+$0.02) smaller than cost per trade (~$0.14).
    round_trip_cost_pct: float = 0.0
    min_edge_ratio: float = 0.0


class _SignalTooSmall(Exception):
    """Raised by ``_build`` when a signal's target cannot clear trading costs.

    Caught by :meth:`BaseStrategyV2.propose`, which converts it into "no
    signal" so an unprofitable setup is simply skipped.
    """


# ---------------------------------------------------------------------------
# Base strategy
# ---------------------------------------------------------------------------
class BaseStrategyV2(ABC):
    """Interface for v20.1 strategies (multi-timeframe)."""

    name: str = "base"
    priority: int = 100

    def __init__(self, params: Dict[str, float]):
        self.params = params

    @abstractmethod
    def evaluate(self, ctx: HtfContext) -> Optional[Signal]: ...

    # -- public entry point used by the engine -------------------------
    def propose(self, ctx: HtfContext) -> Optional[Signal]:
        """Evaluate the strategy, dropping signals that cannot pay for costs.

        Strategies keep expressing their edge in :meth:`evaluate`; the cost
        gate lives here so every family inherits it consistently.
        """
        try:
            return self.evaluate(ctx)
        except _SignalTooSmall:
            return None

    # -- shared signal builder -----------------------------------------
    def _build(self, ctx: HtfContext, side: str, reason: str,
               sl_dist: float, tp_dist: float, tp1_dist: Optional[float] = None,
               confidence: float = 0.5) -> Signal:
        """Build a Signal with explicit distances.

        Distances are floored at ``min_stop_pct`` of price so the position
        sizer budgets the actual stop distance (risk/stop consistency).
        """
        price = ctx.price
        floor = price * ctx.min_stop_pct
        sl_dist = max(sl_dist, floor)
        tp_dist = max(tp_dist, sl_dist * 1.5)
        if tp1_dist is None:
            tp1_dist = sl_dist * 1.2
        tp1_dist = max(tp1_dist, sl_dist * 1.1)
        # ---- Cost gate ------------------------------------------------
        # A target that barely clears fees + slippage is a negative-expectancy
        # trade no matter how good the entry looks. Widen the target to the
        # profitable minimum; if the strategy's own risk model cannot support
        # that target (it would need a > 2x stretch), refuse the signal.
        cost_dist = price * ctx.round_trip_cost_pct
        if cost_dist > 0 and ctx.min_edge_ratio > 0:
            required = cost_dist * ctx.min_edge_ratio
            if tp_dist < required:
                if required > tp_dist * 2.0:
                    raise _SignalTooSmall(
                        f"target {tp_dist / price * 100:.2f}% below "
                        f"cost floor {required / price * 100:.2f}%"
                    )
                tp_dist = required
            tp1_dist = max(tp1_dist, cost_dist * 1.5)
        if side == "buy":
            return Signal(
                side="buy", strategy=self.name, reason=reason, entry=price,
                sl=price - sl_dist, tp1=price + tp1_dist, tp=price + tp_dist,
                rsi=ctx.tf15.rsi, atr=ctx.tf15.atr, htf=ctx.tf1.trend,
                confidence=confidence,
            )
        return Signal(
            side="sell", strategy=self.name, reason=reason, entry=price,
            sl=price + sl_dist, tp1=price - tp1_dist, tp=price - tp_dist,
            rsi=ctx.tf15.rsi, atr=ctx.tf15.atr, htf=ctx.tf1.trend,
            confidence=confidence,
        )


# ---------------------------------------------------------------------------
# Agent A — TrendPullback_HTF
# ---------------------------------------------------------------------------
class TrendPullbackHTF(BaseStrategyV2):
    """1h trend + 15m pullback to the 15m EMA20 that bounces back up."""

    name = "TrendPullback_HTF"
    priority = 1

    def evaluate(self, ctx: HtfContext) -> Optional[Signal]:
        p = self.params
        t1, t15 = ctx.tf1, ctx.tf15
        trend_min = p.get("trend_min", 0.03)
        if t1.trend == "bullish" and t1.strength >= trend_min:
            # Pullback: 15m dips toward EMA20, then a bullish 5m/15m close.
            pullback = t15.lows[-1] <= t15.ema20 * 1.02
            bounce = ctx.candle_bull_5m and t15.closes[-1] > t15.ema20 * 0.995
            if pullback and bounce and ctx.price > t1.ema20 * 0.98 \
                    and 28 < t15.rsi < 62:
                return self._build(
                    ctx, "buy", "1h trend + 15m EMA20 pullback bounce",
                    sl_dist=t15.atr * p.get("sl_m", 2.0),
                    tp_dist=t1.atr * p.get("tp_m", 3.0),
                    confidence=0.6,
                )
        if t1.trend == "bearish" and t1.strength >= trend_min:
            pullback = t15.highs[-1] >= t15.ema20 * 0.98
            bounce = ctx.candle_bear_5m and t15.closes[-1] < t15.ema20 * 1.005
            if pullback and bounce and ctx.price < t1.ema20 * 1.02 \
                    and 38 < t15.rsi < 72:
                return self._build(
                    ctx, "sell", "1h downtrend + 15m EMA20 pullback rejection",
                    sl_dist=t15.atr * p.get("sl_m", 2.0),
                    tp_dist=t1.atr * p.get("tp_m", 3.0),
                    confidence=0.6,
                )
        return None


# ---------------------------------------------------------------------------
# Agent B — HTFBreakout
# ---------------------------------------------------------------------------
class HTFBreakout(BaseStrategyV2):
    """1h Donchian breakout in the direction of the 1h trend.

    v20.3.1: کمی نرم‌تر — نزدیک سقف/کف (۰.۲٪) هم پذیرفته می‌شود تا
    سیگنال در بازارهای واقعی از بین نرود.
    """

    name = "HTF_Breakout"
    priority = 2

    def evaluate(self, ctx: HtfContext) -> Optional[Signal]:
        p = self.params
        t1, t15 = ctx.tf1, ctx.tf15
        trend_min = p.get("trend_min", 0.02)
        tol = p.get("break_tol", 0.002)  # 0.2%
        if t1.trend == "bullish" and t1.strength >= trend_min:
            near_1h_high = t1.closes[-1] >= t1.hh * (1.0 - tol)
            near_15_high = t15.closes[-1] >= t15.hh * (1.0 - tol)
            if near_1h_high and near_15_high and ctx.price > t1.ema20 \
                    and ctx.candle_bull_5m:
                return self._build(
                    ctx, "buy", "1h Donchian breakout (bullish)",
                    sl_dist=max(t1.atr * p.get("sl_m", 1.5), t15.atr * 2.0),
                    tp_dist=t1.atr * p.get("tp_m", 3.0),
                    confidence=0.65,
                )
        if t1.trend == "bearish" and t1.strength >= trend_min:
            near_1h_low = t1.closes[-1] <= t1.ll * (1.0 + tol)
            near_15_low = t15.closes[-1] <= t15.ll * (1.0 + tol)
            if near_1h_low and near_15_low and ctx.price < t1.ema20 \
                    and ctx.candle_bear_5m:
                return self._build(
                    ctx, "sell", "1h Donchian breakdown (bearish)",
                    sl_dist=max(t1.atr * p.get("sl_m", 1.5), t15.atr * 2.0),
                    tp_dist=t1.atr * p.get("tp_m", 3.0),
                    confidence=0.65,
                )
        return None


# ---------------------------------------------------------------------------
# Agent C — MomentumRetrace_RSI
# ---------------------------------------------------------------------------
class MomentumRetraceRSI(BaseStrategyV2):
    """1h trend, 1h RSI in the healthy mid-zone, 15m RSI over-extended."""

    name = "MomentumRetrace_RSI"
    priority = 3

    def evaluate(self, ctx: HtfContext) -> Optional[Signal]:
        p = self.params
        t1, t15 = ctx.tf1, ctx.tf15
        trend_min = p.get("trend_min", 0.05)
        if t1.trend == "bullish" and t1.strength >= trend_min \
                and 40 <= t1.rsi <= 70 and t15.rsi < p.get("rsi15_low", 45) \
                and ctx.candle_bull_5m and ctx.price >= t15.ema20 * 0.995:
            return self._build(
                ctx, "buy", "1h mid-RSI trend + 15m oversold retrace",
                sl_dist=t15.atr * p.get("sl_m", 2.0),
                tp_dist=t1.atr * p.get("tp_m", 2.2),
                confidence=0.6,
            )
        if t1.trend == "bearish" and t1.strength >= trend_min \
                and 30 <= t1.rsi <= 60 and t15.rsi > p.get("rsi15_high", 55) \
                and ctx.candle_bear_5m and ctx.price <= t15.ema20 * 1.005:
            return self._build(
                ctx, "sell", "1h mid-RSI downtrend + 15m overbought retrace",
                sl_dist=t15.atr * p.get("sl_m", 2.0),
                tp_dist=t1.atr * p.get("tp_m", 2.2),
                confidence=0.6,
            )
        return None


# ---------------------------------------------------------------------------
# Agent D — MeanReversion_BB
# ---------------------------------------------------------------------------
class MeanReversionBB(BaseStrategyV2):
    """15m Bollinger touch + RSI extreme, ONLY when the 1h is sideways."""

    name = "MeanReversion_BB"
    priority = 4

    def evaluate(self, ctx: HtfContext) -> Optional[Signal]:
        p = self.params
        t1, t15 = ctx.tf1, ctx.tf15
        if t1.strength > p.get("max_trend", 0.12):
            return None  # only in weak/range-bound regimes
        if t15.bb_lower <= 0 or t15.bb_width > p.get("max_bbw", 0.08):
            return None
        if t15.closes[-1] <= t15.bb_lower * 1.001 and t15.rsi < p.get("rsi_low", 35) \
                and ctx.candle_bull_5m:
            return self._build(
                ctx, "buy", "15m lower-Bollinger touch (range)",
                sl_dist=t15.atr * p.get("sl_m", 1.0),
                tp_dist=max(t15.mid - t15.closes[-1], t15.atr * 0.8),
                confidence=0.45,
            )
        if t15.closes[-1] >= t15.bb_upper * 0.999 and t15.rsi > p.get("rsi_high", 65) \
                and ctx.candle_bear_5m:
            return self._build(
                ctx, "sell", "15m upper-Bollinger touch (range)",
                sl_dist=t15.atr * p.get("sl_m", 1.0),
                tp_dist=max(t15.closes[-1] - t15.mid, t15.atr * 0.8),
                confidence=0.45,
            )
        return None


# ---------------------------------------------------------------------------
# Agent E — VolatilityExpansion
# ---------------------------------------------------------------------------
class VolatilityExpansion(BaseStrategyV2):
    """ATR expansion on 15m + range break, in the direction of the 1h trend."""

    name = "VolatilityExpansion"
    priority = 5

    def evaluate(self, ctx: HtfContext) -> Optional[Signal]:
        p = self.params
        t1, t15 = ctx.tf1, ctx.tf15
        trend_min = p.get("trend_min", 0.05)
        atr_mult = p.get("atr_mult", 1.5)
        # 15m ATR vs its own 50-bar average (expansion).
        atr_series = ind.atr_wilder(t15.highs, t15.lows, t15.closes, 14)
        atr_avg = ind.sma([a or 0.0 for a in atr_series], 50)
        expanded = atr_avg[-1] > 0 and t15.atr > atr_avg[-1] * atr_mult
        if not expanded:
            return None
        vol_surge = t15.volumes[-1] > (ind.sma(t15.volumes, 20)[-1] or 1e-9) * p.get("vol_mult", 1.3)
        if t1.trend == "bullish" and t1.strength >= trend_min \
                and t15.closes[-1] > t15.hh * 0.999 and vol_surge:
            return self._build(
                ctx, "buy", "15m volatility expansion breakout (bullish)",
                sl_dist=t15.atr * p.get("sl_m", 1.8),
                tp_dist=t1.atr * p.get("tp_m", 2.5),
                confidence=0.6,
            )
        if t1.trend == "bearish" and t1.strength >= trend_min \
                and t15.closes[-1] < t15.ll * 1.001 and vol_surge:
            return self._build(
                ctx, "sell", "15m volatility expansion breakdown (bearish)",
                sl_dist=t15.atr * p.get("sl_m", 1.8),
                tp_dist=t1.atr * p.get("tp_m", 2.5),
                confidence=0.6,
            )
        return None


# ---------------------------------------------------------------------------
# Agent F — SwingPullback_1h
# ---------------------------------------------------------------------------
class SwingPullback1h(BaseStrategyV2):
    """1h trend with pullback to the 1h EMA50 that resumes."""

    name = "SwingPullback_1h"
    priority = 6

    def evaluate(self, ctx: HtfContext) -> Optional[Signal]:
        p = self.params
        t1, t15 = ctx.tf1, ctx.tf15
        trend_min = p.get("trend_min", 0.05)
        if t1.trend == "bullish" and t1.strength >= trend_min and t1.ema50 > 0:
            near_ema = t1.lows[-1] <= t1.ema50 * 1.02 and t1.closes[-1] >= t1.ema50 * 0.995
            resume = ctx.candle_bull_5m and t15.closes[-1] > t15.ema20
            if near_ema and resume:
                return self._build(
                    ctx, "buy", "1h EMA50 pullback resume (swing)",
                    sl_dist=t1.atr * p.get("sl_m", 1.2),
                    tp_dist=t1.atr * p.get("tp_m", 3.0),
                    confidence=0.6,
                )
        if t1.trend == "bearish" and t1.strength >= trend_min and t1.ema50 > 0:
            near_ema = t1.highs[-1] >= t1.ema50 * 0.98 and t1.closes[-1] <= t1.ema50 * 1.005
            resume = ctx.candle_bear_5m and t15.closes[-1] < t15.ema20
            if near_ema and resume:
                return self._build(
                    ctx, "sell", "1h EMA50 pullback resume (swing)",
                    sl_dist=t1.atr * p.get("sl_m", 1.2),
                    tp_dist=t1.atr * p.get("tp_m", 3.0),
                    confidence=0.6,
                )
        return None


# ---------------------------------------------------------------------------
# Agent G — DonchianTrend (v20.6)
# ---------------------------------------------------------------------------
class DonchianTrend(BaseStrategyV2):
    """Classic symmetric channel breakout — the only design that survived
    out-of-sample testing on 14 months of real Binance data.

    Why this family, when the other six all failed:

    * **Symmetric.** Every previous family was long-biased in practice (the
      shipped config was even ``SIDES=long``). The 2024-03 -> 2025-04 sample
      splits cleanly into a bull first half and a bear second half, and every
      long-only variant that looked good on the first half lost badly on the
      second. This one takes shorts on the same terms as longs.
    * **Few trades, wide targets.** Costs are the binding constraint
      (see analysis/AUDIT_v20.5.md). A 60-bar channel on 1h data fires rarely,
      and the exit is a trailing channel rather than a fixed target, so the
      winners are allowed to become much larger than the round-trip cost.
    * **No fitted oscillator thresholds.** The only parameters are two
      lookbacks and an ATR stop multiple, which is what makes it hold up
      out-of-sample instead of memorising the training half.

    Entry: close breaks the ``entry_len``-bar extreme *and* agrees with the
    long-term regime filter (EMA200 slope on the trading timeframe).
    Stop: ``sl_m`` x ATR. Target: deliberately far (``tp_m`` x ATR) — the real
    exit is the trailing stop managed by the watchdog.
    """

    name = "Donchian_Trend"
    priority = 0

    def evaluate(self, ctx: HtfContext) -> Optional[Signal]:
        p = self.params
        t5, t1 = ctx.tf5, ctx.tf1
        entry_len = int(p.get("entry_len", 60))
        atr = t5.atr
        if atr <= 0 or len(t5.closes) < entry_len + 5:
            return None

        highs, lows, closes = t5.highs, t5.lows, t5.closes
        prior_h = highs[-entry_len - 1:-1]
        prior_l = lows[-entry_len - 1:-1]
        if not prior_h or not prior_l:
            return None
        hh, ll = max(prior_h), min(prior_l)
        price = closes[-1]

        # Regime filter: only take breakouts that agree with the slow trend.
        ema_slow = t5.ema200 if t5.ema200 else t5.ema50
        if not ema_slow or ema_slow <= 0:
            return None
        slope_ok_up = price > ema_slow
        slope_ok_dn = price < ema_slow

        sl_dist = atr * p.get("sl_m", 2.5)
        tp_dist = atr * p.get("tp_m", 12.0)

        # Breakout quality filter. A close that only just pokes through the
        # channel is usually noise: those produced most of the losing trades
        # in testing (58 stop-outs vs 7 targets before this filter). Require
        # the break to clear the channel edge by a fraction of ATR.
        margin = atr * p.get("break_atr", 0.0)

        # N-04 (optional, default OFF = exact validated v21 behaviour):
        # long-side distance gate. In BOTH v21 validation windows the long
        # book barely earned (window A: +$4.01) or lost (window B: -$8.48,
        # second half -$27.54) while shorts carried the strategy (+$66.5 /
        # +$75.1). The bleeding longs were breakouts hugging a flat/slow
        # EMA200. When long_dist_atr > 0, a long must additionally clear the
        # slow EMA by that many ATR before it is taken. Shorts are untouched.
        long_gate = atr * p.get("long_dist_atr", 0.0)

        if (price > hh + margin and slope_ok_up
                and (price - ema_slow) >= long_gate):
            return self._build(
                ctx, "buy", f"{entry_len}-bar channel breakout (trend up)",
                sl_dist=sl_dist, tp_dist=tp_dist, confidence=0.7,
            )
        if price < ll - margin and slope_ok_dn:
            return self._build(
                ctx, "sell", f"{entry_len}-bar channel breakdown (trend down)",
                sl_dist=sl_dist, tp_dist=tp_dist, confidence=0.7,
            )
        return None


# ---------------------------------------------------------------------------
# Agent H — ImbaFib (v23, owner directive 2026-08-29)
# ---------------------------------------------------------------------------
class ImbaFib(BaseStrategyV2):
    """IMBA ALGO final stack — faithful port of the supplied Python module.

    Entry (long): close in the top band of the ``sensitivity*10``-bar
    fibonacci channel (>= fib236, which implies >= fib50) + STACK filters
    (close above EMA200 and RSI < rsi_long_guard).
    Entry (short): symmetric bottom band (<= fib786) + close below EMA200
    and RSI > rsi_short_guard.
    Stop: the far channel edge (fib786 long / fib236 short) — the module's
    fibonacci stop, NOT an ATR stop.
    Targets: fixed ladder 1/2/3/4% (tp1/tp2/tp3/tp4); the engine books the
    final target at tp4 and shows tp1. Break-even after TP1 and the 10/10/
    10/70 scale-out ladder of the original are approximated by the engine's
    existing BE move (min_profit_be_pct) — the live executor has a single
    partial, so the ladder is documented rather than replicated.

    The Signal is built directly (not via _build) so the module's fixed
    percent targets survive: _build forces tp >= 1.5 x stop, which would
    silently rewrite IMBA's 4% runner into a 1.5x-fib-stop target. The
    shared cost gate is applied manually below.
    """

    name = "Imba_Fib"
    priority = 0

    def evaluate(self, ctx: HtfContext) -> Optional[Signal]:
        p = self.params
        t5 = ctx.tf5
        look = max(1, int(p.get("sensitivity", 18)) * 10)
        closes, highs, lows = t5.closes, t5.highs, t5.lows
        if len(closes) < max(look + 1, 205):
            return None
        price = closes[-1]
        hh, ll = max(highs[-look:]), min(lows[-look:])
        rng = hh - ll
        if rng <= 0 or price <= 0:
            return None
        fib236 = hh - rng * 0.236
        fib50 = hh - rng * 0.50
        fib786 = hh - rng * 0.786

        use_filters = p.get("use_filters", 1) > 0
        if use_filters:
            ema_slow = t5.ema200 if t5.ema200 else t5.ema50
            if not ema_slow or ema_slow <= 0:
                return None
            long_ok = (price > ema_slow
                       and t5.rsi < p.get("rsi_long_guard", 72.0))
            short_ok = (price < ema_slow
                        and t5.rsi > p.get("rsi_short_guard", 28.0))
        else:
            long_ok = short_ok = True

        # ---- v23.2 candidate quality filters (default OFF = module-exact) ----
        # break_margin_atr: the close must clear the OUTER band edge by this
        #   many ATR — rejects marginal pokes (the same lesson that turned
        #   Donchian_Trend profitable: break_atr was its #1 parameter).
        # min_ema_dist_atr: price must stand at least this many ATR away from
        #   the EMA200 in the trade direction — no entries hugging the mean.
        # htf_align: the HTF (4h) trend must agree with the trade direction.
        margin = t5.atr * p.get("break_margin_atr", 0.0)
        min_dist = t5.atr * p.get("min_ema_dist_atr", 0.0)
        if p.get("htf_align", 0) > 0:
            if ctx.tf1.trend == "bullish":
                htf_long_ok, htf_short_ok = True, False
            elif ctx.tf1.trend == "bearish":
                htf_long_ok, htf_short_ok = False, True
            else:
                htf_long_ok = htf_short_ok = False
        else:
            htf_long_ok = htf_short_ok = True
        if use_filters and min_dist > 0 and ema_slow:
            long_ok = long_ok and (price - ema_slow) >= min_dist
            short_ok = short_ok and (ema_slow - price) >= min_dist

        tp_mults = [p.get("tp1", 1.0), p.get("tp2", 2.0),
                    p.get("tp3", 3.0), p.get("tp4", 4.0)]
        floor = price * ctx.min_stop_pct

        def _mk(side: str, sl_raw: float, reason: str) -> Signal:
            sign = 1.0 if side == "buy" else -1.0
            sl_dist = max(abs(price - sl_raw), floor)
            tp_dist = price * tp_mults[3] / 100.0
            tp1_dist = price * tp_mults[0] / 100.0
            # shared cost gate (same rule as BaseStrategyV2._build)
            cost_dist = price * ctx.round_trip_cost_pct
            if cost_dist > 0 and ctx.min_edge_ratio > 0:
                required = cost_dist * ctx.min_edge_ratio
                if required > tp_dist * 2.0:
                    raise _SignalTooSmall(
                        f"target {tp_dist / price * 100:.2f}% below "
                        f"cost floor {required / price * 100:.2f}%")
                tp_dist = max(tp_dist, required)
                tp1_dist = max(tp1_dist, cost_dist * 1.5)
            sl = price - sl_dist if side == "buy" else price + sl_dist
            return Signal(
                side=side, strategy=self.name, reason=reason, entry=price,
                sl=sl, tp1=price + sign * tp1_dist, tp=price + sign * tp_dist,
                rsi=t5.rsi, atr=t5.atr, htf=ctx.tf1.label, confidence=0.6,
            )

        if (long_ok and htf_long_ok and price >= fib50
                and price >= fib236 + margin):
            return _mk("buy", fib786,
                       f"IMBA: top band (>=fib236) + EMA200 up + RSI {t5.rsi:.0f}")
        if (short_ok and htf_short_ok and price <= fib50
                and price <= fib786 - margin):
            return _mk("sell", fib236,
                       f"IMBA: bottom band (<=fib786) + EMA200 dn + RSI {t5.rsi:.0f}")
        return None


# ---------------------------------------------------------------------------
# Agent I — EmaCross_Trend (v23.3, owner addition 2026-08-30)
# ---------------------------------------------------------------------------
def _ema_series(vals: List[float], period: int) -> List[float]:
    """Full EMA series (Wilder-style seeding with the SMA of the first period)."""
    if len(vals) < period:
        return []
    k = 2.0 / (period + 1.0)
    out = [sum(vals[:period]) / period]
    for v in vals[period:]:
        out.append(v * k + out[-1] * (1.0 - k))
    return out


def _adx(h: List[float], l: List[float], c: List[float],
         di_len: int = 14, adx_len: int = 14) -> Optional[float]:
    """Wilder ADX — equivalent of Pine ta.dmi(14, 14) > threshold."""
    n = len(c)
    if n < di_len * 2 + adx_len + 1:
        return None
    trs, pdms, ndms = [], [], []
    for i in range(1, n):
        up, dn = h[i] - h[i - 1], l[i - 1] - l[i]
        pdm = up if (up > dn and up > 0) else 0.0
        ndm = dn if (dn > up and dn > 0) else 0.0
        trs.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))
        pdms.append(pdm)
        ndms.append(ndm)
    atr = float(sum(trs[:di_len]))
    pdm = float(sum(pdms[:di_len]))
    ndm = float(sum(ndms[:di_len]))
    dxs: List[float] = []
    for i in range(di_len, len(trs)):
        atr += trs[i] - trs[i - di_len] if i >= di_len else 0.0
        pdm += pdms[i] - pdms[i - di_len]
        ndm += ndms[i] - ndms[i - di_len]
        pdi = 100.0 * pdm / atr if atr > 0 else 0.0
        ndi = 100.0 * ndm / atr if atr > 0 else 0.0
        dxs.append(100.0 * abs(pdi - ndi) / (pdi + ndi) if pdi + ndi > 0 else 0.0)
    if len(dxs) < adx_len:
        return None
    adx = sum(dxs[:adx_len]) / adx_len
    for dx in dxs[adx_len:]:
        adx = (adx * (adx_len - 1) + dx) / adx_len
    return adx


class EmaCrossTrend(BaseStrategyV2):
    """The owner-supplied Pine 'golden strategy' — faithful port, 4H only.

    Logic (all on the HTF/4h slot, exactly like the Pine running on a 4H
    chart): EMA9/EMA21 crossover + close vs EMA200 regime + ADX(14) > 25.
    Exits: fixed 1% stop / 2% target from entry (Pine strategy.exit).
    Translation notes (documented divergences):
      * Pine reverses on the opposite cross; this engine keeps ONE position
        per symbol and exits on SL/TP — with a 1%/2% bracket the position
        is almost always resolved long before the next cross.
      * Pine sizes 2% of equity; the engine keeps its risk-based sizer
        (0.4% risk / 1% stop distance -> ~40% of free margin, capped at
        MAX_NOTIONAL_USD) — portfolio-consistent.
    """

    name = "EmaCross_Trend"
    priority = 10

    def evaluate(self, ctx: HtfContext) -> Optional[Signal]:
        p = self.params
        t4 = ctx.tf1  # the 4h slot (live and backtest build it identically)
        closes, highs, lows = t4.closes, t4.highs, t4.lows
        need = max(int(p.get("ema_len", 200)), int(p.get("slow_len", 21)) * 4, 60)
        if len(closes) < need + 2:
            return None
        fast = _ema_series(closes, int(p.get("fast_len", 9)))
        slow = _ema_series(closes, int(p.get("slow_len", 21)))
        ema_trend = _ema_series(closes, int(p.get("ema_len", 200)))
        if not (fast and slow and ema_trend):
            return None
        # crossover on the last CLOSED bar (series already forming-dropped)
        x_up = fast[-1] > slow[-1] and fast[-2] <= slow[-2]
        x_dn = fast[-1] < slow[-1] and fast[-2] >= slow[-2]
        price = closes[-1]
        adx = _adx(highs, lows, closes, 14, 14)
        strong = adx is not None and adx > p.get("adx_threshold", 25.0)
        if not strong:
            return None

        sl_pct = p.get("sl_pct", 1.0) / 100.0
        tp_pct = p.get("tp_pct", 2.0) / 100.0
        floor = price * ctx.min_stop_pct

        def _mk(side: str) -> Signal:
            sign = 1.0 if side == "buy" else -1.0
            sl_dist = max(price * sl_pct, floor)
            tp_dist = price * tp_pct
            cost_dist = price * ctx.round_trip_cost_pct
            if cost_dist > 0 and ctx.min_edge_ratio > 0:
                required = cost_dist * ctx.min_edge_ratio
                if required > tp_dist * 2.0:
                    raise _SignalTooSmall("2% target below cost floor")
                tp_dist = max(tp_dist, required)
            return Signal(
                side=side, strategy=self.name,
                reason=f"EMA{int(p.get('fast_len', 9))}/EMA{int(p.get('slow_len', 21))} cross + EMA200 + ADX {adx:.0f}",
                entry=price, sl=price - sign * sl_dist,
                tp1=price + sign * tp_dist, tp=price + sign * tp_dist,
                rsi=t4.rsi, atr=t4.atr, htf=t4.label, confidence=0.6,
            )

        if x_up and price > ema_trend[-1]:
            return _mk("buy")
        if x_dn and price < ema_trend[-1]:
            return _mk("sell")
        return None


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
_STRATEGY_CLASSES: Dict[str, type] = {
    "Imba_Fib": ImbaFib,
    "EmaCross_Trend": EmaCrossTrend,
    "Donchian_Trend": DonchianTrend,
    "TrendPullback_HTF": TrendPullbackHTF,
    "HTF_Breakout": HTFBreakout,
    "MomentumRetrace_RSI": MomentumRetraceRSI,
    "MeanReversion_BB": MeanReversionBB,
    "VolatilityExpansion": VolatilityExpansion,
    "SwingPullback_1h": SwingPullback1h,
}

DEFAULT_V2_PARAMS: Dict[str, Dict[str, float]] = {
    # Golden EMA-cross strategy (v23.3) — Pine defaults, untouched.
    "EmaCross_Trend": {"fast_len": 9, "slow_len": 21, "ema_len": 200,
                       "adx_threshold": 25.0, "sl_pct": 1.0, "tp_pct": 2.0},
    # IMBA ALGO final stack (v23) — module defaults, untouched.
    "Imba_Fib": {"sensitivity": 18, "use_filters": 1, "ema_len": 200,
                 "rsi_long_guard": 72.0, "rsi_short_guard": 28.0,
                 "tp1": 1.0, "tp2": 2.0, "tp3": 3.0, "tp4": 4.0,
                 # v23.2 quality filters — defaults ship OFF until a value
                 # passes the two-window rule (see STRATEGY_v23.md).
                 "break_margin_atr": 0.0, "min_ema_dist_atr": 0.0,
                 "htf_align": 0},
    # Validated plateau centre (analysis/STRATEGY_v20.6.md). break_atr is the
    # single most important parameter: it rejects marginal pokes through the
    # channel, which were the bulk of the losing trades.
    # long_dist_atr (v22): longs must clear the slow EMA by N x ATR; 1.0 is
    # the two-window sweep optimum (analysis/runs/sweep_long_gate_v22.json —
    # OOS window B improves +$4.01 net with window A unchanged; every value
    # >= 1.5 collapses the OOS window, so do NOT raise it without a new
    # two-window validation).
    "Donchian_Trend": {"entry_len": 40, "sl_m": 2.5, "tp_m": 20.0,
                       "break_atr": 1.5, "long_dist_atr": 1.0},
    "TrendPullback_HTF": {"sl_m": 2.0, "tp_m": 3.0, "trend_min": 0.03},
    "HTF_Breakout": {"sl_m": 2.0, "tp_m": 4.0, "trend_min": 0.015, "break_tol": 0.002},
    "MomentumRetrace_RSI": {"sl_m": 2.0, "tp_m": 2.2, "trend_min": 0.02,
                            "rsi15_low": 48, "rsi15_high": 52},
    "MeanReversion_BB": {"sl_m": 1.0, "rsi_low": 35, "rsi_high": 65,
                         "max_trend": 0.15, "max_bbw": 0.10},
    "VolatilityExpansion": {"sl_m": 1.8, "tp_m": 2.5, "trend_min": 0.02,
                            "atr_mult": 1.35, "vol_mult": 1.2},
    "SwingPullback_1h": {"sl_m": 1.2, "tp_m": 3.0, "trend_min": 0.03},
}


def build_strategy(name: str, params: Optional[Dict[str, float]] = None) -> BaseStrategyV2:
    """Instantiate one strategy by name with (optional) parameter overrides."""
    cls = _STRATEGY_CLASSES.get(name)
    if cls is None:
        raise KeyError(f"unknown strategy {name}")
    merged = dict(DEFAULT_V2_PARAMS.get(name, {}))
    if params:
        merged.update(params)
    return cls(merged)


def default_strategies_v2(
    strategy_params: Optional[Dict[str, Dict[str, float]]] = None,
    enabled: Optional[List[str]] = None,
) -> List[BaseStrategyV2]:
    """Build the active strategy set.

    Args:
        strategy_params: override map {name: {param: value}}.
        enabled: subset of strategy names to enable (post-screening config).
    """
    names = enabled or list(_STRATEGY_CLASSES.keys())
    out: List[BaseStrategyV2] = []
    for name in names:
        params = {}
        if strategy_params:
            params = dict(strategy_params.get(name, {}))
        out.append(build_strategy(name, params))
    out.sort(key=lambda s: s.priority)
    return out
