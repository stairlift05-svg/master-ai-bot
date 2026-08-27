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
# Registry
# ---------------------------------------------------------------------------
_STRATEGY_CLASSES: Dict[str, type] = {
    "TrendPullback_HTF": TrendPullbackHTF,
    "HTF_Breakout": HTFBreakout,
    "MomentumRetrace_RSI": MomentumRetraceRSI,
    "MeanReversion_BB": MeanReversionBB,
    "VolatilityExpansion": VolatilityExpansion,
    "SwingPullback_1h": SwingPullback1h,
}

DEFAULT_V2_PARAMS: Dict[str, Dict[str, float]] = {
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
