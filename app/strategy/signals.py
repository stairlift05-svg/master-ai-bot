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
        trend_min = p.get("trend_min", 0.05)
        if t1.trend == "bullish" and t1.strength >= trend_min:
            # Pullback: 15m dips toward EMA20, then a bullish 5m/15m close.
            pullback = t15.lows[-1] <= t15.ema20 * 1.012
            bounce = ctx.candle_bull_5m and t15.closes[-1] > t15.ema20 * 0.998
            if pullback and bounce and ctx.price > t1.ema20 * 0.985 \
                    and 30 < t15.rsi < 60:
                return self._build(
                    ctx, "buy", "1h trend + 15m EMA20 pullback bounce",
                    sl_dist=t15.atr * p.get("sl_m", 2.0),
                    tp_dist=t1.atr * p.get("tp_m", 3.0),
                    confidence=0.6,
                )
        if t1.trend == "bearish" and t1.strength >= trend_min:
            pullback = t15.highs[-1] >= t15.ema20 * 0.988
            bounce = ctx.candle_bear_5m and t15.closes[-1] < t15.ema20 * 1.002
            if pullback and bounce and ctx.price < t1.ema20 * 1.015 \
                    and 40 < t15.rsi < 70:
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
    """1h Donchian breakout in the direction of the 1h trend."""

    name = "HTF_Breakout"
    priority = 2

    def evaluate(self, ctx: HtfContext) -> Optional[Signal]:
        p = self.params
        t1, t15 = ctx.tf1, ctx.tf15
        trend_min = p.get("trend_min", 0.05)
        if t1.trend == "bullish" and t1.strength >= trend_min:
            if t1.closes[-1] > t1.hh * (1.0 - 1e-9) and t15.closes[-1] > t15.hh * 0.999 \
                    and ctx.price > t1.ema20:
                return self._build(
                    ctx, "buy", "1h Donchian breakout (bullish)",
                    sl_dist=max(t1.atr * p.get("sl_m", 1.5), t15.atr * 2.0),
                    tp_dist=t1.atr * p.get("tp_m", 3.0),
                    confidence=0.65,
                )
        if t1.trend == "bearish" and t1.strength >= trend_min:
            if t1.closes[-1] < t1.ll * (1.0 + 1e-9) and t15.closes[-1] < t15.ll * 1.001 \
                    and ctx.price < t1.ema20:
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
    "TrendPullback_HTF": {"sl_m": 2.0, "tp_m": 3.0, "trend_min": 0.05},
    "HTF_Breakout": {"sl_m": 2.0, "tp_m": 4.0, "trend_min": 0.02},
    "MomentumRetrace_RSI": {"sl_m": 2.0, "tp_m": 2.2, "trend_min": 0.03,
                            "rsi15_low": 45, "rsi15_high": 55},
    "MeanReversion_BB": {"sl_m": 1.0, "rsi_low": 35, "rsi_high": 65,
                         "max_trend": 0.12, "max_bbw": 0.08},
    "VolatilityExpansion": {"sl_m": 1.8, "tp_m": 2.5, "trend_min": 0.03,
                            "atr_mult": 1.5, "vol_mult": 1.3},
    "SwingPullback_1h": {"sl_m": 1.2, "tp_m": 3.0, "trend_min": 0.05},
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
