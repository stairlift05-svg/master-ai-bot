"""Signal generation (#02): individual strategy implementations.

Each strategy is a self-contained :class:`BaseStrategy` subclass following the
*Strategy* design pattern: the engine precomputes a :class:`MarketContext`
once per scan (indicators are expensive — we must not recompute them per
strategy), then asks every strategy in priority order for a signal.

The five strategies ported from v19.3, each hardened and documented:

1. ``RSI_Extreme_Bounce``  — mean reversion from RSI extremes with volume.
2. ``Breakout_Momentum``   — breakout of a 12-bar range in a trending HTF.
3. ``SuperTrend_Pullback`` — pullback to the Supertrend band with a bullish
   candle and HTF trend confirmation.
4. ``Volume_Surge``        — volume expansion in the direction of the HTF.
5. ``RSI_Divergence``      — classic divergence with HTF confirmation.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional

from app.models import Signal


@dataclass(frozen=True)
class MarketContext:
    """Precomputed, immutable feature set for one symbol scan."""

    symbol: str
    price: float
    atr: float
    rsi: float
    ema20: float
    vol_cur: float
    vol_sma: float
    h12: float
    l12: float
    htf_trend: str          # "bullish" | "bearish" | "sideways"
    trend_strength: float   # percent
    candle_bull: bool
    candle_bear: bool
    st_direction: Optional[int]
    st_upper: Optional[float]
    st_lower: Optional[float]
    divergence: Optional[str]
    closes: List[float]
    highs: List[float]
    lows: List[float]
    min_stop_pct: float = 0.003  # floor for |stop - price| / price

    # -- convenience --------------------------------------------------
    @property
    def vol_ok(self) -> bool:
        return self.vol_cur > self.vol_sma * 1.15

    @property
    def vol_surge(self) -> bool:
        return self.vol_cur > self.vol_sma * 1.35


class BaseStrategy(ABC):
    """Interface every signal generator must implement."""

    name: str = "base"
    priority: int = 100  # lower wins

    def __init__(self, params: Dict[str, float]):
        self.params = params

    @abstractmethod
    def evaluate(self, ctx: MarketContext) -> Optional[Signal]:
        """Return a :class:`Signal` or ``None`` (no trade)."""

    # -- shared helpers ------------------------------------------------
    def _build(self, ctx: MarketContext, side: str, reason: str,
               confidence: float = 0.5) -> Signal:
        """Build a Signal with a *consistent* stop/target structure.

        The stop distance is floored at ``min_stop_pct`` of price so that the
        position sizer (which applies the same floor) budgets the *actual*
        stop distance.  Targets scale with the floored stop distance to keep
        the reward/risk ratio intact even when ATR is tiny relative to price.
        """
        p = self.params
        price = ctx.price
        sl_m = p.get("sl_m", 1.5)
        tp_m = p.get("tp_m", 3.2)
        tp1_m = p.get("tp1_m", 1.7)
        atr_sl = ctx.atr * sl_m
        floor_dist = price * ctx.min_stop_pct
        sl_dist = max(atr_sl, floor_dist)
        tp_dist = max(ctx.atr * tp_m, sl_dist * (tp_m / sl_m))
        tp1_dist = max(ctx.atr * tp1_m, sl_dist * (tp1_m / sl_m))
        if side == "buy":
            return Signal(
                side="buy", strategy=self.name, reason=reason, entry=price,
                sl=price - sl_dist, tp1=price + tp1_dist, tp=price + tp_dist,
                rsi=ctx.rsi, atr=ctx.atr, htf=ctx.htf_trend,
                confidence=confidence,
            )
        return Signal(
            side="sell", strategy=self.name, reason=reason, entry=price,
            sl=price + sl_dist, tp1=price - tp1_dist, tp=price - tp_dist,
            rsi=ctx.rsi, atr=ctx.atr, htf=ctx.htf_trend,
            confidence=confidence,
        )


class RSIExtremeBounce(BaseStrategy):
    """Mean reversion: buy oversold wicks, sell overbought wicks."""

    name = "RSI_Extreme_Bounce"
    priority = 1

    def evaluate(self, ctx: MarketContext) -> Optional[Signal]:
        if ctx.rsi < 22 and ctx.candle_bull and ctx.vol_cur > ctx.vol_sma * 0.9:
            return self._build(ctx, "buy", "RSI oversold + bullish candle", 0.45)
        if ctx.rsi > 78 and ctx.candle_bear and ctx.vol_cur > ctx.vol_sma * 0.9:
            return self._build(ctx, "sell", "RSI overbought + bearish candle", 0.45)
        return None


class BreakoutMomentum(BaseStrategy):
    """Trend continuation through a 12-bar range breakout."""

    name = "Breakout_Momentum"
    priority = 2

    def evaluate(self, ctx: MarketContext) -> Optional[Signal]:
        if ctx.htf_trend == "bullish" and ctx.price > ctx.ema20 * 1.0005 \
                and ctx.price >= ctx.h12 * 0.997 and 42 < ctx.rsi < 70 and ctx.vol_ok:
            return self._build(ctx, "buy", "Bullish range breakout", 0.6)
        if ctx.htf_trend == "bearish" and ctx.price < ctx.ema20 * 0.9995 \
                and ctx.price <= ctx.l12 * 1.003 and 30 < ctx.rsi < 58 and ctx.vol_ok:
            return self._build(ctx, "sell", "Bearish range breakdown", 0.6)
        return None


class SuperTrendPullback(BaseStrategy):
    """Buy pullbacks to the Supertrend band inside an uptrend."""

    name = "SuperTrend_Pullback"
    priority = 3

    def evaluate(self, ctx: MarketContext) -> Optional[Signal]:
        st_d, st_l, st_u = ctx.st_direction, ctx.st_lower, ctx.st_upper
        if st_d is None or st_l is None or st_u is None:
            return None
        if (ctx.htf_trend == "bullish" and st_d == 1
                and ctx.lows[-1] <= st_l * 1.008 and ctx.candle_bull
                and 38 < ctx.rsi < 68 and ctx.price > ctx.ema20):
            return self._build(ctx, "buy", "Bullish SuperTrend pullback", 0.55)
        if (ctx.htf_trend == "bearish" and st_d == -1
                and ctx.highs[-1] >= st_u * 0.992 and ctx.candle_bear
                and 32 < ctx.rsi < 62 and ctx.price < ctx.ema20):
            return self._build(ctx, "sell", "Bearish SuperTrend pullback", 0.55)
        return None


class VolumeSurge(BaseStrategy):
    """Strong volume in the direction of the higher-timeframe trend."""

    name = "Volume_Surge"
    priority = 4

    def evaluate(self, ctx: MarketContext) -> Optional[Signal]:
        if ctx.htf_trend == "bullish" and ctx.price > ctx.ema20 \
                and ctx.vol_surge and ctx.candle_bull and 43 < ctx.rsi < 70:
            return self._build(ctx, "buy", "Volume surge long", 0.5)
        if ctx.htf_trend == "bearish" and ctx.price < ctx.ema20 \
                and ctx.vol_surge and ctx.candle_bear and 30 < ctx.rsi < 57:
            return self._build(ctx, "sell", "Volume surge short", 0.5)
        return None


class RSIDivergence(BaseStrategy):
    """RSI divergence with HTF trend confirmation."""

    name = "RSI_Divergence"
    priority = 5

    def evaluate(self, ctx: MarketContext) -> Optional[Signal]:
        div = ctx.divergence
        if div == "bullish" and ctx.htf_trend == "bullish" and ctx.rsi < 45 \
                and ctx.candle_bull and ctx.vol_cur > ctx.vol_sma * 1.05:
            return self._build(ctx, "buy", "Bullish RSI divergence", 0.55)
        if div == "bearish" and ctx.htf_trend == "bearish" and ctx.rsi > 55 \
                and ctx.candle_bear and ctx.vol_cur > ctx.vol_sma * 1.05:
            return self._build(ctx, "sell", "Bearish RSI divergence", 0.55)
        return None


def default_strategies(strategy_params: Dict[str, Dict[str, float]]) -> List[BaseStrategy]:
    """Instantiate all strategies with the configured parameter sets."""
    return [
        RSIExtremeBounce(strategy_params.get("RSI_Extreme_Bounce", {})),
        BreakoutMomentum(strategy_params.get("Breakout_Momentum", {})),
        SuperTrendPullback(strategy_params.get("SuperTrend_Pullback", {})),
        VolumeSurge(strategy_params.get("Volume_Surge", {})),
        RSIDivergence(strategy_params.get("RSI_Divergence", {})),
    ]
