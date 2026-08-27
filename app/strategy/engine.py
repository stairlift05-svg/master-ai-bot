"""Signal generation (v20.1): multi-timeframe strategy orchestration.

Builds the shared :class:`HtfContext` (5m/15m/1h features computed exactly
once per scan) and evaluates the enabled v2 strategies in priority order.

Trend determination per timeframe: EMA50 vs EMA200 spread (falling back to
price vs EMA50 while the EMA200 is still warming up).
"""
from __future__ import annotations

import logging
from typing import List, Optional

from app.config import Settings
from app.models import AnalysisResult, CandleSeries
from app.state import EngineState
from app.strategy import indicators as ind
from app.strategy.signals import (
    HtfContext,
    TFContext,
    default_strategies_v2,
    BaseStrategyV2,
)

log = logging.getLogger("quant.strategy")

# v20.6: Donchian_Trend's regime filter uses the EMA200 of the primary
# timeframe. With fewer than 200 closed bars the EMA200 is undefined and the
# context silently falls back to the EMA50 — a DIFFERENT, unvalidated
# strategy. Measured on the real 1h test set: 260-bar warm-up gives
# +3.52% / PF 1.53, the old 120-bar floor gives +3.23% / PF 1.47 (the filter
# is weaker, so it lets marginal trades through). The floor is therefore
# raised to cover the EMA200 plus a small margin.
MIN_BARS_5M = 210
LOOKBACKS = {"15m": 24, "1h": 48}


class StrategyEngine:
    """Evaluates the active v2 strategies against one symbol's data."""

    def __init__(self, settings: Settings, state: EngineState) -> None:
        self._settings = settings
        self._state = state
        self._strategies: List[BaseStrategyV2] = default_strategies_v2(
            getattr(settings, "strategy_params", None),
            enabled=getattr(settings, "enabled_strategies", None),
        )
        self._strategies.sort(key=lambda s: s.priority)

    # ------------------------------------------------------------------
    def analyze(self, df5: CandleSeries, df15: CandleSeries, df1: CandleSeries,
                symbol: str = "", drop_forming: bool = True) -> AnalysisResult:
        """Run the full multi-timeframe signal pipeline.

        Args:
            df5: 5m candles (or primary timeframe).
            df15: 15m candles (may be resampled).
            df1: 1h candles (may be resampled).
            drop_forming: exclude the final bar of each series (live scans
                often run mid-bar; the backtest passes False because bars
                are already closed).
        """
        if drop_forming:
            df5 = df5.without_last()
            df15 = df15.without_last() if len(df15) > 30 else df15
            df1 = df1.without_last() if len(df1) > 30 else df1

        closes5 = df5.closes
        if len(closes5) < MIN_BARS_5M or closes5[-1] <= 0:
            return AnalysisResult(
                "neutral",
                f"insufficient data ({len(closes5)}/{MIN_BARS_5M} bars)",
            )

        ctx = self._build_context(symbol, df5, df15, df1)
        self._state.record_trend_strength(symbol, ctx.tf1.strength)

        for strategy in self._strategies:
            try:
                signal = strategy.propose(ctx)
            except Exception as exc:  # noqa: BLE001 - a strategy must not kill a scan
                log.exception("strategy %s crashed: %s", strategy.name, exc)
                self._state.record_error(f"strategy {strategy.name}: {exc}")
                continue
            if signal is not None:
                sides = getattr(self._settings, "sides", "both")
                if (sides == "long" and signal.side == "sell") \
                        or (sides == "short" and signal.side == "buy"):
                    return AnalysisResult(
                        "neutral", f"side filter ({sides}) blocked {signal.side}",
                        rsi=signal.rsi, atr=signal.atr, htf=signal.htf,
                    )
                return AnalysisResult(
                    "buy" if signal.side == "buy" else "sell",
                    reason=signal.reason, strategy=strategy.name,
                    rsi=signal.rsi, atr=signal.atr, htf=signal.htf,
                    signal=signal,
                )

        return AnalysisResult(
            "neutral", f"no signal (15m RSI={ctx.tf15.rsi:.1f}, 1h {ctx.tf1.trend})",
            rsi=ctx.tf15.rsi, atr=ctx.tf15.atr, htf=ctx.tf1.trend,
        )

    # ------------------------------------------------------------------
    def _build_context(self, symbol: str, df5: CandleSeries,
                       df15: CandleSeries, df1: CandleSeries) -> HtfContext:
        tf5 = self._tf_context("5m", df5, lookback=12)
        tf15 = self._tf_context("15m", df15, lookback=LOOKBACKS["15m"])
        tf1 = self._tf_context("1h", df1, lookback=LOOKBACKS["1h"])
        closes = tf5.closes
        price = closes[-1]
        s = self._settings
        # Round-trip friction: entry fee + exit fee + entry/exit slippage,
        # inflated by the configured fee buffer (conservative).
        round_trip = (2.0 * s.taker_fee + 2.0 * s.slippage_pct) * s.fee_buffer
        return HtfContext(
            symbol=symbol, price=price, tf5=tf5, tf15=tf15, tf1=tf1,
            candle_bull_5m=len(closes) >= 2 and closes[-1] > closes[-2],
            candle_bear_5m=len(closes) >= 2 and closes[-1] < closes[-2],
            min_stop_pct=s.min_stop_pct,
            round_trip_cost_pct=round_trip,
            min_edge_ratio=s.min_edge_ratio,
        )

    # ------------------------------------------------------------------
    def _tf_context(self, label: str, series: CandleSeries,
                    lookback: int) -> TFContext:
        closes, highs, lows, vols = series.closes, series.highs, series.lows, series.volumes
        n = len(closes)
        atr_list = ind.atr_wilder(highs, lows, closes, 14)
        atr = atr_list[-1] or 0.0
        rsi_list = ind.rsi_wilder(closes, 14)
        rsi = rsi_list[-1] if rsi_list[-1] is not None else 50.0
        ema20_list = ind.ema(closes, 20)
        ema20 = ema20_list[-1] or closes[-1]
        ema50_list = ind.ema(closes, 50)
        ema50 = ema50_list[-1] or closes[-1]
        ema200 = None
        if n >= 200:
            ema200 = ind.ema(closes, 200)[-1]
        # Breakout references EXCLUDE the forming/current bar (a breakout is
        # close-now vs the PRIOR N-bar extreme).
        prior_highs = highs[:-1] if len(highs) > 1 else highs
        prior_lows = lows[:-1] if len(lows) > 1 else lows
        hh_list = ind.rolling_max(prior_highs, lookback)
        ll_list = ind.rolling_min(prior_lows, lookback)
        hh = hh_list[-1] if hh_list and hh_list[-1] is not None else (prior_highs[-1] if prior_highs else highs[-1])
        ll = ll_list[-1] if ll_list and ll_list[-1] is not None else (prior_lows[-1] if prior_lows else lows[-1])
        mid, upper, lower = ind.bollinger(closes, 20, 2.0)

        # Trend from EMA50 vs EMA200 (fallback: price vs EMA50).
        if ema200 is not None and ema200 > 0:
            strength = abs(ema50 - ema200) / abs(ema200) * 100.0
            if closes[-1] > ema50 * 0.995 and ema50 >= ema200 * 0.995:
                trend = "bullish"
            elif closes[-1] < ema50 * 1.005 and ema50 <= ema200 * 1.005:
                trend = "bearish"
            else:
                trend = "sideways"
        else:
            strength = abs(ema50 - closes[-1]) / (abs(closes[-1]) + 1e-9) * 100.0
            if closes[-1] > ema50 * 1.002:
                trend = "bullish"
            elif closes[-1] < ema50 * 0.998:
                trend = "bearish"
            else:
                trend = "sideways"

        return TFContext(
            label=label, closes=closes, highs=highs, lows=lows, volumes=vols,
            atr=atr, rsi=rsi, ema20=ema20, ema50=ema50, ema200=ema200,
            hh=hh, ll=ll, trend=trend, strength=strength,
            mid=mid[-1] or 0.0, bb_upper=upper[-1] or 0.0,
            bb_lower=lower[-1] or 0.0,
        )
