"""Signal generation (#02): strategy orchestration engine.

Responsibilities:

* Higher-timeframe trend filter (EMA50 vs EMA200 on the HTF) + strength gauge.
* Single-pass computation of the shared :class:`MarketContext` (indicators
  are computed **once** per scan, not once per strategy).
* Priority-ordered strategy evaluation; the first signal wins.
* Neutral-result reasons are descriptive (feed the Telegram rejection log).
"""
from __future__ import annotations

import logging
from typing import List

from app.config import Settings
from app.models import AnalysisResult, CandleSeries
from app.state import EngineState
from app.strategy import indicators as ind
from app.strategy.signals import (
    MarketContext,
    default_strategies,
    BaseStrategy,
)

log = logging.getLogger("quant.strategy")

MIN_BARS = 55
HTF_EMA_SHORT = 50
HTF_EMA_LONG = 200


class StrategyEngine:
    """Evaluates all strategies against one symbol's closed candle data."""

    def __init__(self, settings: Settings, state: EngineState) -> None:
        self._settings = settings
        self._state = state
        self._strategies: List[BaseStrategy] = default_strategies(
            settings.strategy_params
        )
        self._strategies.sort(key=lambda s: s.priority)

    # ------------------------------------------------------------------
    def analyze(self, df5: CandleSeries, df1: CandleSeries,
                symbol: str = "", drop_forming: bool = True) -> AnalysisResult:
        """Run the full signal pipeline.

        Args:
            df5: 5-minute candles (last bar treated as forming unless
                ``drop_forming=False``, e.g. in the backtest).
            df1: higher-timeframe candles (same convention).
            symbol: AriaX symbol for diagnostics.
            drop_forming: exclude the final bar from analysis.

        Returns:
            :class:`AnalysisResult` — always a structured object, never None.
        """
        if drop_forming:
            df5 = df5.without_last()
            df1 = df1.without_last() if len(df1) > 30 else df1

        closes = df5.closes
        if len(closes) < MIN_BARS or closes[-1] <= 0:
            return AnalysisResult("neutral", "insufficient data")

        # ---- Higher-timeframe trend filter ---------------------------
        htf_trend, trend_strength = self._htf_trend(df1)
        self._state.record_trend_strength(symbol, trend_strength)

        # ---- Shared features (computed once) -------------------------
        highs, lows, volumes = df5.highs, df5.lows, df5.volumes
        price = closes[-1]
        atr_value = ind.atr_wilder(highs, lows, closes, 14)
        atr = atr_value[-1] if atr_value else None
        if not atr or atr <= 0:
            return AnalysisResult("neutral", "ATR zero/invalid", htf=htf_trend)
        rsi_value = ind.rsi_wilder(closes, 14)
        rsi = rsi_value[-1] if rsi_value else None
        if rsi is None or rsi <= 3 or rsi >= 97:
            return AnalysisResult(
                "neutral", f"RSI invalid ({rsi:.1f})" if rsi is not None else "RSI None",
                rsi=rsi or 0.0, atr=atr, htf=htf_trend,
            )

        ema20_list = ind.ema(closes, 20)
        ema20 = ema20_list[-1] or price
        st_dir, st_up, st_lo = ind.supertrend(highs, lows, closes, 10, 3.0)
        vol_sma_list = ind.sma(volumes, 20)
        vol_sma = vol_sma_list[-1] or 1e-9
        h12_list = ind.rolling_max(highs, 12)
        l12_list = ind.rolling_min(lows, 12)
        h12 = h12_list[-1] or price
        l12 = l12_list[-1] or price
        div = ind.detect_rsi_divergence(closes, rsi_value, 28)

        if trend_strength < self._settings.trend_strength_threshold:
            return AnalysisResult(
                "neutral", f"weak trend ({trend_strength:.3f}%)",
                rsi=rsi, atr=atr, htf="weak",
            )
        if htf_trend == "sideways":
            return AnalysisResult(
                "neutral", "HTF trend unclear", rsi=rsi, atr=atr, htf=htf_trend,
            )

        ctx = MarketContext(
            symbol=symbol, price=price, atr=atr, rsi=rsi, ema20=ema20,
            vol_cur=volumes[-1], vol_sma=vol_sma, h12=h12, l12=l12,
            htf_trend=htf_trend, trend_strength=trend_strength,
            candle_bull=len(closes) >= 2 and closes[-1] > closes[-2],
            candle_bear=len(closes) >= 2 and closes[-1] < closes[-2],
            st_direction=st_dir[-1], st_upper=st_up[-1], st_lower=st_lo[-1],
            divergence=div, closes=closes, highs=highs, lows=lows,
            min_stop_pct=self._settings.min_stop_pct,
        )

        # ---- Priority-ordered strategy evaluation --------------------
        for strategy in self._strategies:
            try:
                signal = strategy.evaluate(ctx)
            except Exception as exc:  # noqa: BLE001 - a strategy must not kill a scan
                log.exception("strategy %s crashed: %s", strategy.name, exc)
                self._state.record_error(f"strategy {strategy.name}: {exc}")
                continue
            if signal is not None:
                return AnalysisResult(
                    "buy" if signal.side == "buy" else "sell",
                    reason=signal.reason, strategy=strategy.name,
                    rsi=rsi, atr=atr, htf=htf_trend, signal=signal,
                )

        return AnalysisResult(
            "neutral", f"no signal (RSI={rsi:.1f})", rsi=rsi, atr=atr,
            htf=htf_trend,
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _htf_trend(df1: CandleSeries):
        """Return (trend label, strength percent) from the HTF series."""
        hclose = df1.closes
        if len(hclose) < 30:
            return "sideways", 0.0
        e_short = ind.ema(hclose, min(HTF_EMA_SHORT, len(hclose)))[-1] or hclose[-1]
        e_long = ind.ema(hclose, min(HTF_EMA_LONG, len(hclose)))[-1] or hclose[-1]
        hp = hclose[-1]
        strength = abs(e_short - e_long) / (abs(e_long) + 1e-9) * 100.0
        if hp > e_short * 0.993 and e_short >= e_long * 0.990:
            return "bullish", strength
        if hp < e_short * 1.007 and e_short <= e_long * 1.010:
            return "bearish", strength
        return "sideways", strength
