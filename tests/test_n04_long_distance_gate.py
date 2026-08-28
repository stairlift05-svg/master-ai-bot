"""N-04: optional long-side distance gate for Donchian_Trend.

v22 ships long_dist_atr=1.0 (two-window sweep optimum; see
analysis/runs/sweep_long_gate_v22.json). A long breakout must clear the
slow EMA by that many ATR (shorts are never affected); 0.0 reproduces
the v21 behaviour bit-for-bit.
"""
import os
import unittest

from app.config import Settings


def _ctx(closes, atr=1.0, ema200=100.0):
    from app.strategy.signals import HtfContext, TFContext

    n = len(closes)
    tf = TFContext(label="1h", closes=closes, highs=list(closes),
                   lows=list(closes), volumes=[1.0] * n, atr=atr, rsi=50.0,
                   ema20=closes[-1], ema50=closes[-1], ema200=ema200,
                   trend="bullish", strength=1.0)
    return HtfContext(symbol="ETHUSD", price=closes[-1], tf5=tf, tf15=tf,
                      tf1=tf, candle_bull_5m=True, candle_bear_5m=False,
                      min_stop_pct=0.003, round_trip_cost_pct=0.0014,
                      min_edge_ratio=3.0)


class TestLongDistanceGate(unittest.TestCase):

    def _strat(self, **over):
        from app.strategy.signals import build_strategy
        return build_strategy("Donchian_Trend", over or None)

    def test_default_off_matches_validated_behaviour(self):
        """long_dist_atr=0 (shipped default) takes the same long as v21."""
        closes = [100.0] * 60 + [130.0]
        sig = self._strat().propose(_ctx(closes, ema200=100.0))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "buy")

    def test_gate_blocks_long_hugging_the_ema(self):
        """Breakout 1 ATR above the EMA with gate=2 ATR -> skipped."""
        closes = [100.0] * 60 + [130.0]
        sig = self._strat(long_dist_atr=2.0).propose(_ctx(closes, ema200=129.0))
        self.assertIsNone(sig)  # 130-129 = 1 ATR < 2 ATR gate

    def test_gate_passes_when_well_above_the_ema(self):
        closes = [100.0] * 60 + [130.0]
        sig = self._strat(long_dist_atr=2.0).propose(_ctx(closes, ema200=100.0))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "buy")

    def test_shorts_never_affected_by_the_gate(self):
        closes = [100.0] * 60 + [70.0]
        sig = self._strat(long_dist_atr=9.0).propose(_ctx(closes, ema200=100.0))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "sell")

    def test_env_override_is_wired_and_default_is_v22_shipped(self):
        """v22 ships the two-window sweep optimum (1.0); 0 = v21 behaviour."""
        self.assertEqual(
            Settings.from_env().strategy_params["Donchian_Trend"]["long_dist_atr"], 1.0)
        os.environ["DONCHIAN_LONG_DIST_ATR"] = "0"
        try:
            self.assertEqual(
                Settings.from_env().strategy_params["Donchian_Trend"]["long_dist_atr"], 0.0)
        finally:
            del os.environ["DONCHIAN_LONG_DIST_ATR"]


if __name__ == "__main__":
    unittest.main()
