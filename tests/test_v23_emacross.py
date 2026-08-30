"""v23.3: EmaCross_Trend — the owner-supplied Pine golden strategy, ported."""
import unittest
from unittest import mock

from app.config import Settings
from app.strategy.signals import (HtfContext, TFContext, _adx, _ema_series,
                                  build_strategy)


def _ctx(closes, label="4h"):
    n = len(closes)
    tf = TFContext(label=label, closes=closes, highs=[c * 1.001 for c in closes],
                   lows=[c * 0.999 for c in closes], volumes=[1.0] * n,
                   atr=closes[-1] * 0.005, rsi=55.0, ema20=closes[-1],
                   ema50=closes[-1], ema200=None, trend="bullish", strength=1.0)
    return HtfContext(symbol="ETHUSD", price=closes[-1], tf5=tf, tf15=tf,
                      tf1=tf, candle_bull_5m=True, candle_bear_5m=False,
                      min_stop_pct=0.003, round_trip_cost_pct=0.0014,
                      min_edge_ratio=3.0)


def _series_up():
    """Flat history, one jump bar -> EMA9 crosses OVER EMA21 exactly on the
    last closed bar (the signal bar)."""
    return [100.0] * 217 + [103.0]


def _series_dn():
    return [100.0] * 217 + [97.0]


class TestHelpers(unittest.TestCase):

    def test_ema_series_matches_reference(self):
        s = _ema_series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], 3)
        self.assertAlmostEqual(s[0], 2.0)  # SMA(3) of 1,2,3
        self.assertAlmostEqual(s[1], 3.0)  # 2/4*4 + 2/4*2

    def test_adx_high_for_trend_low_for_chop(self):
        n = 120
        up = [100.0 + i for i in range(n)]  # perfect uptrend
        high = [x + 1.0 for x in up]
        low = [x - 1.0 for x in up]
        self.assertGreater(_adx(high, low, up), 25.0)
        flat = [100.0 + (0.5 if i % 2 else -0.5) for i in range(n)]
        fh = [101.0] * n
        fl = [99.0] * n
        self.assertLess(_adx(fh, fl, flat), 25.0)


class TestEmaCrossTrend(unittest.TestCase):

    def _strat(self, **over):
        return build_strategy("EmaCross_Trend", over or None)

    def test_golden_cross_buys_with_filters(self):
        with mock.patch("app.strategy.signals._adx", return_value=30.0):
            sig = self._strat().propose(_ctx(_series_up()))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "buy")
        self.assertAlmostEqual(sig.sl, sig.entry * 0.99, places=6)
        self.assertAlmostEqual(sig.tp, sig.entry * 1.02, places=6)

    def test_dead_cross_sells_with_filters(self):
        with mock.patch("app.strategy.signals._adx", return_value=30.0):
            sig = self._strat().propose(_ctx(_series_dn()))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "sell")
        self.assertAlmostEqual(sig.sl, sig.entry * 1.01, places=6)

    def test_weak_adx_blocks_entry(self):
        with mock.patch("app.strategy.signals._adx", return_value=10.0):
            self.assertIsNone(self._strat().propose(_ctx(_series_up())))

    def test_registered_not_default_enabled(self):
        from app.strategy.signals import _STRATEGY_CLASSES
        self.assertIn("EmaCross_Trend", _STRATEGY_CLASSES)
        self.assertEqual(Settings().enabled_strategies, ("Imba_Fib",))


if __name__ == "__main__":
    unittest.main()
