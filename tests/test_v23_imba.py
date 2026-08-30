"""v23: IMBA ALGO final stack — faithful port, entry/stop/target semantics."""
import unittest

from app.config import Settings
from app.strategy.signals import HtfContext, TFContext, build_strategy


def _ctx(closes, highs, lows, ema200=100.0, rsi=50.0, cost=0.0014, edge=3.0,
         min_stop=0.003):
    n = len(closes)
    tf = TFContext(label="1h", closes=closes, highs=highs, lows=lows,
                   volumes=[1.0] * n, atr=1.0, rsi=rsi, ema20=closes[-1],
                   ema50=closes[-1], ema200=ema200, trend="bullish", strength=1.0)
    return HtfContext(symbol="ETHUSD", price=closes[-1], tf5=tf, tf15=tf,
                      tf1=tf, candle_bull_5m=True, candle_bear_5m=False,
                      min_stop_pct=min_stop, round_trip_cost_pct=cost,
                      min_edge_ratio=edge)


def _market(final, *, lo=90.0, hi=110.0, n=210):
    """Flat channel [lo, hi] ending at close=final."""
    closes = [ (lo+hi)/2 ] * n
    closes[-1] = final
    return closes, [hi] * n, [lo] * n


class TestImbaEntries(unittest.TestCase):

    def _strat(self, **over):
        return build_strategy("Imba_Fib", over or None)

    def test_long_in_top_band_with_filters(self):
        closes, hs, ls = _market(108.0)   # fib236=105.28, fib50=100, fib786=94.28
        sig = self._strat().propose(_ctx(closes, hs, ls, ema200=100.0, rsi=60.0))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "buy")
        self.assertAlmostEqual(sig.sl, 94.28, places=1)     # fib stop
        self.assertAlmostEqual(sig.tp, 108.0 * 1.04, places=1)  # tp4 = 4%
        self.assertAlmostEqual(sig.tp1, 108.0 * 1.01, places=1)  # tp1 = 1%

    def test_short_in_bottom_band_with_filters(self):
        closes, hs, ls = _market(92.0)
        sig = self._strat().propose(_ctx(closes, hs, ls, ema200=100.0, rsi=40.0))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "sell")
        self.assertAlmostEqual(sig.sl, 105.28, places=1)    # fib236 stop
        self.assertAlmostEqual(sig.tp, 92.0 * 0.96, places=1)

    def test_mid_channel_is_no_signal(self):
        closes, hs, ls = _market(103.0)   # above fib50, below fib236
        self.assertIsNone(
            self._strat().propose(_ctx(closes, hs, ls, ema200=100.0, rsi=60.0)))

    def test_rsi_guard_blocks_overheated_long(self):
        closes, hs, ls = _market(108.0)
        self.assertIsNone(
            self._strat().propose(_ctx(closes, hs, ls, ema200=100.0, rsi=75.0)))

    def test_ema_regime_blocks_counter_trend(self):
        closes, hs, ls = _market(108.0)
        self.assertIsNone(
            self._strat().propose(_ctx(closes, hs, ls, ema200=120.0, rsi=60.0)))

    def test_cost_gate_refuses_when_target_below_floor(self):
        closes, hs, ls = _market(108.0)
        self.assertIsNone(self._strat().propose(
            _ctx(closes, hs, ls, ema200=100.0, rsi=60.0, edge=100.0)))

    def test_registered_and_shipped_default(self):
        from app.strategy.signals import _STRATEGY_CLASSES
        self.assertIn("Imba_Fib", _STRATEGY_CLASSES)
        self.assertEqual(Settings().enabled_strategies, ("Imba_Fib",))

    def test_module_defaults_untouched(self):
        p = Settings().strategy_params["Imba_Fib"]
        self.assertEqual(p["sensitivity"], 18)
        self.assertEqual(p["tp4"], 4.0)
        self.assertEqual(p["rsi_long_guard"], 72.0)


if __name__ == "__main__":
    unittest.main()
