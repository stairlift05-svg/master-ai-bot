"""v23.2: candidate quality filters — default OFF, logic locked by tests."""
import unittest

from app.strategy.signals import HtfContext, TFContext, build_strategy


def _ctx(closes, highs, lows, ema200=100.0, rsi=60.0, atr=1.0, tf1_trend="bullish"):
    n = len(closes)
    tf = TFContext(label="1h", closes=closes, highs=highs, lows=lows,
                   volumes=[1.0] * n, atr=atr, rsi=rsi, ema20=closes[-1],
                   ema50=closes[-1], ema200=ema200, trend="bullish", strength=1.0)
    tf1 = TFContext(label="4h", closes=closes, highs=highs, lows=lows,
                    volumes=[1.0] * n, atr=atr, rsi=rsi, ema20=closes[-1],
                    ema50=closes[-1], ema200=ema200, trend=tf1_trend, strength=1.0)
    return HtfContext(symbol="ETHUSD", price=closes[-1], tf5=tf, tf15=tf,
                      tf1=tf1, candle_bull_5m=True, candle_bear_5m=False,
                      min_stop_pct=0.003, round_trip_cost_pct=0.0014,
                      min_edge_ratio=3.0)


def _market(final, lo=90.0, hi=110.0, n=210):
    closes = [(lo + hi) / 2] * n
    closes[-1] = final
    return closes, [hi] * n, [lo] * n


class TestImbaFilters(unittest.TestCase):

    def test_defaults_off_preserve_module_behaviour(self):
        from app.config import Settings
        p = Settings().strategy_params["Imba_Fib"]
        self.assertEqual(p["break_margin_atr"], 0.0)
        self.assertEqual(p["min_ema_dist_atr"], 0.0)
        self.assertEqual(p["htf_align"], 0)
        closes, hs, ls = _market(108.0)
        sig = build_strategy("Imba_Fib").propose(_ctx(closes, hs, ls))
        self.assertIsNotNone(sig)  # band entry unchanged

    def test_break_margin_blocks_marginal_band_touch(self):
        closes, hs, ls = _market(105.5)  # just above fib236=105.28
        strat = build_strategy("Imba_Fib", {"break_margin_atr": 1.0})
        self.assertIsNone(strat.propose(_ctx(closes, hs, ls, atr=1.0)))
        strat0 = build_strategy("Imba_Fib", {"break_margin_atr": 0.0})
        self.assertIsNotNone(strat0.propose(_ctx(closes, hs, ls, atr=1.0)))

    def test_htf_align_blocks_misaligned_trend(self):
        closes, hs, ls = _market(108.0)
        strat = build_strategy("Imba_Fib", {"htf_align": 1})
        self.assertIsNone(strat.propose(_ctx(closes, hs, ls, tf1_trend="bearish")))
        self.assertIsNotNone(strat.propose(_ctx(closes, hs, ls, tf1_trend="bullish")))


if __name__ == "__main__":
    unittest.main()


class TestStopFib(unittest.TestCase):

    def test_default_matches_module_stop(self):
        from app.config import Settings
        prm = Settings().strategy_params["Imba_Fib"]
        self.assertEqual(prm["stop_fib_long"], 0.786)
        self.assertEqual(prm["stop_fib_short"], 0.236)

    def test_tighter_stop_moves_sl_closer(self):
        closes, hs, ls = _market(108.0)  # hh=110 ll=90 rng=20
        base = build_strategy("Imba_Fib").propose(_ctx(closes, hs, ls, rsi=60.0))
        tight = build_strategy("Imba_Fib", {"stop_fib_long": 0.618}).propose(
            _ctx(closes, hs, ls, rsi=60.0))
        self.assertAlmostEqual(base.sl, 110 - 20 * 0.786, places=1)   # 94.28
        self.assertAlmostEqual(tight.sl, 110 - 20 * 0.618, places=1)  # 97.64


class TestAtrStop(unittest.TestCase):

    def test_default_off_uses_fib_stop(self):
        from app.config import Settings
        self.assertEqual(
            Settings().strategy_params["Imba_Fib"]["sl_atr_mult"], 0.0)

    def test_atr_mult_replaces_stop_distance(self):
        closes, hs, ls = _market(108.0)  # hh=110 ll=90 rng=20, fib stop 94.28
        fib = build_strategy("Imba_Fib").propose(_ctx(closes, hs, ls, rsi=60.0, atr=2.0))
        atr = build_strategy("Imba_Fib", {"sl_atr_mult": 2.5}).propose(
            _ctx(closes, hs, ls, rsi=60.0, atr=2.0))
        self.assertAlmostEqual(fib.sl, 94.28, places=1)
        self.assertAlmostEqual(atr.sl, 108.0 - 2.5 * 2.0, places=6)  # 103.0
