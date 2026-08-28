"""v22.1: reports must not lie about timeframes or version."""
import unittest

from app.config import Settings


class TestContextLabels(unittest.TestCase):
    """The shipped config is 1h primary / 4h mid / 4h HTF; the context
    labels and the no-signal reason must say so (they used to hard-code
    "15m RSI" while actually showing the 4h RSI)."""

    def test_shipped_labels_are_the_configured_timeframes(self):
        from app.strategy.engine import StrategyEngine
        from app.state import EngineState
        s = Settings()
        eng = StrategyEngine(s, EngineState())
        ctx = eng._build_context(
            "ETHUSD",
            *[__import__("app.models", fromlist=["CandleSeries"]).CandleSeries(
                [__import__("app.models", fromlist=["Candle"]).Candle(
                    1_700_000_000_000 + i * 3_600_000, 100.0, 101.0, 99.0,
                    100.0 + (i % 7) * 0.5, 10.0) for i in range(n)]
            ) for n in (260, 60, 60)])
        self.assertEqual(ctx.tf5.label, s.timeframe)      # "1h"
        self.assertEqual(ctx.tf15.label, s.mid_timeframe)  # "4h"
        self.assertEqual(ctx.tf1.label, s.htf_timeframe)   # "4h"

    def test_no_signal_reason_uses_real_labels(self):
        from app.strategy.engine import StrategyEngine
        from app.state import EngineState
        from app.models import Candle, CandleSeries
        s = Settings()
        eng = StrategyEngine(s, EngineState())
        series = CandleSeries([Candle(1_700_000_000_000 + i * 3_600_000,
                                      100.0, 101.0, 99.0, 100.0, 10.0)
                               for i in range(260)])
        res = eng.analyze(series, series, series, symbol="ETHUSD")
        self.assertIn("4h RSI=", res.reason)
        self.assertNotIn("15m RSI=", res.reason)


class TestReportHeader(unittest.TestCase):

    def test_txt_report_header_says_v22(self):
        import asyncio
        from app.observability.reporter import build_txt_report
        from app.state import EngineState

        class _FakeDB:
            async def get_recent_decisions(self, n):
                return []

            async def get_closed_trades(self, n):
                return []

            async def compute_metrics(self):
                from app.models import Metrics
                return Metrics()

        txt = asyncio.run(build_txt_report(
            EngineState(), _FakeDB(), Settings(), {}, {}))
        self.assertIn("MASTER QUANT ENGINE v22", txt)
        self.assertNotIn("v20", txt.split("\n")[2])


if __name__ == "__main__":
    unittest.main()
