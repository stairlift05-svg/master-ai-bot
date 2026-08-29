"""v22.2.1: DATA section must show real fetch counters per configured TF."""
import asyncio
import unittest
from types import SimpleNamespace

from app.config import Settings
from app.observability.reporter import build_txt_report
from app.state import EngineState


class FakeDB:
    async def get_recent_decisions(self, n): return []
    async def get_closed_trades(self, n): return []
    async def compute_metrics(self):
        return SimpleNamespace(total_trades=0, win_rate=0.0, profit_factor=0.0,
                               expectancy=0.0, total_pnl=0.0, max_dd_pct=0.0,
                               sharpe=0.0)


class TestFetchCounters(unittest.TestCase):

    def test_both_columns_show_real_counts(self):
        s = Settings()
        state = EngineState()
        state.record_fetch("ETHUSD", s.timeframe, True)     # primary (1h)
        state.record_fetch("ETHUSD", s.htf_timeframe, True)  # htf (4h)
        state.record_fetch("ETHUSD", s.timeframe, False)     # one primary fail
        txt = asyncio.run(build_txt_report(state, FakeDB(), s, {}, {}))
        self.assertIn(f"{s.timeframe} 1/1", txt)   # primary ok/fail
        self.assertIn(f"{s.htf_timeframe} 1/0", txt)  # htf ok/fail


if __name__ == "__main__":
    unittest.main()
