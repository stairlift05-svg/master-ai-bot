"""v22.2: autonomous 6-hourly Telegram result report."""
import os
import unittest

from app.config import Settings


class TestReportInterval(unittest.TestCase):

    def test_default_is_6h(self):
        self.assertEqual(Settings().report_interval_h, 6.0)

    def test_env_override(self):
        os.environ["REPORT_INTERVAL_H"] = "3"
        try:
            self.assertEqual(Settings.from_env().report_interval_h, 3.0)
        finally:
            del os.environ["REPORT_INTERVAL_H"]

    def test_zero_disables(self):
        os.environ["REPORT_INTERVAL_H"] = "0"
        try:
            self.assertEqual(Settings.from_env().report_interval_h, 0.0)
        finally:
            del os.environ["REPORT_INTERVAL_H"]

    def test_engine_has_report_round(self):
        from app.core.engine import QuantEngine
        self.assertTrue(callable(getattr(QuantEngine, "_report_round", None)))


class TestScheduledRoundWithOpenPosition(unittest.TestCase):
    """v23.6 regression: the 6h round used to iterate the *snapshot*
    (to_dict() dicts) with attribute access -> AttributeError
    'dict' object has no attribute 'opened_at' on every scheduled round
    while a position was open. It must use state.positions() instead."""

    def _make_engine(self, tmpdir):
        import asyncio  # noqa: F401  (kept for the runner below)
        import time as _time
        import os
        from app.config import Settings
        from app.state import EngineState
        from app.models import Position
        from app.core.engine import QuantEngine
        from app.models import Metrics

        class _FakeDB:
            async def get_recent_decisions(self, n):
                return []

            async def get_closed_trades(self, n):
                return []

            async def compute_metrics(self):
                return Metrics()

        class _FakeTG:
            def __init__(self):
                self.sent = []

            async def send_document(self, path, caption):
                self.sent.append((path, caption))

        engine = QuantEngine.__new__(QuantEngine)  # skip heavy __init__
        engine.settings = Settings()
        engine.state = EngineState()
        engine.state.add_position(Position(
            id="p1", symbol="ETHUSD", side="sell", strategy="Imba_Fib",
            entry=2500.0, qty=0.01, sl=2550.0, tp1=2475.0, tp=2400.0,
            opened_at=_time.time() - 3600.0))
        engine.db = _FakeDB()
        engine.prices = {"ETHUSD": 2490.0}
        engine.tg = _FakeTG()
        return engine, os.path.join(tmpdir, "report.txt")

    def test_round_survives_open_position(self):
        import asyncio
        import tempfile
        import os
        with tempfile.TemporaryDirectory() as tmp:
            old_cwd = os.getcwd()
            os.chdir(tmp)
            try:
                engine, path = self._make_engine(tmp)
                asyncio.run(engine._report_round())  # must not raise
            finally:
                os.chdir(old_cwd)
            self.assertEqual(len(engine.tg.sent), 1)
            self.assertIn("6h report", engine.tg.sent[0][1])
            with open(path, "r", encoding="utf-8") as handle:
                txt = handle.read()
            self.assertIn("ETHUSD", txt)       # position is listed
            self.assertIn("1.0h", txt)         # hold time from opened_at

    def test_manual_report_shares_the_same_path(self):
        import asyncio
        import tempfile
        import os
        with tempfile.TemporaryDirectory() as tmp:
            old_cwd = os.getcwd()
            os.chdir(tmp)
            try:
                engine, _ = self._make_engine(tmp)
                asyncio.run(engine._tg_report())  # must not raise either
            finally:
                os.chdir(old_cwd)
            self.assertEqual(len(engine.tg.sent), 1)


if __name__ == "__main__":
    unittest.main()
