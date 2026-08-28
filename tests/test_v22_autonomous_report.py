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


if __name__ == "__main__":
    unittest.main()
