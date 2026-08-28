"""v22: rich Telegram trade messages + TG_NOTIFY_TRADES toggle."""
import os
import unittest

from app.config import Settings
from app.notify.telegram import (format_close_message,
                                 format_open_message)


class TestOpenMessage(unittest.TestCase):

    def test_contains_everything_an_operator_needs(self):
        msg = format_open_message(symbol="ETHUSD", side="buy",
                                  strategy="Donchian_Trend", entry=3456.78,
                                  qty=0.0231, sl=3390.1, tp=4100.2,
                                  paper=True, reason="40-bar breakout")
        for token in ("OPEN BUY", "ETHUSD", "3456.78", "SL 3390.1",
                      "TP 4100.2", "R:R", "PAPER", "40-bar breakout"):
            self.assertIn(token, msg)

    def test_mode_tag_switches_to_live(self):
        msg = format_open_message(symbol="X", side="sell", strategy="s",
                                  entry=1.0, qty=1.0, sl=0.9, tp=1.2,
                                  paper=False)
        self.assertIn("LIVE", msg)
        self.assertNotIn("PAPER", msg)

    def test_rr_is_reward_over_risk(self):
        msg = format_open_message(symbol="X", side="buy", strategy="s",
                                  entry=100.0, qty=1.0, sl=90.0, tp=130.0,
                                  paper=True)
        self.assertIn("1:3.0", msg)


class TestCloseMessage(unittest.TestCase):

    def test_contains_pnl_reason_hold_and_balance(self):
        msg = format_close_message(symbol="ETHUSD", side="sell",
                                   entry=100.0, exit_price=95.0, qty=2.0,
                                   pnl=9.7, fees=0.3, reason="SL",
                                   hold_s=90000, paper=True, balance=509.7)
        for token in ("CLOSE SELL", "ETHUSD", "+9.70$", "-5.00%",
                      "fees $0.30", "SL", "held 1d 1h", "509.70"):
            self.assertIn(token, msg)

    def test_loss_emoji_and_short_hold(self):
        msg = format_close_message(symbol="X", side="buy", entry=100.0,
                                   exit_price=99.0, qty=1.0, pnl=-0.9,
                                   fees=0.1, reason="Trail", hold_s=300,
                                   paper=True)
        self.assertTrue(msg.startswith("🔴"))
        self.assertIn("5m", msg)


class TestToggle(unittest.TestCase):

    def test_default_on(self):
        self.assertTrue(Settings().tg_notify_trades)

    def test_env_can_mute(self):
        os.environ["TG_NOTIFY_TRADES"] = "false"
        try:
            self.assertFalse(Settings.from_env().tg_notify_trades)
        finally:
            del os.environ["TG_NOTIFY_TRADES"]


if __name__ == "__main__":
    unittest.main()
