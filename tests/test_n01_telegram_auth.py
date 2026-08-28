"""N-01: Telegram callback sender authentication.

Before this fix, _poll_once handled callback queries from ANY chat: a
forwarded dashboard message keeps working inline buttons, so any Telegram
user who received a forward could pause the engine or close positions.
"""
import unittest

from app.config import Settings
from app.notify.telegram import TelegramController


def _controller(chat_id: str) -> TelegramController:
    settings = Settings(tg_token="unit-test-token", tg_chat_id=chat_id)
    return TelegramController(settings, None, {})


class TestTelegramSenderAuth(unittest.TestCase):

    def test_callback_from_configured_chat_is_allowed(self):
        ctl = _controller("424242")
        cb = {"data": "pause", "message": {"chat": {"id": 424242}}}
        self.assertTrue(ctl._sender_allowed(cb))

    def test_chat_id_matches_as_string(self):
        """Telegram sends numeric ids; the env var is a string."""
        ctl = _controller("424242")
        cb = {"data": "pause", "message": {"chat": {"id": "424242"}}}
        self.assertTrue(ctl._sender_allowed(cb))

    def test_callback_from_other_chat_is_rejected(self):
        ctl = _controller("424242")
        cb = {"data": "close_pos1", "message": {"chat": {"id": 1337}}}
        self.assertFalse(ctl._sender_allowed(cb))

    def test_callback_without_chat_block_is_rejected(self):
        ctl = _controller("424242")
        self.assertFalse(ctl._sender_allowed({"data": "pause"}))

    def test_no_configured_chat_rejects_everything(self):
        """Without TELEGRAM_CHAT_ID nobody may drive the bot."""
        ctl = _controller("")
        cb = {"data": "pause", "message": {"chat": {"id": 1}}}
        self.assertFalse(ctl._sender_allowed(cb))


if __name__ == "__main__":
    unittest.main()
