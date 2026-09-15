"""v30 identity: the bot is named DonchianGuard, version 30 (owner directive
2026-09-15). Every surface it presents — dashboard, /health, /api/status —
must carry the distinct name so it can never be confused with the other
bots."""
import unittest

from app.config import BOT_NAME, BOT_VERSION, Settings
from app.state import EngineState
from app.server.web import create_app


class _FakeDB:
    async def init(self):
        return None

    async def get_recent_decisions(self, n):
        return []

    async def get_closed_trades(self, n):
        return []

    async def compute_metrics(self):
        from app.models import Metrics
        return Metrics()


def _make():
    class _S:
        dash_token = "test-token-123"
    app = create_app(EngineState(), _FakeDB(), _S())
    app.config["TESTING"] = True
    return app


class TestV30Identity(unittest.TestCase):

    def test_constants(self):
        self.assertEqual(BOT_NAME, "DonchianGuard")
        self.assertEqual(BOT_VERSION, "30")

    def test_settings_defaults(self):
        s = Settings.from_env()
        self.assertEqual(s.bot_name, "DonchianGuard")
        self.assertEqual(s.bot_version, "30")

    def test_health_carries_distinct_name_and_version(self):
        client = _make().test_client()
        r = client.get("/health")
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertTrue(d["ok"])
        self.assertEqual(d["service"], "donchianguard")
        self.assertEqual(d["name"], "DonchianGuard")
        self.assertEqual(d["version"], "30")

    def test_dashboard_title_is_distinct(self):
        client = _make().test_client()
        html = client.get("/?token=test-token-123").get_data(as_text=True)
        self.assertIn("DonchianGuard", html)
        self.assertIn("v30", html)
        self.assertNotIn("IMBA ALGO Engine", html)

    def test_status_carries_identity(self):
        client = _make().test_client()
        r = client.get("/api/status?token=test-token-123")
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertEqual(d["bot_name"], "DonchianGuard")
        self.assertEqual(d["bot_version"], "30")


if __name__ == "__main__":
    unittest.main()
