"""v24 sprint 1.5: POST /api/close/<pid> — operator close via dashboard token.

The closer callable is built in run.py as a thread-safe bridge to the engine
loop; these tests cover the web layer (auth gate, tuple passthrough, 501
when unwired) with a stub closer.
"""
import unittest

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


def _make(closer):
    s = EngineState()
    app = create_app(s, _FakeDB(), None, closer=closer)
    app.config["TESTING"] = True
    # recreate the gate with the testing token (create_app generated one)
    return app


class TestCloseEndpoint(unittest.TestCase):

    def test_close_requires_token(self):
        app = _make(lambda pid: ({"closed": pid}, 200))
        client = app.test_client()
        r = client.post("/api/close/p1")
        self.assertEqual(r.status_code, 401)

    def test_close_with_stable_token(self):
        import os
        os.environ["DASH_TOKEN"] = "test-token-123"
        try:
            calls = []

            def closer(pid):
                calls.append(pid)
                return {"closed": pid, "detail": "TP"}, 200

            # settings object exposing dash_token
            class _S:
                dash_token = "test-token-123"
            app = create_app(EngineState(), _FakeDB(), _S(), closer=closer)
            app.config["TESTING"] = True
            client = app.test_client()
            r = client.post("/api/close/p1?token=test-token-123")
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.get_json()["closed"], "p1")
            self.assertEqual(calls, ["p1"])
            # wrong token
            r2 = client.post("/api/close/p1?token=wrong")
            self.assertEqual(r2.status_code, 401)
        finally:
            del os.environ["DASH_TOKEN"]

    def test_close_unwired_501(self):
        import os
        os.environ["DASH_TOKEN"] = "test-token-123"
        try:
            class _S:
                dash_token = "test-token-123"
            app = create_app(EngineState(), _FakeDB(), _S(), closer=None)
            app.config["TESTING"] = True
            client = app.test_client()
            r = client.post("/api/close/p1?token=test-token-123")
            self.assertEqual(r.status_code, 501)
        finally:
            del os.environ["DASH_TOKEN"]

    def test_closer_tuple_passthrough(self):
        import os
        os.environ["DASH_TOKEN"] = "test-token-123"
        try:
            class _S:
                dash_token = "test-token-123"
            app = create_app(EngineState(), _FakeDB(), _S(),
                             closer=lambda pid: ({"error": "not found"}, 404))
            app.config["TESTING"] = True
            client = app.test_client()
            r = client.post("/api/close/ghost?token=test-token-123")
            self.assertEqual(r.status_code, 404)
            self.assertIn("not found", r.get_json()["error"])
        finally:
            del os.environ["DASH_TOKEN"]


if __name__ == "__main__":
    unittest.main()
