"""N-02: dashboard brute-force lockout + security headers."""
import unittest

from app.config import Settings
from app.server.web import create_app
from app.state import EngineState


def _app(token="unit-test-token"):
    return create_app(EngineState(), None, Settings(dash_token=token))


class TestDashboardLockout(unittest.TestCase):

    def test_health_open_and_hardened_headers(self):
        c = _app().test_client()
        r = c.get("/health")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertEqual(r.headers.get("X-Frame-Options"), "DENY")
        self.assertEqual(r.headers.get("X-Robots-Tag"), "noindex, nofollow")

    def test_wrong_token_is_401(self):
        c = _app().test_client()
        r = c.get("/api/status?token=wrong")
        self.assertEqual(r.status_code, 401)

    def test_lockout_after_repeated_failures(self):
        c = _app().test_client()
        for _ in range(8):
            self.assertEqual(c.get("/api/status?token=wrong").status_code, 401)
        # even the CORRECT token is now locked out for this client
        self.assertEqual(
            c.get("/api/status?token=unit-test-token").status_code, 429)

    def test_correct_token_resets_failure_counter(self):
        c = _app().test_client()
        for _ in range(7):  # one below the limit
            c.get("/api/status?token=wrong")
        self.assertEqual(
            c.get("/api/status?token=unit-test-token").status_code, 200)
        # counter was reset — 7 more failures still do not lock the client
        for _ in range(7):
            c.get("/api/status?token=wrong")
        self.assertEqual(
            c.get("/api/status?token=unit-test-token").status_code, 200)


if __name__ == "__main__":
    unittest.main()
