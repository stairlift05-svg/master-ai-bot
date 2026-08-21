"""API layer (#08): request signing.

Implements the AriaX testnet signing contract:

* Legacy headers ``X-API-Key`` / ``X-API-Secret`` for ``/api/*`` endpoints.
* Bybit-v5 style ``X-BAPI-*`` headers for ``/v5/*`` endpoints, where the
  signature covers ``timestamp + api_key + recv_window + payload`` and the
  payload is the *query string* for GET requests and the raw JSON body for
  POST requests.

The secret never appears in the signature payload — only the derived HMAC —
and header building is fully deterministic for testability.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Dict

from app.config import Settings


class RequestSigner:
    """Builds authenticated headers for AriaX REST requests."""

    def __init__(self, settings: Settings):
        self._key = settings.arlax_key
        self._secret = settings.arlax_secret
        self._recv_window = str(int(settings.recv_window_ms))

    # ------------------------------------------------------------------
    def _sign(self, timestamp_ms: str, payload: str) -> str:
        message = f"{timestamp_ms}{self._key}{self._recv_window}{payload}".encode("utf-8")
        return hmac.new(
            self._secret.encode("utf-8"), message, hashlib.sha256
        ).hexdigest()

    # ------------------------------------------------------------------
    def headers(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        """Return the full header set for a request.

        Args:
            method: HTTP method (``GET`` / ``POST`` / ...).
            path: Request path including any query string, e.g.
                ``/v5/market/kline?category=linear&symbol=ETHUSDT``.
            body: Raw JSON body for POST requests (empty for GET).
        """
        timestamp = str(int(time.time() * 1000))
        if method.upper() == "GET":
            payload = path.split("?", 1)[1] if "?" in path else ""
        else:
            payload = body
        signature = self._sign(timestamp, payload)
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            # Legacy AriaX headers (used by /api/* endpoints).
            "X-API-Key": self._key,
            "X-API-Secret": self._secret,
            # Bybit-v5 style headers (used by /v5/* endpoints).
            "X-BAPI-API-KEY": self._key,
            "X-BAPI-SIGNATURE": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self._recv_window,
        }

    # ------------------------------------------------------------------
    @staticmethod
    def json_body(payload: Dict) -> str:
        """Compact, deterministic JSON serialisation for signing/body."""
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
