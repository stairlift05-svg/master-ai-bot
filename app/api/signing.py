"""API layer (#08): request signing.

Implements the AriaX testnet signing contract:

* Legacy headers ``X-API-Key`` / ``X-API-Secret`` for ``/api/*`` endpoints.
* Bybit-v5 style ``X-BAPI-*`` headers for ``/v5/*`` endpoints, where the
  signature covers ``timestamp + api_key + recv_window + payload`` and the
  payload is the *query string* for GET requests and the raw JSON body for
  POST requests.

The HMAC signature covers the payload; the signature itself never contains
the secret. **Review note (F-06, 2026-08-28):** the legacy contract is
believed to authenticate ``/api/*`` calls with the raw ``X-API-Secret``
header, so it is still sent by default — but that is a design risk (the
secret then travels in a plaintext header on every request and can end up
in proxy/log stores). The OpenAPI spec publishes no security scheme, so we
cannot prove the header is required; it is therefore now a **config option**
(``ARIAX_SEND_SECRET_HEADER``). Once you have verified the exchange accepts
requests without it (one private call each way), set it to ``false`` and the
raw secret no longer leaves the process — only the derived HMAC does.
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
        self._send_secret_header = bool(getattr(settings, "send_secret_header", True))

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
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            # Legacy AriaX headers (used by /api/* endpoints).
            "X-API-Key": self._key,
            # Bybit-v5 style headers (used by /v5/* endpoints).
            "X-BAPI-API-KEY": self._key,
            "X-BAPI-SIGNATURE": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self._recv_window,
        }
        # Review F-06: the legacy contract is believed to authenticate
        # /api/* calls with the raw X-API-Secret header. It is sent only
        # while ARIAX_SEND_SECRET_HEADER=true (default, compatibility).
        # Turn it off once you have verified the exchange still accepts
        # the calls — then the raw secret never leaves the process.
        if self._send_secret_header:
            headers["X-API-Secret"] = self._secret
        return headers

    # ------------------------------------------------------------------
    @staticmethod
    def json_body(payload: Dict) -> str:
        """Compact, deterministic JSON serialisation for signing/body."""
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
