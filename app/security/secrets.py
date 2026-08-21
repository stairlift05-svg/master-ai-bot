"""Security module (#09): secret handling, redaction, input validation.

Principles enforced here:

* Secrets are loaded from the environment / ``.env`` only — never hard-coded.
* Secrets are redacted from every log line and Telegram message.
* Every outbound order is validated against a symbol allowlist and numeric
  sanity bounds *before* it reaches the exchange.
* Credential failures produce an actionable, checklist-style error.
"""
from __future__ import annotations

import os
import re
from typing import List, Optional

from app.config import Settings
from app.errors import ConfigError, OrderRejectedError

_SECRET_PATTERN_CACHE: List[re.Pattern] = []


def load_secrets() -> None:
    """Load ``.env`` if present (idempotent). Called once at startup."""
    try:
        from dotenv import load_dotenv  # optional dependency

        load_dotenv(verbose=False)
    except Exception:  # pragma: no cover - dotenv is optional
        pass


def validate_credentials(settings: Settings) -> None:
    """Raise :class:`ConfigError` with an actionable checklist if missing."""
    problems: List[str] = []
    if not settings.arlax_key:
        problems.append("ARIAX_KEY is empty — create it on the AriaX dashboard")
    if not settings.arlax_secret:
        problems.append("ARIAX_SECRET is empty — create it on the AriaX dashboard")
    if not settings.arlax_base.startswith("https://"):
        problems.append("ARIAX_BASE must be https (got %r)" % settings.arlax_base)
    if problems:
        raise ConfigError(
            "Exchange credentials are invalid:\n  - " + "\n  - ".join(problems) +
            "\nHint: copy .env.example to .env and fill in the values."
        )


def redact(text: str, settings: Optional[Settings] = None) -> str:
    """Replace any occurrence of the API key/secret with ``***``.

    Also masks common accidental secret patterns (long hex/base64 tokens).
    """
    result = str(text)
    if settings:
        for secret in (settings.arlax_key, settings.arlax_secret):
            if secret and secret in result:
                result = result.replace(secret, "***")
    # Heuristic mask for 32-128 char hex/base64 tokens that leak into logs.
    result = re.sub(r"(?i)([a-f0-9]{32,})", "***", result)
    result = re.sub(r"(?i)(secret[\"']?\s*[:=]\s*[\"']?)[^\"'\s,}]{4,}", r"\1***", result)
    return result


class OrderValidator:
    """Local pre-trade validation gate (defence in depth)."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def validate(self, symbol: str, side: str, qty: float, price: float,
                 notional: float) -> None:
        """Raise :class:`OrderRejectedError` on any violation."""
        if symbol not in self._settings.symbols:
            raise OrderRejectedError(symbol, f"symbol not in allowlist {self._settings.symbols}")
        if side not in ("buy", "sell"):
            raise OrderRejectedError(symbol, f"invalid side {side!r}")
        if not (qty > 0 and isinstance(qty, (int, float))):
            raise OrderRejectedError(symbol, f"invalid qty {qty!r}")
        if not (price > 0 and isinstance(price, (int, float))):
            raise OrderRejectedError(symbol, f"invalid price {price!r}")
        if notional <= 0:
            raise OrderRejectedError(symbol, f"invalid notional {notional!r}")
        if notional > self._settings.max_notional_usd * 1.5:
            raise OrderRejectedError(
                symbol,
                f"notional ${notional:.2f} exceeds hard cap "
                f"${self._settings.max_notional_usd * 1.5:.2f}",
            )


def is_secret(text: str) -> bool:
    """Cheap heuristic used by the logging filter."""
    return len(text) >= 32 and any(
        marker in text.lower() for marker in ("secret", "token", "apikey", "bapi")
    )
