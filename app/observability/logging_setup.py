"""Logging & observability (#07): structured logs with secret redaction.

Features:

* A single logger namespace (``quant.*``) with a consistent format.
* A logging filter that redacts any accidental secret material from every
  record (API keys, base64/hex tokens) — defence in depth on top of the
  "never log secrets" coding rule.
* Optional rotating file handler in addition to the console.
"""
from __future__ import annotations

import logging
import re
from logging.handlers import RotatingFileHandler
from typing import Optional

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"

_SECRET_RE = re.compile(r"(?i)([a-f0-9]{32,})")


class SecretRedactionFilter(logging.Filter):
    """Redact long hex/base64 tokens and known secret labels in log records."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            record.msg = _SECRET_RE.sub("***", str(record.msg))
            record.msg = re.sub(
                r"(?i)(secret[\"']?\s*[:=]\s*[\"']?)[^\"'\s,}]{4,}",
                r"\1***", str(record.msg),
            )
            if record.args:
                record.args = tuple(
                    _SECRET_RE.sub("***", str(arg)) if isinstance(arg, str) else arg
                    for arg in record.args
                )
        except Exception:  # pragma: no cover - never break logging
            pass
        return True


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """Configure the root ``quant`` logger. Safe to call multiple times."""
    logger = logging.getLogger("quant")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False
    if logger.handlers:
        return logger  # already configured

    formatter = logging.Formatter(_FORMAT)
    redactor = SecretRedactionFilter()

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.addFilter(redactor)
    logger.addHandler(console)

    if log_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(redactor)
        logger.addHandler(file_handler)

    # Keep third-party noise out of our logs.
    for noisy in ("aiohttp", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logger
