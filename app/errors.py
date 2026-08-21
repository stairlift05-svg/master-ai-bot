"""Centralised exception taxonomy.

Every component raises (or wraps) one of these exception types so that the
orchestration layer can react predictably: retry network errors, halt on
config errors, and surface order rejections to the operator.
"""
from __future__ import annotations

from typing import Any, Optional


class QuantEngineError(Exception):
    """Base class for all engine-specific errors."""


class ConfigError(QuantEngineError):
    """Raised when environment configuration is missing or invalid."""


class AriaXAPIError(QuantEngineError):
    """Raised when the AriaX REST API returns an error status.

    Attributes:
        status: HTTP status code (0 when the request never completed).
        message: Human-readable error message.
        raw: Raw response payload (truncated) for diagnostics.
    """

    def __init__(self, message: str, status: int = 0, raw: Any = None):
        super().__init__(message)
        self.status = int(status)
        self.raw = raw
        self.message = message

    def __str__(self) -> str:  # pragma: no cover - convenience formatting
        base = f"AriaXAPIError(status={self.status}): {self.message}"
        if self.raw is not None:
            return f"{base} | raw={str(self.raw)[:200]}"
        return base


class OrderRejectedError(QuantEngineError):
    """Raised when the exchange refuses an order.

    Attributes:
        symbol: Trading symbol (AriaX format, e.g. ``ETHUSD``).
        reason: Reason reported by the exchange or the local validation gate.
        raw: Optional raw exchange payload.
    """

    def __init__(self, symbol: str, reason: str, raw: Any = None):
        super().__init__(f"Order rejected for {symbol}: {reason}")
        self.symbol = symbol
        self.reason = reason
        self.raw = raw


class DataUnavailableError(QuantEngineError):
    """Raised when no candle source is able to serve data for a symbol."""


class StateConflictError(QuantEngineError):
    """Raised on inconsistent internal/exchange state (e.g. ghost positions)."""


class BacktestError(QuantEngineError):
    """Raised by the backtest/stress harness on invalid simulation input."""
