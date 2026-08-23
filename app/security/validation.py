"""Security (#09): local order validation (defence in depth).

Every order passes through :class:`OrderValidator` before reaching the
exchange.  This gate enforces the symbol allowlist, side/qty/price sanity,
and a hard notional ceiling — independent of the strategy and the sizing
modules, so a bug in either cannot produce a runaway order.
"""
from __future__ import annotations

import math

from app.config import Settings
from app.errors import OrderRejectedError


class OrderValidator:
    """Local pre-trade validation gate."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def validate(self, symbol: str, side: str, qty: float, price: float,
                 notional: float) -> None:
        """Raise :class:`OrderRejectedError` on any violation."""
        if symbol not in self._settings.symbols:
            raise OrderRejectedError(
                symbol, f"symbol not in allowlist {list(self._settings.symbols)}"
            )
        if side not in ("buy", "sell"):
            raise OrderRejectedError(symbol, f"invalid side {side!r}")
        try:
            qty_f, price_f, notional_f = float(qty), float(price), float(notional)
        except (TypeError, ValueError) as exc:
            raise OrderRejectedError(symbol, "non-numeric qty/price/notional") from exc
        # NaN and infinity bypass ordinary <=/> comparisons, so reject every
        # non-finite financial value before applying limits.
        if not all(math.isfinite(v) for v in (qty_f, price_f, notional_f)):
            raise OrderRejectedError(symbol, "non-finite qty/price/notional")
        if qty_f <= 0:
            raise OrderRejectedError(symbol, f"invalid qty {qty!r}")
        if price_f <= 0:
            raise OrderRejectedError(symbol, f"invalid price {price!r}")
        if notional_f <= 0:
            raise OrderRejectedError(symbol, f"invalid notional {notional!r}")
        # Do not trust a caller-supplied notional that disagrees with qty*price.
        computed_notional = qty_f * price_f
        tolerance = max(0.01, computed_notional * 1e-9)
        if abs(notional_f - computed_notional) > tolerance:
            raise OrderRejectedError(symbol, "notional does not equal qty * price")
        hard_cap = self._settings.max_notional_usd * 1.5
        if computed_notional > hard_cap:
            raise OrderRejectedError(
                symbol,
                f"notional ${notional:.2f} exceeds hard cap ${hard_cap:.2f}",
            )
