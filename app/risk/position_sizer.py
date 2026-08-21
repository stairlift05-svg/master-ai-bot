"""Risk management (#01): ATR-based position sizing.

The sizer answers one question: *given a stop distance, how much quantity can
we buy while risking at most ``risk_pct`` of free margin?*

The calculation chain (each step is a hard cap, in order):

    1. risk budget   = free_balance * risk_pct / 100
    2. qty_by_risk   = risk_budget / stop_distance      (distance from ATR stop)
    3. qty_by_notional = MAX_NOTIONAL / price           (per-position notional cap)
    4. qty_by_margin = free_balance * reserve_factor * leverage / price
    5. qty = min(step 2..4), floor-quantized to the exchange step size
    6. refuse the trade if the resulting notional < MIN_ORDER (no "bumping"
       the size up to meet a minimum — that would silently break the risk
       budget, a defect of the original implementation).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.config import Settings
from app.models import MarketMeta
from app.strategy.indicators import quantize


@dataclass(frozen=True)
class SizeOutcome:
    """Result of the sizing computation."""

    ok: bool
    qty: float = 0.0
    notional: float = 0.0
    margin: float = 0.0
    risk_usd: float = 0.0
    reason: str = ""


class PositionSizer:
    """Deterministic, risk-first position sizing."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    # ------------------------------------------------------------------
    def compute(self, symbol: str, price: float, stop: float,
                free_balance: float, meta: Optional[MarketMeta] = None,
                risk_pct: Optional[float] = None) -> SizeOutcome:
        """Compute the maximum safe quantity.

        Args:
            symbol: AriaX symbol (for metadata lookups).
            price: Reference entry price.
            stop: Stop-loss price.
            free_balance: Free futures margin in USDT.
            meta: Optional exchange qty rules.
            risk_pct: Override for the configured risk (adaptive risk uses
                this to shrink exposure).

        Returns:
            :class:`SizeOutcome` — ``ok=False`` with a reason when the trade
            must be skipped.
        """
        s = self._settings
        if price <= 0:
            return SizeOutcome(False, reason="price <= 0")
        if free_balance < s.min_free_margin:
            return SizeOutcome(False, reason=f"free margin ${free_balance:.2f} < min ${s.min_free_margin:.2f}")

        risk_pct = risk_pct if risk_pct is not None else s.risk_pct
        risk_pct = max(0.0, min(risk_pct, s.risk_pct * 2.0))  # clamp override

        distance = abs(price - stop)
        if distance <= 0 or distance / price < s.min_stop_pct:
            distance = price * s.min_stop_pct  # floor the stop distance

        risk_usd = free_balance * (risk_pct / 100.0)
        qty_by_risk = risk_usd / distance
        qty_by_notional = s.max_notional_usd / price
        qty_by_margin = free_balance * s.margin_reserve_factor * s.leverage / price

        raw_qty = min(qty_by_risk, qty_by_notional, qty_by_margin)

        # ---- quantize to exchange step -------------------------------
        step = meta.step if meta else 0.0
        min_qty = meta.min_qty if meta else 0.0
        qty = quantize(raw_qty, step)
        if min_qty > 0 and 0 < qty < min_qty:
            qty = min_qty
        elif qty < 1e-12:
            return SizeOutcome(False, reason="qty quantized to 0")

        notional = qty * price
        margin = notional / s.leverage

        # ---- refuse rather than over-risk -----------------------------
        if notional < s.min_order_usd:
            return SizeOutcome(
                False, qty=qty, notional=notional, margin=margin, risk_usd=risk_usd,
                reason=f"notional ${notional:.2f} < min ${s.min_order_usd:.2f}",
            )
        if margin > free_balance * s.margin_util_cap:
            return SizeOutcome(
                False, qty=qty, notional=notional, margin=margin, risk_usd=risk_usd,
                reason=f"margin ${margin:.2f} exceeds ${free_balance * s.margin_util_cap:.2f}",
            )
        return SizeOutcome(True, qty=qty, notional=notional, margin=margin,
                           risk_usd=risk_usd, reason="ok")
