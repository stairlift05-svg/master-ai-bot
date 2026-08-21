"""Risk management (#01): system-level risk gates.

A pure, IO-free gatekeeper deciding *whether the engine may trade at all*:

* Drawdown halt  — pause trading when equity drawdown reaches MAX_DD.
* Daily-loss halt — pause when the day's PnL breaches MAX_DAILY_LOSS.
* Auto-resume     — self-healing: resume (with a reduced risk factor) once
  the drawdown recovers below ``auto_resume_dd_ratio * MAX_DD`` (no human
  intervention required — a core user requirement).
* Funding drag    — skip entries that pay heavy funding.
* Per-symbol entry cooldown and post-close cooldown.

All decisions return ``(allowed: bool, reason: str)`` pairs.
"""
from __future__ import annotations

import time
from typing import Callable, Dict, Tuple

from app.config import Settings
from app.models import Position
from app.state import EngineState


class RiskManager:
    """Central trading gate (shared by scan loop and executor)."""

    def __init__(self, settings: Settings, state: EngineState,
                 clock: Callable[[], float] = time.time) -> None:
        self._settings = settings
        self._state = state
        self._clock = clock
        self._entry_cooldown: Dict[str, float] = {}       # symbol -> ts
        self._post_close_cooldown: Dict[str, float] = {}  # symbol -> ts
        self._day = -1
        self._entries_today = 0

    # ------------------------------------------------------------------
    # Halts
    # ------------------------------------------------------------------
    def update_halts(self, equity: float, peak: float,
                     day_start: float) -> Tuple[bool, str]:
        """Recompute halt flags from current equity. Returns (trading_ok, reason)."""
        s = self._settings
        dd = (peak - equity) / peak * 100.0 if peak > 0 else 0.0
        daily_pnl_pct = ((equity - day_start) / day_start * 100.0
                         if day_start > 0 else 0.0)

        dd_halted = dd >= s.max_dd_pct
        daily_halted = daily_pnl_pct <= -s.max_daily_loss_pct

        # Self-healing: resume when drawdown recovers meaningfully.
        if dd_halted and dd <= s.max_dd_pct * s.auto_resume_dd_ratio:
            dd_halted = False
        if daily_halted and daily_pnl_pct > -s.max_daily_loss_pct * 0.5:
            daily_halted = False

        self._state.set_many(
            current_dd=round(dd, 2),
            daily_pnl=round(equity - day_start, 2),
            dd_halted=dd_halted,
            daily_halted=daily_halted,
        )

        if dd_halted:
            return False, f"drawdown {dd:.1f}% >= {s.max_dd_pct}%"
        if daily_halted:
            return False, f"daily loss {daily_pnl_pct:.1f}% <= -{s.max_daily_loss_pct}%"
        return True, "ok"

    # ------------------------------------------------------------------
    # Per-symbol gates
    # ------------------------------------------------------------------
    def can_enter_symbol(self, symbol: str) -> Tuple[bool, str]:
        now = self._clock()
        if now < self._entry_cooldown.get(symbol, 0.0):
            return False, f"entry cooldown ({int(self._entry_cooldown[symbol] - now)}s)"
        if now < self._post_close_cooldown.get(symbol, 0.0):
            return False, "post-close cooldown"
        return True, "ok"

    def mark_entry(self, symbol: str) -> None:
        self._entry_cooldown[symbol] = self._clock() + self._settings.entry_cooldown_s

    def mark_close(self, symbol: str) -> None:
        self._post_close_cooldown[symbol] = (
            self._clock() + self._settings.post_close_cooldown_s
        )

    def daily_entries_left(self) -> int:
        """How many more entries are allowed today (daily trade budget)."""
        day = int(self._clock() // 86400)
        if day != self._day:
            self._day = day
            self._entries_today = 0
        return max(0, self._settings.max_daily_entries - self._entries_today)

    def mark_daily_entry(self) -> None:
        day = int(self._clock() // 86400)
        if day != self._day:
            self._day = day
            self._entries_today = 0
        self._entries_today += 1

    def mark_failure(self, symbol: str, failure_count: int) -> None:
        """Exponential per-symbol failure cooldown (backoff on API errors)."""
        s = self._settings
        wait = min(s.error_cooldown_base_s * (2 ** (failure_count - 1)),
                   s.error_cooldown_max_s)
        self._entry_cooldown[symbol] = max(
            self._entry_cooldown.get(symbol, 0.0), self._clock() + wait
        )

    # ------------------------------------------------------------------
    # Portfolio-level gates
    # ------------------------------------------------------------------
    def can_open_new(self, positions: Dict[str, Position],
                     proposed_notional: float, price: float) -> Tuple[bool, str]:
        s = self._settings
        if not self._state.get("is_active", True):
            return False, "engine paused"
        if len(positions) >= s.max_positions:
            return False, f"max positions {s.max_positions} reached"
        agg = sum(p.qty * price for p in positions.values()) + proposed_notional
        if agg > s.max_agg_notional_usd:
            return False, (f"aggregate notional ${agg:.2f} would exceed "
                           f"${s.max_agg_notional_usd:.2f}")
        return True, "ok"

    # ------------------------------------------------------------------
    # Funding gate
    # ------------------------------------------------------------------
    @staticmethod
    def funding_blocked(side: str, funding_pct: float,
                        threshold: float = 0.30) -> Tuple[bool, str]:
        """Block entries that *pay* heavy funding (funding drag protection)."""
        if abs(funding_pct) < threshold:
            return False, ""
        pays = (side == "buy" and funding_pct > 0) or (side == "sell" and funding_pct < 0)
        if pays:
            return True, f"funding drag {funding_pct:+.2f}%"
        return False, ""

    # ------------------------------------------------------------------
    # Adaptive risk factor (module #10)
    # ------------------------------------------------------------------
    def adaptive_risk_pct(self) -> float:
        """Shrink risk after drawdown bands or a loss streak.

        Returns an override for the configured ``risk_pct``.
        """
        s = self._settings
        if not s.adapt_enabled:
            return s.risk_pct
        factor = 1.0
        dd = self._state.get("current_dd", 0.0)
        if dd >= s.adapt_dd_band2_pct and len(s.adapt_factors) > 1:
            factor = s.adapt_factors[1]
        elif dd >= s.adapt_dd_band1_pct and s.adapt_factors:
            factor = s.adapt_factors[0]
        streak = self._state.get("loss_streak", 0)
        if streak >= s.loss_streak_shrink_at:
            factor = min(factor, s.loss_streak_factor)
        return s.risk_pct * factor
