"""Optimization (#10): adaptive risk, portfolio caps, and parameter tuning.

Three distinct concerns live here:

1. ``AdaptiveRisk`` — shrinks the risk-per-trade automatically when the
   equity curve is under stress (drawdown bands / loss streaks) and grows
   it back when conditions normalise.  This is the "no human intervention"
   safety governor.
2. ``PortfolioLimits`` — hard caps on aggregate exposure and per-symbol
   concentration.
3. ``grid_search`` — a tiny parameter-tuning helper for the backtest
   harness (module #05), enabling systematic optimisation of e.g. the
   stop/target ATR multiples offline.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List

from app.config import Settings
from app.models import Position  # noqa: F401 (re-export for convenience)
from app.state import EngineState


@dataclass(frozen=True)
class RiskProfile:
    """The effective risk multiplier and its driver (for reporting)."""

    factor: float
    reason: str


class AdaptiveRisk:
    """Equity-curve-aware risk scaling."""

    def __init__(self, settings: Settings, state: EngineState) -> None:
        self._settings = settings
        self._state = state

    def profile(self) -> RiskProfile:
        """Return the current risk reduction factor and the reason for it."""
        s = self._settings
        if not s.adapt_enabled:
            return RiskProfile(1.0, "adaptive risk disabled")
        factor, reason = 1.0, "normal conditions"
        dd = self._state.get("current_dd", 0.0)
        if dd >= s.adapt_dd_band2_pct and len(s.adapt_factors) > 1:
            factor, reason = s.adapt_factors[1], f"deep drawdown {dd:.1f}%"
        elif dd >= s.adapt_dd_band1_pct and s.adapt_factors:
            factor, reason = s.adapt_factors[0], f"drawdown {dd:.1f}%"
        streak = self._state.get("loss_streak", 0)
        if streak >= s.loss_streak_shrink_at:
            if factor > s.loss_streak_factor:
                factor = s.loss_streak_factor
                reason = f"{streak}-loss streak"
            else:
                reason = f"drawdown {dd:.1f}% + {streak}-loss streak"
        return RiskProfile(factor, reason)

    def effective_risk_pct(self) -> float:
        return self._settings.risk_pct * self.profile().factor


class PortfolioLimits:
    """Aggregate exposure and concentration caps."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def would_exceed(self, open_symbols: set, open_count: int, symbol: str,
                     price: float, proposed_notional: float,
                     open_notional: float = 0.0) -> str:
        """Return an error string if the proposed trade breaks a cap.

        Args:
            open_symbols: symbols currently held.
            open_count: number of currently open positions.
            symbol: proposed symbol.
            price: reference price for exposure estimation.
            proposed_notional: notional of the proposed trade.
            open_notional: known notional of open positions (0 = estimate
                via ``qty * price`` is used instead — see callers).
        """
        s = self._settings
        if symbol in open_symbols:
            return f"symbol {symbol} already open"
        if open_count >= s.max_positions:
            return f"max positions {s.max_positions} reached"
        agg = open_notional + proposed_notional
        if agg > s.max_agg_notional_usd:
            return (f"aggregate notional ${agg:.2f} exceeds "
                    f"${s.max_agg_notional_usd:.2f}")
        return ""


# ---------------------------------------------------------------------------
# Offline parameter tuning helper (used with app/backtest)
# ---------------------------------------------------------------------------


def grid_search(
    runner: Callable[[Dict], Dict],
    param_space: Dict[str, Iterable],
    metric: str = "total_return_pct",
    maximize: bool = True,
) -> List[Dict]:
    """Exhaustive grid search over a parameter space.

    Args:
        runner: callable ``f(params) -> {metric: value, ...}`` (a backtest).
        param_space: ``{param_name: iterable_of_values}``.
        metric: key of the objective metric.
        maximize: True to sort best-first by the objective.

    Returns:
        Sorted list of ``{params, result}`` records.
    """
    keys = list(param_space.keys())
    combos = list(itertools.product(*(param_space[k] for k in keys)))
    records: List[Dict] = []
    for combo in combos:
        params = dict(zip(keys, combo))
        try:
            result = runner(params)
        except Exception as exc:  # noqa: BLE001 - a bad combo must not abort
            result = {"error": str(exc)}
        records.append({"params": params, "result": result})
    records.sort(
        key=lambda r: r["result"].get(metric, float("-inf") if maximize else float("inf")),
        reverse=maximize,
    )
    return records
