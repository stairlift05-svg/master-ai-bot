"""Trade execution (#03): position lifecycle watchdog.

Runs every ~2 s and applies the exit policy to every open position:

* stop-loss / take-profit hits (intrabar aware via the latest price),
* break-even stop after a partial take-profit,
* ATR-based or step-based trailing stop (only ratchets, never loosens),
* partial take-profit at TP1 after a minimum hold with a minimum profit,
* maximum hold time — force close (time stop),
* data-stall protection — if no price arrives for a grace period, close
  defensively rather than fly blind.

The watchdog never mutates shared state without going through ``OrderExecutor``
or ``EngineState``, eliminating the races present in the original v19.3 loop.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, List, Optional, Tuple

from app.config import Settings
from app.execution.executor import OrderExecutor
from app.models import Position
from app.persistence.database import Database
from app.state import EngineState

log = logging.getLogger("quant.watchdog")


class PositionWatchdog:
    """Applies exit policy and triggers closes through the executor."""

    def __init__(self, settings: Settings, state: EngineState,
                 executor: OrderExecutor, db: Database) -> None:
        self._settings = settings
        self._state = state
        self._executor = executor
        self._db = db

    # ------------------------------------------------------------------
    async def scan_once(self, price_provider: Callable[[str], Tuple[float, float]],
                        live_price: Callable[[str], float]) -> List[str]:
        """Check every open position once. Returns the close reasons fired."""
        fired: List[str] = []
        for position in self._state.positions().values():
            if position.strategy == "RealTest":
                continue
            reason = await self._evaluate(position, price_provider, live_price)
            if reason:
                fired.append(reason)
                result = await self._executor.close(position, reason)
                if result is None and self._state.position(position.id) is not None:
                    log.warning("close failed for %s (%s); will retry",
                                position.symbol, reason)
        return fired

    # ------------------------------------------------------------------
    async def _evaluate(self, position: Position,
                        price_provider: Callable[[str], Tuple[float, float]],
                        live_price: Callable[[str], float]) -> Optional[str]:
        s = self._settings
        now = time.time()
        # Stuck symbols: the exchange keeps reporting the position after
        # confirmed closes. Auto-management is suspended (the sync loop
        # allows exactly one recovery+close attempt per re-check window).
        if position.symbol in (self._state.get("stuck_symbols", set()) or set()):
            return None
        price, age = price_provider(position.symbol)
        hold = max(0.0, now - position.opened_at)

        # ---- Data-stall protection -----------------------------------
        if price <= 0 and s.close_on_data_stall and age > s.data_stall_grace_s:
            log.warning("DATA STALL %s (age=%.0fs) -> defensive close",
                        position.symbol, age)
            return "DataStall"
        if price <= 0:
            return None  # keep waiting for data

        # ---- Maximum hold (time stop) ---------------------------------
        if hold >= s.max_hold_s:
            return "MaxHold"

        pnl_pct = position.pnl_pct(price)

        # ---- Partial take-profit + break-even -------------------------
        if s.partial_tp and position.is_partial == 0 and hold >= s.min_hold_partial_s:
            hit = ((position.side == "buy" and price >= position.tp1) or
                   (position.side == "sell" and price <= position.tp1))
            if hit and pnl_pct >= s.min_profit_be_pct:
                if await self._executor.partial_tp(position):
                    return None  # position continues with the remaining half

        # ---- Trailing stop --------------------------------------------
        if hold >= s.min_hold_trail_s and pnl_pct > s.trail_act_pct \
                and pnl_pct > position.highest_pnl_pct:
            self._state.update_position(position.id, highest_pnl_pct=pnl_pct)
            position.highest_pnl_pct = pnl_pct  # keep local mirror fresh
            await self._apply_trail(position, price)

        # ---- Hard exits ------------------------------------------------
        sl_hit = ((position.side == "buy" and price <= position.sl) or
                  (position.side == "sell" and price >= position.sl))
        tp_hit = ((position.side == "buy" and price >= position.tp) or
                  (position.side == "sell" and price <= position.tp))
        if sl_hit or tp_hit:
            if sl_hit and position.is_partial == 1 and abs(position.sl - position.entry) < 1e-8:
                return "BE"
            if sl_hit and position.highest_pnl_pct > s.trail_act_pct:
                return "TrailStop"
            return "SL" if sl_hit else "TP"
        return None

    # ------------------------------------------------------------------
    async def _apply_trail(self, position: Position, price: float) -> None:
        """Ratchet the stop closer to price (ATR-based, min step floor).

        The stop is only ever tightened, never loosened, and the new level is
        persisted to SQLite so a restart keeps the same protective stop.
        """
        s = self._settings
        step_dist = price * s.trail_step_pct / 100.0
        if s.use_atr_trail and position.atr_at_entry > 0:
            atr_dist = position.atr_at_entry * s.atr_trail_mult
            dist = max(step_dist, atr_dist)
        else:
            dist = step_dist
        new_sl: Optional[float] = None
        if position.side == "buy":
            candidate = price - dist
            if candidate > position.sl:
                new_sl = candidate
        else:
            candidate = price + dist
            if candidate < position.sl:
                new_sl = candidate
        if new_sl is not None:
            self._state.update_position(position.id, sl=new_sl)
            await self._db.update_trade(
                position.id, position.qty, new_sl, position.is_partial,
                position.highest_pnl_pct,
            )
            log.info("TRAIL %s %s sl->%.4f", position.symbol,
                     position.side, new_sl)
