"""Thread-safe shared engine state.

The engine runs asyncio loops while Flask serves HTTP in a separate thread;
``EngineState`` guards every mutation with an ``RLock`` and offers a cheap
``snapshot()`` for cross-thread reads (dashboard, Telegram, reports).
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Any, Dict

from app.models import Position


class EngineState:
    """Central mutable state with a coarse-grained lock.

    Attribute-style access (``state.balance``) is intentionally avoided in
    favour of explicit getters/setters so every read/write is auditable.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._d: Dict[str, Any] = {
            "is_active": True,
            "dd_halted": False,
            "daily_halted": False,
            "balance": 0.0,
            "free_balance": 0.0,
            "peak_balance": 0.0,
            "day_start_balance": 0.0,
            "current_dd": 0.0,
            "daily_pnl": 0.0,
            "last_scan": "Never",
            "last_sync": "Never",
            "loss_streak": 0,
            "active_positions": {},          # pid -> Position
            "trend_strengths": deque(maxlen=60),
            "fetch_stats": defaultdict(lambda: {"ok_5m": 0, "fail_5m": 0,
                                                "ok_1h": 0, "fail_1h": 0}),
            "recent_errors": deque(maxlen=25),
            "signal_but_not_executed": deque(maxlen=20),
        }
        self._metrics: Dict[str, float] = {"total_trades": 0, "win_rate": 0.0,
                                           "total_pnl": 0.0}

    # ------------------------------------------------------------------
    # Generic helpers
    # ------------------------------------------------------------------
    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._d.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._d[key] = value

    def set_many(self, **values: Any) -> None:
        """Atomically update several keys under one lock acquisition."""
        with self._lock:
            self._d.update(values)

    def snapshot(self) -> Dict[str, Any]:
        """Return a JSON-serialisable deep-ish copy for external consumers."""
        with self._lock:
            return {
                "is_active": self._d["is_active"],
                "dd_halted": self._d["dd_halted"],
                "daily_halted": self._d["daily_halted"],
                "balance": self._d["balance"],
                "free_balance": self._d["free_balance"],
                "peak_balance": self._d["peak_balance"],
                "day_start_balance": self._d["day_start_balance"],
                "current_dd": self._d["current_dd"],
                "daily_pnl": self._d["daily_pnl"],
                "last_scan": self._d["last_scan"],
                "last_sync": self._d["last_sync"],
                "loss_streak": self._d["loss_streak"],
                "active_positions": {
                    pid: pos.to_dict() for pid, pos in self._d["active_positions"].items()
                },
                "trend_strengths": list(self._d["trend_strengths"]),
                "fetch_stats": {k: dict(v) for k, v in self._d["fetch_stats"].items()},
                "recent_errors": list(self._d["recent_errors"]),
                "signal_but_not_executed": list(self._d["signal_but_not_executed"]),
                "stats": dict(self._metrics),
            }

    # ------------------------------------------------------------------
    # Position registry
    # ------------------------------------------------------------------
    def positions(self) -> Dict[str, Position]:
        with self._lock:
            return dict(self._d["active_positions"])

    def position(self, pid: str) -> Position | None:
        with self._lock:
            return self._d["active_positions"].get(pid)

    def add_position(self, position: Position) -> None:
        with self._lock:
            self._d["active_positions"][position.id] = position

    def remove_position(self, pid: str) -> Position | None:
        with self._lock:
            return self._d["active_positions"].pop(pid, None)

    def update_position(self, pid: str, **changes: Any) -> None:
        with self._lock:
            pos = self._d["active_positions"].get(pid)
            if pos is not None:
                for key, value in changes.items():
                    setattr(pos, key, value)

    # ------------------------------------------------------------------
    # Counters / diagnostics
    # ------------------------------------------------------------------
    def record_fetch(self, symbol: str, timeframe: str, ok: bool) -> None:
        with self._lock:
            stats = self._d["fetch_stats"][symbol]
            key = f"ok_{timeframe}" if ok else f"fail_{timeframe}"
            stats[key] += 1

    def record_error(self, message: str) -> None:
        with self._lock:
            self._d["recent_errors"].append(
                f"{time.strftime('%H:%M:%S')} {str(message)[:120]}"
            )

    def record_missed_signal(self, message: str) -> None:
        with self._lock:
            self._d["signal_but_not_executed"].append(
                f"{time.strftime('%H:%M:%S')} {str(message)[:120]}"
            )

    def record_trend_strength(self, symbol: str, value: float) -> None:
        with self._lock:
            self._d["trend_strengths"].append(
                {"ts": time.strftime("%H:%M:%S"), "symbol": symbol,
                 "value": round(value, 3)}
            )

    def set_metrics(self, metrics: Dict[str, float]) -> None:
        with self._lock:
            self._metrics = dict(metrics)

    def metrics(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._metrics)

    def bump_loss_streak(self, won: bool) -> None:
        with self._lock:
            if won:
                self._d["loss_streak"] = 0
            else:
                self._d["loss_streak"] += 1
