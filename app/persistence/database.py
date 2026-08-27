"""Persistence: SQLite storage for trades, decisions, equity and meta.

Schema notes (vs v19.3):

* ``meta`` key/value table — persists ``day_start_balance`` and
  ``peak_balance`` so daily-PnL tracking and drawdown baselines survive a
  restart (v19.3 lost them on every reboot).
* Indexes on ``trades(status)`` and ``decisions(ts)`` for fast reporting.
* ``update_analytics`` computes win-rate, profit factor, expectancy, max
  drawdown and a Sharpe estimate, excluding ghost/startup closes.
"""
from __future__ import annotations

import json
import logging
import statistics
from typing import Any, Dict, List

import aiosqlite

from app.models import Metrics, Position
from app.state import EngineState

log = logging.getLogger("quant.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    strategy TEXT,
    entry_price REAL,
    qty REAL,
    original_qty REAL,
    sl REAL,
    tp1 REAL,
    tp REAL,
    is_partial INTEGER DEFAULT 0,
    highest_pnl_pct REAL DEFAULT 0,
    status TEXT DEFAULT 'open',
    pnl REAL DEFAULT 0,
    fees_est REAL DEFAULT 0,
    exit_reason TEXT,
    hold_seconds REAL DEFAULT 0,
    opened_at TEXT DEFAULT CURRENT_TIMESTAMP,
    closed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT DEFAULT CURRENT_TIMESTAMP,
    symbol TEXT,
    action TEXT,
    strategy TEXT,
    reason TEXT,
    price REAL,
    rsi REAL,
    atr REAL,
    htf_trend TEXT,
    extra TEXT
);
CREATE INDEX IF NOT EXISTS idx_decisions_ts ON decisions(ts);

CREATE TABLE IF NOT EXISTS equity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT DEFAULT CURRENT_TIMESTAMP,
    balance REAL,
    peak REAL,
    dd REAL
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


class Database:
    """Async SQLite facade."""

    def __init__(self, path: str = "bot.db") -> None:
        self.path = path

    # ------------------------------------------------------------------
    async def init(self) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(_SCHEMA)
            await db.commit()

    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------
    async def insert_trade(self, pos: Position) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """INSERT OR REPLACE INTO trades
                (id, symbol, side, strategy, entry_price, qty, original_qty,
                 sl, tp1, tp)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (pos.id, pos.symbol, pos.side, pos.strategy, pos.entry,
                 pos.qty, pos.qty, pos.sl, pos.tp1, pos.tp),
            )
            await db.commit()

    async def update_trade(self, trade_id: str, qty: float, sl: float,
                           partial: int, highest_pnl_pct: float) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """UPDATE trades SET qty=?, sl=?, is_partial=?,
                   highest_pnl_pct=? WHERE id=?""",
                (qty, sl, partial, highest_pnl_pct, trade_id),
            )
            await db.commit()

    async def close_trade(self, trade_id: str, pnl: float, fees: float = 0.0,
                          reason: str = "", hold: float = 0.0) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """UPDATE trades SET status='closed', pnl=?, fees_est=?,
                   exit_reason=?, hold_seconds=?, closed_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (pnl, fees, reason, hold, trade_id),
            )
            await db.commit()

    async def get_open_trades(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM trades WHERE status='open'") as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def get_closed_trades(self, limit: int = 40) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """SELECT * FROM trades WHERE status='closed'
                   ORDER BY closed_at DESC LIMIT ?""", (limit,),
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]

    # ------------------------------------------------------------------
    # Decisions / equity
    # ------------------------------------------------------------------
    async def log_decision(self, symbol: str, action: str, strategy: str,
                           reason: str, price: float = 0.0, rsi: float = 0.0,
                           atr: float = 0.0, htf: str = "",
                           extra: Any = "") -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """INSERT INTO decisions
                   (symbol, action, strategy, reason, price, rsi, atr,
                    htf_trend, extra)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (symbol, action, strategy, reason[:200], price, rsi, atr, htf,
                 str(extra)[:500]),
            )
            await db.commit()

    async def get_recent_decisions(self, limit: int = 250) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """SELECT * FROM decisions ORDER BY id DESC LIMIT ?""", (limit,),
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def log_equity(self, balance: float, peak: float, dd: float) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO equity (balance, peak, dd) VALUES (?,?,?)",
                (balance, peak, dd),
            )
            await db.commit()

    # ------------------------------------------------------------------
    # Meta (restart-safe state)
    # ------------------------------------------------------------------
    async def save_meta(self, key: str, value: Any) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)",
                (key, json.dumps(value)),
            )
            await db.commit()

    async def load_meta(self, key: str, default: Any = None) -> Any:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT value FROM meta WHERE key=?", (key,),
            ) as cur:
                row = await cur.fetchone()
        if row is None:
            return default
        try:
            return json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            return default

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------
    async def compute_metrics(self) -> Metrics:
        """Aggregate closed-trade statistics (ghost/startup excluded)."""
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """SELECT pnl, hold_seconds FROM trades WHERE status='closed'
                   AND IFNULL(exit_reason,'') NOT LIKE 'ghost%'
                   AND IFNULL(exit_reason,'') NOT LIKE 'startup%'
                   AND IFNULL(exit_reason,'') NOT LIKE 'stuck%'
                   AND IFNULL(exit_reason,'') NOT LIKE '%ghostresp%'""",
            ) as cur:
                rows = [dict(r) for r in await cur.fetchall()]
            async with db.execute(
                """SELECT balance FROM equity ORDER BY id""",
            ) as cur:
                equity_rows = [r[0] for r in await cur.fetchall()]

        if not rows:
            return Metrics()
        pnls = [r["pnl"] for r in rows]
        holds = [r["hold_seconds"] or 0 for r in rows]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))

        # Max drawdown from the equity curve (point-in-time peak tracking).
        peak: float = 0.0
        max_dd: float = 0.0
        for bal in equity_rows:
            if bal is None:
                continue
            peak = max(peak, bal)
            if peak > 0:
                max_dd = max(max_dd, (peak - bal) / peak * 100.0)

        # Simple Sharpe proxy from per-sample equity returns (annualised 365d).
        sharpe = 0.0
        if len(equity_rows) > 2:
            returns = [
                (equity_rows[i] / equity_rows[i - 1]) - 1.0
                for i in range(1, len(equity_rows))
                if equity_rows[i - 1] not in (None, 0)
            ]
            if len(returns) > 2 and statistics.stdev(returns) > 1e-12:
                sharpe = round(
                    statistics.fmean(returns) / statistics.stdev(returns)
                    * (365.0 * 24.0 * 60.0) ** 0.5, 2,
                )

        # Metrics is frozen — build once with kwargs (no field assignment).
        return Metrics(
            total_trades=len(pnls),
            wins=len(wins),
            losses=len(losses),
            win_rate=round(len(wins) / len(pnls) * 100.0, 1),
            total_pnl=round(sum(pnls), 2),
            gross_profit=round(gross_profit, 2),
            gross_loss=round(gross_loss, 2),
            profit_factor=(
                round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0.0
            ),
            expectancy=round(sum(pnls) / len(pnls), 4),
            avg_win=round(gross_profit / len(wins), 4) if wins else 0.0,
            avg_loss=round(-gross_loss / len(losses), 4) if losses else 0.0,
            max_dd_pct=round(max_dd, 2),
            sharpe=sharpe,
            avg_hold_s=round(statistics.fmean(holds), 1) if holds else 0.0,
        )

    async def update_analytics(self, state: EngineState) -> Metrics:
        """Compute metrics and publish them to shared state."""
        metrics = await self.compute_metrics()
        state.set_metrics({
            "total_trades": metrics.total_trades,
            "win_rate": metrics.win_rate,
            "total_pnl": metrics.total_pnl,
            "profit_factor": metrics.profit_factor,
            "expectancy": metrics.expectancy,
            "max_dd": metrics.max_dd_pct,
        })
        return metrics
