#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Master Quant Engine v15.0
Testnet-first, Phemex-aligned data, protected execution, detailed reporting.

Important:
- Default mode is Phemex Testnet.
- Do not use real-account keys while validating this version.
- Exchange-specific protective-order parameters must be verified against the
  Phemex account and current ccxt behaviour before enabling auto-trading.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from threading import Lock, Thread
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite
import ccxt.async_support as ccxt
import pandas as pd
from aiohttp import ClientSession
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash, generate_password_hash


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

load_dotenv()

APP_VERSION = "15.0"
BOT_MODE = os.getenv("BOT_MODE", "testnet").strip().lower()
TESTNET = os.getenv("PHEMEX_TESTNET", "true").lower() in ("true", "1", "yes")

if BOT_MODE != "testnet" or not TESTNET:
    raise RuntimeError(
        "Safety lock: this version is configured for Testnet only. "
        "Set BOT_MODE=testnet and PHEMEX_TESTNET=true."
    )

PHEMEX_API_KEY = os.getenv("PHEMEX_API_KEY", "")
PHEMEX_API_SECRET = os.getenv("PHEMEX_API_SECRET", "")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
TG_ALLOWED_CHAT = os.getenv("TELEGRAM_ALLOWED_CHAT_ID", TG_CHAT)

DASHBOARD_USER = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")
DASHBOARD_PASSWORD_HASH = (
    generate_password_hash(DASHBOARD_PASSWORD) if DASHBOARD_PASSWORD else ""
)

PORT = int(os.getenv("PORT", "10000"))
AUTO_TRADING = os.getenv("AUTO_TRADING", "false").lower() in ("true", "1", "yes")
ENABLE_REAL_TEST_BUTTON = (
    os.getenv("ENABLE_REAL_TEST_BUTTON", "false").lower() in ("true", "1", "yes")
)

SYMBOLS = [
    "ETH/USDT:USDT",
    "BNB/USDT:USDT",
    "XRP/USDT:USDT",
    "ADA/USDT:USDT",
    "DOT/USDT:USDT",
]

TIMEFRAME = "5m"
HTF_TIMEFRAME = "1h"
CANDLE_LIMIT_5M = 180
CANDLE_LIMIT_1H = 260
MAX_CANDLE_AGE_SECONDS = 180

LEVERAGE = 3
RISK_PCT_PER_TRADE = 0.25
MAX_OPEN_POSITIONS = 3
MAX_DIRECTIONAL_POSITIONS = 2
MAX_PORTFOLIO_RISK_PCT = 0.75
MAX_NOTIONAL_EXPOSURE_PCT = 25.0

MAX_DRAWDOWN_PCT = 5.0
MAX_DAILY_LOSS_PCT = 2.0
CONSECUTIVE_LOSS_LIMIT = 2
SYMBOL_COOLDOWN_SECONDS = 4 * 60 * 60

MIN_ORDER_USD = 15.0
MAX_SPREAD_PCT = 0.35
MAX_ENTRY_DEVIATION_PCT = 0.45

TAKER_FEE_RATE = 0.0006
FEE_BUFFER = 1.15
ESTIMATED_SLIPPAGE_PCT = 0.03

SCAN_INTERVAL_SECONDS = 60
PRICE_LOOP_SECONDS = 7
WATCHDOG_SECONDS = 3
SYNC_INTERVAL_SECONDS = 90

PARTIAL_TP_ENABLED = True
PARTIAL_CLOSE_FRACTION = 0.50
TRAIL_ACTIVATION_PCT = 1.20
TRAIL_DISTANCE_PCT = 0.55
EMERGENCY_STOP_PCT = 1.25

STRATEGY_PARAMS = {
    "Breakout_Momentum": {"sl_atr": 1.15, "tp1_atr": 1.40, "tp_atr": 2.60},
    "MTF_Pullback": {"sl_atr": 1.20, "tp1_atr": 1.30, "tp_atr": 2.40},
    "SuperTrend_Pullback": {"sl_atr": 1.15, "tp1_atr": 1.25, "tp_atr": 2.30},
    "Volume_Surge": {"sl_atr": 1.10, "tp1_atr": 1.20, "tp_atr": 2.10},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler("quant_v15.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("QuantV15")

STATE_LOCK = Lock()

SHARED_STATE: Dict[str, Any] = {
    "version": APP_VERSION,
    "mode": BOT_MODE,
    "auto_trading": AUTO_TRADING,
    "is_active": True,
    "risk_halted": False,
    "daily_halted": False,
    "drawdown_halted": False,
    "balance": 0.0,
    "free_balance": 0.0,
    "peak_balance": 0.0,
    "day_start_balance": 0.0,
    "daily_pnl": 0.0,
    "daily_pnl_pct": 0.0,
    "current_drawdown_pct": 0.0,
    "active_positions": {},
    "last_scan": "Never",
    "last_sync": "Never",
    "last_error": "",
    "stats": {
        "total_trades": 0,
        "win_rate": 0.0,
        "total_pnl": 0.0,
        "profit_factor": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
    },
}

SYMBOL_COOLDOWNS: Dict[str, float] = {}
SYMBOL_CONSECUTIVE_LOSSES: Dict[str, int] = {}


# =============================================================================
# 2. HELPERS
# =============================================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def now_ts() -> float:
    return time.time()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def pct_change(current: float, reference: float) -> float:
    if reference <= 0:
        return 0.0
    return (current - reference) / reference * 100.0


def side_sign(side: str) -> int:
    return 1 if side.lower() == "buy" else -1


def is_allowed_telegram_chat(chat_id: Any) -> bool:
    if not TG_ALLOWED_CHAT:
        return False
    return str(chat_id) == str(TG_ALLOWED_CHAT)


# =============================================================================
# 3. DATABASE
# =============================================================================

class Database:
    def __init__(self, path: str = "bot_v15.db"):
        self.path = path

    async def init(self) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    exchange_symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    original_qty REAL NOT NULL,
                    remaining_qty REAL NOT NULL,
                    stop_loss REAL NOT NULL,
                    take_profit_1 REAL NOT NULL,
                    take_profit_final REAL NOT NULL,
                    exchange_entry_order_id TEXT,
                    stop_order_id TEXT,
                    tp_order_id TEXT,
                    highest_pnl_pct REAL DEFAULT 0,
                    realized_pnl REAL DEFAULT 0,
                    estimated_fees REAL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'open',
                    opened_at TEXT NOT NULL,
                    closed_at TEXT,
                    exit_reason TEXT,
                    metadata TEXT DEFAULT '{}'
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_events (
                    id TEXT PRIMARY KEY,
                    trade_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    price REAL,
                    qty REAL,
                    pnl REAL DEFAULT 0,
                    fees REAL DEFAULT 0,
                    order_id TEXT,
                    reason TEXT,
                    payload TEXT DEFAULT '{}'
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    strategy TEXT,
                    reason TEXT,
                    price REAL,
                    atr REAL,
                    rsi REAL,
                    htf_trend TEXT,
                    extra TEXT DEFAULT '{}'
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS equity_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    balance REAL NOT NULL,
                    peak_balance REAL NOT NULL,
                    drawdown_pct REAL NOT NULL,
                    daily_pnl REAL NOT NULL
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS runtime_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            await db.commit()

    async def save_runtime(self, key: str, value: Any) -> None:
        encoded = json.dumps(value, ensure_ascii=False)
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                INSERT INTO runtime_state (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (key, encoded, utc_now()),
            )
            await db.commit()

    async def load_runtime(self, key: str, default: Any = None) -> Any:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT value FROM runtime_state WHERE key=?",
                (key,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return default
                try:
                    return json.loads(row[0])
                except json.JSONDecodeError:
                    return default

    async def insert_trade(self, trade: Dict[str, Any]) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO trades (
                    id, exchange_symbol, side, strategy, entry_price,
                    original_qty, remaining_qty, stop_loss,
                    take_profit_1, take_profit_final,
                    exchange_entry_order_id, stop_order_id, tp_order_id,
                    highest_pnl_pct, realized_pnl, estimated_fees, status,
                    opened_at, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade["id"],
                    trade["symbol"],
                    trade["side"],
                    trade["strategy"],
                    trade["entry"],
                    trade["original_qty"],
                    trade["qty"],
                    trade["sl"],
                    trade["tp1"],
                    trade["tp"],
                    trade.get("entry_order_id"),
                    trade.get("stop_order_id"),
                    trade.get("tp_order_id"),
                    trade.get("highest_pnl_pct", 0.0),
                    trade.get("realized_pnl", 0.0),
                    trade.get("estimated_fees", 0.0),
                    "open",
                    trade["opened_at"],
                    json.dumps(trade.get("metadata", {}), ensure_ascii=False),
                ),
            )
            await db.commit()

    async def update_trade(self, trade: Dict[str, Any]) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                [redacted]des
                SET remaining_qty=?, stop_loss=?, take_profit_1=?,
                    take_profit_final=?, stop_order_id=?, tp_order_id=?,
                    highest_pnl_pct=?, realized_pnl=?, estimated_fees=?,
                    metadata=?
                WHERE id=?
                """,
                (
                    trade["qty"],
                    trade["sl"],
                    trade["tp1"],
                    trade["tp"],
                    trade.get("stop_order_id"),
                    trade.get("tp_order_id"),
                    trade.get("highest_pnl_pct", 0.0),
                    trade.get("realized_pnl", 0.0),
                    trade.get("estimated_fees", 0.0),
                    json.dumps(trade.get("metadata", {}), ensure_ascii=False),
                    trade["id"],
                ),
            )
            await db.commit()

    async def close_trade(
        self,
        trade_id: str,
        realized_pnl: float,
        fees: float,
        reason: str,
    ) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                UPDATE trades
                SET status='closed',
                    realized_pnl=?,
                    estimated_fees=?,
                    exit_reason=?,
                    closed_at=?
                WHERE id=?
                """,
                (realized_pnl, fees, reason, utc_now(), trade_id),
            )
            await db.commit()

    async def add_trade_event(
        self,
        trade_id: str,
        event_type: str,
        price: float = 0.0,
        qty: float = 0.0,
        pnl: float = 0.0,
        fees: float = 0.0,
        order_id: str = "",
        reason: str = "",
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                [redacted] trade_events (
                    id, trade_id, event_type, event_time, price, qty,
                    pnl, fees, order_id, reason, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"evt_{uuid.uuid4().hex}",
                    trade_id,
                    event_type,
                    utc_now(),
                    price,
                    qty,
                    pnl,
                    fees,
                    order_id,
                    reason,
                    json.dumps(payload or {}, ensure_ascii=False),
                ),
            )
            await db.commit()

    async def log_decision(
        self,
        symbol: str,
        action: str,
        strategy: str,
        reason: str,
        price: float = 0.0,
        atr: float = 0.0,
        rsi: float = 0.0,
        htf_trend: str = "",
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                INSERT INTO decisions (
                    created_at, symbol, action, strategy, reason,
                    price, atr, rsi, htf_trend, extra
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    utc_now(),
                    symbol,
                    action,
                    strategy,
                    reason,
                    price,
                    atr,
                    rsi,
                    htf_trend,
                    json.dumps(extra or {}, ensure_ascii=False),
                ),
            )
            await db.commit()

    async def log_equity(
        self,
        balance: float,
        peak_balance: float,
        drawdown_pct: float,
        daily_pnl: float,
    ) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """
                INSERT INTO equity_snapshots (
                    created_at, balance, peak_balance, drawdown_pct, daily_pnl
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (utc_now(), balance, peak_balance, drawdown_pct, daily_pnl),
            )
            await db.commit()

    async def get_open_trades(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM trades WHERE status='open' [redacted]ned_at ASC"
            ) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def get_recent_closed_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM trades
                WHERE status='closed'
                ORDER BY closed_at DESC
                LIMIT ?
                """,
                (limit,),
            ) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def get_recent_decisions(self, limit: int = 200) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM decisions ORDER BY id DESC LIMIT ?",
                (limit,),
            ) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def update_analytics(self) -> Dict[str, Any]:
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                """
                [redacted] realized_pnl
                FROM trades
                WHERE status='closed'
                """
            ) as cursor:
                rows = await cursor.fetchall()

        pnls = [safe_float(row[0]) for row in rows]
        if not pnls:
            stats = {
                "total_trades": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "profit_factor": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
            }
        else:
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p < 0]
            gross_profit = sum(wins)
            gross_loss = abs(sum(losses))
            stats = {
                "total_trades": len(pnls),
                "win_rate": round(len(wins) / len(pnls) * 100.0, 2),
                "total_pnl": round(sum(pnls), 4),
                "profit_factor": round(
                    gross_profit / gross_loss if gross_loss > 0 else 0.0,
                    3,
                ),
                "avg_win": round(sum(wins) / len(wins), 4) if wins else 0.0,
                "avg_loss": round(sum(losses) / len(losses), 4) if losses else 0.0,
            }

        with STATE_LOCK:
            SHARED_STATE["stats"] = stats

        return stats

    async def generate_report(self, prices: Dict[str, float]) -> str:
        decisions = await self.get_recent_decisions(250)
        closed = await self.get_recent_closed_trades(50)
        open_trades = await self.get_open_trades()
        stats = await self.update_analytics()

        with STATE_LOCK:
            state = json.loads(json.dumps(SHARED_STATE))

        lines: List[str] = []
        lines.append("=" * 76)
        lines.append(f"MASTER QUANT ENGINE v{APP_VERSION} | TESTNET DIAGNOSTIC REPORT")
        lines.append(f"Generated: {utc_now()}")
        lines.append("=" * 76)
        lines.append("")
        lines.append("1. RUNTIME AND RISK STATUS")
        lines.append(f"Mode: {state['mode']}")
        lines.append(f"Auto trading: {state['auto_trading']}")
        lines.append(f"Bot active: {state['is_active']}")
        lines.append(f"Risk halted: {state['risk_halted']}")
        lines.append(f"Balance: ${state['balance']:.2f}")
        lines.append(f"Free balance: ${state['free_balance']:.2f}")
        lines.append(f"Peak balance: ${state['peak_balance']:.2f}")
        lines.append(f"Drawdown: {state['current_drawdown_pct']:.2f}%")
        lines.append(f"Daily PnL: ${state['daily_pnl']:.2f} ({state['daily_pnl_pct']:.2f}%)")
        lines.append(f"Open positions: {len(state['active_positions'])}/{MAX_OPEN_POSITIONS}")
        lines.append(f"Last scan: {state['last_scan']}")
        lines.append(f"Last sync: {state['last_sync']}")
        lines.append("")

        lines.append("2. PERFORMANCE")
        lines.append(f"Closed trades: {stats['total_trades']}")
        lines.append(f"Win rate: {stats['win_rate']:.2f}%")
        lines.append(f"Net realized PnL: ${stats['total_pnl']:.4f}")
        lines.append(f"Profit factor: {stats['profit_factor']:.3f}")
        lines.append(f"Average win: ${stats['avg_win']:.4f}")
        lines.append(f"Average loss: ${stats['avg_loss']:.4f}")
        lines.append("")

        lines.append("3. OPEN POSITIONS")
        if not open_trades:
            lines.append("No locally tracked open positions.")
        else:
            for trade in open_trades:
                symbol = trade["exchange_symbol"]
                price = prices.get(symbol, safe_float(trade["entry_price"]))
                side = trade["side"]
                qty = safe_float(trade["remaining_qty"])
                entry = safe_float(trade["entry_price"])
                unrealized = (price - entry) * qty * side_sign(side)
                lines.append(
                    f"{symbol} | {side.upper()} | entry={entry:.6f} "
                    f"| current={price:.6f} | qty={qty:.6f} "
                    f"| unrealized=${unrealized:.4f}"
                )
                lines.append(
                    f"  SL={safe_float(trade['stop_loss']):.6f} "
                    f"| TP1={safe_float(trade['take_profit_1']):.6f} "
                    f"| TP={safe_float(trade['take_profit_final']):.6f} "
                    f"| strategy={trade['strategy']}"
                )
        lines.append("")

        lines.append("4. RECENT CLOSED TRADES")
        if not closed:
            lines.append("No closed trades recorded yet.")
        else:
            for trade in closed[:20]:
                lines.append(
                    f"{trade['exchange_symbol']} | {trade['side'].upper()} "
                    f"| PnL=${safe_float(trade['realized_pnl']):+.4f} "
                    f"| fees=${safe_float(trade['estimated_fees']):.4f} "
                    f"| reason={trade.get('exit_reason') or '-'} "
                    f"| strategy={trade['strategy']}"
                )
        lines.append("")

        lines.append("5. DECISION QUALITY")
        rejection_reasons = Counter()
        per_symbol = defaultdict(lambda: {"signals": 0, "rejections": 0})

        for decision in decisions:
            symbol = decision["symbol"]
            if decision["action"] in ("buy", "sell"):
                per_symbol[symbol]["signals"] += 1
            else:
                per_symbol[symbol]["rejections"] += 1
                rejection_reasons[(decision.get("reason") or "Unknown")[:90]] += 1

        lines.append(f"Recorded decisions: {len(decisions)}")
        lines.append("Top rejection reasons:")
        if rejection_reasons:
            for reason, count in rejection_reasons.most_common(10):
                lines.append(f"- {count}x {reason}")
        else:
            lines.append("- No rejections recorded.")

        lines.append("Per-symbol decisions:")
        if per_symbol:
            for symbol, values in sorted(per_symbol.items()):
                lines.append(
                    f"- {symbol}: signals={values['signals']}, "
                    f"rejections={values['rejections']}"
                )
        lines.append("")

        lines.append("6. COOLDOWNS")
        active_cooldowns = []
        for symbol, until in SYMBOL_COOLDOWNS.items():
            remaining = int(max(0, until - now_ts()))
            if remaining > 0:
                active_cooldowns.append(f"{symbol}: {remaining}s")
        lines.extend(active_cooldowns or ["No active symbol cooldowns."])
        lines.append("")
        lines.append("=" * 76)

        return "\n".join(lines)


# =============================================================================
# 4. INDICATORS
# =============================================================================

class Indicators:
    @staticmethod
    def rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)
        average_gain = gains.ewm(com=period - 1, adjust=False).mean()
        average_loss = losses.ewm(com=period - 1, adjust=False).mean()
        rs = average_gain / average_loss.replace(0, 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        true_range = pd.concat(
            [
                df["high"] - df["low"],
                (df["high"] - df["close"].shift()).abs(),
                (df["low"] - df["close"].shift()).abs(),
            ],
            axis=1,
        ).max(axis=1)
        return true_range.ewm(com=period - 1, adjust=False).mean()

    @staticmethod
    def supertrend(
        df: pd.DataFrame,
        period: int = 10,
        multiplier: float = 3.0,
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        atr_value = Indicators.atr(df, period)
        midpoint = (df["high"] + df["low"]) / 2
        upper = midpoint + multiplier * atr_value
        lower = midpoint - multiplier * atr_value
        direction = pd.Series(1, index=df.index)

        for index in range(1, len(df)):
            if df["close"].iloc[index] > upper.iloc[index - 1]:
                direction.iloc[index] = 1
            elif df["close"].iloc[index] < lower.iloc[index - 1]:
                direction.iloc[index] = -1
            else:
                direction.iloc[index] = direction.iloc[index - 1]

            if direction.iloc[index] == 1:
                lower.iloc[index] = max(lower.iloc[index], lower.iloc[index - 1])
            else:
                upper.iloc[index] = min(upper.iloc[index], upper.iloc[index - 1])

        return direction, upper, lower


# =============================================================================
# 5. STRATEGY
# =============================================================================

class StrategyEngine:
    def analyze(self, df_5m: pd.DataFrame, df_1h: pd.DataFrame) -> Dict[str, Any]:
        df = df_5m.iloc[:-1].copy()
        htf = df_1h.iloc[:-1].copy()

        if len(df) < 80 or len(htf) < 220:
            return {
                "action": "neutral",
                "reason": "Insufficient completed candles",
                "strategy": "",
                "rsi": 0.0,
                "atr": 0.0,
                "htf_trend": "",
            }

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]

        htf_close = htf["close"]
        htf_ema_50 = htf_close.ewm(span=50, adjust=False).mean().iloc[-1]
        htf_ema_200 = htf_close.ewm(span=200, adjust=False).mean().iloc[-1]
        htf_price = float(htf_close.iloc[-1])

        if htf_price > htf_ema_50 and htf_ema_50 > htf_ema_200:
            htf_trend = "bullish"
        elif htf_price < htf_ema_50 and htf_ema_50 < htf_ema_200:
            htf_trend = "bearish"
        else:
            return {
                "action": "neutral",
                "reason": "HTF trend not aligned",
                "strategy": "",
                "rsi": 0.0,
                "atr": 0.0,
                "htf_trend": "sideways",
            }

        atr_series = Indicators.atr(df, 14)
        atr = safe_float(atr_series.iloc[-1])
        atr_average = safe_float(atr_series.rolling(20).mean().iloc[-1])

        if atr <= 0 or atr_average <= 0:
            return {
                "action": "neutral",
                "reason": "Invalid ATR",
                "strategy": "",
                "rsi": 0.0,
                "atr": atr,
                "htf_trend": htf_trend,
            }

        if atr < atr_average * 0.45 or atr > atr_average * 2.8:
            return {
                "action": "neutral",
                "reason": "Volatility outside allowed range",
                "strategy": "",
                "rsi": 0.0,
                "atr": atr,
                "htf_trend": htf_trend,
            }

        rsi_series = Indicators.rsi(close, 14)
        rsi = safe_float(rsi_series.iloc[-1])
        previous_rsi = safe_float(rsi_series.iloc[-2])

        price = safe_float(close.iloc[-1])
        previous_close = safe_float(close.iloc[-2])
        ema20 = safe_float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = safe_float(close.ewm(span=50, adjust=False).mean().iloc[-1])
        volume_sma = safe_float(volume.rolling(20).mean().iloc[-1], 1e-9)
        current_volume = safe_float(volume.iloc[-1])
        volume_ok = current_volume >= volume_sma * 1.12

        highest_12 = safe_float(high.rolling(12).max().iloc[-1])
        lowest_12 = safe_float(low.rolling(12).min().iloc[-1])
        st_direction, st_upper, st_lower = Indicators.supertrend(df)

        if (
            htf_trend == "bullish"
            and price > ema20
            and price >= highest_12 * 0.998
            and 48 < rsi < 74
            and volume_ok
        ):
            return self._build("buy", "Breakout_Momentum", price, atr, rsi, htf_trend)

        if (
            htf_trend == "bearish"
            and price < ema20
            and price <= lowest_12 * 1.002
            and 26 < rsi < 52
            and volume_ok
        ):
            return self._build("sell", "Breakout_Momentum", price, atr, rsi, htf_trend)

        if (
            htf_trend == "bullish"
            and price >= ema20
            and ema20 > ema50
            and previous_rsi <= 48
            and rsi > previous_rsi
            and rsi < 64
        ):
            return self._build("buy", "MTF_Pullback", price, atr, rsi, htf_trend)

        if (
            htf_trend == "bearish"
            and price <= ema20
            and ema20 < ema50
            and previous_rsi >= 52
            and rsi < previous_rsi
            and rsi > 36
        ):
            return self._build("sell", "MTF_Pullback", price, atr, rsi, htf_trend)

        if (
            htf_trend == "bullish"
            and st_direction.iloc[-1] == 1
            and low.iloc[-1] <= st_lower.iloc[-1] * 1.006
            and price > previous_close
            and 40 < rsi < 67
        ):
            return self._build("buy", "SuperTrend_Pullback", price, atr, rsi, htf_trend)

        if (
            htf_trend == "bearish"
            and st_direction.iloc[-1] == -1
            and high.iloc[-1] >= st_upper.iloc[-1] * 0.994
            and price < previous_close
            and 33 < rsi < 60
        ):
            return self._build("sell", "SuperTrend_Pullback", price, atr, rsi, htf_trend)

        return {
            "action": "neutral",
            "reason": f"No qualified signal (RSI={rsi:.1f})",
            "strategy": "",
            "rsi": rsi,
            "atr": atr,
            "htf_trend": htf_trend,
        }

    def _build(
        self,
        side: str,
        strategy: str,
        price: float,
        atr: float,
        rsi: float,
        htf_trend: str,
    ) -> Dict[str, Any]:
        params = STRATEGY_PARAMS[strategy]
        sl_distance = atr * params["sl_atr"]
        tp1_distance = atr * params["tp1_atr"]
        tp_distance = atr * params["tp_atr"]

        if side == "buy":
            sl = price - sl_distance
            tp1 = price + tp1_distance
            tp = price + tp_distance
        else:
            sl = price + sl_distance
            tp1 = price - tp1_distance
            tp = price - tp_distance

        return {
            "action": side,
            "strategy": strategy,
            "sl": sl,
            "tp1": tp1,
            "tp": tp,
            "rsi": rsi,
            "atr": atr,
            "htf_trend": htf_trend,
            "expected_rr": round(tp_distance / sl_distance, 2),
            "reason": f"Qualified {strategy} signal",
        }


# =============================================================================
# 6. RISK MANAGEMENT
# =============================================================================

class RiskManager:
    @staticmethod
    def estimate_trade_risk_usd(entry: float, stop: float, qty: float) -> float:
        return abs(entry - stop) * qty

    @staticmethod
    def calculate_qty(
        exchange: Any,
        symbol: str,
        balance: float,
        free_balance: float,
        entry: float,
        stop: float,
    ) -> Tuple[float, str]:
        if balance <= 0 or free_balance <= 0 or entry <= 0:
            return 0.0, "Invalid balance or price"

        stop_distance = abs(entry - stop)
        if stop_distance <= 0:
            return 0.0, "Invalid stop distance"

        risk_budget = balance * (RISK_PCT_PER_TRADE / 100.0)
        estimated_cost_per_unit = stop_distance + entry * (
            (TAKER_FEE_RATE * 2 * FEE_BUFFER) + (ESTIMATED_SLIPPAGE_PCT / 100.0)
        )

        raw_qty = risk_budget / estimated_cost_per_unit
        max_by_free = (free_balance * LEVERAGE * 0.15) / entry
        max_by_exposure = (balance * MAX_NOTIONAL_EXPOSURE_PCT / 100.0) / entry

        qty = min(raw_qty, max_by_free, max_by_exposure)

        try:
            qty = safe_float(exchange.amount_to_precision(symbol, qty))
        except Exception:
            return 0.0, "Could not format quantity"

        if qty <= 0:
            return 0.0, "Quantity rounds to zero"

        notional = qty * entry
        actual_risk = RiskManager.estimate_trade_risk_usd(entry, stop, qty)

        if notional < MIN_ORDER_USD:
            return 0.0, "Minimum order would exceed risk budget"

        if actual_risk > risk_budget * 1.05:
            return 0.0, "Actual risk exceeds configured budget"

        return qty, "OK"

    @staticmethod
    def can_open_position(
        symbol: str,
        side: str,
        entry: float,
        stop: float,
    ) -> Tuple[bool, str]:
        with STATE_LOCK:
            positions = list(SHARED_STATE["active_positions"].values())
            balance = safe_float(SHARED_STATE["balance"])
            halted = (
                SHARED_STATE["risk_halted"]
                or SHARED_STATE["daily_halted"]
                or SHARED_STATE["drawdown_halted"]
            )

        if halted:
            return False, "Global risk halt is active"

        if len(positions) >= MAX_OPEN_POSITIONS:
            return False, "Maximum open positions reached"

        if any(position["symbol"] == symbol for position in positions):
            return False, "Position already open for this symbol"

        same_direction = sum(1 for position in positions if position["side"] == side)
        if same_direction >= MAX_DIRECTIONAL_POSITIONS:
            return False, "Maximum same-direction positions reached"

        requested_risk = abs(entry - stop)
        current_risk = sum(
            abs(position["entry"] - position["sl"]) * position["qty"]
            for position in positions
        )

        total_risk_pct = (
            (current_risk + requested_risk) / balance * 100.0 if balance > 0 else 100.0
        )

        if total_risk_pct > MAX_PORTFOLIO_RISK_PCT:
            return False, "Maximum portfolio risk would be exceeded"

        cooldown_until = SYMBOL_COOLDOWNS.get(symbol, 0)
        if now_ts() < cooldown_until:
            return False, "Symbol is in cooldown"

        return True, "OK"


# =============================================================================
# 7. TELEGRAM
# =============================================================================

class TelegramController:
    def __init__(self, engine: "QuantEngine"):
        self.engine = engine
        self.offset = 0
        self.base_url = f"https://api.telegram.org/bot{TG_TOKEN}" if TG_TOKEN else ""

    def menu(self) -> Dict[str, Any]:
        active = SHARED_STATE["is_active"]
        return {
            "inline_keyboard": [
                [
                    {"text": "Dashboard", "callback_data": "dashboard"},
                    {"text": "Positions", "callback_data": "positions"},
                ],
                [
                    {
                        "text": "Pause" if active else "Resume",
                        "callback_data": "pause" if active else "resume",
                    },
                    {"text": "Sync", "callback_data": "sync"},
                ],
                [
                    {"text": "Report", "callback_data": "report"},
                    {"text": "Decisions", "callback_data": "decisions"},
                ],
            ]
        }

    async def send(self, text: str, markup: Optional[Dict[str, Any]] = None) -> None:
        if not self.base_url or not TG_CHAT:
            return

        payload: Dict[str, Any] = {
            "chat_id": TG_CHAT,
            "text": text[:3900],
            "parse_mode": "HTML",
        }

        if markup:
            payload["reply_markup"] = markup

        try:
            async with ClientSession() as session:
                await session.post(
                    f"{self.base_url}/sendMessage",
                    json=payload,
                    timeout=15,
                )
        except Exception as error:
            log.error("Telegram send failed: %s", error)

    async def send_document(self, path: str, caption: str = "") -> None:
        if not self.base_url or not TG_CHAT or not os.path.exists(path):
            return

        try:
            form_data = aiohttp.FormData()
            form_data.add_field("chat_id", TG_CHAT)
            form_data.add_field("caption", caption)
            form_data.add_field(
                "document",
                open(path, "rb"),
                filename=os.path.basename(path),
            )

            async with ClientSession() as session:
                await session.post(
                    f"{self.base_url}/sendDocument",
                    data=form_data,
                    timeout=60,
                )
        except Exception as error:
            log.error("Telegram document send failed: %s", error)

    async def poll(self) -> None:
        if not self.base_url:
            return

        await self.send(
            f"🧪 <b>Master Quant v{APP_VERSION} started</b>\n"
            f"Mode: <b>TESTNET</b>\n"
            f"Auto trading: <b>{AUTO_TRADING}</b>",
            self.menu(),
        )

        while True:
            try:
                async with ClientSession() as session:
                    response = await session.get(
                        f"{self.base_url}/getUpdates",
                        params={"offset": self.offset + 1, "timeout": 20},
                        timeout=30,
                    )
                    data = await response.json()

                for update in data.get("result", []):
                    self.offset = update["update_id"]

                    callback = update.get("callback_query")
                    if not callback:
                        continue

                    chat_id = callback.get("message", {}).get("chat", {}).get("id")
                    if not is_allowed_telegram_chat(chat_id):
                        log.warning("Unauthorized Telegram callback ignored.")
                        continue

                    command = callback.get("data", "")
                    await self.handle_command(command)

            except Exception as error:
                log.error("Telegram poll error: %s", error)

            await asyncio.sleep(1)

    async def handle_command(self, command: str) -> None:
        if command == "pause":
            with STATE_LOCK:
                SHARED_STATE["is_active"] = False
            await self.send("⏸️ Bot paused.", self.menu())

        elif command == "resume":
            with STATE_LOCK:
                SHARED_STATE["is_active"] = True
            await self.send("▶️ Bot resumed.", self.menu())

        elif command == "sync":
            await self.engine.smart_sync()
            await self.send("🔄 Exchange synchronization completed.", self.menu())

        elif command == "dashboard":
            with STATE_LOCK:
                state = json.loads(json.dumps(SHARED_STATE))
            await self.send(
                f"📊 <b>Dashboard v{APP_VERSION}</b>\n"
                f"Mode: {state['mode']}\n"
                f"Balance: ${state['balance']:.2f}\n"
                f"Drawdown: {state['current_drawdown_pct']:.2f}%\n"
                f"Daily PnL: ${state['daily_pnl']:.2f}\n"
                f"Positions: {len(state['active_positions'])}/{MAX_OPEN_POSITIONS}\n"
                f"Total PnL: ${state['stats']['total_pnl']:.2f}\n"
                f"Win rate: {state['stats']['win_rate']:.2f}%",
                self.menu(),
            )

        elif command == "positions":
            with STATE_LOCK:
                positions = list(SHARED_STATE["active_positions"].values())

            if not positions:
                await self.send("No active positions.", self.menu())
                return

            lines = ["💼 <b>Open positions</b>"]
            for position in positions:
                price = self.engine.prices.get(position["symbol"], position["entry"])
                pnl = (price - position["entry"]) * position["qty"] * side_sign(
                    position["side"]
                )
                lines.append(
                    f"{position['symbol']} | {position['side'].upper()} "
                    f"| PnL: ${pnl:+.3f}"
                )
            await self.send("\n".join(lines), self.menu())

        elif command == "report":
            report = await self.engine.db.generate_report(self.engine.prices)
            report_path = "quant_report_v15.txt"
            with open(report_path, "w", encoding="utf-8") as report_file:
                report_file.write(report)
            await self.send_document(report_path, "Diagnostic report v15.0")

        elif command == "decisions":
            decisions = await self.engine.db.get_recent_decisions(10)
            if not decisions:
                await self.send("No decisions recorded yet.", self.menu())
                return

            lines = ["🧠 <b>Recent decisions</b>"]
            for decision in decisions:
                icon = "✅" if decision["action"] in ("buy", "sell") else "⛔"
                lines.append(
                    f"{icon} {decision['symbol']} | "
                    f"{decision['action']} | {decision.get('reason', '-')[:85]}"
                )
            await self.send("\n".join(lines), self.menu())


# =============================================================================
# 8. ENGINE
# =============================================================================

class QuantEngine:
    def __init__(self) -> None:
        self.db = Database()
        self.strategy = StrategyEngine()
        self.telegram = TelegramController(self)
        self.prices: Dict[str, float] = {}
        self.last_price_update: Dict[str, float] = {}
        self.last_sync_at = 0.0

        self.exchange = ccxt.phemex(
            {
                "apiKey": PHEMEX_API_KEY,
                "secret": PHEMEX_API_SECRET,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
            }
        )
        self.exchange.set_sandbox_mode(True)

    async def start(self) -> None:
        await self.db.init()
        await self.restore_runtime_state()

        log.info("Master Quant v%s starts in Phemex Testnet mode.", APP_VERSION)

        await self.exchange.load_markets()

        for symbol in SYMBOLS:
            try:
                await self.exchange.set_leverage(LEVERAGE, symbol)
                log.info("Leverage configured for %s", symbol)
            except Exception as error:
                log.warning("Could not set leverage for %s: %s", symbol, error)

        await self.smart_sync()
        await self.update_balance()

        await asyncio.gather(
            self.price_loop(),
            self.scan_loop(),
            self.watchdog_loop(),
            self.telegram.poll(),
        )

    async def restore_runtime_state(self) -> None:
        runtime = await self.db.load_runtime("risk_state", {})

        with STATE_LOCK:
            SHARED_STATE["peak_balance"] = safe_float(runtime.get("peak_balance"))
            SHARED_STATE["day_start_balance"] = safe_float(runtime.get("day_start_balance"))
            SHARED_STATE["daily_halted"] = bool(runtime.get("daily_halted", False))
            SHARED_STATE["drawdown_halted"] = bool(runtime.get("drawdown_halted", False))
            SHARED_STATE["risk_halted"] = bool(runtime.get("risk_halted", False))

    async def persist_runtime_state(self) -> None:
        with STATE_LOCK:
            state = {
                "peak_balance": SHARED_STATE["peak_balance"],
                "day_start_balance": SHARED_STATE["day_start_balance"],
                "daily_halted": SHARED_STATE["daily_halted"],
                "drawdown_halted": SHARED_STATE["drawdown_halted"],
                "risk_halted": SHARED_STATE["risk_halted"],
            }
        await self.db.save_runtime("risk_state", state)

    async def update_balance(self) -> None:
        try:
            balance_data = await self.exchange.fetch_balance()
            usdt = balance_data.get("USDT", {})

            total = safe_float(usdt.get("total"))
            free = safe_float(usdt.get("free"))

            with STATE_LOCK:
                SHARED_STATE["balance"] = total
                SHARED_STATE["free_balance"] = free

                if SHARED_STATE["peak_balance"] <= 0:
                    SHARED_STATE["peak_balance"] = total
                if total > SHARED_STATE["peak_balance"]:
                    SHARED_STATE["peak_balance"] = total

                if SHARED_STATE["day_start_balance"] <= 0:
                    SHARED_STATE["day_start_balance"] = total

                peak = SHARED_STATE["peak_balance"]
                day_start = SHARED_STATE["day_start_balance"]

                drawdown = (
                    (peak - total) / peak * 100.0
                    if peak > 0
                    else 0.0
                )
                daily_pnl = total - day_start
                daily_pnl_pct = pct_change(total, day_start)

                SHARED_STATE["current_drawdown_pct"] = drawdown
                SHARED_STATE["daily_pnl"] = daily_pnl
                SHARED_STATE["daily_pnl_pct"] = daily_pnl_pct
                SHARED_STATE["drawdown_halted"] = drawdown >= MAX_DRAWDOWN_PCT
                SHARED_STATE["daily_halted"] = daily_pnl_pct <= -MAX_DAILY_LOSS_PCT
                SHARED_STATE["risk_halted"] = (
                    SHARED_STATE["drawdown_halted"] or SHARED_STATE["daily_halted"]
                )

            await self.db.log_equity(
                total,
                SHARED_STATE["peak_balance"],
                SHARED_STATE["current_drawdown_pct"],
                SHARED_STATE["daily_pnl"],
            )
            await self.persist_runtime_state()

        except Exception as error:
            log.error("Balance update failed: %s", error)
            with STATE_LOCK:
                SHARED_STATE["last_error"] = f"Balance update: {error}"

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
    ) -> Optional[pd.DataFrame]:
        try:
            candles = await self.exchange.fetch_ohlcv(
                symbol,
                timeframe=timeframe,
                limit=limit,
            )

            if not candles or len(candles) < 50:
                return None

            df = pd.DataFrame(
                candles,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

            latest_ts = df["timestamp"].iloc[-1].timestamp()
            candle_age = now_ts() - latest_ts

            if candle_age > MAX_CANDLE_AGE_SECONDS:
                log.warning(
                    "Stale candle data for %s %s: age %.0fs",
                    symbol,
                    timeframe,
                    candle_age,
                )
                return None

            return df

        except Exception as error:
            log.error("OHLCV fetch failed for %s %s: %s", symbol, timeframe, error)
            return None

    async def refresh_prices(self) -> None:
        try:
            tickers = await self.exchange.fetch_tickers(SYMBOLS)

            for symbol in SYMBOLS:
                ticker = tickers.get(symbol, {})
                last = safe_float(ticker.get("last"))
                bid = safe_float(ticker.get("bid"))
                ask = safe_float(ticker.get("ask"))

                if last <= 0:
                    continue

                if bid > 0 and ask > 0:
                    spread_pct = (ask - bid) / ((ask + bid) / 2) * 100.0
                    if spread_pct > MAX_SPREAD_PCT:
                        log.warning("%s spread too wide: %.3f%%", symbol, spread_pct)
                        continue

                self.prices[symbol] = last
                self.last_price_update[symbol] = now_ts()

        except Exception as error:
            log.error("Price refresh failed: %s", error)
            with STATE_LOCK:
                SHARED_STATE["last_error"] = f"Price refresh: {error}"

    async def price_loop(self) -> None:
        while True:
            await self.refresh_prices()
            await self.update_balance()
            await asyncio.sleep(PRICE_LOOP_SECONDS)

    async def scan_loop(self) -> None:
        while True:
            try:
                with STATE_LOCK:
                    can_scan = (
                        SHARED_STATE["is_active"]
                        and not SHARED_STATE["risk_halted"]
                    )
                    SHARED_STATE["last_scan"] = utc_now()

                if not can_scan:
                    await asyncio.sleep(15)
                    continue

                if now_ts() - self.last_sync_at > SYNC_INTERVAL_SECONDS:
                    await self.smart_sync()

                for symbol in SYMBOLS:
                    await self.scan_symbol(symbol)
                    await asyncio.sleep(1.2)

            except Exception as error:
                log.exception("Scan loop failed: %s", error)

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    async def scan_symbol(self, symbol: str) -> None:
        if now_ts() < SYMBOL_COOLDOWNS.get(symbol, 0):
            return

        with STATE_LOCK:
            if any(
                position["symbol"] == symbol
                for position in SHARED_STATE["active_positions"].values()
            ):
                return

        df_5m = await self.fetch_ohlcv(symbol, TIMEFRAME, CANDLE_LIMIT_5M)
        df_1h = await self.fetch_ohlcv(symbol, HTF_TIMEFRAME, CANDLE_LIMIT_1H)

        if df_5m is None or df_1h is None:
            await self.db.log_decision(
                symbol,
                "neutral",
                "",
                "Missing, stale, or incomplete Phemex candle data",
            )
            return

        signal = self.strategy.analyze(df_5m, df_1h)
        market_price = self.prices.get(symbol, 0.0)

        if market_price <= 0:
            await self.db.log_decision(
                symbol,
                "neutral",
                "",
                "No current Phemex ticker price",
            )
            return

        signal_price = safe_float(df_5m["close"].iloc[-2])
        deviation_pct = abs(market_price - signal_price) / signal_price * 100.0

        if deviation_pct > MAX_ENTRY_DEVIATION_PCT:
            await self.db.log_decision(
                symbol,
                "neutral",
                "",
                "Market price deviates from signal candle",
                price=market_price,
                extra={
                    "signal_price": signal_price,
                    "deviation_pct": round(deviation_pct, 4),
                },
            )
            return

        await self.db.log_decision(
            symbol,
            signal["action"],
            signal.get("strategy", ""),
            signal.get("reason", ""),
            price=market_price,
            atr=signal.get("atr", 0.0),
            rsi=signal.get("rsi", 0.0),
            htf_trend=signal.get("htf_trend", ""),
            extra={"expected_rr": signal.get("expected_rr")},
        )

        if signal["action"] not in ("buy", "sell"):
            return

        if not AUTO_TRADING:
            log.info(
                "Signal detected but auto-trading is disabled: %s %s",
                symbol,
                signal["action"],
            )
            return

        await self.execute_trade(symbol, signal, market_price)

    async def execute_trade(
        self,
        symbol: str,
        signal: Dict[str, Any],
        market_price: float,
    ) -> None:
        allowed, reason = RiskManager.can_open_position(
            symbol,
            signal["action"],
            market_price,
            signal["sl"],
        )

        if not allowed:
            await self.db.log_decision(
                symbol,
                "rejected",
                signal.get("strategy", ""),
                reason,
                price=market_price,
            )
            return

        with STATE_LOCK:
            balance = safe_float(SHARED_STATE["balance"])
            free_balance = safe_float(SHARED_STATE["free_balance"])

        qty, quantity_reason = RiskManager.calculate_qty(
            self.exchange,
            symbol,
            balance,
            free_balance,
            market_price,
            signal["sl"],
        )

        if qty <= 0:
            await self.db.log_decision(
                symbol,
                "rejected",
                signal.get("strategy", ""),
                quantity_reason,
                price=market_price,
            )
            return

        try:
            entry_order = await self.exchange.create_market_order(
                symbol,
                signal["action"],
                qty,
            )

            fill_price = safe_float(entry_order.get("average"), market_price)
            filled_qty = safe_float(entry_order.get("filled"), qty)

            if filled_qty <= 0:
                raise RuntimeError("Entry order returned zero filled quantity")

            trade_id = f"trade_{uuid.uuid4().hex[:16]}"
            trade = {
                "id": trade_id,
                "symbol": symbol,
                "side": signal["action"],
                "strategy": signal["strategy"],
                "entry": fill_price,
                "original_qty": filled_qty,
                "qty": filled_qty,
                "sl": signal["sl"],
                "tp1": signal["tp1"],
                "tp": signal["tp"],
                "entry_order_id": entry_order.get("id"),
                "stop_order_id": None,
                "tp_order_id": None,
                "highest_pnl_pct": 0.0,
                "realized_pnl": 0.0,
                "estimated_fees": self.estimate_order_fee(fill_price, filled_qty),
                "opened_at": utc_now(),
                "metadata": {
                    "signal_atr": signal.get("atr"),
                    "signal_rsi": signal.get("rsi"),
                    "expected_rr": signal.get("expected_rr"),
                    "entry_signal_price": market_price,
                },
            }

            await self.db.insert_trade(trade)
            await self.db.add_trade_event(
                trade_id,
                "entry",
                price=fill_price,
                qty=filled_qty,
                fees=trade["estimated_fees"],
                order_id=str(entry_order.get("id") or ""),
                reason=signal["strategy"],
                payload={"raw_order": entry_order},
            )

            # Protective-order call is intentionally isolated. Verify Phemex's
            # current parameters in Testnet before enabling production usage.
            await self.place_or_replace_protection(trade)

            with STATE_LOCK:
                SHARED_STATE["active_positions"][trade_id] = trade

            await self.telegram.send(
                f"🎯 <b>TESTNET ENTRY</b>\n"
                f"{symbol} | {signal['action'].upper()}\n"
                f"Entry: {fill_price:.6f}\n"
                f"Qty: {filled_qty:.6f}\n"
                f"SL: {trade['sl']:.6f}\n"
                f"TP1: {trade['tp1']:.6f}\n"
                f"TP: {trade['tp']:.6f}\n"
                f"Strategy: {trade['strategy']}"
            )

        except Exception as error:
            log.exception("Entry execution failed for %s: %s", symbol, error)
            await self.db.log_decision(
                symbol,
                "rejected",
                signal.get("strategy", ""),
                f"Entry execution failure: {str(error)[:150]}",
                price=market_price,
            )
            SYMBOL_COOLDOWNS[symbol] = now_ts() + 300

    async def place_or_replace_protection(self, trade: Dict[str, Any]) -> None:
        """
        Safety design:
        - The local watchdog remains a fallback.
        - This function is where exchange-native stop/TP orders belong.
        - Phemex parameter names can vary by market/API version; test the exact
          payload on Testnet and log each response before treating it as final.
        """
        try:
            opposite_side = "sell" if trade["side"] == "buy" else "buy"

            stop_order = await self.exchange.create_order(
                trade["symbol"],
                "market",
                opposite_side,
                trade["qty"],
                None,
                {
                    "reduceOnly": True,
                    "stopPx": trade["sl"],
                    "triggerType": "ByLastPrice",
                },
            )

            trade["stop_order_id"] = stop_order.get("id")
            await self.db.add_trade_event(
                trade["id"],
                "protective_stop_created",
                price=trade["sl"],
                qty=trade["qty"],
                order_id=str(stop_order.get("id") or ""),
                reason="Exchange protective stop",
                payload={"raw_order": stop_order},
            )
            await self.db.update_trade(trade)

        except Exception as error:
            # Critical: do not silently assume protection exists.
            log.error("Protective stop placement failed: %s", error)
            await self.db.add_trade_event(
                trade["id"],
                "protective_stop_failed",
                reason=str(error)[:250],
            )
            await self.telegram.send(
                f"⚠️ <b>PROTECTION WARNING</b>\n"
                f"{trade['symbol']}: exchange stop could not be confirmed.\n"
                f"Local watchdog remains active; inspect Testnet immediately."
            )

    async def cancel_order_safely(self, symbol: str, order_id: Optional[str]) -> None:
        if not order_id:
            return
        try:
            await self.exchange.cancel_order(order_id, symbol)
        except Exception as error:
            log.warning("Could not cancel order %s: %s", order_id, error)

    def estimate_order_fee(self, price: float, qty: float) -> float:
        return abs(price * qty) * TAKER_FEE_RATE * FEE_BUFFER

    def calculate_unrealized_pnl(
        self,
        trade: Dict[str, Any],
        price: float,
    ) -> Tuple[float, float]:
        raw_pnl = (
            (price - trade["entry"])
            * trade["qty"]
            * side_sign(trade["side"])
        )
        pnl_pct = (
            (price - trade["entry"]) / trade["entry"] * 100.0 * side_sign(trade["side"])
            if trade["entry"] > 0
            else 0.0
        )
        return raw_pnl, pnl_pct

    async def partial_close(self, trade_id: str, reason: str) -> None:
        with STATE_LOCK:
            trade = SHARED_STATE["active_positions"].get(trade_id)

        if not trade:
            return

        qty_to_close = safe_float(
            self.exchange.amount_to_precision(
                trade["symbol"],
                trade["qty"] * PARTIAL_CLOSE_FRACTION,
            )
        )

        if qty_to_close <= 0 or qty_to_close >= trade["qty"]:
            return

        try:
            close_side = "sell" if trade["side"] == "buy" else "buy"
            order = await self.exchange.create_market_order(
                trade["symbol"],
                close_side,
                qty_to_close,
                params={"reduceOnly": True},
            )

            fill_price = safe_float(order.get("average"), self.prices.get(trade["symbol"], 0))
            realized = (
                (fill_price - trade["entry"])
                * qty_to_close
                * side_sign(trade["side"])
            )
            fees = self.estimate_order_fee(fill_price, qty_to_close)

            trade["qty"] -= qty_to_close
            trade["realized_pnl"] += realized - fees
            trade["estimated_fees"] += fees
            trade["metadata"]["partial_taken"] = True

            # Lock the remaining quantity near break-even after TP1.
            trade["sl"] = trade["entry"]

            await self.cancel_order_safely(trade["symbol"], trade.get("stop_order_id"))
            trade["stop_order_id"] = None
            await self.place_or_replace_protection(trade)

            await self.db.add_trade_event(
                trade_id,
                "partial_exit",
                price=fill_price,
                qty=qty_to_close,
                pnl=realized,
                fees=fees,
                order_id=str(order.get("id") or ""),
                reason=reason,
                payload={"remaining_qty": trade["qty"]},
            )
            await self.db.update_trade(trade)

            with STATE_LOCK:
                SHARED_STATE["active_positions"][trade_id] = trade

            await self.telegram.send(
                f"🔹 <b>PARTIAL EXIT</b>\n"
                f"{trade['symbol']} | realized: ${realized - fees:+.3f}\n"
                f"Remaining quantity: {trade['qty']:.6f}\n"
                f"Stop moved to break-even."
            )

        except Exception as error:
            log.error("Partial close failed for %s: %s", trade_id, error)

    async def force_close(self, trade_id: str, reason: str) -> None:
        with STATE_LOCK:
            trade = SHARED_STATE["active_positions"].get(trade_id)

        if not trade:
            return

        try:
            close_side = "sell" if trade["side"] == "buy" else "buy"
            order = await self.exchange.create_market_order(
                trade["symbol"],
                close_side,
                trade["qty"],
                params={"reduceOnly": True},
            )

            exit_price = safe_float(
                order.get("average"),
                self.prices.get(trade["symbol"], trade["entry"]),
            )
            raw_pnl = (
                (exit_price - trade["entry"])
                * trade["qty"]
                * side_sign(trade["side"])
            )
            fees = self.estimate_order_fee(exit_price, trade["qty"])
            total_pnl = trade["realized_pnl"] + raw_pnl - fees
            total_fees = trade["estimated_fees"] + fees

            await self.cancel_order_safely(
                trade["symbol"],
                trade.get("stop_order_id"),
            )
            await self.cancel_order_safely(
                trade["symbol"],
                trade.get("tp_order_id"),
            )

            await self.db.add_trade_event(
                trade_id,
                "final_exit",
                price=exit_price,
                qty=trade["qty"],
                pnl=raw_pnl,
                fees=fees,
                order_id=str(order.get("id") or ""),
                reason=reason,
            )
            await self.db.close_trade(
                trade_id,
                total_pnl,
                total_fees,
                reason,
            )

            with STATE_LOCK:
                SHARED_STATE["active_positions"].pop(trade_id, None)

            symbol = trade["symbol"]
            if total_pnl < 0:
                SYMBOL_CONSECUTIVE_LOSSES[symbol] = (
                    SYMBOL_CONSECUTIVE_LOSSES.get(symbol, 0) + 1
                )
                if SYMBOL_CONSECUTIVE_LOSSES[symbol] >= CONSECUTIVE_LOSS_LIMIT:
                    SYMBOL_COOLDOWNS[symbol] = now_ts() + SYMBOL_COOLDOWN_SECONDS
            else:
                SYMBOL_CONSECUTIVE_LOSSES.pop(symbol, None)

            await self.db.update_analytics()
            await self.telegram.send(
                f"{'🟢' if total_pnl >= 0 else '🔴'} <b>POSITION CLOSED</b>\n"
                f"{symbol} | reason: {reason}\n"
                f"Net PnL: ${total_pnl:+.4f}\n"
                f"Estimated fees: ${total_fees:.4f}"
            )

        except Exception as error:
            log.exception("Force close failed for %s: %s", trade_id, error)

    async def watchdog_loop(self) -> None:
        while True:
            try:
                with STATE_LOCK:
                    positions = list(SHARED_STATE["active_positions"].items())

                for trade_id, trade in positions:
                    price = self.prices.get(trade["symbol"])
                    if not price:
                        continue

                    _, pnl_pct = self.calculate_unrealized_pnl(trade, price)
                    trade["highest_pnl_pct"] = max(
                        safe_float(trade.get("highest_pnl_pct")),
                        pnl_pct,
                    )

                    emergency_hit = pnl_pct <= -EMERGENCY_STOP_PCT
                    stop_hit = (
                        price <= trade["sl"]
                        if trade["side"] == "buy"
                        else price >= trade["sl"]
                    )
                    tp1_hit = (
                        price >= trade["tp1"]
                        if trade["side"] == "buy"
                        else price <= trade["tp1"]
                    )
                    final_tp_hit = (
                        price >= trade["tp"]
                        if trade["side"] == "buy"
                        else price <= trade["tp"]
                    )

                    partial_taken = bool(
                        trade.get("metadata", {}).get("partial_taken", False)
                    )

                    if (
                        PARTIAL_TP_ENABLED
                        and not partial_taken
                        and tp1_hit
                    ):
                        await self.partial_close(trade_id, "TP1 reached")
                        continue

                    if pnl_pct >= TRAIL_ACTIVATION_PCT:
                        if trade["side"] == "buy":
                            new_stop = price * (1 - TRAIL_DISTANCE_PCT / 100.0)
                            if new_stop > trade["sl"]:
                                trade["sl"] = new_stop
                        else:
                            new_stop = price * (1 + TRAIL_DISTANCE_PCT / 100.0)
                            if new_stop < trade["sl"]:
                                trade["sl"] = new_stop

                        await self.db.update_trade(trade)

                    if emergency_hit:
                        await self.force_close(trade_id, "Emergency loss limit")
                    elif stop_hit:
                        await self.force_close(trade_id, "Stop loss or trailing stop")
                    elif final_tp_hit:
                        await self.force_close(trade_id, "Final take profit")

            except Exception as error:
                log.exception("Watchdog error: %s", error)

            await asyncio.sleep(WATCHDOG_SECONDS)

    async def smart_sync(self) -> None:
        """
        The exchange is the primary source of truth for live positions.
        A production version should extend this with explicit reconciliation
        of open conditional orders and per-order exchange fills.
        """
        try:
            remote_positions = await self.exchange.fetch_positions(SYMBOLS)
            remote_by_symbol: Dict[str, Dict[str, Any]] = {}

            for remote in remote_positions:
                contracts = safe_float(remote.get("contracts"))
                if abs(contracts) <= 0:
                    continue

                symbol = remote.get("symbol")
                if symbol not in SYMBOLS:
                    continue

                remote_by_symbol[symbol] = {
                    "qty": abs(contracts),
                    "side": "buy" if contracts > 0 else "sell",
                    "entry": safe_float(
                        remote.get("entryPrice") or remote.get("avgEntryPrice")
                    ),
                }

            with STATE_LOCK:
                local_positions = dict(SHARED_STATE["active_positions"])

            # Close local records that no longer exist on the exchange.
            for trade_id, trade in local_positions.items():
                if trade["symbol"] not in remote_by_symbol:
                    await self.db.close_trade(
                        trade_id,
                        trade.get("realized_pnl", 0.0),
                        trade.get("estimated_fees", 0.0),
                        "Position absent during exchange sync",
                    )
                    with STATE_LOCK:
                        SHARED_STATE["active_positions"].pop(trade_id, None)

            # Recover exchange positions missing from local state.
            with STATE_LOCK:
                tracked_symbols = {
                    trade["symbol"]
                    for trade in SHARED_STATE["active_positions"].values()
                }

            for symbol, remote in remote_by_symbol.items():
                if symbol in tracked_symbols:
                    continue

                entry = remote["entry"] or self.prices.get(symbol, 0.0)
                if entry <= 0:
                    continue

                emergency_distance = entry * (EMERGENCY_STOP_PCT / 100.0)
                if remote["side"] == "buy":
                    sl = entry - emergency_distance
                    tp1 = entry + emergency_distance
                    tp = entry + emergency_distance * 2
                else:
                    sl = entry + emergency_distance
                    tp1 = entry - emergency_distance
                    tp = entry - emergency_distance * 2

                recovered = {
                    "id": f"recovered_{uuid.uuid4().hex[:16]}",
                    "symbol": symbol,
                    "side": remote["side"],
                    "strategy": "Recovered",
                    "entry": entry,
                    "original_qty": remote["qty"],
                    "qty": remote["qty"],
                    "sl": sl,
                    "tp1": tp1,
                    "tp": tp,
                    "entry_order_id": None,
                    "stop_order_id": None,
                    "tp_order_id": None,
                    "highest_pnl_pct": 0.0,
                    "realized_pnl": 0.0,
                    "estimated_fees": 0.0,
                    "opened_at": utc_now(),
                    "metadata": {
                        "recovered": True,
                        "warning": "Original entry timestamp unavailable",
                    },
                }

                await self.db.insert_trade(recovered)
                await self.db.add_trade_event(
                    recovered["id"],
                    "recovered",
                    price=entry,
                    qty=remote["qty"],
                    reason="Recovered from exchange state",
                )

                with STATE_LOCK:
                    SHARED_STATE["active_positions"][recovered["id"]] = recovered

                await self.telegram.send(
                    f"🔄 <b>RECOVERED POSITION</b>\n"
                    f"{symbol} | {remote['side'].upper()}\n"
                    f"Entry: {entry:.6f}\n"
                    f"Qty: {remote['qty']:.6f}\n"
                    f"Review protective orders in Testnet."
                )

            self.last_sync_at = now_ts()
            with STATE_LOCK:
                SHARED_STATE["last_sync"] = utc_now()

            log.info("Exchange synchronization completed.")

        except Exception as error:
            log.exception("Exchange sync failed: %s", error)
            with STATE_LOCK:
                SHARED_STATE["last_error"] = f"Exchange sync: {error}"


# =============================================================================
# 9. DASHBOARD
# =============================================================================

app = Flask(__name__)
auth = HTTPBasicAuth()

@auth.verify_password
def verify_password(username: str, password: str) -> Optional[str]:
    if (
        username == DASHBOARD_USER
        and DASHBOARD_PASSWORD_HASH
        and check_password_hash(DASHBOARD_PASSWORD_HASH, password)
    ):
        return username
    return None


@app.route("/api/status")
@auth.login_required
def api_status():
    with STATE_LOCK:
        return jsonify(SHARED_STATE)


@app.route("/")
@auth.login_required
def dashboard():
    return render_template_string(
        """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Master Quant v15.0</title>
  <style>
    body { background:#0d1117; color:#c9d1d9; font-family:system-ui,sans-serif; margin:0; padding:24px; }
    h1 { color:#58a6ff; margin:0 0 8px; }
    .hint { color:#8b949e; margin-bottom:20px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; }
    .card { background:#161b22; border:1px solid #30363d; border-radius:12px; padding:16px; }
    .label { color:#8b949e; font-size:0.85rem; }
    .value { font-size:1.45rem; color:#58a6ff; font-weight:700; margin-top:7px; }
    .warn { color:#f0883e; }
    .bad { color:#ff7b72; }
    .ok { color:#3fb950; }
  </style>
</head>
<body>
  <h1>Master Quant v15.0</h1>
  <div class="hint">Phemex Testnet monitoring dashboard</div>
  <div class="grid">
    <div class="card"><div class="label">Mode</div><div class="value" id="mode">-</div></div>
    <div class="card"><div class="label">Balance</div><div class="value" id="balance">-</div></div>
    <div class="card"><div class="label">Daily PnL</div><div class="value" id="daily">-</div></div>
    <div class="card"><div class="label">Drawdown</div><div class="value" id="dd">-</div></div>
    <div class="card"><div class="label">Open positions</div><div class="value" id="positions">-</div></div>
    <div class="card"><div class="label">Total realized PnL</div><div class="value" id="pnl">-</div></div>
    <div class="card"><div class="label">Win rate</div><div class="value" id="wr">-</div></div>
    <div class="card"><div class="label">Risk status</div><div class="value" id="risk">-</div></div>
  </div>
  <script>
    async function refresh() {
      try {
        const response = await fetch('/api/status');
        const data = await response.json();
        document.getElementById('mode').textContent = data.mode || '-';
        document.getElementById('balance').textContent = '$' + (data.balance || 0).toFixed(2);
        document.getElementById('daily').textContent = '$' + (data.daily_pnl || 0).toFixed(2);
        document.getElementById('dd').textContent = (data.current_drawdown_pct || 0).toFixed(2) + '%';
        document.getElementById('positions').textContent =
          Object.keys(data.active_positions || {}).length;
        document.getElementById('pnl').textContent =
          '$' + ((data.stats || {}).total_pnl || 0).toFixed(2);
        document.getElementById('wr').textContent =
          (((data.stats || {}).win_rate || 0).toFixed(2) + '%');
        document.getElementById('risk').textContent =
          data.risk_halted ? 'HALTED' : 'ACTIVE';
      } catch (_) {}
    }
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
        """
    )


def run_web() -> None:
    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False,
        use_reloader=False,
    )


# =============================================================================
# 10. MAIN
# =============================================================================

async def main() -> None:
    engine = QuantEngine()
    try:
        await engine.start()
    finally:
        await engine.exchange.close()


if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    asyncio.run(main())
