#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v16.2 – Phemex-Only (RSI Divergence Filter)
- فیلتر واگرایی RSI اضافه شد
- ۳ استراتژی فعال
- تعادل کیفیت و تعداد معاملات
- ۶ نماد سالم
"""

import asyncio
import logging
import os
import sys
import time
import traceback
import uuid
from collections import Counter, defaultdict
from datetime import datetime
from threading import Thread, Lock
from typing import Dict, List, Any, Optional

import aiohttp
import aiosqlite
import ccxt.async_support as ccxt
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string

load_dotenv()

# ===================== CONFIG =====================
API_KEY    = os.getenv("PHEMEX_API_KEY", "")
API_SECRET = os.getenv("PHEMEX_API_SECRET", "")
TESTNET    = os.getenv("PHEMEX_TESTNET", "False").lower() in ("true", "1", "yes")
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT    = os.getenv("TELEGRAM_CHAT_ID", "")

SYMBOLS = [
    "ETH/USDT:USDT",
    "BNB/USDT:USDT",
    "XRP/USDT:USDT",
    "DOT/USDT:USDT",
    "AVAX/USDT:USDT",
    "DOGE/USDT:USDT",
]

STRATEGY_PARAMS = {
    "Breakout_Momentum":   {"sl_m": 1.50, "tp_m": 3.6, "tp1_m": 1.9},
    "SuperTrend_Pullback": {"sl_m": 1.45, "tp_m": 3.1, "tp1_m": 1.65},
    "Volume_Surge":        {"sl_m": 1.40, "tp_m": 2.9, "tp1_m": 1.50},
}

TIMEFRAME = "5m"
HTF_TIMEFRAME = "1h"
RISK_PCT = 0.42
LEVERAGE = 5
MAX_POS = 5
MAX_DD = 7.5
MAX_DAILY_LOSS = 3.8
MIN_ORDER_USD = 17.0
MAX_EXPOSURE_PCT = 28.0
TAKER_FEE = 0.0006
FEE_BUFFER = 1.30
TRAIL_ACT = 3.2
TRAIL_STEP = 1.0
PARTIAL_TP = True
MIN_HOLD_FOR_PARTIAL = 720
MIN_HOLD_FOR_TRAIL = 1080
MIN_PROFIT_FOR_BE = 0.75
TEST_SYMBOL = "DOGE/USDT:USDT"
TEST_USD = 12.0
CONSECUTIVE_LOSS_LIMIT = 2
SYMBOL_COOLDOWN_HOURS = 5
POST_CLOSE_COOLDOWN = 1800
SCAN_INTERVAL = 50
SYMBOL_DELAY = 1.3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("QuantV16.2")

SHARED_STATE: Dict[str, Any] = {
    "is_active": True,
    "dd_halted": False,
    "daily_halted": False,
    "balance": 0.0,
    "peak_balance": 0.0,
    "day_start_balance": 0.0,
    "current_dd": 0.0,
    "daily_pnl": 0.0,
    "active_positions": {},
    "last_scan": "Never",
    "stats": {"total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0},
    "consecutive_losses": {},
}
STATE_LOCK = Lock()
SYMBOL_ERROR_COOLDOWN: Dict[str, float] = {}
SYMBOL_ERROR_COUNT: Dict[str, int] = {}
SYMBOL_POST_CLOSE_COOLDOWN: Dict[str, float] = {}

# ===================== DATABASE =====================
class Database:
    def __init__(self, path="bot.db"):
        self.path = path

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY, symbol TEXT, side TEXT, strategy TEXT,
                    entry_price REAL, qty REAL, original_qty REAL,
                    sl REAL, tp1 REAL, tp REAL, is_partial INTEGER DEFAULT 0,
                    highest_pnl_pct REAL DEFAULT 0, status TEXT DEFAULT 'open',
                    pnl REAL DEFAULT 0, fees_est REAL DEFAULT 0,
                    exit_reason TEXT, hold_seconds REAL DEFAULT 0,
                    opened_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT, action TEXT, strategy TEXT, reason TEXT,
                    price REAL, rsi REAL, atr REAL, htf_trend TEXT, extra TEXT
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS equity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    balance REAL, peak REAL, dd REAL
                )""")
            await db.commit()

    async def insert_trade(self, t: dict):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO trades
                (id,symbol,side,strategy,entry_price,qty,original_qty,sl,tp1,tp)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (t["id"], t["symbol"], t["side"], t["strategy"], t["entry"],
                 t["qty"], t["qty"], t["sl"], t["tp1"], t["tp"]))
            await db.commit()

    async def update_trade(self, tid, qty, sl, partial, hp):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE trades SET qty=?,sl=?,is_partial=?,highest_pnl_pct=? WHERE id=?",
                (qty, sl, partial, hp, tid))
            await db.commit()

    async def close_trade(self, tid, pnl, fees=0.0, reason="", hold=0.0):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                UPDATE trades SET status='closed', pnl=?, fees_est=?, exit_reason=?,
                hold_seconds=?, closed_at=CURRENT_TIMESTAMP WHERE id=?""",
                (pnl, fees, reason, hold, tid))
            await db.commit()

    async def log_decision(self, symbol, action, strategy, reason, price=0, rsi=0, atr=0, htf="", extra=""):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO decisions (symbol,action,strategy,reason,price,rsi,atr,htf_trend,extra)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (symbol, action, strategy, reason, price, rsi, atr, htf, str(extra)[:500]))
            await db.commit()

    async def log_equity(self, balance, peak, dd):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO equity (balance,peak,dd) VALUES (?,?,?)",
                (balance, peak, dd))
            await db.commit()

    async def get_open_trades(self) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM trades WHERE status='open'") as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_closed_trades(self, limit=50) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?",
                (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_recent_decisions(self, limit=250) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM decisions ORDER BY id DESC LIMIT ?", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def update_analytics(self):
        async with aiosqlite.connect(self.path) as db:
            async with db.execute("SELECT pnl FROM trades WHERE status='closed'") as c:
                rows = await c.fetchall()
                if not rows:
                    return
                pnls = [r[0] for r in rows]
                wins = sum(1 for p in pnls if p > 0)
                with STATE_LOCK:
                    SHARED_STATE["stats"] = {
                        "total_trades": len(pnls),
                        "win_rate": round(wins / len(pnls) * 100, 1),
                        "total_pnl": round(sum(pnls), 2),
                    }

    async def generate_txt_report(self, prices: Dict[str, float] = None) -> str:
        prices = prices or {}
        decisions = await self.get_recent_decisions(200)
        closed = await self.get_closed_trades(30)
        with STATE_LOCK:
            st = dict(SHARED_STATE)

        lines = [
            "=" * 70,
            "       MASTER QUANT ENGINE v16.2 – Phemex-Only (RSI Divergence) REPORT",
            f"       Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
            "=" * 70,
            "",
            "┌─ 1. DASHBOARD ─────────────────────────────────────────────────────",
            f"│  Balance        : ${st.get('balance', 0):.2f}",
            f"│  Peak Balance   : ${st.get('peak_balance', 0):.2f}",
            f"│  Current DD     : {st.get('current_dd', 0):.2f}%",
            f"│  Daily PnL      : ${st.get('daily_pnl', 0):.2f}",
            f"│  Open Positions : {len(st.get('active_positions', {}))} / {MAX_POS}",
            f"│  Total Trades   : {st.get('stats', {}).get('total_trades', 0)}",
            f"│  Win Rate       : {st.get('stats', {}).get('win_rate', 0)}%",
            f"│  Total PnL      : ${st.get('stats', {}).get('total_pnl', 0):.2f}",
            f"│  Last Scan      : {st.get('last_scan', 'Never')}",
            f"│  Bot Active     : {st.get('is_active')}",
            "└────────────────────────────────────────────────────────────────────",
            "",
            "┌─ 2. OPEN POSITIONS ────────────────────────────────────────────────",
        ]
        active = st.get("active_positions", {})
        if not active:
            lines.append("│  (none)")
        else:
            for p in active.values():
                pr = prices.get(p["symbol"], p["entry"])
                pnl = (pr - p["entry"]) * p["qty"] * (1 if p["side"] == "buy" else -1)
                lines.append(
                    f"│  {p['symbol']:<18} {p['side'].upper():<5} "
                    f"Entry:{p['entry']:.5f} Qty:{p['qty']:.4f} PnL:${pnl:+.3f} {p.get('strategy','')}"
                )
                lines.append(f"│     SL:{p['sl']:.5f} TP:{p['tp']:.5f} Partial:{p.get('is_partial',0)}")
        lines.append("└────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("┌─ 3. CLOSED TRADES ─────────────────────────────────────────────────")
        if not closed:
            lines.append("│  (none)")
        else:
            for t in closed:
                emoji = "WIN " if t["pnl"] > 0 else "LOSS"
                hold_m = (t.get("hold_seconds") or 0) / 60
                lines.append(
                    f"│  [{emoji}] {t['symbol']:<16} {t['side']:<4} "
                    f"PnL:${t['pnl']:+.3f} Hold:{hold_m:.1f}m {t.get('exit_reason','')}"
                )
        lines.append("└────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("┌─ 4. DECISION BREAKDOWN ────────────────────────────────────────────")
        reasons = Counter()
        if decisions:
            by_symbol = defaultdict(lambda: {"sig": 0, "rej": 0})
            signals = 0
            for d in decisions:
                if d["action"] == "neutral":
                    reasons[(d["reason"] or "Unknown")[:60]] += 1
                    by_symbol[d["symbol"]]["rej"] += 1
                else:
                    signals += 1
                    by_symbol[d["symbol"]]["sig"] += 1
            lines.append(f"│  Total:{len(decisions)}  Signals:{signals}  Rejected:{len(decisions)-signals}")
            lines.append("│  Top Rejections:")
            for reason, count in reasons.most_common(10):
                lines.append(f"│    {count:3d} × {reason}")
            lines.append("│  Per Symbol:")
            for sym, v in sorted(by_symbol.items()):
                lines.append(f"│    {sym:<18} Sig:{v['sig']:3d} Rej:{v['rej']:3d}")
        else:
            lines.append("│  (no decisions yet)")
        lines.append("└────────────────────────────────────────────────────────────────────")
        lines.append("")