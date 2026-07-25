#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v12.2 (Phemex Fully Fixed)
- Custom OHLCV fetcher → solves code 30000 permanently
- Position mode + leverage handling improved
- Full AI Observer + decision logging
"""

import asyncio
import logging
import os
import time
import uuid
from collections import Counter, defaultdict
from threading import Thread
from typing import Dict, List, Optional, Tuple

import aiohttp
import aiosqlite
import ccxt.async_support as ccxt
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string
from flask_httpauth import HTTPBasicAuth

# ============================================================================
# 1. CONFIG
# ============================================================================
load_dotenv()

API_KEY = os.getenv("PHEMEX_API_KEY", "")
API_SECRET = os.getenv("PHEMEX_API_SECRET", "")
TESTNET = os.getenv("PHEMEX_TESTNET", "False").lower() in ("true", "1", "yes")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

WEB_USER = os.getenv("WEB_ADMIN_USER", "") or "admin"
WEB_PASS = os.getenv("WEB_ADMIN_PASS", "") or "admin123"

SYMBOLS = [
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "BNB/USDT:USDT",
    "XRP/USDT:USDT",
    "ADA/USDT:USDT",
    "DOT/USDT:USDT",
]

TIMEFRAME = "5m"
HTF_TIMEFRAME = "1h"
RISK_PCT = 0.5
LEVERAGE = 5
MAX_POS = 3
MAX_DD = 8.0
MAX_DAILY_LOSS_PCT = 4.0
MIN_ORDER_USD = 16.0
MAX_EXPOSURE_PCT = 35.0
TAKER_FEE = 0.0006
FEE_BUFFER = 1.15

TRAIL_ACT = 1.8
TRAIL_STEP = 0.6
PARTIAL_TP = True
RELAXED_MODE = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.FileHandler("quant_bot_v12.log"), logging.StreamHandler()],
)
log = logging.getLogger("QuantV12.2")

SHARED_STATE = {
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
    "health": {"last_price_ok": True, "errors_1h": 0},
}

STATE_LOCK = asyncio.Lock()

# ============================================================================
# 2. DATABASE
# ============================================================================
class AsyncDB:
    def __init__(self, db_path="bot_v12.db"):
        self.db_path = db_path

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY, symbol TEXT, side TEXT, strategy TEXT,
                    entry_price REAL, qty REAL, original_qty REAL, sl REAL, tp1 REAL, tp REAL,
                    is_partial INTEGER DEFAULT 0, highest_pnl_pct REAL DEFAULT 0,
                    status TEXT DEFAULT 'open', pnl REAL DEFAULT 0, fees_est REAL DEFAULT 0,
                    exit_reason TEXT, hold_seconds REAL DEFAULT 0,
                    opened_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT, action TEXT, strategy TEXT, reason TEXT,
                    price REAL, rsi REAL, atr REAL, htf_trend TEXT, extra TEXT
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS observer_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT DEFAULT CURRENT_TIMESTAMP, report TEXT
                )""")
            await db.commit()

    async def insert_trade(self, t: Dict):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO trades (id, symbol, side, strategy, entry_price, qty, original_qty, sl, tp1, tp)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (t["id"], t["symbol"], t["side"], t["strategy"], t["entry"], t["qty"], t["qty"], t["sl"], t["tp1"], t["tp"]),
            )
            await db.commit()

    async def update_trade(self, t_id, qty, sl, is_partial, highest_pnl):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE trades SET qty=?, sl=?, is_partial=?, highest_pnl_pct=? WHERE id=?",
                (qty, sl, is_partial, highest_pnl, t_id),
            )
            await db.commit()

    async def close_trade(self, t_id, pnl, fees_est=0.0, reason="", hold_seconds=0.0):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """UPDATE trades SET status='closed', pnl=?, fees_est=?, exit_reason=?, hold_seconds=?,
                   closed_at=CURRENT_TIMESTAMP WHERE id=?""",
                (pnl, fees_est, reason, hold_seconds, t_id),
            )
            await db.commit()

    async def log_decision(self, symbol, action, strategy, reason, price=0.0, rsi=0.0, atr=0.0, htf_trend="", extra=""):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO decisions (symbol, action, strategy, reason, price, rsi, atr, htf_trend, extra)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (symbol, action, strategy, reason, price, rsi, atr, htf_trend, extra),
            )
            await db.commit()

    async def get_open_trades(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM trades WHERE status='open'") as c:
                return [dict(r) for r in await c.fetchall()]

    async def update_analytics(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT pnl FROM trades WHERE status='closed'") as c:
                rows = await c.fetchall()
                if not rows:
                    return
                pnls = [r[0] for r in rows]
                wins = len([p for p in pnls if p > 0])
                total = len(pnls)
                async with STATE_LOCK:
                    SHARED_STATE["stats"] = {
                        "total_trades": total,
                        "win_rate": round((wins / total) * 100, 1) if total else 0.0,
                        "total_pnl": round(sum(pnls), 2),
                    }

    async def get_recent_decisions(self, limit=300):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM decisions ORDER BY id DESC LIMIT ?", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_closed_trades(self, limit=200):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?", (limit,)
            ) as c:
                return [dict(r) for r in await c.fetchall()]

    async def save_observer_report(self, report):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO observer_logs (report) VALUES (?)", (report,))
            await db.commit()

# ============================================================================
# 3. INDICATORS + STRATEGY
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close, n=14):
        delta = close.diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        ma_up = up.ewm(com=n-1, adjust=False).mean()
        ma_down = down.ewm(com=n-1, adjust=False).mean()
        rs = ma_up / ma_down.replace(0, 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def atr(df, n=14):
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs(),
        ], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

    @staticmethod
    def supertrend(df, period=10, multiplier=3.0):
        atr = Indicators.atr(df, period)
        hl2 = (df["high"] + df["low"]) / 2
        upper = hl2 + multiplier * atr
        lower = hl2 - multiplier * atr
        direction = pd.Series(1, index=df.index)
        for i in range(1, len(df)):
            if df["close"].iloc[i] > upper.iloc[i-1]:
                direction.iloc[i] = 1
            elif df["close"].iloc[i] < lower.iloc[i-1]:
                direction.iloc[i] = -1
            else:
                direction.iloc[i] = direction.iloc[i-1]
                if direction.iloc[i] == 1 and lower.iloc[i] < lower.iloc[i-1]:
                    lower.iloc[i] = lower.iloc[i-1]
                if direction.iloc[i] == -1 and upper.iloc[i] > upper.iloc[i-1]:
                    upper.iloc[i] = upper.iloc[i-1]
        return direction, upper, lower

    @staticmethod
    def sma(s, p): return s.rolling(p).mean()
    @staticmethod
    def highest(s, p): return s.rolling(p).max()
    @staticmethod
    def lowest(s, p): return s.rolling(p).min()


class StrategyEngine:
    def analyze(self, df_5m, df_1h):
        df_c = df_5m.iloc[:-1].copy()
        df_htf = df_1h.iloc[:-1].copy()
        if len(df_c) < 60 or len(df_htf) < 50:
            return {"action": "neutral", "reason": "داده ناکافی", "strat": "", "rsi": 0, "atr": 0, "htf": ""}

        htf_close = df_htf["close"]
        ema50 = htf_close.ewm(span=50, adjust=False).mean().iloc[-1]
        ema200 = htf_close.ewm(span=min(200, len(df_htf)), adjust=False).mean().iloc[-1]
        htf_price = htf_close.iloc[-1]

        if htf_price > ema50 and ema50 > ema200 * 0.998:
            htf_trend = "bullish"
        elif htf_price < ema50 and ema50 < ema200 * 1.002:
            htf_trend = "bearish"
        else:
            return {"action": "neutral", "reason": "روند HTF نامشخص", "strat": "", "rsi": 0, "atr": 0, "htf": "sideways"}

        c, high, low, vol = df_c["close"], df_c["high"], df_c["low"], df_c["volume"]
        price = float(c.iloc[-1])
        atr_s = Indicators.atr(df_c, 14)
        atr = float(atr_s.iloc[-1])
        if atr <= 0:
            return {"action": "neutral", "reason": "ATR نامعتبر", "strat": "", "rsi": 0, "atr": 0, "htf": htf_trend}

        atr_sma = float(Indicators.sma(atr_s, 20).iloc[-1])
        low_m, high_m = (0.45, 3.2) if RELAXED_MODE else (0.55, 2.8)
        if atr < atr_sma * low_m:
            return {"action": "neutral", "reason": f"نوسان کم ATR={atr:.5f}", "strat": "", "rsi": 0, "atr": atr, "htf": htf_trend}
        if atr > atr_sma * high_m:
            return {"action": "neutral", "reason": f"نوسان زیاد ATR={atr:.5f}", "strat": "", "rsi": 0, "atr": atr, "htf": htf_trend}

        rsi_s = Indicators.rsi(c, 14)
        rsi_curr, rsi_prev = float(rsi_s.iloc[-1]), float(rsi_s.iloc[-2])
        ema20 = float(c.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50_ltf = float(c.ewm(span=50, adjust=False).mean().iloc[-1])
        st_dir, st_up, st_lo = Indicators.supertrend(df_c)
        vol_sma = float(Indicators.sma(vol, 20).iloc[-1])
        vol_curr = float(vol.iloc[-1])
        h10 = float(Indicators.highest(high, 10).iloc[-1])
        l10 = float(Indicators.lowest(low, 10).iloc[-1])
        ph10 = float(Indicators.highest(high, 10).iloc[-2])
        pl10 = float(Indicators.lowest(low, 10).iloc[-2])

        # Breakout
        if htf_trend == "bullish" and price > ema20 and price >= h10 * 0.999:
            if ph10 <= h10 and 48 < rsi_curr < 75 and vol_curr > vol_sma * (1.15 if RELAXED_MODE else 1.3):
                return self._sig("buy", "Breakout_Momentum", price, atr, rsi_curr, htf_trend)
        if htf_trend == "bearish" and price < ema20 and price <= l10 * 1.001:
            if pl10 >= l10 and 25 < rsi_curr < 52 and vol_curr > vol_sma * (1.15 if RELAXED_MODE else 1.3):
                return self._sig("sell", "Breakout_Momentum", price, atr, rsi_curr, htf_trend)

        # MTF Pullback
        if htf_trend == "bullish" and price > ema20 and ema20 > ema50_ltf * 0.999:
            if rsi_prev <= (42 if RELAXED_MODE else 40) and rsi_curr > rsi_prev and rsi_curr < 62:
                return self._sig("buy", "MTF_Pullback", price, atr, rsi_curr, htf_trend)
        if htf_trend == "bearish" and price < ema20 and ema20 < ema50_ltf * 1.001:
            if rsi_prev >= (58 if RELAXED_MODE else 60) and rsi_curr < rsi_prev and rsi_curr > 38:
                return self._sig("sell", "MTF_Pullback", price, atr, rsi_curr, htf_trend)

        # SuperTrend
        if htf_trend == "bullish" and st_dir.iloc[-1] == 1:
            if low.iloc[-1] <= st_lo.iloc[-1] * 1.005 and price > low.iloc[-1] and c.iloc[-1] > c.iloc[-2] and 38 < rsi_curr < 65:
                return self._sig("buy", "SuperTrend_Pullback", price, atr, rsi_curr, htf_trend)
        if htf_trend == "bearish" and st_dir.iloc[-1] == -1:
            if high.iloc[-1] >= st_up.iloc[-1] * 0.995 and price < high.iloc[-1] and c.iloc[-1] < c.iloc[-2] and 35 < rsi_curr < 62:
                return self._sig("sell", "SuperTrend_Pullback", price, atr, rsi_curr, htf_trend)

        # Volume
        if htf_trend == "bullish" and price > ema20 and vol_curr > vol_sma * (1.5 if RELAXED_MODE else 1.8):
            if c.iloc[-1] > c.iloc[-2] and 48 < rsi_curr < 70:
                return self._sig("buy", "Volume_Surge", price, atr, rsi_curr, htf_trend)
        if htf_trend == "bearish" and price < ema20 and vol_curr > vol_sma * (1.5 if RELAXED_MODE else 1.8):
            if c.iloc[-1] < c.iloc[-2] and 30 < rsi_curr < 52:
                return self._sig("sell", "Volume_Surge", price, atr, rsi_curr, htf_trend)

        return {
            "action": "neutral",
            "reason": f"هیچ استراتژی (RSI={rsi_curr:.1f}, HTF={htf_trend})",
            "strat": "", "rsi": rsi_curr, "atr": atr, "htf": htf_trend,
        }

    def _sig(self, side, strat, price, atr, rsi, htf):
        sl_m, tp_m, tp1_m = 1.5, 2.8, 1.4
        if strat == "Breakout_Momentum":
            sl_m, tp_m, tp1_m = 1.25, 3.2, 1.8
        elif strat == "Volume_Surge":
            sl_m, tp_m, tp1_m = 1.35, 2.4, 1.4
        if side == "buy":
            return {"action": side, "strat": strat, "sl": price - atr*sl_m, "tp": price + atr*tp_m, "tp1": price + atr*tp1_m,
                    "reason": f"سیگنال {strat}", "rsi": rsi, "atr": atr, "htf": htf}
        return {"action": side, "strat": strat, "sl": price + atr*sl_m, "tp": price - atr*tp_m, "tp1": price - atr*tp1_m,
                "reason": f"سیگنال {strat}", "rsi": rsi, "atr": atr, "htf": htf}

# ============================================================================
# 4. AI OBSERVER
# ============================================================================
class AIObserver:
    def __init__(self, db):
        self.db = db

    async def generate_report(self):
        decisions = await self.db.get_recent_decisions(400)
        closed = await self.db.get_closed_trades(150)
        lines = ["🤖 <b>گزارش ناظر هوش مصنوعی</b>\n"]

        if decisions:
            reasons = Counter()
            neutral = signal = 0
            for d in decisions:
                if d["action"] == "neutral":
                    neutral += 1
                    reasons[(d["reason"] or "نامشخص")[:70]] += 1
                else:
                    signal += 1
            lines.append(f"📊 از {len(decisions)} تصمیم: سیگنال={signal} | رد={neutral}")
            if reasons:
                lines.append("\n🚫 بیشترین دلایل رد:")
                for r, c in reasons.most_common(5):
                    lines.append(f"   • {c}× {r}")
        else:
            lines.append("هنوز تصمیمی نیست.")

        if closed:
            pnls = [t["pnl"] for t in closed]
            wins = [p for p in pnls if p > 0]
            early = [t for t in closed if (t.get("hold_seconds") or 9999) < 300]
            by_s = defaultdict(list)
            exits = Counter()
            for t in closed:
                by_s[t["strategy"]].append(t["pnl"])
                exits[t.get("exit_reason") or "نامشخص"] += 1
            lines.append(f"\n📈 معاملات بسته ({len(closed)}): برد={len(wins)} | باخت={len(pnls)-len(wins)}")
            if early:
                lines.append(f"   خروج زودهنگام: {len(early)} ⚠️")
            lines.append("\n🎯 استراتژی‌ها:")
            for s, v in by_s.items():
                wr = len([x for x in v if x > 0]) / len(v) * 100 if v else 0
                lines.append(f"   • {s}: {len(v)} | WR={wr:.0f}% | Σ=${sum(v):.2f}")
        else:
            lines.append("\nهنوز معامله بسته‌شده‌ای نیست.")

        lines.append("\n💡 توصیه: داده در حال جمع‌آوری است.")
        report = "\n".join(lines)
        await self.db.save_observer_report(report)
        return report

# ============================================================================
# 5. TELEGRAM
# ============================================================================
class AsyncTelegram:
    def __init__(self, engine):
        self.engine = engine
        self.base_url = f"https://api.telegram.org/bot{TG_TOKEN}"