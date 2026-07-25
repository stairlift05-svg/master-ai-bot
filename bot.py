#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v12.1 (Phemex Fixed + AI Observer)
- Fixed Phemex code 30000 (public instance for OHLCV)
- Fixed / safely ignored code 20004 (position mode)
- Full decision logging + AI Observer diagnostics
- Risk management + fee estimation + trailing + partial TP
- Compatible with UptimeRobot / Render (port 10000)
"""

import asyncio
import logging
import os
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime
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
# 1. CONFIGURATION
# ============================================================================
load_dotenv()

API_KEY = os.getenv("PHEMEX_API_KEY", "")
API_SECRET = os.getenv("PHEMEX_API_SECRET", "")
TESTNET = os.getenv("PHEMEX_TESTNET", "False").lower() in ("true", "1", "yes")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

WEB_USER = os.getenv("WEB_ADMIN_USER", "")
WEB_PASS = os.getenv("WEB_ADMIN_PASS", "")
if not WEB_USER or not WEB_PASS:
    WEB_USER = "admin"
    WEB_PASS = "admin123"
    print("⚠️ WARNING: Using default WEB credentials. Set WEB_ADMIN_USER / WEB_ADMIN_PASS in .env!")

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

# True = فیلترها کمی شل‌تر (برای تولید سیگنال بیشتر)
RELAXED_MODE = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler("quant_bot_v12.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("QuantV12.1")

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
    "last_rejection_summary": "هنوز داده‌ای نیست",
    "observer_report": "در حال جمع‌آوری داده...",
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
                    id TEXT PRIMARY KEY,
                    symbol TEXT,
                    side TEXT,
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
                )
            """)
            await db.execute("""
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
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS observer_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    report TEXT
                )
            """)
            await db.commit()

    async def insert_trade(self, t: Dict):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO trades
                   (id, symbol, side, strategy, entry_price, qty, original_qty, sl, tp1, tp)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (t["id"], t["symbol"], t["side"], t["strategy"], t["entry"], t["qty"],
                 t["qty"], t["sl"], t["tp1"], t["tp"]),
            )
            await db.commit()

    async def update_trade(self, t_id: str, qty: float, sl: float, is_partial: int, highest_pnl: float):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE trades SET qty=?, sl=?, is_partial=?, highest_pnl_pct=? WHERE id=?",
                (qty, sl, is_partial, highest_pnl, t_id),
            )
            await db.commit()

    async def close_trade(self, t_id: str, pnl: float, fees_est: float = 0.0,
                          reason: str = "", hold_seconds: float = 0.0):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """UPDATE trades SET status='closed', pnl=?, fees_est=?, exit_reason=?,
                   hold_seconds=?, closed_at=CURRENT_TIMESTAMP WHERE id=?""",
                (pnl, fees_est, reason, hold_seconds, t_id),
            )
            await db.commit()

    async def log_decision(self, symbol: str, action: str, strategy: str, reason: str,
                           price: float = 0.0, rsi: float = 0.0, atr: float = 0.0,
                           htf_trend: str = "", extra: str = ""):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO decisions (symbol, action, strategy, reason, price, rsi, atr, htf_trend, extra)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (symbol, action, strategy, reason, price, rsi, atr, htf_trend, extra),
            )
            await db.commit()

    async def get_open_trades(self) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM trades WHERE status='open'") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def update_analytics(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT pnl FROM trades WHERE status='closed'") as cursor:
                rows = await cursor.fetchall()
                if not rows:
                    return
                pnls = [r[0] for r in rows]
                wins = len([p for p in pnls if p > 0])
                total = len(pnls)
                async with STATE_LOCK:
                    SHARED_STATE["stats"] = {
                        "total_trades": total,
                        "win_rate": round((wins / total) * 100, 1) if total > 0 else 0.0,
                        "total_pnl": round(sum(pnls), 2),
                    }

    async def get_recent_decisions(self, limit: int = 300) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM decisions ORDER BY id DESC LIMIT ?", (limit,)
            ) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def get_closed_trades(self, limit: int = 200) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?",
                (limit,),
            ) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def save_observer_report(self, report: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO observer_logs (report) VALUES (?)", (report,))
            await db.commit()

# ============================================================================
# 3. INDICATORS + STRATEGY
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close: pd.Series, n=14):
        delta = close.diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        ma_up = up.ewm(com=n - 1, adjust=False).mean()
        ma_down = down.ewm(com=n - 1, adjust=False).mean()
        rs = ma_up / ma_down.replace(0, 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def atr(df: pd.DataFrame, n=14):
        tr = pd.concat(
            [
                df["high"] - df["low"],
                (df["high"] - df["close"].shift()).abs(),
                (df["low"] - df["close"].shift()).abs(),
            ],
            axis=1,
        ).max(axis=1)
        return tr.ewm(com=n - 1, adjust=False).mean()

    @staticmethod
    def supertrend(df: pd.DataFrame, period=10, multiplier=3.0):
        atr = Indicators.atr(df, period)
        hl2 = (df["high"] + df["low"]) / 2
        upper = hl2 + multiplier * atr
        lower = hl2 - multiplier * atr
        direction = pd.Series(1, index=df.index)
        for i in range(1, len(df)):
            if df["close"].iloc[i] > upper.iloc[i - 1]:
                direction.iloc[i] = 1
            elif df["close"].iloc[i] < lower.iloc[i - 1]:
                direction.iloc[i] = -1
            else:
                direction.iloc[i] = direction.iloc[i - 1]
                if direction.iloc[i] == 1 and lower.iloc[i] < lower.iloc[i - 1]:
                    lower.iloc[i] = lower.iloc[i - 1]
                if direction.iloc[i] == -1 and upper.iloc[i] > upper.iloc[i - 1]:
                    upper.iloc[i] = upper.iloc[i - 1]
        return direction, upper, lower

    @staticmethod
    def sma(series: pd.Series, period: int):
        return series.rolling(window=period).mean()

    @staticmethod
    def highest(series: pd.Series, period: int):
        return series.rolling(window=period).max()

    @staticmethod
    def lowest(series: pd.Series, period: int):
        return series.rolling(window=period).min()


class StrategyEngine:
    def analyze(self, df_5m: pd.DataFrame, df_1h: pd.DataFrame) -> Dict:
        df_c = df_5m.iloc[:-1].copy()
        df_htf = df_1h.iloc[:-1].copy()

        if len(df_c) < 60 or len(df_htf) < 50:
            return {
                "action": "neutral",
                "reason": "داده ناکافی (کمتر از ۶۰ کندل)",
                "strat": "",
                "rsi": 0,
                "atr": 0,
                "htf": "",
            }

        htf_close = df_htf["close"]
        ema50_htf = htf_close.ewm(span=50, adjust=False).mean().iloc[-1]
        ema200_htf = htf_close.ewm(span=min(200, len(df_htf)), adjust=False).mean().iloc[-1]
        htf_price = htf_close.iloc[-1]

        if htf_price > ema50_htf and ema50_htf > ema200_htf * 0.998:
            htf_trend = "bullish"
        elif htf_price < ema50_htf and ema50_htf < ema200_htf * 1.002:
            htf_trend = "bearish"
        else:
            return {
                "action": "neutral",
                "reason": f"روند HTF نامشخص (price={htf_price:.4f})",
                "strat": "",
                "rsi": 0,
                "atr": 0,
                "htf": "sideways",
            }

        c = df_c["close"]
        high = df_c["high"]
        low = df_c["low"]
        vol = df_c["volume"]
        price = float(c.iloc[-1])
        atr_series = Indicators.atr(df_c, 14)
        atr = float(atr_series.iloc[-1])
        if atr <= 0:
            return {"action": "neutral", "reason": "ATR نامعتبر", "strat": "", "rsi": 0, "atr": 0, "htf": htf_trend}

        atr_sma = float(Indicators.sma(atr_series, 20).iloc[-1])
        low_mult = 0.45 if RELAXED_MODE else 0.55
        high_mult = 3.2 if RELAXED_MODE else 2.8
        if atr < atr_sma * low_mult:
            return {
                "action": "neutral",
                "reason": f"نوسان خیلی کم (ATR={atr:.5f})",
                "strat": "",
                "rsi": 0,
                "atr": atr,
                "htf": htf_trend,
            }
        if atr > atr_sma * high_mult:
            return {
                "action": "neutral",
                "reason": f"نوسان خیلی زیاد (ATR={atr:.5f})",
                "strat": "",
                "rsi": 0,
                "atr": atr,
                "htf": htf_trend,
            }

        rsi_series = Indicators.rsi(c, 14)
        rsi_curr = float(rsi_series.iloc[-1])
        rsi_prev = float(rsi_series.iloc[-2])
        ema20 = float(c.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = float(c.ewm(span=50, adjust=False).mean().iloc[-1])
        st_dir, st_upper, st_lower = Indicators.supertrend(df_c)
        vol_sma = float(Indicators.sma(vol, 20).iloc[-1])
        vol_curr = float(vol.iloc[-1])
        highest_10 = float(Indicators.highest(high, 10).iloc[-1])
        lowest_10 = float(Indicators.lowest(low, 10).iloc[-1])
        prev_high_10 = float(Indicators.highest(high, 10).iloc[-2])
        prev_low_10 = float(Indicators.lowest(low, 10).iloc[-2])

        # STRATEGY 1: Breakout Momentum
        if htf_trend == "bullish" and price > ema20 and price >= highest_10 * 0.999:
            if (prev_high_10 <= highest_10) and (48 < rsi_curr < 75) and (vol_curr > vol_sma * (1.15 if RELAXED_MODE else 1.3)):
                return self._build_signal("buy", "Breakout_Momentum", price, atr, rsi_curr, htf_trend)
        if htf_trend == "bearish" and price < ema20 and price <= lowest_10 * 1.001:
            if (prev_low_10 >= lowest_10) and (25 < rsi_curr < 52) and (vol_curr > vol_sma * (1.15 if RELAXED_MODE else 1.3)):
                return self._build_signal("sell", "Breakout_Momentum", price, atr, rsi_curr, htf_trend)

        # STRATEGY 2: MTF Pullback
        if htf_trend == "bullish" and price > ema20 and ema20 > ema50 * 0.999:
            if rsi_prev <= (42 if RELAXED_MODE else 40) and rsi_curr > rsi_prev and rsi_curr < 62:
                return self._build_signal("buy", "MTF_Pullback", price, atr, rsi_curr, htf_trend)
        if htf_trend == "bearish" and price < ema20 and ema20 < ema50 * 1.001:
            if rsi_prev >= (58 if RELAXED_MODE else 60) and rsi_curr < rsi_prev and rsi_curr > 38:
                return self._build_signal("sell", "MTF_Pullback", price, atr, rsi_curr, htf_trend)

        # STRATEGY 3: SuperTrend Pullback
        if htf_trend == "bullish" and st_dir.iloc[-1] == 1:
            if low.iloc[-1] <= st_lower.iloc[-1] * 1.005 and price > low.iloc[-1] and c.iloc[-1] > c.iloc[-2]:
                if 38 < rsi_curr < 65:
                    return self._build_signal("buy", "SuperTrend_Pullback", price, atr, rsi_curr, htf_trend)
        if htf_trend == "bearish" and st_dir.iloc[-1] == -1:
            if high.iloc[-1] >= st_upper.iloc[-1] * 0.995 and price < high.iloc[-1] and c.iloc[-1] < c.iloc[-2]:
                if 35 < rsi_curr < 62:
                    return self._build_signal("sell", "SuperTrend_Pullback", price, atr, rsi_curr, htf_trend)

        # STRATEGY 4: Volume Surge
        if htf_trend == "bullish" and price > ema20 and vol_curr > vol_sma * (1.5 if RELAXED_MODE else 1.8):
            if c.iloc[-1] > c.iloc[-2] and 48 < rsi_curr < 70:
                return self._build_signal("buy", "Volume_Surge", price, atr, rsi_curr, htf_trend)
        if htf_trend == "bearish" and price < ema20 and vol_curr > vol_sma * (1.5 if RELAXED_MODE else 1.8):
            if c.iloc[-1] < c.iloc[-2] and 30 < rsi_curr < 52:
                return self._build_signal("sell", "Volume_Surge", price, atr, rsi_curr, htf_trend)

        return {
            "action": "neutral",
            "reason": f"هیچ استراتژی شرایط نداشت (RSI={rsi_curr:.1f}, HTF={htf_trend}, VolRatio={vol_curr/max(vol_sma,1e-9):.2f})",
            "strat": "",
            "rsi": rsi_curr,
            "atr": atr,
            "htf": htf_trend,
        }

    def _build_signal(self, side: str, strat: str, price: float, atr: float, rsi: float, htf: str) -> Dict:
        sl_mult, tp_mult, tp1_mult = 1.5, 2.8, 1.4
        if strat == "Breakout_Momentum":
            sl_mult, tp_mult, tp1_mult = 1.25, 3.2, 1.8
        elif strat == "Volume_Surge":
            sl_mult, tp_mult, tp1_mult = 1.35, 2.4, 1.4

        if side == "buy":
            sl = price - atr * sl_mult
            tp = price + atr * tp_mult
            tp1 = price + atr * tp1_mult
        else:
            sl = price + atr * sl_mult
            tp = price - atr * tp_mult
            tp1 = price - atr * tp1_mult

        return {
            "action": side,
            "strat": strat,
            "sl": sl,
            "tp": tp,
            "tp1": tp1,
            "reason": f"سیگنال {strat} تأیید شد",
            "rsi": rsi,
            "atr": atr,
            "htf": htf,
        }

# ============================================================================
# 4. AI OBSERVER
# ============================================================================
class AIObserver:
    def __init__(self, db: AsyncDB):
        self.db = db

    async def generate_report(self) -> str:
        decisions = await self.db.get_recent_decisions(400)
        closed = await self.db.get_closed_trades(150)

        lines = []
        lines.append("🤖 <b>گزارش ناظر هوش مصنوعی (AI Observer)</b>\n")

        if decisions:
            reasons = Counter()
            neutral_count = 0
            signal_count = 0
            for d in decisions:
                if d["action"] == "neutral":
                    neutral_count += 1
                    r = (d["reason"] or "نامشخص")[:70]
                    reasons[r] += 1
                else:
                    signal_count += 1

            lines.append(f"📊 از {len(decisions)} تصمیم اخیر:")
            lines.append(f"   • سیگنال معتبر: {signal_count}")
            lines.append(f"   • رد شده: {neutral_count}")
            if reasons:
                lines.append("\n🚫 <b>بیشترین دلایل عدم معامله:</b>")
                for reason, cnt in reasons.most_common(6):
                    lines.append(f"   • {cnt}× {reason}")
        else:
            lines.append("هنوز تصمیمی ثبت نشده.")

        if closed:
            pnls = [t["pnl"] for t in closed]
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p <= 0]
            early_exits = [t for t in closed if (t.get("hold_seconds") or 9999) < 300]
            by_strat = defaultdict(list)
            exit_reasons = Counter()
            for t in closed:
                by_strat[t["strategy"]].append(t["pnl"])
                exit_reasons[t.get("exit_reason") or "نامشخص"] += 1

            lines.append(f"\n📈 <b>معاملات بسته شده ({len(closed)}):</b>")
            lines.append(f"   • برد: {len(wins)} | باخت: {len(losses)}")
            if pnls:
                lines.append(f"   • میانگین PnL: ${sum(pnls)/len(pnls):.2f}")
            if early_exits:
                lines.append(f"   • خروج زودهنگام (<۵ دقیقه): {len(early_exits)} ⚠️")

            lines.append("\n📉 <b>دلایل خروج:</b>")
            for reason, cnt in exit_reasons.most_common(5):
                lines.append(f"   • {cnt}× {reason}")

            lines.append("\n🎯 <b>عملکرد استراتژی‌ها:</b>")
            for strat, vals in by_strat.items():
                wr = len([v for v in vals if v > 0]) / len(vals) * 100 if vals else 0
                lines.append(f"   • {strat}: {len(vals)} معامله | WR={wr:.0f}% | Sum=${sum(vals):.2f}")
        else:
            lines.append("\nهنوز معامله بسته‌شده‌ای وجود ندارد.")

        lines.append("\n💡 <b>توصیه ناظر:</b>")
        if decisions and neutral_count > signal_count * 8:
            lines.append("• فیلترها خیلی سخت هستند. RELAXED_MODE را چک کنید.")
        if closed and len(early_exits) > len(closed) * 0.35:
            lines.append("• خروج‌های زودهنگام زیاد است → TRAIL یا SL را بررسی کنید.")
        if not closed and signal_count == 0:
            lines.append("• هیچ سیگنالی تولید نشده → بازار رنج یا فیلتر HTF مانع است.")
        if not any(l.startswith("•") for l in lines[-3:]):
            lines.append("• داده در حال جمع‌آوری است. بعداً می‌توان مدل ML واقعی آموزش داد.")

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
        self.offset = 0

    def main_menu(self):
        btn = "⏸️ Pause" if SHARED_STATE["is_active"] else "▶️ Start"
        action = "cmd_pause" if SHARED_STATE["is_active"] else "cmd_start"
        return {
            "inline_keyboard": [
                [
                    {"text": "📊 Dashboard", "callback_data": "cmd_dash"},
                    {"text": "💼 Positions", "callback_data": "cmd_pos"},
                ],
                [
                    {"text": "🔄 Sync", "callback_data": "cmd_sync"},
                    {"text": btn, "callback_data": action},
                ],
                [
                    {"text": "🤖 AI Observer", "callback_data": "cmd_observer"},
                    {"text": "🚫 Last Rejections", "callback_data": "cmd_rejections"},
                ],
                [{"text": "⚡ Live Test", "callback_data": "cmd_livetest"}],
            ]
        }

    def close_menu(self, pid):
        return {"inline_keyboard": [[{"text": "❌ Force Close", "callback_data": f"close_{pid}"}]]}

    async def send(self, msg: str, reply_markup=None):
        if not TG_TOKEN:
            return
        if len(msg) > 4000:
            msg = msg[:3900] + "\n\n... (truncated)"
        payload = {"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"{self.base_url}/sendMessage", json=payload, timeout=15)
        except Exception as e:
            log.error(f"TG send error: {e}")

    async def answer_callback(self, cb_id: str, text: str):
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(
                    f"{self.base_url}/answerCallbackQuery",
                    json={"callback_query_id": cb_id, "text": text},
                    timeout=5,
                )
        except Exception:
            pass

    async def poll(self):
        if not TG_TOKEN:
            return
        mode = "TESTNET" if TESTNET else "MAINNET"
        await self.send(
            f"🚀 <b>Master Quant V12.1 Online</b>\n"
            f"Network: <b>{mode}</b>\n"
            f"Phemex 30000/20004 Fixed + AI Observer\n"
            f"RELAXED_MODE = {RELAXED_MODE}",
            self.main_menu(),
        )
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{self.base_url}/getUpdates?offset={self.offset + 1}&timeout=10"
                    ) as r:
                        data = await r.json()
                        for upd in data.get("result", []):
                            self.offset = upd["update_id"]
                            if "message" in upd and upd["message"].get("text", "") in ("/start", "/menu"):
                                await self.send("🎛️ Control Panel V12.1", self.main_menu())
                            if "callback_query" in upd:
                                cb = upd["callback_query"]
                                data_cb = cb["data"]
                                await self.answer_callback(cb["id"], "OK")
                                if data_cb == "cmd_start":
                                    async with STATE_LOCK:
                                        SHARED_STATE["is_active"] = True
                                    await self.send("▶️ Started", self.main_menu())
                                elif data_cb == "cmd_pause":
                                    async with STATE_LOCK:
                                        SHARED_STATE["is_active"] = False
                                    await self.send("⏸️ Paused", self.main_menu())
                                elif data_cb == "cmd_sync":
                                    await self.send("🔄 Syncing...")
                                    await self.engine.smart_sync_positions()
                                elif data_cb == "cmd_dash":
                                    async with STATE_LOCK:
                                        st = dict(SHARED_STATE)
                                    await self.send(
                                        f"📊 <b>V12.1 Dashboard</b>\n"
                                        f"State: {'✅' if st['is_active'] else '⏸️'}\n"
                                        f"Balance: <b>${st['balance']:.2f}</b>\n"
                                        f"DD: {st['current_dd']:.1f}% | Daily: ${st['daily_pnl']:.2f}\n"
                                        f"Pos: {len(st['active_positions'])}/{MAX_POS}\n"
                                        f"Total PnL: ${st['stats']['total_pnl']:.2f} | WR: {st['stats']['win_rate']}%\n"
                                        f"Last scan: {st['last_scan']}",
                                        self.main_menu(),
                                    )
                                elif data_cb == "cmd_pos":
                                    async with STATE_LOCK:
                                        pos = dict(SHARED_STATE["active_positions"])
                                    if not pos:
                                        await self.send("💤 هیچ پوزیشن فعالی نیست", self.main_menu())
                                    else:
                                        for pid, p in pos.items():
                                            c_price = self.engine.prices.get(p["symbol"], p["entry"])
                                            pnl = (c_price - p["entry"]) * p["qty"] * (1 if p["side"] == "buy" else -1)
                                            await self.send(
                                                f"{'🟢' if pnl >= 0 else '🔴'} <b>{p['symbol']}</b> {p['side'].upper()}\n"
                                                f"Entry: {p['entry']:.4f} | PnL: ${pnl:.2f}",
                                                self.close_menu(pid),
                                            )
                                elif data_cb.startswith("close_"):
                                    await self.engine.force_close_position(
                                        data_cb.split("close_")[1], "Telegram force close"
                                    )
                                elif data_cb == "cmd_livetest":
                                    asyncio.create_task(self.engine.run_live_test())
                                elif data_cb == "cmd_observer":
                                    await self.send("🤖 در حال تولید گزارش ناظر...")
                                    report = await self.engine.observer.generate_report()
                                    await self.send(report, self.main_menu())
                                elif data_cb == "cmd_rejections":
                                    decs = await self.engine.db.get_recent_decisions(25)
                                    if not decs:
                                        await self.send("هنوز ردشدگی ثبت نشده", self.main_menu())
                                    else:
                                        msg = "🚫 <b>آخرین تصمیم‌ها:</b>\n\n"
                                        for d in decs[:12]:
                                            icon = "✅" if d["action"] != "neutral" else "⛔"
                                            msg += f"{icon} <b>{d['symbol']}</b> | {d['action']}\n{d['reason'][:90]}\n\n"
                                        await self.send(msg, self.main_menu())
            except Exception as e:
                log.error(f"TG poll error: {e}")
            await asyncio.sleep(1)

# ============================================================================
# 6. CORE ENGINE (Phemex Fixed)
# ============================================================================
class QuantEngine:
    def __init__(self):
        self.db = AsyncDB()
        self.strategy = StrategyEngine()
        self.observer = AIObserver(self.db)
        self.tg = AsyncTelegram(self)

        # Private instance (برای معامله و بالانس)
        self.ex = ccxt.phemex({
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        })
        self.ex.set_sandbox_mode(TESTNET)

        # Public instance (فقط برای OHLCV و تیکر - حل خطای 30000)
        self.ex_public = ccxt.phemex({
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        })
        self.ex_public.set_sandbox_mode(TESTNET)

        self.prices: Dict[str, float] = {}
        self.markets_cache = {}
        self.loop_count = 0
        self.open_times: Dict[str, float] = {}

    async def start(self):
        await self.db.init_db()
        await self.db.update_analytics()

        try:
            self.markets_cache = await self.ex.load_markets()
            await self.ex_public.load_markets()
            log.info(f"Loaded {len(self.markets_cache)} markets")

            # تلاش برای تنظیم Position Mode به One-Way (حل 20004)
            try:
                await self.ex.set_position_mode(False)  # False = One-Way mode
                log.info("Position mode set to One-Way")
            except Exception as e:
                log.warning(f"set_position_mode failed (ممکن است از قبل تنظیم باشد): {e}")

            for sym in SYMBOLS:
                try:
                    await self.ex.set_leverage(LEVERAGE, sym)
                    log.info(f"Leverage {LEVERAGE}x → {sym}")
                except Exception as e:
                    log.warning(f"Leverage set skipped for {sym}: {e}")

        except Exception as e:
            log.error(f"Market loading failed: {e}")

        for t in await self.db.get_open_trades():
            async with STATE_LOCK:
                SHARED_STATE["active_positions"][t["id"]] = {
                    "id": t["id"],
                    "symbol": t["symbol"],
                    "side": t["side"],
                    "strategy": t["strategy"],
                    "entry": t["entry_price"],
                    "qty": t["qty"],
                    "sl": t["sl"],
                    "tp": t["tp"],
                    "tp1": t["tp1"],
                    "is_partial": t["is_partial"],
                    "highest_pnl_pct": t["highest_pnl_pct"],
                }
                self.open_times[t["id"]] = time.time()

        await self.smart_sync_positions()

        try:
            bal = await self.ex.fetch_balance()
            usdt = float(bal.get("USDT", {}).get("total", 0) or 0)
            async with STATE_LOCK:
                SHARED_STATE["balance"] = usdt
                SHARED_STATE["peak_balance"] = max(SHARED_STATE["peak_balance"], usdt)
                if SHARED_STATE["day_start_balance"] <= 0:
                    SHARED_STATE["day_start_balance"] = usdt
        except Exception:
            pass

        asyncio.create_task(self.periodic_observer())

        await asyncio.gather(
            self.price_loop(),
            self.scan_loop(),
            self.watchdog_loop(),
            self.tg.poll(),
        )

    async def periodic_observer(self):
        while True:
            await asyncio.sleep(6 * 3600)
            try:
                report = await self.observer.generate_report()
                await self.tg.send(report)
            except Exception as e:
                log.error(f"Periodic observer: {e}")

    def _estimate_fees(self, notional: float) -> float:
        return notional * TAKER_FEE * 2 * FEE_BUFFER

    async def check_balance_before_trade(self, symbol: str, side: str, qty: float, price: float) -> Tuple[bool, str]:
        try:
            balance = await self.ex.fetch_balance()
            free = float(balance.get("USDT", {}).get("free", 0) or 0)
            margin_needed = (qty * price) / LEVERAGE
            required = margin_needed * 1.02
            if free < required:
                return False, f"موجودی آزاد کافی نیست (نیاز \~${required:.2f}, دارید ${free:.2f})"
            return True, "OK"
        except Exception as e:
            return False, f"خطا در چک موجودی: {e}"

    def calculate_safe_order_amount(self, symbol: str, target_usd: float, price: float) -> float:
        if price <= 0 or target_usd <= 0:
            return 0.0
        try:
            amount = target_usd / price
            amount = float(self.ex.amount_to_precision(symbol, amount))
            if amount * price < MIN_ORDER_USD:
                amount = float(self.ex.amount_to_precision(symbol, MIN_ORDER_USD / price))
            return max(amount, 0.0)
        except Exception as e:
            log.warning(f"Precision {symbol}: {e}")
            return 0.0

    async def auto_adjust_order_size(self, symbol: str, target_usd: float) -> float:
        try:
            balance = await self.ex.fetch_balance()
            free = float(balance.get("USDT", {}).get("free", 0) or 0)
            price = self.prices.get(symbol)
            if not price or price <= 0:
                return 0.0
            async with STATE_LOCK:
                current_exposure = sum(
                    p["qty"] * self.prices.get(p["symbol"], p["entry"])
                    for p in SHARED_STATE["active_positions"].values()
                )
                max_total = SHARED_STATE["balance"] * (MAX_EXPOSURE_PCT / 100)
                remaining = max(0.0, max_total - current_exposure)
            max_by_balance = free * 0.18 * LEVERAGE
            adjusted = min(target_usd, max_by_balance, remaining)
            if adjusted < MIN_ORDER_USD:
                return 0.0
            return self.calculate_safe_order_amount(symbol, adjusted, price)
        except Exception as e:
            log.error(f"Auto adjust: {e}")
            return 0.0

    async def safe_close_position(self, symbol: str, side: str, amount: float) -> Optional[Dict]:
        if amount <= 0:
            return None
        close_side = "sell" if side == "buy" else "buy"
        amount = float(self.ex.amount_to_precision(symbol, abs(amount)))
        for params in [
            {"reduceOnly": True},
            {"reduceOnly": True, "posSide": "long" if side == "buy" else "short"},
            {},
        ]:
            try:
                return await self.ex.create_market_order(symbol, close_side, amount, params=params)
            except Exception as e:
                log.warning(f"Close attempt failed: {e}")
                await asyncio.sleep(0.35)
        try:
            positions = await self.ex.fetch_positions([symbol])
            for pos in positions:
                size = abs(float(pos.get("contracts") or 0))
                if size > 0:
                    exact = float(self.ex.amount_to_precision(symbol, size))
                    return await self.ex.create_market_order(symbol, close_side, exact)
        except Exception as e:
            log.error(f"Final close failed: {e}")
        return None

    async def smart_sync_positions(self):
        try:
            remote = await self.ex.fetch_positions()
            active_remote = set()
            for pos in remote:
                size = abs(float(pos.get("contracts") or pos.get("info", {}).get("size") or 0))
                if size <= 0:
                    continue
                raw = pos.get("symbol", "")
                matched = next((s for s in SYMBOLS if s.split("/")[0] in raw or raw in s), None)
                if not matched:
                    continue
                active_remote.add(matched)
                entry = float(pos.get("entryPrice") or pos.get("info", {}).get("entryPrice") or 0)
                side = "buy" if pos.get("side") == "long" else "sell"
                async with STATE_LOCK:
                    already = any(p["symbol"] == matched for p in SHARED_STATE["active_positions"].values())
                if not already and entry > 0:
                    pid = f"sync_{uuid.uuid4().hex[:8]}"
                    pos_data = {
                        "id": pid,
                        "symbol": matched,
                        "side": side,
                        "strategy": "Adopted",
                        "entry": entry,
                        "qty": size,
                        "sl": entry * 0.92 if side == "buy" else entry * 1.08,
                        "tp": entry * 1.08 if side == "buy" else entry * 0.92,
                        "tp1": entry * 1.04 if side == "buy" else entry * 0.96,
                        "is_partial": 0,
                        "highest_pnl_pct": 0.0,
                    }
                    async with STATE_LOCK:
                        SHARED_STATE["active_positions"][pid] = pos_data
                    self.open_times[pid] = time.time()
                    await self.db.insert_trade(pos_data)
                    await self.tg.send(f"🔄 Adopted {matched} ({side})")

            async with STATE_LOCK:
                to_remove = [
                    pid for pid, p in SHARED_STATE["active_positions"].items()
                    if p["symbol"] not in active_remote and p["strategy"] != "LiveTest"
                ]
            for pid in to_remove:
                await self.db.close_trade(pid, 0.0, reason="Synced closed remotely")
                async with STATE_LOCK:
                    SHARED_STATE["active_positions"].pop(pid, None)
                self.open_times.pop(pid, None)
        except Exception as e:
            log.error(f"Sync error: {e}")

    async def price_loop(self):
        while True:
            try:
                # از public برای تیکر استفاده می‌کنیم
                tickers = await self.ex_public.fetch_tickers(SYMBOLS)
                for s, d in tickers.items():
                    if d.get("last"):
                        self.prices[s] = float(d["last"])

                bal = await self.ex.fetch_balance()
                current = float(bal.get("USDT", {}).get("total", 0) or 0)
                async with STATE_LOCK:
                    SHARED_STATE["balance"] = current
                    if current > SHARED_STATE["peak_balance"]:
                        SHARED_STATE["peak_balance"] = current
                    if SHARED_STATE["day_start_balance"] <= 0:
                        SHARED_STATE["day_start_balance"] = current
                    peak = SHARED_STATE["peak_balance"]
                    if peak > 0:
                        dd = ((peak - current) / peak) * 100
                        SHARED_STATE["current_dd"] = dd
                        if dd >= MAX_DD and not SHARED_STATE["dd_halted"]:
                            SHARED_STATE["dd_halted"] = True
                            await self.tg.send(f"⛔ DD HALT {dd:.1f}%")
                        elif dd < MAX_DD * 0.75 and SHARED_STATE["dd_halted"]:
                            SHARED_STATE["dd_halted"] = False
                            await self.tg.send("✅ DD recovered")
                    day_start = SHARED_STATE["day_start_balance"]
                    if day_start > 0:
                        daily_pnl = current - day_start
                        SHARED_STATE["daily_pnl"] = daily_pnl
                        daily_pct = (daily_pnl / day_start) * 100
                        if daily_pct <= -MAX_DAILY_LOSS_PCT and not SHARED_STATE["daily_halted"]:
                            SHARED_STATE["daily_halted"] = True
                            await self.tg.send(f"🛑 Daily loss limit ({daily_pct:.1f}%)")
                        elif daily_pct > -MAX_DAILY_LOSS_PCT * 0.5:
                            SHARED_STATE["daily_halted"] = False
                async with STATE_LOCK:
                    SHARED_STATE["health"]["last_price_ok"] = True
            except Exception as e:
                log.error(f"Price loop: {e}")
                async with STATE_LOCK:
                    SHARED_STATE["health"]["last_price_ok"] = False
            await asyncio.sleep(2)

    async def scan_loop(self):
        while True:
            self.loop_count += 1
            if self.loop_count % 40 == 0:
                await self.smart_sync_positions()

            async with STATE_LOCK:
                can_scan = (
                    SHARED_STATE["is_active"]
                    and not SHARED_STATE["dd_halted"]
                    and not SHARED_STATE["daily_halted"]
                    and len(SHARED_STATE["active_positions"]) < MAX_POS
                )
            if not can_scan:
                if self.loop_count % 20 == 0:
                    await self.db.log_decision("ALL", "neutral", "", "ربات متوقف / DD / Daily / سقف پوزیشن")
                await asyncio.sleep(6)
                continue

            async with STATE_LOCK:
                SHARED_STATE["last_scan"] = time.strftime("%H:%M:%S")

            for sym in SYMBOLS:
                async with STATE_LOCK:
                    has_pos = any(p["symbol"] == sym for p in SHARED_STATE["active_positions"].values())
                if has_pos:
                    continue
                try:
                    # ===== حل خطای 30000: استفاده از ex_public =====
                    raw_5m = await self.ex_public.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=120)
                    await asyncio.sleep(0.3)
                    raw_1h = await self.ex_public.fetch_ohlcv(sym, timeframe=HTF_TIMEFRAME, limit=100)

                    if not raw_5m or not raw_1h or len(raw_5m) < 50:
                        await self.db.log_decision(sym, "neutral", "", "OHLCV خالی یا ناکافی")
                        continue

                    df_5m = pd.DataFrame(raw_5m, columns=["ts", "open", "high", "low", "close", "volume"])
                    df_1h = pd.DataFrame(raw_1h, columns=["ts", "open", "high", "low", "close", "volume"])

                    sig = self.strategy.analyze(df_5m, df_1h)

                    await self.db.log_decision(
                        sym,
                        sig["action"],
                        sig.get("strat", ""),
                        sig.get("reason", ""),
                        price=self.prices.get(sym, 0),
                        rsi=sig.get("rsi", 0),
                        atr=sig.get("atr", 0),
                        htf_trend=sig.get("htf", ""),
                    )

                    if sig["action"] != "neutral":
                        await self.execute_trade(sym, sig)

                except Exception as e:
                    err_msg = str(e)
                    log.error(f"Scan {sym}: {err_msg}")
                    await self.db.log_decision(sym, "neutral", "", f"خطای اسکن: {err_msg[:100]}")
                await asyncio.sleep(0.55)
            await asyncio.sleep(16)

    async def execute_trade(self, sym: str, sig: Dict):
        price = self.prices.get(sym)
        async with STATE_LOCK:
            bal = SHARED_STATE["balance"]
        if not price or price <= 0 or bal < 15:
            await self.db.log_decision(sym, "rejected", sig.get("strat", ""), "قیمت یا بالانس نامعتبر")
            return

        risk_amount = bal * (RISK_PCT / 100)
        dist = abs(price - sig["sl"])
        if dist <= 0:
            await self.db.log_decision(sym, "rejected", sig.get("strat", ""), "فاصله SL صفر")
            return

        target_usd = (risk_amount / dist) * price * 0.92
        qty = await self.auto_adjust_order_size(sym, target_usd)
        if qty <= 0:
            await self.db.log_decision(sym, "rejected", sig.get("strat", ""), "سایز سفارش صفر یا زیر حداقل")
            return

        ok, msg = await self.check_balance_before_trade(sym, sig["action"], qty, price)
        if not ok:
            await self.db.log_decision(sym, "rejected", sig.get("strat", ""), msg)
            await self.tg.send(f"⚠️ رد معامله {sym}\n{msg}")
            return

        if qty * price < MIN_ORDER_USD:
            await self.db.log_decision(sym, "rejected", sig.get("strat", ""), "مقدار کمتر از حداقل سفارش")
            return

        try:
            order = await self.ex.create_market_order(sym, sig["action"], qty)
            fill = float(order.get("average") or order.get("price") or price)
            pid = f"pos_{uuid.uuid4().hex[:8]}"
            pos = {
                "id": pid,
                "symbol": sym,
                "side": sig["action"],
                "strategy": sig["strat"],
                "entry": fill,
                "qty": qty,
                "sl": sig["sl"],
                "tp": sig["tp"],
                "tp1": sig["tp1"],
                "is_partial": 0,
                "highest_pnl_pct": 0.0,
            }
            async with STATE_LOCK:
                SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()
            await self.db.insert_trade(pos)
            await self.db.log_decision(sym, "entered", sig["strat"], f"ورود موفق @ {fill:.4f}")

            await self.tg.send(
                f"🎯 <b>ورود {sig['action'].upper()} ({sig['strat']})</b>\n"
                f"{sym} @ {fill:.4f}\nQty: {qty:.4f}"
            )

            try:
                sl_side = "sell" if sig["action"] == "buy" else "buy"
                await self.ex.create_order(
                    sym, "stop", sl_side, qty, None,
                    params={"stopPrice": sig["sl"], "reduceOnly": True},
                )
            except Exception as e:
                log.warning(f"Exchange SL failed: {e}")
        except Exception as e:
            err = str(e)[:120]
            log.error(f"Execute {sym}: {err}")
            await self.db.log_decision(sym, "rejected", sig.get("strat", ""), f"خطای اجرا: {err}")

    async def run_live_test(self):
        await self.tg.send("⚡ Live Test محافظه‌کارانه...")
        try:
            bal = await self.ex.fetch_balance()
            free = float(bal.get("USDT", {}).get("free", 0) or 0)
            if free < 25:
                await self.tg.send("❌ موجودی آزاد کم است")
                return
            test_usd = min(12.0, free * 0.06)
            sym = "ETH/USDT:USDT"
            price = self.prices.get(sym)
            if not price:
                return
            qty = self.calculate_safe_order_amount(sym, test_usd, price)
            if qty <= 0:
                return
            await self.ex.create_market_order(sym, "buy", qty)
            pid = f"test_{uuid.uuid4().hex[:6]}"
            pos = {
                "id": pid, "symbol": sym, "side": "buy", "strategy": "LiveTest",
                "entry": price, "qty": qty,
                "sl": price * 0.97, "tp": price * 1.03, "tp1": price * 1.015,
                "is_partial": 0, "highest_pnl_pct": 0.0,
            }
            async with STATE_LOCK:
                SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()
            await self.tg.send(f"🧪 تست باز شد {sym}")
            await asyncio.sleep(20)
            await self.force_close_position(pid, "LiveTest end")
            await self.tg.send("✅ تست تمام شد")
        except Exception as e:
            await self.tg.send(f"❌ خطا در تست: {e}")

    async def force_close_position(self, pid: str, reason: str):
        async with STATE_LOCK:
            pos = SHARED_STATE["active_positions"].get(pid)
        if not pos:
            return
        price = self.prices.get(pos["symbol"], pos["entry"])
        qty = pos["qty"]
        symbol = pos["symbol"]
        side = pos["side"]
        hold = time.time() - self.open_times.get(pid, time.time())

        try:
            await self.safe_close_position(symbol, side, qty)
            raw_pnl = (price - pos["entry"]) * qty * (1 if side == "buy" else -1)
            fees = self._estimate_fees(qty * price + qty * pos["entry"])
            net_pnl = raw_pnl - fees

            if pos["strategy"] != "LiveTest":
                await self.db.close_trade(pid, net_pnl, fees, reason, hold)
            async with STATE_LOCK:
                SHARED_STATE["active_positions"].pop(pid, None)
            self.open_times.pop(pid, None)
            await self.db.update_analytics()

            icon = "🟢" if net_pnl >= 0 else "🔴"
            await self.tg.send(
                f"{icon} <b>بسته شد ({reason})</b>\n"
                f"{symbol} | Net: \( {net_pnl:.2f} (fees\~ \){fees:.2f})\n"
                f"Hold: {hold/60:.1f} min | {pos['entry']:.4f} → {price:.4f}"
            )
        except Exception as e:
            log.error(f"Force close {pid}: {e}")

    async def watchdog_loop(self):
        while True:
            async with STATE_LOCK:
                items = list(SHARED_STATE["active_positions"].items())
            for pid, pos in items:
                if pos["strategy"] == "LiveTest":
                    continue
                price = self.prices.get(pos["symbol"])
                if not price or price <= 0:
                    continue
                pnl_pct = (
                    ((price - pos["entry"]) / pos["entry"]) * 100
                    if pos["side"] == "buy"
                    else ((pos["entry"] - price) / pos["entry"]) * 100
                )
                if pnl_pct > TRAIL_ACT:
                    if pnl_pct > pos["highest_pnl_pct"]:
                        pos["highest_pnl_pct"] = pnl_pct
                        if pos["side"] == "buy":
                            new_sl = price * (1 - TRAIL_STEP / 100)
                            if new_sl > pos["sl"]:
                                pos["sl"] = new_sl
                                await self.db.update_trade(pid, pos["qty"], pos["sl"], pos["is_partial"], pos["highest_pnl_pct"])
                        else:
                            new_sl = price * (1 + TRAIL_STEP / 100)
                            if new_sl < pos["sl"]:
                                pos["sl"] = new_sl
                                await self.db.update_trade(pid, pos["qty"], pos["sl"], pos["is_partial"], pos["highest_pnl_pct"])
                if PARTIAL_TP and pos["is_partial"] == 0:
                    hit = (pos["side"] == "buy" and price >= pos["tp1"]) or (pos["side"] == "sell" and price <= pos["tp1"])
                    if hit:
                        try:
                            half = float(self.ex.amount_to_precision(pos["symbol"], pos["qty"] / 2))
                            if 0 < half < pos["qty"]:
                                await self.safe_close_position(pos["symbol"], pos["side"], half)
                                pos["qty"] -= half
                                pos["is_partial"] = 1
                                pos["sl"] = pos["entry"]
                                await self.db.update_trade(pid, pos["qty"], pos["sl"], 1, pos["highest_pnl_pct"])
                                await self.tg.send(f"🔹 Partial TP → BE  {pos['symbol']}")
                        except Exception as e:
                            log.error(f"Partial: {e}")
                sl_hit = (pos["side"] == "buy" and price <= pos["sl"]) or (pos["side"] == "sell" and price >= pos["sl"])
                tp_hit = (pos["side"] == "buy" and price >= pos["tp"]) or (pos["side"] == "sell" and price <= pos["tp"])
                if sl_hit or tp_hit:
                    await self.force_close_position(pid, "SL/Trail" if sl_hit else "TP")
            await asyncio.sleep(1.2)

# ============================================================================
# 7. WEB DASHBOARD
# ============================================================================
app = Flask(__name__)
auth = HTTPBasicAuth()

@auth.verify_password
def verify(u, p):
    return u == WEB_USER and p == WEB_PASS

@app.before_request
@auth.login_required
def require_login():
    pass

@app.route("/api/status")
def api_status():
    return jsonify(SHARED_STATE)

@app.route("/")
def dashboard():
    html = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Quant V12.1</title>
<style>
body{font-family:system-ui;background:#0d1117;color:#c9d1d9;padding:20px}
.card{background:#161b22;border:1px solid #30363d;padding:18px;border-radius:8px;margin-bottom:12px}
.badge{background:#238636;color:#fff;padding:3px 8px;border-radius:10px;font-size:12px}
.stat{display:inline-block;margin-right:28px}
.stat-value{font-size:22px;font-weight:700;color:#58a6ff}
</style></head><body>
<h1>🚀 Master Quant Engine V12.1</h1>
<div class="card"><h2>Status: <span class="badge">ONLINE</span></h2>
<p>Phemex Fixed (30000/20004) + AI Observer + Full Diagnostics</p></div>
<div class="card"><h3>Live Stats</h3>
<div class="stat"><span class="stat-value" id="pos">0</span><br>Positions</div>
<div class="stat"><span class="stat-value" id="bal">0.00</span><br>Balance</div>
<div class="stat"><span class="stat-value" id="pnl">0.00</span><br>Total PnL</div>
<div class="stat"><span class="stat-value" id="wr">0%</span><br>Win Rate</div>
</div>
<script>
setInterval(async()=>{
  const r=await fetch('/api/status');
  const d=await r.json();
  document.getElementById('pos').textContent=Object.keys(d.active_positions||{}).length;
  document.getElementById('bal').textContent=(d.balance||0).toFixed(2);
  document.getElementById('pnl').textContent=(d.stats?.total_pnl||0).toFixed(2);
  document.getElementById('wr').textContent=(d.stats?.win_rate||0)+'%';
},4000);
</script>
</body></html>"""
    return render_template_string(html)

def run_web():
    app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False)

# ============================================================================
# 8. MAIN
# ============================================================================
if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    engine = QuantEngine()
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        log.info("Shutting down...")
    except Exception as e:
        log.error(f"Fatal: {e}")
    finally:
        try:
            asyncio.run(engine.ex.close())
            asyncio.run(engine.ex_public.close())
        except Exception:
            pass