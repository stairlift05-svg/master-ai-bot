#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v16.0 – Phemex-Only (Optimized & Fixed)
- بهبود فیلترهای تشخیص روند و کاهش سیگنال‌های غلط
- بهینه‌سازی نسبت ریسک به ریوارد و مدیریت استاپ‌لاس
- رفع مشکلات ضرردهی نسخه‌های قبل
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
    "Breakout_Momentum":   {"sl_m": 1.50, "tp_m": 3.8, "tp1_m": 2.0},
    "MTF_Pullback":        {"sl_m": 1.60, "tp_m": 3.2, "tp1_m": 1.7},
    "SuperTrend_Pullback": {"sl_m": 1.45, "tp_m": 3.0, "tp1_m": 1.6},
    "Volume_Surge":        {"sl_m": 1.40, "tp_m": 2.9, "tp1_m": 1.5},
}

TIMEFRAME = "5m"
HTF_TIMEFRAME = "1h"
RISK_PCT = 0.35  # کاهش ریسک جهت حفظ سرمایه
LEVERAGE = 5
MAX_POS = 4      # کاهش تعداد پوزیشن‌های همزمان برای دقت بیشتر
MAX_DD = 7.0
MAX_DAILY_LOSS = 3.5
MIN_ORDER_USD = 16.0
MAX_EXPOSURE_PCT = 25.0
TAKER_FEE = 0.0006
FEE_BUFFER = 1.3
TRAIL_ACT = 2.5
TRAIL_STEP = 0.8
PARTIAL_TP = True
MIN_HOLD_FOR_PARTIAL = 720          # ۱۲ دقیقه
MIN_HOLD_FOR_TRAIL = 1080           # ۱۸ دقیقه
MIN_PROFIT_FOR_BE = 0.65            # حداقل سود برای ورود به نقطه سر به سر
TEST_SYMBOL = "DOGE/USDT:USDT"
TEST_USD = 12.0
CONSECUTIVE_LOSS_LIMIT = 2
SYMBOL_COOLDOWN_HOURS = 6           # افزایش زمان کلدداون در صورت ضرر متوالی
POST_CLOSE_COOLDOWN = 1800          # ۳۰ دقیقه وقفه پس از بسته شدن پوزیشن
SCAN_INTERVAL = 60
SYMBOL_DELAY = 1.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("QuantV16")

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
    def __init__(self, path="bot_v16.db"):
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
            "       MASTER QUANT ENGINE v16.0 – REPORT",
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
        return "\n".join(lines)

# ===================== INDICATORS =====================
class Indicators:
    @staticmethod
    def rsi(series: pd.Series, n=14) -> pd.Series:
        delta = series.diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        ma_up = up.ewm(com=n - 1, adjust=False).mean()
        ma_down = down.ewm(com=n - 1, adjust=False).mean()
        rs = ma_up / ma_down.replace(0, 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def atr(df: pd.DataFrame, n=14) -> pd.Series:
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs(),
        ], axis=1).max(axis=1)
        return tr.ewm(com=n - 1, adjust=False).mean()

    @staticmethod
    def supertrend(df: pd.DataFrame, period=10, mult=3.0):
        atr = Indicators.atr(df, period)
        hl2 = (df["high"] + df["low"]) / 2
        upper = hl2 + mult * atr
        lower = hl2 - mult * atr
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
    def sma(s, p):
        return s.rolling(p).mean()

    @staticmethod
    def highest(s, p):
        return s.rolling(p).max()

    @staticmethod
    def lowest(s, p):
        return s.rolling(p).min()

# ===================== STRATEGY =====================
class StrategyEngine:
    def analyze(self, df_5m: pd.DataFrame, df_1h: pd.DataFrame) -> dict:
        df = df_5m.iloc[:-1].copy()
        htf = df_1h.iloc[:-1].copy()
        if len(df) < 55 or len(htf) < 35:
            return {"action": "neutral", "reason": "داده ناکافی", "strat": "", "rsi": 0, "atr": 0, "htf": ""}

        if df["close"].iloc[-1] <= 0 or df["high"].iloc[-1] <= 0:
            return {"action": "neutral", "reason": "داده قیمت نامعتبر", "strat": "", "rsi": 0, "atr": 0, "htf": ""}

        hclose = htf["close"]
        e50 = hclose.ewm(span=50, adjust=False).mean().iloc[-1]
        e200 = hclose.ewm(span=min(200, len(htf)), adjust=False).mean().iloc[-1]
        hp = float(hclose.iloc[-1])

        # فیلتر قدرت روند اصلاح‌شده و سخت‌گیرانه‌تر در نسخه ۱۶
        trend_strength = abs(e50 - e200) / (e200 + 1e-9) * 100
        if trend_strength < 0.25:
            return {"action": "neutral", "reason": "روند ضعیف یا رنج", "strat": "", "rsi": 0, "atr": 0, "htf": "weak"}

        if hp > e50 and e50 > e200:
            htf_trend = "bullish"
        elif hp < e50 and e50 < e200:
            htf_trend = "bearish"
        else:
            return {"action": "neutral", "reason": "روند HTF متناقض", "strat": "", "rsi": 0, "atr": 0, "htf": "sideways"}

        c, high, low, vol = df["close"], df["high"], df["low"], df["volume"]
        price = float(c.iloc[-1])
        atr_s = Indicators.atr(df, 14)
        atr = float(atr_s.iloc[-1])
        if atr <= 0 or pd.isna(atr):
            return {"action": "neutral", "reason": "ATR نامعتبر", "strat": "", "rsi": 0, "atr": 0, "htf": htf_trend}

        rsi_s = Indicators.rsi(c)
        rsi = float(rsi_s.iloc[-1])
        rsi_p = float(rsi_s.iloc[-2])

        if rsi >= 95.0 or rsi <= 5.0 or pd.isna(rsi):
            return {"action": "neutral", "reason": f"RSI اشباع خطرناک ({rsi:.1f})", "strat": "", "rsi": rsi, "atr": atr, "htf": htf_trend}

        ema20 = float(c.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = float(c.ewm(span=50, adjust=False).mean().iloc[-1])
        st_d, st_u, st_l = Indicators.supertrend(df)
        vsma = float(Indicators.sma(vol, 20).iloc[-1]) or 1e-9
        vcur = float(vol.iloc[-1])
        h10 = float(Indicators.highest(high, 10).iloc[-1])
        l10 = float(Indicators.lowest(low, 10).iloc[-1])
        vol_ok = vcur > vsma * 1.25  # نیاز به حجم بالاتر برای تایید سیگنال

        # شرایط ورود بهینه شده برای کاهش ضرر
        if htf_trend == "bullish" and price > ema20 and price >= h10 * 0.998 and 45 < rsi < 70 and vol_ok:
            return self._build("buy", "Breakout_Momentum", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and price < ema20 and price <= l10 * 1.002 and 30 < rsi < 55 and vol_ok:
            return self._build("sell", "Breakout_Momentum", price, atr, rsi, htf_trend)

        if htf_trend == "bullish" and price > ema20 and ema20 > ema50 and rsi_p <= 48 and rsi > rsi_p:
            return self._build("buy", "MTF_Pullback", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and price < ema20 and ema20 < ema50 and rsi_p >= 52 and rsi < rsi_p:
            return self._build("sell", "MTF_Pullback", price, atr, rsi, htf_trend)

        if htf_trend == "bullish" and st_d.iloc[-1] == 1 and 40 < rsi < 65:
            return self._build("buy", "SuperTrend_Pullback", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and st_d.iloc[-1] == -1 and 35 < rsi < 60:
            return self._build("sell", "SuperTrend_Pullback", price, atr, rsi, htf_trend)

        return {"action": "neutral", "reason": f"بدون سیگنال معتبر (RSI={rsi:.1f})", "strat": "", "rsi": rsi, "atr": atr, "htf": htf_trend}

    def _build(self, side, strat, price, atr, rsi, htf):
        p = STRATEGY_PARAMS.get(strat, {"sl_m": 1.5, "tp_m": 3.2, "tp1_m": 1.6})
        if side == "buy":
            return {
                "action": side, "strat": strat,
                "sl": price - atr * p["sl_m"], "tp": price + atr * p["tp_m"], "tp1": price + atr * p["tp1_m"],
                "reason": f"سیگنال تایید شده {strat}", "rsi": rsi, "atr": atr, "htf": htf,
                "expected_rr": round(p["tp_m"] / p["sl_m"], 2),
            }
        return {
            "action": side, "strat": strat,
            "sl": price + atr * p["sl_m"], "tp": price - atr * p["tp_m"], "tp1": price - atr * p["tp1_m"],
            "reason": f"سیگنال تایید شده {strat}", "rsi": rsi, "atr": atr, "htf": htf,
            "expected_rr": round(p["tp_m"] / p["sl_m"], 2),
        }

# ===================== RISK =====================
class RiskManager:
    @staticmethod
    def calculate_qty(balance, price, sl, free_usdt, symbol, exchange) -> float:
        if price <= 0 or balance <= 0:
            return 0.0
        dist = abs(price - sl)
        if dist <= 0:
            return 0.0
        qty = (balance * (RISK_PCT / 100.0)) / dist
        qty = min(qty, (free_usdt * 0.12 * LEVERAGE) / price, (balance * MAX_EXPOSURE_PCT / 100.0) / price)
        try:
            qty = float(exchange.amount_to_precision(symbol, qty))
            if qty * price < MIN_ORDER_USD:
                qty = float(exchange.amount_to_precision(symbol, MIN_ORDER_USD / price))
        except Exception:
            return 0.0
        return max(qty, 0.0)

# ===================== TELEGRAM =====================
class TelegramController:
    def __init__(self, engine):
        self.engine = engine
        self.base = f"https://api.telegram.org/bot{TG_TOKEN}"
        self.offset = 0

    def menu(self):
        btn = "⏸️ Pause" if SHARED_STATE["is_active"] else "▶️ Start"
        act = "cmd_pause" if SHARED_STATE["is_active"] else "cmd_start"
        return {"inline_keyboard": [
            [{"text": "📊 Dashboard", "callback_data": "cmd_dash"}, {"text": "💼 Positions", "callback_data": "cmd_pos"}],
            [{"text": "🔄 Sync", "callback_data": "cmd_sync"}, {"text": btn, "callback_data": act}],
            [{"text": "📄 Report", "callback_data": "cmd_txt"}, {"text": "🚫 Rejections", "callback_data": "cmd_rej"}],
        ]}

    async def send(self, text, markup=None):
        if not TG_TOKEN:
            return
        if len(text) > 4000:
            text = text[:3900] + "\n..."
        payload = {"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML"}
        if markup:
            payload["reply_markup"] = markup
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"{self.base}/sendMessage", json=payload, timeout=12)
        except Exception as e:
            log.error(f"TG send: {e}")

    async def send_document(self, path, caption=""):
        if not os.path.exists(path):
            await self.send("❌ فایل یافت نشد")
            return
        try:
            form = aiohttp.FormData()
            form.add_field("chat_id", TG_CHAT)
            form.add_field("caption", caption)
            form.add_field("document", open(path, "rb"), filename=os.path.basename(path))
            async with aiohttp.ClientSession() as s:
                await s.post(f"{self.base}/sendDocument", data=form, timeout=60)
        except Exception as e:
            await self.send(f"❌ {e}")

    async def poll(self):
        if not TG_TOKEN:
            while True:
                await asyncio.sleep(60)
            return
        await self.send("🚀 <b>Master Quant v16.0 Activated</b>\nنسخه بهینه‌سازی شده و ضد ضرر", self.menu())
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"{self.base}/getUpdates?offset={self.offset+1}&timeout=8") as r:
                        data = await r.json()
                        for u in data.get("result", []):
                            self.offset = u["update_id"]
                            if "callback_query" not in u:
                                continue
                            cb = u["callback_query"]
                            d = cb["data"]
                            try:
                                async with aiohttp.ClientSession() as ss:
                                    await ss.post(
                                        f"{self.base}/answerCallbackQuery",
                                        json={"callback_query_id": cb["id"], "text": "OK"}, timeout=4)
                            except Exception:
                                pass
                            if d == "cmd_start":
                                with STATE_LOCK:
                                    SHARED_STATE["is_active"] = True
                                await self.send("▶️ ربات شروع به کار کرد", self.menu())
                            elif d == "cmd_pause":
                                with STATE_LOCK:
                                    SHARED_STATE["is_active"] = False
                                await self.send("⏸️ ربات متوقف شد", self.menu())
                            elif d == "cmd_dash":
                                with STATE_LOCK:
                                    st = dict(SHARED_STATE)
                                await self.send(
                                    f"📊 <b>Dashboard v16.0</b>\nBalance: <b>${st['balance']:.2f}</b>\n"
                                    f"DD: {st['current_dd']:.1f}% | Pos: {len(st['active_positions'])}/{MAX_POS}\n"
                                    f"PnL: ${st['stats']['total_pnl']:.2f} | WR: {st['stats']['win_rate']}%", self.menu())
                            elif d == "cmd_txt":
                                report = await self.engine.db.generate_txt_report(self.engine.prices)
                                with open("report_v16.txt", "w", encoding="utf-8") as f:
                                    f.write(report)
                                await self.send_document("report_v16.txt", "📄 گزارش نسخه ۱۶")
            except Exception as e:
                log.error(f"TG poll: {e}")
            await asyncio.sleep(1)

# ===================== ENGINE =====================
class QuantEngine:
    def __init__(self):
        self.db = Database()
        self.strategy = StrategyEngine()
        self.risk = RiskManager()
        self.tg = TelegramController(self)

        self.ex = ccxt.phemex({
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        })
        self.ex.set_sandbox_mode(TESTNET)
        self.prices: Dict[str, float] = {}
        self.open_times: Dict[str, float] = {}

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 100) -> list:
        try:
            candles = await self.ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if not candles or len(candles) < 40:
                return []
            return candles
        except Exception as e:
            log.warning(f"fetch_ohlcv {symbol}: {e}")
            return []

    async def start(self):
        await self.db.init()
        log.info("v16.0 Engine starting...")
        try:
            await self.ex.load_markets()
            await self.ex.set_position_mode(False)
        except Exception as e:
            log.error(f"Init error: {e}")

        for sym in SYMBOLS:
            try:
                await self.ex.set_leverage(LEVERAGE, sym)
            except Exception:
                pass

        await asyncio.gather(
            self.price_loop(),
            self.scan_loop(),
            self.watchdog_loop(),
            self.tg.poll(),
        )

    async def update_balance(self):
        try:
            bal = await self.ex.fetch_balance()
            usdt = float(bal.get("USDT", {}).get("total", 0) or 0)
            with STATE_LOCK:
                SHARED_STATE["balance"] = usdt
                if usdt > SHARED_STATE["peak_balance"]:
                    SHARED_STATE["peak_balance"] = usdt
                if SHARED_STATE["day_start_balance"] <= 0:
                    SHARED_STATE["day_start_balance"] = usdt
        except Exception as e:
            log.error(f"Balance: {e}")

    async def price_loop(self):
        while True:
            try:
                tickers = await self.ex.fetch_tickers(SYMBOLS)
                for s, d in tickers.items():
                    if d.get("last"):
                        self.prices[s] = float(d["last"])
                await self.update_balance()
            except Exception as e:
                log.error(f"price_loop: {e}")
            await asyncio.sleep(8)

    async def scan_loop(self):
        while True:
            with STATE_LOCK:
                can = (
                    SHARED_STATE["is_active"]
                    and not SHARED_STATE["dd_halted"]
                    and not SHARED_STATE["daily_halted"]
                    and len(SHARED_STATE["active_positions"]) < MAX_POS
                )
                open_syms = {p["symbol"] for p in SHARED_STATE["active_positions"].values()}

            if not can:
                await asyncio.sleep(15)
                continue

            for sym in SYMBOLS:
                if sym in open_syms or sym in SYMBOL_ERROR_COOLDOWN or sym in SYMBOL_POST_CLOSE_COOLDOWN:
                    continue
                try:
                    raw5, raw1 = await asyncio.gather(
                        self.fetch_ohlcv(sym, TIMEFRAME, 100),
                        self.fetch_ohlcv(sym, HTF_TIMEFRAME, 100),
                    )
                    await asyncio.sleep(SYMBOL_DELAY)
                    if not raw5 or len(raw5) < 50:
                        continue

                    df5 = pd.DataFrame(raw5, columns=["ts", "open", "high", "low", "close", "volume"])
                    df1 = pd.DataFrame(raw1, columns=["ts", "open", "high", "low", "close", "volume"]) if raw1 else df5
                    sig = self.strategy.analyze(df5, df1)
                    price = self.prices.get(sym) or float(df5["close"].iloc[-1])

                    await self.db.log_decision(sym, sig["action"], sig.get("strat", ""), sig.get("reason", ""), price)

                    if sig["action"] != "neutral":
                        await self.execute_trade(sym, sig)
                except Exception as e:
                    log.error(f"scan {sym}: {e}")
            await asyncio.sleep(SCAN_INTERVAL)

    async def execute_trade(self, sym: str, sig: dict):
        price = self.prices.get(sym)
        with STATE_LOCK:
            bal = SHARED_STATE["balance"]
        if not price or bal < 20:
            return
        try:
            bal_data = await self.ex.fetch_balance()
            free = float(bal_data.get("USDT", {}).get("free", 0) or 0)
            qty = self.risk.calculate_qty(bal, price, sig["sl"], free, sym, self.ex)
            if qty <= 0:
                return
            order = await self.ex.create_market_order(sym, sig["action"], qty)
            fill = float(order.get("average") or price)
            pid = f"pos_v16_{uuid.uuid4().hex[:8]}"
            pos = {
                "id": pid, "symbol": sym, "side": sig["action"], "strategy": sig["strat"],
                "entry": fill, "qty": qty, "sl": sig["sl"], "tp": sig["tp"], "tp1": sig["tp1"],
                "is_partial": 0, "highest_pnl_pct": 0.0,
            }
            with STATE_LOCK:
                SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()
            await self.db.insert_trade(pos)
            await self.tg.send(f"🎯 <b>{sig['action'].upper()}</b> ({sig['strat']})\n{sym} @ {fill:.4f}")
        except Exception as e:
            log.error(f"Execute error {sym}: {e}")

    async def force_close(self, pid: str, reason: str):
        with STATE_LOCK:
            pos = SHARED_STATE["active_positions"].get(pid)
        if not pos:
            return
        price = self.prices.get(pos["symbol"], pos["entry"])
        hold = time.time() - self.open_times.get(pid, time.time())
        try:
            close_side = "sell" if pos["side"] == "buy" else "buy"
            await self.ex.create_market_order(pos["symbol"], close_side, pos["qty"], params={"reduceOnly": True})
            raw_pnl = (price - pos["entry"]) * pos["qty"] * (1 if pos["side"] == "buy" else -1)
            fees = abs(raw_pnl) * TAKER_FEE * 2 * FEE_BUFFER
            net = raw_pnl - fees
            await self.db.close_trade(pid, net, fees, reason, hold)
            with STATE_LOCK:
                SHARED_STATE["active_positions"].pop(pid, None)
            self.open_times.pop(pid, None)
            await self.db.update_analytics()
            SYMBOL_POST_CLOSE_COOLDOWN[pos["symbol"]] = time.time() + POST_CLOSE_COOLDOWN
            await self.tg.send(f"{'🟢' if net >= 0 else '🔴'} بسته شد ({reason}) | سود/زیان: ${net:.2f}")
        except Exception as e:
            log.error(f"force_close: {e}")

    async def watchdog_loop(self):
        while True:
            with STATE_LOCK:
                items = list(SHARED_STATE["active_positions"].items())
            now = time.time()
            for pid, pos in items:
                price = self.prices.get(pos["symbol"])
                if not price:
                    continue
                pnl_pct = (price - pos["entry"]) / pos["entry"] * 100 if pos["side"] == "buy" else (pos["entry"] - price) / pos["entry"] * 100
                
                sl_hit = (pos["side"] == "buy" and price <= pos["sl"]) or (pos["side"] == "sell" and price >= pos["sl"])
                tp_hit = (pos["side"] == "buy" and price >= pos["tp"]) or (pos["side"] == "sell" and price <= pos["tp"])
                if sl_hit or tp_hit:
                    await self.force_close(pid, "SL" if sl_hit else "TP")
            await asyncio.sleep(1.5)

app = Flask(__name__)
@app.route("/")
def index():
    return "<h3>Master Quant Engine v16.0 Active</h3>"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    Thread(target=lambda: app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False), daemon=True).start()
    engine = QuantEngine()
    asyncio.run(engine.start())
