#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v13.2 - Final Stable Version
- Robust OHLCV (multi-symbol try + ccxt fallback)
- No-password Web Dashboard
- Fixed Risk Math
- Real Test Trade
- Modular Structure
"""

import asyncio
import logging
import os
import time
import uuid
from collections import Counter, defaultdict
from threading import Thread, Lock
from typing import Dict, List, Any

import aiohttp
import aiosqlite
import ccxt.async_support as ccxt
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string

# ============================================================================
# 1. CONFIGURATION
# ============================================================================
load_dotenv()

API_KEY          = os.getenv("PHEMEX_API_KEY", "")
API_SECRET       = os.getenv("PHEMEX_API_SECRET", "")
TESTNET          = os.getenv("PHEMEX_TESTNET", "False").lower() in ("true", "1", "yes")
TG_TOKEN         = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT          = os.getenv("TELEGRAM_CHAT_ID", "")

SYMBOLS = [
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "BNB/USDT:USDT",
    "XRP/USDT:USDT",
    "ADA/USDT:USDT",
    "DOT/USDT:USDT",
]

TIMEFRAME        = "5m"
HTF_TIMEFRAME    = "1h"
RISK_PCT         = 0.5
LEVERAGE         = 5
MAX_POS          = 3
MAX_DD           = 8.0
MAX_DAILY_LOSS   = 4.0
MIN_ORDER_USD    = 16.0
MAX_EXPOSURE_PCT = 30.0
TAKER_FEE        = 0.0006
FEE_BUFFER       = 1.2
TRAIL_ACT        = 1.8
TRAIL_STEP       = 0.6
PARTIAL_TP       = True
RELAXED_MODE     = True
TEST_SYMBOL      = "ADA/USDT:USDT"
TEST_USD         = 12.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.FileHandler("quant_v13.log"), logging.StreamHandler()]
)
log = logging.getLogger("QuantV13.2")

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
}
STATE_LOCK = Lock()

# ============================================================================
# 2. DATABASE
# ============================================================================
class Database:
    def __init__(self, path="bot_v13.db"):
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
                    price REAL, rsi REAL, atr REAL, htf_trend TEXT
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
                INSERT INTO trades (id,symbol,side,strategy,entry_price,qty,original_qty,sl,tp1,tp)
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

    async def log_decision(self, symbol, action, strategy, reason, price=0, rsi=0, atr=0, htf=""):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO decisions (symbol,action,strategy,reason,price,rsi,atr,htf_trend)
                VALUES (?,?,?,?,?,?,?,?)""",
                (symbol, action, strategy, reason, price, rsi, atr, htf))
            await db.commit()

    async def log_equity(self, balance, peak, dd):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("INSERT INTO equity (balance,peak,dd) VALUES (?,?,?)", (balance, peak, dd))
            await db.commit()

    async def get_open_trades(self) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM trades WHERE status='open'") as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_closed_trades(self, limit=100) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_recent_decisions(self, limit=200) -> List[dict]:
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
                        "total_pnl": round(sum(pnls), 2)
                    }

# ============================================================================
# 3. INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(series: pd.Series, n=14) -> pd.Series:
        delta = series.diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        ma_up = up.ewm(com=n-1, adjust=False).mean()
        ma_down = down.ewm(com=n-1, adjust=False).mean()
        rs = ma_up / ma_down.replace(0, 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def atr(df: pd.DataFrame, n=14) -> pd.Series:
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs()
        ], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

    @staticmethod
    def supertrend(df: pd.DataFrame, period=10, mult=3.0):
        atr = Indicators.atr(df, period)
        hl2 = (df["high"] + df["low"]) / 2
        upper = hl2 + mult * atr
        lower = hl2 - mult * atr
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

# ============================================================================
# 4. STRATEGY ENGINE
# ============================================================================
class StrategyEngine:
    def analyze(self, df_5m: pd.DataFrame, df_1h: pd.DataFrame) -> dict:
        df = df_5m.iloc[:-1].copy()
        htf = df_1h.iloc[:-1].copy()
        if len(df) < 60 or len(htf) < 40:
            return {"action": "neutral", "reason": "داده ناکافی", "strat": "", "rsi": 0, "atr": 0, "htf": ""}

        hclose = htf["close"]
        e50 = hclose.ewm(span=50, adjust=False).mean().iloc[-1]
        e200 = hclose.ewm(span=min(200, len(htf)), adjust=False).mean().iloc[-1]
        hp = float(hclose.iloc[-1])
        if hp > e50 and e50 > e200 * 0.998:
            htf_trend = "bullish"
        elif hp < e50 and e50 < e200 * 1.002:
            htf_trend = "bearish"
        else:
            return {"action": "neutral", "reason": "روند HTF نامشخص", "strat": "", "rsi": 0, "atr": 0, "htf": "sideways"}

        c = df["close"]
        high = df["high"]
        low = df["low"]
        vol = df["volume"]
        price = float(c.iloc[-1])
        atr_s = Indicators.atr(df, 14)
        atr = float(atr_s.iloc[-1])
        if atr <= 0:
            return {"action": "neutral", "reason": "ATR صفر", "strat": "", "rsi": 0, "atr": 0, "htf": htf_trend}

        atr_sma = float(Indicators.sma(atr_s, 20).iloc[-1])
        low_m = 0.45 if RELAXED_MODE else 0.55
        high_m = 3.2 if RELAXED_MODE else 2.8
        if atr < atr_sma * low_m or atr > atr_sma * high_m:
            return {"action": "neutral", "reason": f"نوسان نامناسب ATR={atr:.5f}", "strat": "", "rsi": 0, "atr": atr, "htf": htf_trend}

        rsi_s = Indicators.rsi(c)
        rsi = float(rsi_s.iloc[-1])
        rsi_p = float(rsi_s.iloc[-2])
        ema20 = float(c.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = float(c.ewm(span=50, adjust=False).mean().iloc[-1])
        st_d, st_u, st_l = Indicators.supertrend(df)
        vsma = float(Indicators.sma(vol, 20).iloc[-1])
        vcur = float(vol.iloc[-1])
        h10 = float(Indicators.highest(high, 10).iloc[-1])
        l10 = float(Indicators.lowest(low, 10).iloc[-1])

        if htf_trend == "bullish" and price > ema20 and price >= h10 * 0.999 and 48 < rsi < 75 and vcur > vsma * (1.15 if RELAXED_MODE else 1.3):
            return self._build("buy", "Breakout_Momentum", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and price < ema20 and price <= l10 * 1.001 and 25 < rsi < 52 and vcur > vsma * (1.15 if RELAXED_MODE else 1.3):
            return self._build("sell", "Breakout_Momentum", price, atr, rsi, htf_trend)

        if htf_trend == "bullish" and price > ema20 > ema50 * 0.999 and rsi_p <= (42 if RELAXED_MODE else 40) and rsi > rsi_p and rsi < 62:
            return self._build("buy", "MTF_Pullback", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and price < ema20 < ema50 * 1.001 and rsi_p >= (58 if RELAXED_MODE else 60) and rsi < rsi_p and rsi > 38:
            return self._build("sell", "MTF_Pullback", price, atr, rsi, htf_trend)

        if htf_trend == "bullish" and st_d.iloc[-1] == 1 and low.iloc[-1] <= st_l.iloc[-1] * 1.005 and c.iloc[-1] > c.iloc[-2] and 38 < rsi < 65:
            return self._build("buy", "SuperTrend_Pullback", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and st_d.iloc[-1] == -1 and high.iloc[-1] >= st_u.iloc[-1] * 0.995 and c.iloc[-1] < c.iloc[-2] and 35 < rsi < 62:
            return self._build("sell", "SuperTrend_Pullback", price, atr, rsi, htf_trend)

        if htf_trend == "bullish" and price > ema20 and vcur > vsma * (1.5 if RELAXED_MODE else 1.8) and c.iloc[-1] > c.iloc[-2] and 48 < rsi < 70:
            return self._build("buy", "Volume_Surge", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and price < ema20 and vcur > vsma * (1.5 if RELAXED_MODE else 1.8) and c.iloc[-1] < c.iloc[-2] and 30 < rsi < 52:
            return self._build("sell", "Volume_Surge", price, atr, rsi, htf_trend)

        return {"action": "neutral", "reason": f"بدون سیگنال (RSI={rsi:.1f})", "strat": "", "rsi": rsi, "atr": atr, "htf": htf_trend}

    def _build(self, side, strat, price, atr, rsi, htf):
        sl_m, tp_m, tp1_m = 1.5, 2.8, 1.4
        if strat == "Breakout_Momentum":
            sl_m, tp_m, tp1_m = 1.25, 3.2, 1.8
        elif strat == "Volume_Surge":
            sl_m, tp_m, tp1_m = 1.35, 2.4, 1.4
        if side == "buy":
            return {"action": side, "strat": strat, "sl": price - atr * sl_m, "tp": price + atr * tp_m,
                    "tp1": price + atr * tp1_m, "reason": f"سیگنال {strat}", "rsi": rsi, "atr": atr, "htf": htf}
        return {"action": side, "strat": strat, "sl": price + atr * sl_m, "tp": price - atr * tp_m,
                "tp1": price - atr * tp1_m, "reason": f"سیگنال {strat}", "rsi": rsi, "atr": atr, "htf": htf}

# ============================================================================
# 5. RISK MANAGER
# ============================================================================
class RiskManager:
    @staticmethod
    def calculate_qty(balance: float, price: float, sl: float, free_usdt: float, symbol: str, exchange) -> float:
        if price <= 0 or balance <= 0:
            return 0.0
        dist = abs(price - sl)
        if dist <= 0:
            return 0.0
        risk_usd = balance * (RISK_PCT / 100.0)
        qty = risk_usd / dist
        max_by_free = (free_usdt * 0.18 * LEVERAGE) / price
        max_by_exposure = (balance * MAX_EXPOSURE_PCT / 100.0) / price
        qty = min(qty, max_by_free, max_by_exposure)
        try:
            qty = float(exchange.amount_to_precision(symbol, qty))
            if qty * price < MIN_ORDER_USD:
                qty = float(exchange.amount_to_precision(symbol, MIN_ORDER_USD / price))
        except Exception:
            return 0.0
        return max(qty, 0.0)

# ============================================================================
# 6. ANALYTICS
# ============================================================================
class Analytics:
    def __init__(self, db: Database):
        self.db = db

    async def full_report(self) -> str:
        decisions = await self.db.get_recent_decisions(300)
        closed = await self.db.get_closed_trades(100)
        lines = ["🤖 <b>AI Observer v13.2 – گزارش کامل</b>\n"]

        if decisions:
            reasons = Counter()
            neu = sig = 0
            for d in decisions:
                if d["action"] == "neutral":
                    neu += 1
                    reasons[(d["reason"] or "?")[:65]] += 1
                else:
                    sig += 1
            lines.append(f"📊 تصمیم‌ها: کل {len(decisions)} | سیگنال {sig} | رد {neu}")
            lines.append("\n🚫 بیشترین دلایل رد:")
            for r, c in reasons.most_common(6):
                lines.append(f"  • {c}× {r}")
        else:
            lines.append("هنوز تصمیمی ثبت نشده.")

        if closed:
            pnls = [t["pnl"] for t in closed]
            wins = sum(1 for p in pnls if p > 0)
            early = sum(1 for t in closed if (t.get("hold_seconds") or 9999) < 180)
            by_strat = defaultdict(list)
            for t in closed:
                by_strat[t["strategy"]].append(t["pnl"])
            lines.append(f"\n📈 معاملات بسته: {len(closed)} | برد: {wins} ({wins/len(closed)*100:.0f}%)")
            if early:
                lines.append(f"  ⚠️ خروج زودهنگام (<۳دقیقه): {early}")
            lines.append("\n🎯 عملکرد استراتژی:")
            for s, vals in by_strat.items():
                wr = sum(1 for v in vals if v > 0) / len(vals) * 100
                lines.append(f"  • {s}: {len(vals)} معامله | WR={wr:.0f}% | Σ=${sum(vals):.2f}")
        else:
            lines.append("\nهنوز معامله بسته‌شده‌ای وجود ندارد.")

        lines.append("\n💡 داده در دیتابیس bot_v13.db ذخیره می‌شود.")
        return "\n".join(lines)

# ============================================================================
# 7. TELEGRAM
# ============================================================================
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
            [{"text": "🤖 Full Report", "callback_data": "cmd_report"}, {"text": "🚫 Rejections", "callback_data": "cmd_rej"}],
            [{"text": "⚡ REAL TEST TRADE (30s)", "callback_data": "cmd_realtest"}],
        ]}

    async def send(self, text: str, markup=None):
        if not TG_TOKEN:
            return
        if len(text) > 4000:
            text = text[:3900] + "\n...(truncated)"
        payload = {"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML"}
        if markup:
            payload["reply_markup"] = markup
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"{self.base}/sendMessage", json=payload, timeout=12)
        except Exception as e:
            log.error(f"TG send: {e}")

    async def poll(self):
        if not TG_TOKEN:
            return
        await self.send(f"🚀 <b>Master Quant v13.2 Online</b>\nRobust OHLCV + No-Password Dashboard", self.menu())
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
                                    await ss.post(f"{self.base}/answerCallbackQuery",
                                                  json={"callback_query_id": cb["id"], "text": "OK"}, timeout=4)
                            except Exception:
                                pass

                            if d == "cmd_start":
                                with STATE_LOCK:
                                    SHARED_STATE["is_active"] = True
                                await self.send("▶️ Bot Started", self.menu())
                            elif d == "cmd_pause":
                                with STATE_LOCK:
                                    SHARED_STATE["is_active"] = False
                                await self.send("⏸️ Bot Paused", self.menu())
                            elif d == "cmd_dash":
                                with STATE_LOCK:
                                    st = dict(SHARED_STATE)
                                await self.send(
                                    f"📊 <b>Dashboard v13.2</b>\n"
                                    f"Balance: <b>${st['balance']:.2f}</b>\n"
                                    f"DD: {st['current_dd']:.1f}% | Daily: ${st['daily_pnl']:.2f}\n"
                                    f"Pos: {len(st['active_positions'])}/{MAX_POS}\n"
                                    f"PnL: ${st['stats']['total_pnl']:.2f} | WR: {st['stats']['win_rate']}%\n"
                                    f"Last scan: {st['last_scan']}", self.menu())
                            elif d == "cmd_pos":
                                with STATE_LOCK:
                                    pos = dict(SHARED_STATE["active_positions"])
                                if not pos:
                                    await self.send("💤 هیچ پوزیشن فعالی نیست", self.menu())
                                else:
                                    for pid, p in pos.items():
                                        pr = self.engine.prices.get(p["symbol"], p["entry"])
                                        pnl = (pr - p["entry"]) * p["qty"] * (1 if p["side"] == "buy" else -1)
                                        await self.send(f"{'🟢' if pnl >= 0 else '🔴'} <b>{p['symbol']}</b> {p['side']}\nEntry: {p['entry']:.4f} | PnL: ${pnl:.2f}")
                            elif d == "cmd_sync":
                                await self.engine.smart_sync()
                                await self.send("🔄 Sync انجام شد", self.menu())
                            elif d == "cmd_report":
                                await self.send(await self.engine.analytics.full_report(), self.menu())
                            elif d == "cmd_rej":
                                decs = await self.engine.db.get_recent_decisions(12)
                                msg = "🚫 <b>آخرین تصمیم‌ها:</b>\n\n"
                                for x in decs:
                                    icon = "✅" if x["action"] != "neutral" else "⛔"
                                    msg += f"{icon} <b>{x['symbol']}</b>\n{x['reason'][:85]}\n\n"
                                await self.send(msg or "داده‌ای نیست", self.menu())
                            elif d == "cmd_realtest":
                                asyncio.create_task(self.engine.real_test_trade())
            except Exception as e:
                log.error(f"TG poll: {e}")
            await asyncio.sleep(1)

# ============================================================================
# 8. ENGINE
# ============================================================================
class QuantEngine:
    def __init__(self):
        self.db = Database()
        self.strategy = StrategyEngine()
        self.risk = RiskManager()
        self.analytics = Analytics(self.db)
        self.tg = TelegramController(self)
        self.ex = ccxt.phemex({
            "apiKey": API_KEY, "secret": API_SECRET,
            "enableRateLimit": True, "options": {"defaultType": "swap"}
        })
        self.ex.set_sandbox_mode(TESTNET)
        self.prices: Dict[str, float] = {}
        self.open_times: Dict[str, float] = {}
        self.base_url = "https://testnet-api.phemex.com" if TESTNET else "https://api.phemex.com"

    async def fetch_ohlcv_direct(self, symbol: str, timeframe: str, limit: int = 100) -> list:
        """نسخه نهایی قوی با چندین فرمت نماد + fallback"""
        res_map = {
            "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
            "1h": 3600, "4h": 14400, "1d": 86400
        }
        resolution = res_map.get(timeframe, 300)

        candidates = []
        try:
            market = self.ex.market(symbol)
            if market:
                if market.get("id"):
                    candidates.append(market["id"])
                if market.get("symbol"):
                    candidates.append(market["symbol"].replace("/", "").replace(":USDT", ""))
        except Exception:
            pass

        base = symbol.split("/")[0]
        candidates.extend([
            f"{base}USDT",
            f"{base}USD",
            f"u{base}USD",
            f"c{base}USD",
            symbol.replace("/", "").replace(":USDT", ""),
        ])
        candidates = list(dict.fromkeys([c for c in candidates if c]))

        async with aiohttp.ClientSession() as session:
            for sym_id in candidates:
                try:
                    url = f"{self.base_url}/exchange/public/md/v2/kline"
                    params = {"symbol": sym_id, "resolution": resolution, "limit": limit}
                    async with session.get(url, params=params, timeout=10) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json(content_type=None)

                    rows = []
                    if isinstance(data, dict):
                        data_section = data.get("data")
                        if isinstance(data_section, dict):
                            rows = data_section.get("rows") or []
                        elif isinstance(data_section, list):
                            rows = data_section
                        else:
                            rows = data.get("rows") or []

                    if not rows:
                        continue

                    ohlcv = []
                    for row in rows:
                        try:
                            if isinstance(row, (list, tuple)) and len(row) >= 7:
                                ts = int(row[0])
                                if ts < 1e12:
                                    ts *= 1000
                                ohlcv.append([
                                    ts,
                                    float(row[3]),
                                    float(row[4]),
                                    float(row[5]),
                                    float(row[6]),
                                    float(row[7]) if len(row) > 7 else 0.0
                                ])
                        except Exception:
                            continue

                    if len(ohlcv) >= 30:
                        ohlcv.sort(key=lambda x: x[0])
                        log.info(f"OHLCV OK {symbol} via {sym_id} → {len(ohlcv)} candles")
                        return ohlcv[-limit:]
                except Exception:
                    continue

        # Fallback به ccxt
        try:
            log.warning(f"Direct OHLCV failed for {symbol}, trying ccxt fallback...")
            candles = await self.ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if candles and len(candles) >= 30:
                log.info(f"OHLCV fallback OK {symbol} → {len(candles)} candles")
                return candles
        except Exception as e:
            log.warning(f"ccxt fallback failed {symbol}: {e}")

        log.warning(f"All OHLCV methods failed for {symbol}")
        return []

    async def start(self):
        await self.db.init()
        try:
            await self.ex.load_markets()
            log.info("Markets loaded")
            for sym in SYMBOLS:
                try:
                    await self.ex.set_leverage(LEVERAGE, sym)
                    log.info(f"Leverage {LEVERAGE}x → {sym}")
                except Exception as e:
                    log.warning(f"Leverage {sym}: {e}")
        except Exception as e:
            log.error(f"Init: {e}")

        for t in await self.db.get_open_trades():
            with STATE_LOCK:
                SHARED_STATE["active_positions"][t["id"]] = {
                    "id": t["id"], "symbol": t["symbol"], "side": t["side"], "strategy": t["strategy"],
                    "entry": t["entry_price"], "qty": t["qty"], "sl": t["sl"], "tp": t["tp"],
                    "tp1": t["tp1"], "is_partial": t.get("is_partial", 0), "highest_pnl_pct": t.get("highest_pnl_pct", 0)
                }
                self.open_times[t["id"]] = time.time()

        await self.smart_sync()
        try:
            bal = await self.ex.fetch_balance()
            usdt = float(bal.get("USDT", {}).get("total", 0) or 0)
            with STATE_LOCK:
                SHARED_STATE["balance"] = usdt
                SHARED_STATE["peak_balance"] = max(SHARED_STATE["peak_balance"], usdt)
                if SHARED_STATE["day_start_balance"] <= 0:
                    SHARED_STATE["day_start_balance"] = usdt
        except Exception:
            pass

        await asyncio.gather(
            self.price_loop(),
            self.scan_loop(),
            self.watchdog_loop(),
            self.tg.poll()
        )

    async def price_loop(self):
        while True:
            try:
                tickers = await self.ex.fetch_tickers(SYMBOLS)
                for s, d in tickers.items():
                    if d.get("last"):
                        self.prices[s] = float(d["last"])
                bal = await self.ex.fetch_balance()
                cur = float(bal.get("USDT", {}).get("total", 0) or 0)
                with STATE_LOCK:
                    SHARED_STATE["balance"] = cur
                    if cur > SHARED_STATE["peak_balance"]:
                        SHARED_STATE["peak_balance"] = cur
                    peak = SHARED_STATE["peak_balance"]
                    if peak > 0:
                        dd = (peak - cur) / peak * 100
                        SHARED_STATE["current_dd"] = dd
                        if dd >= MAX_DD:
                            SHARED_STATE["dd_halted"] = True
                        elif dd < MAX_DD * 0.7:
                            SHARED_STATE["dd_halted"] = False
                    day_start = SHARED_STATE["day_start_balance"]
                    if day_start > 0:
                        SHARED_STATE["daily_pnl"] = cur - day_start
                        if (cur - day_start) / day_start * 100 <= -MAX_DAILY_LOSS:
                            SHARED_STATE["daily_halted"] = True
                        else:
                            SHARED_STATE["daily_halted"] = False
                await self.db.log_equity(cur, peak, SHARED_STATE["current_dd"])
            except Exception as e:
                log.error(f"price_loop: {e}")
            await asyncio.sleep(3)

    async def scan_loop(self):
        while True:
            with STATE_LOCK:
                can = (SHARED_STATE["is_active"] and not SHARED_STATE["dd_halted"]
                       and not SHARED_STATE["daily_halted"] and len(SHARED_STATE["active_positions"]) < MAX_POS)
            if not can:
                await asyncio.sleep(8)
                continue
            with STATE_LOCK:
                SHARED_STATE["last_scan"] = time.strftime("%H:%M:%S")
            for sym in SYMBOLS:
                with STATE_LOCK:
                    if any(p["symbol"] == sym for p in SHARED_STATE["active_positions"].values()):
                        continue
                try:
                    raw5 = await self.fetch_ohlcv_direct(sym, TIMEFRAME, 120)
                    await asyncio.sleep(0.25)
                    raw1 = await self.fetch_ohlcv_direct(sym, HTF_TIMEFRAME, 80)
                    if not raw5 or len(raw5) < 50:
                        await self.db.log_decision(sym, "neutral", "", "OHLCV خالی")
                        continue
                    df5 = pd.DataFrame(raw5, columns=["ts", "open", "high", "low", "close", "volume"])
                    df1 = pd.DataFrame(raw1, columns=["ts", "open", "high", "low", "close", "volume"]) if raw1 and len(raw1) > 20 else df5
                    sig = self.strategy.analyze(df5, df1)
                    await self.db.log_decision(sym, sig["action"], sig.get("strat", ""), sig.get("reason", ""),
                                               self.prices.get(sym, 0), sig.get("rsi", 0), sig.get("atr", 0), sig.get("htf", ""))
                    if sig["action"] != "neutral":
                        await self.execute_trade(sym, sig)
                except Exception as e:
                    log.error(f"scan {sym}: {e}")
                await asyncio.sleep(0.4)
            await asyncio.sleep(18)

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
                await self.db.log_decision(sym, "rejected", sig.get("strat", ""), "حجم صفر")
                return
            order = await self.ex.create_market_order(sym, sig["action"], qty)
            fill = float(order.get("average") or price)
            pid = f"pos_{uuid.uuid4().hex[:8]}"
            pos = {
                "id": pid, "symbol": sym, "side": sig["action"], "strategy": sig["strat"],
                "entry": fill, "qty": qty, "sl": sig["sl"], "tp": sig["tp"], "tp1": sig["tp1"],
                "is_partial": 0, "highest_pnl_pct": 0.0
            }
            with STATE_LOCK:
                SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()
            await self.db.insert_trade(pos)
            await self.tg.send(f"🎯 <b>ورود {sig['action'].upper()}</b> ({sig['strat']})\n{sym} @ {fill:.4f}\nQty: {qty:.4f}")
            try:
                sl_side = "sell" if sig["action"] == "buy" else "buy"
                await self.ex.create_order(sym, "stop", sl_side, qty, None,
                                           params={"stopPrice": sig["sl"], "reduceOnly": True})
            except Exception as e:
                log.warning(f"Exchange SL failed: {e}")
        except Exception as e:
            log.error(f"execute: {e}")
            await self.db.log_decision(sym, "rejected", sig.get("strat", ""), str(e)[:90])

    async def real_test_trade(self):
        await self.tg.send("⚡ <b>شروع تست واقعی ۳۰ ثانیه‌ای روی ADA...</b>")
        try:
            bal = await self.ex.fetch_balance()
            free = float(bal.get("USDT", {}).get("free", 0) or 0)
            if free < 20:
                await self.tg.send("❌ موجودی آزاد کافی نیست")
                return
            price = self.prices.get(TEST_SYMBOL)
            if not price:
                await self.tg.send("❌ قیمت ADA در دسترس نیست")
                return
            qty = float(self.ex.amount_to_precision(TEST_SYMBOL, min(TEST_USD, free * 0.08) / price))
            if qty * price < 5:
                await self.tg.send("❌ حجم تست خیلی کوچک است")
                return
            order = await self.ex.create_market_order(TEST_SYMBOL, "buy", qty)
            fill = float(order.get("average") or price)
            pid = f"test_{uuid.uuid4().hex[:6]}"
            pos = {
                "id": pid, "symbol": TEST_SYMBOL, "side": "buy", "strategy": "RealTest",
                "entry": fill, "qty": qty, "sl": fill * 0.97, "tp": fill * 1.03, "tp1": fill * 1.015,
                "is_partial": 0, "highest_pnl_pct": 0.0
            }
            with STATE_LOCK:
                SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()
            await self.tg.send(f"🧪 پوزیشن تست باز شد\n{TEST_SYMBOL} @ {fill:.5f}\nQty: {qty:.2f}\n۳۰ ثانیه صبر...")
            await asyncio.sleep(30)
            await self.force_close(pid, "RealTest 30s completed")
            await self.tg.send("✅ <b>تست واقعی با موفقیت انجام و بسته شد</b>")
        except Exception as e:
            await self.tg.send(f"❌ خطا در تست واقعی: {e}")
            log.error(f"real_test: {e}")

    async def smart_sync(self):
        try:
            remote = await self.ex.fetch_positions()
            active = set()
            for p in remote:
                size = abs(float(p.get("contracts") or 0))
                if size <= 0:
                    continue
                raw = p.get("symbol", "")
                matched = next((s for s in SYMBOLS if s.split("/")[0] in raw), None)
                if matched:
                    active.add(matched)
            with STATE_LOCK:
                to_del = [pid for pid, p in SHARED_STATE["active_positions"].items()
                          if p["symbol"] not in active and p["strategy"] not in ("RealTest", "LiveTest")]
            for pid in to_del:
                await self.db.close_trade(pid, 0.0, reason="remote close")
                with STATE_LOCK:
                    SHARED_STATE["active_positions"].pop(pid, None)
        except Exception as e:
            log.error(f"sync: {e}")

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
            fees = abs(raw_pnl) * TAKER_FEE * 2 * FEE_BUFFER if raw_pnl != 0 else pos["qty"] * price * TAKER_FEE * 2
            net = raw_pnl - fees
            if pos["strategy"] not in ("RealTest", "LiveTest"):
                await self.db.close_trade(pid, net, fees, reason, hold)
            with STATE_LOCK:
                SHARED_STATE["active_positions"].pop(pid, None)
            self.open_times.pop(pid, None)
            await self.db.update_analytics()
            icon = "🟢" if net >= 0 else "🔴"
            await self.tg.send(f"{icon} <b>بسته شد</b> ({reason})\n{pos['symbol']} | Net ${net:.2f}")
        except Exception as e:
            log.error(f"force_close: {e}")

    async def watchdog_loop(self):
        while True:
            with STATE_LOCK:
                items = list(SHARED_STATE["active_positions"].items())
            for pid, pos in items:
                if pos["strategy"] in ("RealTest", "LiveTest"):
                    continue
                price = self.prices.get(pos["symbol"])
                if not price:
                    continue
                pnl_pct = ((price - pos["entry"]) / pos["entry"] * 100) if pos["side"] == "buy" else ((pos["entry"] - price) / pos["entry"] * 100)
                if pnl_pct > TRAIL_ACT and pnl_pct > pos["highest_pnl_pct"]:
                    pos["highest_pnl_pct"] = pnl_pct
                    new_sl = price * (1 - TRAIL_STEP / 100) if pos["side"] == "buy" else price * (1 + TRAIL_STEP / 100)
                    if (pos["side"] == "buy" and new_sl > pos["sl"]) or (pos["side"] == "sell" and new_sl < pos["sl"]):
                        pos["sl"] = new_sl
                        await self.db.update_trade(pid, pos["qty"], pos["sl"], pos["is_partial"], pos["highest_pnl_pct"])
                if PARTIAL_TP and pos["is_partial"] == 0:
                    hit = (pos["side"] == "buy" and price >= pos["tp1"]) or (pos["side"] == "sell" and price <= pos["tp1"])
                    if hit:
                        try:
                            half = float(self.ex.amount_to_precision(pos["symbol"], pos["qty"] / 2))
                            if half > 0:
                                close_side = "sell" if pos["side"] == "buy" else "buy"
                                await self.ex.create_market_order(pos["symbol"], close_side, half, params={"reduceOnly": True})
                                pos["qty"] -= half
                                pos["is_partial"] = 1
                                pos["sl"] = pos["entry"]
                                await self.db.update_trade(pid, pos["qty"], pos["sl"], 1, pos["highest_pnl_pct"])
                                await self.tg.send(f"🔹 Partial TP → Break-even  {pos['symbol']}")
                        except Exception:
                            pass
                sl_hit = (pos["side"] == "buy" and price <= pos["sl"]) or (pos["side"] == "sell" and price >= pos["sl"])
                tp_hit = (pos["side"] == "buy" and price >= pos["tp"]) or (pos["side"] == "sell" and price <= pos["tp"])
                if sl_hit or tp_hit:
                    await self.force_close(pid, "SL/Trail" if sl_hit else "TP")
            await asyncio.sleep(1.5)

# ============================================================================
# 9. WEB DASHBOARD (بدون پسورد)
# ============================================================================
app = Flask(__name__)

@app.route("/api/status")
def api_status():
    with STATE_LOCK:
        return jsonify(dict(SHARED_STATE))

@app.route("/")
def dashboard():
    html = """
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Master Quant v13.2</title>
<style>
  :root { --bg:#0d1117; --card:#161b22; --border:#30363d; --text:#c9d1d9; --accent:#58a6ff; --green:#3fb950; }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background:var(--bg); color:var(--text); padding:20px; }
  h1 { color:var(--accent); margin-bottom:8px; font-size:1.6rem; }
  .subtitle { color:#8b949e; margin-bottom:24px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(160px,1fr)); gap:14px; margin-bottom:24px; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:16px; }
  .card h3 { font-size:0.8rem; color:#8b949e; margin-bottom:6px; }
  .value { font-size:1.45rem; font-weight:700; color:var(--accent); }
  .badge { background:var(--green); color:#fff; padding:2px 8px; border-radius:20px; font-size:0.75rem; }
  .footer { margin-top:30px; font-size:0.8rem; color:#8b949e; text-align:center; }
</style>
</head>
<body>
  <h1>🚀 Master Quant Engine v13.2</h1>
  <p class="subtitle">Final Stable • Robust OHLCV • No Password</p>

  <div class="grid">
    <div class="card"><h3>وضعیت</h3><div class="value"><span class="badge">ONLINE</span></div></div>
    <div class="card"><h3>موجودی</h3><div class="value" id="balance">0.00</div></div>
    <div class="card"><h3>پوزیشن‌ها</h3><div class="value" id="pos">0</div></div>
    <div class="card"><h3>Total PnL</h3><div class="value" id="pnl">0.00</div></div>
    <div class="card"><h3>Win Rate</h3><div class="value" id="wr">0%</div></div>
    <div class="card"><h3>Drawdown</h3><div class="value" id="dd">0.0%</div></div>
  </div>

  <div class="card">
    <h3>آخرین اسکن</h3>
    <p id="lastscan">–</p>
  </div>

  <div class="footer">Master Quant v13.2 – Dashboard بدون پسورد</div>

<script>
async function refresh() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    document.getElementById('balance').textContent = (d.balance || 0).toFixed(2);
    document.getElementById('pos').textContent = Object.keys(d.active_positions || {}).length;
    document.getElementById('pnl').textContent = (d.stats?.total_pnl || 0).toFixed(2);
    document.getElementById('wr').textContent = (d.stats?.win_rate || 0) + '%';
    document.getElementById('dd').textContent = (d.current_dd || 0).toFixed(1) + '%';
    document.getElementById('lastscan').textContent = d.last_scan || '–';
  } catch(e) {}
}
refresh();
setInterval(refresh, 4000);
</script>
</body>
</html>
"""
    return render_template_string(html)

def run_web():
    app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False)

# ============================================================================
# 10. MAIN
# ============================================================================
if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    engine = QuantEngine()
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        log.info("Shutdown")
    except Exception as e:
        log.error(f"Fatal: {e}")
        raise