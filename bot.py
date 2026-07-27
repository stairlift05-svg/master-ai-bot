#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v14.2 (Hybrid Tuned)
- سیگنال بیشتر (فیلتر شل‌تر)
- DOT با آستانه اختلاف قیمت 4.5%
- SL/Trail بازتر + حداقل زمان نگهداری ۱۰ دقیقه
- کاهش فشار روی Binance
- گزارش تشخیصی کامل + Recovery + Cooldown
"""

import asyncio
import logging
import os
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime
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
    "BNB/USDT:USDT",
    "XRP/USDT:USDT",
    "ADA/USDT:USDT",
    "DOT/USDT:USDT",
]

BINANCE_SYMBOL_MAP = {
    "ETH/USDT:USDT": "ETH/USDT",
    "BNB/USDT:USDT": "BNB/USDT",
    "XRP/USDT:USDT": "XRP/USDT",
    "ADA/USDT:USDT": "ADA/USDT",
    "DOT/USDT:USDT": "DOT/USDT",
}

SYMBOL_CONFIG = {
    "ETH/USDT:USDT": {"max_price_diff": 1.1},
    "BNB/USDT:USDT": {"max_price_diff": 1.3},
    "XRP/USDT:USDT": {"max_price_diff": 1.7},
    "ADA/USDT:USDT": {"max_price_diff": 1.7},
    "DOT/USDT:USDT": {"max_price_diff": 4.5},   # افزایش برای کاهش رد شدن
}

STRATEGY_PARAMS = {
    # SL کمی بازتر، TP منطقی برای RR بهتر
    "Breakout_Momentum":   {"sl_m": 1.25, "tp_m": 3.6, "tp1_m": 1.9},
    "MTF_Pullback":        {"sl_m": 1.40, "tp_m": 3.0, "tp1_m": 1.5},
    "SuperTrend_Pullback": {"sl_m": 1.30, "tp_m": 2.8, "tp1_m": 1.4},
    "Volume_Surge":        {"sl_m": 1.25, "tp_m": 2.5, "tp1_m": 1.3},
}

TIMEFRAME              = "5m"
HTF_TIMEFRAME          = "1h"
RISK_PCT               = 0.45          # کمی محافظه‌کارتر
LEVERAGE               = 5
MAX_POS                = 10
MAX_DD                 = 8.0
MAX_DAILY_LOSS         = 4.0
MIN_ORDER_USD          = 16.0
MAX_EXPOSURE_PCT       = 35.0
TAKER_FEE              = 0.0006
FEE_BUFFER             = 1.2
TRAIL_ACT              = 2.4           # دیرتر فعال می‌شود
TRAIL_STEP             = 0.7
PARTIAL_TP             = True
MIN_HOLD_SECONDS       = 600           # ۱۰ دقیقه قبل از اجازه SL/Trail
TEST_SYMBOL            = "ADA/USDT:USDT"
TEST_USD               = 12.0
CONSECUTIVE_LOSS_LIMIT = 2
SYMBOL_COOLDOWN_HOURS  = 4
SCAN_INTERVAL          = 55
SYMBOL_DELAY           = 2.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.FileHandler("quant_v14.log"), logging.StreamHandler()]
)
log = logging.getLogger("QuantV14.2")

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

# ============================================================================
# 2. DATABASE
# ============================================================================
class Database:
    def __init__(self, path="bot_v14.db"):
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
            await db.execute("INSERT INTO equity (balance,peak,dd) VALUES (?,?,?)", (balance, peak, dd))
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
                "SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?", (limit,)) as c:
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
                        "total_pnl": round(sum(pnls), 2)
                    }

    async def generate_txt_report(self, prices: Dict[str, float] = None) -> str:
        prices = prices or {}
        decisions = await self.get_recent_decisions(200)
        closed = await self.get_closed_trades(30)
        open_trades = await self.get_open_trades()

        lines = []
        lines.append("=" * 70)
        lines.append("       MASTER QUANT ENGINE v14.2 – FULL DIAGNOSTIC REPORT")
        lines.append(f"       Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        lines.append("=" * 70)
        lines.append("")

        with STATE_LOCK:
            st = dict(SHARED_STATE)
        lines.append("┌─ 1. DASHBOARD ─────────────────────────────────────────────────────")
        lines.append(f"│  Balance        : ${st.get('balance', 0):.2f}")
        lines.append(f"│  Peak Balance   : ${st.get('peak_balance', 0):.2f}")
        lines.append(f"│  Current DD     : {st.get('current_dd', 0):.2f}%")
        lines.append(f"│  Daily PnL      : ${st.get('daily_pnl', 0):.2f}")
        lines.append(f"│  Open Positions : {len(st.get('active_positions', {}))} / {MAX_POS}")
        lines.append(f"│  Total Trades   : {st.get('stats', {}).get('total_trades', 0)}")
        lines.append(f"│  Win Rate       : {st.get('stats', {}).get('win_rate', 0)}%")
        lines.append(f"│  Total PnL      : ${st.get('stats', {}).get('total_pnl', 0):.2f}")
        lines.append(f"│  Last Scan      : {st.get('last_scan', 'Never')}")
        lines.append(f"│  Bot Active     : {st.get('is_active')}")
        lines.append("└────────────────────────────────────────────────────────────────────")
        lines.append("")

        lines.append("┌─ 2. OPEN POSITIONS ────────────────────────────────────────────────")
        active = st.get("active_positions", {})
        if not active:
            lines.append("│  (هیچ پوزیشن بازی وجود ندارد)")
        else:
            for pid, p in active.items():
                pr = prices.get(p["symbol"], p["entry"])
                pnl = (pr - p["entry"]) * p["qty"] * (1 if p["side"] == "buy" else -1)
                hold = time.time() - (p.get("_open_ts") or time.time())
                lines.append(f"│  {p['symbol']:<18} {p['side'].upper():<5} Entry:{p['entry']:.5f}  "
                             f"Qty:{p['qty']:.4f}  PnL:${pnl:+.3f}  Strat:{p.get('strategy','')}")
                lines.append(f"│     SL:{p['sl']:.5f}  TP:{p['tp']:.5f}  Partial:{p.get('is_partial',0)}")
        lines.append("└────────────────────────────────────────────────────────────────────")
        lines.append("")

        lines.append("┌─ 3. CLOSED TRADES (Last 30) ───────────────────────────────────────")
        if not closed:
            lines.append("│  (هنوز معامله بسته‌شده‌ای ثبت نشده)")
        else:
            for t in closed:
                emoji = "WIN " if t["pnl"] > 0 else "LOSS"
                hold_m = (t.get("hold_seconds") or 0) / 60
                lines.append(f"│  [{emoji}] {t['symbol']:<16} {t['side']:<4} PnL:${t['pnl']:+.3f}  "
                             f"Hold:{hold_m:.1f}m  Reason:{t.get('exit_reason','')}")
                lines.append(f"│         Entry:{t['entry_price']:.5f}  Strat:{t.get('strategy','')}")
        lines.append("└────────────────────────────────────────────────────────────────────")
        lines.append("")

        lines.append("┌─ 4. DECISION BREAKDOWN ────────────────────────────────────────────")
        if decisions:
            reasons = Counter()
            by_symbol = defaultdict(lambda: {"sig": 0, "rej": 0})
            signals = 0
            for d in decisions:
                if d["action"] == "neutral":
                    reasons[(d["reason"] or "Unknown")[:60]] += 1
                    by_symbol[d["symbol"]]["rej"] += 1
                else:
                    signals += 1
                    by_symbol[d["symbol"]]["sig"] += 1
            lines.append(f"│  Total Decisions : {len(decisions)}")
            lines.append(f"│  Signals         : {signals}")
            lines.append(f"│  Rejected        : {len(decisions) - signals}")
            lines.append("│")
            lines.append("│  Top Rejection Reasons:")
            for reason, count in reasons.most_common(10):
                lines.append(f"│    {count:3d} × {reason}")
            lines.append("│")
            lines.append("│  Per Symbol:")
            for sym, v in sorted(by_symbol.items()):
                lines.append(f"│    {sym:<18} Signal:{v['sig']:3d}  Reject:{v['rej']:3d}")
        else:
            lines.append("│  (هنوز تصمیمی ثبت نشده)")
        lines.append("└────────────────────────────────────────────────────────────────────")
        lines.append("")

        lines.append("┌─ 5. LAST 15 DECISIONS (Detailed) ──────────────────────────────────")
        for d in (decisions or [])[:15]:
            icon = "SIG" if d["action"] != "neutral" else "REJ"
            lines.append(f"│  [{icon}] {d.get('ts','')[:19]}  {d['symbol']}")
            lines.append(f"│       Action:{d['action']:<8} Strat:{d.get('strategy') or '-':<20} RSI:{d.get('rsi') or 0:.1f}")
            lines.append(f"│       Reason: {d.get('reason','')[:65]}")
            if d.get("extra"):
                lines.append(f"│       Extra : {str(d['extra'])[:65]}")
        lines.append("└────────────────────────────────────────────────────────────────────")
        lines.append("")

        lines.append("┌─ 6. ERROR & COOLDOWN STATUS ───────────────────────────────────────")
        if SYMBOL_ERROR_COOLDOWN:
            now = time.time()
            for sym, until in list(SYMBOL_ERROR_COOLDOWN.items()):
                remain = max(0, int(until - now))
                cnt = SYMBOL_ERROR_COUNT.get(sym, 0)
                lines.append(f"│  {sym:<18} errors:{cnt}  cooldown remaining: {remain}s")
        else:
            lines.append("│  (هیچ نمادی در cooldown نیست)")
        with STATE_LOCK:
            cl = SHARED_STATE.get("consecutive_losses", {})
        if cl:
            lines.append("│  Consecutive Losses:")
            for sym, info in cl.items():
                lines.append(f"│    {sym}: {info.get('count',0)} losses")
        lines.append("└────────────────────────────────────────────────────────────────────")
        lines.append("")

        lines.append("┌─ 7. AUTO RECOMMENDATIONS ──────────────────────────────────────────")
        recs = []
        if decisions:
            top_reason = reasons.most_common(1)[0][0] if reasons else ""
            if "کندل" in top_reason or "Binance" in top_reason:
                recs.append("مشکل دریافت کندل Binance → صبر یا افزایش SCAN_INTERVAL.")
            if "اختلاف قیمت" in top_reason:
                recs.append("اختلاف قیمت هنوز زیاد است → آستانه نماد مربوطه را چک کنید.")
            if "روند HTF" in top_reason:
                recs.append("بازار احتمالاً رنج است؛ سیگنال کمتر طبیعی است.")
        wr = st.get("stats", {}).get("win_rate", 0)
        ntr = st.get("stats", {}).get("total_trades", 0)
        if ntr >= 8 and wr < 35:
            recs.append(f"Win Rate پایین ({wr}%) با {ntr} معامله → پارامترها را بازبینی کنید.")
        if not recs:
            recs.append("وضعیت در محدوده قابل قبول. به جمع‌آوری داده ادامه دهید.")
        for r in recs:
            lines.append(f"│  • {r}")
        lines.append("└────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("=" * 70)
        lines.append("End of Diagnostic Report – v14.2")
        lines.append("=" * 70)
        return "\n".join(lines)

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
# 4. STRATEGY (More signals)
# ============================================================================
class StrategyEngine:
    def analyze(self, df_5m: pd.DataFrame, df_1h: pd.DataFrame) -> dict:
        df = df_5m.iloc[:-1].copy()
        htf = df_1h.iloc[:-1].copy()
        if len(df) < 55 or len(htf) < 35:
            return {"action": "neutral", "reason": "داده ناکافی", "strat": "", "rsi": 0, "atr": 0, "htf": ""}

        hclose = htf["close"]
        e50 = hclose.ewm(span=50, adjust=False).mean().iloc[-1]
        e200 = hclose.ewm(span=min(200, len(htf)), adjust=False).mean().iloc[-1]
        hp = float(hclose.iloc[-1])

        # فیلتر روند نرم‌تر
        if hp > e50 * 0.995 and e50 >= e200 * 0.992:
            htf_trend = "bullish"
        elif hp < e50 * 1.005 and e50 <= e200 * 1.008:
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
        if atr < atr_sma * 0.35 or atr > atr_sma * 3.8:
            return {"action": "neutral", "reason": "نوسان نامناسب", "strat": "", "rsi": 0, "atr": atr, "htf": htf_trend}

        rsi_s = Indicators.rsi(c)
        rsi = float(rsi_s.iloc[-1])
        rsi_p = float(rsi_s.iloc[-2])
        ema20 = float(c.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = float(c.ewm(span=50, adjust=False).mean().iloc[-1])
        st_d, st_u, st_l = Indicators.supertrend(df)
        vsma = float(Indicators.sma(vol, 20).iloc[-1]) or 1e-9
        vcur = float(vol.iloc[-1])
        h10 = float(Indicators.highest(high, 10).iloc[-1])
        l10 = float(Indicators.lowest(low, 10).iloc[-1])
        vol_ok = vcur > vsma * 1.05

        # Breakout
        if htf_trend == "bullish" and price > ema20 and price >= h10 * 0.997 and 44 < rsi < 80 and vol_ok:
            return self._build("buy", "Breakout_Momentum", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and price < ema20 and price <= l10 * 1.003 and 20 < rsi < 56 and vol_ok:
            return self._build("sell", "Breakout_Momentum", price, atr, rsi, htf_trend)

        # MTF Pullback
        if htf_trend == "bullish" and price > ema20 * 0.997 and ema20 >= ema50 * 0.995 and rsi_p <= 48 and rsi > rsi_p and rsi < 68:
            return self._build("buy", "MTF_Pullback", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and price < ema20 * 1.003 and ema20 <= ema50 * 1.005 and rsi_p >= 52 and rsi < rsi_p and rsi > 32:
            return self._build("sell", "MTF_Pullback", price, atr, rsi, htf_trend)

        # SuperTrend
        if htf_trend == "bullish" and st_d.iloc[-1] == 1 and low.iloc[-1] <= st_l.iloc[-1] * 1.01 and c.iloc[-1] > c.iloc[-2] and 34 < rsi < 70:
            return self._build("buy", "SuperTrend_Pullback", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and st_d.iloc[-1] == -1 and high.iloc[-1] >= st_u.iloc[-1] * 0.99 and c.iloc[-1] < c.iloc[-2] and 30 < rsi < 66:
            return self._build("sell", "SuperTrend_Pullback", price, atr, rsi, htf_trend)

        # Volume Surge
        if htf_trend == "bullish" and price > ema20 and vcur > vsma * 1.25 and c.iloc[-1] > c.iloc[-2] and 42 < rsi < 74:
            return self._build("buy", "Volume_Surge", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and price < ema20 and vcur > vsma * 1.25 and c.iloc[-1] < c.iloc[-2] and 26 < rsi < 58:
            return self._build("sell", "Volume_Surge", price, atr, rsi, htf_trend)

        return {"action": "neutral", "reason": f"بدون سیگنال (RSI={rsi:.1f})", "strat": "", "rsi": rsi, "atr": atr, "htf": htf_trend}

    def _build(self, side, strat, price, atr, rsi, htf):
        params = STRATEGY_PARAMS.get(strat, {"sl_m": 1.3, "tp_m": 2.6, "tp1_m": 1.3})
        sl_m, tp_m, tp1_m = params["sl_m"], params["tp_m"], params["tp1_m"]
        if side == "buy":
            return {
                "action": side, "strat": strat,
                "sl": price - atr * sl_m, "tp": price + atr * tp_m, "tp1": price + atr * tp1_m,
                "reason": f"سیگنال {strat}", "rsi": rsi, "atr": atr, "htf": htf,
                "expected_rr": round(tp_m / sl_m, 2)
            }
        return {
            "action": side, "strat": strat,
            "sl": price + atr * sl_m, "tp": price - atr * tp_m, "tp1": price - atr * tp1_m,
            "reason": f"سیگنال {strat}", "rsi": rsi, "atr": atr, "htf": htf,
            "expected_rr": round(tp_m / sl_m, 2)
        }

# ============================================================================
# 5. RISK
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
# 6. TELEGRAM
# ============================================================================
class Analytics:
    def __init__(self, db: Database):
        self.db = db

    async def full_report(self, prices=None) -> str:
        return await self.db.generate_txt_report(prices)


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
            [{"text": "🤖 Report", "callback_data": "cmd_report"}, {"text": "🚫 Rejections", "callback_data": "cmd_rej"}],
            [{"text": "⚡ REAL TEST", "callback_data": "cmd_realtest"}],
            [{"text": "📄 Download TXT Report", "callback_data": "cmd_txt"}],
        ]}

    async def send(self, text: str, markup=None):
        if not TG_TOKEN: return
        if len(text) > 4000: text = text[:3900] + "\n..."
        payload = {"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML"}
        if markup: payload["reply_markup"] = markup
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"{self.base}/sendMessage", json=payload, timeout=12)
        except Exception as e:
            log.error(f"TG: {e}")

    async def send_document(self, path: str, caption=""):
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
            await self.send(f"❌ خطا در ارسال فایل: {e}")

    async def poll(self):
        if not TG_TOKEN: return
        await self.send(
            "🚀 <b>Master Quant v14.2 Online</b>\n"
            "More signals · Wider SL · Min-hold 10m · DOT 4.5%",
            self.menu()
        )
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"{self.base}/getUpdates?offset={self.offset+1}&timeout=8") as r:
                        data = await r.json()
                        for u in data.get("result", []):
                            self.offset = u["update_id"]
                            if "callback_query" not in u: continue
                            cb = u["callback_query"]
                            d = cb["data"]
                            try:
                                async with aiohttp.ClientSession() as ss:
                                    await ss.post(f"{self.base}/answerCallbackQuery",
                                                  json={"callback_query_id": cb["id"], "text": "OK"}, timeout=4)
                            except: pass

                            if d == "cmd_start":
                                with STATE_LOCK: SHARED_STATE["is_active"] = True
                                await self.send("▶️ Started", self.menu())
                            elif d == "cmd_pause":
                                with STATE_LOCK: SHARED_STATE["is_active"] = False
                                await self.send("⏸️ Paused", self.menu())
                            elif d == "cmd_dash":
                                with STATE_LOCK: st = dict(SHARED_STATE)
                                await self.send(
                                    f"📊 <b>Dashboard v14.2</b>\nBalance: <b>${st['balance']:.2f}</b>\n"
                                    f"DD: {st['current_dd']:.1f}% | Pos: {len(st['active_positions'])}/{MAX_POS}\n"
                                    f"PnL: ${st['stats']['total_pnl']:.2f} | WR: {st['stats']['win_rate']}%\n"
                                    f"Last: {st['last_scan']}", self.menu())
                            elif d == "cmd_pos":
                                with STATE_LOCK: pos = dict(SHARED_STATE["active_positions"])
                                if not pos:
                                    await self.send("💤 هیچ پوزیشنی نیست", self.menu())
                                else:
                                    for p in pos.values():
                                        pr = self.engine.prices.get(p["symbol"], p["entry"])
                                        pnl = (pr - p["entry"]) * p["qty"] * (1 if p["side"] == "buy" else -1)
                                        await self.send(f"{'🟢' if pnl >= 0 else '🔴'} {p['symbol']} | {p['side'].upper()} | ${pnl:.2f}")
                            elif d == "cmd_sync":
                                await self.engine.smart_sync()
                                await self.send("🔄 Sync + Recovery انجام شد", self.menu())
                            elif d in ("cmd_report", "cmd_txt"):
                                report = await self.engine.analytics.full_report(self.engine.prices)
                                with open("quant_report.txt", "w", encoding="utf-8") as f:
                                    f.write(report)
                                await self.send_document("quant_report.txt", "📄 گزارش تشخیصی v14.2")
                            elif d == "cmd_rej":
                                decs = await self.engine.db.get_recent_decisions(12)
                                msg = "🚫 <b>آخرین تصمیم‌ها:</b>\n\n"
                                for x in decs:
                                    icon = "✅" if x["action"] != "neutral" else "⛔"
                                    msg += f"{icon} <b>{x['symbol']}</b>\n{x['reason'][:70]}\n\n"
                                await self.send(msg, self.menu())
                            elif d == "cmd_realtest":
                                asyncio.create_task(self.engine.real_test_trade())
            except Exception as e:
                log.error(f"TG poll: {e}")
            await asyncio.sleep(1)

# ============================================================================
# 7. ENGINE
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

        self.ex_data = ccxt.binance({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"}
        })

        self.prices: Dict[str, float] = {}
        self.open_times: Dict[str, float] = {}
        self._binance_ban_until = 0

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 100) -> list:
        if time.time() < self._binance_ban_until:
            return []
        b_symbol = BINANCE_SYMBOL_MAP.get(symbol)
        if not b_symbol:
            return []
        try:
            candles = await self.ex_data.fetch_ohlcv(b_symbol, timeframe=timeframe, limit=limit)
            if candles and len(candles) >= 40:
                return candles
            return []
        except Exception as e:
            err = str(e)
            if "418" in err or "banned" in err.lower() or "-1003" in err:
                self._binance_ban_until = time.time() + 220
                log.error("Binance temporarily banned – pause 220s")
            await self.db.log_decision(symbol, "neutral", "", "خطا در دریافت کندل Binance", extra=err[:150])
            return []

    async def start(self):
        await self.db.init()
        log.info("v14.2 Hybrid Tuned starting...")

        try:
            await self.ex.load_markets()
            log.info("Phemex markets loaded")
        except Exception as e:
            log.error(f"Phemex load_markets: {e}")

        try:
            await self.ex_data.load_markets()
            log.info("Binance markets loaded")
        except Exception as e:
            log.warning(f"Binance load_markets: {e}")

        try:
            await self.ex.set_position_mode(False)
            log.info("Position mode → One-Way")
        except Exception as e:
            log.warning(f"Position mode: {e}")

        for sym in SYMBOLS:
            try:
                await self.ex.set_leverage(LEVERAGE, sym)
                log.info(f"Leverage OK → {sym}")
            except Exception as e:
                log.warning(f"Leverage {sym}: {e}")

        for t in await self.db.get_open_trades():
            with STATE_LOCK:
                SHARED_STATE["active_positions"][t["id"]] = {
                    "id": t["id"], "symbol": t["symbol"], "side": t["side"], "strategy": t["strategy"],
                    "entry": t["entry_price"], "qty": t["qty"], "sl": t["sl"], "tp": t["tp"],
                    "tp1": t["tp1"], "is_partial": t.get("is_partial", 0), "highest_pnl_pct": t.get("highest_pnl_pct", 0)
                }
                self.open_times[t["id"]] = time.time()
            log.info(f"Loaded from DB: {t['symbol']}")

        await self.smart_sync()
        await self.update_balance()

        await asyncio.gather(
            self.price_loop(),
            self.scan_loop(),
            self.watchdog_loop(),
            self.tg.poll()
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
            log.info(f"Balance: ${usdt:.2f}")
        except Exception as e:
            log.error(f"Balance error: {e}")

    async def price_loop(self):
        while True:
            try:
                tickers = await self.ex.fetch_tickers(SYMBOLS)
                for s, d in tickers.items():
                    if d.get("last"):
                        self.prices[s] = float(d["last"])
                await self.update_balance()
                with STATE_LOCK:
                    cur = SHARED_STATE["balance"]
                    peak = SHARED_STATE["peak_balance"]
                    if peak > 0:
                        dd = (peak - cur) / peak * 100
                        SHARED_STATE["current_dd"] = dd
                        SHARED_STATE["dd_halted"] = dd >= MAX_DD
                    day_start = SHARED_STATE["day_start_balance"]
                    if day_start > 0:
                        SHARED_STATE["daily_pnl"] = cur - day_start
                        SHARED_STATE["daily_halted"] = ((cur - day_start) / day_start * 100) <= -MAX_DAILY_LOSS
                await self.db.log_equity(cur, peak, SHARED_STATE.get("current_dd", 0))
            except Exception as e:
                log.error(f"price_loop: {e}")
            await asyncio.sleep(9)

    async def scan_loop(self):
        while True:
            with STATE_LOCK:
                can = (SHARED_STATE["is_active"] and not SHARED_STATE["dd_halted"]
                       and not SHARED_STATE["daily_halted"] and len(SHARED_STATE["active_positions"]) < MAX_POS)
            if not can:
                await asyncio.sleep(15)
                continue

            with STATE_LOCK:
                SHARED_STATE["last_scan"] = time.strftime("%H:%M:%S")

            for sym in SYMBOLS:
                if sym in SYMBOL_ERROR_COOLDOWN and time.time() < SYMBOL_ERROR_COOLDOWN[sym]:
                    continue
                with STATE_LOCK:
                    if any(p["symbol"] == sym for p in SHARED_STATE["active_positions"].values()):
                        continue
                try:
                    raw5 = await self.fetch_ohlcv(sym, TIMEFRAME, 120)
                    await asyncio.sleep(SYMBOL_DELAY)
                    raw1 = await self.fetch_ohlcv(sym, HTF_TIMEFRAME, 80)
                    await asyncio.sleep(0.9)

                    if not raw5 or len(raw5) < 50:
                        continue

                    df5 = pd.DataFrame(raw5, columns=["ts", "open", "high", "low", "close", "volume"])
                    df1 = pd.DataFrame(raw1, columns=["ts", "open", "high", "low", "close", "volume"]) if raw1 and len(raw1) > 25 else df5
                    sig = self.strategy.analyze(df5, df1)

                    phemex_price = self.prices.get(sym)
                    binance_price = float(df5["close"].iloc[-1])

                    if not phemex_price or phemex_price <= 0 or binance_price <= 0:
                        continue

                    ratio = phemex_price / binance_price
                    if ratio > 2.0 or ratio < 0.5:
                        await self.db.log_decision(sym, "neutral", "", f"قیمت غیرمنطقی (نسبت {ratio:.2f})", price=phemex_price)
                        continue

                    max_diff = SYMBOL_CONFIG.get(sym, {}).get("max_price_diff", 1.3)
                    avg_price = (phemex_price + binance_price) / 2
                    diff_pct = abs(phemex_price - binance_price) / avg_price * 100

                    if diff_pct > max_diff:
                        await self.db.log_decision(sym, "neutral", "", f"اختلاف قیمت ({diff_pct:.2f}% > {max_diff}%)",
                                                   price=phemex_price,
                                                   extra=f"Phemex={phemex_price:.5f} | Binance={binance_price:.5f}")
                        continue

                    await self.db.log_decision(sym, sig["action"], sig.get("strat", ""), sig.get("reason", ""),
                                               phemex_price, sig.get("rsi", 0), sig.get("atr", 0), sig.get("htf", ""))

                    if sig["action"] != "neutral":
                        atr = sig.get("atr", 0)
                        if atr > 0:
                            params = STRATEGY_PARAMS.get(sig.get("strat", ""), {"sl_m": 1.3, "tp_m": 2.6, "tp1_m": 1.3})
                            if sig["action"] == "buy":
                                sig["sl"] = phemex_price - atr * params["sl_m"]
                                sig["tp"] = phemex_price + atr * params["tp_m"]
                                sig["tp1"] = phemex_price + atr * params["tp1_m"]
                            else:
                                sig["sl"] = phemex_price + atr * params["sl_m"]
                                sig["tp"] = phemex_price - atr * params["tp_m"]
                                sig["tp1"] = phemex_price - atr * params["tp1_m"]
                        await self.execute_trade(sym, sig)

                except Exception as e:
                    log.error(f"scan {sym}: {e}")
                await asyncio.sleep(SYMBOL_DELAY)

            await asyncio.sleep(SCAN_INTERVAL)

    async def execute_trade(self, sym: str, sig: dict):
        if sym in SYMBOL_ERROR_COOLDOWN and time.time() < SYMBOL_ERROR_COOLDOWN[sym]:
            return
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
            SYMBOL_ERROR_COUNT.pop(sym, None)
            SYMBOL_ERROR_COOLDOWN.pop(sym, None)
            await self.tg.send(
                f"🎯 <b>ورود {sig['action'].upper()}</b> ({sig['strat']})\n"
                f"{sym} @ {fill:.4f}\nRR≈{sig.get('expected_rr', '?')} | MinHold {MIN_HOLD_SECONDS//60}m"
            )
        except Exception as e:
            err = str(e)
            SYMBOL_ERROR_COUNT[sym] = SYMBOL_ERROR_COUNT.get(sym, 0) + 1
            count = SYMBOL_ERROR_COUNT[sym]
            cooldown = min(300 * (2 ** (count - 1)), 3600)
            SYMBOL_ERROR_COOLDOWN[sym] = time.time() + cooldown
            log.error(f"❌ {sym} خطا #{count} → cooldown {cooldown}s")
            await self.db.log_decision(sym, "rejected", sig.get("strat", ""), err[:120])
            if "20004" in err or "INCONSISTENT" in err.upper():
                try:
                    await self.ex.set_position_mode(False)
                except Exception:
                    pass

    async def real_test_trade(self):
        await self.tg.send("⚡ شروع تست واقعی ۳۰ ثانیه‌ای...")
        try:
            await self.update_balance()
            with STATE_LOCK:
                free = SHARED_STATE["balance"]
            if free < 20:
                await self.tg.send("❌ موجودی کافی نیست")
                return
            price = self.prices.get(TEST_SYMBOL)
            if not price:
                await self.tg.send("❌ قیمت در دسترس نیست")
                return
            qty = float(self.ex.amount_to_precision(TEST_SYMBOL, min(TEST_USD, free * 0.08) / price))
            order = await self.ex.create_market_order(TEST_SYMBOL, "buy", qty)
            fill = float(order.get("average") or price)
            pid = f"test_{uuid.uuid4().hex[:6]}"
            pos = {"id": pid, "symbol": TEST_SYMBOL, "side": "buy", "strategy": "RealTest",
                   "entry": fill, "qty": qty, "sl": fill * 0.97, "tp": fill * 1.03, "tp1": fill * 1.015,
                   "is_partial": 0, "highest_pnl_pct": 0.0}
            with STATE_LOCK:
                SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()
            await self.tg.send(f"🧪 تست باز شد @ {fill:.5f}")
            await asyncio.sleep(30)
            await self.force_close(pid, "RealTest completed")
            await self.tg.send("✅ تست واقعی بسته شد")
        except Exception as e:
            await self.tg.send(f"❌ خطا: {e}")

    async def smart_sync(self):
        try:
            remote_positions = await self.ex.fetch_positions()
            remote_map = {}
            for p in remote_positions:
                contracts = float(p.get("contracts") or 0)
                if abs(contracts) <= 0:
                    continue
                raw = p.get("symbol", "")
                matched = next((s for s in SYMBOLS if s.split("/")[0] in raw), None)
                if matched:
                    side = "buy" if contracts > 0 else "sell"
                    entry = float(p.get("entryPrice") or p.get("avgEntryPrice") or 0)
                    remote_map[matched] = {"symbol": matched, "side": side, "qty": abs(contracts), "entry": entry}

            with STATE_LOCK:
                local_items = list(SHARED_STATE["active_positions"].items())
            for pid, pos in local_items:
                if pos["strategy"] == "RealTest":
                    continue
                if pos["symbol"] not in remote_map:
                    await self.db.close_trade(pid, 0.0, reason="not found on exchange")
                    with STATE_LOCK:
                        SHARED_STATE["active_positions"].pop(pid, None)
                    self.open_times.pop(pid, None)

            with STATE_LOCK:
                known = {p["symbol"] for p in SHARED_STATE["active_positions"].values()}
            for sym, rpos in remote_map.items():
                if sym in known:
                    continue
                log.warning(f"🔄 Recovered: {sym}")
                pid = f"recovered_{uuid.uuid4().hex[:8]}"
                entry = rpos["entry"] if rpos["entry"] > 0 else self.prices.get(sym, 0)
                if entry <= 0:
                    continue
                atr_a = entry * 0.015
                if rpos["side"] == "buy":
                    sl, tp, tp1 = entry - atr_a * 1.4, entry + atr_a * 2.8, entry + atr_a * 1.4
                else:
                    sl, tp, tp1 = entry + atr_a * 1.4, entry - atr_a * 2.8, entry - atr_a * 1.4
                pos = {"id": pid, "symbol": sym, "side": rpos["side"], "strategy": "Recovered",
                       "entry": entry, "qty": rpos["qty"], "sl": sl, "tp": tp, "tp1": tp1,
                       "is_partial": 0, "highest_pnl_pct": 0.0}
                with STATE_LOCK:
                    SHARED_STATE["active_positions"][pid] = pos
                self.open_times[pid] = time.time()
                await self.db.insert_trade(pos)
                await self.tg.send(f"🔄 پوزیشن بازیابی شد\n{sym} | {rpos['side'].upper()} | Entry:{entry:.5f}")
            log.info(f"Sync done. Active: {len(SHARED_STATE['active_positions'])}")
        except Exception as e:
            log.error(f"smart_sync: {e}")

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
            if pos["strategy"] != "RealTest":
                await self.db.close_trade(pid, net, fees, reason, hold)
            with STATE_LOCK:
                SHARED_STATE["active_positions"].pop(pid, None)
            self.open_times.pop(pid, None)
            await self.db.update_analytics()

            sym = pos["symbol"]
            if net < 0:
                with STATE_LOCK:
                    cl = SHARED_STATE["consecutive_losses"]
                    if sym not in cl:
                        cl[sym] = {"count": 0, "last_loss": 0}
                    cl[sym]["count"] += 1
                    cl[sym]["last_loss"] = time.time()
                    if cl[sym]["count"] >= CONSECUTIVE_LOSS_LIMIT:
                        SYMBOL_ERROR_COOLDOWN[sym] = time.time() + SYMBOL_COOLDOWN_HOURS * 3600
                        await self.tg.send(f"⚠️ {sym}: {cl[sym]['count']} ضرر متوالی → cooldown {SYMBOL_COOLDOWN_HOURS}h")
            else:
                with STATE_LOCK:
                    SHARED_STATE["consecutive_losses"].pop(sym, None)
            await self.tg.send(f"{'🟢' if net >= 0 else '🔴'} بسته شد ({reason}) | ${net:.2f}")
        except Exception as e:
            log.error(f"force_close: {e}")

    async def watchdog_loop(self):
        while True:
            with STATE_LOCK:
                items = list(SHARED_STATE["active_positions"].items())
            now = time.time()
            for pid, pos in items:
                if pos["strategy"] == "RealTest":
                    continue
                price = self.prices.get(pos["symbol"])
                if not price:
                    continue

                hold = now - self.open_times.get(pid, now)
                can_sl_trail = hold >= MIN_HOLD_SECONDS   # حداقل ۱۰ دقیقه

                pnl_pct = ((price - pos["entry"]) / pos["entry"] * 100) if pos["side"] == "buy" else ((pos["entry"] - price) / pos["entry"] * 100)

                # Trailing فقط بعد از min-hold و بعد از TRAIL_ACT
                if can_sl_trail and pnl_pct > TRAIL_ACT and pnl_pct > pos["highest_pnl_pct"]:
                    pos["highest_pnl_pct"] = pnl_pct
                    new_sl = price * (1 - TRAIL_STEP / 100) if pos["side"] == "buy" else price * (1 + TRAIL_STEP / 100)
                    if (pos["side"] == "buy" and new_sl > pos["sl"]) or (pos["side"] == "sell" and new_sl < pos["sl"]):
                        pos["sl"] = new_sl
                        await self.db.update_trade(pid, pos["qty"], pos["sl"], pos["is_partial"], pos["highest_pnl_pct"])

                # Partial TP (اجازه از همان اول)
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
                                await self.tg.send(f"🔹 Partial TP → BE {pos['symbol']}")
                        except Exception:
                            pass

                # SL فقط بعد از min-hold | TP همیشه
                sl_hit = can_sl_trail and ((pos["side"] == "buy" and price <= pos["sl"]) or (pos["side"] == "sell" and price >= pos["sl"]))
                tp_hit = (pos["side"] == "buy" and price >= pos["tp"]) or (pos["side"] == "sell" and price <= pos["tp"])
                if sl_hit or tp_hit:
                    await self.force_close(pid, "SL/Trail" if sl_hit else "TP")
            await asyncio.sleep(2.0)

# ============================================================================
# 8. WEB
# ============================================================================
app = Flask(__name__)

@app.route("/api/status")
def api_status():
    with STATE_LOCK:
        return jsonify(dict(SHARED_STATE))

@app.route("/")
def dashboard():
    return render_template_string("""
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Quant v14.2</title>
<style>
body{font-family:system-ui;background:#0d1117;color:#c9d1d9;padding:20px}
h1{color:#58a6ff}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin:20px 0}
.card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px}
.value{font-size:1.4rem;font-weight:700;color:#58a6ff}
</style>
</head>
<body>
<h1>🚀 Master Quant v14.2</h1>
<div class="grid">
<div class="card">موجودی<div class="value" id="bal">0.00</div></div>
<div class="card">پوزیشن<div class="value" id="pos">0</div></div>
<div class="card">PnL<div class="value" id="pnl">0.00</div></div>
<div class="card">Win Rate<div class="value" id="wr">0%</div></div>
</div>
<p>آخرین اسکن: <span id="scan">–</span></p>
<script>
async function r(){try{const d=await(await fetch('/api/status')).json();
document.getElementById('bal').textContent=(d.balance||0).toFixed(2);
document.getElementById('pos').textContent=Object.keys(d.active_positions||{}).length;
document.getElementById('pnl').textContent=(d.stats?.total_pnl||0).toFixed(2);
document.getElementById('wr').textContent=(d.stats?.win_rate||0)+'%';
document.getElementById('scan').textContent=d.last_scan||'–';}catch(e){}}
r();setInterval(r,5000);
</script>
</body></html>
""")

def run_web():
    app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False)

# ============================================================================
# 9. MAIN
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