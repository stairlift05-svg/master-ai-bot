#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v16.16 – Phemex-Only
Fix: Order fill verify + robust symbol match + delayed ghost cleanup
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
    "SOL/USDT:USDT",
]

STRATEGY_PARAMS = {
    "Breakout_Momentum":    {"sl_m": 1.50, "tp_m": 3.6, "tp1_m": 1.9},
    "SuperTrend_Pullback":  {"sl_m": 1.45, "tp_m": 3.1, "tp1_m": 1.65},
    "Volume_Surge":         {"sl_m": 1.40, "tp_m": 2.9, "tp1_m": 1.50},
    "RSI_Divergence":       {"sl_m": 1.55, "tp_m": 3.4, "tp1_m": 1.8},
    "RSI_Extreme_Bounce":   {"sl_m": 1.35, "tp_m": 2.6, "tp1_m": 1.40},
    "OrderFlow_Proxy":      {"sl_m": 1.40, "tp_m": 2.8, "tp1_m": 1.45},
    "Footprint_Absorption": {"sl_m": 1.45, "tp_m": 2.7, "tp1_m": 1.40},
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
MAX_HOLD_SECONDS = 4 * 3600
TEST_SYMBOL = "SOL/USDT:USDT"
TEST_USD = 12.0
CONSECUTIVE_LOSS_LIMIT = 2
SYMBOL_COOLDOWN_HOURS = 5
POST_CLOSE_COOLDOWN = 1800
SCAN_INTERVAL = 55
SYMBOL_DELAY = 2.0
TREND_STRENGTH_THRESHOLD = 0.01
SYNC_INTERVAL = 90
GHOST_MISS_LIMIT = 3          # بعد از ۳ بار نبودن روی صرافی → ghost

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("QuantV16.16")

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
    "last_sync": "Never",
    "stats": {"total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0},
    "consecutive_losses": {},
    "fetch_stats": defaultdict(lambda: {"ok_5m": 0, "fail_5m": 0, "ok_1h": 0, "fail_1h": 0}),
    "recent_errors": [],
    "signal_but_not_executed": [],
    "trend_strengths": [],
}
STATE_LOCK = Lock()
SYMBOL_ERROR_COOLDOWN: Dict[str, float] = {}
SYMBOL_ERROR_COUNT: Dict[str, int] = {}
SYMBOL_POST_CLOSE_COOLDOWN: Dict[str, float] = {}
# شمارنده نبودن پوزیشن روی صرافی (برای تأخیر در ghost)
POSITION_MISS_COUNT: Dict[str, int] = {}

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

    async def get_recent_decisions(self, limit=300) -> List[dict]:
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

    async def generate_txt_report(self, prices: Dict[str, float] = None, open_times: Dict[str, float] = None) -> str:
        prices = prices or {}
        open_times = open_times or {}
        decisions = await self.get_recent_decisions(250)
        closed = await self.get_closed_trades(40)
        with STATE_LOCK:
            st = dict(SHARED_STATE)
            fetch_stats = dict(st.get("fetch_stats", {}))
            recent_errors = list(st.get("recent_errors", []))[-12:]
            signal_not_exec = list(st.get("signal_but_not_executed", []))[-8:]
            trend_vals = list(st.get("trend_strengths", []))[-30:]
            active = dict(st.get("active_positions", {}))

        now = time.time()
        lines = []
        lines.append("=" * 80)
        lines.append("     MASTER QUANT ENGINE v16.16  |  EXECUTION FIX REPORT")
        lines.append(f"     Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        lines.append("=" * 80)
        lines.append("")
        lines.append("┌─ 1. EXECUTIVE SUMMARY ───────────────────────────────────────────────────")
        lines.append(f"│  Equity            : ${st.get('balance', 0):,.2f}")
        lines.append(f"│  Peak Equity       : ${st.get('peak_balance', 0):,.2f}")
        lines.append(f"│  Drawdown          : {st.get('current_dd', 0):.2f}%")
        lines.append(f"│  Daily P&L         : ${st.get('daily_pnl', 0):+.2f}")
        lines.append(f"│  Open Risk         : {len(active)} / {MAX_POS} slots")
        lines.append(f"│  Closed Trades     : {st.get('stats', {}).get('total_trades', 0)}")
        lines.append(f"│  Win Rate          : {st.get('stats', {}).get('win_rate', 0):.1f}%")
        lines.append(f"│  Realized P&L      : ${st.get('stats', {}).get('total_pnl', 0):+.2f}")
        lines.append(f"│  Bot Status        : {'ACTIVE' if st.get('is_active') else 'PAUSED'}")
        lines.append(f"│  Last Scan         : {st.get('last_scan', '–')}")
        lines.append(f"│  Last Sync         : {st.get('last_sync', '–')}")
        lines.append("└──────────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("┌─ 2. OPEN POSITIONS (LIVE RISK) ──────────────────────────────────────────")
        if not active:
            lines.append("│  (flat – no open risk)")
        else:
            total_upnl = 0.0
            for pid, p in active.items():
                pr = prices.get(p["symbol"], p["entry"])
                side_sign = 1 if p["side"] == "buy" else -1
                upnl = (pr - p["entry"]) * p["qty"] * side_sign
                total_upnl += upnl
                hold_h = (now - open_times.get(pid, now)) / 3600
                miss = POSITION_MISS_COUNT.get(pid, 0)
                lines.append(f"│  {p['symbol']:<18} {p['side'].upper():<5}  {p.get('strategy',''):<20}")
                lines.append(f"│    Entry {p['entry']:.5f}  Mark {pr:.5f}  Qty {p['qty']:.4f}")
                lines.append(f"│    uPnL ${upnl:+.3f}  Hold {hold_h:.1f}h  MissCount {miss}/{GHOST_MISS_LIMIT}")
                lines.append(f"│    SL {p['sl']:.5f}  TP {p['tp']:.5f}")
                lines.append("│")
            lines.append(f"│  TOTAL uPnL: ${total_upnl:+.3f}")
        lines.append("└──────────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("┌─ 3. MARKET DATA HEALTH ──────────────────────────────────────────────────")
        lines.append("│  Symbol            | 5m OK | 5m Fail | 1h OK | 1h Fail")
        lines.append("│  ------------------+-------+---------+-------+--------")
        for sym in SYMBOLS:
            s = fetch_stats.get(sym, {"ok_5m": 0, "fail_5m": 0, "ok_1h": 0, "fail_1h": 0})
            lines.append(f"│  {sym:<18} | {s['ok_5m']:5d} | {s['fail_5m']:7d} | {s['ok_1h']:5d} | {s['fail_1h']:6d}")
        lines.append("└──────────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("┌─ 4. TREND REGIME ────────────────────────────────────────────────────────")
        if trend_vals:
            vals = [v["value"] for v in trend_vals]
            lines.append(f"│  Samples {len(vals)}  Avg {sum(vals)/len(vals):.3f}%  Min {min(vals):.3f}%  Max {max(vals):.3f}%")
            for item in trend_vals[-8:]:
                lines.append(f"│    {item['ts']}  {item['symbol']:<18}  {item['value']:.3f}%")
        else:
            lines.append("│  (no samples)")
        lines.append("└──────────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("┌─ 5. CLOSED TRADES (RECENT) ──────────────────────────────────────────────")
        if not closed:
            lines.append("│  (none)")
        else:
            for t in closed[:15]:
                tag = "WIN " if t["pnl"] > 0 else "LOSS"
                hold_m = (t.get("hold_seconds") or 0) / 60
                lines.append(f"│  [{tag}] {t['symbol']:<16} {t['side']:<4} ${t['pnl']:+.3f}  {hold_m:.0f}m  {t.get('exit_reason','')[:30]}")
        lines.append("└──────────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("┌─ 6. DECISION ENGINE ─────────────────────────────────────────────────────")
        reasons = Counter()
        by_symbol = defaultdict(lambda: {"sig": 0, "rej": 0})
        signals = 0
        for d in decisions:
            if d["action"] in ("neutral", "rejected"):
                reasons[(d["reason"] or "?")[:65]] += 1
                by_symbol[d["symbol"]]["rej"] += 1
            else:
                signals += 1
                by_symbol[d["symbol"]]["sig"] += 1
        lines.append(f"│  Decisions {len(decisions)}  |  Signals {signals}  |  Rejected {len(decisions)-signals}")
        lines.append("│  Top blockers:")
        for reason, count in reasons.most_common(8):
            lines.append(f"│    {count:4d} × {reason}")
        lines.append("│  Per symbol:")
        for sym in SYMBOLS:
            v = by_symbol.get(sym, {"sig": 0, "rej": 0})
            lines.append(f"│    {sym:<18} Sig {v['sig']:3d}  Rej {v['rej']:3d}")
        lines.append("└──────────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("┌─ 7. EXECUTION GAPS & ERRORS ──────────────────────────────────────────────")
        if signal_not_exec:
            for item in signal_not_exec:
                lines.append(f"│  {item}")
        else:
            lines.append("│  (no missed signals)")
        if recent_errors:
            lines.append("│  Recent errors:")
            for err in recent_errors[-8:]:
                lines.append(f"│    {err}")
        lines.append("└──────────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("┌─ 8. LAST 15 DECISIONS ───────────────────────────────────────────────────")
        for d in (decisions or [])[:15]:
            icon = "SIG" if d["action"] not in ("neutral", "rejected") else "REJ"
            ts = (d.get("ts") or "")[:19]
            lines.append(f"│  [{icon}] {ts} {d['symbol']:<18} {d.get('reason','')[:50]}")
        lines.append("└──────────────────────────────────────────────────────────────────────────")
        lines.append("")
        lines.append("=" * 80)
        lines.append("  End of Report – Master Quant v16.16 Execution Fix")
        lines.append("=" * 80)
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

    @staticmethod
    def volume_delta(df: pd.DataFrame) -> pd.Series:
        direction = (df["close"] - df["open"]).apply(lambda x: 1 if x >= 0 else -1)
        return df["volume"] * direction

    @staticmethod
    def detect_rsi_divergence(df: pd.DataFrame, lookback: int = 28) -> Optional[str]:
        if len(df) < lookback + 5:
            return None
        close = df["close"].iloc[-lookback:]
        rsi = Indicators.rsi(df["close"]).iloc[-lookback:]
        price_lows, rsi_lows = [], []
        price_highs, rsi_highs = [], []
        for i in range(3, len(close) - 3):
            if (close.iloc[i] < close.iloc[i-1] and close.iloc[i] < close.iloc[i-2] and
                close.iloc[i] < close.iloc[i-3] and close.iloc[i] < close.iloc[i+1] and close.iloc[i] < close.iloc[i+2]):
                price_lows.append((i, float(close.iloc[i])))
                rsi_lows.append((i, float(rsi.iloc[i])))
            if (close.iloc[i] > close.iloc[i-1] and close.iloc[i] > close.iloc[i-2] and
                close.iloc[i] > close.iloc[i-3] and close.iloc[i] > close.iloc[i+1] and close.iloc[i] > close.iloc[i+2]):
                price_highs.append((i, float(close.iloc[i])))
                rsi_highs.append((i, float(rsi.iloc[i])))
        if len(price_lows) >= 2 and len(rsi_lows) >= 2:
            p1, p2 = price_lows[-2], price_lows[-1]
            r1, r2 = rsi_lows[-2], rsi_lows[-1]
            if (p2[0] - p1[0]) >= 4 and p2[1] < p1[1] * 0.998 and r2[1] > r1[1] + 1.5:
                return "bullish"
        if len(price_highs) >= 2 and len(rsi_highs) >= 2:
            p1, p2 = price_highs[-2], price_highs[-1]
            r1, r2 = rsi_highs[-2], rsi_highs[-1]
            if (p2[0] - p1[0]) >= 4 and p2[1] > p1[1] * 1.002 and r2[1] < r1[1] - 1.5:
                return "bearish"
        return None

# ===================== STRATEGY =====================
class StrategyEngine:
    def analyze(self, df_5m: pd.DataFrame, df_1h: pd.DataFrame, symbol: str = "") -> dict:
        df = df_5m.iloc[:-1].copy()
        htf = df_1h.iloc[:-1].copy() if len(df_1h) > 30 else df
        if len(df) < 55:
            return {"action": "neutral", "reason": "داده ناکافی", "strat": "", "rsi": 0, "atr": 0, "htf": ""}
        if df["close"].iloc[-1] <= 0:
            return {"action": "neutral", "reason": "داده قیمت نامعتبر", "strat": "", "rsi": 0, "atr": 0, "htf": ""}

        hclose = htf["close"]
        e50 = hclose.ewm(span=50, adjust=False).mean().iloc[-1]
        e200 = hclose.ewm(span=min(200, len(htf)), adjust=False).mean().iloc[-1]
        hp = float(hclose.iloc[-1])
        trend_strength = abs(e50 - e200) / (e200 + 1e-9) * 100

        with STATE_LOCK:
            SHARED_STATE["trend_strengths"].append({
                "ts": datetime.utcnow().strftime("%H:%M:%S"),
                "symbol": symbol,
                "value": round(trend_strength, 3)
            })
            if len(SHARED_STATE["trend_strengths"]) > 80:
                SHARED_STATE["trend_strengths"] = SHARED_STATE["trend_strengths"][-80:]

        weak_trend = trend_strength < TREND_STRENGTH_THRESHOLD
        if hp > e50 * 0.993 and e50 >= e200 * 0.990:
            htf_trend = "bullish"
        elif hp < e50 * 1.007 and e50 <= e200 * 1.010:
            htf_trend = "bearish"
        else:
            htf_trend = "sideways"

        c, high, low, vol = df["close"], df["high"], df["low"], df["volume"]
        price = float(c.iloc[-1])
        o = float(df["open"].iloc[-1])
        atr_s = Indicators.atr(df, 14)
        atr = float(atr_s.iloc[-1])
        if atr <= 0 or pd.isna(atr):
            return {"action": "neutral", "reason": "ATR صفر", "strat": "", "rsi": 0, "atr": 0, "htf": htf_trend}

        rsi = float(Indicators.rsi(c).iloc[-1])
        if pd.isna(rsi) or rsi >= 98 or rsi <= 2:
            return {"action": "neutral", "reason": f"RSI نامعتبر ({rsi:.1f})", "strat": "", "rsi": rsi, "atr": atr, "htf": htf_trend}

        ema20 = float(c.ewm(span=20, adjust=False).mean().iloc[-1])
        st_d, st_u, st_l = Indicators.supertrend(df)
        vsma = float(Indicators.sma(vol, 20).iloc[-1]) or 1e-9
        vcur = float(vol.iloc[-1])
        h12 = float(Indicators.highest(high, 12).iloc[-1])
        l12 = float(Indicators.lowest(low, 12).iloc[-1])
        vol_ok = vcur > vsma * 1.15
        candle_bull = c.iloc[-1] > c.iloc[-2]
        candle_bear = c.iloc[-1] < c.iloc[-2]
        body = abs(price - o)
        range_ = float(high.iloc[-1] - low.iloc[-1]) + 1e-12
        upper_wick = float(high.iloc[-1] - max(price, o))
        lower_wick = float(min(price, o) - low.iloc[-1])

        vdelta = Indicators.volume_delta(df)
        delta_sum = float(vdelta.iloc[-6:].sum())
        delta_prev = float(vdelta.iloc[-12:-6].sum()) if len(vdelta) >= 12 else 0
        delta_sma = float(vdelta.iloc[-20:].mean()) if len(vdelta) >= 20 else 0

        if rsi < 20 and candle_bull and vcur > vsma * 0.85:
            return self._build("buy", "RSI_Extreme_Bounce", price, atr, rsi, htf_trend)
        if rsi > 80 and candle_bear and vcur > vsma * 0.85:
            return self._build("sell", "RSI_Extreme_Bounce", price, atr, rsi, htf_trend)

        if (vcur > vsma * 1.6 and body < range_ * 0.35 and lower_wick > body * 1.2
                and candle_bull and rsi < 45 and price > l12 * 0.998):
            return self._build("buy", "Footprint_Absorption", price, atr, rsi, htf_trend)
        if (vcur > vsma * 1.6 and body < range_ * 0.35 and upper_wick > body * 1.2
                and candle_bear and rsi > 55 and price < h12 * 1.002):
            return self._build("sell", "Footprint_Absorption", price, atr, rsi, htf_trend)

        if (delta_sum > abs(delta_sma) * 2.2 and delta_sum > delta_prev
                and candle_bull and price > ema20 and 35 < rsi < 72 and vcur > vsma * 1.05):
            return self._build("buy", "OrderFlow_Proxy", price, atr, rsi, htf_trend)
        if (delta_sum < -abs(delta_sma) * 2.2 and delta_sum < delta_prev
                and candle_bear and price < ema20 and 28 < rsi < 65 and vcur > vsma * 1.05):
            return self._build("sell", "OrderFlow_Proxy", price, atr, rsi, htf_trend)

        if weak_trend:
            return {"action": "neutral", "reason": f"روند ضعیف ({trend_strength:.3f}%)", "strat": "", "rsi": rsi, "atr": atr, "htf": "weak"}
        if htf_trend == "sideways":
            return {"action": "neutral", "reason": "روند HTF نامشخص", "strat": "", "rsi": rsi, "atr": atr, "htf": "sideways"}

        if htf_trend == "bullish" and price > ema20 * 1.0005 and price >= h12 * 0.997 and 42 < rsi < 72 and vol_ok:
            return self._build("buy", "Breakout_Momentum", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and price < ema20 * 0.9995 and price <= l12 * 1.003 and 28 < rsi < 58 and vol_ok:
            return self._build("sell", "Breakout_Momentum", price, atr, rsi, htf_trend)
        if htf_trend == "bullish" and st_d.iloc[-1] == 1 and low.iloc[-1] <= st_l.iloc[-1] * 1.008 and candle_bull and 38 < rsi < 68 and price > ema20:
            return self._build("buy", "SuperTrend_Pullback", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and st_d.iloc[-1] == -1 and high.iloc[-1] >= st_u.iloc[-1] * 0.992 and candle_bear and 32 < rsi < 62 and price < ema20:
            return self._build("sell", "SuperTrend_Pullback", price, atr, rsi, htf_trend)
        if htf_trend == "bullish" and price > ema20 and vcur > vsma * 1.35 and candle_bull and 43 < rsi < 70:
            return self._build("buy", "Volume_Surge", price, atr, rsi, htf_trend)
        if htf_trend == "bearish" and price < ema20 and vcur > vsma * 1.35 and candle_bear and 30 < rsi < 57:
            return self._build("sell", "Volume_Surge", price, atr, rsi, htf_trend)

        divergence = Indicators.detect_rsi_divergence(df, lookback=28)
        if divergence == "bullish" and htf_trend == "bullish" and rsi < 45 and vcur > vsma * 1.10 and candle_bull:
            return self._build("buy", "RSI_Divergence", price, atr, rsi, htf_trend)
        if divergence == "bearish" and htf_trend == "bearish" and rsi > 55 and vcur > vsma * 1.10 and candle_bear:
            return self._build("sell", "RSI_Divergence", price, atr, rsi, htf_trend)

        return {"action": "neutral", "reason": f"بدون سیگنال (RSI={rsi:.1f})", "strat": "", "rsi": rsi, "atr": atr, "htf": htf_trend}

    def _build(self, side, strat, price, atr, rsi, htf):
        p = STRATEGY_PARAMS.get(strat, {"sl_m": 1.50, "tp_m": 3.2, "tp1_m": 1.7})
        if side == "buy":
            return {"action": side, "strat": strat, "sl": price - atr * p["sl_m"], "tp": price + atr * p["tp_m"],
                    "tp1": price + atr * p["tp1_m"], "reason": f"سیگنال {strat}", "rsi": rsi, "atr": atr, "htf": htf}
        return {"action": side, "strat": strat, "sl": price + atr * p["sl_m"], "tp": price - atr * p["tp_m"],
                "tp1": price - atr * p["tp1_m"], "reason": f"سیگنال {strat}", "rsi": rsi, "atr": atr, "htf": htf}

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
        qty = min(qty, (free_usdt * 0.14 * LEVERAGE) / price, (balance * MAX_EXPOSURE_PCT / 100.0) / price)
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
        rows = [
            [{"text": "📊 Dashboard", "callback_data": "cmd_dash"}, {"text": "💼 Positions", "callback_data": "cmd_pos"}],
            [{"text": "🔄 Sync", "callback_data": "cmd_sync"}, {"text": btn, "callback_data": act}],
            [{"text": "📄 Report", "callback_data": "cmd_txt"}, {"text": "🚫 Rejections", "callback_data": "cmd_rej"}],
            [{"text": "⚡ REAL TEST", "callback_data": "cmd_realtest"}],
        ]
        with STATE_LOCK:
            positions = list(SHARED_STATE["active_positions"].items())
        for pid, p in positions[:5]:
            short = p["symbol"].split("/")[0]
            rows.append([{"text": f"❌ Close {short} {p['side'].upper()}", "callback_data": f"close_{pid}"}])
        return {"inline_keyboard": rows}

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
            await self.send("❌ file not found")
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
        await self.send("🚀 <b>Master Quant v16.16</b>\nExecution + Ghost delay fix", self.menu())
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
                            if d.startswith("close_"):
                                pid = d.replace("close_", "", 1)
                                await self.engine.force_close(pid, "Manual_TG")
                                await self.send(f"✅ Close requested", self.menu())
                            elif d == "cmd_start":
                                with STATE_LOCK:
                                    SHARED_STATE["is_active"] = True
                                await self.send("▶️ Started", self.menu())
                            elif d == "cmd_pause":
                                with STATE_LOCK:
                                    SHARED_STATE["is_active"] = False
                                await self.send("⏸️ Paused", self.menu())
                            elif d == "cmd_dash":
                                with STATE_LOCK:
                                    st = dict(SHARED_STATE)
                                await self.send(
                                    f"📊 <b>v16.16</b>\nEquity: <b>${st['balance']:.2f}</b>\n"
                                    f"DD: {st['current_dd']:.1f}% | Pos: {len(st['active_positions'])}/{MAX_POS}\n"
                                    f"PnL: ${st['stats']['total_pnl']:.2f} | Scan: {st['last_scan']}",
                                    self.menu())
                            elif d == "cmd_pos":
                                with STATE_LOCK:
                                    pos = dict(SHARED_STATE["active_positions"])
                                if not pos:
                                    await self.send("💤 flat", self.menu())
                                else:
                                    msg = "💼 <b>Open</b>\n\n"
                                    for pid, p in pos.items():
                                        pr = self.engine.prices.get(p["symbol"], p["entry"])
                                        pnl = (pr - p["entry"]) * p["qty"] * (1 if p["side"] == "buy" else -1)
                                        msg += f"{'🟢' if pnl>=0 else '🔴'} {p['symbol']} {p['side'].upper()} ${pnl:+.2f}\n"
                                    await self.send(msg, self.menu())
                            elif d == "cmd_sync":
                                await self.engine.smart_sync()
                                await self.send("🔄 Sync done", self.menu())
                            elif d == "cmd_txt":
                                report = await self.engine.db.generate_txt_report(
                                    self.engine.prices, self.engine.open_times)
                                with open("report.txt", "w", encoding="utf-8") as f:
                                    f.write(report)
                                await self.send_document("report.txt", "📄 Report v16.16")
                            elif d == "cmd_rej":
                                decs = await self.engine.db.get_recent_decisions(12)
                                msg = "🚫 <b>Last</b>\n\n"
                                for x in decs:
                                    icon = "✅" if x["action"] not in ("neutral", "rejected") else "⛔"
                                    msg += f"{icon} {x['symbol']}\n{x['reason'][:70]}\n\n"
                                await self.send(msg, self.menu())
                            elif d == "cmd_realtest":
                                asyncio.create_task(self.engine.real_test_trade())
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
            "apiKey": API_KEY, "secret": API_SECRET,
            "enableRateLimit": True, "options": {"defaultType": "swap"},
        })
        self.ex.set_sandbox_mode(TESTNET)
        self.prices: Dict[str, float] = {}
        self.open_times: Dict[str, float] = {}

    def _record_error(self, msg: str):
        with STATE_LOCK:
            errs = SHARED_STATE["recent_errors"]
            errs.append(f"{datetime.utcnow().strftime('%H:%M:%S')} {msg[:120]}")
            if len(errs) > 30:
                SHARED_STATE["recent_errors"] = errs[-30:]

    def _record_fetch(self, symbol: str, timeframe: str, success: bool):
        with STATE_LOCK:
            s = SHARED_STATE["fetch_stats"][symbol]
            if timeframe == "5m":
                if success: s["ok_5m"] += 1
                else: s["fail_5m"] += 1
            else:
                if success: s["ok_1h"] += 1
                else: s["fail_1h"] += 1

    def _match_symbol(self, raw: str) -> Optional[str]:
        """تطبیق قوی نماد remote با SYMBOLS"""
        if not raw:
            return None
        raw_u = raw.upper().replace(":USDT", "").replace("/USDT", "").replace("USDT", "")
        for s in SYMBOLS:
            base = s.split("/")[0].upper()
            if base == raw_u or base in raw.upper() or raw.upper() in s.upper():
                return s
        return None

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 100) -> list:
        try:
            actual_limit = 50 if timeframe == "1h" else limit
            candles = await self.ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=actual_limit)
            if not candles or len(candles) < 30 or candles[-1][4] <= 0:
                self._record_fetch(symbol, timeframe, False)
                return []
            self._record_fetch(symbol, timeframe, True)
            return candles
        except Exception as e:
            self._record_fetch(symbol, timeframe, False)
            self._record_error(f"fetch {symbol} {timeframe}: {str(e)[:80]}")
            return []

    async def get_live_price(self, symbol: str) -> float:
        price = self.prices.get(symbol)
        if price and price > 0:
            return price
        try:
            ticker = await self.ex.fetch_ticker(symbol)
            price = float(ticker.get("last") or ticker.get("close") or 0)
            if price > 0:
                self.prices[symbol] = price
                return price
        except Exception as e:
            self._record_error(f"ticker {symbol}: {e}")
        return 0.0

    async def start(self):
        await self.db.init()
        log.info("v16.16 Execution Fix starting...")
        try:
            await self.ex.load_markets()
            log.info("Markets loaded")
        except Exception as e:
            log.error(f"load_markets: {e}")

        try:
            await self.ex.set_position_mode(False, SYMBOLS[0])
        except Exception:
            try:
                await self.ex.set_position_mode(False)
            except Exception:
                pass

        for sym in SYMBOLS:
            try:
                await self.ex.set_leverage(LEVERAGE, sym)
                log.info(f"Leverage OK → {sym}")
                await asyncio.sleep(0.5)
            except Exception as e:
                log.warning(f"Leverage {sym}: {e}")

        # فقط پوزیشن‌هایی که واقعاً روی صرافی هستند را لود کن
        await self.smart_sync(load_from_db=True)
        await self.update_balance()
        await asyncio.gather(
            self.price_loop(), self.scan_loop(), self.watchdog_loop(),
            self.sync_loop(), self.tg.poll(),
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
            self._record_error(f"Balance: {e}")

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
            await asyncio.sleep(12)

    async def sync_loop(self):
        while True:
            await asyncio.sleep(SYNC_INTERVAL)
            try:
                await self.smart_sync()
            except Exception as e:
                log.error(f"sync_loop: {e}")

    async def scan_loop(self):
        while True:
            with STATE_LOCK:
                can = (SHARED_STATE["is_active"] and not SHARED_STATE["dd_halted"] and
                       not SHARED_STATE["daily_halted"] and len(SHARED_STATE["active_positions"]) < MAX_POS)
                open_syms = {p["symbol"] for p in SHARED_STATE["active_positions"].values()}
            if not can:
                await asyncio.sleep(15)
                continue
            with STATE_LOCK:
                SHARED_STATE["last_scan"] = time.strftime("%H:%M:%S")

            for sym in SYMBOLS:
                if sym in open_syms:
                    continue
                if sym in SYMBOL_ERROR_COOLDOWN and time.time() < SYMBOL_ERROR_COOLDOWN[sym]:
                    continue
                if sym in SYMBOL_POST_CLOSE_COOLDOWN and time.time() < SYMBOL_POST_CLOSE_COOLDOWN[sym]:
                    continue
                try:
                    raw5 = await self.fetch_ohlcv(sym, TIMEFRAME, 100)
                    await asyncio.sleep(1.0)
                    raw1 = await self.fetch_ohlcv(sym, HTF_TIMEFRAME, 50)
                    await asyncio.sleep(SYMBOL_DELAY)
                    if not raw5 or len(raw5) < 50:
                        continue
                    df5 = pd.DataFrame(raw5, columns=["ts", "open", "high", "low", "close", "volume"])
                    df1 = pd.DataFrame(raw1, columns=["ts", "open", "high", "low", "close", "volume"]) if raw1 and len(raw1) > 30 else df5.copy()
                    last_close = float(df5["close"].iloc[-1])
                    if last_close > 0:
                        self.prices[sym] = last_close
                    sig = self.strategy.analyze(df5, df1, symbol=sym)
                    price = self.prices.get(sym) or last_close
                    if price <= 0:
                        continue
                    await self.db.log_decision(sym, sig["action"], sig.get("strat", ""), sig.get("reason", ""),
                                               price, sig.get("rsi", 0), sig.get("atr", 0), sig.get("htf", ""))
                    if sig["action"] != "neutral":
                        atr = sig.get("atr", 0)
                        if atr > 0:
                            p = STRATEGY_PARAMS.get(sig.get("strat", ""), {"sl_m": 1.50, "tp_m": 3.2, "tp1_m": 1.7})
                            if sig["action"] == "buy":
                                sig["sl"] = price - atr * p["sl_m"]
                                sig["tp"] = price + atr * p["tp_m"]
                                sig["tp1"] = price + atr * p["tp1_m"]
                            else:
                                sig["sl"] = price + atr * p["sl_m"]
                                sig["tp"] = price - atr * p["tp_m"]
                                sig["tp1"] = price - atr * p["tp1_m"]
                        await self.execute_trade(sym, sig)
                except Exception as e:
                    log.error(f"scan {sym}: {e}")
                    self._record_error(f"scan {sym}: {e}")
                await asyncio.sleep(0.5)
            await asyncio.sleep(SCAN_INTERVAL)

    async def execute_trade(self, sym: str, sig: dict):
        def record_miss(reason: str):
            with STATE_LOCK:
                lst = SHARED_STATE["signal_but_not_executed"]
                lst.append(f"{datetime.utcnow().strftime('%H:%M:%S')} {sym} {sig.get('strat')} → {reason}")
                if len(lst) > 20:
                    SHARED_STATE["signal_but_not_executed"] = lst[-20:]

        if sym in SYMBOL_ERROR_COOLDOWN and time.time() < SYMBOL_ERROR_COOLDOWN[sym]:
            return
        if sym in SYMBOL_POST_CLOSE_COOLDOWN and time.time() < SYMBOL_POST_CLOSE_COOLDOWN[sym]:
            return

        price = await self.get_live_price(sym)
        if not price or price <= 0:
            record_miss("no price")
            return

        with STATE_LOCK:
            bal = SHARED_STATE["balance"]
            open_count = len(SHARED_STATE["active_positions"])
        if bal < 22 or open_count >= MAX_POS:
            return

        try:
            bal_data = await self.ex.fetch_balance()
            free = float(bal_data.get("USDT", {}).get("free", 0) or 0)
            qty = self.risk.calculate_qty(bal, price, sig["sl"], free, sym, self.ex)
            if qty <= 0 or qty * price < MIN_ORDER_USD:
                record_miss("qty too small")
                return

            order = await self.ex.create_market_order(sym, sig["action"], qty)
            log.info(f"ORDER RAW {sym}: {order}")

            # تأیید پر شدن
            fill = float(order.get("average") or order.get("price") or 0)
            filled = float(order.get("filled") or order.get("amount") or 0)
            status = str(order.get("status") or "").lower()

            if fill <= 0:
                fill = price
            if filled <= 0:
                filled = qty

            # اگر سفارش فوراً reject شده
            if status in ("canceled", "cancelled", "rejected", "expired"):
                reason = f"order status={status}"
                await self.db.log_decision(sym, "rejected", sig.get("strat", ""), reason)
                record_miss(reason)
                self._record_error(f"ORDER REJECT {sym}: {status}")
                return

            # صبر کوتاه و تأیید از صرافی
            await asyncio.sleep(1.5)
            remote_ok = False
            try:
                positions = await self.ex.fetch_positions([sym])
                for p in positions:
                    contracts = abs(float(p.get("contracts") or 0))
                    if contracts > 0:
                        remote_ok = True
                        entry_r = float(p.get("entryPrice") or p.get("avgEntryPrice") or 0)
                        if entry_r > 0:
                            fill = entry_r
                        filled = contracts
                        break
            except Exception as e:
                log.warning(f"post-order position check: {e}")

            if not remote_ok:
                # سفارش ثبت شد ولی پوزیشن دیده نشد — هنوز ثبت محلی نکن
                reason = "order sent but position not visible yet"
                await self.db.log_decision(sym, "rejected", sig.get("strat", ""), reason)
                record_miss(reason)
                self._record_error(f"NO POS AFTER ORDER {sym}")
                await self.tg.send(f"⚠️ {sym}\nسفارش رفت ولی پوزیشن دیده نشد")
                return

            pid = f"pos_{uuid.uuid4().hex[:8]}"
            pos = {
                "id": pid, "symbol": sym, "side": sig["action"], "strategy": sig["strat"],
                "entry": fill, "qty": filled, "sl": sig["sl"], "tp": sig["tp"], "tp1": sig["tp1"],
                "is_partial": 0, "highest_pnl_pct": 0.0,
            }
            with STATE_LOCK:
                SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()
            POSITION_MISS_COUNT[pid] = 0
            await self.db.insert_trade(pos)
            SYMBOL_ERROR_COUNT.pop(sym, None)
            SYMBOL_ERROR_COOLDOWN.pop(sym, None)
            await self.tg.send(
                f"🎯 <b>{sig['action'].upper()}</b> {sig['strat']}\n{sym} @ {fill:.4f}\nQty: {filled:.4f}",
                self.tg.menu())
            log.info(f"TRADE CONFIRMED {sym} {sig['action']} qty={filled:.4f} @ {fill:.4f}")
        except Exception as e:
            err = str(e)
            SYMBOL_ERROR_COUNT[sym] = SYMBOL_ERROR_COUNT.get(sym, 0) + 1
            count = SYMBOL_ERROR_COUNT[sym]
            SYMBOL_ERROR_COOLDOWN[sym] = time.time() + min(350 * (2 ** (count - 1)), 5400)
            await self.db.log_decision(sym, "rejected", sig.get("strat", ""), f"API: {err[:80]}")
            record_miss(err[:80])
            self._record_error(f"EXECUTE {sym}: {err[:80]}")
            await self.tg.send(f"❌ {sym}\n{err[:120]}")

    async def real_test_trade(self):
        await self.tg.send("⚡ Real test...")
        try:
            await self.update_balance()
            price = await self.get_live_price(TEST_SYMBOL)
            if not price:
                await self.tg.send("❌ no price")
                return
            with STATE_LOCK:
                free = SHARED_STATE["balance"]
            qty = float(self.ex.amount_to_precision(TEST_SYMBOL, min(TEST_USD, free * 0.07) / price))
            order = await self.ex.create_market_order(TEST_SYMBOL, "buy", qty)
            log.info(f"TEST ORDER: {order}")
            fill = float(order.get("average") or price)
            pid = f"test_{uuid.uuid4().hex[:6]}"
            pos = {"id": pid, "symbol": TEST_SYMBOL, "side": "buy", "strategy": "RealTest",
                   "entry": fill, "qty": qty, "sl": fill * 0.97, "tp": fill * 1.03, "tp1": fill * 1.015,
                   "is_partial": 0, "highest_pnl_pct": 0.0}
            with STATE_LOCK:
                SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()
            await self.tg.send(f"🧪 opened @ {fill:.5f}")
            await asyncio.sleep(25)
            await self.force_close(pid, "RealTest")
            await self.tg.send("✅ test closed", self.tg.menu())
        except Exception as e:
            await self.tg.send(f"❌ {e}")

    async def _estimate_atr(self, sym: str, entry: float) -> float:
        try:
            raw = await self.fetch_ohlcv(sym, TIMEFRAME, 50)
            if raw and len(raw) >= 20:
                df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
                atr = float(Indicators.atr(df, 14).iloc[-1])
                if atr > 0:
                    return atr
        except Exception:
            pass
        return entry * 0.012

    async def smart_sync(self, load_from_db: bool = False):
        try:
            remote_positions = await self.ex.fetch_positions()
            remote_map = {}
            remote_raw_log = []
            for p in remote_positions:
                contracts = float(p.get("contracts") or 0)
                if abs(contracts) <= 0:
                    continue
                raw = p.get("symbol", "")
                remote_raw_log.append(f"{raw}:{contracts}")
                matched = self._match_symbol(raw)
                if matched:
                    side = "buy" if contracts > 0 else "sell"
                    entry = float(p.get("entryPrice") or p.get("avgEntryPrice") or 0)
                    remote_map[matched] = {
                        "symbol": matched, "side": side,
                        "qty": abs(contracts), "entry": entry, "raw": raw,
                    }
            if remote_raw_log:
                log.info(f"Remote positions: {remote_raw_log}")
            else:
                log.info("Remote positions: (none)")

            with STATE_LOCK:
                local_items = list(SHARED_STATE["active_positions"].items())

            for pid, pos in local_items:
                if pos["strategy"] == "RealTest":
                    continue
                if pos["symbol"] in remote_map:
                    POSITION_MISS_COUNT[pid] = 0
                    continue
                # روی صرافی نیست
                POSITION_MISS_COUNT[pid] = POSITION_MISS_COUNT.get(pid, 0) + 1
                miss = POSITION_MISS_COUNT[pid]
                log.warning(f"Position miss {pos['symbol']} {pid} count={miss}/{GHOST_MISS_LIMIT}")
                if miss >= GHOST_MISS_LIMIT:
                    log.warning(f"Ghost confirmed → remove {pos['symbol']} {pid}")
                    await self.db.close_trade(pid, 0.0, reason="ghost_confirmed", hold=0)
                    with STATE_LOCK:
                        SHARED_STATE["active_positions"].pop(pid, None)
                    self.open_times.pop(pid, None)
                    POSITION_MISS_COUNT.pop(pid, None)
                    await self.tg.send(f"👻 Ghost removed: {pos['symbol']} (after {miss} misses)")

            with STATE_LOCK:
                known = {p["symbol"] for p in SHARED_STATE["active_positions"].values()}

            for sym, rpos in remote_map.items():
                if sym in known:
                    continue
                log.warning(f"Recovered remote {sym} raw={rpos.get('raw')}")
                pid = f"recovered_{uuid.uuid4().hex[:8]}"
                entry = rpos["entry"] if rpos["entry"] > 0 else self.prices.get(sym, 0)
                if entry <= 0:
                    continue
                atr = await self._estimate_atr(sym, entry)
                if rpos["side"] == "buy":
                    sl, tp, tp1 = entry - atr * 1.50, entry + atr * 3.2, entry + atr * 1.7
                else:
                    sl, tp, tp1 = entry + atr * 1.50, entry - atr * 3.2, entry - atr * 1.7
                pos = {"id": pid, "symbol": sym, "side": rpos["side"], "strategy": "Recovered",
                       "entry": entry, "qty": rpos["qty"], "sl": sl, "tp": tp, "tp1": tp1,
                       "is_partial": 0, "highest_pnl_pct": 0.0}
                with STATE_LOCK:
                    SHARED_STATE["active_positions"][pid] = pos
                self.open_times[pid] = time.time()
                POSITION_MISS_COUNT[pid] = 0
                await self.db.insert_trade(pos)
                await self.tg.send(f"🔄 Recovered {sym} {rpos['side'].upper()} @ {entry:.5f}")

            # در استارت: پوزیشن‌های DB که روی صرافی نیستند را سریع ghost نکن — فقط اگر load_from_db
            if load_from_db:
                db_opens = await self.db.get_open_trades()
                for t in db_opens:
                    if t["symbol"] in remote_map:
                        continue
                    # در دیتابیس باز است ولی روی صرافی نیست → ببند
                    await self.db.close_trade(t["id"], 0.0, reason="startup_ghost", hold=0)
                    log.info(f"Startup ghost closed in DB: {t['symbol']}")

            with STATE_LOCK:
                SHARED_STATE["last_sync"] = time.strftime("%H:%M:%S")
            log.info(f"Sync done active={len(SHARED_STATE['active_positions'])}")
        except Exception as e:
            log.error(f"smart_sync: {e}")
            self._record_error(f"smart_sync: {e}")

    async def force_close(self, pid: str, reason: str):
        with STATE_LOCK:
            pos = SHARED_STATE["active_positions"].get(pid)
        if not pos:
            return
        price = self.prices.get(pos["symbol"]) or await self.get_live_price(pos["symbol"]) or pos["entry"]
        hold = time.time() - self.open_times.get(pid, time.time())
        try:
            close_side = "sell" if pos["side"] == "buy" else "buy"
            await self.ex.create_market_order(pos["symbol"], close_side, pos["qty"], params={"reduceOnly": True})
            raw_pnl = (price - pos["entry"]) * pos["qty"] * (1 if pos["side"] == "buy" else -1)
            fees = abs(price * pos["qty"]) * TAKER_FEE * 2 * FEE_BUFFER
            net = raw_pnl - fees
            if pos["strategy"] != "RealTest":
                await self.db.close_trade(pid, net, fees, reason, hold)
            with STATE_LOCK:
                SHARED_STATE["active_positions"].pop(pid, None)
            self.open_times.pop(pid, None)
            POSITION_MISS_COUNT.pop(pid, None)
            await self.db.update_analytics()
            SYMBOL_POST_CLOSE_COOLDOWN[pos["symbol"]] = time.time() + POST_CLOSE_COOLDOWN
            await self.tg.send(f"{'🟢' if net >= 0 else '🔴'} closed ({reason}) | ${net:.2f}", self.tg.menu())
        except Exception as e:
            err = str(e)
            if any(x in err.lower() for x in ("not found", "39999", "reduce", "no position")):
                await self.db.close_trade(pid, 0.0, 0, f"ghost_{reason}", hold)
                with STATE_LOCK:
                    SHARED_STATE["active_positions"].pop(pid, None)
                self.open_times.pop(pid, None)
                POSITION_MISS_COUNT.pop(pid, None)
                await self.tg.send(f"👻 local close {pos['symbol']}")
            else:
                log.error(f"force_close: {e}")
                self._record_error(f"force_close: {e}")

    async def watchdog_loop(self):
        while True:
            with STATE_LOCK:
                items = list(SHARED_STATE["active_positions"].items())
            now = time.time()
            for pid, pos in items:
                if pos["strategy"] == "RealTest":
                    continue
                price = self.prices.get(pos["symbol"]) or await self.get_live_price(pos["symbol"])
                if not price:
                    continue
                hold = now - self.open_times.get(pid, now)
                if hold >= MAX_HOLD_SECONDS:
                    await self.force_close(pid, "MaxHold_4h")
                    continue
                can_partial = hold >= MIN_HOLD_FOR_PARTIAL
                can_trail = hold >= MIN_HOLD_FOR_TRAIL
                pnl_pct = ((price - pos["entry"]) / pos["entry"] * 100 if pos["side"] == "buy"
                           else (pos["entry"] - price) / pos["entry"] * 100)
                if can_trail and pnl_pct > TRAIL_ACT and pnl_pct > pos["highest_pnl_pct"]:
                    pos["highest_pnl_pct"] = pnl_pct
                    new_sl = price * (1 - TRAIL_STEP / 100) if pos["side"] == "buy" else price * (1 + TRAIL_STEP / 100)
                    if (pos["side"] == "buy" and new_sl > pos["sl"]) or (pos["side"] == "sell" and new_sl < pos["sl"]):
                        pos["sl"] = new_sl
                        await self.db.update_trade(pid, pos["qty"], pos["sl"], pos["is_partial"], pos["highest_pnl_pct"])
                if PARTIAL_TP and pos["is_partial"] == 0 and can_partial:
                    hit_tp1 = ((pos["side"] == "buy" and price >= pos["tp1"]) or (pos["side"] == "sell" and price <= pos["tp1"]))
                    if hit_tp1 and pnl_pct >= MIN_PROFIT_FOR_BE:
                        try:
                            half = float(self.ex.amount_to_precision(pos["symbol"], pos["qty"] / 2))
                            if half > 0:
                                close_side = "sell" if pos["side"] == "buy" else "buy"
                                await self.ex.create_market_order(pos["symbol"], close_side, half, params={"reduceOnly": True})
                                pos["qty"] -= half
                                pos["is_partial"] = 1
                                pos["sl"] = pos["entry"]
                                await self.db.update_trade(pid, pos["qty"], pos["sl"], 1, pos["highest_pnl_pct"])
                                await self.tg.send(f"🔹 Partial TP {pos['symbol']}")
                        except Exception as e:
                            log.error(f"partial: {e}")
                sl_hit = (pos["side"] == "buy" and price <= pos["sl"]) or (pos["side"] == "sell" and price >= pos["sl"])
                tp_hit = (pos["side"] == "buy" and price >= pos["tp"]) or (pos["side"] == "sell" and price <= pos["tp"])
                if sl_hit or tp_hit:
                    reason = "SL" if sl_hit else "TP"
                    if sl_hit and pos.get("is_partial") == 1 and abs(pos["sl"] - pos["entry"]) < 1e-8:
                        reason = "BE"
                    elif sl_hit and pos.get("highest_pnl_pct", 0) > TRAIL_ACT:
                        reason = "Trail"
                    await self.force_close(pid, reason)
            await asyncio.sleep(1.8)

# ===================== WEB =====================
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
<head><meta charset="UTF-8"><title>Quant v16.16</title>
<style>
body{font-family:system-ui;background:#0d1117;color:#c9d1d9;padding:20px}
h1{color:#58a6ff}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px}
.card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px}
.value{font-size:1.4rem;font-weight:700;color:#58a6ff}
</style></head>
<body>
<h1>🚀 Master Quant v16.16</h1>
<div class="grid">
<div class="card">موجودی<div class="value" id="bal">0</div></div>
<div class="card">پوزیشن<div class="value" id="pos">0</div></div>
<div class="card">PnL<div class="value" id="pnl">0</div></div>
<div class="card">WR<div class="value" id="wr">0%</div></div>
</div>
<p>اسکن: <span id="scan">–</span></p>
<script>
async function r(){try{const d=await(await fetch('/api/status')).json();
document.getElementById('bal').textContent=(d.balance||0).toFixed(2);
document.getElementById('pos').textContent=Object.keys(d.active_positions||{}).length;
document.getElementById('pnl').textContent=(d.stats?.total_pnl||0).toFixed(2);
document.getElementById('wr').textContent=(d.stats?.win_rate||0)+'%';
document.getElementById('scan').textContent=d.last_scan||'–';}catch(e){}}
r();setInterval(r,5000);
</script></body></html>
""")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    def run_web():
        try:
            print(f"Flask on 0.0.0.0:{port}", flush=True)
            app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
        except Exception as e:
            print("Flask error:", e, flush=True)
            traceback.print_exc()
    try:
        print("=== Master Quant v16.16 Execution Fix starting ===", flush=True)
        Thread(target=run_web, daemon=True).start()
        time.sleep(1)
        engine = QuantEngine()
        print("Engine ready", flush=True)
        asyncio.run(engine.start())
    except Exception as e:
        print("FATAL:", e, flush=True)
        traceback.print_exc()
        time.sleep(20)
        raise