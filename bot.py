#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v12.3 - Stable Phemex Fix
- Direct HTTP for OHLCV (solves 30000 permanently)
- Clean startup, no early exit
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

load_dotenv()

API_KEY = os.getenv("PHEMEX_API_KEY", "")
API_SECRET = os.getenv("PHEMEX_API_SECRET", "")
TESTNET = os.getenv("PHEMEX_TESTNET", "False").lower() in ("true", "1", "yes")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
WEB_USER = os.getenv("WEB_ADMIN_USER", "") or "admin"
WEB_PASS = os.getenv("WEB_ADMIN_PASS", "") or "admin123"

SYMBOLS = ["ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT", "XRP/USDT:USDT", "ADA/USDT:USDT", "DOT/USDT:USDT"]
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s",
                    handlers=[logging.FileHandler("quant_bot.log"), logging.StreamHandler()])
log = logging.getLogger("QuantV12.3")

SHARED_STATE = {
    "is_active": True, "dd_halted": False, "daily_halted": False,
    "balance": 0.0, "peak_balance": 0.0, "day_start_balance": 0.0,
    "current_dd": 0.0, "daily_pnl": 0.0, "active_positions": {},
    "last_scan": "Never", "stats": {"total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0},
}
STATE_LOCK = asyncio.Lock()

# ---------- DB ----------
class AsyncDB:
    def __init__(self): self.db_path = "bot_v12.db"
    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY, symbol TEXT, side TEXT, strategy TEXT, entry_price REAL, qty REAL,
                original_qty REAL, sl REAL, tp1 REAL, tp REAL, is_partial INTEGER DEFAULT 0,
                highest_pnl_pct REAL DEFAULT 0, status TEXT DEFAULT 'open', pnl REAL DEFAULT 0,
                fees_est REAL DEFAULT 0, exit_reason TEXT, hold_seconds REAL DEFAULT 0,
                opened_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT DEFAULT CURRENT_TIMESTAMP,
                symbol TEXT, action TEXT, strategy TEXT, reason TEXT, price REAL, rsi REAL, atr REAL, htf_trend TEXT)""")
            await db.commit()
    async def insert_trade(self, t):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO trades (id,symbol,side,strategy,entry_price,qty,original_qty,sl,tp1,tp) VALUES (?,?,?,?,?,?,?,?,?,?)",
                             (t["id"],t["symbol"],t["side"],t["strategy"],t["entry"],t["qty"],t["qty"],t["sl"],t["tp1"],t["tp"]))
            await db.commit()
    async def update_trade(self, tid, qty, sl, partial, hp):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE trades SET qty=?,sl=?,is_partial=?,highest_pnl_pct=? WHERE id=?", (qty,sl,partial,hp,tid))
            await db.commit()
    async def close_trade(self, tid, pnl, fees=0.0, reason="", hold=0.0):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE trades SET status='closed',pnl=?,fees_est=?,exit_reason=?,hold_seconds=?,closed_at=CURRENT_TIMESTAMP WHERE id=?",
                             (pnl,fees,reason,hold,tid))
            await db.commit()
    async def log_decision(self, symbol, action, strategy, reason, price=0, rsi=0, atr=0, htf=""):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO decisions (symbol,action,strategy,reason,price,rsi,atr,htf_trend) VALUES (?,?,?,?,?,?,?,?)",
                             (symbol,action,strategy,reason,price,rsi,atr,htf))
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
                if rows:
                    pnls = [r[0] for r in rows]
                    wins = len([p for p in pnls if p > 0])
                    async with STATE_LOCK:
                        SHARED_STATE["stats"] = {"total_trades": len(pnls), "win_rate": round(wins/len(pnls)*100,1), "total_pnl": round(sum(pnls),2)}
    async def get_recent_decisions(self, limit=200):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM decisions ORDER BY id DESC LIMIT ?", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]
    async def get_closed_trades(self, limit=100):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

# ---------- Indicators & Strategy ----------
class Indicators:
    @staticmethod
    def rsi(close, n=14):
        delta = close.diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        rs = up.ewm(com=n-1, adjust=False).mean() / down.ewm(com=n-1, adjust=False).mean().replace(0, 1e-10)
        return 100 - (100 / (1 + rs))
    @staticmethod
    def atr(df, n=14):
        tr = pd.concat([df["high"]-df["low"], (df["high"]-df["close"].shift()).abs(), (df["low"]-df["close"].shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()
    @staticmethod
    def supertrend(df, period=10, mult=3.0):
        atr = Indicators.atr(df, period)
        hl2 = (df["high"] + df["low"]) / 2
        upper, lower = hl2 + mult*atr, hl2 - mult*atr
        direction = pd.Series(1, index=df.index)
        for i in range(1, len(df)):
            if df["close"].iloc[i] > upper.iloc[i-1]: direction.iloc[i] = 1
            elif df["close"].iloc[i] < lower.iloc[i-1]: direction.iloc[i] = -1
            else:
                direction.iloc[i] = direction.iloc[i-1]
                if direction.iloc[i]==1 and lower.iloc[i] < lower.iloc[i-1]: lower.iloc[i] = lower.iloc[i-1]
                if direction.iloc[i]==-1 and upper.iloc[i] > upper.iloc[i-1]: upper.iloc[i] = upper.iloc[i-1]
        return direction, upper, lower
    @staticmethod
    def sma(s, p): return s.rolling(p).mean()
    @staticmethod
    def highest(s, p): return s.rolling(p).max()
    @staticmethod
    def lowest(s, p): return s.rolling(p).min()

class StrategyEngine:
    def analyze(self, df5, df1h):
        df = df5.iloc[:-1].copy()
        htf = df1h.iloc[:-1].copy()
        if len(df) < 60 or len(htf) < 40:
            return {"action": "neutral", "reason": "داده کم", "strat": "", "rsi": 0, "atr": 0, "htf": ""}
        hclose = htf["close"]
        e50 = hclose.ewm(span=50, adjust=False).mean().iloc[-1]
        e200 = hclose.ewm(span=min(200, len(htf)), adjust=False).mean().iloc[-1]
        hp = hclose.iloc[-1]
        htf_trend = "bullish" if hp > e50 > e200*0.998 else ("bearish" if hp < e50 < e200*1.002 else "sideways")
        if htf_trend == "sideways":
            return {"action": "neutral", "reason": "روند HTF نامشخص", "strat": "", "rsi": 0, "atr": 0, "htf": "sideways"}
        c, high, low, vol = df["close"], df["high"], df["low"], df["volume"]
        price = float(c.iloc[-1])
        atrs = Indicators.atr(df, 14)
        atr = float(atrs.iloc[-1])
        if atr <= 0: return {"action": "neutral", "reason": "ATR صفر", "strat": "", "rsi": 0, "atr": 0, "htf": htf_trend}
        atr_sma = float(Indicators.sma(atrs, 20).iloc[-1])
        if atr < atr_sma * (0.45 if RELAXED_MODE else 0.55) or atr > atr_sma * (3.2 if RELAXED_MODE else 2.8):
            return {"action": "neutral", "reason": f"نوسان نامناسب ATR={atr:.5f}", "strat": "", "rsi": 0, "atr": atr, "htf": htf_trend}
        rsi_s = Indicators.rsi(c)
        rsi, rsi_p = float(rsi_s.iloc[-1]), float(rsi_s.iloc[-2])
        ema20 = float(c.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = float(c.ewm(span=50, adjust=False).mean().iloc[-1])
        st_d, st_u, st_l = Indicators.supertrend(df)
        vsma = float(Indicators.sma(vol, 20).iloc[-1])
        vcur = float(vol.iloc[-1])
        h10 = float(Indicators.highest(high, 10).iloc[-1])
        l10 = float(Indicators.lowest(low, 10).iloc[-1])
        # Breakout
        if htf_trend=="bullish" and price>ema20 and price>=h10*0.999 and 48<rsi<75 and vcur>vsma*(1.15 if RELAXED_MODE else 1.3):
            return self._sig("buy", "Breakout_Momentum", price, atr, rsi, htf_trend)
        if htf_trend=="bearish" and price<ema20 and price<=l10*1.001 and 25<rsi<52 and vcur>vsma*(1.15 if RELAXED_MODE else 1.3):
            return self._sig("sell", "Breakout_Momentum", price, atr, rsi, htf_trend)
        # Pullback
        if htf_trend=="bullish" and price>ema20>ema50*0.999 and rsi_p<=(42 if RELAXED_MODE else 40) and rsi>rsi_p and rsi<62:
            return self._sig("buy", "MTF_Pullback", price, atr, rsi, htf_trend)
        if htf_trend=="bearish" and price<ema20<ema50*1.001 and rsi_p>=(58 if RELAXED_MODE else 60) and rsi<rsi_p and rsi>38:
            return self._sig("sell", "MTF_Pullback", price, atr, rsi, htf_trend)
        # SuperTrend
        if htf_trend=="bullish" and st_d.iloc[-1]==1 and low.iloc[-1]<=st_l.iloc[-1]*1.005 and c.iloc[-1]>c.iloc[-2] and 38<rsi<65:
            return self._sig("buy", "SuperTrend_Pullback", price, atr, rsi, htf_trend)
        if htf_trend=="bearish" and st_d.iloc[-1]==-1 and high.iloc[-1]>=st_u.iloc[-1]*0.995 and c.iloc[-1]<c.iloc[-2] and 35<rsi<62:
            return self._sig("sell", "SuperTrend_Pullback", price, atr, rsi, htf_trend)
        # Volume
        if htf_trend=="bullish" and price>ema20 and vcur>vsma*(1.5 if RELAXED_MODE else 1.8) and c.iloc[-1]>c.iloc[-2] and 48<rsi<70:
            return self._sig("buy", "Volume_Surge", price, atr, rsi, htf_trend)
        if htf_trend=="bearish" and price<ema20 and vcur>vsma*(1.5 if RELAXED_MODE else 1.8) and c.iloc[-1]<c.iloc[-2] and 30<rsi<52:
            return self._sig("sell", "Volume_Surge", price, atr, rsi, htf_trend)
        return {"action": "neutral", "reason": f"بدون سیگنال (RSI={rsi:.1f})", "strat": "", "rsi": rsi, "atr": atr, "htf": htf_trend}
    def _sig(self, side, strat, price, atr, rsi, htf):
        slm, tpm, tp1m = 1.5, 2.8, 1.4
        if strat == "Breakout_Momentum": slm, tpm, tp1m = 1.25, 3.2, 1.8
        if strat == "Volume_Surge": slm, tpm, tp1m = 1.35, 2.4, 1.4
        if side == "buy":
            return {"action": side, "strat": strat, "sl": price-atr*slm, "tp": price+atr*tpm, "tp1": price+atr*tp1m, "reason": f"سیگنال {strat}", "rsi": rsi, "atr": atr, "htf": htf}
        return {"action": side, "strat": strat, "sl": price+atr*slm, "tp": price-atr*tpm, "tp1": price-atr*tp1m, "reason": f"سیگنال {strat}", "rsi": rsi, "atr": atr, "htf": htf}

# ---------- AI Observer ----------
class AIObserver:
    def __init__(self, db): self.db = db
    async def generate_report(self):
        decs = await self.db.get_recent_decisions(300)
        closed = await self.db.get_closed_trades(80)
        lines = ["🤖 <b>AI Observer Report</b>\n"]
        if decs:
            reasons = Counter()
            neu = sig = 0
            for d in decs:
                if d["action"] == "neutral":
                    neu += 1
                    reasons[(d["reason"] or "?")[:60]] += 1
                else: sig += 1
            lines.append(f"از {len(decs)} تصمیم → سیگنال: {sig} | رد: {neu}")
            for r, c in reasons.most_common(5):
                lines.append(f"• {c}× {r}")
        if closed:
            pnls = [t["pnl"] for t in closed]
            wins = sum(1 for p in pnls if p > 0)
            lines.append(f"\nمعاملات بسته: {len(closed)} | برد: {wins}")
        else:
            lines.append("\nهنوز معامله بسته‌شده‌ای نیست.")
        lines.append("\n💡 داده در حال جمع‌آوری است.")
        return "\n".join(lines)

# ---------- Telegram ----------
class AsyncTelegram:
    def __init__(self, engine):
        self.engine = engine
        self.base = f"https://api.telegram.org/bot{TG_TOKEN}"
        self.offset = 0
    def menu(self):
        btn = "⏸️ Pause" if SHARED_STATE["is_active"] else "▶️ Start"
        act = "cmd_pause" if SHARED_STATE["is_active"] else "cmd_start"
        return {"inline_keyboard": [
            [{"text": "📊 Dash", "callback_data": "cmd_dash"}, {"text": "💼 Pos", "callback_data": "cmd_pos"}],
            [{"text": "🔄 Sync", "callback_data": "cmd_sync"}, {"text": btn, "callback_data": act}],
            [{"text": "🤖 Observer", "callback_data": "cmd_obs"}, {"text": "🚫 Rejects", "callback_data": "cmd_rej"}],
        ]}
    async def send(self, msg, markup=None):
        if not TG_TOKEN: return
        if len(msg) > 4000: msg = msg[:3900] + "..."
        payload = {"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}
        if markup: payload["reply_markup"] = markup
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"{self.base}/sendMessage", json=payload, timeout=12)
        except Exception as e: log.error(f"TG send: {e}")
    async def poll(self):
        if not TG_TOKEN: return
        await self.send(f"🚀 Quant V12.3 Online\n{'TESTNET' if TESTNET else 'MAINNET'}", self.menu())
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"{self.base}/getUpdates?offset={self.offset+1}&timeout=8") as r:
                        data = await r.json()
                        for u in data.get("result", []):
                            self.offset = u["update_id"]
                            if "callback_query" in u:
                                cb = u["callback_query"]
                                d = cb["data"]
                                try:
                                    async with aiohttp.ClientSession() as ss:
                                        await ss.post(f"{self.base}/answerCallbackQuery", json={"callback_query_id": cb["id"], "text": "OK"}, timeout=4)
                                except: pass
                                if d == "cmd_start":
                                    async with STATE_LOCK: SHARED_STATE["is_active"] = True
                                    await self.send("▶️ Started", self.menu())
                                elif d == "cmd_pause":
                                    async with STATE_LOCK: SHARED_STATE["is_active"] = False
                                    await self.send("⏸️ Paused", self.menu())
                                elif d == "cmd_dash":
                                    async with STATE_LOCK: st = dict(SHARED_STATE)
                                    await self.send(f"Balance: ${st['balance']:.2f}\nDD: {st['current_dd']:.1f}%\nPos: {len(st['active_positions'])}\nPnL: ${st['stats']['total_pnl']:.2f}", self.menu())
                                elif d == "cmd_pos":
                                    async with STATE_LOCK: pos = dict(SHARED_STATE["active_positions"])
                                    if not pos: await self.send("No positions", self.menu())
                                    else:
                                        for pid, p in pos.items():
                                            pr = self.engine.prices.get(p["symbol"], p["entry"])
                                            pnl = (pr - p["entry"]) * p["qty"] * (1 if p["side"]=="buy" else -1)
                                            await self.send(f"{p['symbol']} {p['side']} PnL ${pnl:.2f}")
                                elif d == "cmd_sync":
                                    await self.engine.smart_sync_positions()
                                    await self.send("Synced", self.menu())
                                elif d == "cmd_obs":
                                    await self.send(await self.engine.observer.generate_report(), self.menu())
                                elif d == "cmd_rej":
                                    decs = await self.engine.db.get_recent_decisions(15)
                                    msg = "Last decisions:\n"
                                    for x in decs:
                                        msg += f"{'✅' if x['action']!='neutral' else '⛔'} {x['symbol']}: {x['reason'][:70]}\n"
                                    await self.send(msg or "No data", self.menu())
            except Exception as e: log.error(f"TG poll: {e}")
            await asyncio.sleep(1)

# ---------- Engine ----------
class QuantEngine:
    def __init__(self):
        self.db = AsyncDB()
        self.strategy = StrategyEngine()
        self.observer = AIObserver(self.db)
        self.tg = AsyncTelegram(self)
        self.ex = ccxt.phemex({"apiKey": API_KEY, "secret": API_SECRET, "enableRateLimit": True, "options": {"defaultType": "swap"}})
        self.ex.set_sandbox_mode(TESTNET)
        self.prices = {}
        self.open_times = {}
        self.loop_count = 0
        self.base_url = "https://api.phemex.com" if not TESTNET else "https://testnet-api.phemex.com"

    async def fetch_ohlcv_direct(self, symbol: str, timeframe: str, limit: int = 100) -> list:
        """درخواست مستقیم به Phemex – حل قطعی 30000"""
        try:
            # تبدیل نماد به فرمت Phemex (ETHUSDT)
            market = self.ex.market(symbol)
            sym_id = market["id"]
            res_map = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
            resolution = res_map.get(timeframe, 300)
            url = f"{self.base_url}/md/v2/kline"
            params = {"symbol": sym_id, "resolution": resolution, "limit": limit}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as resp:
                    data = await resp.json()
            rows = []
            if isinstance(data, dict):
                rows = data.get("data", {}).get("rows") or data.get("data") or data.get("result") or []
            elif isinstance(data, list):
                rows = data
            ohlcv = []
            for row in rows:
                if isinstance(row, (list, tuple)) and len(row) >= 5:
                    ts = int(row[0])
                    if ts < 1e12: ts *= 1000
                    ohlcv.append([ts, float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5] if len(row)>5 else 0)])
            return ohlcv[-limit:] if ohlcv else []
        except Exception as e:
            log.warning(f"Direct OHLCV {symbol}: {e}")
            return []

    async def start(self):
        await self.db.init_db()
        try:
            await self.ex.load_markets()
            log.info("Markets loaded")
            for sym in SYMBOLS:
                try:
                    await self.ex.set_leverage(LEVERAGE, sym)
                    log.info(f"Leverage OK → {sym}")
                except Exception as e:
                    log.warning(f"Leverage {sym}: {e}")
        except Exception as e:
            log.error(f"Load markets: {e}")

        for t in await self.db.get_open_trades():
            async with STATE_LOCK:
                SHARED_STATE["active_positions"][t["id"]] = {
                    "id": t["id"], "symbol": t["symbol"], "side": t["side"], "strategy": t["strategy"],
                    "entry": t["entry_price"], "qty": t["qty"], "sl": t["sl"], "tp": t["tp"], "tp1": t["tp1"],
                    "is_partial": t.get("is_partial", 0), "highest_pnl_pct": t.get("highest_pnl_pct", 0)
                }
                self.open_times[t["id"]] = time.time()

        await self.smart_sync_positions()
        try:
            bal = await self.ex.fetch_balance()
            usdt = float(bal.get("USDT", {}).get("total", 0) or 0)
            async with STATE_LOCK:
                SHARED_STATE["balance"] = usdt
                SHARED_STATE["peak_balance"] = max(SHARED_STATE.get("peak_balance", 0), usdt)
                if SHARED_STATE["day_start_balance"] <= 0:
                    SHARED_STATE["day_start_balance"] = usdt
        except: pass

        await asyncio.gather(self.price_loop(), self.scan_loop(), self.watchdog_loop(), self.tg.poll())

    async def price_loop(self):
        while True:
            try:
                tickers = await self.ex.fetch_tickers(SYMBOLS)
                for s, d in tickers.items():
                    if d.get("last"): self.prices[s] = float(d["last"])
                bal = await self.ex.fetch_balance()
                cur = float(bal.get("USDT", {}).get("total", 0) or 0)
                async with STATE_LOCK:
                    SHARED_STATE["balance"] = cur
                    if cur > SHARED_STATE["peak_balance"]: SHARED_STATE["peak_balance"] = cur
                    peak = SHARED_STATE["peak_balance"]
                    if peak > 0:
                        dd = (peak - cur) / peak * 100
                        SHARED_STATE["current_dd"] = dd
                        if dd >= MAX_DD: SHARED_STATE["dd_halted"] = True
                        elif dd < MAX_DD * 0.7: SHARED_STATE["dd_halted"] = False
            except Exception as e: log.error(f"price: {e}")
            await asyncio.sleep(3)

    async def scan_loop(self):
        while True:
            self.loop_count += 1
            async with STATE_LOCK:
                can = SHARED_STATE["is_active"] and not SHARED_STATE["dd_halted"] and not SHARED_STATE["daily_halted"] and len(SHARED_STATE["active_positions"]) < MAX_POS
            if not can:
                await asyncio.sleep(8)
                continue
            async with STATE_LOCK: SHARED_STATE["last_scan"] = time.strftime("%H:%M:%S")
            for sym in SYMBOLS:
                async with STATE_LOCK:
                    if any(p["symbol"] == sym for p in SHARED_STATE["active_positions"].values()): continue
                try:
                    raw5 = await self.fetch_ohlcv_direct(sym, TIMEFRAME, 120)
                    await asyncio.sleep(0.2)
                    raw1 = await self.fetch_ohlcv_direct(sym, HTF_TIMEFRAME, 80)
                    if not raw5 or len(raw5) < 50:
                        await self.db.log_decision(sym, "neutral", "", "OHLCV خالی")
                        continue
                    df5 = pd.DataFrame(raw5, columns=["ts","open","high","low","close","volume"])
                    df1 = pd.DataFrame(raw1, columns=["ts","open","high","low","close","volume"]) if raw1 and len(raw1)>20 else df5
                    sig = self.strategy.analyze(df5, df1)
                    await self.db.log_decision(sym, sig["action"], sig.get("strat",""), sig.get("reason",""),
                                               self.prices.get(sym,0), sig.get("rsi",0), sig.get("atr",0), sig.get("htf",""))
                    if sig["action"] != "neutral":
                        await self.execute_trade(sym, sig)
                except Exception as e:
                    log.error(f"scan {sym}: {e}")
                await asyncio.sleep(0.4)
            await asyncio.sleep(18)

    async def execute_trade(self, sym, sig):
        price = self.prices.get(sym)
        async with STATE_LOCK: bal = SHARED_STATE["balance"]
        if not price or bal < 15: return
        dist = abs(price - sig["sl"])
        if dist <= 0: return
        target = (bal * RISK_PCT / 100 / dist) * price * 0.9
        try:
            bal_data = await self.ex.fetch_balance()
            free = float(bal_data.get("USDT", {}).get("free", 0) or 0)
            qty = float(self.ex.amount_to_precision(sym, min(target, free*0.15*LEVERAGE) / price))
            if qty * price < MIN_ORDER_USD: return
            order = await self.ex.create_market_order(sym, sig["action"], qty)
            fill = float(order.get("average") or price)
            pid = f"pos_{uuid.uuid4().hex[:8]}"
            pos = {"id": pid, "symbol": sym, "side": sig["action"], "strategy": sig["strat"],
                   "entry": fill, "qty": qty, "sl": sig["sl"], "tp": sig["tp"], "tp1": sig["tp1"],
                   "is_partial": 0, "highest_pnl_pct": 0.0}
            async with STATE_LOCK: SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()
            await self.db.insert_trade(pos)
            await self.tg.send(f"🎯 {sig['action'].upper()} {sig['strat']}\n{sym} @ {fill:.4f}")
        except Exception as e:
            log.error(f"execute: {e}")
            await self.db.log_decision(sym, "rejected", sig.get("strat",""), str(e)[:80])

    async def smart_sync_positions(self):
        try:
            remote = await self.ex.fetch_positions()
            active = set()
            for p in remote:
                size = abs(float(p.get("contracts") or 0))
                if size <= 0: continue
                raw = p.get("symbol", "")
                matched = next((s for s in SYMBOLS if s.split("/")[0] in raw), None)
                if matched: active.add(matched)
            async with STATE_LOCK:
                for pid in list(SHARED_STATE["active_positions"]):
                    if SHARED_STATE["active_positions"][pid]["symbol"] not in active and SHARED_STATE["active_positions"][pid]["strategy"] != "LiveTest":
                        await self.db.close_trade(pid, 0.0, reason="remote")
                        del SHARED_STATE["active_positions"][pid]
        except Exception as e: log.error(f"sync: {e}")

    async def force_close_position(self, pid, reason):
        async with STATE_LOCK: pos = SHARED_STATE["active_positions"].get(pid)
        if not pos: return
        price = self.prices.get(pos["symbol"], pos["entry"])
        hold = time.time() - self.open_times.get(pid, time.time())
        try:
            side = "sell" if pos["side"] == "buy" else "buy"
            await self.ex.create_market_order(pos["symbol"], side, pos["qty"], params={"reduceOnly": True})
            raw = (price - pos["entry"]) * pos["qty"] * (1 if pos["side"]=="buy" else -1)
            fees = abs(raw) * TAKER_FEE * 2 * FEE_BUFFER
            net = raw - fees
            if pos["strategy"] != "LiveTest":
                await self.db.close_trade(pid, net, fees, reason, hold)
            async with STATE_LOCK: SHARED_STATE["active_positions"].pop(pid, None)
            self.open_times.pop(pid, None)
            await self.db.update_analytics()
            await self.tg.send(f"{'🟢' if net>=0 else '🔴'} Closed {pos['symbol']} Net ${net:.2f}")
        except Exception as e: log.error(f"close: {e}")

    async def watchdog_loop(self):
        while True:
            async with STATE_LOCK: items = list(SHARED_STATE["active_positions"].items())
            for pid, pos in items:
                if pos["strategy"] == "LiveTest": continue
                price = self.prices.get(pos["symbol"])
                if not price: continue
                pnl_pct = ((price-pos["entry"])/pos["entry"]*100) if pos["side"]=="buy" else ((pos["entry"]-price)/pos["entry"]*100)
                if pnl_pct > TRAIL_ACT and pnl_pct > pos["highest_pnl_pct"]:
                    pos["highest_pnl_pct"] = pnl_pct
                    new_sl = price*(1-TRAIL_STEP/100) if pos["side"]=="buy" else price*(1+TRAIL_STEP/100)
                    if (pos["side"]=="buy" and new_sl > pos["sl"]) or (pos["side"]=="sell" and new_sl < pos["sl"]):
                        pos["sl"] = new_sl
                        await self.db.update_trade(pid, pos["qty"], pos["sl"], pos["is_partial"], pos["highest_pnl_pct"])
                if PARTIAL_TP and pos["is_partial"]==0:
                    hit = (pos["side"]=="buy" and price>=pos["tp1"]) or (pos["side"]=="sell" and price<=pos["tp1"])
                    if hit:
                        try:
                            half = float(self.ex.amount_to_precision(pos["symbol"], pos["qty"]/2))
                            if half > 0:
                                side = "sell" if pos["side"]=="buy" else "buy"
                                await self.ex.create_market_order(pos["symbol"], side, half, params={"reduceOnly": True})
                                pos["qty"] -= half
                                pos["is_partial"] = 1
                                pos["sl"] = pos["entry"]
                                await self.db.update_trade(pid, pos["qty"], pos["sl"], 1, pos["highest_pnl_pct"])
                        except: pass
                sl_hit = (pos["side"]=="buy" and price<=pos["sl"]) or (pos["side"]=="sell" and price>=pos["sl"])
                tp_hit = (pos["side"]=="buy" and price>=pos["tp"]) or (pos["side"]=="sell" and price<=pos["tp"])
                if sl_hit or tp_hit:
                    await self.force_close_position(pid, "SL/Trail" if sl_hit else "TP")
            await asyncio.sleep(1.5)

# ---------- Web ----------
app = Flask(__name__)
auth = HTTPBasicAuth()
@auth.verify_password
def verify(u, p): return u == WEB_USER and p == WEB_PASS
@app.before_request
@auth.login_required
def req(): pass
@app.route("/api/status")
def status(): return jsonify(SHARED_STATE)
@app.route("/")
def home():
    return render_template_string("""<!DOCTYPE html><html><head><title>V12.3</title>
<style>body{font-family:sans-serif;background:#0d1117;color:#c9d1d9;padding:20px}
.card{background:#161b22;padding:15px;border-radius:8px;margin:10px 0}</style></head>
<body><h1>Quant V12.3</h1><div class="card">Status: ONLINE (Direct OHLCV)</div>
<div class="card">Balance: <span id="b">-</span> | Pos: <span id="p">0</span></div>
<script>setInterval(async()=>{const d=await(await fetch('/api/status')).json();
document.getElementById('b').textContent=(d.balance||0).toFixed(2);
document.getElementById('p').textContent=Object.keys(d.active_positions||{}).length;},5000)</script></body></html>""")
def run_web():
    app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    engine = QuantEngine()
    try:
        asyncio.run(engine.start())
    except Exception as e:
        log.error(f"Fatal: {e}")
        raise