#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Trading Bot Pro v7.0.0 - Interactive Telegram Control & Advanced Analytics
ویژگی‌ها:
- پنل کنترل کامل تلگرام (دستورات start, stop, dashboard, positions)
- داشبورد تحلیلی عمیق (Win Rate, Profit Factor, Max Drawdown, Avg Win/Loss)
- مدیریت هوشمند افت حساب (ممانعت از ورود به معاملات جدید + ادامه مدیریت پوزیشن‌های باز)
- هماهنگ‌سازی پوزیشن‌های باز صرافی و دیتابیس
"""

import os
import sys
import re
import json
import time
import uuid
import logging
import threading
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

# ── بررسی نسخه پایتون ──────────────────────────────────────────────────
if sys.version_info < (3, 10):
    print("[CRITICAL] Python 3.10+ لازم است")
    sys.exit(1)

import pandas as pd
import numpy as np
import requests
import ccxt

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", stream=sys.stdout)
log = logging.getLogger("Bot")

# ============================================================================
# CONFIG
# ============================================================================
class Cfg:
    @staticmethod
    def s(k: str, d: str = "") -> str: return os.getenv(k, d).strip()
    @staticmethod
    def f(k: str, d: float) -> float:
        try: return float(os.getenv(k, str(d)).strip())
        except: return d
    @staticmethod
    def b(k: str, d: bool = False) -> bool:
        return os.getenv(k, "true" if d else "false").strip().lower() in ("1", "true", "yes", "on")

API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")
DB_URL     = Cfg.s("DATABASE_URL")

SYMBOLS = [
    "BTC/USDT:USDT","ETH/USDT:USDT","SOL/USDT:USDT","XRP/USDT:USDT",
    "BNB/USDT:USDT","DOGE/USDT:USDT","ADA/USDT:USDT","AVAX/USDT:USDT"
]

RISK_PCT   = Cfg.f("RISK_PER_TRADE", 1.5)
MAX_DD     = Cfg.f("MAX_DRAWDOWN", 10.0)
MAX_POS    = 3
DRY_RUN    = Cfg.b("DRY_RUN", True)
TESTNET    = Cfg.b("PHEMEX_TESTNET", False)
PORT       = 10000

# ============================================================================
# INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close: pd.Series, n: int = 14) -> pd.Series:
        delta = close.diff()
        up   = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        rs   = up.ewm(com=n-1, adjust=False).mean() / (down.ewm(com=n-1, adjust=False).mean() + 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def ema(close: pd.Series, n: int) -> pd.Series:
        return close.ewm(span=n, adjust=False).mean()

    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
        up = high.diff()
        down = -low.diff()
        plus_dm = np.where((up > down) & (up > 0), up, 0.0)
        minus_dm = np.where((down > up) & (down > 0), down, 0.0)
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        atr_val = tr.ewm(com=n-1, adjust=False).mean()
        plus_di = 100 * (pd.Series(plus_dm).ewm(com=n-1, adjust=False).mean() / (atr_val + 1e-10))
        minus_di = 100 * (pd.Series(minus_dm).ewm(com=n-1, adjust=False).mean() / (atr_val + 1e-10))
        dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10))
        return dx.ewm(com=n-1, adjust=False).mean()

    @staticmethod
    def bbands(close: pd.Series, n: int = 20, std: float = 2.0):
        mid = close.rolling(n).mean()
        sd  = close.rolling(n).std()
        return mid - std*sd, mid, mid + std*sd

    @staticmethod
    def safe(s, idx: int = -1) -> float:
        try:
            if s is None: return 0.0
            v = s.iloc[idx]
            return float(v) if not (v != v) else 0.0
        except Exception: return 0.0

IND = Indicators()

# ============================================================================
# DATABASE & ADVANCED ANALYTICS
# ============================================================================
class DB:
    _SCHEMA = [
        """CREATE TABLE IF NOT EXISTS trades (
            id          TEXT PRIMARY KEY,
            symbol      TEXT NOT NULL,
            side        TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price  REAL,
            quantity    REAL NOT NULL,
            stop_loss   REAL NOT NULL,
            take_profit REAL NOT NULL,
            status      TEXT DEFAULT 'open',
            strategy    TEXT,
            confidence  INTEGER DEFAULT 0,
            pnl         REAL DEFAULT 0,
            pnl_pct     REAL DEFAULT 0,
            is_partial  INTEGER DEFAULT 0,
            exit_reason TEXT,
            opened_at   TEXT DEFAULT CURRENT_TIMESTAMP,
            closed_at   TEXT
        )"""
    ]

    def __init__(self):
        self._lock = threading.Lock()
        self._path = "bot.db"
        self._boot()

    def _boot(self):
        import sqlite3
        with self._lock:
            c = sqlite3.connect(self._path)
            cur = c.cursor()
            for s in self._SCHEMA: cur.execute(s)
            c.commit()
            c.close()

    def _cx(self):
        import sqlite3
        return sqlite3.connect(self._path, timeout=15)

    def run(self, sql: str, p: tuple = ()) -> Optional[List]:
        try:
            with self._lock:
                c = self._cx()
                cur = c.cursor()
                cur.execute(sql, p)
                c.commit()
                if sql.strip().upper().startswith("SELECT"):
                    res = cur.fetchall()
                    c.close()
                    return res
                c.close()
        except Exception as e:
            log.error("DB Error: %s", e)
        return None

    def open_trades(self) -> List[Dict]:
        rows = self.run("SELECT id,symbol,side,entry_price,quantity,stop_loss,take_profit,strategy,confidence,is_partial FROM trades WHERE status='open'")
        if not rows: return []
        k = ["id","symbol","side","entry","qty","sl","tp","strategy","conf","is_partial"]
        return [dict(zip(k, r)) for r in rows]

    def insert(self, t: Dict):
        self.run(
            "INSERT OR IGNORE INTO trades (id,symbol,side,entry_price,quantity,stop_loss,take_profit,strategy,confidence) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (t["id"], t["symbol"], t["side"], t["entry"], t["qty"], t["sl"], t["tp"], t["strategy"], t["conf"])
        )

    def update_partial(self, tid: str, new_qty: float, new_sl: float):
        self.run("UPDATE trades SET quantity=?, stop_loss=?, is_partial=1 WHERE id=?", (new_qty, new_sl, tid))

    def close(self, tid: str, ep: float, pnl: float, pct: float, reason: str):
        self.run(
            "UPDATE trades SET status='closed',exit_price=?,pnl=?,pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?",
            (ep, pnl, pct, reason, tid)
        )

    def get_advanced_analytics(self) -> Dict:
        """محاسبه کامل آمار عملکرد ربات جهت ارزیابی عمیق"""
        rows = self.run("SELECT pnl, pnl_pct FROM trades WHERE status='closed'")
        if not rows:
            return {
                "total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0,
                "profit_factor": 0.0, "best_trade": 0.0, "worst_trade": 0.0,
                "avg_win": 0.0, "avg_loss": 0.0
            }

        pnls = [r[0] for r in rows]
        wins = [p for p in pnls if p > 0]
        losses = [abs(p) for p in pnls if p < 0]

        total_trades = len(pnls)
        win_rate = (len(wins) / total_trades * 100) if total_trades > 0 else 0.0
        total_pnl = sum(pnls)
        
        gross_profit = sum(wins)
        gross_loss = sum(losses)
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else gross_profit

        return {
            "total_trades": total_trades,
            "wins_count": len(wins),
            "losses_count": len(losses),
            "win_rate": round(win_rate, 1),
            "total_pnl": round(total_pnl, 2),
            "profit_factor": round(profit_factor, 2),
            "best_trade": round(max(pnls), 2) if pnls else 0.0,
            "worst_trade": round(min(pnls), 2) if pnls else 0.0,
            "avg_win": round(sum(wins)/len(wins), 2) if wins else 0.0,
            "avg_loss": round(sum(losses)/len(losses), 2) if losses else 0.0
        }

database = DB()

# ============================================================================
# EXCHANGE
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex = None
        self._connect()

    def _connect(self):
        if not API_KEY: return
        try:
            self._ex = ccxt.phemex({
                "apiKey": API_KEY, "secret": API_SECRET,
                "enableRateLimit": True, "timeout": 30000,
                "options": {"defaultType": "swap"}
            })
            if TESTNET: self._ex.set_sandbox_mode(True)
            self._ex.load_markets()
        except Exception as e:
            log.error("Exchange Connect Error: %s", e)

    def fetch_multi_ohlcv(self, sym: str) -> Dict[str, pd.DataFrame]:
        timeframes = ["1m", "3m", "5m", "15m"]
        result = {}
        for tf in timeframes:
            try:
                raw = self._ex.fetch_ohlcv(sym, tf, limit=100) if self._ex else self._mock_ohlcv()
                df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
                df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
                result[tf] = df
            except Exception: return {}
        return result

    def fetch_real_open_positions(self) -> List[Dict]:
        if not self._ex or DRY_RUN: return []
        try:
            positions = self._ex.fetch_positions()
            active = []
            for p in positions:
                contracts = float(p.get("contracts", 0) or 0)
                if contracts > 0:
                    active.append({
                        "symbol": p.get("symbol"),
                        "side": "long" if p.get("side") == "long" else "short",
                        "qty": contracts,
                        "entry": float(p.get("entryPrice", 0))
                    })
            return active
        except Exception: return []

    def _mock_ohlcv(self):
        now = int(time.time() * 1000)
        return [[now - i*60000, 100, 101, 99, 100, 10] for i in range(100)]

    def balance(self) -> float:
        if self._ex is None or DRY_RUN: return 10_000.0
        try:
            b = self._ex.fetch_balance()
            return float(b.get("USDT", {}).get("free", 0.0))
        except Exception: return 0.0

    def order(self, sym: str, side: str, qty: float) -> Optional[Dict]:
        if DRY_RUN: return {"id": f"dry_{uuid.uuid4().hex[:6]}", "ok": True}
        try:
            return self._ex.create_order(sym, "market", side, qty)
        except Exception as e:
            log.error("Order Failed: %s", e)
            return None

EX = Exchange()

# ============================================================================
# INTERACTIVE TELEGRAM BOT & DASHBOARD ENGINE
# ============================================================================
class TelegramBotHandler:
    def __init__(self, engine):
        self.engine = engine
        self.last_update_id = 0
        if TG_TOKEN:
            threading.Thread(target=self._poll_updates, daemon=True).start()

    def send(self, msg: str):
        if not TG_TOKEN or not TG_CHAT: return
        try:
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}, timeout=10)
        except Exception as e:
            log.warning("Telegram Post Error: %s", e)

    def _poll_updates(self):
        """حلقه دریافت دستورات از تلگرام (Polling)"""
        while True:
            try:
                url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?offset={self.last_update_id + 1}&timeout=10"
                res = requests.get(url, timeout=12).json()
                if res.get("ok"):
                    for update in res.get("result", []):
                        self.last_update_id = update["update_id"]
                        if "message" in update and "text" in update["message"]:
                            text = update["message"]["text"].strip()
                            self._handle_command(text)
            except Exception:
                pass
            time.sleep(2)

    def _handle_command(self, cmd: str):
        if cmd in ("/start", "/start_bot", "🟢 شروع ربات"):
            self.engine.is_active = True
            self.send("🟢 <b>ربات فعال شد!</b>\nاسکن بازار و معامله‌گری جدید آغاز گردید.")

        elif cmd in ("/stop", "/stop_bot", "🔴 توقف ربات"):
            self.engine.is_active = False
            self.send("🔴 <b>ربات متوقف شد!</b>\nورود به معاملات جدید معلق شد. پوزیشن‌های باز همچنان مدیریت می‌شوند.")

        elif cmd in ("/dashboard", "📊 داشبورد تحلیلی"):
            self.send_dashboard()

        elif cmd in ("/positions", "💼 پوزیشن‌های باز"):
            self.send_positions()

    def send_dashboard(self):
        stats = database.get_advanced_analytics()
        bal = EX.balance()
        status_str = "🟢 فعال (Scanning)" if self.engine.is_active else "🔴 متوقف شده"
        if self.engine.is_dd_halted:
            status_str = "⚠️ قفل افت حساب (Drawdown Halt)"

        msg = (
            f"📊 <b>داشبورد ارزیابی عملکرد ربات</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ <b>وضعیت ربات:</b> {status_str}\n"
            f"💰 <b>موجودی کیف‌پول:</b> {bal:,.2f} USDT\n"
            f"📈 <b>سود/زیان کل:</b> {stats['total_pnl']:+,.2f} USDT\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>تعداد کل معاملات:</b> {stats['total_trades']}\n"
            f"✅ <b>برد:</b> {stats['wins_count']} | ❌ <b>باخت:</b> {stats['losses_count']}\n"
            f"🔥 <b>وین‌ریت (Win Rate):</b> {stats['win_rate']}%\n"
            f"⚡ <b>پرافیت فاکتور (Profit Factor):</b> {stats['profit_factor']}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🟢 <b>بهترین معامله:</b> +{stats['best_trade']:.2f} $\n"
            f"🔴 <b>بدترین معامله:</b> {stats['worst_trade']:.2f} $\n"
            f"📊 <b>میانگین سود:</b> +{stats['avg_win']:.2f} $\n"
            f"📊 <b>میانگین زیان:</b> -{stats['avg_loss']:.2f} $\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🛡️ <b>افت حساب روزانه (Drawdown):</b> {self.engine.current_dd:.1f}% / {MAX_DD}%\n"
            f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self.send(msg)

    def send_positions(self):
        with self.engine._lock:
            pos_list = list(self.engine._pos.values())

        if not pos_list:
            self.send("💼 <b>هیچ پوزیشن بازی وجود ندارد.</b>")
            return

        msg = f"💼 <b>پوزیشن‌های فعال ({len(pos_list)}/{MAX_POS}):</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        for p in pos_list:
            msg += (
                f"📌 <b>{p['symbol']}</b> ({p['side'].upper()})\n"
                f"ورود: {p['entry']:.4f} | استاپ: {p['sl']:.4f}\n"
                f"حجم: {p['qty']} | استراتژی: {p['strategy']}\n"
                f"────────────────────\n"
            )
        self.send(msg)

# ============================================================================
# VIRTUAL THINK-TANK ENGINE
# ============================================================================
@dataclass
class ThinkTankOutput:
    action: str = "neutral"
    strategy: str = ""
    conf: int = 0
    reason: str = ""
    sl: float = 0.0
    tp1: float = 0.0
    entry: float = 0.0

class VirtualThinkTank:
    def analyze(self, sym: str, dfs: Dict[str, pd.DataFrame]) -> ThinkTankOutput:
        if not dfs or any(len(dfs[tf]) < 30 for tf in ["1m", "3m", "5m", "15m"]):
            return ThinkTankOutput()

        df1m, df3m, df5m, df15m = dfs["1m"], dfs["3m"], dfs["5m"], dfs["15m"]
        adx15 = IND.safe(IND.adx(df15m["high"], df15m["low"], df15m["close"]))
        ema200_15 = IND.safe(IND.ema(df15m["close"], 200))
        ema50_15  = IND.safe(IND.ema(df15m["close"], 50))
        price15   = IND.safe(df15m["close"])

        # === استراتژی اول: شکارچی روند ===
        if adx15 > 25:
            trend = "long" if price15 > ema200_15 and ema50_15 > ema200_15 else ("short" if price15 < ema200_15 and ema50_15 < ema200_15 else None)
            if trend:
                rsi3 = IND.safe(IND.rsi(df3m["close"], 7))
                rsi5 = IND.safe(IND.rsi(df5m["close"], 7))
                pullback = (rsi3 < 30 or rsi5 < 30) if trend == "long" else (rsi3 > 70 or rsi5 > 70)
                if pullback:
                    ema9_1m = IND.ema(df1m["close"], 9)
                    c_prev, c_curr = df1m["close"].iloc[-2], df1m["close"].iloc[-1]
                    e_prev, e_curr = ema9_1m.iloc[-2], ema9_1m.iloc[-1]
                    trigger = (c_prev <= e_prev and c_curr > e_curr) if trend == "long" else (c_prev >= e_prev and c_curr < e_curr)
                    if trigger:
                        atr3 = IND.safe(IND.atr(df3m["high"], df3m["low"], df3m["close"])) or (c_curr * 0.01)
                        entry = c_curr
                        sl = entry - (1.5 * atr3) if trend == "long" else entry + (1.5 * atr3)
                        tp1 = entry + abs(entry - sl) if trend == "long" else entry - abs(entry - sl)
                        return ThinkTankOutput(
                            action="buy" if trend == "long" else "sell",
                            strategy="Strat1_TrendPullback", conf=85,
                            reason=f"ADX15={adx15:.1f} Trend={trend}", sl=sl, tp1=tp1, entry=entry
                        )

        # === استراتژی دوم: رفت و برگشت نقدینگی ===
        if adx15 < 25:
            bb_lo15, _, bb_hi15 = IND.bbands(df15m["close"], 20, 2.0)
            h5, l5 = IND.safe(df5m["high"]), IND.safe(df5m["low"])

            fake_long  = l5 < IND.safe(bb_lo15)
            fake_short = h5 > IND.safe(bb_hi15)

            if fake_long or fake_short:
                rsi3_s = IND.rsi(df3m["close"], 14)
                div_long  = (IND.safe(df3m["close"], -1) < IND.safe(df3m["close"], -3)) and (IND.safe(rsi3_s, -1) > IND.safe(rsi3_s, -3)) if fake_long else False
                div_short = (IND.safe(df3m["close"], -1) > IND.safe(df3m["close"], -3)) and (IND.safe(rsi3_s, -1) < IND.safe(rsi3_s, -3)) if fake_short else False

                if div_long or div_short:
                    bb_lo1, _, bb_hi1 = IND.bbands(df1m["close"], 20, 2.0)
                    c1 = IND.safe(df1m["close"])
                    if (div_long and c1 > IND.safe(bb_lo1)) or (div_short and c1 < IND.safe(bb_hi1)):
                        atr5 = IND.safe(IND.atr(df5m["high"], df5m["low"], df5m["close"])) or (c1 * 0.01)
                        entry = c1
                        sl = entry - (1.5 * atr5) if div_long else entry + (1.5 * atr5)
                        tp1 = entry + (1.5 * atr5) if div_long else entry - (1.5 * atr5)
                        return ThinkTankOutput(
                            action="buy" if div_long else "sell",
                            strategy="Strat2_LiquiditySweep", conf=80,
                            reason=f"ADX15={adx15:.1f} Range Sweep", sl=sl, tp1=tp1, entry=entry
                        )

        return ThinkTankOutput()

THINK_TANK = VirtualThinkTank()

# ============================================================================
# ENGINE
# ============================================================================
class Engine:
    def __init__(self):
        self._pos : Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self.is_active = True           # قابلیت فعال/غیرفعال‌سازی با تلگرام
        self.is_dd_halted = False       # قفل افت حساب
        self.current_dd = 0.0
        self.peak_balance = None
        self.tg_handler = None
        self._boot()

    def _boot(self):
        bal = EX.balance()
        self.peak_balance = bal
        for t in database.open_trades():
            self._pos[t["id"]] = t

        # بازیابی پوزیشن‌های واقعی صرافی
        real_positions = EX.fetch_real_open_positions()
        for rp in real_positions:
            if not any(p["symbol"] == rp["symbol"] for p in self._pos.values()):
                pid = f"sync_{uuid.uuid4().hex[:6]}"
                entry = rp["entry"]
                atr = entry * 0.01
                sl = entry - (1.5 * atr) if rp["side"] == "long" else entry + (1.5 * atr)
                tp = entry + (1.5 * atr) if rp["side"] == "long" else entry - (1.5 * atr)

                pos = {
                    "id": pid, "symbol": rp["symbol"], "side": rp["side"],
                    "entry": entry, "qty": rp["qty"], "sl": sl, "tp": tp,
                    "strategy": "ReSynced", "conf": 100, "is_partial": 0
                }
                with self._lock: self._pos[pid] = pos
                database.insert(pos)

    def check_drawdown(self, current_bal: float):
        """چک کردن افت حساب جهت جلوگیری از ورود به معاملات جدید"""
        if self.peak_balance is None or current_bal > self.peak_balance:
            self.peak_balance = current_bal

        if self.peak_balance > 0:
            self.current_dd = (self.peak_balance - current_bal) / self.peak_balance * 100.0
            
            if self.current_dd >= MAX_DD and not self.is_dd_halted:
                self.is_dd_halted = True
                if self.tg_handler:
                    self.tg_handler.send(
                        f"🚨 <b>هشدار افت حساب (Drawdown Guard)!</b>\n"
                        f"افت حساب به {self.current_dd:.1f}% رسید.\n"
                        f"ورود به معاملات جدید معلق شد؛ اما پوزیشن‌های باز تا بسته‌شدن مدیریت می‌شوند."
                    )
            elif self.current_dd < (MAX_DD * 0.7) and self.is_dd_halted:
                self.is_dd_halted = False
                if self.tg_handler:
                    self.tg_handler.send("✅ <b>افت حساب بهبود یافت. ورود به معاملات جدید مجاز است.</b>")

    def loop(self):
        log.info("▶️ موتور اصلی ربات اجرا شد.")
        while True:
            try:
                bal = EX.balance()
                self.check_drawdown(bal)

                # ۱. مدیریت همیشه فعال پوزیشن‌های باز (حتی هنگام Stop یا Drawdown)
                self._manage_positions()

                # ۲. ورود به معامله جدید تنها در صورت فعال بودن و عدم وجود قفل افت حساب
                if self.is_active and not self.is_dd_halted and len(self._pos) < MAX_POS:
                    self._scan(bal)

                time.sleep(10)
            except Exception as e:
                log.error("Engine Loop Error: %s", e)
                time.sleep(10)

    def _scan(self, bal: float):
        for sym in SYMBOLS:
            try:
                with self._lock:
                    if len(self._pos) >= MAX_POS or sym in [p["symbol"] for p in self._pos.values()]:
                        continue

                dfs = EX.fetch_multi_ohlcv(sym)
                if not dfs: continue

                output = THINK_TANK.analyze(sym, dfs)

                if output.action in ("buy", "sell"):
                    risk_amt = bal * (RISK_PCT / 100.0)
                    sl_dist = abs(output.entry - output.sl) or (output.entry * 0.005)
                    qty = risk_amt / sl_dist
                    if (qty * output.entry) > (bal * 0.20): qty = (bal * 0.20) / output.entry

                    if qty > 0:
                        self._open_position(sym, output, round(qty, 5))
            except Exception as e:
                log.error("[%s] Scan Error: %s", sym, e)

    def _open_position(self, sym: str, out: ThinkTankOutput, qty: float):
        side = "long" if out.action == "buy" else "short"
        pid  = f"p_{uuid.uuid4().hex[:8]}"

        if not EX.order(sym, "buy" if side == "long" else "sell", qty): return

        pos = {
            "id": pid, "symbol": sym, "side": side, "entry": out.entry,
            "qty": qty, "sl": out.sl, "tp": out.tp1, "strategy": out.strategy,
            "conf": out.conf, "is_partial": 0
        }

        with self._lock: self._pos[pid] = pos
        database.insert(pos)

        if self.tg_handler:
            self.tg_handler.send(
                f"🎯 <b>پوزیشن جدید ({out.strategy})</b>\n"
                f"نماد: {sym} | جهت: {side.upper()}\n"
                f"ورود: {out.entry:.4f} | استاپ: {out.sl:.4f} | TP1: {out.tp1:.4f}"
            )

    def _manage_positions(self):
        """مدیریت هوشمند پوزیشن‌های باز (SL/TP1/Trailing Stop)"""
        with self._lock: snap = dict(self._pos)

        for pid, pos in snap.items():
            try:
                dfs = EX.fetch_multi_ohlcv(pos["symbol"])
                if not dfs or "1m" not in dfs: continue

                price = IND.safe(dfs["1m"]["close"])
                side = pos["side"]

                # حد ضرر
                sl_hit = (side == "long" and price <= pos["sl"]) or (side == "short" and price >= pos["sl"])
                if sl_hit:
                    self._close_position(pid, pos, price, "Stop Loss")
                    continue

                # خروج ۵۰٪ (TP1) و Breakeven
                if not pos.get("is_partial", 0):
                    tp1_hit = (side == "long" and price >= pos["tp"]) or (side == "short" and price <= pos["tp"])
                    if tp1_hit:
                        half_qty = pos["qty"] / 2.0
                        EX.order(pos["symbol"], "sell" if side == "long" else "buy", half_qty)
                        
                        pos["sl"] = pos["entry"]
                        pos["qty"] = half_qty
                        pos["is_partial"] = 1
                        database.update_partial(pid, half_qty, pos["entry"])
                        if self.tg_handler:
                            self.tg_handler.send(f"🎯 <b>خروج ۵۰٪ حجم (TP1)</b>\nنماد: {pos['symbol']}\nاستاپ به نقطه ورود منتقل شد.")

                # Trailing Stop EMA20 (3m)
                if pos.get("is_partial", 0):
                    ema20_3m = IND.safe(IND.ema(dfs["3m"]["close"], 20))
                    if (side == "long" and price < ema20_3m) or (side == "short" and price > ema20_3m):
                        self._close_position(pid, pos, price, "Trailing Stop (EMA20)")

            except Exception as e:
                log.error("Manage Error [%s]: %s", pos["symbol"], e)

    def _close_position(self, pid: str, pos: Dict, price: float, reason: str):
        EX.order(pos["symbol"], "sell" if pos["side"] == "long" else "buy", pos["qty"])
        pnl = (price - pos["entry"]) * pos["qty"] if pos["side"] == "long" else (pos["entry"] - price) * pos["qty"]
        pct = (price - pos["entry"]) / pos["entry"] * 100 if pos["side"] == "long" else (pos["entry"] - price) / pos["entry"] * 100

        database.close(pid, price, pnl, pct, reason)
        with self._lock: self._pos.pop(pid, None)

        if self.tg_handler:
            self.tg_handler.send(f"🏁 <b>بستن پوزیشن ({reason})</b>\nنماد: {pos['symbol']}\nسود/زیان: {pnl:+.2f}$ ({pct:+.2f}%)")

# ============================================================================
# FLASK SERVER
# ============================================================================
app = Flask(__name__)
engine = None

@app.route('/')
def home():
    return "<h1>🤖 Master-AI Bot v7.0.0 (Interactive Telegram & Dashboard)</h1>"

@app.route('/health')
def health():
    return {"status": "ok", "active_positions": len(engine._pos) if engine else 0}

def main():
    global engine
    engine = Engine()
    tg_handler = TelegramBotHandler(engine)
    engine.tg_handler = tg_handler
    
    threading.Thread(target=engine.loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)

if __name__ == "__main__":
    main()
