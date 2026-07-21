#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Trading Bot Pro v9.0.0 - Quant Master Edition
ویژگی‌ها:
- سیستم اتاق فکر فوق‌حرفه‌ای با ۳ استراتژی اصلاح‌شده برای افزایش تعداد معاملات
- داشبورد وب گرافیکی فرانت‌اند (Chart.js + کارت‌های زنده)
- پنل کامل تلگرام همراه با گزارشات زنده و تنظیمات پویا
- مدیریت هوشمند افت حساب و هماهنگ‌سازی خودکار پوزیشن‌های صرافی
"""

import os
import sys
import json
import time
import uuid
import logging
import threading
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

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
log = logging.getLogger("MasterQuant")

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
    def i(k: str, d: int) -> int:
        try: return int(os.getenv(k, str(d)).strip())
        except: return d
    @staticmethod
    def b(k: str, d: bool = False) -> bool:
        return os.getenv(k, "true" if d else "false").strip().lower() in ("1", "true", "yes", "on")

API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")

SYMBOLS = [
    "BTC/USDT:USDT","ETH/USDT:USDT","SOL/USDT:USDT","XRP/USDT:USDT",
    "BNB/USDT:USDT","DOGE/USDT:USDT","ADA/USDT:USDT","AVAX/USDT:USDT",
    "DOT/USDT:USDT","LINK/USDT:USDT"
]

RISK_PCT   = Cfg.f("RISK_PER_TRADE", 1.5)
MAX_DD     = Cfg.f("MAX_DRAWDOWN", 10.0)
MAX_POS    = Cfg.i("MAX_POSITIONS", 5) # افزایش ظرفیت هم‌زمان برای معامله بیشتر
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
    def macd(close: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9):
        e_fast = close.ewm(span=fast, adjust=False).mean()
        e_slow = close.ewm(span=slow, adjust=False).mean()
        line   = e_fast - e_slow
        signal = line.ewm(span=sig, adjust=False).mean()
        hist   = line - signal
        return line, signal, hist

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

    def get_recent_closed(self, limit: int = 10) -> List[Dict]:
        rows = self.run("SELECT symbol, side, entry_price, exit_price, pnl, pnl_pct, exit_reason, closed_at FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?", (limit,))
        if not rows: return []
        k = ["symbol", "side", "entry", "exit", "pnl", "pct", "reason", "time"]
        return [dict(zip(k, r)) for r in rows]

    def get_advanced_analytics(self) -> Dict:
        rows = self.run("SELECT pnl, pnl_pct FROM trades WHERE status='closed'")
        if not rows:
            return {
                "total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0,
                "profit_factor": 0.0, "best_trade": 0.0, "worst_trade": 0.0,
                "avg_win": 0.0, "avg_loss": 0.0, "wins_count": 0, "losses_count": 0
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
            log.info("✅ اتصال صرافی فیمکس برقرار شد.")
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

    def fetch_exchange_trade_history(self) -> List[Dict]:
        if not self._ex or DRY_RUN: return []
        all_trades = []
        try:
            for sym in SYMBOLS[:5]:
                try:
                    trades = self._ex.fetch_my_trades(sym, limit=5)
                    for t in trades:
                        all_trades.append({
                            "symbol": t.get("symbol"),
                            "side": t.get("side"),
                            "price": t.get("price"),
                            "amount": t.get("amount"),
                            "cost": t.get("cost"),
                            "time": datetime.fromtimestamp(t.get("timestamp", 0)/1000).strftime('%m-%d %H:%M')
                        })
                except Exception: continue
            return all_trades
        except Exception as e:
            log.error("Fetch Exchange Trades Error: %s", e)
            return []

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
# ADVANCED VIRTUAL THINK-TANK (اتاق فکر بازطراحی شده - افزایش تعداد معاملات)
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
        ema20_15 = IND.safe(IND.ema(df15m["close"], 20))
        ema50_15 = IND.safe(IND.ema(df15m["close"], 50))
        price15  = IND.safe(df15m["close"])

        # --------------------------------------------------------------------
        # استراتژی ۱: Trend Momentum Scalper (تعدیل شروط برای شکار بیشتر روندهای ۵m)
        # --------------------------------------------------------------------
        if adx15 > 20: # حد آستانه ADX انعطاف‌پذیرتر شد
            trend = "long" if price15 > ema20_15 and ema20_15 > ema50_15 else ("short" if price15 < ema20_15 and ema20_15 < ema50_15 else None)
            
            if trend:
                rsi3 = IND.safe(IND.rsi(df3m["close"], 14))
                m_line, m_sig, m_hist = IND.macd(df3m["close"])
                macd_h = IND.safe(m_hist)
                
                # شرط ورود: اصلاح RSI به محدوده خنثی + کراس مکدی
                pullback_ok = (rsi3 < 48 and macd_h > 0) if trend == "long" else (rsi3 > 52 and macd_h < 0)
                
                if pullback_ok:
                    c1 = IND.safe(df1m["close"])
                    ema9_1 = IND.safe(IND.ema(df1m["close"], 9))
                    trigger = (c1 > ema9_1) if trend == "long" else (c1 < ema9_1)
                    
                    if trigger:
                        atr3 = IND.safe(IND.atr(df3m["high"], df3m["low"], df3m["close"])) or (c1 * 0.008)
                        entry = c1
                        sl = entry - (1.2 * atr3) if trend == "long" else entry + (1.2 * atr3)
                        tp1 = entry + (1.5 * atr3) if trend == "long" else entry - (1.5 * atr3)
                        return ThinkTankOutput(
                            action="buy" if trend == "long" else "sell",
                            strategy="Strat1_MomentumScalp", conf=85,
                            reason=f"ADX15={adx15:.1f} Trend={trend} MACD/RSI Sync", sl=sl, tp1=tp1, entry=entry
                        )

        # --------------------------------------------------------------------
        # استراتژی ۲: Mean Reversion Scalper (نوسان‌گیری فعال در بازار رنج)
        # --------------------------------------------------------------------
        if adx15 <= 20:
            bb_lo5, _, bb_hi5 = IND.bbands(df5m["close"], 20, 2.0)
            c5 = IND.safe(df5m["close"])
            rsi5 = IND.safe(IND.rsi(df5m["close"], 14))

            reach_lo = c5 <= IND.safe(bb_lo5) or rsi5 < 35
            reach_hi = c5 >= IND.safe(bb_hi5) or rsi5 > 65

            if reach_lo or reach_hi:
                c1 = IND.safe(df1m["close"])
                rsi1 = IND.safe(IND.rsi(df1m["close"], 7))
                
                if reach_lo and rsi1 > 30: # بازگشت صعودی از اشباع فروش
                    atr1 = IND.safe(IND.atr(df1m["high"], df1m["low"], df1m["close"])) or (c1 * 0.006)
                    entry = c1
                    sl = entry - (1.3 * atr1)
                    tp1 = entry + (1.6 * atr1)
                    return ThinkTankOutput(
                        action="buy", strategy="Strat2_MeanReversion", conf=80,
                        reason=f"Range Reversion BB/RSI 5m", sl=sl, tp1=tp1, entry=entry
                    )
                elif reach_hi and rsi1 < 70: # بازگشت نزولی از اشباع خرید
                    atr1 = IND.safe(IND.atr(df1m["high"], df1m["low"], df1m["close"])) or (c1 * 0.006)
                    entry = c1
                    sl = entry + (1.3 * atr1)
                    tp1 = entry - (1.6 * atr1)
                    return ThinkTankOutput(
                        action="sell", strategy="Strat2_MeanReversion", conf=80,
                        reason=f"Range Reversion BB/RSI 5m", sl=sl, tp1=tp1, entry=entry
                    )

        # --------------------------------------------------------------------
        # استراتژی ۳: Micro-Breakout & Vol Spike (شکار شکست‌های سریع)
        # --------------------------------------------------------------------
        v5_curr = IND.safe(df5m["vol"])
        v5_ma = IND.safe(df5m["vol"].rolling(20).mean())
        if v5_curr > 1.8 * v5_ma: # جهش ناگهانی حجم معامله
            c1 = IND.safe(df1m["close"])
            ema20_1 = IND.safe(IND.ema(df1m["close"], 20))
            if c1 > ema20_1:
                atr1 = IND.safe(IND.atr(df1m["high"], df1m["low"], df1m["close"])) or (c1 * 0.005)
                entry = c1
                sl = entry - (1.2 * atr1)
                tp1 = entry + (1.8 * atr1)
                return ThinkTankOutput(
                    action="buy", strategy="Strat3_VolBreakout", conf=88,
                    reason=f"Volume Spike {v5_curr/v5_ma:.1f}x", sl=sl, tp1=tp1, entry=entry
                )

        return ThinkTankOutput()

THINK_TANK = VirtualThinkTank()

# ============================================================================
# INTERACTIVE TELEGRAM BOT & KEYBOARD
# ============================================================================
class TelegramBotHandler:
    def __init__(self, engine):
        self.engine = engine
        self.last_update_id = 0
        if TG_TOKEN:
            threading.Thread(target=self._poll_updates, daemon=True).start()

    def send(self, msg: str, reply_markup=None):
        if not TG_TOKEN or not TG_CHAT: return
        try:
            data = {"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}
            if reply_markup:
                data["reply_markup"] = json.dumps(reply_markup)
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data, timeout=10)
        except Exception as e:
            log.warning("Telegram Post Error: %s", e)

    def _get_menu_keyboard(self):
        return {
            "keyboard": [
                [{"text": "📊 داشبورد تحلیلی"}, {"text": "💼 پوزیشن‌های باز"}],
                [{"text": "📜 گزارش صرافی"}, {"text": "🔴 توقف ربات"}],
                [{"text": "🟢 شروع ربات"}]
            ],
            "resize_keyboard": True,
            "persistent": True
        }

    def _poll_updates(self):
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
            except Exception: pass
            time.sleep(2)

    def _handle_command(self, cmd: str):
        kb = self._get_menu_keyboard()

        if cmd in ("/start", "/start_bot", "🟢 شروع ربات"):
            self.engine.is_active = True
            self.send("🟢 <b>موتور معامله‌گری فعال شد!</b>\nاسکن زنده جفت‌ارزها آغاز گردید.", reply_markup=kb)

        elif cmd in ("/stop", "/stop_bot", "🔴 توقف ربات"):
            self.engine.is_active = False
            self.send("🔴 <b>موتور معامله‌گری متوقف شد!</b>\nورود جدید غیرفعال شد. پوزیشن‌های باز مدیریت می‌شوند.", reply_markup=kb)

        elif cmd in ("/dashboard", "📊 داشبورد تحلیلی"):
            self.send_dashboard()

        elif cmd in ("/positions", "💼 پوزیشن‌های باز"):
            self.send_positions()

        elif cmd in ("/exchange_history", "📜 گزارش صرافی"):
            self.send_exchange_history()

    def send_dashboard(self):
        stats = database.get_advanced_analytics()
        bal = EX.balance()
        status_str = "🟢 فعال" if self.engine.is_active else "🔴 متوقف"
        if self.engine.is_dd_halted: status_str = "⚠️ قفل افت حساب"

        msg = (
            f"📊 <b>داشبورد ارزیابی عملکرد ربات (v9.0 Pro)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ <b>وضعیت:</b> {status_str}\n"
            f"💰 <b>موجودی:</b> {bal:,.2f} USDT\n"
            f"📈 <b>سود/زیان کل:</b> {stats['total_pnl']:+,.2f} USDT\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>کل معاملات:</b> {stats['total_trades']}\n"
            f"✅ <b>برد:</b> {stats['wins_count']} | ❌ <b>باخت:</b> {stats['losses_count']}\n"
            f"🔥 <b>وین‌ریت:</b> {stats['win_rate']}%\n"
            f"⚡ <b>پرافیت فاکتور:</b> {stats['profit_factor']}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🛡️ <b>افت حساب:</b> {self.engine.current_dd:.1f}% / {MAX_DD}%\n"
            f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self.send(msg, reply_markup=self._get_menu_keyboard())

    def send_positions(self):
        with self.engine._lock: pos_list = list(self.engine._pos.values())
        if not pos_list:
            self.send("💼 <b>هیچ پوزیشن بازی وجود ندارد.</b>", reply_markup=self._get_menu_keyboard())
            return

        msg = f"💼 <b>پوزیشن‌های فعال ({len(pos_list)}/{MAX_POS}):</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        for p in pos_list:
            msg += (
                f"📌 <b>{p['symbol']}</b> ({p['side'].upper()})\n"
                f"ورود: {p['entry']:.4f} | استاپ: {p['sl']:.4f}\n"
                f"حجم: {p['qty']} | استراتژی: {p['strategy']}\n"
                f"────────────────────\n"
            )
        self.send(msg, reply_markup=self._get_menu_keyboard())

    def send_exchange_history(self):
        self.send("⏳ استعلام تاریخچه از صرافی Phemex...")
        trades = EX.fetch_exchange_trade_history()
        if not trades:
            self.send("📜 <b>هیچ معامله‌ای در تاریخچه یافت نشد (یا در حالت Dry-Run هستید).</b>", reply_markup=self._get_menu_keyboard())
            return

        msg = f"📜 <b>گزارش اخیر صرافی Phemex:</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        for t in trades[:8]:
            side_emoji = "🟢" if t["side"].lower() == "buy" else "🔴"
            msg += f"{side_emoji} <b>{t['symbol']}</b> | {t['side'].upper()}\nقیمت: {t['price']} | حجم: {t['amount']}\nزمان: {t['time']}\n────────────────────\n"
        self.send(msg, reply_markup=self._get_menu_keyboard())

# ============================================================================
# ENGINE
# ============================================================================
class Engine:
    def __init__(self):
        self._pos : Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self.is_active = True
        self.is_dd_halted = False
        self.current_dd = 0.0
        self.peak_balance = None
        self.tg_handler = None
        self._boot()

    def _boot(self):
        bal = EX.balance()
        self.peak_balance = bal
        for t in database.open_trades(): self._pos[t["id"]] = t

        real_positions = EX.fetch_real_open_positions()
        for rp in real_positions:
            if not any(p["symbol"] == rp["symbol"] for p in self._pos.values()):
                pid = f"sync_{uuid.uuid4().hex[:6]}"
                entry = rp["entry"]
                atr = entry * 0.008
                sl = entry - (1.3 * atr) if rp["side"] == "long" else entry + (1.3 * atr)
                tp = entry + (1.6 * atr) if rp["side"] == "long" else entry - (1.6 * atr)

                pos = {
                    "id": pid, "symbol": rp["symbol"], "side": rp["side"],
                    "entry": entry, "qty": rp["qty"], "sl": sl, "tp": tp,
                    "strategy": "ReSynced", "conf": 100, "is_partial": 0
                }
                with self._lock: self._pos[pid] = pos
                database.insert(pos)

    def check_drawdown(self, current_bal: float):
        if self.peak_balance is None or current_bal > self.peak_balance:
            self.peak_balance = current_bal

        if self.peak_balance > 0:
            self.current_dd = (self.peak_balance - current_bal) / self.peak_balance * 100.0
            if self.current_dd >= MAX_DD and not self.is_dd_halted:
                self.is_dd_halted = True
                if self.tg_handler:
                    self.tg_handler.send(f"🚨 <b>هشدار افت حساب (Drawdown Guard)!</b>\nافت به {self.current_dd:.1f}% رسید. ورود جدید معلق شد.")
            elif self.current_dd < (MAX_DD * 0.7) and self.is_dd_halted:
                self.is_dd_halted = False
                if self.tg_handler:
                    self.tg_handler.send("✅ <b>افت حساب بهبود یافت. معامله‌گری مجاز است.</b>")

    def loop(self):
        log.info("▶️ موتور اصلی اجرا شد.")
        while True:
            try:
                bal = EX.balance()
                self.check_drawdown(bal)
                self._manage_positions()

                if self.is_active and not self.is_dd_halted and len(self._pos) < MAX_POS:
                    self._scan(bal)

                time.sleep(8)
            except Exception as e:
                log.error("Engine Loop Error: %s", e)
                time.sleep(8)

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
                    if (qty * output.entry) > (bal * 0.15): qty = (bal * 0.15) / output.entry

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

                # خروج ۵۰٪ (TP1)
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
                            self.tg_handler.send(f"🎯 <b>خروج ۵۰٪ (TP1)</b>\nنماد: {pos['symbol']}\nاستاپ به نقطه ورود منتقل شد.")

                # Trailing Stop EMA20 (3m)
                if pos.get("is_partial", 0):
                    ema20_3m = IND.safe(IND.ema(dfs["3m"]["close"], 20))
                    if (side == "long" and price < ema20_3m) or (side == "short" and price > ema20_3m):
                        self._close_position(pid, pos, price, "Trailing Stop")

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
# FLASK WEB DASHBOARD SERVER (داشبورد فوق‌حرفه‌ای مرورگر)
# ============================================================================
app = Flask(__name__)
engine = None

@app.route('/')
def home():
    stats = database.get_advanced_analytics()
    bal = EX.balance()
    pos_list = list(engine._pos.values()) if engine else []
    recent_closed = database.get_recent_closed(5)
    status_str = "🟢 فعال (Scanning)" if (engine and engine.is_active) else "🔴 متوقف"
    
    # ساخت ردیف‌های جدول پوزیشن‌های باز
    pos_rows = ""
    for p in pos_list:
        pos_rows += f"<tr><td><b>{p['symbol']}</b></td><td><span class='badge {p['side']}'>{p['side'].upper()}</span></td><td>{p['entry']:.4f}</td><td>{p['sl']:.4f}</td><td>{p['qty']}</td><td>{p['strategy']}</td></tr>"
    if not pos_rows:
        pos_rows = "<tr><td colspan='6' style='text-align:center;color:#8b949e;'>هیچ پوزیشن بازی وجود ندارد</td></tr>"

    # ساخت ردیف‌های جدول تاریخچه
    history_rows = ""
    for h in recent_closed:
        pnl_class = "green" if h["pnl"] > 0 else "red"
        history_rows += f"<tr><td><b>{h['symbol']}</b></td><td>{h['side'].upper()}</td><td>{h['entry']:.4f}</td><td>{h['exit']:.4f}</td><td class='{pnl_class}'>{h['pnl']:+.2f} $ ({h['pct']:+.2f}%)</td><td>{h['reason']}</td></tr>"
    if not history_rows:
        history_rows = "<tr><td colspan='6' style='text-align:center;color:#8b949e;'>تاریخچه‌ای ثبت نشده است</td></tr>"

    return f"""
    <!DOCTYPE html>
    <html dir="rtl" lang="fa">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Quant Dashboard | Master-AI Bot</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, sans-serif; background-color: #0b0e14; color: #e1e7ec; margin: 0; padding: 20px; }}
            .container {{ max-width: 1100px; margin: 0 auto; }}
            .header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1f2937; padding-bottom: 15px; }}
            .status-badge {{ padding: 6px 12px; border-radius: 20px; background: #16222f; border: 1px solid #238636; color: #3fb950; font-size: 14px; }}
            .card-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 15px; margin: 20px 0; }}
            .card {{ background: #121824; padding: 18px; border-radius: 10px; border: 1px solid #1f2937; text-align: center; }}
            .card h3 {{ margin: 0; font-size: 13px; color: #8b949e; text-transform: uppercase; }}
            .card p {{ margin: 8px 0 0 0; font-size: 22px; font-weight: bold; color: #58a6ff; }}
            .section {{ background: #121824; border-radius: 10px; border: 1px solid #1f2937; padding: 20px; margin-bottom: 20px; }}
            .section h2 {{ margin-top: 0; font-size: 16px; color: #f0f6fc; border-bottom: 1px solid #1f2937; padding-bottom: 10px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 14px; }}
            th, td {{ padding: 10px; text-align: right; border-bottom: 1px solid #1f2937; }}
            th {{ color: #8b949e; font-weight: normal; }}
            .badge {{ padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }}
            .badge.long {{ background: #13382c; color: #3fb950; }}
            .badge.short {{ background: #441c24; color: #f85149; }}
            .green {{ color: #3fb950; font-weight: bold; }}
            .red {{ color: #f85149; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🤖 Master-AI Quant Bot <small style="font-size:12px;color:#8b949e;">v9.0 Pro</small></h1>
                <div class="status-badge">{status_str}</div>
            </div>
            
            <div class="card-grid">
                <div class="card"><h3>موجودی کیف‌پول</h3><p>${bal:,.2f}</p></div>
                <div class="card"><h3>سود/زیان کل</h3><p class="{'green' if stats['total_pnl']>=0 else 'red'}">{stats['total_pnl']:+,.2f} $</p></div>
                <div class="card"><h3>وین‌ریت (Win Rate)</h3><p>{stats['win_rate']}%</p></div>
                <div class="card"><h3>کل معاملات</h3><p>{stats['total_trades']}</p></div>
                <div class="card"><h3>پرافیت فاکتور</h3><p>{stats['profit_factor']}</p></div>
                <div class="card"><h3>افت حساب (DD)</h3><p style="color:#e3b341;">{engine.current_dd if engine else 0:.1f}%</p></div>
            </div>

            <div class="section">
                <h2>💼 پوزیشن‌های فعال در حال مدیریت ({len(pos_list)}/{MAX_POS})</h2>
                <table>
                    <thead>
                        <tr><th>نماد</th><th>جهت</th><th>قیمت ورود</th><th>حد ضرر</th><th>حجم</th><th>استراتژی</th></tr>
                    </thead>
                    <tbody>{pos_rows}</tbody>
                </table>
            </div>

            <div class="section">
                <h2>📜 تاریخچه ۵ معامله اخیر ربات</h2>
                <table>
                    <thead>
                        <tr><th>نماد</th><th>جهت</th><th>ورود</th><th>خروج</th><th>سود/زیان</th><th>دلیل خروج</th></tr>
                    </thead>
                    <tbody>{history_rows}</tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """

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
