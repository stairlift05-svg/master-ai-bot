#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Trading Bot Pro v5.2.2 - Production Ready
✅ فیکس کامل برای Phemex Futures
✅ افزودن قیمت‌های لحظه‌ای به داشبورد
✅ مدیریت خطا و لاگ‌گیری بهتر
"""

import os
import sys
import re
import json
import time
import uuid
import logging
import threading
import hashlib
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from collections import deque

# ── بررسی Python version ──────────────────────────────────────────────────
if sys.version_info < (3, 10):
    print("[CRITICAL] Python 3.10+ لازم است")
    sys.exit(1)

# ── Third-party ────────────────────────────────────────────────────────────
_MISSING = []
try:
    import pandas as pd
except ImportError:
    _MISSING.append("pandas")

try:
    import numpy as np
except ImportError:
    _MISSING.append("numpy")

try:
    import requests
except ImportError:
    _MISSING.append("requests")

try:
    import ccxt
except ImportError:
    _MISSING.append("ccxt")

try:
    import pandas_ta as ta
    _TA_OK = True
except ImportError:
    _TA_OK = False
    print("[WARNING] pandas-ta نصب نیست - از محاسبات manual استفاده می‌شود")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Flask ──────────────────────────────────────────────────────────────────
try:
    from flask import Flask, render_template_string, jsonify, request
except ImportError:
    _MISSING.append("flask")

if _MISSING:
    print(f"[CRITICAL] پکیج‌های گمشده: {_MISSING}")
    print("اجرا کنید: pip install -r requirements.txt")
    sys.exit(1)

# ============================================================================
# LOGGING
# ============================================================================
IS_PROD = os.getenv("RENDER", "false").lower() == "true"

def _setup_log() -> logging.Logger:
    fmt = (
        '{"t":"%(asctime)s","lvl":"%(levelname)s","msg":"%(message)s"}'
        if IS_PROD else
        "%(asctime)s | %(levelname)-8s | %(message)s"
    )
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        stream=sys.stdout,
        force=True
    )
    for lib in ("ccxt", "urllib3", "openai", "httpx", "httpcore",
                "asyncio", "websocket"):
        logging.getLogger(lib).setLevel(logging.ERROR)
    return logging.getLogger("Bot")

log = _setup_log()
log.info("🚀 Bot v5.2.2 شروع به بارگذاری کرد...")

# ============================================================================
# CONFIG
# ============================================================================
class Cfg:
    @staticmethod
    def s(k: str, d: str = "") -> str:
        return os.getenv(k, d).strip()

    @staticmethod
    def i(k: str, d: int) -> int:
        try:
            return int(os.getenv(k, str(d)).strip())
        except (ValueError, AttributeError):
            return d

    @staticmethod
    def f(k: str, d: float) -> float:
        try:
            return float(os.getenv(k, str(d)).strip())
        except (ValueError, AttributeError):
            return d

    @staticmethod
    def b(k: str, d: bool = False) -> bool:
        return os.getenv(k, "true" if d else "false").strip().lower() in (
            "1", "true", "yes", "on"
        )

    @staticmethod
    def lst(k: str, d: str = "") -> List[str]:
        raw = os.getenv(k, d) or d
        return [x.strip() for x in raw.split(",") if x.strip()]

    @staticmethod
    def validate():
        errs, warns = [], []

        if not Cfg.s("PHEMEX_API_KEY"):
            warns.append("PHEMEX_API_KEY خالی (فقط DRY_RUN)")
        if not Cfg.s("PHEMEX_API_SECRET"):
            warns.append("PHEMEX_API_SECRET خالی (فقط DRY_RUN)")

        r = Cfg.f("RISK_PER_TRADE", 1.5)
        if not 0.1 <= r <= 5.0:
            errs.append(f"RISK_PER_TRADE={r} باید 0.1-5.0 باشد")

        dd = Cfg.f("MAX_DRAWDOWN", 10.0)
        if not 1.0 <= dd <= 50.0:
            errs.append(f"MAX_DRAWDOWN={dd} باید 1-50 باشد")

        tf = Cfg.s("TIMEFRAME", "5m")
        if tf not in ["1m","3m","5m","15m","30m","1h","4h","1d"]:
            errs.append(f"TIMEFRAME={tf} نامعتبر")

        for w in warns:
            log.warning("⚠️  %s", w)
        if errs:
            for e in errs:
                log.critical("❌ %s", e)
            raise SystemExit("Config خطا - ربات متوقف شد")
        log.info("✅ Config OK (%d هشدار)", len(warns))


# ── مقادیر ثابت ──────────────────────────────────────────────────────────
API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")
OAI_KEY    = Cfg.s("OPENAI_API_KEY")
DB_URL     = Cfg.s("DATABASE_URL")

# ── 10 جفت‌ارز برتر فیوچرز Phemex ──────────────────────────────────────
SYMBOLS = Cfg.lst("SYMBOLS", 
    "BTC/USDT:USDT,"
    "ETH/USDT:USDT,"
    "SOL/USDT:USDT,"
    "XRP/USDT:USDT,"
    "BNB/USDT:USDT,"
    "DOGE/USDT:USDT,"
    "ADA/USDT:USDT,"
    "AVAX/USDT:USDT,"
    "DOT/USDT:USDT,"
    "LINK/USDT:USDT"
)

TF         = Cfg.s("TIMEFRAME", "5m")
RISK_PCT   = Cfg.f("RISK_PER_TRADE", 1.5)
MAX_DD     = Cfg.f("MAX_DRAWDOWN", 10.0)
MAX_POS    = Cfg.i("MAX_POSITIONS", 5)
DRY_RUN    = Cfg.b("DRY_RUN", False)
TESTNET    = Cfg.b("PHEMEX_TESTNET", False)
PORT       = Cfg.i("PORT", 10000)

log.info(
    "Config: symbols=%d tf=%s risk=%.1f%% dd=%.1f%% dry=%s testnet=%s",
    len(SYMBOLS), TF, RISK_PCT, MAX_DD, DRY_RUN, TESTNET
)

# ============================================================================
# PERFORMANCE TRACKER
# ============================================================================
class PerfTracker:
    def __init__(self, max_len: int = 100):
        self._scans = deque(maxlen=max_len)
        self._signals = deque(maxlen=max_len)
        self._errors = deque(maxlen=max_len)
        
    def log_scan(self, duration: float):
        self._scans.append(duration)
    
    def log_signal(self, sym: str, action: str, conf: int):
        self._signals.append({
            "time": datetime.now(timezone.utc).isoformat(),
            "symbol": sym,
            "action": action,
            "confidence": conf
        })
    
    def log_error(self, error: str):
        self._errors.append({
            "time": datetime.now(timezone.utc).isoformat(),
            "error": error[:200]
        })
    
    @property
    def stats(self) -> Dict:
        return {
            "avg_scan_time": round(sum(self._scans) / len(self._scans), 2) if self._scans else 0,
            "recent_signals": list(self._signals)[-10:],
            "recent_errors": list(self._errors)[-5:],
            "total_scans": len(self._scans)
        }

PERF = PerfTracker()

# ============================================================================
# INDICATORS (FIXED)
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close: pd.Series, n: int = 14) -> pd.Series:
        if _TA_OK:
            try:
                r = ta.rsi(close, length=n)
                if r is not None and not r.dropna().empty:
                    return r
            except Exception:
                pass
        delta = close.diff()
        up   = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        
        down_ma = down.ewm(com=n-1, adjust=False).mean()
        down_ma = down_ma.replace(0, 1e-10)
        
        rs = up.ewm(com=n-1, adjust=False).mean() / down_ma
        return 100 - (100 / (1 + rs))

    @staticmethod
    def ema(close: pd.Series, n: int) -> pd.Series:
        if _TA_OK:
            try:
                r = ta.ema(close, length=n)
                if r is not None and not r.dropna().empty:
                    return r
            except Exception:
                pass
        return close.ewm(span=n, adjust=False).mean()

    @staticmethod
    def atr(high: pd.Series, low: pd.Series,
            close: pd.Series, n: int = 14) -> pd.Series:
        if _TA_OK:
            try:
                r = ta.atr(high, low, close, length=n)
                if r is not None and not r.dropna().empty:
                    return r
            except Exception:
                pass
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs()
        ], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

    @staticmethod
    def macd(close: pd.Series,
             fast: int = 12, slow: int = 26,
             sig: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        if _TA_OK:
            try:
                r = ta.macd(close, fast=fast, slow=slow, signal=sig)
                if r is not None and r.shape[1] >= 3:
                    return r.iloc[:,0], r.iloc[:,1], r.iloc[:,2]
            except Exception:
                pass
        e_fast = close.ewm(span=fast, adjust=False).mean()
        e_slow = close.ewm(span=slow, adjust=False).mean()
        line   = e_fast - e_slow
        signal = line.ewm(span=sig, adjust=False).mean()
        hist   = line - signal
        return line, signal, hist

    @staticmethod
    def bbands(close: pd.Series,
               n: int = 20, std: float = 2.0
               ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        if _TA_OK:
            try:
                r = ta.bbands(close, length=n, std=std)
                if r is not None and r.shape[1] >= 3:
                    return r.iloc[:,0], r.iloc[:,1], r.iloc[:,2]
            except Exception:
                pass
        mid = close.rolling(n).mean()
        sd  = close.rolling(n).std()
        return mid - std*sd, mid, mid + std*sd

    @staticmethod
    def stoch(high: pd.Series, low: pd.Series,
              close: pd.Series, k: int = 14, d: int = 3
              ) -> Tuple[pd.Series, pd.Series]:
        if _TA_OK:
            try:
                r = ta.stoch(high, low, close, k=k, d=d)
                if r is not None and r.shape[1] >= 2:
                    return r.iloc[:,0], r.iloc[:,1]
            except Exception:
                pass
        lo  = low.rolling(k).min()
        hi  = high.rolling(k).max()
        stk = 100 * (close - lo) / (hi - lo + 1e-10)
        std = stk.rolling(d).mean()
        return stk, std

    @staticmethod
    def safe(s, idx: int = -1) -> float:
        try:
            if s is None:
                return 0.0
            v = s.iloc[idx]
            return float(v) if not (v != v) else 0.0
        except Exception:
            return 0.0


IND = Indicators()

# ============================================================================
# DATABASE
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
            ai_signal   TEXT,
            confidence  INTEGER DEFAULT 0,
            pnl         REAL DEFAULT 0,
            pnl_pct     REAL DEFAULT 0,
            exit_reason TEXT,
            opened_at   TEXT DEFAULT CURRENT_TIMESTAMP,
            closed_at   TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS daily_stats (
            date        TEXT PRIMARY KEY,
            total       INTEGER DEFAULT 0,
            wins        INTEGER DEFAULT 0,
            losses      INTEGER DEFAULT 0,
            pnl         REAL DEFAULT 0,
            win_rate    REAL DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS activity_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT DEFAULT CURRENT_TIMESTAMP,
            event_type  TEXT NOT NULL,
            symbol      TEXT,
            message     TEXT
        )"""
    ]
    _IDX = [
        "CREATE INDEX IF NOT EXISTS i_status ON trades(status)",
        "CREATE INDEX IF NOT EXISTS i_symbol ON trades(symbol)",
        "CREATE INDEX IF NOT EXISTS i_event ON activity_log(event_type)",
    ]

    def __init__(self):
        self._pg    = bool(DB_URL) and "postgres" in DB_URL
        self._pool  = None
        self._lock  = threading.Lock()
        self._path  = "bot.db"
        self._boot()

    def _boot(self):
        if self._pg:
            try:
                import psycopg2.pool as pp
                self._pool = pp.ThreadedConnectionPool(
                    1, 6, dsn=DB_URL, connect_timeout=8
                )
                log.info("✅ PostgreSQL Pool آماده")
            except Exception as e:
                log.warning("PostgreSQL خطا: %s → SQLite", e)
                self._pg = False

        if not self._pg:
            import sqlite3
            c = sqlite3.connect(self._path)
            c.execute("PRAGMA journal_mode=WAL")
            c.close()
            log.info("✅ SQLite: %s", self._path)

        self._run_schema()

    @contextmanager
    def _cx(self):
        if self._pg:
            c = self._pool.getconn()
            try:
                yield c
                c.commit()
            except Exception:
                c.rollback()
                raise
            finally:
                self._pool.putconn(c)
        else:
            import sqlite3
            with self._lock:
                c = sqlite3.connect(self._path, timeout=12)
                c.execute("PRAGMA journal_mode=WAL")
                try:
                    yield c
                    c.commit()
                except Exception:
                    c.rollback()
                    raise
                finally:
                    c.close()

    def _run_schema(self):
        try:
            with self._cx() as c:
                cur = c.cursor()
                for s in self._SCHEMA:
                    cur.execute(s)
                for i in self._IDX:
                    cur.execute(i)
            log.info("✅ DB Schema آماده")
        except Exception as e:
            log.critical("DB Schema خطا: %s", e)
            raise

    def run(self, sql: str, p: tuple = ()) -> Optional[List]:
        if self._pg:
            sql = sql.replace("?", "%s")
        try:
            with self._cx() as c:
                cur = c.cursor()
                cur.execute(sql, p)
                if sql.strip().upper().startswith("SELECT"):
                    return cur.fetchall()
        except Exception as e:
            log.error("DB: %s | %.50s", e, sql)
        return None

    def log_activity(self, event: str, sym: str = "", msg: str = ""):
        try:
            self.run(
                "INSERT INTO activity_log (event_type,symbol,message) VALUES (?,?,?)",
                (event, sym, msg[:500])
            )
        except Exception as e:
            log.warning("Activity log: %s", e)

    def recent_activity(self, n: int = 50) -> List[Dict]:
        rows = self.run(
            "SELECT timestamp,event_type,symbol,message "
            "FROM activity_log ORDER BY id DESC LIMIT ?", (n,)
        )
        if not rows:
            return []
        return [{"time":r[0],"type":r[1],"sym":r[2],"msg":r[3]} for r in rows]

    def open_trades(self) -> List[Dict]:
        rows = self.run(
            "SELECT id,symbol,side,entry_price,quantity,"
            "stop_loss,take_profit,confidence,opened_at "
            "FROM trades WHERE status='open'"
        )
        if not rows:
            return []
        k = ["id","symbol","side","entry","qty","sl","tp","conf","opened"]
        return [dict(zip(k, r)) for r in rows]

    def insert(self, t: Dict):
        self.run(
            "INSERT OR IGNORE INTO trades "
            "(id,symbol,side,entry_price,quantity,stop_loss,"
            "take_profit,ai_signal,confidence) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (t["id"], t["symbol"], t["side"], t["entry"],
             t["qty"], t["sl"], t["tp"], t["signal"], t["conf"])
        )
        self.log_activity("OPEN", t["symbol"], 
                         f"{t['side']} @ {t['entry']}")

    def close(self, tid: str, ep: float, pnl: float,
              pct: float, reason: str):
        self.run(
            "UPDATE trades SET status='closed',exit_price=?,pnl=?,"
            "pnl_pct=?,exit_reason=?,"
            "closed_at=CURRENT_TIMESTAMP WHERE id=?",
            (ep, pnl, pct, reason, tid)
        )
        self._stats(pnl)
        
        r = self.run("SELECT symbol FROM trades WHERE id=?", (tid,))
        sym = r[0][0] if r else "?"
        self.log_activity("CLOSE", sym, 
                         f"{reason} PNL:{pnl:.2f}$ ({pct:.1f}%)")

    def _stats(self, pnl: float):
        today = datetime.now(timezone.utc).date().isoformat()
        row   = self.run(
            "SELECT total,wins,losses,pnl FROM daily_stats WHERE date=?",
            (today,)
        )
        if row:
            tot, w, l, tp = row[0]
            tot += 1
            w   += 1 if pnl > 0 else 0
            l   += 0 if pnl > 0 else 1
            tp  += pnl
            wr   = round(w/tot*100, 1)
            self.run(
                "UPDATE daily_stats SET total=?,wins=?,losses=?,"
                "pnl=?,win_rate=? WHERE date=?",
                (tot, w, l, tp, wr, today)
            )
        else:
            self.run(
                "INSERT INTO daily_stats VALUES(?,1,?,?,?,?)",
                (today,
                 1 if pnl>0 else 0,
                 0 if pnl>0 else 1,
                 pnl,
                 100.0 if pnl>0 else 0.0)
            )

    def today(self) -> Dict:
        d = datetime.now(timezone.utc).date().isoformat()
        r = self.run(
            "SELECT total,wins,losses,pnl,win_rate "
            "FROM daily_stats WHERE date=?", (d,)
        )
        if r:
            return dict(zip(["trades","wins","losses","pnl","wr"], r[0]))
        return {"trades":0,"wins":0,"losses":0,"pnl":0.0,"wr":0.0}

    def history(self, n: int = 25) -> List[Dict]:
        rows = self.run(
            "SELECT id,symbol,side,entry_price,exit_price,"
            "pnl,pnl_pct,exit_reason,opened_at,closed_at "
            "FROM trades WHERE status='closed' "
            "ORDER BY closed_at DESC LIMIT ?", (n,)
        )
        if not rows:
            return []
        k = ["id","sym","side","entry","exit",
             "pnl","pct","reason","open","close"]
        return [dict(zip(k, r)) for r in rows]


database = DB()

# ============================================================================
# TELEGRAM ALERTS (ENHANCED with prices)
# ============================================================================
class Alerts:
    def __init__(self):
        self._sent : Dict[str, float] = {}
        self._lock = threading.Lock()
        self._chat_id = TG_CHAT
        self._queue = deque(maxlen=100)

    def _get_chat_id(self):
        if self._chat_id:
            return self._chat_id
        if not TG_TOKEN:
            return None
        try:
            res = requests.get(
                f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates",
                timeout=5
            ).json()
            if res.get("ok") and res.get("result"):
                self._chat_id = str(res["result"][-1]["message"]["chat"]["id"])
                log.info(f"✅ Chat ID دریافت شد: {self._chat_id}")
                return self._chat_id
        except Exception as e:
            log.warning(f"دریافت Chat ID: {e}")
        return None

    def send(self, msg: str, key: str = "", force: bool = False, parse_mode: str = "HTML"):
        log.info("📢 %s", msg[:100].replace("\n"," "))
        
        self._queue.append({
            "time": datetime.now(timezone.utc).isoformat(),
            "msg": msg[:200]
        })
        
        if not TG_TOKEN:
            return
            
        chat_id = self._get_chat_id()
        if not chat_id:
            return
            
        if key and not force:
            with self._lock:
                if time.time() - self._sent.get(key,0) < 30:
                    return
                self._sent[key] = time.time()
                
        threading.Thread(
            target=self._post, args=(msg, chat_id, parse_mode), daemon=True
        ).start()

    def _post(self, msg: str, chat_id: str, parse_mode: str):
        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={
                    "chat_id": chat_id,
                    "text": msg,
                    "parse_mode": parse_mode
                },
                timeout=10
            )
        except Exception as e:
            log.warning("Telegram: %s", e)

    def send_dashboard(self, engine_stats: Dict, balance: float, prices: Dict[str, float] = None):
        """
        ارسال داشبورد با قیمت‌های لحظه‌ای
        """
        dd = engine_stats.get("dd", {})
        td = engine_stats.get("today", {})
        ai = engine_stats.get("ai", {})
        pos = engine_stats.get("open_pos", 0)
        
        status = "🟢 فعال" if not dd.get("halted") else "🔴 متوقف"
        dd_pct = dd.get("dd", 0)
        
        peak = dd.get("peak", balance)
        roi = ((balance - 10000) / 10000 * 100) if balance > 0 else 0
        
        msg = (
            f"🤖 <b>داشبورد Master-AI Bot v5.2.2</b>\n"
            f"{'🔵 DRY-RUN' if DRY_RUN else '🟢 LIVE FUTURES'} | {TF} | {len(SYMBOLS)} جفت‌ارز\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>موجودی:</b> {balance:,.2f} USDT\n"
            f"📊 <b>ROI:</b> {roi:+.2f}%\n"
            f"🎯 <b>وضعیت:</b> {status}\n"
            f"📉 <b>Drawdown:</b> {dd_pct:.1f}% / {MAX_DD}%\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 <b>آمار امروز:</b>\n"
            f"   معاملات: {td.get('trades', 0)}\n"
            f"   برد: {td.get('wins', 0)} | باخت: {td.get('losses', 0)}\n"
            f"   وین‌ریت: {td.get('wr', 0):.1f}%\n"
            f"   سود/زیان: {'+' if td.get('pnl',0) >= 0 else ''}{td.get('pnl',0):.2f} USDT\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>پوزیشن‌های باز:</b> {pos}/{MAX_POS}\n"
            f"🧠 <b>هوش مصنوعی:</b> {ai.get('calls', 0)} درخواست\n"
            f"⏱️ <b>آپتایم:</b> {engine_stats.get('uptime', '?')}\n"
            f"🔄 <b>اسکن:</b> {engine_stats.get('cycles', 0)} چرخه\n"
        )
        
        # ➕ افزودن قیمت‌های لحظه‌ای (حداکثر ۵ نماد)
        if prices:
            price_lines = []
            for sym, price in list(prices.items())[:5]:
                price_lines.append(f"   {sym}: {price:,.2f}")
            if price_lines:
                msg += f"━━━━━━━━━━━━━━━━━━━━\n"
                msg += f"💹 <b>قیمت‌های لحظه‌ای:</b>\n"
                msg += "\n".join(price_lines)
                if len(prices) > 5:
                    msg += f"\n   ... و {len(prices)-5} نماد دیگر"
        
        msg += f"\n━━━━━━━━━━━━━━━━━━━━\n🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"
        
        self.send(msg, key="dashboard", force=True)
    
    @property
    def recent(self) -> List[Dict]:
        return list(self._queue)


TG = Alerts()

# ============================================================================
# EXCHANGE - Phemex Futures
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex    = None
        self._pc   : Dict[str, Tuple[float,float]] = {}
        self._ptl   = 2.5
        self._connect()

    def _connect(self):
        if not API_KEY:
            log.warning("⚠️  API_KEY خالی - Exchange غیرفعال (DRY_RUN)")
            return
        try:
            self._ex = ccxt.phemex({
                "apiKey"          : API_KEY,
                "secret"          : API_SECRET,
                "enableRateLimit" : True,
                "timeout"         : 30000,
                "options"         : {
                    "defaultType": "swap",  # ✅ FUTURES MODE
                },
            })
            
            if TESTNET:
                self._ex.set_sandbox_mode(True)
                log.info("⚠️  حالت Testnet فعال است")
            else:
                log.info("🌐 حالت Mainnet FUTURES فعال است")
            
            markets = self._ex.load_markets()
            log.info("✅ Exchange: Phemex Futures - %d بازار بارگذاری شد", len(markets))
            
            swap_markets = [s for s in markets.keys() if s.endswith(":USDT")]
            log.info("📊 %d بازار فیوچرز USDT یافت شد", len(swap_markets))
            
            for sym in SYMBOLS:
                if sym in markets:
                    log.info("   ✅ %s موجود است (Type: %s)", 
                            sym, markets[sym].get('type', '?'))
                else:
                    log.warning("   ❌ %s موجود نیست", sym)
                    
        except Exception as e:
            log.error("Exchange connect: %s", e)
            PERF.log_error(f"Exchange connect: {e}")
            self._ex = None

    def _retry_ohlcv(self, sym: str, tf: str, lim: int) -> List:
        for attempt in range(5):
            try:
                if self._ex is None:
                    raise ConnectionError("Exchange نیست")
                
                if sym not in self._ex.markets:
                    log.warning("بارگذاری مجدد بازارها...")
                    self._ex.load_markets()
                    
                return self._ex.fetch_ohlcv(sym, tf, limit=lim)
                
            except Exception as e:
                log.warning("ohlcv attempt %d [%s]: %s", attempt+1, sym, str(e)[:100])
                if attempt < 4:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"ohlcv {sym} شکست پس از 5 تلاش")

    def ohlcv(self, sym: str, tf: str, lim: int = 150) -> pd.DataFrame:
        raw = self._retry_ohlcv(sym, tf, lim)
        df  = pd.DataFrame(
            raw, columns=["ts","open","high","low","close","vol"]
        )
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df.dropna().reset_index(drop=True)

    def _fetch_price_raw(self, sym: str) -> float:
        for attempt in range(3):
            try:
                if self._ex is None:
                    raise ConnectionError
                return float(self._ex.fetch_ticker(sym)["last"])
            except Exception as e:
                log.warning("price attempt %d [%s]: %s", attempt+1, sym, e)
                if attempt < 2:
                    time.sleep(1.5 ** attempt)
        raise RuntimeError(f"price {sym} شکست")

    def price(self, sym: str) -> float:
        now = time.time()
        if sym in self._pc:
            p, t = self._pc[sym]
            if now - t < self._ptl:
                return p
        p = self._fetch_price_raw(sym)
        self._pc[sym] = (p, now)
        return p

    def prices_bulk(self, syms: List[str]) -> Dict[str, float]:
        out = {}
        if self._ex is None:
            return out
        try:
            tickers = self._ex.fetch_tickers(syms)
            for s, t in tickers.items():
                if t.get("last"):
                    p = float(t["last"])
                    out[s] = p
                    self._pc[s] = (p, time.time())
        except Exception:
            for s in syms:
                try:
                    out[s] = self.price(s)
                except Exception:
                    pass
        return out

    def balance(self) -> float:
        if self._ex is None or DRY_RUN:
            return 10_000.0
        try:
            b = self._ex.fetch_balance()
            for key in ("USDT","usdt","USD","usd"):
                if key in b and b[key].get("free"):
                    return float(b[key]["free"])
            return 0.0
        except Exception as e:
            log.warning("balance: %s", e)
            return 0.0

    def order(self, sym: str, side: str, qty: float) -> Optional[Dict]:
        if DRY_RUN:
            oid = f"dry_{uuid.uuid4().hex[:6]}"
            log.info("🔵 DRY %s %s %.5f → %s", side, sym, qty, oid)
            return {"id": oid, "ok": True}
        if self._ex is None:
            return None
        try:
            o = self._ex.create_order(sym, "market", side, qty)
            log.info("✅ Order %s", o.get("id"))
            return o
        except Exception as e:
            log.error("Order [%s %s]: %s", side, sym, e)
            TG.send(f"❌ سفارش خطا: {side} {sym}\n{e}", force=True)
            PERF.log_error(f"Order {side} {sym}: {e}")
            return None


EX = Exchange()

# ============================================================================
# SIGNAL
# ============================================================================
@dataclass
class Sig:
    action: str = "neutral"
    conf  : int = 0
    reason: str = ""
    risk  : str = "medium"
    src   : str = "tech"
    ind   : Dict = field(default_factory=dict)
    _bs   : int = 0
    _ss   : int = 0

    @property
    def ok(self) -> bool:
        return self.action in ("buy","sell") and self.conf >= 48

# ============================================================================
# TECHNICAL ANALYSIS
# ============================================================================
class Tech:
    def run(self, df: pd.DataFrame) -> Sig:
        if len(df) < 35:
            return Sig(reason="داده کم")

        c = df["close"]
        h = df["high"]
        l = df["low"]
        v = df["vol"]

        try:
            rsi_s            = IND.rsi(c, 14)
            macd_l, macd_s, macd_h = IND.macd(c)
            ema20            = IND.ema(c, 20)
            ema50            = IND.ema(c, 50)
            atr_s            = IND.atr(h, l, c, 14)
            bb_lo, bb_mid, bb_hi = IND.bbands(c, 20)
            stk, std_         = IND.stoch(h, l, c)
        except Exception as e:
            log.warning("Tech indicators خطا: %s", e)
            return Sig(reason=f"Indicator error: {e}")

        def sv(s):
            return IND.safe(s)

        rsi   = sv(rsi_s)
        mh    = sv(macd_h)
        ml    = sv(macd_l)
        ms    = sv(macd_s)
        e20   = sv(ema20)
        e50   = sv(ema50)
        atr   = sv(atr_s)
        bbl   = sv(bb_lo)
        bbh   = sv(bb_hi)
        sk    = sv(stk)
        price = float(c.iloc[-1])

        avg_v = float(v.rolling(20).mean().iloc[-1]) or 1.0
        vr    = float(v.iloc[-1]) / avg_v

        bs, ss = 0, 0
        tags   = []

        if   rsi < 25: bs += 35; tags.append(f"RSI={rsi:.0f}(OS++)")
        elif rsi < 35: bs += 25; tags.append(f"RSI={rsi:.0f}(OS)")
        elif rsi < 45: bs += 12
        elif rsi > 75: ss += 35; tags.append(f"RSI={rsi:.0f}(OB++)")
        elif rsi > 65: ss += 25; tags.append(f"RSI={rsi:.0f}(OB)")
        elif rsi > 55: ss += 12

        if   mh > 0 and ml > ms: bs += 25; tags.append("MACD↑")
        elif mh < 0 and ml < ms: ss += 25; tags.append("MACD↓")

        if   price > e20 > e50 > 0: bs += 22; tags.append("EMA↑")
        elif 0 < price < e20 < e50: ss += 22; tags.append("EMA↓")

        if   bbl > 0 and price <= bbl: bs += 18; tags.append("BB_lo")
        elif bbh > 0 and price >= bbh: ss += 18; tags.append("BB_hi")

        if   sk < 20: bs += 12; tags.append("Stoch_OS")
        elif sk > 80: ss += 12; tags.append("Stoch_OB")

        if vr > 1.5:
            if   bs > ss: bs += 10;  tags.append("Vol↑")
            elif ss > bs: ss += 10;  tags.append("Vol↑")

        ind = {
            "rsi":round(rsi,1), "macd_h":round(mh,5),
            "e20":round(e20,4), "e50":round(e50,4),
            "atr":round(atr,4), "vr":round(vr,2),
            "sk":round(sk,1), "bbl":round(bbl,4),
            "bbh":round(bbh,4), "price":round(price,4)
        }

        thr = 32
        
        if bs >= thr and bs > ss:
            cf = min(95, int(bs * 1.25))
            sig = Sig("buy", cf, "|".join(tags[:3]),
                     "low" if cf>75 else "medium", "tech", ind)
            sig._bs = bs
            sig._ss = ss
            return sig
            
        if ss >= thr and ss > bs:
            cf = min(95, int(ss * 1.25))
            sig = Sig("sell", cf, "|".join(tags[:3]),
                     "low" if cf>75 else "medium", "tech", ind)
            sig._bs = bs
            sig._ss = ss
            return sig

        sig = Sig(reason=f"B={bs} S={ss}", ind=ind)
        sig._bs = bs
        sig._ss = ss
        return sig


TECH = Tech()

# ============================================================================
# AI ENGINE
# ============================================================================
class AI:
    _CPK = 0.002

    def __init__(self):
        self._c     = None
        self._cache : Dict[str, Tuple[Sig, float]] = {}
        self._ttl   = 60
        self._calls = 0
        self._cost  = 0.0
        self._init()

    def _init(self):
        if not OAI_KEY:
            return
        try:
            from openai import OpenAI
            self._c = OpenAI(api_key=OAI_KEY, timeout=25.0)
            log.info("🧠 OpenAI آماده")
        except Exception as e:
            log.warning("OpenAI: %s", e)

    def analyze(self, df: pd.DataFrame,
                sym: str, tech: Sig, n_open: int) -> Sig:
        if not self._c:
            return tech

        ck = hashlib.md5(
            f"{sym}_{df['close'].iloc[-1]}_{df['ts'].iloc[-1]}".encode()
        ).hexdigest()
        if ck in self._cache:
            s, t = self._cache[ck]
            if time.time() - t < self._ttl:
                return s

        prompt = self._prompt(df, sym, tech, n_open)
        try:
            r = self._c.chat.completions.create(
                model      = "gpt-3.5-turbo",
                messages   = [
                    {"role":"system",
                     "content":"Professional crypto quant. "
                               "Reply ONLY valid JSON, no markdown."},
                    {"role":"user","content":prompt}
                ],
                temperature= 0.1,
                max_tokens = 100,
            )
            raw = r.choices[0].message.content.strip()
            raw = re.sub(r"```[a-z]*|```","",raw).strip()
            aj  = json.loads(raw)

            self._calls += 1
            toks = getattr(r.usage,"total_tokens",180)
            self._cost += toks/1000 * self._CPK

        except Exception as e:
            log.warning("AI خطا: %s", e)
            return tech

        result = self._merge(aj, tech)
        self._cache[ck] = (result, time.time())
        log.info("🤖 [%s] AI=%s(%d%%) Tech=%s(%d%%) → %s(%d%%)",
                 sym, aj.get("signal","?"),aj.get("confidence",0),
                 tech.action, tech.conf, result.action, result.conf)
        return result

    def _merge(self, ai: Dict, tech: Sig) -> Sig:
        aa  = ai.get("signal","neutral")
        ac  = int(ai.get("confidence",0))
        ar  = str(ai.get("reason",""))
        arl = ai.get("risk_level","medium")

        if aa == tech.action and aa != "neutral":
            cf = min(95, int((ac+tech.conf)/2*1.15))
            return Sig(aa, cf, f"AI+Tech:{ar}"[:70], arl, "combined", tech.ind)

        if aa != "neutral" and tech.action != "neutral" and aa != tech.action:
            return Sig("neutral",0,f"Conflict AI={aa} Tech={tech.action}",
                       src="conflict", ind=tech.ind)

        if aa != "neutral" and tech.action == "neutral" and ac >= 75:
            return Sig(aa, ac, ar, arl, "ai", tech.ind)

        return tech

    def _prompt(self, df: pd.DataFrame,
                sym: str, tech: Sig, n_open: int) -> str:
        rows = df.tail(8)[["ts","open","high","low","close","vol"]].copy()
        rows["ts"] = rows["ts"].astype(str)
        i = tech.ind
        return (
            f"Analyze {sym} {TF} FUTURES.\n"
            f"CANDLES: {json.dumps(rows.to_dict('records'),default=str)}\n"
            f"RSI={i.get('rsi')} MACD_h={i.get('macd_h')} "
            f"EMA20={i.get('e20')} ATR={i.get('atr')} VR={i.get('vr')}x\n"
            f"TECH_SIGNAL: {tech.action.upper()}({tech.conf}%) BS={tech._bs} SS={tech._ss}\n"
            f"CONTEXT: open={n_open}/{MAX_POS}\n"
            f'JSON: {{"signal":"buy"|"sell"|"neutral",'
            f'"confidence":0-100,"reason":"<12w",'
            f'"risk_level":"low"|"medium"|"high"}}'
        )

    @property
    def stats(self) -> Dict:
        return {"calls":self._calls,
                "cost":round(self._cost,5),
                "cache":len(self._cache)}


AI_ENG = AI()

# ============================================================================
# TIMER, DDG, SIZER, TRAIL, CORR (سایر اجزای استراتژی)
# ============================================================================
class Timer:
    """مدیریت زمان - جلوگیری از معامله در زمان‌های خاص (اختیاری)"""
    @staticmethod
    def allowed() -> bool:
        # همیشه مجاز (در آینده قابل توسعه)
        return True

class DDG:
    """مدیریت Drawdown"""
    def __init__(self, max_dd: float):
        self.max_dd = max_dd
        self.peak = 0.0
        self.halted = False

    def update(self, balance: float):
        if balance > self.peak:
            self.peak = balance
        if self.peak > 0:
            dd = (self.peak - balance) / self.peak * 100
            if dd >= self.max_dd:
                self.halted = True
        return self.stats

    @property
    def stats(self) -> Dict:
        return {"dd": round((self.peak - 10000) / 10000 * 100, 1) if self.peak else 0,
                "halted": self.halted,
                "peak": self.peak}

class Sizer:
    """محاسبه حجم معامله بر اساس ریسک"""
    @staticmethod
    def qty(balance: float, risk_pct: float, entry: float, stop: float) -> float:
        if stop >= entry:
            return 0.0
        risk_amount = balance * (risk_pct / 100)
        price_diff = entry - stop
        return risk_amount / price_diff if price_diff > 0 else 0.0

class Trail:
    """مدیریت حد ضرر متحرک"""
    @staticmethod
    def update(sl: float, price: float, entry: float, pct: float) -> float:
        new_sl = price * (1 - pct/100)
        return max(sl, new_sl) if price > entry else sl

class Corr:
    """همبستگی بین جفت‌ارزها (اختیاری)"""
    @staticmethod
    def pearson(dfs: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        # ساده‌سازی: فقط همبستگی با BTC
        res = {}
        btc_close = dfs.get("BTC/USDT:USDT", pd.DataFrame())["close"]
        if btc_close.empty:
            return res
        for sym, df in dfs.items():
            if sym == "BTC/USDT:USDT" or df.empty:
                continue
            try:
                corr = df["close"].corr(btc_close)
                res[sym] = round(corr, 2)
            except:
                pass
        return res

# ============================================================================
# ENGINE - هسته اصلی ربات
# ============================================================================
class Engine:
    def __init__(self):
        self.running = False
        self.cycle_count = 0
        self.start_time = time.time()
        self.dd = DDG(MAX_DD)
        self.balance = EX.balance()
        self.last_dashboard = 0
        self.last_activity = 0
        self._lock = threading.Lock()

    def run(self):
        """حلقه اصلی ربات"""
        self.running = True
        log.info("🔄 Engine شروع به کار کرد...")
        
        # ارسال پیام شروع
        TG.send("🚀 ربات Master-AI v5.2.2 شروع به کار کرد", force=True)
        
        while self.running:
            cycle_start = time.time()
            self.cycle_count += 1
            
            try:
                # 1. به‌روزرسانی موجودی و محاسبه drawdown
                self.balance = EX.balance()
                dd_stats = self.dd.update(self.balance)
                
                # 2. دریافت لیست معاملات باز
                open_trades = database.open_trades()
                
                # 3. اسکن جفت‌ارزها
                for sym in SYMBOLS:
                    if not self.running:
                        break
                    
                    try:
                        self._scan_symbol(sym, open_trades)
                    except Exception as e:
                        log.error(f"خطا در اسکن {sym}: {e}")
                        PERF.log_error(f"Scan {sym}: {e}")
                        continue
                
                # 4. ارسال گزارش دوره‌ای (هر ۵ دقیقه)
                if time.time() - self.last_dashboard > 300:
                    prices = EX.prices_bulk(SYMBOLS) if EX._ex is not None else {}
                    TG.send_dashboard(
                        engine_stats={
                            "dd": self.dd.stats,
                            "today": database.today(),
                            "ai": AI_ENG.stats,
                            "open_pos": len(open_trades),
                            "uptime": str(timedelta(seconds=int(time.time() - self.start_time))),
                            "cycles": self.cycle_count
                        },
                        balance=self.balance,
                        prices=prices
                    )
                    self.last_dashboard = time.time()
                
                # 5. نمایش لاگ وضعیت هر ۶۰ ثانیه
                if time.time() - self.last_activity > 60:
                    log.info("📊 چرخه %d | موجودی %.2f | پوزیشن %d | DD %.1f%%",
                            self.cycle_count, self.balance, len(open_trades),
                            dd_stats.get("dd", 0))
                    self.last_activity = time.time()
                
                # 6. زمان اسکن
                duration = time.time() - cycle_start
                PERF.log_scan(duration)
                
                # 7. مکث بین اسکن‌ها
                sleep_time = max(2, 60 - duration)  # حداقل ۲ ثانیه
                time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                log.info("🛑 دریافت سیگنال توقف")
                break
            except Exception as e:
                log.critical("❌ خطای fatal در حلقه اصلی: %s", e, exc_info=True)
                PERF.log_error(f"Engine loop: {e}")
                time.sleep(10)  # توقف موقت قبل از ادامه
        
        log.info("🛑 Engine متوقف شد")
        TG.send("🛑 ربات متوقف شد", force=True)

    def _scan_symbol(self, sym: str, open_trades: List[Dict]):
        """اسکن یک جفت‌ارز و تصمیم‌گیری"""
        # بررسی توقف به دلیل drawdown
        if self.dd.halted:
            return
        
        # بررسی تعداد معاملات باز
        if len(open_trades) >= MAX_POS:
            return
        
        # بررسی وجود معامله باز برای این نماد
        for t in open_trades:
            if t["symbol"] == sym:
                return  # قبلاً باز است
        
        # دریافت دیتا
        try:
            df = EX.ohlcv(sym, TF, 150)
            if len(df) < 35:
                return
        except Exception as e:
            log.warning(f"ohlcv {sym}: {e}")
            return
        
        # محاسبه سیگنال تکنیکال
        tech = TECH.run(df)
        if not tech.ok:
            return
        
        # اعمال هوش مصنوعی (اختیاری)
        sig = AI_ENG.analyze(df, sym, tech, len(open_trades))
        if not sig.ok:
            return
        
        # دریافت قیمت لحظه‌ای
        try:
            price = EX.price(sym)
        except Exception as e:
            log.warning(f"price {sym}: {e}")
            return
        
        # محاسبه حد ضرر و حد سود
        atr = tech.ind.get("atr", 0)
        if atr <= 0:
            atr = price * 0.01  # 1% fallback
        
        if sig.action == "buy":
            sl = price - atr * 1.5
            tp = price + atr * 2.5
        else:  # sell
            sl = price + atr * 1.5
            tp = price - atr * 2.5
        
        # محاسبه حجم معامله
        qty = Sizer.qty(self.balance, RISK_PCT, price, sl)
        if qty <= 0:
            return
        
        # ثبت سیگنال
        PERF.log_signal(sym, sig.action, sig.conf)
        log.info("📈 [%s] %s %.5f | SL=%.2f TP=%.2f | Qty=%.3f | Conf=%d%%",
                sym, sig.action.upper(), price, sl, tp, qty, sig.conf)
        
        # ارسال سفارش
        order = EX.order(sym, sig.action, qty)
        if order:
            # ذخیره در دیتابیس
            database.insert({
                "id": order.get("id", uuid.uuid4().hex),
                "symbol": sym,
                "side": sig.action,
                "entry": price,
                "qty": qty,
                "sl": sl,
                "tp": tp,
                "signal": sig.src,
                "conf": sig.conf
            })
            
            # ارسال هشدار تلگرام
            TG.send(
                f"📊 <b>معامله جدید</b>\n"
                f"نماد: {sym}\n"
                f"سمت: {'🟢 خرید' if sig.action=='buy' else '🔴 فروش'}\n"
                f"قیمت ورود: {price:,.2f}\n"
                f"حد ضرر: {sl:,.2f}\n"
                f"حد سود: {tp:,.2f}\n"
                f"حجم: {qty:.5f}\n"
                f"اطمینان: {sig.conf}%",
                key=f"trade_{sym}_{int(time.time())}"
            )

    def stop(self):
        self.running = False

# ============================================================================
# FLASK WEB SERVER
# ============================================================================
app = Flask(__name__)

# HTML dashboard (ساده)
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Master-AI Bot Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; background: #0d1117; color: #c9d1d9; padding: 20px; }
        h1 { color: #58a6ff; }
        .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; margin: 10px 0; }
        .status { color: #3fb950; }
        .error { color: #f85149; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { border: 1px solid #30363d; padding: 8px; text-align: left; }
        th { background: #21262d; }
        pre { background: #0d1117; padding: 10px; border-radius: 4px; overflow-x: auto; }
    </style>
</head>
<body>
    <h1>🤖 Master-AI Bot v5.2.2</h1>
    <div class="card">
        <h2>📊 وضعیت کلی</h2>
        <p><strong>موجودی:</strong> ${{ balance }}</p>
        <p><strong>وضعیت:</strong> <span class="status">{{ "🟢 فعال" if running else "🔴 متوقف" }}</span></p>
        <p><strong>چرخه:</strong> {{ cycles }}</p>
        <p><strong>آپتایم:</strong> {{ uptime }}</p>
        <p><strong>Drawdown:</strong> {{ dd }}% / {{ max_dd }}%</p>
    </div>
    <div class="card">
        <h2>📈 آمار امروز</h2>
        <p>معاملات: {{ stats.trades }} | برد: {{ stats.wins }} | باخت: {{ stats.losses }}</p>
        <p>وین‌ریت: {{ stats.wr }}% | سود/زیان: ${{ stats.pnl }}</p>
    </div>
    <div class="card">
        <h2>💹 قیمت‌های لحظه‌ای</h2>
        <ul>
        {% for sym, price in prices.items() %}
            <li>{{ sym }}: ${{ price }}</li>
        {% endfor %}
        </ul>
    </div>
    <div class="card">
        <h2>📋 معاملات باز</h2>
        <table>
            <tr><th>نماد</th><th>سمت</th><th>ورود</th><th>SL</th><th>TP</th><th>اطمینان</th></tr>
            {% for t in open_trades %}
            <tr>
                <td>{{ t.symbol }}</td>
                <td>{{ t.side }}</td>
                <td>${{ t.entry }}</td>
                <td>${{ t.sl }}</td>
                <td>${{ t.tp }}</td>
                <td>{{ t.conf }}%</td>
            </tr>
            {% endfor %}
        </table>
    </div>
    <div class="card">
        <h2>📝 فعالیت‌های اخیر</h2>
        <pre>{{ activity }}</pre>
    </div>
    <div class="card">
        <h2>🧠 AI</h2>
        <p>درخواست‌ها: {{ ai.calls }} | هزینه: ${{ ai.cost }} | کش: {{ ai.cache }}</p>
    </div>
</body>
</html>
"""

@app.route('/')
def dashboard():
    open_trades = database.open_trades()
    today = database.today()
    prices = EX.prices_bulk(SYMBOLS) if EX._ex is not None else {}
    dd_stats = engine.dd.stats if engine else {}
    return render_template_string(
        HTML_TEMPLATE,
        balance=f"{EX.balance():,.2f}",
        running=engine.running if engine else False,
        cycles=engine.cycle_count if engine else 0,
        uptime=str(timedelta(seconds=int(time.time() - engine.start_time))) if engine else "0",
        dd=dd_stats.get("dd", 0),
        max_dd=MAX_DD,
        stats=today,
        prices=prices,
        open_trades=open_trades,
        activity="\n".join([f"{a['time']} - {a['type']}: {a['msg']}" for a in database.recent_activity(10)]),
        ai=AI_ENG.stats
    )

@app.route('/api/status')
def api_status():
    """API برای دریافت وضعیت به صورت JSON"""
    return jsonify({
        "running": engine.running if engine else False,
        "balance": EX.balance(),
        "cycles": engine.cycle_count if engine else 0,
        "uptime": str(timedelta(seconds=int(time.time() - engine.start_time))) if engine else "0",
        "dd": engine.dd.stats if engine else {},
        "today": database.today(),
        "open_trades": database.open_trades(),
        "recent": TG.recent[-5:],
        "ai": AI_ENG.stats,
        "prices": EX.prices_bulk(SYMBOLS) if EX._ex is not None else {}
    })

@app.route('/api/control', methods=['POST'])
def control():
    """کنترل ربات از طریق API"""
    action = request.json.get('action', '').lower()
    if action == 'start':
        if not engine.running:
            threading.Thread(target=engine.run, daemon=True).start()
            return jsonify({"status": "started"})
    elif action == 'stop':
        if engine.running:
            engine.stop()
            return jsonify({"status": "stopped"})
    return jsonify({"status": "ok"})

# ============================================================================
# MAIN - ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    try:
        # ایجاد و راه‌اندازی Engine
        engine = Engine()
        
        # اجرای ربات در یک ترد جداگانه
        bot_thread = threading.Thread(target=engine.run, daemon=True)
        bot_thread.start()
        
        # راه‌اندازی Flask وب سرور
        log.info("🌐 Flask server starting on 0.0.0.0:%d", PORT)
        app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
        
    except KeyboardInterrupt:
        log.info("🛑 دریافت سیگنال Ctrl+C - خروج")
    except Exception as e:
        log.critical("❌ خطای fatal در main: %s", e, exc_info=True)
        sys.exit(1)
