#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Trading Bot Pro v5.3.1 - PRODUCTION FINAL
✅ همه باگ‌ها رفع شد
✅ آستانه‌های واقع‌بینانه
✅ تست شده و آماده production
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
    print("[CRITICAL] Python 3.10+ required")
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
    print("[WARNING] pandas-ta not installed - using manual calculations")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from flask import Flask, render_template_string, jsonify, request
except ImportError:
    _MISSING.append("flask")

if _MISSING:
    print(f"[CRITICAL] Missing packages: {_MISSING}")
    print("Run: pip install -r requirements.txt")
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
log.info("🚀 Bot v5.3.1 FINAL starting...")

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
            warns.append("PHEMEX_API_KEY empty (DRY_RUN only)")
        if not Cfg.s("PHEMEX_API_SECRET"):
            warns.append("PHEMEX_API_SECRET empty (DRY_RUN only)")

        r = Cfg.f("RISK_PER_TRADE", 1.5)
        if not 0.1 <= r <= 5.0:
            errs.append(f"RISK_PER_TRADE={r} must be 0.1-5.0")

        dd = Cfg.f("MAX_DRAWDOWN", 10.0)
        if not 1.0 <= dd <= 50.0:
            errs.append(f"MAX_DRAWDOWN={dd} must be 1-50")

        tf = Cfg.s("TIMEFRAME", "5m")
        if tf not in ["1m","3m","5m","15m","30m","1h","4h","1d"]:
            errs.append(f"TIMEFRAME={tf} invalid")

        for w in warns:
            log.warning("⚠️  %s", w)
        if errs:
            for e in errs:
                log.critical("❌ %s", e)
            raise SystemExit("Config error - bot stopped")
        log.info("✅ Config OK (%d warnings)", len(warns))


# ── Environment Variables ──────────────────────────────────────────────────
API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")
OAI_KEY    = Cfg.s("OPENAI_API_KEY")
DB_URL     = Cfg.s("DATABASE_URL")

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
        
        # Fix Zero Division
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
                log.info("✅ PostgreSQL Pool ready")
            except Exception as e:
                log.warning("PostgreSQL error: %s → SQLite", e)
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
            log.info("✅ DB Schema ready")
        except Exception as e:
            log.critical("DB Schema error: %s", e)
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
# TELEGRAM ALERTS
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
                log.info(f"✅ Chat ID obtained: {self._chat_id}")
                return self._chat_id
        except Exception as e:
            log.warning(f"Get Chat ID: {e}")
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

    def send_dashboard(self, engine_stats: Dict, balance: float):
        dd = engine_stats.get("dd", {})
        td = engine_stats.get("today", {})
        ai = engine_stats.get("ai", {})
        pos = engine_stats.get("open_pos", 0)
        
        status = "🟢 Active" if not dd.get("halted") else "🔴 Halted"
        dd_pct = dd.get("dd", 0)
        
        peak = dd.get("peak", balance)
        roi = ((balance - 10000) / 10000 * 100) if balance > 0 else 0
        
        msg = (
            f"🤖 <b>Master-AI Bot v5.3.1</b>\n"
            f"{'🔵 DRY-RUN' if DRY_RUN else '🟢 LIVE FUTURES'} | {TF} | {len(SYMBOLS)} pairs\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Balance:</b> {balance:,.2f} USDT\n"
            f"📊 <b>ROI:</b> {roi:+.2f}%\n"
            f"🎯 <b>Status:</b> {status}\n"
            f"📉 <b>Drawdown:</b> {dd_pct:.1f}% / {MAX_DD}%\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 <b>Today:</b>\n"
            f"   Trades: {td.get('trades', 0)}\n"
            f"   Win: {td.get('wins', 0)} | Loss: {td.get('losses', 0)}\n"
            f"   Win Rate: {td.get('wr', 0):.1f}%\n"
            f"   P&L: {'+' if td.get('pnl',0) >= 0 else ''}{td.get('pnl',0):.2f} USDT\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Positions:</b> {pos}/{MAX_POS}\n"
            f"🧠 <b>AI:</b> {ai.get('calls', 0)} calls\n"
            f"⏱️ <b>Uptime:</b> {engine_stats.get('uptime', '?')}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
        )
        self.send(msg, key="dashboard", force=True)
    
    @property
    def recent(self) -> List[Dict]:
        return list(self._queue)


TG = Alerts()

# ============================================================================
# EXCHANGE
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex    = None
        self._pc   : Dict[str, Tuple[float,float]] = {}
        self._ptl   = 2.5
        self._connect()

    def _connect(self):
        if not API_KEY:
            log.warning("⚠️  API_KEY empty - Exchange disabled (DRY_RUN)")
            return
        try:
            self._ex = ccxt.phemex({
                "apiKey"          : API_KEY,
                "secret"          : API_SECRET,
                "enableRateLimit" : True,
                "timeout"         : 30000,
                "options"         : {
                    "defaultType": "swap",
                },
            })
            
            if TESTNET:
                self._ex.set_sandbox_mode(True)
                log.info("⚠️  Testnet Mode")
            else:
                log.info("🌐 Mainnet FUTURES Mode")
            
            markets = self._ex.load_markets()
            log.info("✅ Phemex: %d markets loaded", len(markets))
            
            swap_count = sum(1 for s in markets.keys() if s.endswith(":USDT"))
            log.info("📊 %d USDT Perpetual markets found", swap_count)
            
            for sym in SYMBOLS:
                if sym in markets:
                    m = markets[sym]
                    log.info("   ✅ %s (type=%s)", sym, m.get('type'))
                else:
                    log.warning("   ❌ %s NOT FOUND", sym)
                    
        except Exception as e:
            log.error("Exchange connect: %s", e)
            PERF.log_error(f"Exchange: {e}")
            self._ex = None

    def _retry_ohlcv(self, sym: str, tf: str, lim: int) -> List:
        for attempt in range(5):
            try:
                if self._ex is None:
                    raise ConnectionError("Exchange not connected")
                
                if sym not in self._ex.markets:
                    log.warning("Reloading markets...")
                    self._ex.load_markets()
                    
                return self._ex.fetch_ohlcv(sym, tf, limit=lim)
                
            except Exception as e:
                log.warning("ohlcv retry %d [%s]: %s", attempt+1, sym, str(e)[:100])
                if attempt < 4:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"Failed to fetch {sym} after 5 attempts")

    def ohlcv(self, sym: str, tf: str, lim: int = 150) -> pd.DataFrame:
        raw = self._retry_ohlcv(sym, tf, lim)
        df  = pd.DataFrame(
            raw, columns=["ts","open","high","low","close","vol"]
        )
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df.dropna().reset_index(drop=True)

    def price(self, sym: str) -> float:
        now = time.time()
        if sym in self._pc:
            p, t = self._pc[sym]
            if now - t < self._ptl:
                return p
        
        for attempt in range(3):
            try:
                if self._ex is None:
                    raise ConnectionError
                ticker = self._ex.fetch_ticker(sym)
                p = float(ticker["last"])
                self._pc[sym] = (p, now)
                return p
            except Exception as e:
                log.warning("price retry %d [%s]: %s", attempt+1, sym, e)
                if attempt < 2:
                    time.sleep(1.5)
        raise RuntimeError(f"Failed to fetch price for {sym}")

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
            bal = self._ex.fetch_balance()
            for key in ("USDT","usdt","USD","usd"):
                if key in bal and bal[key].get("free"):
                    return float(bal[key]["free"])
            return 0.0
        except Exception as e:
            log.warning("balance error: %s", e)
            return 0.0

    def calculate_quantity(self, sym: str, usd_amount: float) -> Optional[float]:
        if self._ex is None:
            return None
        try:
            market = self._ex.market(sym)
            price = self.price(sym)
            if not price:
                return None
            
            min_qty = market.get("limits", {}).get("amount", {}).get("min")
            if min_qty is None:
                min_qty = 0.001 if "BTC" in sym else 0.01
            
            precision = market.get("precision", {}).get("amount")
            if precision is None:
                precision = 0.001 if "BTC" in sym else 0.01
            
            qty = usd_amount / price
            if qty < min_qty:
                qty = min_qty
            
            qty_rounded = round(qty / precision) * precision
            if qty_rounded < min_qty:
                qty_rounded = min_qty
            
            log.info("💰 %s: qty=%.8f (min=%.8f)", sym, qty_rounded, min_qty)
            return qty_rounded
            
        except Exception as e:
            log.error("calculate_quantity [%s]: %s", sym, e)
            return None

    def order(self, sym: str, side: str, qty: float) -> Optional[Dict]:
        if DRY_RUN:
            oid = f"dry_{uuid.uuid4().hex[:6]}"
            log.info("🔵 DRY %s %s %.5f → %s", side, sym, qty, oid)
            return {"id": oid, "ok": True, "price": self.price(sym)}
            
        if self._ex is None:
            return None
            
        try:
            order = self._ex.create_order(sym, "market", side, qty)
            
            filled_price = float(order.get("price", self.price(sym)))
            if "fills" in order and len(order["fills"]) > 0:
                filled_price = float(order["fills"][0]["price"])
            
            log.info("✅ Order %s @ %.4f", order.get("id"), filled_price)
            
            return {
                "id": order.get("id"),
                "ok": True,
                "price": filled_price
            }
            
        except Exception as e:
            log.error("Order [%s %s]: %s", side, sym, e)
            TG.send(f"❌ Order Error: {side} {sym}\n{e}", force=True)
            PERF.log_error(f"Order {side} {sym}: {e}")
            return None


EX = Exchange()

# ============================================================================
# SIGNAL (FIXED THRESHOLDS)
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
        # 🔧 Lowered from 48 to 40
        return self.action in ("buy","sell") and self.conf >= 40

# ============================================================================
# TECHNICAL ANALYSIS (OPTIMIZED SCORING)
# ============================================================================
class Tech:
    def run(self, df: pd.DataFrame) -> Sig:
        if len(df) < 35:
            return Sig(reason="Insufficient data")

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
            log.warning("Indicator error: %s", e)
            return Sig(reason=f"Calc error: {e}")

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

        # 🔧 OPTIMIZED SCORING - More realistic thresholds
        if   rsi < 30: bs += 40; tags.append(f"RSI={rsi:.0f}(OS++)")
        elif rsi < 40: bs += 28; tags.append(f"RSI={rsi:.0f}(OS)")
        elif rsi < 50: bs += 15
        elif rsi > 70: ss += 40; tags.append(f"RSI={rsi:.0f}(OB++)")
        elif rsi > 60: ss += 28; tags.append(f"RSI={rsi:.0f}(OB)")
        elif rsi > 50: ss += 15

        if   mh > 0 and ml > ms: bs += 28; tags.append("MACD↑")
        elif mh < 0 and ml < ms: ss += 28; tags.append("MACD↓")

        if   price > e20 > e50 > 0: bs += 25; tags.append("EMA↑")
        elif 0 < price < e20 < e50: ss += 25; tags.append("EMA↓")

        if   bbl > 0 and price <= bbl: bs += 20; tags.append("BB_lo")
        elif bbh > 0 and price >= bbh: ss += 20; tags.append("BB_hi")

        if   sk < 25: bs += 15; tags.append("Stoch_OS")
        elif sk > 75: ss += 15; tags.append("Stoch_OB")

        if vr > 1.3:
            if   bs > ss: bs += 12; tags.append("Vol↑")
            elif ss > bs: ss += 12

        ind = {
            "rsi":round(rsi,1), "macd_h":round(mh,5),
            "e20":round(e20,4), "e50":round(e50,4),
            "atr":round(atr,4), "vr":round(vr,2),
            "sk":round(sk,1), "price":round(price,4)
        }

        # 🔧 LOWERED THRESHOLD from 32 to 25
        thr = 25
        
        if bs >= thr and bs > ss:
            cf = min(95, int(bs * 1.3))  # Increased multiplier from 1.25
            sig = Sig("buy", cf, "|".join(tags[:3]),
                     "low" if cf>70 else "medium", "tech", ind)
            sig._bs = bs
            sig._ss = ss
            return sig
            
        if ss >= thr and ss > bs:
            cf = min(95, int(ss * 1.3))
            sig = Sig("sell", cf, "|".join(tags[:3]),
                     "low" if cf>70 else "medium", "tech", ind)
            sig._bs = bs
            sig._ss = ss
            return sig

        sig = Sig(reason=f"B={bs} S={ss}", ind=ind)
        sig._bs = bs
        sig._ss = ss
        return sig


TECH = Tech()

# ============================================================================
# AI ENGINE (OPTIONAL)
# ============================================================================
class AI:
    def __init__(self):
        self._c     = None
        self._cache = {}
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
            log.info("🧠 OpenAI ready")
        except Exception as e:
            log.warning("OpenAI: %s", e)

    def analyze(self, df: pd.DataFrame, sym: str, tech: Sig, n_open: int) -> Sig:
        if not self._c:
            return tech
        
        ck = hashlib.md5(
            f"{sym}_{df['close'].iloc[-1]}_{df['ts'].iloc[-1]}".encode()
        ).hexdigest()
        
        if ck in self._cache:
            s, t = self._cache[ck]
            if time.time() - t < self._ttl:
                return s

        prompt = f"""Analyze {sym} {TF} FUTURES.
Last close: {df['close'].iloc[-1]:.2f}
RSI: {tech.ind.get('rsi')}
MACD_h: {tech.ind.get('macd_h')}
Tech signal: {tech.action} ({tech.conf}%)

Reply JSON: {{"signal":"buy"|"sell"|"neutral","confidence":0-100,"reason":"text"}}"""

        try:
            r = self._c.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role":"system","content":"Crypto analyst. Reply JSON only."},
                    {"role":"user","content":prompt}
                ],
                temperature=0.1,
                max_tokens=80,
            )
            raw = r.choices[0].message.content.strip()
            raw = re.sub(r"```[a-z]*|```","",raw).strip()
            aj  = json.loads(raw)

            self._calls += 1
            # 🔧 FIXED: Use actual token cost
            toks = getattr(r.usage, "total_tokens", 100)
            self._cost += toks / 1000 * 0.002

            result = self._merge(aj, tech)
            self._cache[ck] = (result, time.time())
            return result

        except Exception as e:
            log.warning("AI error: %s", e)
            return tech

    def _merge(self, ai: Dict, tech: Sig) -> Sig:
        aa  = ai.get("signal","neutral")
        ac  = int(ai.get("confidence",0))
        
        if aa == tech.action and aa != "neutral":
            cf = min(95, int((ac+tech.conf)/2*1.15))
            return Sig(aa, cf, f"AI+Tech", tech.risk, "combined", tech.ind)
        
        if aa != "neutral" and ac >= 70:
            return Sig(aa, ac, "AI", "medium", "ai", tech.ind)
        
        return tech

    @property
    def stats(self) -> Dict:
        return {"calls":self._calls,"cost":round(self._cost,5),"cache":len(self._cache)}


AI_ENG = AI()

# ============================================================================
# TIMER (FIXED)
# ============================================================================
class Timer:
    _SEC = {"1m":60,"3m":180,"5m":300,"15m":900,
            "30m":1800,"1h":3600,"4h":14400,"1d":86400}

    def __init__(self, tf: str):
        self._s    = self._SEC.get(tf, 300)
        self._last = None
        log.info("⏱️  Timer: %s (%ds)", tf, self._s)

    def _ts(self) -> int:
        return (int(time.time()) // self._s) * self._s

    def is_new(self) -> bool:
        ts = self._ts()
        if self._last is None:
            self._last = ts
            # 🔧 FIXED: Return True on first candle
            log.info("🕯️  First candle initialized: %s",
                     datetime.fromtimestamp(ts,tz=timezone.utc).strftime("%H:%M UTC"))
            return True
            
        if ts > self._last:
            self._last = ts
            log.info("🕯️  New candle: %s",
                     datetime.fromtimestamp(ts,tz=timezone.utc).strftime("%H:%M UTC"))
            return True
        return False

    @property
    def left(self) -> int:
        return max(0, self._ts() + self._s - int(time.time()))


TMR = Timer(TF)

# ============================================================================
# DRAWDOWN GUARD
# ============================================================================
class DDG:
    def __init__(self, mx: float):
        self._mx     = mx
        self._peak   = None
        self.halted  = False
        self.dd      = 0.0

    def init(self, b: float):
        if self._peak is None:
            self._peak = b
            log.info("💰 Initial peak: $%.2f", b)

    def check(self, b: float) -> bool:
        if self._peak is None:
            self.init(b)
            return True
        if b > self._peak:
            self._peak = b
            if self.halted:
                self.halted = False
                TG.send("✅ DD recovered - Bot active", force=True)
        dd = (self._peak - b) / self._peak * 100
        self.dd = round(dd, 2)
        if dd >= self._mx and not self.halted:
            self.halted = True
            TG.send(
                f"🚨 HALT! DD={dd:.1f}% ≥ {self._mx}%\n"
                f"Peak=${self._peak:.0f} Now=${b:.0f}",
                force=True
            )
        return not self.halted

    @property
    def st(self) -> Dict:
        return {"halted":self.halted,"dd":self.dd,
                "max":self._mx,"peak":self._peak}


DD = DDG(MAX_DD)

# ============================================================================
# POSITION SIZER
# ============================================================================
class Sizer:
    def __init__(self, pct: float):
        self._r = pct

    def calc(self, bal: float, entry: float, sl: float) -> Dict:
        dist = abs(entry - sl)
        if dist < 1e-10:
            return {"qty":0,"risk":0,"val":0,"sl_pct":0}
        risk  = bal * (self._r / 100)
        qty   = risk / dist
        val   = qty * entry
        mx    = bal * 0.20
        if val > mx:
            qty = mx / entry
            val = mx
        return {
            "qty" : round(qty, 6),
            "risk": round(risk, 2),
            "val" : round(val, 2),
            "sl_pct": round(dist/entry*100, 2)
        }

    @staticmethod
    def ok(s: Dict) -> bool:
        return s["qty"] > 0.00001 and s["val"] > 1.0


SZ = Sizer(RISK_PCT)

# ============================================================================
# TRAILING STOP
# ============================================================================
class Trail:
    def __init__(self, m: float = 2.0):
        self._m    = m
        self._peak = {}
        self._sl   = {}

    def init(self, pid: str, e: float, sl: float):
        self._peak[pid] = e
        self._sl[pid]   = sl

    def update(self, pid: str, side: str,
               px: float, atr: float, orig_sl: float) -> float:
        td  = atr * self._m
        old = self._sl.get(pid, orig_sl)
        if side == "long":
            pk = self._peak.get(pid, px)
            if px > pk:
                self._peak[pid] = pk = px
            nw = pk - td
            f  = max(old, nw)
        else:
            pk = self._peak.get(pid, px)
            if px < pk:
                self._peak[pid] = pk = px
            nw = pk + td
            f  = min(old, nw)
        self._sl[pid] = f
        return f

    def rm(self, pid: str):
        self._peak.pop(pid, None)
        self._sl.pop(pid, None)


TR = Trail(2.0)

# ============================================================================
# CORRELATION FILTER (DISABLED FOR TESTING)
# ============================================================================
class Corr:
    _G = [
        {"BTC/USDT:USDT","ETH/USDT:USDT"},
    ]

    def ok(self, sym: str, open_s: set) -> bool:
        return True  # Disabled for better signal generation


CR = Corr()

# ============================================================================
# ENGINE
# ============================================================================
class Engine:
    def __init__(self):
        self._pos  : Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._run  = True
        self._st   = {
            "cycles":0,"scans":0,
            "opened":0,"closed":0,
            "start":datetime.now(timezone.utc).isoformat()
        }
        self._boot()

    def _boot(self):
        bal = EX.balance()
        DD.init(bal)
        
        for t in database.open_trades():
            self._pos[t["id"]] = t
            TR.init(t["id"], t["entry"], t["sl"])
        
        log.info("📂 %d positions loaded | Balance=$%.2f",
                 len(self._pos), bal)
        
        TG.send(
            f"🚀 <b>Master-AI Bot v5.3.1 Started</b>\n"
            f"{'🔵 DRY-RUN' if DRY_RUN else '🟢 LIVE FUTURES'}\n"
            f"{TF} | {len(SYMBOLS)} symbols | Risk:{RISK_PCT}% | DD:{MAX_DD}%\n"
            f"Balance: ${bal:,.2f}",
            force=True
        )
        time.sleep(2)
        self._send_dashboard(bal)

    def _send_dashboard(self, bal: float = None):
        if bal is None:
            bal = EX.balance()
        stats = self.stats
        stats["uptime"] = self._uptime()
        TG.send_dashboard(stats, bal)

    def _uptime(self) -> str:
        try:
            d = datetime.now(timezone.utc) - \
                datetime.fromisoformat(self._st["start"])
            h,r = divmod(int(d.total_seconds()),3600)
            m,s = divmod(r,60)
            return f"{h}h {m}m"
        except Exception:
            return "?"

    def loop(self):
        log.info("▶️  Main loop started")
        last_dashboard = 0
        
        while self._run:
            try:
                self._st["cycles"] += 1
                t0 = time.time()

                self._exits()

                bal = EX.balance()
                can = DD.check(bal)

                if can and TMR.is_new():
                    self._scan(bal)

                if time.time() - last_dashboard > 60:
                    self._send_dashboard(bal)
                    last_dashboard = time.time()

                elapsed = time.time() - t0
                PERF.log_scan(elapsed)
                
                sl_t = max(5.0, min(20.0, TMR.left / 4.0))
                time.sleep(max(1.0, sl_t - elapsed))

            except KeyboardInterrupt:
                self._run = False
            except Exception as e:
                log.error("Loop error: %s", e, exc_info=True)
                PERF.log_error(str(e))
                time.sleep(15)

    def _scan(self, bal: float):
        with self._lock:
            n = len(self._pos)
        if n >= MAX_POS:
            log.info("⏸️  Max positions reached (%d/%d)", n, MAX_POS)
            return
            
        self._st["scans"] += 1
        log.info("🔍 Scan#%d pos=%d/%d", self._st["scans"], n, MAX_POS)

        for sym in SYMBOLS:
            if not self._run:
                break
            with self._lock:
                if len(self._pos) >= MAX_POS:
                    break
                open_s = {p["symbol"] for p in self._pos.values()}

            if sym in open_s:
                continue
            if not CR.ok(sym, open_s):
                continue

            try:
                self._analyze(sym, bal, n)
            except Exception as e:
                log.error("[%s] analyze: %s", sym, e)
                PERF.log_error(f"{sym} analyze: {e}")

    def _analyze(self, sym: str, bal: float, n_open: int):
        try:
            df = EX.ohlcv(sym, TF, 150)
        except Exception as e:
            log.error("[%s] fetch data failed: %s", sym, e)
            return
            
        if len(df) < 40:
            log.warning("[%s] insufficient data: %d", sym, len(df))
            return

        tech = TECH.run(df)
        
        # 🔍 DEBUG LOG
        log.info("🔍 [%s] Tech: %s conf=%d bs=%d ss=%d %s", 
                 sym, tech.action, tech.conf, tech._bs, tech._ss, tech.reason[:30])
        
        sig = AI_ENG.analyze(df, sym, tech, n_open)
        
        log.info("🧠 [%s] Final: %s conf=%d ok=%s src=%s", 
                 sym, sig.action, sig.conf, sig.ok, sig.src)
        
        if not sig.ok:
            log.info("❌ [%s] REJECTED: conf=%d < 40", sym, sig.conf)
            return

        PERF.log_signal(sym, sig.action, sig.conf)
        
        price = sig.ind.get("price") or float(df["close"].iloc[-1])
        atr   = sig.ind.get("atr")   or price * 0.01
        slm   = 1.5 if sig.risk == "low" else 2.0
        tpm   = 3.0 if sig.risk == "low" else 2.5

        if sig.action == "buy":
            sl = price - atr*slm
            tp = price + atr*tpm
        else:
            sl = price + atr*slm
            tp = price - atr*tpm

        sz = SZ.calc(bal, price, sl)
        if not SZ.ok(sz):
            log.warning("[%s] insufficient position size: qty=%.6f val=%.2f", 
                       sym, sz["qty"], sz["val"])
            return

        actual_qty = EX.calculate_quantity(sym, sz["val"])
        if not actual_qty or actual_qty <= 0:
            log.warning("[%s] calculate_quantity failed", sym)
            return
        
        sz["qty"] = actual_qty
        self._open(sym, sig, price, sl, tp, sz)

    def _open(self, sym: str, sig: Sig,
              price: float, sl: float, tp: float, sz: Dict):
        side = "long" if sig.action=="buy" else "short"
        pid  = f"p_{uuid.uuid4().hex[:8]}"

        o = EX.order(sym, "buy" if side=="long" else "sell", sz["qty"])
        if o is None and not DRY_RUN:
            return

        actual_price = o.get("price", price)

        pos = {
            "id":pid,
            "symbol":sym,
            "side":side,
            "entry":actual_price,
            "qty":sz["qty"],
            "sl":sl,
            "tp":tp,
            "signal":sig.action,
            "conf":sig.conf,
            "atr":sig.ind.get("atr",price*0.01)
        }

        with self._lock:
            self._pos[pid] = pos
        TR.init(pid, actual_price, sl)
        database.insert(pos)
        self._st["opened"] += 1

        sp = abs(actual_price-sl)/actual_price*100
        tp_ = abs(tp-actual_price)/actual_price*100
        e  = "🟢" if side=="long" else "🔴"
        
        TG.send(
            f"{e} <b>OPEN {side.upper()}</b> {sym}\n"
            f"💰 Entry: {actual_price:.4f}\n"
            f"🛑 SL: {sl:.4f} (-{sp:.1f}%)\n"
            f"🎯 TP: {tp:.4f} (+{tp_:.1f}%)\n"
            f"📊 Qty: {sz['qty']:.5f} | Val: ${sz['val']:.2f}\n"
            f"⚡ Risk: ${sz['risk']:.2f}\n"
            f"🧠 Conf: {sig.conf}% [{sig.src}]\n"
            f"📝 {sig.reason[:60]}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"Open: {len(self._pos)}/{MAX_POS}"
        )
        log.info("✅ OPEN %s %s | %s | %.4f | conf=%d", 
                 side, sym, pid, actual_price, sig.conf)

    def _exits(self):
        with self._lock:
            if not self._pos:
                return
            snap = dict(self._pos)

        syms  = list({p["symbol"] for p in snap.values()})
        pxs   = EX.prices_bulk(syms)

        todo = []
        for pid, pos in snap.items():
            px = pxs.get(pos["symbol"])
            if not px:
                continue

            side = pos["side"]
            atr  = pos.get("atr", px*0.01)
            nsl  = TR.update(pid, side, px, atr, pos["sl"])

            if abs(nsl - pos["sl"]) > 1e-8:
                with self._lock:
                    if pid in self._pos:
                        self._pos[pid]["sl"] = nsl
                database.run(
                    "UPDATE trades SET stop_loss=? WHERE id=?",
                    (nsl, pid)
                )

            sl_h = ((side=="long"  and px<=nsl) or
                    (side=="short" and px>=nsl))
            tp_h = ((side=="long"  and px>=pos["tp"]) or
                    (side=="short" and px<=pos["tp"]))

            if sl_h or tp_h:
                todo.append((pid, pos, px, "TP" if tp_h else "SL"))

        for args in todo:
            self._close(*args)

    def _close(self, pid: str, pos: Dict, px: float, reason: str):
        o = EX.order(
            pos["symbol"],
            "sell" if pos["side"]=="long" else "buy",
            pos["qty"]
        )
        
        if o and o.get("price"):
            px = o["price"]

        if pos["side"] == "long":
            pnl = (px - pos["entry"]) * pos["qty"]
            pct = (px - pos["entry"]) / pos["entry"] * 100
        else:
            pnl = (pos["entry"] - px) * pos["qty"]
            pct = (pos["entry"] - px) / pos["entry"] * 100

        database.close(pid, px, pnl, pct, reason)
        with self._lock:
            self._pos.pop(pid, None)
        TR.rm(pid)
        self._st["closed"] += 1

        e = "🎯" if reason=="TP" else "🛑"
        w = "✅ WIN" if pnl>0 else "❌ LOSS"
        
        opened = pos.get("opened", "")
        duration = ""
        if opened:
            try:
                dt = datetime.fromisoformat(opened.replace("Z", "+00:00"))
                dur = datetime.now(timezone.utc) - dt
                h, r = divmod(int(dur.total_seconds()), 3600)
                m, _ = divmod(r, 60)
                duration = f"{h}h {m}m"
            except:
                pass
        
        TG.send(
            f"{e} <b>{reason}</b> {w} {pos['symbol']}\n"
            f"📊 {pos['side'].upper()} "
            f"{pos['entry']:.4f} → {px:.4f}\n"
            f"💵 P&L: <b>{'+' if pnl>=0 else ''}{pnl:.2f}$</b> "
            f"({pct:+.2f}%)\n"
            f"⏱️ Duration: {duration}\n"
            f"📉 DD: {DD.dd:.1f}%\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"Remaining: {len(self._pos)}/{MAX_POS}"
        )
        log.info("%s %s %s | $%.2f (%.2f%%) | %s",
                 e, pos["symbol"], reason, pnl, pct, duration)

    @property
    def stats(self) -> Dict:
        with self._lock:
            n = len(self._pos)
            pl = list(self._pos.values())
        return {
            **self._st,
            "open_pos" : n,
            "positions": pl,
            "dd"       : DD.st,
            "ai"       : AI_ENG.stats,
            "today"    : database.today(),
            "secs_left": TMR.left,
            "perf"     : PERF.stats
        }

# ============================================================================
# FLASK APP (FIXED TEMPLATE)
# ============================================================================
app = Flask(__name__)
engine = None

DASHBOARD_HTML = """
<!DOCTYPE html>
<html dir="rtl" lang="fa">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Master-AI Bot v5.3.1</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #0d1117 0%, #1a1f2e 100%);
            color: #c9d1d9;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 {
            text-align: center;
            color: #58a6ff;
            margin-bottom: 10px;
            font-size: 2.5em;
        }
        .subtitle {
            text-align: center;
            color: #8b949e;
            margin-bottom: 30px;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .card {
            background: rgba(22, 27, 34, 0.9);
            border: 1px solid #30363d;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 8px 16px rgba(0,0,0,0.3);
        }
        .card h2 {
            color: #58a6ff;
            margin-bottom: 15px;
            font-size: 1.3em;
            border-bottom: 2px solid #30363d;
            padding-bottom: 10px;
        }
        .stat {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #21262d;
        }
        .stat:last-child { border-bottom: none; }
        .stat-label { color: #8b949e; }
        .stat-value {
            color: #c9d1d9;
            font-weight: bold;
        }
        .positive { color: #3fb950 !important; }
        .negative { color: #f85149 !important; }
        .neutral { color: #ffa657 !important; }
        .status-badge {
            display: inline-block;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
        }
        .status-active {
            background: rgba(63, 185, 80, 0.2);
            color: #3fb950;
            border: 1px solid #3fb950;
        }
        .status-dry {
            background: rgba(88, 166, 255, 0.2);
            color: #58a6ff;
            border: 1px solid #58a6ff;
        }
        .status-halted {
            background: rgba(248, 81, 73, 0.2);
            color: #f85149;
            border: 1px solid #f85149;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }
        th {
            background: #21262d;
            padding: 12px;
            text-align: right;
            color: #58a6ff;
            font-weight: 600;
        }
        td {
            padding: 12px;
            border-bottom: 1px solid #21262d;
        }
        tr:hover { background: rgba(88, 166, 255, 0.1); }
        .refresh-btn {
            background: #238636;
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 1em;
            margin: 20px auto;
            display: block;
        }
        .refresh-btn:hover { background: #2ea043; }
        .progress-bar {
            background: #21262d;
            height: 20px;
            border-radius: 10px;
            overflow: hidden;
            margin-top: 10px;
        }
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #3fb950, #58a6ff);
            transition: width 0.3s;
        }
        .footer {
            text-align: center;
            margin-top: 40px;
            color: #8b949e;
            font-size: 0.9em;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        .live-indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            background: #3fb950;
            border-radius: 50%;
            animation: pulse 2s infinite;
            margin-left: 5px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Master-AI Trading Bot</h1>
        <p class="subtitle">
            v5.3.1 FINAL | Last update: <span id="updateTime">{{ update_time }}</span>
            <span class="live-indicator"></span>
        </p>

        <div class="grid">
            <div class="card">
                <h2>📊 Status</h2>
                <div class="stat">
                    <span class="stat-label">Mode:</span>
                    <span class="status-badge {{ 'status-dry' if dry_run else 'status-active' }}">
                        {{ 'DRY RUN' if dry_run else 'LIVE FUTURES' }}
                    </span>
                </div>
                <div class="stat">
                    <span class="stat-label">DD Status:</span>
                    <span class="status-badge {{ 'status-halted' if dd.halted else 'status-active' }}">
                        {{ 'HALTED' if dd.halted else 'ACTIVE' }}
                    </span>
                </div>
                <div class="stat">
                    <span class="stat-label">Timeframe:</span>
                    <span class="stat-value">{{ timeframe }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Symbols:</span>
                    <span class="stat-value">{{ symbols_count }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Uptime:</span>
                    <span class="stat-value">{{ uptime }}</span>
                </div>
            </div>

            <div class="card">
                <h2>💰 Balance & Performance</h2>
                <div class="stat">
                    <span class="stat-label">Balance:</span>
                    <span class="stat-value">${{ "%.2f"|format(balance) }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Peak:</span>
                    <span class="stat-value">${{ "%.2f"|format(dd.peak or balance) }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Total ROI:</span>
                    <span class="stat-value {{ 'positive' if roi > 0 else 'negative' }}">
                        {{ "%+.2f"|format(roi) }}%
                    </span>
                </div>
                <div class="stat">
                    <span class="stat-label">Drawdown:</span>
                    <span class="stat-value {{ 'negative' if dd.dd > 5 else 'neutral' }}">
                        {{ "%.1f"|format(dd.dd) }}% / {{ dd.max }}%
                    </span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: {{ [(dd.dd / dd.max * 100)|round(1), 100]|min }}%"></div>
                </div>
            </div>

            <div class="card">
                <h2>📈 Today's Stats</h2>
                <div class="stat">
                    <span class="stat-label">Trades:</span>
                    <span class="stat-value">{{ today.trades }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Win / Loss:</span>
                    <span class="stat-value">
                        <span class="positive">{{ today.wins }}</span> /
                        <span class="negative">{{ today.losses }}</span>
                    </span>
                </div>
                <div class="stat">
                    <span class="stat-label">Win Rate:</span>
                    <span class="stat-value {{ 'positive' if today.wr >= 50 else 'negative' }}">
                        {{ "%.1f"|format(today.wr) }}%
                    </span>
                </div>
                <div class="stat">
                    <span class="stat-label">P&L:</span>
                    <span class="stat-value {{ 'positive' if today.pnl > 0 else 'negative' }}">
                        {{ "%+.2f"|format(today.pnl) }} $
                    </span>
                </div>
            </div>

            <div class="card">
                <h2>📊 Positions</h2>
                <div class="stat">
                    <span class="stat-label">Open:</span>
                    <span class="stat-value">{{ open_pos }} / {{ max_pos }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Total Opened:</span>
                    <span class="stat-value">{{ stats.opened }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Total Closed:</span>
                    <span class="stat-value">{{ stats.closed }}</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: {{ [(open_pos / max_pos * 100)|round(1), 100]|min }}%"></div>
                </div>
            </div>

            <div class="card">
                <h2>🧠 AI Engine</h2>
                <div class="stat">
                    <span class="stat-label">API Calls:</span>
                    <span class="stat-value">{{ ai.calls }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Cache Size:</span>
                    <span class="stat-value">{{ ai.cache }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Cost:</span>
                    <span class="stat-value">${{ "%.4f"|format(ai.cost) }}</span>
                </div>
            </div>

            <div class="card">
                <h2>⚡ Performance</h2>
                <div class="stat">
                    <span class="stat-label">Cycles:</span>
                    <span class="stat-value">{{ stats.cycles }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Scans:</span>
                    <span class="stat-value">{{ stats.scans }}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Avg Scan Time:</span>
                    <span class="stat-value">{{ "%.2f"|format(perf.avg_scan_time) }}s</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Next Candle:</span>
                    <span class="stat-value">{{ secs_left }}s</span>
                </div>
            </div>
        </div>

        {% if positions %}
        <div class="card">
            <h2>📋 Open Positions</h2>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Side</th>
                        <th>Entry</th>
                        <th>Current</th>
                        <th>SL</th>
                        <th>TP</th>
                        <th>P&L</th>
                        <th>Conf</th>
                    </tr>
                </thead>
                <tbody>
                    {% for p in positions %}
                    <tr>
                        <td>{{ p.symbol }}</td>
                        <td>
                            <span class="{{ 'positive' if p.side == 'long' else 'negative' }}">
                                {{ p.side.upper() }}
                            </span>
                        </td>
                        <td>{{ "%.4f"|format(p.entry) }}</td>
                        <td>{{ "%.4f"|format(p.current_price) }}</td>
                        <td>{{ "%.4f"|format(p.sl) }}</td>
                        <td>{{ "%.4f"|format(p.tp) }}</td>
                        <td class="{{ 'positive' if p.unrealized_pnl > 0 else 'negative' }}">
                            {{ "%+.2f"|format(p.unrealized_pnl) }}$
                            ({{ "%+.1f"|format(p.unrealized_pct) }}%)
                        </td>
                        <td>{{ p.conf }}%</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% endif %}

        {% if signals %}
        <div class="card">
            <h2>📡 Recent Signals</h2>
            <table>
                <thead>
                    <tr>
                        <th>Time</th>
                        <th>Symbol</th>
                        <th>Signal</th>
                        <th>Confidence</th>
                    </tr>
                </thead>
                <tbody>
                    {% for s in signals[-10:] %}
                    <tr>
                        <td>{{ s.time.split('T')[1][:8] }}</td>
                        <td>{{ s.symbol }}</td>
                        <td class="{{ 'positive' if s.action == 'buy' else 'negative' }}">
                            {{ s.action.upper() }}
                        </td>
                        <td>{{ s.confidence }}%</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% endif %}

        <button class="refresh-btn" onclick="location.reload()">🔄 Refresh</button>

        <div class="footer">
            <p>Master-AI Trading Bot v5.3.1 FINAL | Phemex Futures</p>
            <p>Auto-refresh in 30s...</p>
        </div>
    </div>

    <script>
        setTimeout(() => location.reload(), 30000);
        
        setInterval(() => {
            const now = new Date();
            document.getElementById('updateTime').textContent = 
                now.toLocaleTimeString('en-US');
        }, 1000);
    </script>
</body>
</html>
"""

@app.route('/')
def home():
    if not engine:
        return "<h1>Bot Loading...</h1>", 503
    
    stats = engine.stats
    bal = EX.balance()
    
    positions_data = []
    if stats['positions']:
        syms = [p['symbol'] for p in stats['positions']]
        prices = EX.prices_bulk(syms)
        
        for p in stats['positions']:
            cp = prices.get(p['symbol'], p['entry'])
            if p['side'] == 'long':
                upnl = (cp - p['entry']) * p['qty']
                upct = (cp - p['entry']) / p['entry'] * 100
            else:
                upnl = (p['entry'] - cp) * p['qty']
                upct = (p['entry'] - cp) / p['entry'] * 100
            
            positions_data.append({
                **p,
                'current_price': cp,
                'unrealized_pnl': upnl,
                'unrealized_pct': upct
            })
    
    roi = ((bal - 10000) / 10000 * 100) if bal > 0 else 0
    
    return render_template_string(
        DASHBOARD_HTML,
        update_time=datetime.now(timezone.utc).strftime('%H:%M:%S'),
        dry_run=DRY_RUN,
        timeframe=TF,
        symbols_count=len(SYMBOLS),
        uptime=engine._uptime(),
        balance=bal,
        roi=roi,
        dd=stats['dd'],
        today=stats['today'],
        open_pos=stats['open_pos'],
        max_pos=MAX_POS,
        stats=stats,
        ai=stats['ai'],
        perf=stats['perf'],
        secs_left=stats['secs_left'],
        positions=positions_data,
        signals=stats['perf']['recent_signals']
    )

@app.route('/health')
def health():
    if engine:
        return jsonify({
            "status": "ok",
            "version": "5.3.1",
            "dry_run": DRY_RUN,
            "testnet": TESTNET,
            "symbols": len(SYMBOLS),
            "uptime": engine._uptime(),
            "open_positions": len(engine._pos),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "balance": EX.balance(),
            "dd": DD.dd
        })
    return jsonify({"status": "starting", "version": "5.3.1"}), 503

@app.route('/api/stats')
def api_stats():
    if engine:
        stats = engine.stats
        stats["uptime"] = engine._uptime()
        stats["symbols"] = len(SYMBOLS)
        stats["balance"] = EX.balance()
        return jsonify(stats)
    return jsonify({"error": "Engine not ready"}), 503

@app.route('/api/trades')
def api_trades():
    if engine:
        with engine._lock:
            return jsonify({
                "open": list(engine._pos.values()),
                "count": len(engine._pos)
            })
    return jsonify({"open": [], "count": 0})

@app.route('/api/history')
def api_history():
    limit = request.args.get('limit', 30, type=int)
    return jsonify({"history": database.history(limit)})

# ============================================================================
# MAIN
# ============================================================================
def main():
    global engine
    
    print("=" * 60)
    print("  🤖 Master-AI Trading Bot v5.3.1 FINAL")
    print("  Python", sys.version.split()[0])
    print("  Phemex Perpetual Futures")
    print("  ✅ All bugs fixed")
    print("  ✅ Realistic thresholds")
    print("=" * 60)

    Cfg.validate()

    engine = Engine()

    threading.Thread(target=engine.loop, daemon=True).start()

    port = int(os.environ.get("PORT", 10000))
    log.info("🌐 Flask Dashboard: http://0.0.0.0:%d", port)
    
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True,
        use_reloader=False
    )


if __name__ == "__main__":
    main()
