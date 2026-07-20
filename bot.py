#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Trading Bot Pro v5.1.0 - Flask Edition
قابل اجرا روی Render با پایداری بالا - 10 جفت‌ارز فیوچرز
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
from datetime import datetime, timezone
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple

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
    print("[WARNING] pandas-ta نصب نیست")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Flask ──────────────────────────────────────────────────────────────────
try:
    from flask import Flask
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
log.info("🚀 Bot شروع به بارگذاری کرد...")

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

        r = Cfg.f("RISK_PER_TRADE", 1.0)
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
    "BTC/USDT,"
    "ETH/USDT,"
    "SOL/USDT,"
    "XRP/USDT,"
    "BNB/USDT,"
    "DOGE/USDT,"
    "ADA/USDT,"
    "AVAX/USDT,"
    "DOT/USDT,"
    "LINK/USDT"
)

TF         = Cfg.s("TIMEFRAME", "5m")
RISK_PCT   = Cfg.f("RISK_PER_TRADE", 1.0)
MAX_DD     = Cfg.f("MAX_DRAWDOWN", 10.0)
MAX_POS    = Cfg.i("MAX_POSITIONS", 3)
DRY_RUN    = Cfg.b("DRY_RUN", True)
TESTNET    = Cfg.b("PHEMEX_TESTNET", True)
PORT       = Cfg.i("PORT", 10000)

log.info(
    "Config: symbols=%d tf=%s risk=%.1f%% dd=%.1f%% dry=%s",
    len(SYMBOLS), TF, RISK_PCT, MAX_DD, DRY_RUN
)

# ============================================================================
# INDICATORS
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
        rs   = up.ewm(com=n-1, adjust=False).mean() / \
               down.ewm(com=n-1, adjust=False).mean()
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
    ]
    _IDX = [
        "CREATE INDEX IF NOT EXISTS i_status ON trades(status)",
        "CREATE INDEX IF NOT EXISTS i_symbol ON trades(symbol)",
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

    def open_trades(self) -> List[Dict]:
        rows = self.run(
            "SELECT id,symbol,side,entry_price,quantity,"
            "stop_loss,take_profit,confidence "
            "FROM trades WHERE status='open'"
        )
        if not rows:
            return []
        k = ["id","symbol","side","entry","qty","sl","tp","conf"]
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

    def close(self, tid: str, ep: float, pnl: float,
              pct: float, reason: str):
        self.run(
            "UPDATE trades SET status='closed',exit_price=?,pnl=?,"
            "pnl_pct=?,exit_reason=?,"
            "closed_at=CURRENT_TIMESTAMP WHERE id=?",
            (ep, pnl, pct, reason, tid)
        )
        self._stats(pnl)

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
# ALERTS
# ============================================================================
class Alerts:
    def __init__(self):
        self._sent : Dict[str, float] = {}
        self._lock = threading.Lock()
        self._chat_id = TG_CHAT

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

    def send(self, msg: str, key: str = "", force: bool = False):
        log.info("📢 %s", msg[:100].replace("\n"," "))
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
            target=self._post, args=(msg, chat_id), daemon=True
        ).start()

    def _post(self, msg: str, chat_id: str):
        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={
                    "chat_id": chat_id,
                    "text": msg,
                    "parse_mode": "HTML"
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
        
        status = "🟢 فعال" if not dd.get("halted") else "🔴 متوقف"
        dd_pct = dd.get("dd", 0)
        
        msg = (
            f"🤖 <b>داشبورد Master-AI Bot</b>\n"
            f"{'🔵 DRY-RUN' if DRY_RUN else '🟢 LIVE'} | {TF} | {len(SYMBOLS)} جفت‌ارز\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>موجودی:</b> {balance:,.2f} USDT\n"
            f"📊 <b>وضعیت:</b> {status}\n"
            f"📉 <b>Drawdown:</b> {dd_pct:.1f}% / {MAX_DD}%\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 <b>آمار امروز:</b>\n"
            f"   معاملات: {td.get('trades', 0)}\n"
            f"   برد: {td.get('wins', 0)} | باخت: {td.get('losses', 0)}\n"
            f"   وین‌ریت: {td.get('wr', 0):.1f}%\n"
            f"   سود/زیان: {'+' if td.get('pnl',0) >= 0 else ''}{td.get('pnl',0):.2f} USDT\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>پوزیشن‌های باز:</b> {pos}/{MAX_POS}\n"
            f"🧠 <b>هوش مصنوعی:</b> {ai.get('calls', 0)} درخواست | هزینه: ${ai.get('cost', 0):.4f}\n"
            f"⏱️ <b>آپتایم:</b> {engine_stats.get('uptime', '?')}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self.send(msg, key="dashboard", force=True)


TG = Alerts()

# ============================================================================
# EXCHANGE - پشتیبانی از فیوچرز با فرمت صحیح
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
                "timeout"         : 20000,
                "options"         : {
                    "defaultType": "swap",  # فیوچرز
                    "adjustForTimeDifference": True,
                },
            })
            if TESTNET:
                self._ex.set_sandbox_mode(True)
            
            # بارگذاری بازارها برای تأیید
            self._ex.load_markets()
            log.info("✅ Exchange: Phemex (testnet=%s) - %d بازار بارگذاری شد", 
                     TESTNET, len(self._ex.markets))
        except Exception as e:
            log.error("Exchange connect: %s", e)
            self._ex = None

    def _retry_ohlcv(self, sym: str, tf: str, lim: int) -> List:
        for attempt in range(3):
            try:
                if self._ex is None:
                    raise ConnectionError("Exchange نیست")
                # اطمینان از وجود نماد در بازار
                if sym not in self._ex.markets:
                    log.warning("نماد %s در بازار موجود نیست", sym)
                    raise ValueError(f"Symbol {sym} not found")
                return self._ex.fetch_ohlcv(sym, tf, limit=lim)
            except Exception as e:
                log.warning("ohlcv attempt %d [%s]: %s", attempt+1, sym, e)
                if attempt < 2:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"ohlcv {sym} شکست")

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

    @property
    def ok(self) -> bool:
        return self.action in ("buy","sell") and self.conf >= 65

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

        if   rsi < 25: bs += 30; tags.append(f"RSI={rsi:.0f}(OS++)")
        elif rsi < 35: bs += 20; tags.append(f"RSI={rsi:.0f}(OS)")
        elif rsi < 45: bs += 8
        elif rsi > 75: ss += 30; tags.append(f"RSI={rsi:.0f}(OB++)")
        elif rsi > 65: ss += 20; tags.append(f"RSI={rsi:.0f}(OB)")
        elif rsi > 55: ss += 8

        if   mh > 0 and ml > ms: bs += 20; tags.append("MACD↑")
        elif mh < 0 and ml < ms: ss += 20; tags.append("MACD↓")

        if   price > e20 > e50 > 0: bs += 20; tags.append("EMA↑")
        elif 0 < price < e20 < e50: ss += 20; tags.append("EMA↓")

        if   bbl > 0 and price <= bbl: bs += 15; tags.append("BB_lo")
        elif bbh > 0 and price >= bbh: ss += 15; tags.append("BB_hi")

        if   sk < 20: bs += 10; tags.append("Stoch_OS")
        elif sk > 80: ss += 10; tags.append("Stoch_OB")

        if vr > 1.5:
            if   bs > ss: bs += 8;  tags.append("Vol↑")
            elif ss > bs: ss += 8;  tags.append("Vol↑")

        ind = {
            "rsi":round(rsi,1), "macd_h":round(mh,5),
            "e20":round(e20,4), "e50":round(e50,4),
            "atr":round(atr,4), "vr":round(vr,2),
            "sk":round(sk,1), "bbl":round(bbl,4),
            "bbh":round(bbh,4), "price":round(price,4)
        }

        thr = 46
        if bs >= thr and bs > ss:
            cf = min(92, int(bs * 1.05))
            return Sig("buy",  cf, "|".join(tags[:3]),
                       "low" if cf>75 else "medium", "tech", ind)
        if ss >= thr and ss > bs:
            cf = min(92, int(ss * 1.05))
            return Sig("sell", cf, "|".join(tags[:3]),
                       "low" if cf>75 else "medium", "tech", ind)

        return Sig(reason=f"B={bs} S={ss}", ind=ind)


TECH = Tech()

# ============================================================================
# AI ENGINE
# ============================================================================
class AI:
    _CPK = 0.002

    def __init__(self):
        self._c     = None
        self._cache : Dict[str, Tuple[Sig, float]] = {}
        self._ttl   = 120
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
        log.info("🤖 [%s] AI=%s(%d%%) Tech=%s → %s(%d%%)",
                 sym, aj.get("signal","?"),aj.get("confidence",0),
                 tech.action, result.action, result.conf)
        return result

    def _merge(self, ai: Dict, tech: Sig) -> Sig:
        aa  = ai.get("signal","neutral")
        ac  = int(ai.get("confidence",0))
        ar  = str(ai.get("reason",""))
        arl = ai.get("risk_level","medium")

        if aa == tech.action and aa != "neutral":
            cf = min(95, int((ac+tech.conf)/2*1.1))
            return Sig(aa, cf, f"AI+Tech:{ar}"[:70], arl, "combined", tech.ind)

        if aa != "neutral" and tech.action != "neutral" and aa != tech.action:
            return Sig("neutral",0,f"Conflict AI={aa} Tech={tech.action}",
                       src="conflict", ind=tech.ind)

        if aa != "neutral" and tech.action == "neutral" and ac >= 80:
            return Sig(aa, ac, ar, arl, "ai", tech.ind)

        return tech

    def _prompt(self, df: pd.DataFrame,
                sym: str, tech: Sig, n_open: int) -> str:
        rows = df.tail(8)[["ts","open","high","low","close","vol"]].copy()
        rows["ts"] = rows["ts"].astype(str)
        i = tech.ind
        return (
            f"Analyze {sym} {TF}.\n"
            f"CANDLES: {json.dumps(rows.to_dict('records'),default=str)}\n"
            f"RSI={i.get('rsi')} MACD_h={i.get('macd_h')} "
            f"EMA20={i.get('e20')} ATR={i.get('atr')} VR={i.get('vr')}x\n"
            f"TECH_SIGNAL: {tech.action.upper()}({tech.conf}%)\n"
            f"CONTEXT: open={n_open}/{MAX_POS}\n"
            f"RULES: BUY if RSI<65&macd_h>0; SELL if RSI>35&macd_h<0\n"
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
# TIMER
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
            return False
        if ts > self._last:
            self._last = ts
            log.info("🕯️  کندل جدید: %s",
                     datetime.fromtimestamp(ts,tz=timezone.utc)
                     .strftime("%H:%M UTC"))
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
            log.info("💰 Peak اولیه: $%.2f", b)

    def check(self, b: float) -> bool:
        if self._peak is None:
            self.init(b)
            return True
        if b > self._peak:
            self._peak = b
            if self.halted:
                self.halted = False
                TG.send("✅ Drawdown بهبود - ربات فعال", force=True)
        dd = (self._peak - b) / self._peak * 100
        self.dd = round(dd, 2)
        if dd >= self._mx and not self.halted:
            self.halted = True
            TG.send(
                f"🚨 توقف! DD={dd:.1f}% ≥ {self._mx}%\n"
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
        self._peak : Dict[str, float] = {}
        self._sl   : Dict[str, float] = {}

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
# CORRELATION FILTER
# ============================================================================
class Corr:
    _G = [
        {"BTC/USDT","ETH/USDT","BNB/USDT"},
        {"SOL/USDT","AVAX/USDT"},
        {"XRP/USDT","ADA/USDT"},
        {"DOGE/USDT","SHIB/USDT"},
        {"DOT/USDT","LINK/USDT"},
    ]

    def ok(self, sym: str, open_s: set) -> bool:
        for g in self._G:
            if sym in g and g & open_s:
                log.info("🔗 %s block (همبسته با %s)", sym, g&open_s)
                return False
        return True


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
        log.info("📂 %d پوزیشن بارگذاری | Balance=$%.2f",
                 len(self._pos), bal)
        TG.send(
            f"🤖 <b>Master-AI Bot v5.1 شروع شد</b>\n"
            f"{'🔵 DRY-RUN' if DRY_RUN else '🟢 LIVE'} | "
            f"{TF} | {len(SYMBOLS)} جفت‌ارز | Risk:{RISK_PCT}% | DD:{MAX_DD}%\n"
            f"Balance: ${bal:.2f}",
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
            return f"{h}h {m}m {s}s"
        except Exception:
            return "?"

    # ── Main Loop ─────────────────────────────────────────────────────────
    def loop(self):
        log.info("▶️  Main loop شروع")
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

                if time.time() - last_dashboard > 30:
                    self._send_dashboard(bal)
                    last_dashboard = time.time()

                elapsed = time.time() - t0
                sl_t    = max(5.0, min(20.0, TMR.left / 4.0))
                time.sleep(max(1.0, sl_t - elapsed))

            except KeyboardInterrupt:
                self._run = False
            except Exception as e:
                log.error("Loop: %s", e, exc_info=True)
                time.sleep(15)

    # ── Scan ──────────────────────────────────────────────────────────────
    def _scan(self, bal: float):
        with self._lock:
            n = len(self._pos)
        if n >= MAX_POS:
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

    def _analyze(self, sym: str, bal: float, n_open: int):
        df = EX.ohlcv(sym, TF, 150)
        if len(df) < 40:
            return

        tech = TECH.run(df)
        sig  = AI_ENG.analyze(df, sym, tech, n_open)

        if not sig.ok:
            return

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
            return

        self._open(sym, sig, price, sl, tp, sz)

    # ── Open ──────────────────────────────────────────────────────────────
    def _open(self, sym: str, sig: Sig,
              price: float, sl: float, tp: float, sz: Dict):
        side = "long" if sig.action=="buy" else "short"
        pid  = f"p_{uuid.uuid4().hex[:8]}"

        o = EX.order(sym, "buy" if side=="long" else "sell", sz["qty"])
        if o is None and not DRY_RUN:
            return

        pos = {"id":pid,"symbol":sym,"side":side,"entry":price,
               "qty":sz["qty"],"sl":sl,"tp":tp,
               "signal":sig.action,"conf":sig.conf,
               "atr":sig.ind.get("atr",price*0.01)}

        with self._lock:
            self._pos[pid] = pos
        TR.init(pid, price, sl)
        database.insert(pos)
        self._st["opened"] += 1

        sp = abs(price-sl)/price*100
        tp_ = abs(tp-price)/price*100
        e  = "🟢" if side=="long" else "🔴"
        
        TG.send(
            f"{e} <b>باز کردن {side.upper()}</b> {sym}\n"
            f"ورود: {price:.4f} | حد ضرر: {sl:.4f}(-{sp:.1f}%)\n"
            f"حد سود: {tp:.4f}(+{tp_:.1f}%) | حجم: {sz['qty']:.5f}\n"
            f"ریسک: ${sz['risk']:.1f} | اطمینان: {sig.conf}%[{sig.src}]\n"
            f"دلیل: {sig.reason[:55]}"
        )
        log.info("✅ OPEN %s %s | %s | %.4f", side, sym, pid, price)

    # ── Exits ─────────────────────────────────────────────────────────────
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
        EX.order(pos["symbol"],
                 "sell" if pos["side"]=="long" else "buy",
                 pos["qty"])

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
        w = "✅برد" if pnl>0 else "❌باخت"
        
        TG.send(
            f"{e} <b>{reason}</b> {w} {pos['symbol']}\n"
            f"{pos['side'].upper()} "
            f"{pos['entry']:.4f}→{px:.4f}\n"
            f"سود/زیان: {'+' if pnl>0 else ''}{pnl:.2f}$({pct:+.2f}%)\n"
            f"Drawdown: {DD.dd:.1f}%"
        )
        log.info("%s %s %s | $%.2f (%.2f%%)",
                 e, pos["symbol"], reason, pnl, pct)

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
            "secs_left": TMR.left
        }

# ============================================================================
# FLASK APP
# ============================================================================
app = Flask(__name__)
engine = None

@app.route('/')
def home():
    return """
    <html dir="rtl">
    <head><title>Master-AI Bot</title></head>
    <body style="font-family:Tahoma;background:#0d1117;color:#c9d1d9;text-align:center;padding:50px;">
        <h1>🤖 Master-AI Trading Bot v5.1</h1>
        <p>ربات با 10 جفت‌ارز فیوچرز در حال اجراست...</p>
        <p>📊 <a href="/api/stats" style="color:#58a6ff;">مشاهده آمار</a></p>
        <p>❤️ <a href="/health" style="color:#58a6ff;">وضعیت سلامت</a></p>
    </body>
    </html>
    """

@app.route('/health')
def health():
    if engine:
        return {
            "status": "ok",
            "version": "5.1.0",
            "dry_run": DRY_RUN,
            "symbols": len(SYMBOLS),
            "uptime": engine._uptime(),
            "open_positions": len(engine._pos) if engine else 0,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    return {"status": "starting", "version": "5.1.0"}

@app.route('/api/stats')
def api_stats():
    if engine:
        stats = engine.stats
        stats["uptime"] = engine._uptime()
        stats["symbols"] = len(SYMBOLS)
        return stats
    return {"error": "Engine not ready"}

@app.route('/api/trades')
def api_trades():
    if engine:
        with engine._lock:
            return {"open": list(engine._pos.values()), "count": len(engine._pos)}
    return {"open": [], "count": 0}

@app.route('/api/history')
def api_history():
    return {"history": database.history(30)}

@app.route('/api/symbols')
def api_symbols():
    return {"symbols": SYMBOLS, "count": len(SYMBOLS)}

# ============================================================================
# MAIN
# ============================================================================
def main():
    global engine
    log.info("=" * 50)
    log.info("  Master-AI Bot v5.1.0 - Flask Edition")
    log.info("  Python %s", sys.version.split()[0])
    log.info("  10 جفت‌ارز فیوچرز Phemex")
    log.info("=" * 50)

    Cfg.validate()

    engine = Engine()

    threading.Thread(target=engine.loop, daemon=True).start()

    port = int(os.environ.get("PORT", 10000))
    log.info("🌐 Flask Server on port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
