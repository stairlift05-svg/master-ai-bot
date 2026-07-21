#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Trading Bot Pro v6.0.0 - Cascade Multi-Timeframe Strategy Engine
سیستم جامع اتاق فکر مجازی (Trend Analyst, Momentum, Sniper Exec, Risk Manager)
سازگار با Phemex Futures (Testnet & Mainnet) - Flask Edition
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

# ── بررسی نسخه پایتون ──────────────────────────────────────────────────
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
    for lib in ("ccxt", "urllib3", "openai", "httpx", "httpcore", "asyncio", "websocket"):
        logging.getLogger(lib).setLevel(logging.ERROR)
    return logging.getLogger("Bot")

log = _setup_log()
log.info("🚀 Master-AI v6.0.0 (Virtual Think-Tank Engine) شروع به بارگذاری کرد...")

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
        return os.getenv(k, "true" if d else "false").strip().lower() in ("1", "true", "yes", "on")

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

        dd = Cfg.f("MAX_DRAWDOWN", 5.0) # مطابق قوانین جدید Kill Switch
        if not 1.0 <= dd <= 50.0:
            errs.append(f"MAX_DRAWDOWN={dd} باید 1-50 باشد")

        for w in warns:
            log.warning("⚠️  %s", w)
        if errs:
            for e in errs:
                log.critical("❌ %s", e)
            raise SystemExit("Config خطا - ربات متوقف شد")
        log.info("✅ Config OK (%d هشدار)", len(warns))


# ── ثوابت ──────────────────────────────────────────────────────────
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

RISK_PCT   = Cfg.f("RISK_PER_TRADE", 1.5) # ۱.۵ درصد ریسک در هر معامله
MAX_DD     = Cfg.f("MAX_DRAWDOWN", 5.0)    # افت ۵ درصدی = Kill Switch
MAX_POS    = Cfg.i("MAX_POSITIONS", 3)
DRY_RUN    = Cfg.b("DRY_RUN", True)
TESTNET    = Cfg.b("PHEMEX_TESTNET", False)
PORT       = Cfg.i("PORT", 10000)

# ============================================================================
# INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close: pd.Series, n: int = 14) -> pd.Series:
        if _TA_OK:
            try:
                r = ta.rsi(close, length=n)
                if r is not None and not r.dropna().empty: return r
            except Exception: pass
        delta = close.diff()
        up   = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        rs   = up.ewm(com=n-1, adjust=False).mean() / (down.ewm(com=n-1, adjust=False).mean() + 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def ema(close: pd.Series, n: int) -> pd.Series:
        if _TA_OK:
            try:
                r = ta.ema(close, length=n)
                if r is not None and not r.dropna().empty: return r
            except Exception: pass
        return close.ewm(span=n, adjust=False).mean()

    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
        if _TA_OK:
            try:
                r = ta.atr(high, low, close, length=n)
                if r is not None and not r.dropna().empty: return r
            except Exception: pass
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs()
        ], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
        if _TA_OK:
            try:
                r = ta.adx(high, low, close, length=n)
                if r is not None and not r.dropna().empty:
                    return r.iloc[:, 0] # ADX_14
            except Exception: pass
        
        # محاسبه دستی ADX
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
    def macd(close: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        if _TA_OK:
            try:
                r = ta.macd(close, fast=fast, slow=slow, signal=sig)
                if r is not None and r.shape[1] >= 3:
                    return r.iloc[:,0], r.iloc[:,1], r.iloc[:,2]
            except Exception: pass
        e_fast = close.ewm(span=fast, adjust=False).mean()
        e_slow = close.ewm(span=slow, adjust=False).mean()
        line   = e_fast - e_slow
        signal = line.ewm(span=sig, adjust=False).mean()
        hist   = line - signal
        return line, signal, hist

    @staticmethod
    def bbands(close: pd.Series, n: int = 20, std: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        if _TA_OK:
            try:
                r = ta.bbands(close, length=n, std=std)
                if r is not None and r.shape[1] >= 3:
                    return r.iloc[:,0], r.iloc[:,1], r.iloc[:,2]
            except Exception: pass
        mid = close.rolling(n).mean()
        sd  = close.rolling(n).std()
        return mid - std*sd, mid, mid + std*sd

    @staticmethod
    def vwap(high: pd.Series, low: pd.Series, close: pd.Series, vol: pd.Series) -> pd.Series:
        tp = (high + low + close) / 3.0
        return (tp * vol).cumsum() / (vol.cumsum() + 1e-10)

    @staticmethod
    def safe(s, idx: int = -1) -> float:
        try:
            if s is None: return 0.0
            v = s.iloc[idx]
            return float(v) if not (v != v) else 0.0
        except Exception: return 0.0

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
            strategy    TEXT,
            confidence  INTEGER DEFAULT 0,
            pnl         REAL DEFAULT 0,
            pnl_pct     REAL DEFAULT 0,
            is_partial  INTEGER DEFAULT 0,
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
                self._pool = pp.ThreadedConnectionPool(1, 6, dsn=DB_URL, connect_timeout=8)
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
                for s in self._SCHEMA: cur.execute(s)
            log.info("✅ DB Schema آماده")
        except Exception as e:
            log.critical("DB Schema خطا: %s", e)
            raise

    def run(self, sql: str, p: tuple = ()) -> Optional[List]:
        if self._pg: sql = sql.replace("?", "%s")
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
        self._stats(pnl)

    def _stats(self, pnl: float):
        today = datetime.now(timezone.utc).date().isoformat()
        row   = self.run("SELECT total,wins,losses,pnl FROM daily_stats WHERE date=?", (today,))
        if row:
            tot, w, l, tp = row[0]
            tot += 1
            w   += 1 if pnl > 0 else 0
            l   += 0 if pnl > 0 else 1
            tp  += pnl
            wr   = round(w/tot*100, 1)
            self.run("UPDATE daily_stats SET total=?,wins=?,losses=?,pnl=?,win_rate=? WHERE date=?", (tot, w, l, tp, wr, today))
        else:
            self.run("INSERT INTO daily_stats VALUES(?,1,?,?,?,?)",
                     (today, 1 if pnl>0 else 0, 0 if pnl>0 else 1, pnl, 100.0 if pnl>0 else 0.0))

    def today(self) -> Dict:
        d = datetime.now(timezone.utc).date().isoformat()
        r = self.run("SELECT total,wins,losses,pnl,win_rate FROM daily_stats WHERE date=?", (d,))
        if r: return dict(zip(["trades","wins","losses","pnl","wr"], r[0]))
        return {"trades":0,"wins":0,"losses":0,"pnl":0.0,"wr":0.0}

    def history(self, n: int = 25) -> List[Dict]:
        rows = self.run("SELECT id,symbol,side,entry_price,exit_price,pnl,pnl_pct,exit_reason,opened_at,closed_at FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?", (n,))
        if not rows: return []
        k = ["id","sym","side","entry","exit","pnl","pct","reason","open","close"]
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

    def send(self, msg: str, key: str = "", force: bool = False):
        log.info("📢 %s", msg[:100].replace("\n"," "))
        if not TG_TOKEN or not self._chat_id: return
        if key and not force:
            with self._lock:
                if time.time() - self._sent.get(key, 0) < 30: return
                self._sent[key] = time.time()
        threading.Thread(target=self._post, args=(msg, self._chat_id), daemon=True).start()

    def _post(self, msg: str, chat_id: str):
        try:
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
        except Exception as e: log.warning("Telegram: %s", e)

TG = Alerts()

# ============================================================================
# EXCHANGE
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex = None
        self._connect()

    def _connect(self):
        if not API_KEY:
            log.warning("⚠️ API_KEY خالی - حالت Simulation (DRY_RUN)")
            return
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
            log.error("Exchange Error: %s", e)

    def fetch_multi_ohlcv(self, sym: str) -> Dict[str, pd.DataFrame]:
        """دریافت همزمان داده‌های تایم‌فریم‌های ۱، ۳، ۵ و ۱۵ دقیقه"""
        timeframes = ["1m", "3m", "5m", "15m"]
        result = {}
        for tf in timeframes:
            try:
                raw = self._ex.fetch_ohlcv(sym, tf, limit=100) if self._ex else self._mock_ohlcv()
                df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
                df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
                result[tf] = df
            except Exception as e:
                log.error("[%s-%s] OHLCV Fetch Failure: %s", sym, tf, e)
                return {}
        return result

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
        if DRY_RUN:
            oid = f"dry_{uuid.uuid4().hex[:6]}"
            log.info("🔵 DRY RUN Order: %s %s %.5f → ID: %s", side, sym, qty, oid)
            return {"id": oid, "ok": True}
        try:
            return self._ex.create_order(sym, "market", side, qty)
        except Exception as e:
            log.error("Order Failed [%s %s]: %s", side, sym, e)
            return None

EX = Exchange()

# ============================================================================
# VIRTUAL THINK-TANK ENGINE (اتاق فکر مجازی ۴ بخشی)
# ============================================================================
@dataclass
class ThinkTankOutput:
    action: str = "neutral"      # "buy", "sell", "neutral"
    strategy: str = ""           # Strategy 1, 2, or 3
    conf: int = 0
    reason: str = ""
    sl: float = 0.0              # Dynamic SL based on ATR
    tp1: float = 0.0             # 1:1 TP for partial close
    entry: float = 0.0

class VirtualThinkTank:
    """اتاق فکر مجازی ترکیبی Top-Down Multi-Timeframe"""
    
    def analyze(self, sym: str, dfs: Dict[str, pd.DataFrame]) -> ThinkTankOutput:
        if not dfs or any(len(dfs[tf]) < 30 for tf in ["1m", "3m", "5m", "15m"]):
            return ThinkTankOutput(reason="داده‌های تایم‌فریم‌ها ناقص است")

        df1m, df3m, df5m, df15m = dfs["1m"], dfs["3m"], dfs["5m"], dfs["15m"]

        # --------------------------------------------------------------------
        # 1. بخش ۱: تحلیلگر روند (15m Regime Filter)
        # --------------------------------------------------------------------
        adx15 = IND.safe(IND.adx(df15m["high"], df15m["low"], df15m["close"]))
        ema200_15 = IND.safe(IND.ema(df15m["close"], 200))
        ema50_15  = IND.safe(IND.ema(df15m["close"], 50))
        price15   = IND.safe(df15m["close"])

        # --------------------------------------------------------------------
        # 2. بخش ۲ و ۳: چک کردن ۳ استراتژی بر اساس Market Regime
        # --------------------------------------------------------------------
        
        # === استراتژی اول: شکارچی روند (Trend Pullback Scalper) ===
        if adx15 > 25:
            # 15m Trend Filter
            trend = "long" if price15 > ema200_15 and ema50_15 > ema200_15 else ("short" if price15 < ema200_15 and ema50_15 < ema200_15 else None)
            
            if trend:
                rsi3 = IND.safe(IND.rsi(df3m["close"], 7))
                rsi5 = IND.safe(IND.rsi(df5m["close"], 7))
                
                # Momentum Check (Pullback Condition)
                pullback = (rsi3 < 30 or rsi5 < 30) if trend == "long" else (rsi3 > 70 or rsi5 > 70)
                
                if pullback:
                    # 1m Execution (Sniper) - Cross EMA 9
                    ema9_1m = IND.ema(df1m["close"], 9)
                    c_prev, c_curr = df1m["close"].iloc[-2], df1m["close"].iloc[-1]
                    e_prev, e_curr = ema9_1m.iloc[-2], ema9_1m.iloc[-1]

                    trigger = (c_prev <= e_prev and c_curr > e_curr) if trend == "long" else (c_prev >= e_prev and c_curr < e_curr)
                    
                    if trigger:
                        atr3 = IND.safe(IND.atr(df3m["high"], df3m["low"], df3m["close"]))
                        entry = c_curr
                        sl = entry - (1.5 * atr3) if trend == "long" else entry + (1.5 * atr3)
                        tp1 = entry + abs(entry - sl) if trend == "long" else entry - abs(entry - sl)
                        return ThinkTankOutput(
                            action="buy" if trend == "long" else "sell",
                            strategy="Strat1_TrendPullback",
                            conf=85, reason=f"ADX15={adx15:.1f} Trend={trend} RSI_PB Trigger_EMA9",
                            sl=sl, tp1=tp1, entry=entry
                        )

        # === استراتژی دوم: رفت و برگشت نقدینگی (Liquidity Sweep - Range) ===
        if adx15 < 25:
            bb_lo15, bb_mid15, bb_hi15 = IND.bbands(df15m["close"], 20, 2.0)
            bbl15, bbh15 = IND.safe(bb_lo15), IND.safe(bb_hi15)
            
            p5 = IND.safe(df5m["close"])
            h5, l5 = IND.safe(df5m["high"]), IND.safe(df5m["low"])

            # 5m Fakeout check
            fakeout_long  = l5 < bbl15
            fakeout_short = h5 > bbh15

            if fakeout_long or fakeout_short:
                # 3m MACD/RSI Divergence check
                rsi3_s = IND.rsi(df3m["close"], 14)
                rsi_curr, rsi_prev = IND.safe(rsi3_s, -1), IND.safe(rsi3_s, -3)
                p3_curr, p3_prev = IND.safe(df3m["close"], -1), IND.safe(df3m["close"], -3)
                
                div_long  = (p3_curr < p3_prev) and (rsi_curr > rsi_prev) if fakeout_long else False
                div_short = (p3_curr > p3_prev) and (rsi_curr < rsi_prev) if fakeout_short else False

                if div_long or div_short:
                    # 1m Close inside BB
                    bb_lo1, _, bb_hi1 = IND.bbands(df1m["close"], 20, 2.0)
                    c1 = IND.safe(df1m["close"])
                    
                    if (div_long and c1 > IND.safe(bb_lo1)) or (div_short and c1 < IND.safe(bb_hi1)):
                        atr5 = IND.safe(IND.atr(df5m["high"], df5m["low"], df5m["close"]))
                        entry = c1
                        sl = entry - (1.5 * atr5) if div_long else entry + (1.5 * atr5)
                        tp1 = entry + abs(entry - sl) if div_long else entry - abs(entry - sl)
                        return ThinkTankOutput(
                            action="buy" if div_long else "sell",
                            strategy="Strat2_LiquiditySweep",
                            conf=80, reason=f"ADX15={adx15:.1f} Range Sweep Divergence Confirmed",
                            sl=sl, tp1=tp1, entry=entry
                        )

        # === استراتژی سوم: انفجار حجم (Volume Breakout & VWAP) ===
        # Bollinger Squeeze in 15m
        bb_lo15, bb_mid15, bb_hi15 = IND.bbands(df15m["close"], 20, 2.0)
        bb_width = (bb_hi15 - bb_lo15) / (bb_mid15 + 1e-10)
        is_squeeze = IND.safe(bb_width) < 0.02  # باندهای بسیار باریک

        if is_squeeze:
            vwap5 = IND.safe(IND.vwap(df5m["high"], df5m["low"], df5m["close"], df5m["vol"]))
            v5_curr = IND.safe(df5m["vol"])
            v5_ma = IND.safe(df5m["vol"].rolling(20).mean())
            c5 = IND.safe(df5m["close"])

            # Volume > 2x MA20 Volume & Price Breakout VWAP
            vol_spike = v5_curr >= 2.0 * v5_ma
            breakout_long  = vol_spike and c5 > vwap5 and c5 > IND.safe(bb_hi15)
            breakout_short = vol_spike and c5 < vwap5 and c5 < IND.safe(bb_lo15)

            if breakout_long or breakout_short:
                # 1m Inside Bar Breakout Execution
                c1_prev, h1_prev, l1_prev = df1m["close"].iloc[-2], df1m["high"].iloc[-2], df1m["low"].iloc[-2]
                c1_curr = df1m["close"].iloc[-1]
                
                # Check if Inside Bar Broken
                if breakout_long and c1_curr > h1_prev:
                    entry = c1_curr
                    atr3 = IND.safe(IND.atr(df3m["high"], df3m["low"], df3m["close"]))
                    sl = entry - (1.5 * atr3)
                    tp1 = entry + abs(entry - sl)
                    return ThinkTankOutput(
                        action="buy", strategy="Strat3_VolumeBreakout",
                        conf=90, reason="15m Squeeze + 5m Vol Spike + 1m InsideBar Break",
                        sl=sl, tp1=tp1, entry=entry
                    )
                elif breakout_short and c1_curr < l1_prev:
                    entry = c1_curr
                    atr3 = IND.safe(IND.atr(df3m["high"], df3m["low"], df3m["close"]))
                    sl = entry + (1.5 * atr3)
                    tp1 = entry - abs(entry - sl)
                    return ThinkTankOutput(
                        action="sell", strategy="Strat3_VolumeBreakout",
                        conf=90, reason="15m Squeeze + 5m Vol Spike + 1m InsideBar Break",
                        sl=sl, tp1=tp1, entry=entry
                    )

        return ThinkTankOutput(reason="هیچ سیگنال تاییدشده‌ای یافت نشد")

THINK_TANK = VirtualThinkTank()

# ============================================================================
# RISK MANAGER & KILL SWITCH (مدیریت ریسک)
# ============================================================================
class RiskManager:
    def __init__(self):
        self.consecutive_losses = 0
        self.halted_until = None
        self.initial_daily_balance = None

    def check_kill_switch(self, current_balance: float) -> bool:
        """بررسی قوانین توقف اضطراری (Kill Switch)"""
        now = datetime.now(timezone.utc)

        # اگر ربات در حالت قفل ۲۴ ساعته باشد
        if self.halted_until and now < self.halted_until:
            log.warning("⛔ Kill Switch فعال است. باقی‌مانده: %s", str(self.halted_until - now))
            return False

        if self.initial_daily_balance is None:
            self.initial_daily_balance = current_balance

        # قانون ۱: ۳ معامله ضررده متوالی
        if self.consecutive_losses >= 3:
            self._trigger_kill_switch("3 باخت متوالی ثبت شد.")
            return False

        # قانون ۲: افت ۵ درصدی کل حساب در روز
        drawdown = (self.initial_daily_balance - current_balance) / self.initial_daily_balance * 100
        if drawdown >= MAX_DD:
            self._trigger_kill_switch(f"افت حساب بیش از {MAX_DD}% بود ({drawdown:.1f}%)")
            return False

        return True

    def _trigger_kill_switch(self, reason: str):
        self.halted_until = datetime.now(timezone.utc) + timedelta(hours=24)
        TG.send(f"🚨 <b>KILL SWITCH TRIGGERED!</b>\nدلیل: {reason}\nربات به مدت ۲۴ ساعت خاموش شد.", force=True)
        log.critical("🚨 KILL SWITCH ACTIVATED: %s", reason)

    def record_trade_result(self, pnl: float):
        if pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

    def calculate_position_size(self, balance: float, entry: float, sl: float) -> Dict:
        """محاسبه دقیق حجم معامله بر اساس ریسک ۱ الی ۲ درصد کل سرمایه"""
        risk_amount = balance * (RISK_PCT / 100.0)
        sl_distance = abs(entry - sl)
        if sl_distance == 0: return {"qty": 0, "risk": 0}
        
        qty = risk_amount / sl_distance
        return {"qty": round(qty, 5), "risk": round(risk_amount, 2)}

RISK = RiskManager()

# ============================================================================
# ENGINE (موتور اجرای ربات)
# ============================================================================
class Engine:
    def __init__(self):
        self._pos : Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._run  = True
        self._boot()

    def _boot(self):
        bal = EX.balance()
        for t in database.open_trades():
            self._pos[t["id"]] = t
        log.info("📂 تعداد %d پوزیشن فعال بازیابی شد | موجودی: $%.2f", len(self._pos), bal)

    def loop(self):
        log.info("▶️ اصلی ربات آغاز به کار کرد.")
        while self._run:
            try:
                bal = EX.balance()
                
                # مدیریت و خروج از معاملات فعال
                self._manage_positions()

                # بررسی Kill Switch قبل از اسکن بازار
                if RISK.check_kill_switch(bal) and len(self._pos) < MAX_POS:
                    self._scan(bal)

                time.sleep(10) # چک کردن هر ۱۰ ثانیه
            except Exception as e:
                log.error("Engine Loop Error: %s", e, exc_info=True)
                time.sleep(15)

    def _scan(self, bal: float):
        for sym in SYMBOLS:
            with self._lock:
                if len(self._pos) >= MAX_POS or sym in [p["symbol"] for p in self._pos.values()]:
                    continue

            dfs = EX.fetch_multi_ohlcv(sym)
            if not dfs: continue

            # ارزیابی توسط اتاق فکر
            output = THINK_TANK.analyze(sym, dfs)

            if output.action in ("buy", "sell"):
                sz = RISK.calculate_position_size(bal, output.entry, output.sl)
                if sz["qty"] > 0:
                    self._open_position(sym, output, sz)

    def _open_position(self, sym: str, out: ThinkTankOutput, sz: Dict):
        side = "long" if out.action == "buy" else "short"
        pid  = f"p_{uuid.uuid4().hex[:8]}"

        order_res = EX.order(sym, "buy" if side == "long" else "sell", sz["qty"])
        if not order_res: return

        pos = {
            "id": pid, "symbol": sym, "side": side, "entry": out.entry,
            "qty": sz["qty"], "sl": out.sl, "tp": out.tp1, "strategy": out.strategy,
            "conf": out.conf, "is_partial": 0
        }

        with self._lock:
            self._pos[pid] = pos
        database.insert(pos)

        TG.send(
            f"🎯 <b>پوزیشن جدید ({out.strategy})</b>\n"
            f"نماد: {sym} | جهت: {side.upper()}\n"
            f"ورود: {out.entry:.4f} | حدضرر (ATR): {out.sl:.4f}\n"
            f"هدف اول (TP1 1:1): {out.tp1:.4f} | حجم: {sz['qty']}\n"
            f"توضیحات: {out.reason}"
        )

    def _manage_positions(self):
        """مدیریت پوزیشن‌های باز: Partial TP + Trailing Stop بر اساس EMA 20 (3m)"""
        with self._lock:
            snap = dict(self._pos)

        for pid, pos in snap.items():
            dfs = EX.fetch_multi_ohlcv(pos["symbol"])
            if not dfs or "1m" not in dfs: continue

            price = IND.safe(dfs["1m"]["close"])
            side = pos["side"]

            # ۱. بررسی Stop Loss
            sl_hit = (side == "long" and price <= pos["sl"]) or (side == "short" and price >= pos["sl"])
            if sl_hit:
                self._close_position(pid, pos, price, "Stop Loss")
                continue

            # ۲. خروج پله‌ای (Partial TP 1:1) و انتقال SL به Breakeven
            if not pos.get("is_partial", 0):
                tp1_hit = (side == "long" and price >= pos["tp"]) or (side == "short" and price <= pos["tp"])
                if tp1_hit:
                    half_qty = pos["qty"] / 2.0
                    EX.order(pos["symbol"], "sell" if side == "long" else "buy", half_qty)
                    
                    # ریسک فری (Breakeven)
                    pos["sl"] = pos["entry"]
                    pos["qty"] = half_qty
                    pos["is_partial"] = 1
                    database.update_partial(pid, half_qty, pos["entry"])

                    TG.send(f"🎯 <b>خروج پله‌ای (TP1) ۵۰٪ معامله بسته شد</b>\nنماد: {pos['symbol']}\nاستاپ به نقطه ورود (Breakeven) منتقل شد.")

            # ۳. Trailing Stop بر اساس EMA20 در تایم ۳ دقیقه برای باقی‌مانده حجم
            if pos.get("is_partial", 0):
                ema20_3m = IND.safe(IND.ema(dfs["3m"]["close"], 20))
                if side == "long" and price < ema20_3m:
                    self._close_position(pid, pos, price, "Trailing Stop (EMA20 3m)")
                elif side == "short" and price > ema20_3m:
                    self._close_position(pid, pos, price, "Trailing Stop (EMA20 3m)")

    def _close_position(self, pid: str, pos: Dict, price: float, reason: str):
        EX.order(pos["symbol"], "sell" if pos["side"] == "long" else "buy", pos["qty"])

        pnl = (price - pos["entry"]) * pos["qty"] if pos["side"] == "long" else (pos["entry"] - price) * pos["qty"]
        pct = (price - pos["entry"]) / pos["entry"] * 100 if pos["side"] == "long" else (pos["entry"] - price) / pos["entry"] * 100

        database.close(pid, price, pnl, pct, reason)
        RISK.record_trade_result(pnl)

        with self._lock:
            self._pos.pop(pid, None)

        TG.send(f"🏁 <b>بستن پوزیشن ({reason})</b>\nنماد: {pos['symbol']}\nسود/زیان: {pnl:.2f}$ ({pct:.2f}%)")

# ============================================================================
# FLASK APP
# ============================================================================
app = Flask(__name__)
engine = None

@app.route('/')
def home():
    return "<h1>🤖 Master-AI v6.0.0 (Virtual Think-Tank) is Running!</h1>"

@app.route('/health')
def health():
    return {"status": "ok", "version": "6.0.0", "active_positions": len(engine._pos) if engine else 0}

def main():
    global engine
    Cfg.validate()
    engine = Engine()
    threading.Thread(target=engine.loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)

if __name__ == "__main__":
    main()
