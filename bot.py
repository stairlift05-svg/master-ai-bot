#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Trading Bot Pro v5.4.2 - Bulletproof Edition
✅ فیکس حساسیت به اشتباه تایپی در TIMEFRAME
✅ فیکس Phemex OHLCV (Error 30000)
✅ داشبورد حرفه‌ای
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

if sys.version_info < (3, 10):
    print("[CRITICAL] Python 3.10+ required")
    sys.exit(1)

_MISSING = []
try: import pandas as pd
except ImportError: _MISSING.append("pandas")
try: import numpy as np
except ImportError: _MISSING.append("numpy")
try: import requests
except ImportError: _MISSING.append("requests")
try: import ccxt
except ImportError: _MISSING.append("ccxt")
try:
    import pandas_ta as ta
    _TA_OK = True
except ImportError:
    _TA_OK = False
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError: pass
try:
    from flask import Flask, render_template_string, jsonify, request
except ImportError: _MISSING.append("flask")

if _MISSING:
    sys.exit(1)

# ============================================================================
# LOGGING
# ============================================================================
IS_PROD = os.getenv("RENDER", "false").lower() == "true"

def _setup_log() -> logging.Logger:
    fmt = '{"t":"%(asctime)s","lvl":"%(levelname)s","msg":"%(message)s"}' if IS_PROD else "%(asctime)s | %(levelname)-8s | %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, stream=sys.stdout, force=True)
    for lib in ("ccxt", "urllib3", "openai", "httpx", "httpcore"):
        logging.getLogger(lib).setLevel(logging.ERROR)
    return logging.getLogger("Bot")

log = _setup_log()

# ============================================================================
# CONFIG
# ============================================================================
class Cfg:
    @staticmethod
    def s(k: str, d: str = "") -> str: return os.getenv(k, d).strip()
    @staticmethod
    def i(k: str, d: int) -> int:
        try: return int(os.getenv(k, str(d)).strip())
        except: return d
    @staticmethod
    def f(k: str, d: float) -> float:
        try: return float(os.getenv(k, str(d)).strip())
        except: return d
    @staticmethod
    def b(k: str, d: bool = False) -> bool:
        return os.getenv(k, "true" if d else "false").strip().lower() in ("1", "true", "yes", "on")
    @staticmethod
    def lst(k: str, d: str = "") -> List[str]:
        raw = os.getenv(k, d) or d
        return [x.strip() for x in raw.split(",") if x.strip()]

API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")
OAI_KEY    = Cfg.s("OPENAI_API_KEY")
DB_URL     = Cfg.s("DATABASE_URL")

SYMBOLS = Cfg.lst("SYMBOLS", 
    "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,XRP/USDT:USDT,BNB/USDT:USDT,"
    "DOGE/USDT:USDT,ADA/USDT:USDT,AVAX/USDT:USDT,DOT/USDT:USDT,LINK/USDT:USDT"
)

# 🚀 FIX: اگر کاربر به اشتباه چند تایم‌فریم وارد کرد، فقط اولی را بگیر
_raw_tf    = Cfg.s("TIMEFRAME", "5m")
TF         = _raw_tf.split(",")[0].strip()
if TF not in ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"]:
    TF = "5m" # مقدار پیش‌فرض در صورت اشتباه تایپی

RISK_PCT   = Cfg.f("RISK_PER_TRADE", 1.5)
MAX_DD     = Cfg.f("MAX_DRAWDOWN", 10.0)
MAX_POS    = Cfg.i("MAX_POSITIONS", 5)
DRY_RUN    = Cfg.b("DRY_RUN", False)
TESTNET    = Cfg.b("PHEMEX_TESTNET", False)
PORT       = Cfg.i("PORT", 10000)

log.info("Config: TF=%s | Risk=%.1f%% | MaxDD=%.1f%% | Dry=%s", TF, RISK_PCT, MAX_DD, DRY_RUN)

# ============================================================================
# PERFORMANCE TRACKER
# ============================================================================
class PerfTracker:
    def __init__(self, max_len: int = 100):
        self._scans = deque(maxlen=max_len)
        self._signals = deque(maxlen=max_len)
    def log_scan(self, duration: float): self._scans.append(duration)
    def log_signal(self, sym: str, action: str, conf: int):
        self._signals.append({"time": datetime.now(timezone.utc).isoformat(), "symbol": sym, "action": action, "confidence": conf})
    @property
    def stats(self) -> Dict:
        return {"avg_scan_time": round(sum(self._scans)/len(self._scans), 2) if self._scans else 0, "recent_signals": list(self._signals)[-10:]}

PERF = PerfTracker()

# ============================================================================
# INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close: pd.Series, n: int = 14) -> pd.Series:
        delta = close.diff()
        up   = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        down_ma = down.ewm(com=n-1, adjust=False).mean().replace(0, 1e-10)
        rs = up.ewm(com=n-1, adjust=False).mean() / down_ma
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
        return line, signal, line - signal

    @staticmethod
    def bbands(close: pd.Series, n: int = 20, std: float = 2.0):
        mid = close.rolling(n).mean()
        sd  = close.rolling(n).std()
        return mid - std*sd, mid, mid + std*sd

    @staticmethod
    def stoch(high: pd.Series, low: pd.Series, close: pd.Series, k: int = 14, d: int = 3):
        lo  = low.rolling(k).min()
        hi  = high.rolling(k).max()
        stk = 100 * (close - lo) / (hi - lo + 1e-10)
        return stk, stk.rolling(d).mean()

    @staticmethod
    def safe(s, idx: int = -1) -> float:
        try: v = s.iloc[idx]; return float(v) if not (v != v) else 0.0
        except Exception: return 0.0

IND = Indicators()

# ============================================================================
# DATABASE
# ============================================================================
class DB:
    def __init__(self):
        self._path  = "bot.db"
        self._lock  = threading.Lock()
        self._boot()

    def _boot(self):
        import sqlite3
        with sqlite3.connect(self._path) as c:
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("""CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL, entry_price REAL NOT NULL, exit_price REAL, quantity REAL NOT NULL, stop_loss REAL NOT NULL, take_profit REAL NOT NULL, status TEXT DEFAULT 'open', ai_signal TEXT, confidence INTEGER DEFAULT 0, pnl REAL DEFAULT 0, pnl_pct REAL DEFAULT 0, exit_reason TEXT, opened_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS daily_stats (date TEXT PRIMARY KEY, total INTEGER DEFAULT 0, wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, pnl REAL DEFAULT 0, win_rate REAL DEFAULT 0)""")

    @contextmanager
    def _cx(self):
        import sqlite3
        with self._lock:
            c = sqlite3.connect(self._path, timeout=12)
            try: yield c; c.commit()
            except: c.rollback(); raise
            finally: c.close()

    def run(self, sql: str, p: tuple = ()) -> Optional[List]:
        try:
            with self._cx() as c:
                cur = c.cursor()
                cur.execute(sql, p)
                if sql.strip().upper().startswith("SELECT"): return cur.fetchall()
        except Exception as e: log.error("DB: %s", e)
        return None

    def open_trades(self) -> List[Dict]:
        rows = self.run("SELECT id,symbol,side,entry_price,quantity,stop_loss,take_profit,confidence,opened_at FROM trades WHERE status='open'")
        return [dict(zip(["id","symbol","side","entry","qty","sl","tp","conf","opened"], r)) for r in rows] if rows else []

    def insert(self, t: Dict):
        self.run("INSERT OR IGNORE INTO trades (id,symbol,side,entry_price,quantity,stop_loss,take_profit,ai_signal,confidence) VALUES (?,?,?,?,?,?,?,?,?)",
                 (t["id"], t["symbol"], t["side"], t["entry"], t["qty"], t["sl"], t["tp"], t["signal"], t["conf"]))

    def close(self, tid: str, ep: float, pnl: float, pct: float, reason: str):
        self.run("UPDATE trades SET status='closed',exit_price=?,pnl=?,pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?", (ep, pnl, pct, reason, tid))
        self._stats(pnl)

    def _stats(self, pnl: float):
        today = datetime.now(timezone.utc).date().isoformat()
        row = self.run("SELECT total,wins,losses,pnl FROM daily_stats WHERE date=?", (today,))
        if row:
            tot, w, l, tp = row[0]
            tot += 1; w += 1 if pnl > 0 else 0; l += 0 if pnl > 0 else 1; tp += pnl
            self.run("UPDATE daily_stats SET total=?,wins=?,losses=?,pnl=?,win_rate=? WHERE date=?", (tot, w, l, tp, round(w/tot*100, 1), today))
        else:
            self.run("INSERT INTO daily_stats VALUES(?,1,?,?,?,?)", (today, 1 if pnl>0 else 0, 0 if pnl>0 else 1, pnl, 100.0 if pnl>0 else 0.0))

    def today(self) -> Dict:
        d = datetime.now(timezone.utc).date().isoformat()
        r = self.run("SELECT total,wins,losses,pnl,win_rate FROM daily_stats WHERE date=?", (d,))
        return dict(zip(["trades","wins","losses","pnl","wr"], r[0])) if r else {"trades":0,"wins":0,"losses":0,"pnl":0.0,"wr":0.0}

database = DB()

# ============================================================================
# TELEGRAM ALERTS
# ============================================================================
class Alerts:
    def __init__(self):
        self._chat_id = TG_CHAT

    def _get_chat_id(self):
        if self._chat_id: return self._chat_id
        if not TG_TOKEN: return None
        try:
            res = requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates", timeout=5).json()
            if res.get("ok") and res.get("result"): return str(res["result"][-1]["message"]["chat"]["id"])
        except: pass
        return None

    def send(self, msg: str, key: str = "", force: bool = False):
        if not TG_TOKEN: return
        chat_id = self._get_chat_id()
        if not chat_id: return
        threading.Thread(target=lambda: requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10), daemon=True).start()

    def send_dashboard(self, engine_stats: Dict, balance: float):
        dd, td, pos = engine_stats.get("dd", {}), engine_stats.get("today", {}), engine_stats.get("open_pos", 0)
        msg = f"🤖 <b>Bot v5.4.2</b>\n{'🔵 DRY' if DRY_RUN else '🟢 LIVE'} | {TF}\n💰 Bal: {balance:,.2f}\n📉 DD: {dd.get('dd',0):.1f}%\n📊 Pos: {pos}/{MAX_POS}\n📈 Today: {td.get('pnl',0):+.2f}$"
        self.send(msg, force=True)

TG = Alerts()

# ============================================================================
# EXCHANGE 
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex = None
        self._pc = {}
        self._connect()

    def _connect(self):
        if not API_KEY: return
        try:
            self._ex = ccxt.phemex({"apiKey": API_KEY, "secret": API_SECRET, "enableRateLimit": True, "timeout": 30000, "options": {"defaultType": "swap"}})
            if TESTNET: self._ex.set_sandbox_mode(True)
            self._ex.load_markets()
            log.info("✅ Exchange connected.")
        except Exception as e: log.error("Exchange connect: %s", e)

    def _retry_ohlcv(self, sym: str, tf: str, lim: int) -> List:
        for attempt in range(4):
            try:
                tf_ms = self._ex.parse_timeframe(tf) * 1000
                since = self._ex.milliseconds() - (lim * tf_ms)
                return self._ex.fetch_ohlcv(sym, tf, since=since, limit=lim, params={'type': 'swap'})
            except Exception as e:
                log.warning("ohlcv retry %d [%s]: %s", attempt+1, sym, str(e)[:100])
                time.sleep(2)
        raise RuntimeError(f"Failed to fetch {sym}")

    def ohlcv(self, sym: str, tf: str, lim: int = 250) -> pd.DataFrame:
        raw = self._retry_ohlcv(sym, tf, lim)
        df  = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df.dropna().reset_index(drop=True)

    def price(self, sym: str) -> float:
        now = time.time()
        if sym in self._pc and now - self._pc[sym][1] < 2.5: return self._pc[sym][0]
        try:
            p = float(self._ex.fetch_ticker(sym)["last"])
            self._pc[sym] = (p, now); return p
        except: return 0.0

    def prices_bulk(self, syms: List[str]) -> Dict[str, float]:
        out = {}
        try:
            for s, t in self._ex.fetch_tickers(syms).items():
                if t.get("last"): out[s] = float(t["last"])
        except: pass
        return out

    def balance(self) -> float:
        if not self._ex or DRY_RUN: return 10000.0
        try:
            b = self._ex.fetch_balance()
            for key in ("USDT","USD"):
                if key in b and b[key].get("free"): return float(b[key]["free"])
        except: return 0.0

    def calculate_quantity(self, sym: str, usd_amount: float):
        try:
            m = self._ex.market(sym)
            p = self.price(sym)
            min_q = m.get("limits", {}).get("amount", {}).get("min", 0.001)
            prec = m.get("precision", {}).get("amount", 0.001)
            return max(min_q, round((usd_amount / p) / prec) * prec)
        except: return None

    def order(self, sym: str, side: str, qty: float):
        if DRY_RUN: return {"id": f"dry_{uuid.uuid4().hex[:6]}", "price": self.price(sym)}
        try:
            o = self._ex.create_order(sym, "market", side, qty)
            fp = float(o.get("price", self.price(sym)))
            if "fills" in o and o["fills"]: fp = float(o["fills"][0]["price"])
            return {"id": o.get("id"), "price": fp}
        except Exception as e:
            log.error("Order [%s %s]: %s", side, sym, e)
            return None

EX = Exchange()

# ============================================================================
# TECHNICAL ANALYSIS
# ============================================================================
@dataclass
class Sig:
    action: str = "neutral"
    conf  : int = 0
    reason: str = ""
    ind   : Dict = field(default_factory=dict)
    
    @property
    def ok(self) -> bool: return self.action in ("buy","sell") and self.conf >= 40

class Tech:
    def run(self, df: pd.DataFrame) -> Sig:
        if len(df) < 200: return Sig(reason="Data < 200")
        c, h, l, v = df["close"], df["high"], df["low"], df["vol"]

        try:
            rsi = IND.safe(IND.rsi(c, 14))
            ml, ms, mh = IND.macd(c)
            mh = IND.safe(mh)
            e20, e50, e200 = IND.safe(IND.ema(c, 20)), IND.safe(IND.ema(c, 50)), IND.safe(IND.ema(c, 200))
            atr = IND.safe(IND.atr(h, l, c, 14))
            bbl, bbm, bbh = IND.bbands(c, 20)
            bbl, bbh = IND.safe(bbl), IND.safe(bbh)
            sk, std = IND.stoch(h, l, c)
            sk = IND.safe(sk)
            price = float(c.iloc[-1])
            vr = float(v.iloc[-1]) / (float(v.rolling(20).mean().iloc[-1]) or 1.0)
        except Exception as e: return Sig(reason=f"Ind err: {e}")

        bs, ss, tags = 0, 0, []

        trend = "BULL" if price > e200 else "BEAR"
        if trend == "BULL": bs += 15; ss -= 10; tags.append("Trend=UP")
        else: ss += 15; bs -= 10; tags.append("Trend=DWN")

        if rsi < 35: bs += 35; tags.append("RSI_OS")
        elif rsi < 45: bs += 15
        if rsi > 65: ss += 35; tags.append("RSI_OB")
        elif rsi > 55: ss += 15

        if mh > 0 and ml > ms: bs += 25
        if mh < 0 and ml < ms: ss += 25

        if price > e20 > e50: bs += 25
        if price < e20 < e50: ss += 25

        if sk < 25: bs += 15
        if sk > 75: ss += 15

        if vr > 1.3:
            if bs > ss: bs += 15
            else: ss += 15

        ind = {"rsi":round(rsi,1), "macd_h":round(mh,4), "e200":round(e200,4), "atr":round(atr,4), "price":price}
        
        thr = 30 
        if bs >= thr and bs > ss: return Sig("buy", min(95, int(bs * 1.3)), "|".join(tags[:3]), ind=ind)
        if ss >= thr and ss > bs: return Sig("sell", min(95, int(ss * 1.3)), "|".join(tags[:3]), ind=ind)

        return Sig(reason=f"B={bs} S={ss}", ind=ind)

TECH = Tech()

# ============================================================================
# AI ENGINE
# ============================================================================
class AI:
    def __init__(self):
        self._c = None
        self._calls = 0
        if OAI_KEY:
            try:
                from openai import OpenAI
                self._c = OpenAI(api_key=OAI_KEY, timeout=15.0)
            except: pass

    def analyze(self, sym, tech, n_open):
        if not self._c: return tech
        prompt = f"Sym:{sym} RSI:{tech.ind.get('rsi')} Tech:{tech.action}. Reply JSON {{\"signal\":\"buy\"|\"sell\"|\"neutral\",\"confidence\":0-100}}"
        try:
            r = self._c.chat.completions.create(model="gpt-3.5-turbo", messages=[{"role":"user","content":prompt}], max_tokens=50, temperature=0.1)
            aj = json.loads(re.sub(r"```[a-z]*|```","", r.choices[0].message.content).strip())
            aa = aj.get("signal","neutral")
            ac = int(aj.get("confidence",0))
            if aa == tech.action and aa != "neutral": tech.conf = min(95, int((ac+tech.conf)/2*1.15))
            self._calls += 1
            return tech
        except: return tech

AI_ENG = AI()

# ============================================================================
# ENGINE UTILS
# ============================================================================
class Timer:
    def __init__(self, tf):
        self._s = {"1m":60,"3m":180,"5m":300,"15m":900,"30m":1800,"1h":3600,"4h":14400,"1d":86400}.get(tf, 300)
        self._last = None
    def is_new(self):
        ts = (int(time.time()) // self._s) * self._s
        if self._last is None or ts > self._last:
            self._last = ts; return True
        return False

TMR = Timer(TF)

class DDG:
    def __init__(self, mx): self.mx, self.pk, self.halted, self.dd = mx, None, False, 0.0
    def check(self, b):
        if self.pk is None: self.pk = b; return True
        if b > self.pk: self.pk = b; self.halted = False
        self.dd = round((self.pk - b) / self.pk * 100, 2)
        if self.dd >= self.mx: self.halted = True
        return not self.halted
    @property
    def st(self): return {"halted":self.halted,"dd":self.dd,"max":self.mx,"peak":self.pk}

DD = DDG(MAX_DD)

class Trail:
    def __init__(self): self._pk = {}; self._sl = {}
    def init(self, pid, e, sl): self._pk[pid] = e; self._sl[pid] = sl
    def update(self, pid, side, px, atr, osl):
        td = atr * 2.0
        if side == "long":
            self._pk[pid] = max(self._pk.get(pid, px), px)
            self._sl[pid] = max(self._sl.get(pid, osl), self._pk[pid] - td)
        else:
            self._pk[pid] = min(self._pk.get(pid, px), px)
            self._sl[pid] = min(self._sl.get(pid, osl), self._pk[pid] + td)
        return self._sl[pid]
    def rm(self, pid): self._pk.pop(pid, None); self._sl.pop(pid, None)

TR = Trail()

# ============================================================================
# CORE ENGINE
# ============================================================================
class Engine:
    def __init__(self):
        self._pos = {}
        self._lock = threading.Lock()
        self._run = True
        self._st = {"cycles":0, "scans":0, "opened":0, "closed":0, "start":datetime.now(timezone.utc).isoformat()}
        bal = EX.balance()
        DD.check(bal)
        for t in database.open_trades(): self._pos[t["id"]] = t; TR.init(t["id"], t["entry"], t["sl"])
        TG.send(f"🚀 <b>Bot v5.4.2 Started</b>\nTF: {TF} | Bal: ${bal:,.2f}", force=True)

    def loop(self):
        while self._run:
            try:
                self._st["cycles"] += 1
                t0 = time.time()
                self._exits()
                bal = EX.balance()
                if DD.check(bal) and TMR.is_new(): self._scan(bal)
                time.sleep(max(1.0, 5.0 - (time.time() - t0)))
            except Exception as e: log.error("Loop err: %s", e); time.sleep(10)

    def _scan(self, bal):
        with self._lock: n = len(self._pos)
        if n >= MAX_POS: return
        self._st["scans"] += 1
        
        for sym in SYMBOLS:
            with self._lock:
                if len(self._pos) >= MAX_POS: break
                if sym in [p["symbol"] for p in self._pos.values()]: continue
            
            try:
                df = EX.ohlcv(sym, TF, 250)
                if len(df) < 200: continue
                
                tech = TECH.run(df)
                sig = AI_ENG.analyze(sym, tech, n) if tech.conf >= 30 else tech
                
                if sig.ok:
                    px = sig.ind["price"]
                    atr = sig.ind["atr"]
                    sl = px - atr*1.5 if sig.action=="buy" else px + atr*1.5
                    tp = px + atr*3.0 if sig.action=="buy" else px - atr*3.0
                    
                    risk = bal * (RISK_PCT/100)
                    dist = abs(px - sl)
                    if dist > 0:
                        val = (risk/dist) * px
                        qty = EX.calculate_quantity(sym, val)
                        if qty and qty > 0: self._open(sym, sig, px, sl, tp, qty)
            except Exception as e: log.error("Analyze %s: %s", sym, e)

    def _open(self, sym, sig, px, sl, tp, qty):
        side = "long" if sig.action=="buy" else "short"
        o = EX.order(sym, "buy" if side=="long" else "sell", qty)
        if o:
            pid = f"p_{uuid.uuid4().hex[:6]}"
            fpx = o["price"]
            pos = {"id":pid,"symbol":sym,"side":side,"entry":fpx,"qty":qty,"sl":sl,"tp":tp,"signal":sig.action,"conf":sig.conf,"atr":sig.ind.get("atr",px*0.01)}
            with self._lock: self._pos[pid] = pos
            TR.init(pid, fpx, sl)
            database.insert(pos)
            self._st["opened"] += 1
            TG.send(f"🟢 <b>OPEN {side.upper()}</b> {sym}\nEntry: {fpx:.4f}\nSL: {sl:.4f}\nQty: {qty}\nConf: {sig.conf}%")

    def _exits(self):
        with self._lock: snap = dict(self._pos)
        if not snap: return
        pxs = EX.prices_bulk(list({p["symbol"] for p in snap.values()}))
        
        for pid, pos in snap.items():
            px = pxs.get(pos["symbol"])
            if not px: continue
            
            nsl = TR.update(pid, pos["side"], px, pos.get("atr", px*0.01), pos["sl"])
            if abs(nsl - pos["sl"]) > 1e-8:
                with self._lock:
                    if pid in self._pos: self._pos[pid]["sl"] = nsl
                database.run("UPDATE trades SET stop_loss=? WHERE id=?", (nsl, pid))

            if (pos["side"]=="long" and px>=pos["tp"]) or (pos["side"]=="short" and px<=pos["tp"]):
                self._close(pid, pos, px, "TP")
            elif (pos["side"]=="long" and px<=nsl) or (pos["side"]=="short" and px>=nsl):
                self._close(pid, pos, px, "SL")

    def _close(self, pid, pos, px, reason):
        o = EX.order(pos["symbol"], "sell" if pos["side"]=="long" else "buy", pos["qty"])
        if o: px = o.get("price", px)
        
        pnl = (px - pos["entry"]) * pos["qty"] if pos["side"]=="long" else (pos["entry"] - px) * pos["qty"]
        pct = pnl / (pos["entry"] * pos["qty"]) * 100
        
        database.close(pid, px, pnl, pct, reason)
        with self._lock: self._pos.pop(pid, None)
        TR.rm(pid)
        self._st["closed"] += 1
        TG.send(f"🎯 <b>CLOSE {reason}</b> {pos['symbol']}\nP&L: {pnl:+.2f}$ ({pct:+.2f}%)")

    @property
    def stats(self):
        with self._lock: return {"cycles":self._st["cycles"], "scans":self._st["scans"], "opened":self._st["opened"], "closed":self._st["closed"], "open_pos": len(self._pos), "positions": list(self._pos.values()), "dd": DD.st, "today": database.today()}

# ============================================================================
# FULL FLASK DASHBOARD
# ============================================================================
app = Flask(__name__)
engine = None

DASHBOARD_HTML = """
<!DOCTYPE html>
<html dir="rtl" lang="fa">
<head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Master-AI Dashboard</title>
    <style>
        body { font-family: Tahoma, sans-serif; background: #0d1117; color: #c9d1d9; padding: 20px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .card { background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 20px; }
        h1, h2 { color: #58a6ff; }
        .stat { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #21262d; }
        .pos { color: #3fb950; } .neg { color: #f85149; }
        table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #30363d; }
        th { color: #58a6ff; }
    </style>
</head>
<body>
    <h1 style="text-align:center">🤖 Master-AI Bot v5.4.2</h1>
    <p style="text-align:center">Last Update: {{ time }}</p>
    
    <div class="grid">
        <div class="card">
            <h2>📊 Status</h2>
            <div class="stat"><span>Mode:</span> <span>{{ 'DRY' if dry else 'LIVE FUTURES' }}</span></div>
            <div class="stat"><span>Balance:</span> <span>${{ "%.2f"|format(bal) }}</span></div>
            <div class="stat"><span>Drawdown:</span> <span class="{{ 'neg' if dd.dd>5 else '' }}">{{ dd.dd }}%</span></div>
            <div class="stat"><span>Timeframe:</span> <span>{{ tf }}</span></div>
        </div>
        
        <div class="card">
            <h2>📈 Today</h2>
            <div class="stat"><span>P&L:</span> <span class="{{ 'pos' if today.pnl>0 else 'neg' }}">{{ "%+.2f"|format(today.pnl) }}$</span></div>
            <div class="stat"><span>Win Rate:</span> <span>{{ today.wr }}% ({{ today.wins }}W / {{ today.losses }}L)</span></div>
            <div class="stat"><span>Positions:</span> <span>{{ open_pos }} / {{ max_pos }}</span></div>
            <div class="stat"><span>Total Scans:</span> <span>{{ stats.scans }}</span></div>
        </div>
    </div>

    {% if positions %}
    <div class="card" style="margin-top: 20px;">
        <h2>📋 Open Positions</h2>
        <table>
            <tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Current</th><th>SL</th><th>P&L</th><th>Conf</th></tr>
            {% for p in positions %}
            <tr>
                <td>{{ p.symbol }}</td>
                <td class="{{ 'pos' if p.side=='long' else 'neg' }}">{{ p.side.upper() }}</td>
                <td>{{ "%.4f"|format(p.entry) }}</td>
                <td>{{ "%.4f"|format(p.current_price) }}</td>
                <td>{{ "%.4f"|format(p.sl) }}</td>
                <td class="{{ 'pos' if p.unrealized_pnl>0 else 'neg' }}">{{ "%+.2f"|format(p.unrealized_pnl) }}$</td>
                <td>{{ p.conf }}%</td>
            </tr>
            {% endfor %}
        </table>
    </div>
    {% endif %}
    
    <script>setTimeout(() => location.reload(), 30000);</script>
</body>
</html>
"""

@app.route('/')
def home():
    if not engine: return "Loading...", 503
    st = engine.stats
    bal = EX.balance()
    
    positions_data = []
    if st['positions']:
        prices = EX.prices_bulk([p['symbol'] for p in st['positions']])
        for p in st['positions']:
            cp = prices.get(p['symbol'], p['entry'])
            upnl = (cp - p['entry']) * p['qty'] if p['side']=='long' else (p['entry'] - cp) * p['qty']
            positions_data.append({**p, 'current_price': cp, 'unrealized_pnl': upnl})
            
    return render_template_string(DASHBOARD_HTML, time=datetime.now().strftime('%H:%M:%S'), dry=DRY_RUN, bal=bal, dd=st['dd'], tf=TF, today=st['today'], open_pos=st['open_pos'], max_pos=MAX_POS, stats=st, positions=positions_data)

def main():
    global engine
    log.info("="*50)
    log.info(" MASTER-AI V5.4.2 BULLETPROOF EDITION")
    log.info("="*50)
    engine = Engine()
    threading.Thread(target=engine.loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
