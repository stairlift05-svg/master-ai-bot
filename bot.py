#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Trading Bot Pro v5.4.0 - PREMIUM EDITION
✅ فیکس خطای 30000 صرافی Phemex (OHLCV)
✅ ارتقاء استراتژی: فیلتر روند کلان (EMA 200)
✅ آماده معامله
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

def _setup_log():
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
    def s(k, d=""): return os.getenv(k, d).strip()
    @staticmethod
    def i(k, d): return int(os.getenv(k, str(d)).strip())
    @staticmethod
    def f(k, d): return float(os.getenv(k, str(d)).strip())
    @staticmethod
    def b(k, d=False): return os.getenv(k, "true" if d else "false").strip().lower() in ("1", "true", "yes", "on")
    @staticmethod
    def lst(k, d=""): return [x.strip() for x in (os.getenv(k, d) or d).split(",") if x.strip()]

API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")
OAI_KEY    = Cfg.s("OPENAI_API_KEY")
DB_URL     = Cfg.s("DATABASE_URL")

SYMBOLS    = Cfg.lst("SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,XRP/USDT:USDT,BNB/USDT:USDT,DOGE/USDT:USDT,ADA/USDT:USDT,AVAX/USDT:USDT,DOT/USDT:USDT,LINK/USDT:USDT")
TF         = Cfg.s("TIMEFRAME", "5m")
RISK_PCT   = Cfg.f("RISK_PER_TRADE", 1.5)
MAX_DD     = Cfg.f("MAX_DRAWDOWN", 10.0)
MAX_POS    = Cfg.i("MAX_POSITIONS", 5)
DRY_RUN    = Cfg.b("DRY_RUN", False)
TESTNET    = Cfg.b("PHEMEX_TESTNET", False)
PORT       = Cfg.i("PORT", 10000)

# ============================================================================
# PERFORMANCE TRACKER
# ============================================================================
class PerfTracker:
    def __init__(self, max_len=100):
        self._scans = deque(maxlen=max_len)
        self._signals = deque(maxlen=max_len)
        self._errors = deque(maxlen=max_len)
        
    def log_scan(self, duration): self._scans.append(duration)
    def log_signal(self, sym, action, conf):
        self._signals.append({"time": datetime.now(timezone.utc).isoformat(), "symbol": sym, "action": action, "confidence": conf})
    def log_error(self, error):
        self._errors.append({"time": datetime.now(timezone.utc).isoformat(), "error": error[:200]})
    
    @property
    def stats(self):
        return {
            "avg_scan_time": round(sum(self._scans) / len(self._scans), 2) if self._scans else 0,
            "recent_signals": list(self._signals)[-10:],
            "recent_errors": list(self._errors)[-5:],
            "total_scans": len(self._scans)
        }

PERF = PerfTracker()

# ============================================================================
# INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close, n=14):
        delta = close.diff()
        up = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        down_ma = down.ewm(com=n-1, adjust=False).mean().replace(0, 1e-10)
        rs = up.ewm(com=n-1, adjust=False).mean() / down_ma
        return 100 - (100 / (1 + rs))

    @staticmethod
    def ema(close, n):
        return close.ewm(span=n, adjust=False).mean()

    @staticmethod
    def atr(high, low, close, n=14):
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

    @staticmethod
    def macd(close, fast=12, slow=26, sig=9):
        e_fast = close.ewm(span=fast, adjust=False).mean()
        e_slow = close.ewm(span=slow, adjust=False).mean()
        line = e_fast - e_slow
        signal = line.ewm(span=sig, adjust=False).mean()
        hist = line - signal
        return line, signal, hist

    @staticmethod
    def bbands(close, n=20, std=2.0):
        mid = close.rolling(n).mean()
        sd = close.rolling(n).std()
        return mid - std*sd, mid, mid + std*sd

    @staticmethod
    def stoch(high, low, close, k=14, d=3):
        lo = low.rolling(k).min()
        hi = high.rolling(k).max()
        stk = 100 * (close - lo) / (hi - lo + 1e-10)
        std = stk.rolling(d).mean()
        return stk, std

    @staticmethod
    def safe(s, idx=-1):
        try:
            v = s.iloc[idx]
            return float(v) if not (v != v) else 0.0
        except: return 0.0

IND = Indicators()

# ============================================================================
# DATABASE
# ============================================================================
class DB:
    def __init__(self):
        self._path = "bot.db"
        self._lock = threading.Lock()
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

    def run(self, sql, p=()):
        try:
            with self._cx() as c:
                cur = c.cursor()
                cur.execute(sql, p)
                if sql.strip().upper().startswith("SELECT"): return cur.fetchall()
        except Exception as e: log.error("DB: %s", e)
        return None

    def open_trades(self):
        rows = self.run("SELECT id,symbol,side,entry_price,quantity,stop_loss,take_profit,confidence,opened_at FROM trades WHERE status='open'")
        return [dict(zip(["id","symbol","side","entry","qty","sl","tp","conf","opened"], r)) for r in rows] if rows else []

    def insert(self, t):
        self.run("INSERT OR IGNORE INTO trades (id,symbol,side,entry_price,quantity,stop_loss,take_profit,ai_signal,confidence) VALUES (?,?,?,?,?,?,?,?,?)",
                 (t["id"], t["symbol"], t["side"], t["entry"], t["qty"], t["sl"], t["tp"], t["signal"], t["conf"]))

    def close(self, tid, ep, pnl, pct, reason):
        self.run("UPDATE trades SET status='closed',exit_price=?,pnl=?,pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?", (ep, pnl, pct, reason, tid))
        self._stats(pnl)

    def _stats(self, pnl):
        today = datetime.now(timezone.utc).date().isoformat()
        row = self.run("SELECT total,wins,losses,pnl FROM daily_stats WHERE date=?", (today,))
        if row:
            tot, w, l, tp = row[0]
            tot, w, l, tp = tot+1, w+(1 if pnl>0 else 0), l+(0 if pnl>0 else 1), tp+pnl
            self.run("UPDATE daily_stats SET total=?,wins=?,losses=?,pnl=?,win_rate=? WHERE date=?", (tot, w, l, tp, round(w/tot*100,1), today))
        else:
            self.run("INSERT INTO daily_stats VALUES(?,1,?,?,?,?)", (today, 1 if pnl>0 else 0, 0 if pnl>0 else 1, pnl, 100.0 if pnl>0 else 0.0))

    def today(self):
        r = self.run("SELECT total,wins,losses,pnl,win_rate FROM daily_stats WHERE date=?", (datetime.now(timezone.utc).date().isoformat(),))
        return dict(zip(["trades","wins","losses","pnl","wr"], r[0])) if r else {"trades":0,"wins":0,"losses":0,"pnl":0.0,"wr":0.0}

database = DB()

# ============================================================================
# TELEGRAM ALERTS
# ============================================================================
class Alerts:
    def __init__(self):
        self._sent = {}
        self._lock = threading.Lock()
        self._chat_id = TG_CHAT
        self._queue = deque(maxlen=50)

    def _get_chat_id(self):
        if self._chat_id: return self._chat_id
        if not TG_TOKEN: return None
        try:
            res = requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates", timeout=5).json()
            if res.get("ok") and res.get("result"): return str(res["result"][-1]["message"]["chat"]["id"])
        except: pass
        return None

    def send(self, msg, key="", force=False, parse_mode="HTML"):
        log.info("📢 %s", msg[:100].replace("\n"," "))
        self._queue.append({"time": datetime.now(timezone.utc).isoformat(), "msg": msg[:200]})
        if not TG_TOKEN: return
        chat_id = self._get_chat_id()
        if not chat_id: return
        if key and not force:
            with self._lock:
                if time.time() - self._sent.get(key,0) < 30: return
                self._sent[key] = time.time()
        threading.Thread(target=lambda: requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": parse_mode}, timeout=10), daemon=True).start()

    def send_dashboard(self, engine_stats, balance):
        dd, td, ai, pos = engine_stats.get("dd", {}), engine_stats.get("today", {}), engine_stats.get("ai", {}), engine_stats.get("open_pos", 0)
        roi = ((balance - 10000) / 10000 * 100) if balance > 0 else 0
        msg = f"🤖 <b>Bot v5.4.0</b>\n{'🔵 DRY' if DRY_RUN else '🟢 LIVE'} | {TF}\n💰 Bal: {balance:,.2f} ({roi:+.2f}%)\n📉 DD: {dd.get('dd',0):.1f}%\n📊 Pos: {pos}/{MAX_POS}\n📈 Today P&L: {td.get('pnl',0):+.2f}$"
        self.send(msg, key="dashboard", force=True)

TG = Alerts()

# ============================================================================
# EXCHANGE (FIXED OHLCV FOR PHEMEX)
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex = None
        self._pc = {}
        self._connect()

    def _connect(self):
        if not API_KEY: return
        try:
            self._ex = ccxt.phemex({"apiKey": API_KEY, "secret": API_SECRET, "enableRateLimit": True, "options": {"defaultType": "swap"}})
            if TESTNET: self._ex.set_sandbox_mode(True)
            self._ex.load_markets()
            log.info("✅ Phemex connected. Markets loaded.")
        except Exception as e:
            log.error("Exchange error: %s", e)

    def _retry_ohlcv(self, sym, tf, lim):
        for attempt in range(4):
            try:
                # 🔧 FIXED: Calculate 'since' to prevent Phemex 30000 error
                tf_ms = self._ex.parse_timeframe(tf) * 1000
                since = self._ex.milliseconds() - (lim * tf_ms)
                
                # Fetch with explicit 'since'
                return self._ex.fetch_ohlcv(sym, tf, since=since, limit=lim, params={'type': 'swap'})
            except Exception as e:
                log.warning("ohlcv retry %d [%s]: %s", attempt+1, sym, str(e)[:100])
                time.sleep(2)
        raise RuntimeError(f"Failed OHLCV {sym}")

    def ohlcv(self, sym, tf, lim=250): # Increased to 250 for EMA200
        raw = self._retry_ohlcv(sym, tf, lim)
        df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df.dropna().reset_index(drop=True)

    def price(self, sym):
        now = time.time()
        if sym in self._pc and now - self._pc[sym][1] < 2.5: return self._pc[sym][0]
        p = float(self._ex.fetch_ticker(sym)["last"])
        self._pc[sym] = (p, now)
        return p

    def prices_bulk(self, syms):
        out = {}
        if not self._ex: return out
        try:
            tickers = self._ex.fetch_tickers(syms)
            for s, t in tickers.items():
                if t.get("last"):
                    p = float(t["last"])
                    out[s] = p
                    self._pc[s] = (p, time.time())
        except: pass
        return out

    def balance(self):
        if not self._ex or DRY_RUN: return 10000.0
        try:
            bal = self._ex.fetch_balance()
            for k in ("USDT","usdt","USD","usd"):
                if k in bal and bal[k].get("free"): return float(bal[k]["free"])
            return 0.0
        except: return 0.0

    def calculate_quantity(self, sym, usd_amount):
        if not self._ex: return None
        try:
            m = self._ex.market(sym)
            p = self.price(sym)
            min_q = m.get("limits", {}).get("amount", {}).get("min", 0.001)
            prec = m.get("precision", {}).get("amount", 0.001)
            q = max(min_q, round((usd_amount / p) / prec) * prec)
            return q
        except Exception as e: log.error("Qty err: %s", e); return None

    def order(self, sym, side, qty):
        if DRY_RUN: return {"id": f"dry_{uuid.uuid4().hex[:6]}", "price": self.price(sym)}
        try:
            o = self._ex.create_order(sym, "market", side, qty)
            fp = float(o.get("price", self.price(sym)))
            if "fills" in o and o["fills"]: fp = float(o["fills"][0]["price"])
            return {"id": o.get("id"), "price": fp}
        except Exception as e:
            log.error("Order err [%s]: %s", sym, e)
            return None

EX = Exchange()

# ============================================================================
# TECHNICAL ANALYSIS (UPGRADED WITH EMA 200)
# ============================================================================
@dataclass
class Sig:
    action: str = "neutral"
    conf: int = 0
    reason: str = ""
    risk: str = "medium"
    src: str = "tech"
    ind: Dict = field(default_factory=dict)
    
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

        # 🚀 MACRO TREND FILTER
        trend = "BULL" if price > e200 else "BEAR"
        if trend == "BULL": bs += 15; ss -= 10; tags.append("Trend=UP")
        else: ss += 15; bs -= 10; tags.append("Trend=DWN")

        if rsi < 30: bs += 35; tags.append("RSI_OS+")
        elif rsi < 40: bs += 20; tags.append("RSI_OS")
        if rsi > 70: ss += 35; tags.append("RSI_OB+")
        elif rsi > 60: ss += 20; tags.append("RSI_OB")

        if mh > 0: bs += 20
        if mh < 0: ss += 20

        if price > e20 > e50: bs += 20
        if price < e20 < e50: ss += 20

        if sk < 25: bs += 15
        if sk > 75: ss += 15

        if vr > 1.3:
            if bs > ss: bs += 10
            else: ss += 10

        ind = {"rsi":round(rsi,1), "macd_h":round(mh,4), "e200":round(e200,4), "atr":round(atr,4), "price":price}
        
        thr = 35 # Threshold
        if bs >= thr and bs > ss:
            cf = min(95, int(bs * 1.2))
            return Sig("buy", cf, "|".join(tags[:3]), "low" if cf>70 else "medium", "tech", ind)
        if ss >= thr and ss > bs:
            cf = min(95, int(ss * 1.2))
            return Sig("sell", cf, "|".join(tags[:3]), "low" if cf>70 else "medium", "tech", ind)

        return Sig(reason=f"B={bs} S={ss}", ind=ind)

TECH = Tech()

# ============================================================================
# AI ENGINE
# ============================================================================
class AI:
    def __init__(self):
        self._c = None
        self._cache = {}
        if OAI_KEY:
            try:
                from openai import OpenAI
                self._c = OpenAI(api_key=OAI_KEY, timeout=15.0)
            except: pass

    def analyze(self, sym, tech, n_open):
        if not self._c: return tech
        ck = f"{sym}_{tech.ind.get('price')}"
        if ck in self._cache: return self._cache[ck]
        
        prompt = f"Sym:{sym} RSI:{tech.ind.get('rsi')} Trend:{'UP' if tech.ind.get('price')>tech.ind.get('e200',0) else 'DWN'} Tech:{tech.action}. Reply JSON {{\"signal\":\"buy\"|\"sell\"|\"neutral\",\"confidence\":0-100}}"
        try:
            r = self._c.chat.completions.create(model="gpt-3.5-turbo", messages=[{"role":"user","content":prompt}], max_tokens=50, temperature=0.1)
            raw = re.sub(r"```[a-z]*|```","", r.choices[0].message.content).strip()
            aj = json.loads(raw)
            aa = aj.get("signal","neutral")
            ac = int(aj.get("confidence",0))
            
            if aa == tech.action and aa != "neutral":
                tech.conf = min(95, int((ac+tech.conf)/2*1.15))
                tech.src = "ai+tech"
            self._cache[ck] = tech
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
        if self._last is None: self._last = ts; return True
        if ts > self._last: self._last = ts; return True
        return False

TMR = Timer(TF)

class DDG:
    def __init__(self, mx):
        self.mx, self.pk, self.halted, self.dd = mx, None, False, 0.0
    def check(self, b):
        if self.pk is None: self.pk = b; return True
        if b > self.pk: self.pk = b; self.halted = False
        self.dd = round((self.pk - b) / self.pk * 100, 2)
        if self.dd >= self.mx: self.halted = True
        return not self.halted

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
        self._st = {"scans":0, "opened":0, "closed":0, "start":datetime.now(timezone.utc).isoformat()}
        bal = EX.balance()
        DD.check(bal)
        for t in database.open_trades():
            self._pos[t["id"]] = t; TR.init(t["id"], t["entry"], t["sl"])
        TG.send(f"🚀 <b>Bot v5.4.0 Started</b>\nBalance: ${bal:,.2f}", force=True)

    def loop(self):
        log.info("▶️ Loop started")
        while self._run:
            try:
                self._exits()
                bal = EX.balance()
                if DD.check(bal) and TMR.is_new(): self._scan(bal)
                time.sleep(5)
            except Exception as e: log.error("Loop err: %s", e); time.sleep(10)

    def _scan(self, bal):
        with self._lock: n = len(self._pos)
        if n >= MAX_POS: return
        self._st["scans"] += 1
        log.info("🔍 Scan #%d", self._st["scans"])
        
        for sym in SYMBOLS:
            with self._lock:
                if len(self._pos) >= MAX_POS: break
                if sym in [p["symbol"] for p in self._pos.values()]: continue
            
            try:
                df = EX.ohlcv(sym, TF, 250) # 250 for EMA200
                if len(df) < 200: continue
                
                tech = TECH.run(df)
                if tech.conf >= 30: # Only bother AI if tech is decent
                    sig = AI_ENG.analyze(sym, tech, n)
                else: sig = tech
                
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
                        if qty and qty > 0:
                            self._open(sym, sig, px, sl, tp, qty, risk)
            except Exception as e: log.error("Analyze %s: %s", sym, e)

    def _open(self, sym, sig, px, sl, tp, qty, risk):
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
            log.info("✅ OPEN %s %s @ %.4f", side, sym, fpx)

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

            tp_hit = (pos["side"]=="long" and px>=pos["tp"]) or (pos["side"]=="short" and px<=pos["tp"])
            sl_hit = (pos["side"]=="long" and px<=nsl) or (pos["side"]=="short" and px>=nsl)
            
            if tp_hit or sl_hit:
                self._close(pid, pos, px, "TP" if tp_hit else "SL")

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
        log.info("✅ CLOSE %s %s PNL=%.2f", pos["symbol"], reason, pnl)

    @property
    def stats(self):
        with self._lock: return {"open_pos": len(self._pos), "positions": list(self._pos.values()), "dd": DD.st, "today": database.today()}

# ============================================================================
# FLASK WEB
# ============================================================================
app = Flask(__name__)
engine = None

@app.route('/')
def home():
    if not engine: return "Starting...", 503
    st = engine.stats
    html = f"""
    <html dir="rtl"><body style="background:#0d1117;color:#c9d1d9;font-family:Tahoma;padding:20px;">
    <h2>🤖 Master-AI Bot v5.4.0 (Premium)</h2>
    <p>وضعیت: 🟢 فعال</p>
    <p>پوزیشن‌های باز: {st['open_pos']} / {MAX_POS}</p>
    <p>سود امروز: {st['today'].get('pnl',0):+.2f}$</p>
    <p>موجودی: ${EX.balance():.2f}</p>
    <script>setTimeout(()=>location.reload(), 30000);</script>
    </body></html>
    """
    return html

def main():
    global engine
    log.info("="*50)
    log.info(" MASTER-AI V5.4.0 (FIXED Phemex OHLCV)")
    log.info("="*50)
    engine = Engine()
    threading.Thread(target=engine.loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
