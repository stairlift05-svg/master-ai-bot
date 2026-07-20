#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=========================================================
🔥 ALMASI TRAD v177 - High-Frequency Quant Scalper 🔥
Architecture: 7-Expert Ensemble Engine + Volume Booster
=========================================================
"""

import os
import sys
import re
import json
import time
import uuid
import logging
import threading
from datetime import datetime, timezone
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional, Dict, List
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
    from dotenv import load_dotenv
    load_dotenv()
except ImportError: pass
try:
    from flask import Flask, render_template_string
except ImportError: _MISSING.append("flask")

if _MISSING:
    print(f"[CRITICAL] Missing libraries: {', '.join(_MISSING)}")
    sys.exit(1)

# ============================================================================
# LOGGING
# ============================================================================
IS_PROD = os.getenv("RENDER", "false").lower() == "true"
def _setup_log() -> logging.Logger:
    fmt = '{"t":"%(asctime)s","lvl":"%(levelname)s","msg":"%(message)s"}' if IS_PROD else "%(asctime)s | %(levelname)-8s | %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, stream=sys.stdout, force=True)
    for lib in ("ccxt", "urllib3", "openai", "httpx", "httpcore", "werkzeug"):
        logging.getLogger(lib).setLevel(logging.ERROR)
    return logging.getLogger("Almasi")

log = _setup_log()

# ============================================================================
# CONFIGURATION
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

# Optimized for Almasi Scalper
SYMBOLS    = Cfg.lst("SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,XRP/USDT:USDT,DOGE/USDT:USDT")
_raw_tf    = Cfg.s("TIMEFRAME", "3m") # Default to 3m for scalping
TF         = _raw_tf.split(",")[0].strip()
if TF not in ["1m", "3m", "5m", "15m"]: TF = "3m"

RISK_PCT   = Cfg.f("RISK_PER_TRADE", 2.0)
MAX_DD     = Cfg.f("MAX_DRAWDOWN", 15.0)
MAX_POS    = Cfg.i("MAX_POSITIONS", 5)
DRY_RUN    = Cfg.b("DRY_RUN", False)
TESTNET    = Cfg.b("PHEMEX_TESTNET", False)
PORT       = Cfg.i("PORT", 10000)

log.info("💎 ALMASI TRAD v177 CONFIG | TF: %s | Risk: %.1f%% | DryRun: %s", TF, RISK_PCT, DRY_RUN)

# ============================================================================
# DATABASE
# ============================================================================
class DB:
    def __init__(self):
        self._path  = "almasi_v177.db"
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
    def __init__(self): self._chat_id = TG_CHAT
    def _get_chat_id(self):
        if self._chat_id: return self._chat_id
        if not TG_TOKEN: return None
        try:
            res = requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates", timeout=5).json()
            if res.get("ok") and res.get("result"): return str(res["result"][-1]["message"]["chat"]["id"])
        except: pass
        return None
    def send(self, msg: str, force: bool = False):
        if not TG_TOKEN: return
        chat_id = self._get_chat_id()
        if not chat_id: return
        threading.Thread(target=lambda: requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10), daemon=True).start()

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
            self._ex = ccxt.phemex({"apiKey": API_KEY, "secret": API_SECRET, "enableRateLimit": True, "options": {"defaultType": "swap"}})
            if TESTNET: self._ex.set_sandbox_mode(True)
            self._ex.load_markets()
            log.info("✅ Phemex connected.")
        except Exception as e: log.error("Exchange connect: %s", e)

    def ohlcv(self, sym: str, tf: str, lim: int = 100) -> pd.DataFrame:
        for _ in range(3):
            try:
                raw = self._ex.fetch_ohlcv(sym, tf, limit=lim, params={'type': 'swap'})
                df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
                return df.dropna().reset_index(drop=True)
            except: time.sleep(1)
        raise RuntimeError(f"Fetch failed {sym}")

    def price(self, sym: str) -> float:
        now = time.time()
        if sym in self._pc and now - self._pc[sym][1] < 2: return self._pc[sym][0]
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
        if not self._ex or DRY_RUN: return 5000.0
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
# INDICATORS (7-STAR SCALP ENGINE)
# ============================================================================
class Indicators:
    @staticmethod
    def ema(close: pd.Series, n: int) -> pd.Series: return close.ewm(span=n, adjust=False).mean()
    
    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(alpha=1/n, adjust=False).mean()

    @staticmethod
    def macd(close: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9):
        line = Indicators.ema(close, fast) - Indicators.ema(close, slow)
        return line, Indicators.ema(line, sig), line - Indicators.ema(line, sig)

    @staticmethod
    def bbands(close: pd.Series, n: int = 20, std: float = 2.0):
        mid = close.rolling(n).mean()
        sd = close.rolling(n).std()
        return mid - std*sd, mid, mid + std*sd

    @staticmethod
    def williams_r(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
        hh = high.rolling(n).max()
        ll = low.rolling(n).min()
        return -100 * (hh - close) / (hh - ll + 1e-10)

    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
        up, down = high.diff(), -low.diff()
        pdm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=high.index)
        ndm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=high.index)
        tr = Indicators.atr(high, low, close, n)
        pdi = 100 * pdm.ewm(alpha=1/n, adjust=False).mean() / (tr + 1e-10)
        ndi = 100 * ndm.ewm(alpha=1/n, adjust=False).mean() / (tr + 1e-10)
        dx = 100 * (pdi - ndi).abs() / (pdi + ndi + 1e-10)
        return dx.ewm(alpha=1/n, adjust=False).mean()

    @staticmethod
    def keltner_channels(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 20, m: float = 1.5):
        ema20 = Indicators.ema(close, n)
        atr_val = Indicators.atr(high, low, close, n)
        return ema20 - (m * atr_val), ema20 + (m * atr_val)

    @staticmethod
    def safe(s, idx: int = -1) -> float:
        try: v = s.iloc[idx]; return float(v) if not pd.isna(v) else 0.0
        except: return 0.0

IND = Indicators()

# ============================================================================
# TECHNICAL ANALYSIS (ALMASI 7-EXPERT LOGIC)
# ============================================================================
@dataclass
class Sig:
    action: str = "neutral"
    conf  : int = 0
    reason: str = ""
    ind   : Dict = field(default_factory=dict)
    @property
    def ok(self) -> bool: return self.action in ("buy","sell") and self.conf >= 70

class Tech:
    def run(self, df: pd.DataFrame) -> Sig:
        if len(df) < 50: return Sig(reason="Data < 50")
        c, h, l, v = df["close"], df["high"], df["low"], df["vol"]

        atr = IND.safe(IND.atr(h, l, c, 14))
        price = float(c.iloc[-1])
        ind_dict = {"price": price, "atr": atr}
        w_sig = r_sig = c_sig = b_sig = e_sig = t_sig = m_sig = 0
        experts = []

        try:
            # 1. Williams %R
            wr = IND.williams_r(h, l, c, 14)
            wr0, wr1 = IND.safe(wr, -1), IND.safe(wr, -2)
            if wr1 < -80 and wr0 >= -80: w_sig = 1
            elif wr1 > -20 and wr0 <= -20: w_sig = -1

            # 2. Raschke Holy Grail
            adx_series = IND.adx(h, l, c, 14)
            sma20 = c.rolling(20).mean()
            if IND.safe(adx_series, -1) > 25:
                if l.iloc[-1] <= IND.safe(sma20, -1) and c.iloc[-1] > IND.safe(sma20, -1): r_sig = 1
                elif h.iloc[-1] >= IND.safe(sma20, -1) and c.iloc[-1] < IND.safe(sma20, -1): r_sig = -1

            # 3. Carter TTM Squeeze
            bbl, _, bbh = IND.bbands(c, 20, 2.0)
            kcl, kcu = IND.keltner_channels(h, l, c, 20, 1.5)
            _, _, macd_hist = IND.macd(c)
            hist0, hist1 = IND.safe(macd_hist, -1), IND.safe(macd_hist, -2)
            if (IND.safe(bbh, -1) < IND.safe(kcu, -1)) and (IND.safe(bbl, -1) > IND.safe(kcl, -1)):
                if hist0 > 0 and hist0 > hist1: c_sig = 1
                elif hist0 < 0 and hist0 < hist1: c_sig = -1

            # 4. Al Brooks Gap Bar
            ema20 = IND.ema(c, 20)
            e0 = IND.safe(ema20, -1)
            if all(l.iloc[i] > IND.safe(ema20, i) for i in range(-4, -1)) and l.iloc[-1] < e0 and c.iloc[-1] > df["open"].iloc[-1]: b_sig = 1
            if all(h.iloc[i] < IND.safe(ema20, i) for i in range(-4, -1)) and h.iloc[-1] > e0 and c.iloc[-1] < df["open"].iloc[-1]: b_sig = -1

            # 5. Elder Impulse
            ema13 = IND.ema(c, 13)
            ema13_0, ema13_1 = IND.safe(ema13, -1), IND.safe(ema13, -2)
            if ema13_0 > ema13_1 and hist0 > hist1: e_sig = 1
            elif ema13_0 < ema13_1 and hist0 < hist1: e_sig = -1

            # 6. Turtle Micro Breakout
            dh20 = h.rolling(20).max().shift(1)
            dl20 = l.rolling(20).min().shift(1)
            if c.iloc[-1] > IND.safe(dh20, -1): t_sig = 1
            elif c.iloc[-1] < IND.safe(dl20, -1): t_sig = -1
            
            # 7. Fast Momentum Scalp (Almasi Special)
            e3, e8 = IND.ema(c, 3), IND.ema(c, 8)
            if e3.iloc[-1] > e8.iloc[-1] and e3.iloc[-2] <= e8.iloc[-2]: m_sig = 1
            elif e3.iloc[-1] < e8.iloc[-1] and e3.iloc[-2] >= e8.iloc[-2]: m_sig = -1

        except Exception as e:
            return Sig(reason=f"Math Err: {e}", ind=ind_dict)

        total_score = w_sig + r_sig + c_sig + b_sig + e_sig + t_sig + m_sig
        
        # 🟢 VSA: Volume Surge Multiplier
        vol_ma = v.rolling(20).mean()
        if float(v.iloc[-1]) > float(IND.safe(vol_ma, -1)) * 1.5:
            if total_score > 0: total_score += 1
            elif total_score < 0: total_score -= 1
            experts.append("Vol_Surge")

        if w_sig: experts.append("Williams")
        if r_sig: experts.append("Raschke")
        if c_sig: experts.append("Carter")
        if b_sig: experts.append("Brooks")
        if e_sig: experts.append("Elder")
        if t_sig: experts.append("Turtle")
        if m_sig: experts.append("FastMom")

        AGREEMENT_THRESHOLD = 3 
        
        if total_score >= AGREEMENT_THRESHOLD:
            conf = min(99, 65 + (total_score * 5))
            return Sig("buy", conf, f"Score:+{total_score} [{','.join(experts)}]", ind=ind_dict)
        elif total_score <= -AGREEMENT_THRESHOLD:
            conf = min(99, 65 + (abs(total_score) * 5))
            return Sig("sell", conf, f"Score:{total_score} [{','.join(experts)}]", ind=ind_dict)

        return Sig(reason=f"Neutral (Score: {total_score})", ind=ind_dict)

TECH = Tech()

# ============================================================================
# AI ENGINE (Optional Final Check)
# ============================================================================
class AI:
    def __init__(self):
        self._c = None
        if OAI_KEY:
            try:
                from openai import OpenAI
                self._c = OpenAI(api_key=OAI_KEY, timeout=10.0)
            except: pass

    def analyze(self, sym, tech):
        if not self._c or tech.conf < 70: return tech
        prompt = f"Sym:{sym} Action:{tech.action} Conf:{tech.conf} Tech:{tech.reason}. JSON reply: {{\"signal\":\"buy\"|\"sell\"|\"neutral\",\"confidence\":0-100}}"
        try:
            r = self._c.chat.completions.create(model="gpt-3.5-turbo", messages=[{"role":"user","content":prompt}], max_tokens=50, temperature=0.1)
            aj = json.loads(re.sub(r"```[a-z]*|```","", r.choices[0].message.content).strip())
            if aj.get("signal") == tech.action: tech.conf = min(99, int((aj.get("confidence", tech.conf) + tech.conf) / 2))
            return tech
        except: return tech

AI_ENG = AI()

# ============================================================================
# TRAILING STOP & RISK MGT
# ============================================================================
class Trail:
    def __init__(self): self._pk = {}; self._sl = {}
    def init(self, pid, e, sl): self._pk[pid] = e; self._sl[pid] = sl
    
    def update(self, pid, side, px, entry, atr, osl):
        # Almasi Aggressive Trailing for Scalping
        in_profit = (px > entry + (atr*0.5)) if side == "long" else (px < entry - (atr*0.5))
        td = atr * 1.0 if in_profit else atr * 2.0 # Tighten stop if in profit
        
        if side == "long":
            self._pk[pid] = max(self._pk.get(pid, px), px)
            self._sl[pid] = max(self._sl.get(pid, osl), self._pk[pid] - td)
        else:
            self._pk[pid] = min(self._pk.get(pid, px), px)
            self._sl[pid] = min(self._sl.get(pid, osl), self._pk[pid] + td)
        return self._sl[pid]
        
    def rm(self, pid): self._pk.pop(pid, None); self._sl.pop(pid, None)

TR = Trail()

class Engine:
    def __init__(self):
        self._pos = {}
        self._lock = threading.Lock()
        self._run = True
        self._st = {"cycles":0, "scans":0, "opened":0, "closed":0}
        for t in database.open_trades(): self._pos[t["id"]] = t; TR.init(t["id"], t["entry"], t["sl"])
        TG.send(f"💎 <b>Almasi Trad v177 Started</b>\nTF: {TF} | Mode: {'DRY' if DRY_RUN else 'LIVE'}", force=True)

    def loop(self):
        last_scan = 0
        scan_interval = {"1m": 45, "3m": 120, "5m": 240}.get(TF, 120)
        
        while self._run:
            try:
                self._st["cycles"] += 1
                self._exits()
                now = time.time()
                if now - last_scan >= scan_interval:
                    self._scan(EX.balance())
                    last_scan = now
                time.sleep(2)
            except Exception as e: log.error("Loop err: %s", e); time.sleep(5)

    def _scan(self, bal):
        with self._lock: n = len(self._pos)
        if n >= MAX_POS: return
        self._st["scans"] += 1
        
        for sym in SYMBOLS:
            with self._lock:
                if len(self._pos) >= MAX_POS: break
                if sym in [p["symbol"] for p in self._pos.values()]: continue
            
            try:
                df = EX.ohlcv(sym, TF, 100)
                tech = TECH.run(df)
                sig = AI_ENG.analyze(sym, tech) if tech.conf >= 70 else tech
                
                if sig.ok:
                    px = sig.ind["price"]
                    atr = sig.ind["atr"]
                    sl = px - (atr*1.5) if sig.action=="buy" else px + (atr*1.5)
                    tp = px + (atr*3.5) if sig.action=="buy" else px - (atr*3.5)
                    
                    risk = bal * (RISK_PCT/100)
                    dist = abs(px - sl)
                    if dist > 0:
                        qty = EX.calculate_quantity(sym, (risk/dist) * px)
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
            TG.send(f"🟢 <b>ALMASI OPEN {side.upper()}</b> {sym}\nEntry: {fpx:.4f}\nSL: {sl:.4f}\nReason: {sig.reason}\nConf: {sig.conf}%")

    def _exits(self):
        with self._lock: snap = dict(self._pos)
        if not snap: return
        pxs = EX.prices_bulk(list({p["symbol"] for p in snap.values()}))
        
        for pid, pos in snap.items():
            px = pxs.get(pos["symbol"])
            if not px: continue
            
            nsl = TR.update(pid, pos["side"], px, pos["entry"], pos.get("atr", px*0.01), pos["sl"])
            if abs(nsl - pos["sl"]) > 1e-8:
                with self._lock:
                    if pid in self._pos: self._pos[pid]["sl"] = nsl
                database.run("UPDATE trades SET stop_loss=? WHERE id=?", (nsl, pid))

            if (pos["side"]=="long" and px>=pos["tp"]) or (pos["side"]=="short" and px<=pos["tp"]):
                self._close(pid, pos, px, "TP (Scalp Target)")
            elif (pos["side"]=="long" and px<=nsl) or (pos["side"]=="short" and px>=nsl):
                self._close(pid, pos, px, "Trailing SL")

    def _close(self, pid, pos, px, reason):
        o = EX.order(pos["symbol"], "sell" if pos["side"]=="long" else "buy", pos["qty"])
        if o: px = o.get("price", px)
        pnl = (px - pos["entry"]) * pos["qty"] if pos["side"]=="long" else (pos["entry"] - px) * pos["qty"]
        pct = pnl / (pos["entry"] * pos["qty"]) * 100
        database.close(pid, px, pnl, pct, reason)
        with self._lock: self._pos.pop(pid, None)
        TR.rm(pid)
        self._st["closed"] += 1
        icon = "💸" if pnl > 0 else "🩸"
        TG.send(f"{icon} <b>ALMASI CLOSE</b> {pos['symbol']}\nReason: {reason}\nP&L: {pnl:+.2f}$ ({pct:+.2f}%)")

    @property
    def stats(self):
        with self._lock: return {"scans":self._st["scans"], "open_pos": len(self._pos), "positions": list(self._pos.values()), "today": database.today()}

# ============================================================================
# FLASK DASHBOARD
# ============================================================================
app = Flask(__name__)
engine = None

HTML = """
<!DOCTYPE html><html dir="rtl" lang="fa"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Almasi Trad v177</title>
<style>body{font-family:Tahoma;background:#0d1117;color:#c9d1d9;padding:20px}.card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:20px;margin-bottom:20px}h1,h2{color:#58a6ff;text-align:center}.pos{color:#3fb950}.neg{color:#f85149}table{width:100%;border-collapse:collapse}th,td{padding:10px;text-align:left;border-bottom:1px solid #30363d}</style></head>
<body>
    <h1>💎 Almasi Trad v177 - Scalper</h1>
    <div class="card"><h2>📈 Today's Performance</h2>
        <p>P&L: <span class="{{ 'pos' if today.pnl>0 else 'neg' }}">{{ "%+.2f"|format(today.pnl) }}$</span> | Win Rate: {{ today.wr }}% | Trades: {{ today.trades }}</p>
        <p>Mode: {{ 'DRY RUN' if dry else 'LIVE' }} | TF: {{ tf }} | Scans: {{ stats.scans }}</p>
    </div>
    {% if positions %}<div class="card"><h2>📋 Live Positions ({{ open_pos }}/{{ max_pos }})</h2><table>
        <tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Current</th><th>SL</th><th>P&L</th><th>Conf</th></tr>
        {% for p in positions %}<tr><td>{{ p.symbol }}</td><td class="{{ 'pos' if p.side=='long' else 'neg' }}">{{ p.side.upper() }}</td><td>{{ "%.4f"|format(p.entry) }}</td><td>{{ "%.4f"|format(p.current_price) }}</td><td>{{ "%.4f"|format(p.sl) }}</td><td class="{{ 'pos' if p.unrealized_pnl>0 else 'neg' }}">{{ "%+.2f"|format(p.unrealized_pnl) }}$</td><td>{{ p.conf }}%</td></tr>{% endfor %}
    </table></div>{% endif %}
    <script>setTimeout(()=>location.reload(), 20000);</script>
</body></html>"""

@app.route('/')
def home():
    if not engine: return "Starting Almasi Engine...", 503
    st = engine.stats
    p_data = []
    if st['positions']:
        pxs = EX.prices_bulk([p['symbol'] for p in st['positions']])
        for p in st['positions']:
            cp = pxs.get(p['symbol'], p['entry'])
            upnl = (cp - p['entry']) * p['qty'] if p['side']=='long' else (p['entry'] - cp) * p['qty']
            p_data.append({**p, 'current_price': cp, 'unrealized_pnl': upnl})
    return render_template_string(HTML, dry=DRY_RUN, tf=TF, today=st['today'], open_pos=st['open_pos'], max_pos=MAX_POS, stats=st, positions=p_data)

def main():
    global engine
    log.info("="*50)
    log.info("💎 STARTING ALMASI TRAD V177 SCALPER 💎")
    log.info("="*50)
    engine = Engine()
    threading.Thread(target=engine.loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__": main()
