#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=========================================================
💎 ALMASI TRAD v178 - Advanced Radar & Analytics Edition
Architecture: 7-Expert Ensemble Engine + Live Scanner
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

if sys.version_info < (3, 10):
    print("[CRITICAL] Python 3.10+ required")
    sys.exit(1)

_MISSING = []
for lib in ["pandas", "numpy", "requests", "ccxt", "flask"]:
    try: __import__(lib)
    except ImportError: _MISSING.append(lib)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError: pass

if _MISSING:
    print(f"[CRITICAL] Missing libs: {', '.join(_MISSING)}")
    sys.exit(1)

import pandas as pd
import numpy as np
import requests
import ccxt
from flask import Flask, render_template_string

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", stream=sys.stdout, force=True)
for lib in ("ccxt", "urllib3", "openai", "httpx", "httpcore", "werkzeug"):
    logging.getLogger(lib).setLevel(logging.ERROR)
log = logging.getLogger("Almasi")

# ============================================================================
# CONFIGURATION
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
        return os.getenv(k, "true" if d else "false").strip().lower() in ("1", "true", "yes")

API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")

SYMBOLS    = [x.strip() for x in Cfg.s("SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,XRP/USDT:USDT,DOGE/USDT:USDT").split(",")]
TF         = Cfg.s("TIMEFRAME", "3m").split(",")[0].strip()
if TF not in ["1m", "3m", "5m", "15m"]: TF = "3m"

RISK_PCT   = Cfg.f("RISK_PER_TRADE", 2.0)
MAX_POS    = int(Cfg.f("MAX_POSITIONS", 5.0))
DRY_RUN    = Cfg.b("DRY_RUN", False)
PORT       = int(Cfg.f("PORT", 10000))

# ============================================================================
# DATABASE
# ============================================================================
class DB:
    def __init__(self):
        self._path = "almasi_v178.db"
        self._lock = threading.Lock()
        self._boot()

    def _boot(self):
        import sqlite3
        with sqlite3.connect(self._path) as c:
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("""CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, symbol TEXT, side TEXT, entry_price REAL, exit_price REAL, quantity REAL, stop_loss REAL, take_profit REAL, status TEXT DEFAULT 'open', ai_signal TEXT, confidence INTEGER, pnl REAL DEFAULT 0, pnl_pct REAL DEFAULT 0, exit_reason TEXT, opened_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS daily_stats (date TEXT PRIMARY KEY, total INTEGER DEFAULT 0, wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, pnl REAL DEFAULT 0, win_rate REAL DEFAULT 0)""")

    @contextmanager
    def _cx(self):
        import sqlite3
        with self._lock:
            c = sqlite3.connect(self._path, timeout=10)
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
        rows = self.run("SELECT id,symbol,side,entry_price,quantity,stop_loss,take_profit,ai_signal,confidence FROM trades WHERE status='open'")
        return [dict(zip(["id","symbol","side","entry","qty","sl","tp","reason","conf"], r)) for r in rows] if rows else []

    def recent_closed(self, limit=5) -> List[Dict]:
        rows = self.run(f"SELECT symbol, side, entry_price, exit_price, pnl_pct, exit_reason, closed_at FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT {limit}")
        return [dict(zip(["symbol","side","entry","exit","pct","reason","time"], r)) for r in rows] if rows else []

    def insert(self, t: Dict):
        self.run("INSERT OR IGNORE INTO trades (id,symbol,side,entry_price,quantity,stop_loss,take_profit,ai_signal,confidence) VALUES (?,?,?,?,?,?,?,?,?)",
                 (t["id"], t["symbol"], t["side"], t["entry"], t["qty"], t["sl"], t["tp"], t["reason"], t["conf"]))

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
# TELEGRAM
# ============================================================================
class Alerts:
    def send(self, msg: str):
        if not TG_TOKEN or not TG_CHAT: return
        threading.Thread(target=lambda: requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}, timeout=5), daemon=True).start()
TG = Alerts()

# ============================================================================
# EXCHANGE
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex = ccxt.phemex({"apiKey": API_KEY, "secret": API_SECRET, "options": {"defaultType": "swap"}}) if API_KEY else None
    
    def ohlcv(self, sym: str, tf: str) -> pd.DataFrame:
        if not self._ex: return pd.DataFrame()
        for _ in range(2):
            try:
                df = pd.DataFrame(self._ex.fetch_ohlcv(sym, tf, limit=80), columns=["ts","open","high","low","close","vol"])
                return df.dropna().reset_index(drop=True)
            except: time.sleep(1)
        return pd.DataFrame()

    def price(self, sym: str) -> float:
        try: return float(self._ex.fetch_ticker(sym)["last"]) if self._ex else 0.0
        except: return 0.0

    def prices_bulk(self, syms: List[str]) -> Dict[str, float]:
        try: return {s: float(t["last"]) for s, t in self._ex.fetch_tickers(syms).items() if t.get("last")} if self._ex else {}
        except: return {}

    def balance(self) -> float:
        if DRY_RUN or not self._ex: return 5000.0
        try:
            b = self._ex.fetch_balance()
            return float(b.get("USDT", {}).get("free", 0.0))
        except: return 0.0

    def order(self, sym: str, side: str, qty: float):
        if DRY_RUN: return {"id": f"dry_{uuid.uuid4().hex[:6]}", "price": self.price(sym)}
        try:
            o = self._ex.create_order(sym, "market", side, qty)
            return {"id": o.get("id"), "price": float(o.get("fills", [{"price": self.price(sym)}])[0]["price"])}
        except Exception as e: log.error("Order Err: %s", e); return None

EX = Exchange()

# ============================================================================
# INDICATORS & MATH
# ============================================================================
class IND:
    @staticmethod
    def ema(c: pd.Series, n: int): return c.ewm(span=n, adjust=False).mean()
    @staticmethod
    def atr(h: pd.Series, l: pd.Series, c: pd.Series, n: int):
        tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(alpha=1/n, adjust=False).mean()
    @staticmethod
    def macd(c: pd.Series):
        line = IND.ema(c, 12) - IND.ema(c, 26)
        return line, IND.ema(line, 9), line - IND.ema(line, 9)
    @staticmethod
    def bbands(c: pd.Series, n=20): return c.rolling(n).mean() - 2*c.rolling(n).std(), c.rolling(n).mean() + 2*c.rolling(n).std()
    @staticmethod
    def safe(s, idx=-1):
        try: v = s.iloc[idx]; return float(v) if not pd.isna(v) else 0.0
        except: return 0.0

# ============================================================================
# ENGINE LOGIC (7 EXPERTS)
# ============================================================================
class Tech:
    def run(self, df: pd.DataFrame) -> Dict:
        if len(df) < 50: return {"score": 0, "experts": [], "atr": 0, "price": 0}
        c, h, l, v = df["close"], df["high"], df["low"], df["vol"]
        
        atr = IND.safe(IND.atr(h, l, c, 14))
        px = float(c.iloc[-1])
        score = 0; exps = []

        try:
            # 1. Momentum (%R pseudo)
            if c.iloc[-1] > h.rolling(14).max().shift(1).iloc[-1]: score+=1; exps.append("Mom_Up")
            elif c.iloc[-1] < l.rolling(14).min().shift(1).iloc[-1]: score-=1; exps.append("Mom_Dn")

            # 2. Trend Pullback (SMA20)
            sma20 = IND.safe(c.rolling(20).mean())
            if l.iloc[-1] <= sma20 < c.iloc[-1]: score+=1; exps.append("Pullback_Buy")
            elif h.iloc[-1] >= sma20 > c.iloc[-1]: score-=1; exps.append("Pullback_Sell")

            # 3. TTM Squeeze Volatility
            _, _, hist = IND.macd(c)
            h0, h1 = IND.safe(hist, -1), IND.safe(hist, -2)
            if h0 > 0 and h0 > h1: score+=1; exps.append("Sqz_Bull")
            elif h0 < 0 and h0 < h1: score-=1; exps.append("Sqz_Bear")

            # 4. Elder Impulse
            e13_0, e13_1 = IND.safe(IND.ema(c, 13), -1), IND.safe(IND.ema(c, 13), -2)
            if e13_0 > e13_1 and h0 > h1: score+=1; exps.append("Elder_Up")
            elif e13_0 < e13_1 and h0 < h1: score-=1; exps.append("Elder_Dn")

            # 5. Fast Micro Scalp
            e3, e8 = IND.safe(IND.ema(c, 3)), IND.safe(IND.ema(c, 8))
            if e3 > e8: score+=1; exps.append("Fast_Up")
            elif e3 < e8: score-=1; exps.append("Fast_Dn")

            # 6. VSA (Volume)
            if float(v.iloc[-1]) > float(IND.safe(v.rolling(20).mean(), -1)) * 1.5:
                if score > 0: score+=1; exps.append("Vol_Boost")
                elif score < 0: score-=1; exps.append("Vol_Boost")

        except Exception as e: log.error("Math Err: %s", e)

        return {"score": score, "experts": exps, "atr": atr, "price": px}

TECH = Tech()

# ============================================================================
# TRAILING STOP 
# ============================================================================
class Trail:
    def __init__(self): self._pk = {}; self._sl = {}
    def init(self, pid, e, sl): self._pk[pid] = e; self._sl[pid] = sl
    def update(self, pid, side, px, entry, atr, osl):
        td = atr * (1.0 if (px > entry + atr*0.5 if side=="long" else px < entry - atr*0.5) else 2.0)
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
        self.radar = {} # Live market scanner data
        self._lock = threading.Lock()
        for t in database.open_trades(): self._pos[t["id"]] = t; TR.init(t["id"], t["entry"], t["sl"])
        TG.send(f"💎 <b>Almasi v178 Started</b>\nTF: {TF} | Mode: {'DRY' if DRY_RUN else 'LIVE'}")

    def loop(self):
        while True:
            try:
                self._exits()
                now = time.time()
                if not hasattr(self, '_lscan') or now - self._lscan > 60:
                    self._scan(EX.balance())
                    self._lscan = now
                time.sleep(2)
            except Exception as e: log.error("Loop err: %s", e); time.sleep(5)

    def _scan(self, bal):
        with self._lock: n = len(self._pos)
        
        for sym in SYMBOLS:
            res = TECH.run(EX.ohlcv(sym, TF))
            sc, exps, px, atr = res["score"], res["experts"], res["price"], res["atr"]
            
            # Update Radar for Dashboard
            self.radar[sym] = {"score": sc, "experts": ", ".join(exps) if exps else "None", "price": px, "time": datetime.now().strftime("%H:%M:%S")}
            
            with self._lock:
                if len(self._pos) >= MAX_POS or sym in [p["symbol"] for p in self._pos.values()]: continue

            if sc >= 3 or sc <= -3:
                side = "long" if sc > 0 else "short"
                sl = px - (atr*1.5) if side=="long" else px + (atr*1.5)
                tp = px + (atr*3.5) if side=="long" else px - (atr*3.5)
                dist = abs(px - sl)
                
                if dist > 0:
                    m_qty = EX._ex.market(sym).get("limits",{}).get("amount",{}).get("min",0.001) if EX._ex else 0.001
                    qty = max(m_qty, round(((bal * (RISK_PCT/100))/dist) * px / m_qty) * m_qty)
                    
                    o = EX.order(sym, "buy" if side=="long" else "sell", qty)
                    if o:
                        pid = f"p_{uuid.uuid4().hex[:5]}"
                        fpx = o["price"]
                        pos = {"id":pid,"symbol":sym,"side":side,"entry":fpx,"qty":qty,"sl":sl,"tp":tp,"reason":f"Score {sc} [{self.radar[sym]['experts']}]", "conf": 80 + abs(sc)*5, "atr":atr}
                        with self._lock: self._pos[pid] = pos
                        TR.init(pid, fpx, sl)
                        database.insert(pos)
                        TG.send(f"🟢 <b>OPEN {side.upper()}</b> {sym}\nEntry: {fpx}\nScore: {sc}\nReason: {pos['reason']}")

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
                self._close(pid, pos, px, "Take Profit Hit")
            elif (pos["side"]=="long" and px<=nsl) or (pos["side"]=="short" and px>=nsl):
                # اگر قیمت خروج بهتر از قیمت ورود باشد، تیتر لاگ فرق می‌کند
                is_win = (px > pos["entry"] if pos["side"]=="long" else px < pos["entry"])
                self._close(pid, pos, px, "Trailing Stop (In Profit)" if is_win else "Stop Loss Hit")

    def _close(self, pid, pos, px, reason):
        EX.order(pos["symbol"], "sell" if pos["side"]=="long" else "buy", pos["qty"])
        pnl = (px - pos["entry"]) * pos["qty"] if pos["side"]=="long" else (pos["entry"] - px) * pos["qty"]
        pct = pnl / (pos["entry"] * pos["qty"]) * 100
        database.close(pid, px, pnl, pct, reason)
        with self._lock: self._pos.pop(pid, None)
        TR.rm(pid)
        TG.send(f"{'💸' if pnl>0 else '🩸'} <b>CLOSE</b> {pos['symbol']}\nReason: {reason}\nP&L: {pnl:+.2f}$ ({pct:+.2f}%)")

# ============================================================================
# FLASK WEB DASHBOARD
# ============================================================================
app = Flask(__name__)
engine = None

HTML = """
<!DOCTYPE html><html dir="ltr" lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Almasi Quant v178</title>
<style>
    body{font-family:'Segoe UI',Tahoma,sans-serif;background:#0d1117;color:#c9d1d9;padding:15px;margin:0;}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(350px,1fr));gap:15px;}
    .card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:15px;}
    h1,h2{color:#58a6ff;margin-top:0; border-bottom:1px solid #30363d; padding-bottom:10px;}
    .pos{color:#3fb950; font-weight:bold;} .neg{color:#f85149; font-weight:bold;}
    table{width:100%;border-collapse:collapse;font-size:14px;}
    th,td{padding:8px;text-align:left;border-bottom:1px solid #21262d;}
    th{color:#8b949e;}
    .badge{background:#238636; color:#fff; padding:2px 6px; border-radius:10px; font-size:12px;}
    .badge-wait{background:#8b949e; color:#fff; padding:2px 6px; border-radius:10px; font-size:12px;}
</style></head>
<body>
    <h1 style="text-align:center">💎 Almasi Quant Engine v178</h1>
    <div style="text-align:center; margin-bottom:20px; color:#8b949e;">Status: {{ '🔴 DRY RUN' if dry else '🟢 LIVE' }} | Timeframe: {{ tf }} | Bal: ${{ "%.2f"|format(bal) }}</div>
    
    <div class="grid">
        <div class="card">
            <h2>📈 Today's P&L</h2>
            <div style="font-size:24px;" class="{{ 'pos' if today.pnl>0 else 'neg' }}">{{ "%+.2f"|format(today.pnl) }} USDT</div>
            <p>Win Rate: {{ today.wr }}% ({{ today.trades }} Trades)</p>
            <p>Active Positions: {{ open_pos }} / {{ max_pos }}</p>
        </div>

        <div class="card">
            <h2>📡 Live Radar (Why no trade?)</h2>
            <table><tr><th>Symbol</th><th>Score</th><th>Active Experts</th><th>Status</th></tr>
            {% for sym, data in radar.items() %}
            <tr>
                <td>{{ sym }}</td>
                <td class="{{ 'pos' if data.score>=3 else ('neg' if data.score<=-3 else '') }}">{{ data.score }}</td>
                <td style="font-size:12px; color:#8b949e;">{{ data.experts }}</td>
                <td>{% if data.score>=3 or data.score<=-3 %}<span class="badge">Trade</span>{% else %}<span class="badge-wait">Wait</span>{% endif %}</td>
            </tr>
            {% endfor %}
            </table>
        </div>
    </div>

    <div class="card" style="margin-top:15px;">
        <h2>📋 Open Positions</h2>
        <table><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Current</th><th>SL (Trailing)</th><th>P&L</th><th>Why Entered?</th></tr>
        {% for p in positions %}
        <tr><td>{{ p.symbol }}</td><td class="{{ 'pos' if p.side=='long' else 'neg' }}">{{ p.side.upper() }}</td><td>{{ "%.4f"|format(p.entry) }}</td><td>{{ "%.4f"|format(p.current_price) }}</td><td style="color:#d2a8ff">{{ "%.4f"|format(p.sl) }}</td><td class="{{ 'pos' if p.unrealized_pnl>0 else 'neg' }}">{{ "%+.2f"|format(p.unrealized_pnl) }}$</td><td style="font-size:12px">{{ p.reason }}</td></tr>
        {% else %}<tr><td colspan="7" style="text-align:center">No active trades. Radar is searching...</td></tr>{% endfor %}
        </table>
    </div>

    <div class="card" style="margin-top:15px;">
        <h2>📜 Recent Exit Logs (Why did it close?)</h2>
        <table><tr><th>Time</th><th>Symbol</th><th>Side</th><th>P&L %</th><th>Exit Reason</th></tr>
        {% for t in history %}
        <tr><td style="color:#8b949e">{{ t.time[11:16] }}</td><td>{{ t.symbol }}</td><td>{{ t.side.upper() }}</td><td class="{{ 'pos' if t.pct>0 else 'neg' }}">{{ "%+.2f"|format(t.pct) }}%</td><td style="font-size:13px">{{ t.reason }}</td></tr>
        {% else %}<tr><td colspan="5" style="text-align:center">No closed trades yet today.</td></tr>{% endfor %}
        </table>
    </div>
    <script>setTimeout(()=>location.reload(), 15000);</script>
</body></html>
"""

@app.route('/')
def home():
    if not engine: return "Warming up Almasi v178 Engine...", 503
    with engine._lock: pos_list = list(engine._pos.values())
    p_data = []
    if pos_list:
        pxs = EX.prices_bulk([p['symbol'] for p in pos_list])
        for p in pos_list:
            cp = pxs.get(p['symbol'], p['entry'])
            p_data.append({**p, 'current_price': cp, 'unrealized_pnl': (cp - p['entry']) * p['qty'] if p['side']=='long' else (p['entry'] - cp) * p['qty']})
    
    return render_template_string(HTML, dry=DRY_RUN, tf=TF, bal=EX.balance(), today=database.today(), 
                                  open_pos=len(pos_list), max_pos=MAX_POS, radar=engine.radar, 
                                  positions=p_data, history=database.recent_closed(7))

if __name__ == "__main__":
    engine = Engine()
    threading.Thread(target=engine.loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
