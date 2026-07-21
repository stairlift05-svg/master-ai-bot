#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=========================================================
💎 ALMASI QUANT v183 Pro - Full Fixed Version
=========================================================
"""

import os
import sys
import time
import uuid
import logging
import threading
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Optional, Dict, List

if sys.version_info < (3, 10):
    print("[CRITICAL] Python 3.10+ required")
    sys.exit(1)

import pandas as pd
import requests
import ccxt
from flask import Flask, render_template_string

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError: pass

# LOGGING
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", stream=sys.stdout, force=True)
for lib in ("ccxt", "urllib3", "werkzeug"):
    logging.getLogger(lib).setLevel(logging.ERROR)
log = logging.getLogger("Almasi")

# CONFIG
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
        return os.getenv(k, "true" if d else "false").strip().lower() in ("1", "true", "yes")

API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")

_def_sym   = "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,XRP/USDT:USDT,DOGE/USDT:USDT"
SYMBOLS    = [x.strip() for x in Cfg.s("SYMBOLS", _def_sym).split(",")]
TF         = Cfg.s("TIMEFRAME", "3m").split(",")[0].strip()

RISK_PCT   = Cfg.f("RISK_PER_TRADE", 0.8)
LEVERAGE   = Cfg.i("LEVERAGE", 5)
MAX_POS    = Cfg.i("MAX_POSITIONS", 3)
DRY_RUN    = Cfg.b("DRY_RUN", True)
STRICT_QUALITY = Cfg.b("STRICT_QUALITY", True)
PORT       = Cfg.i("PORT", 10000)

TAKER_FEE  = 0.0006
COOLDOWN_SEC = 120

# DATABASE
class DB:
    def __init__(self):
        self._path = "almasi_v183.db"
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

    def recent_closed(self, limit=7) -> List[Dict]:
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

# TELEGRAM
class Alerts:
    def send(self, msg: str):
        if not TG_TOKEN or not TG_CHAT: return
        threading.Thread(target=lambda: requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}, timeout=5), daemon=True).start()
TG = Alerts()

# EXCHANGE
class Exchange:
    def __init__(self):
        self._ex = ccxt.phemex({"apiKey": API_KEY, "secret": API_SECRET, "options": {"defaultType": "swap"}, 'enableRateLimit': True}) if API_KEY else None
        if self._ex and not DRY_RUN:
            self._ex.load_markets()
            self._set_leverage()

    def _set_leverage(self):
        for sym in SYMBOLS:
            try:
                self._ex.set_leverage(LEVERAGE, sym)
                log.info(f"Leverage set to {LEVERAGE}x for {sym}")
            except Exception as e:
                log.warning(f"Could not set leverage for {sym}: {str(e)[:80]}")

    def ohlcv(self, sym: str, tf: str, limit: int = 300) -> pd.DataFrame:
        for _ in range(3):
            try:
                df = pd.DataFrame(self._ex.fetch_ohlcv(sym, tf, limit=limit), columns=["ts","open","high","low","close","vol"])
                return df.dropna().reset_index(drop=True)
            except Exception as e:
                log.warning(f"OHLCV retry for {sym}: {e}")
                time.sleep(1.5)
        return pd.DataFrame()

    def price(self, sym: str) -> float:
        try: return float(self._ex.fetch_ticker(sym)["last"]) if self._ex else 0.0
        except: return 0.0

    def prices_bulk(self, syms: List[str]) -> Dict[str, float]:
        try: return {s: float(t["last"]) for s, t in self._ex.fetch_tickers(syms).items() if t.get("last")} if self._ex else {}
        except: return {}

    def balance(self) -> float:
        if DRY_RUN or not self._ex: return 1000.0
        try:
            b = self._ex.fetch_balance()
            return float(b.get("USDT", {}).get("free", 0.0))
        except: return 0.0

    def order(self, sym: str, side: str, qty: float, sl: float = None, tp: float = None, reduce_only: bool = False):
        if DRY_RUN: 
            return {"id": f"dry_{uuid.uuid4().hex[:8]}", "price": self.price(sym)}
        try:
            params = {'reduceOnly': True} if reduce_only else {}
            if sl and not reduce_only: params['stopLossPrice'] = sl
            if tp and not reduce_only: params['takeProfitPrice'] = tp
            o = self._ex.create_order(sym, "market", side, qty, params=params)
            filled_price = float(o.get("price") or self.price(sym))
            return {"id": o.get("id"), "price": filled_price}
        except Exception as e: 
            log.error("Order Err [%s %s]: %s", side, sym, e)
            return None

EX = Exchange()

# INDICATORS
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
    def safe(s, idx=-1):
        try: v = s.iloc[idx]; return float(v) if not pd.isna(v) else 0.0
        except: return 0.0

# TECH
class Tech:
    def run(self, raw_df: pd.DataFrame) -> Dict:
        if len(raw_df) < 200: return {"score": 0, "experts": [], "atr": 0, "price": 0, "ts": 0}
        df = raw_df.iloc[:-1].copy()
        c, h, l, v, o = df["close"], df["high"], df["low"], df["vol"], df["open"]
        candle_ts = int(df["ts"].iloc[-1])
        atr = IND.safe(IND.atr(h, l, c, 14))
        px = float(c.iloc[-1])
        score = 0; exps = []

        try:
            hh = h.rolling(14).max()
            ll = l.rolling(14).min()
            wr = -100 * (hh - c) / (hh - ll + 1e-10)
            wr0 = IND.safe(wr, -1)
            if wr0 >= -40: score+=1; exps.append("WillR_Buy")
            elif wr0 <= -60: score-=1; exps.append("WillR_Sell")

            sma20 = IND.safe(c.rolling(20).mean())
            ema200 = IND.safe(IND.ema(c, 200))
            buffer = px * 0.003
            if px > ema200 and l.iloc[-1] <= (sma20 + buffer) and c.iloc[-1] > (sma20 - buffer): 
                score+=1; exps.append("Pullback_Buy")
            elif px < ema200 and h.iloc[-1] >= (sma20 - buffer) and c.iloc[-1] < (sma20 + buffer): 
                score-=1; exps.append("Pullback_Sell")

            _, _, hist = IND.macd(c)
            h0, h1 = IND.safe(hist, -1), IND.safe(hist, -2)
            if h0 > 0 and h0 > h1: score+=1; exps.append("Sqz_Bull")
            elif h0 < 0 and h0 < h1: score-=1; exps.append("Sqz_Bear")

            e13_0, e13_1 = IND.safe(IND.ema(c, 13), -1), IND.safe(IND.ema(c, 13), -2)
            if e13_0 > e13_1 and h0 > h1: score+=1; exps.append("Elder_Up")
            elif e13_0 < e13_1 and h0 < h1: score-=1; exps.append("Elder_Dn")

            e3, e8 = IND.safe(IND.ema(c, 3)), IND.safe(IND.ema(c, 8))
            if e3 > e8: score+=1; exps.append("Fast_Up")
            elif e3 < e8: score-=1; exps.append("Fast_Dn")

            vol_ma = IND.safe(v.rolling(20).mean(), -1)
            if float(v.iloc[-1]) > float(vol_ma) * 1.2:
                if c.iloc[-1] > o.iloc[-1]: score+=1; exps.append("Vol_Bull")
                elif c.iloc[-1] < o.iloc[-1]: score-=1; exps.append("Vol_Bear")
        except Exception as e: log.error("Math Err: %s", e)

        return {"score": score, "experts": exps, "atr": atr, "price": px, "ts": candle_ts}

TECH = Tech()

# TRAIL
class Trail:
    def __init__(self): self._pk = {}; self._sl = {}
    def init(self, pid, e, sl): self._pk[pid] = e; self._sl[pid] = sl
    def update(self, pid, side, px, entry, atr, osl):
        in_profit = (px > entry + atr*0.5 if side=="long" else px < entry - atr*0.5)
        td = atr * (1.0 if in_profit else 1.5) 
        if side == "long":
            self._pk[pid] = max(self._pk.get(pid, px), px)
            self._sl[pid] = max(self._sl.get(pid, osl), self._pk[pid] - td)
        else:
            self._pk[pid] = min(self._pk.get(pid, px), px)
            self._sl[pid] = min(self._sl.get(pid, osl), self._pk[pid] + td)
        return self._sl[pid]
    def rm(self, pid): self._pk.pop(pid, None); self._sl.pop(pid, None)

TR = Trail()

# ENGINE
class Engine:
    def __init__(self):
        self._pos = {}
        self._pending = set()
        self._cooldown = {}
        self._last_signal = {}
        self.radar = {}
        self._lock = threading.Lock()
        for t in database.open_trades(): 
            self._pos[t["id"]] = t
            TR.init(t["id"], t["entry"], t["sl"])
        TG.send(f"🛡 <b>Almasi v183 Pro Started</b>\nDRY: {DRY_RUN} | Strict: {STRICT_QUALITY}")

    def loop(self):
        while True:
            try:
                self._exits()
                now = time.time()
                if not hasattr(self, '_lscan') or now - self._lscan > 15:
                    self._scan(EX.balance())
                    self._lscan = now
                time.sleep(2)
            except Exception as e: 
                log.error("Loop err: %s", e); time.sleep(5)

    def _scan(self, bal):
        now = time.time()
        for sym in SYMBOLS:
            if now - self._cooldown.get(sym, 0) < COOLDOWN_SEC:
                self.radar[sym] = {"score": 0, "experts": "Cooldown", "status": "Cooldown"}
                continue

            res = TECH.run(EX.ohlcv(sym, TF))
            sc, exps, px, atr, c_ts = res["score"], res["experts"], res["price"], res["atr"], res["ts"]
            if self._last_signal.get(sym) == c_ts: continue

            self.radar[sym] = {"score": sc, "experts": ", ".join(exps) if exps else "None", "price": px, "status": "Wait"}
            
            if sc >= 2 or sc <= -2:
                side = "long" if sc > 0 else "short"
                has_quality = True
                if STRICT_QUALITY and abs(sc) == 2:
                    if side == "long":
                        has_quality = any(x in exps for x in ["Elder_Up", "Sqz_Bull", "Pullback_Buy"])
                    else:
                        has_quality = any(x in exps for x in ["Elder_Dn", "Sqz_Bear", "Pullback_Sell"])
                if not has_quality:
                    self.radar[sym]["status"] = "Rejected (Weak)"
                    continue
                self.radar[sym]["status"] = "Trade Triggered"
                self._execute_trade(sym, side, sc, exps, px, atr, bal)

    def _execute_trade(self, sym, side, sc, exps, px, atr, bal):
        with self._lock:
            if len(self._pos) >= MAX_POS or any(p["symbol"] == sym for p in self._pos.values()) or sym in self._pending:
                return
            self._pending.add(sym)
        try:
            risk_amount = bal * (RISK_PCT / 100)
            stop_distance = max(atr * 1.5, px * 0.005)
            risk_qty = risk_amount / stop_distance
            max_notional = bal * 0.25 * LEVERAGE
            max_qty = max_notional / px
            qty = max(0.001, round(min(risk_qty, max_qty), 4))

            est_sl = px - stop_distance if side=="long" else px + stop_distance
            est_tp = px + (atr*4) if side=="long" else px - (atr*4)
            
            o = EX.order(sym, "buy" if side=="long" else "sell", qty, sl=est_sl, tp=est_tp)
            if o:
                pid = f"p_{uuid.uuid4().hex[:6]}"
                fpx = o["price"]
                real_sl = fpx - (atr*1.5) if side=="long" else fpx + (atr*1.5)
                real_tp = fpx + (atr*4) if side=="long" else fpx - (atr*4)
                pos = {"id":pid,"symbol":sym,"side":side,"entry":fpx,"qty":qty,"sl":real_sl,"tp":real_tp,"reason":f"Score {sc}","conf":80,"atr":atr}
                with self._lock: self._pos[pid] = pos
                TR.init(pid, fpx, real_sl)
                database.insert(pos)
                self._last_signal[sym] = c_ts
                TG.send(f"🟢 OPEN {side.upper()} {sym} | Entry: {fpx}")
        finally:
            with self._lock:
                if sym in self._pending: self._pending.remove(sym)

    def _exits(self):
        with self._lock: snap = dict(self._pos)
        if not snap: return
        pxs = EX.prices_bulk(list({p["symbol"] for p in snap.values()}))
        for pid, pos in snap.items():
            px = pxs.get(pos["symbol"])
            if not px: continue
            current_atr = pos.get("atr") or (abs(pos["entry"] - pos["sl"]) / 1.5)
            nsl = TR.update(pid, pos["side"], px, pos["entry"], current_atr, pos["sl"])
            if abs(nsl - pos["sl"]) > 1e-6:
                with self._lock:
                    if pid in self._pos: self._pos[pid]["sl"] = nsl
            if (pos["side"]=="long" and px >= pos["tp"]) or (pos["side"]=="short" and px <= pos["tp"]):
                self._close(pid, pos, px, "TP")
            elif (pos["side"]=="long" and px <= nsl) or (pos["side"]=="short" and px >= nsl):
                self._close(pid, pos, px, "SL/Trailing")

    def _close(self, pid, pos, px, reason):
        with self._lock:
            if pid not in self._pos: return
            active = self._pos.pop(pid)
        TR.rm(pid)
        self._cooldown[active["symbol"]] = time.time()
        EX.order(active["symbol"], "sell" if active["side"]=="long" else "buy", active["qty"], reduce_only=True)
        gross = (px - active["entry"]) * active["qty"] if active["side"]=="long" else (active["entry"] - px) * active["qty"]
        net = gross - (active["entry"]*active["qty"]*TAKER_FEE + px*active["qty"]*TAKER_FEE) if DRY_RUN else gross
        pct = (net / (active["entry"] * active["qty"])) * LEVERAGE * 100 if active["entry"]*active["qty"] else 0
        database.close(pid, px, net, pct, reason)
        TG.send(f"{'💸' if net>0 else '🩸'} CLOSE {active['symbol']} | {reason} | {net:+.2f}$")

# FLASK
app = Flask(__name__)

@app.route('/')
def home():
    return f"""
    <h1>Almasi v183 Pro</h1>
    <p>Balance: ${EX.balance():.2f}</p>
    <p>Status: {'DRY RUN' if DRY_RUN else 'LIVE'}</p>
    <p>Open Positions: {len(engine._pos) if 'engine' in globals() else 0}</p>
    """

if __name__ == "__main__":
    engine = Engine()
    threading.Thread(target=engine.loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)