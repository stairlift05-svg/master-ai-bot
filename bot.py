#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🤖 Almasi Traid v177 - PRO & DEBUG Edition
"""

import os
import sys
import uuid
import time
import logging
import threading
from datetime import datetime, timezone
from contextlib import contextmanager
from dataclasses import dataclass

# وارد کردن کتابخانه‌های حیاتی
import pandas as pd
import numpy as np
import requests
import ccxt
from flask import Flask, render_template_string

# ============================================================================
# LOGGING & CONFIG
# ============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", stream=sys.stdout)
for lib in ("ccxt", "urllib3", "openai"): logging.getLogger(lib).setLevel(logging.ERROR)
log = logging.getLogger("Almasi_v177")

class Cfg:
    @staticmethod
    def s(k: str, d: str = "") -> str: return os.getenv(k, d).strip()
    @staticmethod
    def f(k: str, d: float) -> float:
        try: return float(os.getenv(k, str(d)).strip())
        except: return d
    @staticmethod
    def b(k: str, d: bool = False) -> bool: return os.getenv(k, "true" if d else "false").strip().lower() in ("1", "true", "yes", "on")
    @staticmethod
    def lst(k: str, d: str = "") -> list: return [x.strip() for x in (os.getenv(k, d) or d).split(",") if x.strip()]

API_KEY    = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN   = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT    = Cfg.s("TELEGRAM_CHAT_ID")

SYMBOLS    = Cfg.lst("SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT,XRP/USDT:USDT")
RISK_PCT   = Cfg.f("RISK_PER_TRADE", 1.5)
MAX_POS    = int(Cfg.f("MAX_POSITIONS", 5))
DRY_RUN    = Cfg.b("DRY_RUN", False)
PORT       = int(Cfg.f("PORT", 10000))

# ============================================================================
# EXCHANGE INTERFACE (STRICT LIVE MODE + DEBUG)
# ============================================================================
class Exchange:
    def __init__(self):
        log.info("🔄 Connecting to Phemex API...")
        if not API_KEY or not API_SECRET:
            log.warning("⚠️ API Keys are missing in .env! (PHEMEX_API_KEY / PHEMEX_API_SECRET)")
            
        try:
            self.ex = ccxt.phemex({
                "apiKey": API_KEY, 
                "secret": API_SECRET, 
                "enableRateLimit": True,
                "options": {"defaultType": "swap"} # تنظیم حیاتی برای فیوچرز
            })
            self.ex.load_markets()
            log.info("✅ Phemex Markets Loaded Successfully.")
            
            # تست اولیه موجودی برای لاگ کردن ارور احتمالی
            self.bal()
            
        except ccxt.AuthenticationError:
            log.error("❌ Auth Error: کلیدهای API اشتباه است یا آی‌پی سرور Render در Phemex وایت‌لیست نشده است!")
        except Exception as e:
            log.error(f"❌ Connection Error: {e}")

    def ohlcv(self, sym, tf, lim=100) -> pd.DataFrame:
        try:
            raw = self.ex.fetch_ohlcv(sym, tf, limit=lim)
            df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
            return df
        except ccxt.BadSymbol:
            log.error(f"❌ Bad Symbol: {sym}. در Phemex فرمت باید به شکل BTC/USDT:USDT باشد.")
            return pd.DataFrame()
        except Exception as e:
            log.error(f"❌ OHLCV Error ({sym}): {e}")
            return pd.DataFrame()

    def price(self, sym):
        try: 
            return float(self.ex.fetch_ticker(sym)["last"])
        except Exception as e:
            log.error(f"❌ Price Error ({sym}): {e}")
            return 0.0
        
    def bal(self):
        if DRY_RUN: return 5000.0
        try:
            b = self.ex.fetch_balance({'type': 'swap'})
            usdt_bal = b.get("USDT", {}).get("free", 0)
            log.info(f"💰 Real USDT Balance: {usdt_bal}")
            if usdt_bal == 0:
                log.warning("⚠️ Connected, but USDT balance is 0. Check if funds are in 'Contract/Futures' wallet.")
            return float(usdt_bal)
        except ccxt.AuthenticationError:
            log.error("❌ Balance Error: API Permission denied. تیک Contract/Trade را در صرافی فعال کرده‌اید؟")
            return 0.0
        except Exception as e: 
            log.error(f"❌ Balance Fetch Error: {e}")
            return 0.0

EX = Exchange()

# ============================================================================
# INDICATORS (MATH LOGIC)
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close: pd.Series, n: int = 14):
        delta = close.diff()
        up = delta.clip(lower=0).ewm(com=n-1, adjust=False).mean()
        down = (-delta).clip(lower=0).ewm(com=n-1, adjust=False).mean().replace(0, 1e-10)
        return 100 - (100 / (1 + up / down))

    @staticmethod
    def ema(close: pd.Series, n: int): return close.ewm(span=n, adjust=False).mean()

    @staticmethod
    def atr(high, low, close, n=14):
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

    @staticmethod
    def adx(high, low, close, n=14):
        up, down = high.diff(), -low.diff()
        pos_dm = np.where((up > down) & (up > 0), up, 0.0)
        neg_dm = np.where((down > up) & (down > 0), down, 0.0)
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        atr = pd.Series(tr).ewm(alpha=1/n, adjust=False).mean()
        pdi = 100 * pd.Series(pos_dm).ewm(alpha=1/n, adjust=False).mean() / atr
        ndi = 100 * pd.Series(neg_dm).ewm(alpha=1/n, adjust=False).mean() / atr
        dx = 100 * (abs(pdi - ndi) / (pdi + ndi + 1e-10))
        return dx.ewm(alpha=1/n, adjust=False).mean()

    @staticmethod
    def bbands(close: pd.Series, n: int = 20, std: float = 2.0):
        mid = close.rolling(n).mean()
        sd = close.rolling(n).std()
        return mid - std*sd, mid, mid + std*sd

    @staticmethod
    def vwap(df: pd.DataFrame):
        q = df['vol'] * (df['high'] + df['low'] + df['close']) / 3
        return q.cumsum() / df['vol'].cumsum()

IND = Indicators()

# ============================================================================
# DATABASE 
# ============================================================================
class DB:
    def __init__(self):
        self._path = "almasi_v177.db"
        self._lock = threading.Lock()
        with self._cx() as c:
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("""CREATE TABLE IF NOT EXISTS trades (id TEXT PRIMARY KEY, symbol TEXT, side TEXT, entry_price REAL, exit_price REAL, quantity REAL, stop_loss REAL, take_profit REAL, status TEXT DEFAULT 'open', strategy TEXT, tp1_hit INTEGER DEFAULT 0, pnl REAL DEFAULT 0, opened_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS daily_stats (date TEXT PRIMARY KEY, total INTEGER DEFAULT 0, wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, pnl REAL DEFAULT 0)""")

    @contextmanager
    def _cx(self):
        import sqlite3
        with self._lock:
            c = sqlite3.connect(self._path, timeout=10)
            try: yield c; c.commit()
            except: c.rollback(); raise
            finally: c.close()

    def run(self, sql: str, p: tuple = ()):
        try:
            with self._cx() as c:
                cur = c.cursor(); cur.execute(sql, p)
                if sql.strip().upper().startswith("SELECT"): return cur.fetchall()
        except Exception as e: log.error("DB Err: %s", e)
        return []

    def open_trades(self):
        rows = self.run("SELECT id,symbol,side,entry_price,quantity,stop_loss,take_profit,strategy,tp1_hit FROM trades WHERE status='open'")
        return [{"id":r[0],"symbol":r[1],"side":r[2],"entry":r[3],"qty":r[4],"sl":r[5],"tp":r[6],"strat":r[7],"tp1_hit":bool(r[8])} for r in rows]

    def log_trade(self, pnl):
        today = datetime.now(timezone.utc).date().isoformat()
        r = self.run("SELECT total, wins, losses, pnl FROM daily_stats WHERE date=?", (today,))
        if r:
            t, w, l, p = r[0]
            self.run("UPDATE daily_stats SET total=?, wins=?, losses=?, pnl=? WHERE date=?", (t+1, w+(1 if pnl>0 else 0), l+(1 if pnl<=0 else 0), p+pnl, today))
        else:
            self.run("INSERT INTO daily_stats VALUES(?,1,?,?,?)", (today, 1 if pnl>0 else 0, 1 if pnl<=0 else 0, pnl))

db = DB()

# ============================================================================
# TELEGRAM ALERTS
# ============================================================================
class Telegram:
    def send(self, msg: str):
        if not TG_TOKEN or not TG_CHAT: return
        try: requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}, timeout=5)
        except: pass

TG = Telegram()

# ============================================================================
# MULTI-TIMEFRAME STRATEGY (THINK TANK)
# ============================================================================
@dataclass
class Signal:
    action: str = "neutral"
    strategy: str = ""
    px: float = 0.0
    sl: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0

class MultiTFStrategy:
    def analyze(self, sym: str) -> Signal:
        dfs = {tf: EX.ohlcv(sym, tf) for tf in ["15m", "5m", "3m", "1m"]}
        if any(df.empty for df in dfs.values()): return Signal()

        df15, df5, df3, df1 = dfs["15m"], dfs["5m"], dfs["3m"], dfs["1m"]
        adx15 = IND.adx(df15['high'], df15['low'], df15['close']).iloc[-1]
        ema200_15 = IND.ema(df15['close'], 200).iloc[-1]
        c15, atr15 = df15['close'].iloc[-1], IND.atr(df15['high'], df15['low'], df15['close']).iloc[-1]
        
        regime = "TREND" if adx15 > 25 else "RANGE"
        
        if regime == "TREND":
            trend_dir = "LONG" if c15 > ema200_15 else "SHORT"
            rsi5 = IND.rsi(df5['close'], 7).iloc[-1]
            ema9_1, c1 = IND.ema(df1['close'], 9).iloc[-1], df1['close'].iloc[-1]
            
            if trend_dir == "LONG" and rsi5 < 35 and c1 > ema9_1:
                return Signal("buy", "S1_Trend_Scalp", c1, c1 - (atr15*1.2), c1 + (atr15*1.2), c1 + (atr15*3))
            elif trend_dir == "SHORT" and rsi5 > 65 and c1 < ema9_1:
                return Signal("sell", "S1_Trend_Scalp", c1, c1 + (atr15*1.2), c1 - (atr15*1.2), c1 - (atr15*3))
                
        if regime == "RANGE":
            bbl, bbm, bbh = IND.bbands(df15['close'], 20, 2)
            c5, c1 = df5['close'].iloc[-1], df1['close'].iloc[-1]
            if df5['high'].iloc[-2] > bbh.iloc[-2] and c5 < bbh.iloc[-1] and c1 < bbm.iloc[-1]:
                return Signal("sell", "S2_Liq_Sweep", c1, c1 + (atr15*1), c1 - (atr15*1), bbl.iloc[-1])
            if df5['low'].iloc[-2] < bbl.iloc[-2] and c5 > bbl.iloc[-1] and c1 > bbm.iloc[-1]:
                return Signal("buy", "S2_Liq_Sweep", c1, c1 - (atr15*1), c1 + (atr15*1), bbh.iloc[-1])

        return Signal()

Brain = MultiTFStrategy()

# ============================================================================
# TRADING ENGINE
# ============================================================================
class Engine:
    def __init__(self):
        self.pos = {p['id']: p for p in db.open_trades()}
        self.bal = EX.bal()
        TG.send(f"💎 <b>Almasi Traid v177 STARTED</b>\nMode: {'DRY' if DRY_RUN else 'LIVE'}\nBal: ${self.bal:.2f}")

    def loop(self):
        while True:
            try:
                self.bal = EX.bal()
                if self.bal > 0:
                    self._manage_positions()
                    if len(self.pos) < MAX_POS: self._scan_market()
                time.sleep(30)
            except Exception as e: log.error("Engine Err: %s", e); time.sleep(10)

    def _scan_market(self):
        active_syms = [p['symbol'] for p in self.pos.values()]
        for sym in SYMBOLS:
            if sym in active_syms or len(self.pos) >= MAX_POS: continue
            sig = Brain.analyze(sym)
            if sig.action != "neutral":
                qty = (self.bal * (RISK_PCT/100)) / abs(sig.px - sig.sl)
                self._open_trade(sym, sig, qty)

    def _open_trade(self, sym, sig, qty):
        pid = f"A177_{uuid.uuid4().hex[:6]}"
        p = {"id": pid, "symbol": sym, "side": sig.action, "entry": sig.px, "qty": qty, "sl": sig.sl, "tp": sig.tp2, "strat": sig.strategy, "tp1_hit": False, "tp1_price": sig.tp1}
        self.pos[pid] = p
        db.run("INSERT INTO trades (id,symbol,side,entry_price,quantity,stop_loss,take_profit,strategy) VALUES (?,?,?,?,?,?,?,?)", (pid, sym, p['side'], p['entry'], qty, p['sl'], p['tp'], p['strat']))
        TG.send(f"🟢 <b>OPEN {p['side'].upper()}</b>\nSym: {sym}\nStrat: {p['strat']}\nEntry: {p['entry']:.4f}\nTP1: {p['tp1_price']:.4f}\nSL: {p['sl']:.4f}")

    def _manage_positions(self):
        for pid, p in list(self.pos.items()):
            px = EX.price(p['symbol'])
            if px == 0: continue
            is_long = p['side'] == 'long'
            
            if not p['tp1_hit']:
                if (is_long and px >= p.get('tp1_price', p['entry'])) or (not is_long and px <= p.get('tp1_price', p['entry'])):
                    p['qty'] /= 2
                    p['sl'] = p['entry']
                    p['tp1_hit'] = True
                    db.run("UPDATE trades SET quantity=?, stop_loss=?, tp1_hit=1 WHERE id=?", (p['qty'], p['sl'], pid))
                    TG.send(f"💸 <b>TP1 HIT!</b> {p['symbol']}\nSecured 50% profit. SL moved to Breakeven.")

            if (is_long and (px <= p['sl'] or px >= p['tp'])) or (not is_long and (px >= p['sl'] or px <= p['tp'])):
                pnl = (px - p['entry']) * p['qty'] if is_long else (p['entry'] - px) * p['qty']
                reason = "TP2" if pnl > 0 else "SL"
                db.run("UPDATE trades SET status='closed', exit_price=?, pnl=? WHERE id=?", (px, pnl, pid))
                db.log_trade(pnl)
                del self.pos[pid]
                TG.send(f"🔴 <b>CLOSED {reason}</b>\nSym: {p['symbol']}\nPnL: {pnl:+.2f}$")

ENG = Engine()

# ============================================================================
# FLASK DASHBOARD
# ============================================================================
app = Flask(__name__)
HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Almasi Traid v177</title>
    <style>
        :root { --bg: #0b0e14; --panel: #151a23; --text: #e2e8f0; --green: #00ff9d; --red: #ff3366; --blue: #00d2ff; }
        body { font-family: Tahoma, sans-serif; background: var(--bg); color: var(--text); padding: 20px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .card { background: var(--panel); padding: 20px; border-radius: 12px; border: 1px solid #2d3748; }
        .card h2 { color: #a0aec0; border-bottom: 1px solid #2d3748; padding-bottom: 10px; }
        .stat { display: flex; justify-content: space-between; margin: 12px 0; }
        .up { color: var(--green); } .down { color: var(--red); }
        table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #2d3748; }
        th { color: var(--blue); }
    </style>
</head>
<body>
    <h1 style="text-align:center; color: var(--blue);">💎 Almasi Traid v177</h1>
    <div class="grid">
        <div class="card">
            <h2>💰 Account</h2>
            <div class="stat"><span>Mode</span> <span>{{ 'Dry/Test' if dry else 'Live' }}</span></div>
            <div class="stat"><span>Balance</span> <span>${{ "%.2f"|format(bal) }}</span></div>
        </div>
        <div class="card">
            <h2>📊 Today</h2>
            <div class="stat"><span>PnL</span> <span class="{% if today.pnl>0 %}up{% elif today.pnl<0 %}down{% endif %}">{{ "%+.2f"|format(today.pnl) }}$</span></div>
            <div class="stat"><span>Wins/Losses</span> <span>{{ today.wins }} / {{ today.losses }}</span></div>
        </div>
    </div>
    <div class="card" style="margin-top: 20px;">
        <h2>📡 Positions</h2>
        <table>
            <tr><th>Symbol</th><th>Side</th><th>Strategy</th><th>Entry</th><th>PnL</th></tr>
            {% for p in positions %}
            <tr>
                <td>{{ p.symbol }}</td>
                <td class="{% if p.side == 'long' %}up{% else %}down{% endif %}">{{ p.side.upper() }}</td>
                <td>{{ p.strat }}</td>
                <td>{{ "%.4f"|format(p.entry) }}</td>
                <td class="{% if p.u_pnl>0 %}up{% else %}down{% endif %}">{{ "%+.2f"|format(p.u_pnl) }}$</td>
            </tr>
            {% else %}
            <tr><td colspan="5" style="text-align:center;">No active positions.</td></tr>
            {% endfor %}
        </table>
    </div>
    <script>setInterval(() => window.location.reload(), 15000);</script>
</body>
</html>
"""

@app.route('/')
def dashboard():
    today_dt = datetime.now(timezone.utc).date().isoformat()
    td = db.run("SELECT total, wins, losses, pnl FROM daily_stats WHERE date=?", (today_dt,))
    td_dict = {"total": td[0][0], "wins": td[0][1], "losses": td[0][2], "pnl": td[0][3]} if td else {"total":0, "wins":0, "losses":0, "pnl":0.0}
    active = []
    for p in ENG.pos.values():
        px = EX.price(p['symbol'])
        u_pnl = (px - p['entry']) * p['qty'] if p['side'] == 'long' else (p['entry'] - px) * p['qty']
        p_copy = p.copy(); p_copy['u_pnl'] = u_pnl
        active.append(p_copy)
    return render_template_string(HTML, dry=DRY_RUN, bal=ENG.bal, today=td_dict, positions=active)

if __name__ == "__main__":
    threading.Thread(target=ENG.loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
