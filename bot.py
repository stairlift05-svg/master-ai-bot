#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v8.1 (Pro Money Management)
- Fully Async (ccxt, aiosqlite, aiohttp)
- Anti-Repainting Strategies
- Advanced Position Sizing & Risk Management
- Live AJAX Web Dashboard (Basic Auth)
- Local Watchdog & Diagnostics
"""

import asyncio
import logging
import os
import time
import uuid
from threading import Thread
from typing import Dict, List

import aiohttp
import aiosqlite
import ccxt.async_support as ccxt
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string
from flask_httpauth import HTTPBasicAuth

# ============================================================================
# 1. CONFIGURATION
# ============================================================================
load_dotenv()

API_KEY = os.getenv("PHEMEX_API_KEY", "")
API_SECRET = os.getenv("PHEMEX_API_SECRET", "")
TESTNET = os.getenv("PHEMEX_TESTNET", "True").lower() in ("true", "1", "yes")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

WEB_USER = os.getenv("WEB_ADMIN_USER", "admin")
WEB_PASS = os.getenv("WEB_ADMIN_PASS", "admin123")

SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT"]
TIMEFRAME = "5m"
RISK_PCT = 1.0    # Percentage of total balance to risk per trade
LEVERAGE = 10     # Default Leverage

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("QuantV8")

# Shared State for Web & Diagnostics
SHARED_STATE = {
    "is_active": True,
    "balance": 0.0,
    "active_positions": {},
    "last_scan": "Never",
    "diagnostics": {"health_score": 100, "issues": []},
    "stats": {"total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0}
}

# ============================================================================
# 2. ASYNC DATABASE
# ============================================================================
class AsyncDB:
    def __init__(self, db_path="bot_v8.db"):
        self.db_path = db_path

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY, symbol TEXT, side TEXT, strategy TEXT,
                    entry_price REAL, qty REAL, sl REAL, tp REAL,
                    status TEXT DEFAULT 'open', pnl REAL DEFAULT 0,
                    opened_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT
                )
            """)
            await db.commit()

    async def insert_trade(self, t: Dict):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO trades (id, symbol, side, strategy, entry_price, qty, sl, tp) VALUES (?,?,?,?,?,?,?,?)",
                (t['id'], t['symbol'], t['side'], t['strategy'], t['entry'], t['qty'], t['sl'], t['tp'])
            )
            await db.commit()

    async def close_trade(self, t_id: str, pnl: float):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE trades SET status='closed', pnl=?, closed_at=CURRENT_TIMESTAMP WHERE id=?",
                (pnl, t_id)
            )
            await db.commit()

    async def update_analytics(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT pnl FROM trades WHERE status='closed'") as cursor:
                rows = await cursor.fetchall()
                if not rows: return
                pnls = [r[0] for r in rows]
                wins = len([p for p in pnls if p > 0])
                total = len(pnls)
                SHARED_STATE["stats"] = {
                    "total_trades": total,
                    "win_rate": round((wins / total) * 100, 1) if total > 0 else 0.0,
                    "total_pnl": round(sum(pnls), 2)
                }

    async def get_open_trades(self) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM trades WHERE status='open'") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

# ============================================================================
# 3. ANTI-REPAINTING STRATEGIES
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close: pd.Series, n=14):
        delta = close.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=n-1, adjust=False).mean()
        ema_down = down.ewm(com=n-1, adjust=False).mean()
        rs = ema_up / ema_down
        return 100 - (100 / (1 + rs))

    @staticmethod
    def macd(close: pd.Series):
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        macd_line = exp1 - exp2
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        return macd_line, signal_line

    @staticmethod
    def bollinger(close: pd.Series, n=20, std=2):
        ma = close.rolling(n).mean()
        band = close.rolling(n).std() * std
        return ma + band, ma - band

    @staticmethod
    def atr(df: pd.DataFrame, n=14):
        tr = pd.concat([df['high'] - df['low'], (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

class StrategyEngine:
    def get_closed(self, df: pd.DataFrame):
        return df.iloc[:-1].copy()

    def analyze(self, df: pd.DataFrame) -> Dict:
        df_c = self.get_closed(df)
        if len(df_c) < 35: return {"action": "neutral"}
        
        c = df_c['close']
        atr_val = Indicators.atr(df_c).iloc[-1]
        price = c.iloc[-1]
        
        signals = []
        
        rsi = Indicators.rsi(c)
        if rsi.iloc[-1] < 30 and rsi.iloc[-2] >= 30:
            signals.append({"strat": "RSI_Oversold", "side": "buy", "conf": 75})
        elif rsi.iloc[-1] > 70 and rsi.iloc[-2] <= 70:
            signals.append({"strat": "RSI_Overbought", "side": "sell", "conf": 75})

        macd, sig = Indicators.macd(c)
        if macd.iloc[-1] > sig.iloc[-1] and macd.iloc[-2] <= sig.iloc[-2]:
            signals.append({"strat": "MACD_CrossUp", "side": "buy", "conf": 80})
        elif macd.iloc[-1] < sig.iloc[-1] and macd.iloc[-2] >= sig.iloc[-2]:
            signals.append({"strat": "MACD_CrossDown", "side": "sell", "conf": 80})

        up, down = Indicators.bollinger(c)
        if price > up.iloc[-1]:
            signals.append({"strat": "BB_BreakUp", "side": "buy", "conf": 70})
        elif price < down.iloc[-1]:
            signals.append({"strat": "BB_BreakDown", "side": "sell", "conf": 70})

        if not signals: return {"action": "neutral"}

        best = sorted(signals, key=lambda x: x['conf'], reverse=True)[0]
        
        return {
            "action": best['side'],
            "strategy": best['strat'],
            "confidence": best['conf'],
            "sl": price - (atr_val * 1.5) if best['side'] == 'buy' else price + (atr_val * 1.5),
            "tp": price + (atr_val * 3.0) if best['side'] == 'buy' else price - (atr_val * 3.0),
            "atr": atr_val
        }

# ============================================================================
# 4. INTERACTIVE ASYNC TELEGRAM
# ============================================================================
class AsyncTelegram:
    def __init__(self, engine):
        self.engine = engine
        self.base_url = f"https://api.telegram.org/bot{TG_TOKEN}"
        self.last_update_id = 0

    def get_keyboard(self):
        return {
            "keyboard": [
                [{"text": "📊 Status"}, {"text": "📈 Positions"}],
                [{"text": "▶️ Start"}, {"text": "⏹ Stop"}]
            ],
            "resize_keyboard": True
        }

    async def send_message(self, text: str):
        if not TG_TOKEN or not TG_CHAT: return
        payload = {"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML", "reply_markup": self.get_keyboard()}
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(f"{self.base_url}/sendMessage", json=payload)
        except Exception: pass

    async def poll_updates(self):
        if not TG_TOKEN: return
        while True:
            try:
                url = f"{self.base_url}/getUpdates?offset={self.last_update_id + 1}&timeout=10"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        data = await response.json()
                        if data.get("ok"):
                            for upd in data.get("result", []):
                                self.last_update_id = upd["update_id"]
                                await self.handle_command(upd.get("message", {}).get("text", ""))
            except Exception: pass
            await asyncio.sleep(1)

    async def handle_command(self, cmd: str):
        if cmd in ("▶️ Start", "/start"):
            SHARED_STATE["is_active"] = True
            await self.send_message("▶️ <b>Bot Activated</b>")
        elif cmd in ("⏹ Stop", "/stop"):
            SHARED_STATE["is_active"] = False
            await self.send_message("⏹ <b>Bot Paused</b>")
        elif cmd in ("📊 Status", "/status"):
            bal = SHARED_STATE["balance"]
            h_score = SHARED_STATE["diagnostics"]["health_score"]
            act = "Running" if SHARED_STATE["is_active"] else "Paused"
            await self.send_message(f"📊 <b>System Status</b>\nMode: {act}\nBalance: ${bal:.2f}\nHealth: {h_score}/100")
        elif cmd in ("📈 Positions", "/pos"):
            pos = SHARED_STATE["active_positions"]
            if not pos:
                await self.send_message("📭 No active positions.")
                return
            msg = "🏦 <b>Active Positions</b>\n"
            for p in pos.values():
                msg += f"• {p['symbol']} | {p['side'].upper()} | En: {p['entry']:.4f}\n"
            await self.send_message(msg)

# ============================================================================
# 5. DIAGNOSTICS AGENT
# ============================================================================
class DiagnosticsAgent:
    def __init__(self, ex):
        self.ex = ex

    async def monitor_loop(self):
        while True:
            score = 100
            issues = []
            
            if not API_KEY:
                score -= 50; issues.append("API Keys missing")
            if SHARED_STATE["balance"] < 10:
                score -= 20; issues.append("Low Balance (< $10)")
                
            SHARED_STATE["diagnostics"] = {"health_score": max(0, score), "issues": issues}
            await asyncio.sleep(60)

# ============================================================================
# 6. CORE QUANT ENGINE (With Money Management)
# ============================================================================
class QuantEngine:
    def __init__(self):
        self.db = AsyncDB()
        self.strategy = StrategyEngine()
        self.tg = AsyncTelegram(self)
        
        self.ex = ccxt.phemex({
            'apiKey': API_KEY, 'secret': API_SECRET,
            'enableRateLimit': True, 'options': {'defaultType': 'swap'}
        })
        self.ex.set_sandbox_mode(TESTNET)
        self.diag = DiagnosticsAgent(self.ex)
        self.latest_prices = {}

    async def start(self):
        await self.db.init_db()
        await self.db.update_analytics()
        
        # 🔴 V8.1: Load markets & Set Leverage at startup
        try:
            await self.ex.load_markets()
            for sym in SYMBOLS:
                try:
                    await self.ex.set_leverage(LEVERAGE, sym)
                except Exception: pass # Ignore if already set
        except Exception as e:
            log.warning(f"Market init warning: {e}")

        open_trades = await self.db.get_open_trades()
        for t in open_trades:
            SHARED_STATE["active_positions"][t['id']] = t
            
        await self.tg.send_message("🚀 <b>Quant V8.1 Online (Pro Money Management)</b>")
        
        await asyncio.gather(
            self.update_prices_loop(),
            self.market_scanner_loop(),
            self.local_watchdog_loop(),
            self.tg.poll_updates(),
            self.diag.monitor_loop()
        )

    async def update_prices_loop(self):
        while True:
            try:
                tickers = await self.ex.fetch_tickers(SYMBOLS)
                for sym, data in tickers.items():
                    if data.get('last'): self.latest_prices[sym] = data['last']
                    
                bal = await self.ex.fetch_balance()
                SHARED_STATE["balance"] = bal.get('USDT', {}).get('total', 0.0)
            except Exception: pass
            await asyncio.sleep(2)

    async def market_scanner_loop(self):
        while True:
            if not SHARED_STATE["is_active"]:
                await asyncio.sleep(5); continue
                
            SHARED_STATE["last_scan"] = time.strftime("%H:%M:%S")
            for sym in SYMBOLS:
                if any(p['symbol'] == sym for p in SHARED_STATE["active_positions"].values()):
                    continue
                
                try:
                    raw = await self.ex.fetch_ohlcv(sym, TIMEFRAME, limit=100)
                    if not raw: continue
                    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
                    
                    sig = self.strategy.analyze(df)
                    if sig['action'] != 'neutral':
                        await self.execute_trade(sym, sig)
                except Exception as e: log.error(f"Scan err {sym}: {e}")
                
                await asyncio.sleep(1)
            await asyncio.sleep(45)

    async def execute_trade(self, sym: str, sig: Dict):
        price = self.latest_prices.get(sym)
        if not price: return
        bal = SHARED_STATE["balance"]
        
        # 🔴 V8.1: Safe Money Management
        if bal < 10.0:
            log.info(f"Skipping trade {sym}: Insufficient balance (${bal:.2f})")
            return
            
        sl_distance = abs(price - sig['sl'])
        if sl_distance == 0: return

        # Calculate exact quantity based on Risk %
        risk_amount = bal * (RISK_PCT / 100)
        raw_qty = risk_amount / sl_distance
        
        # Format quantity to match exchange precision rules
        try:
            qty_str = self.ex.amount_to_precision(sym, raw_qty)
            qty = float(qty_str)
        except Exception:
            qty = round(raw_qty, 3) # Fallback

        if qty <= 0:
            log.warning(f"Calculated qty for {sym} is 0. Check risk settings.")
            return

        side = sig['action']
        
        try:
            order = await self.ex.create_market_order(sym, side, qty)
            fill_price = order.get('average') or price
            pid = f"pos_{uuid.uuid4().hex[:8]}"
            
            pos_data = {
                "id": pid, "symbol": sym, "side": side, "strategy": sig['strategy'],
                "entry": fill_price, "qty": qty, "sl": sig['sl'], "tp": sig['tp']
            }
            
            SHARED_STATE["active_positions"][pid] = pos_data
            await self.db.insert_trade(pos_data)
            
            sl_side = 'sell' if side == 'buy' else 'buy'
            await self.ex.create_order(sym, 'stop', sl_side, qty, sig['sl'], params={'stopPrice': sig['sl'], 'reduceOnly': True})
            
            await self.tg.send_message(f"✅ <b>Trade Opened</b>\n{sym} | {side.upper()}\nStrat: {sig['strategy']}\nQty: {qty}\nEn: {fill_price:.4f}")
        except Exception as e: log.error(f"Exec err {sym}: {e}")

    async def local_watchdog_loop(self):
        while True:
            for pid, pos in list(SHARED_STATE["active_positions"].items()):
                price = self.latest_prices.get(pos['symbol'])
                if not price: continue

                sl_hit = (pos['side'] == 'buy' and price <= pos['sl']) or (pos['side'] == 'sell' and price >= pos['sl'])
                tp_hit = (pos['side'] == 'buy' and price >= pos['tp']) or (pos['side'] == 'sell' and price <= pos['tp'])

                if sl_hit or tp_hit:
                    reason = "SL" if sl_hit else "TP"
                    close_side = 'sell' if pos['side'] == 'buy' else 'buy'
                    try:
                        await self.ex.create_order(pos['symbol'], 'market', close_side, pos['qty'], params={'reduceOnly': True})
                        pnl = (price - pos['entry']) * pos['qty'] * (1 if pos['side'] == 'buy' else -1)
                        
                        await self.db.close_trade(pid, pnl)
                        del SHARED_STATE["active_positions"][pid]
                        await self.db.update_analytics()
                        
                        await self.tg.send_message(f"{'🟢' if pnl>0 else '🔴'} <b>Trade Closed ({reason})</b>\n{pos['symbol']}\nPnL: ${pnl:.2f}")
                    except Exception as e: log.error(f"Close err {pid}: {e}")
            await asyncio.sleep(1)

# ============================================================================
# 7. LIVE AJAX WEB DASHBOARD
# ============================================================================
app = Flask(__name__)
auth = HTTPBasicAuth()

@auth.verify_password
def verify(u, p): return u == WEB_USER and p == WEB_PASS

@app.route("/api/status")
@auth.login_required
def api_status():
    return jsonify(SHARED_STATE)

@app.route("/")
@auth.login_required
def dashboard():
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Quant V8 Pro Dashboard</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0d1117; color: #c9d1d9; padding: 20px; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
            .card { background: #161b22; border: 1px solid #30363d; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
            h2 { color: #58a6ff; margin-top: 0; border-bottom: 1px solid #30363d; padding-bottom: 10px; }
            .val { font-size: 1.5em; font-weight: bold; color: #3fb950; }
            .pos-item { background: #21262d; padding: 10px; margin-top: 10px; border-radius: 6px; border-left: 4px solid #58a6ff; }
            .badge { background: #238636; padding: 4px 8px; border-radius: 12px; font-size: 0.8em; }
        </style>
        <script>
            async function fetchData() {
                try {
                    let res = await fetch('/api/status');
                    let data = await res.json();
                    
                    document.getElementById('bal').innerText = '$' + data.balance.toFixed(2);
                    document.getElementById('scan').innerText = data.last_scan;
                    document.getElementById('state').innerText = data.is_active ? "Running" : "Paused";
                    document.getElementById('state').style.color = data.is_active ? "#3fb950" : "#f85149";
                    document.getElementById('health').innerText = data.diagnostics.health_score + "/100";
                    document.getElementById('trades').innerText = data.stats.total_trades;
                    document.getElementById('winrate').innerText = data.stats.win_rate + "%";
                    document.getElementById('pnl').innerText = '$' + data.stats.total_pnl;
                    
                    let posDiv = document.getElementById('positions');
                    posDiv.innerHTML = '';
                    let count = 0;
                    for (let id in data.active_positions) {
                        count++;
                        let p = data.active_positions[id];
                        posDiv.innerHTML += `<div class='pos-item'>
                            <b>${p.symbol}</b> <span class='badge'>${p.side.toUpperCase()}</span><br>
                            Strategy: ${p.strategy} | Entry: ${p.entry.toFixed(4)} | Qty: ${p.qty}
                        </div>`;
                    }
                    if (count === 0) posDiv.innerHTML = "<p style='color:#8b949e'>No active positions.</p>";
                    
                } catch (e) { console.error("Update failed", e); }
            }
            setInterval(fetchData, 2000);
            window.onload = fetchData;
        </script>
    </head>
    <body>
        <h1 style="text-align:center; color:#fff;">🤖 Master Quant V8 Engine</h1>
        <div class="grid">
            <div class="card">
                <h2>System Core</h2>
                <p>Status: <span id="state" class="val">Loading...</span></p>
                <p>Wallet: <span id="bal" class="val">...</span></p>
                <p>Last Scan: <span id="scan" style="color:#58a6ff">...</span></p>
            </div>
            <div class="card">
                <h2>Analytics</h2>
                <p>Health Score: <span id="health" class="val">...</span></p>
                <p>Total Trades: <span id="trades" class="val">...</span></p>
                <p>Win Rate: <span id="winrate" class="val">...</span></p>
                <p>Total PnL: <span id="pnl" class="val">...</span></p>
            </div>
            <div class="card" style="grid-column: 1 / -1;">
                <h2>Live Positions</h2>
                <div id="positions">Loading positions...</div>
            </div>
        </div>
    </body>
    </html>
    """
    return render_template_string(html_template)

def run_web():
    log.info(f"🌐 Web UI: http://0.0.0.0:10000 (User: {WEB_USER})")
    app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False)

# ============================================================================
# 8. LAUNCHER
# ============================================================================
if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    
    engine = QuantEngine()
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        log.info("🛑 Shutting down V8 gracefully...")
    finally:
        asyncio.run(engine.ex.close())
