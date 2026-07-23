#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v9.1 (Pro Management Update)
- Drawdown Halt only stops NEW trades (Watchdog keeps managing).
- Adopts Orphan/Manual/Restarted trades automatically.
- Telegram 1-Click Close functionality added.
- Micro-Trading, Trailing SL, Partial TP enabled.
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
RISK_PCT = 1.0
LEVERAGE = 5
MAX_POS = 4
MAX_DD = 10.0  

TRAIL_ACT = 1.5    
TRAIL_STEP = 0.5   
PARTIAL_TP = True  

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("QuantV9.1")

SHARED_STATE = {
    "is_active": True,
    "dd_halted": False,
    "balance": 0.0,
    "peak_balance": 0.0,
    "current_dd": 0.0,
    "active_positions": {},
    "last_scan": "Never",
    "diagnostics": {"score": 100, "issues": [], "market_regime": "Neutral"},
    "stats": {"total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0},
    "debug_signals": {}
}

# ============================================================================
# 2. ASYNC DATABASE
# ============================================================================
class AsyncDB:
    def __init__(self, db_path="bot_v9.db"):
        self.db_path = db_path

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY, symbol TEXT, side TEXT, strategy TEXT,
                    entry_price REAL, qty REAL, original_qty REAL, 
                    sl REAL, tp1 REAL, tp REAL,
                    is_partial INTEGER DEFAULT 0, highest_pnl_pct REAL DEFAULT 0,
                    status TEXT DEFAULT 'open', pnl REAL DEFAULT 0,
                    opened_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT
                )
            """)
            await db.commit()

    async def insert_trade(self, t: Dict):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO trades (id, symbol, side, strategy, entry_price, qty, original_qty, sl, tp1, tp) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (t['id'], t['symbol'], t['side'], t['strategy'], t['entry'], t['qty'], t['qty'], t['sl'], t['tp1'], t['tp'])
            )
            await db.commit()

    async def update_trade(self, t_id: str, qty: float, sl: float, is_partial: int, highest_pnl: float):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE trades SET qty=?, sl=?, is_partial=?, highest_pnl_pct=? WHERE id=?",
                (qty, sl, is_partial, highest_pnl, t_id)
            )
            await db.commit()

    async def close_trade(self, t_id: str, pnl: float):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE trades SET status='closed', pnl=?, closed_at=CURRENT_TIMESTAMP WHERE id=?",
                (pnl, t_id)
            )
            await db.commit()

    async def get_open_trades(self) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM trades WHERE status='open'") as cursor:
                return [dict(row) for row in await cursor.fetchall()]

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

# ============================================================================
# 3. ANTI-REPAINTING STRATEGIES
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close: pd.Series, n=14):
        delta = close.diff(); up = delta.clip(lower=0); down = -1 * delta.clip(upper=0)
        rs = up.ewm(com=n-1, adjust=False).mean() / down.ewm(com=n-1, adjust=False).mean()
        return 100 - (100 / (1 + rs))

    @staticmethod
    def atr(df: pd.DataFrame, n=14):
        tr = pd.concat([df['high'] - df['low'], (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

class StrategyEngine:
    def analyze(self, df: pd.DataFrame) -> Dict:
        df_c = df.iloc[:-1].copy() 
        if len(df_c) < 30: return {"action": "neutral"}
        
        c = df_c['close']
        atr = Indicators.atr(df_c).iloc[-1]
        price = c.iloc[-1]
        rsi = Indicators.rsi(c)
        
        ema20 = c.ewm(span=20).mean()
        ema50 = c.ewm(span=50).mean()
        
        sig = {"action": "neutral"}
        
        if price > ema20.iloc[-1] > ema50.iloc[-1] and rsi.iloc[-1] < 45:
            sig = {"action": "buy", "strat": "Pullback_Long", "conf": 80}
        elif price < ema20.iloc[-1] < ema50.iloc[-1] and rsi.iloc[-1] > 55:
            sig = {"action": "sell", "strat": "Pullback_Short", "conf": 80}

        if sig['action'] != 'neutral':
            side = sig['action']
            sig['sl'] = price - (atr * 1.5) if side == 'buy' else price + (atr * 1.5)
            sig['tp'] = price + (atr * 3.0) if side == 'buy' else price - (atr * 3.0)
            sig['tp1'] = price + (atr * 1.5) if side == 'buy' else price - (atr * 1.5) 
            sig['atr'] = atr
            
        return sig

# ============================================================================
# 4. ASYNC TELEGRAM (With 1-Click Close Feature)
# ============================================================================
class AsyncTelegram:
    def __init__(self, engine):
        self.engine = engine
        self.base_url = f"https://api.telegram.org/bot{TG_TOKEN}"
        self.offset = 0

    def kb(self):
        return {"keyboard": [[{"text": "📊 Dash"}, {"text": "📈 Pos"}], [{"text": "▶️ Start"}, {"text": "⏹ Stop"}]], "resize_keyboard": True}

    async def send(self, msg: str):
        if not TG_TOKEN: return
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"{self.base_url}/sendMessage", json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML", "reply_markup": self.kb()})
        except: pass

    async def poll(self):
        if not TG_TOKEN: return
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"{self.base_url}/getUpdates?offset={self.offset+1}&timeout=10") as r:
                        data = await r.json()
                        for upd in data.get("result", []):
                            self.offset = upd["update_id"]
                            txt = upd.get("message", {}).get("text", "")
                            
                            # Standard Commands
                            if txt in ("▶️ Start", "/start"):
                                SHARED_STATE["is_active"] = True; await self.send("▶️ <b>Bot Started</b>")
                            elif txt in ("⏹ Stop", "/stop"):
                                SHARED_STATE["is_active"] = False; await self.send("⏹ <b>Bot Stopped</b>")
                            elif txt in ("📊 Dash", "/status"):
                                b = SHARED_STATE["balance"]; dd = SHARED_STATE["current_dd"]
                                halt = "⚠️ DD Halted!" if SHARED_STATE["dd_halted"] else "✅ OK"
                                await self.send(f"📊 <b>Status</b>\nBal: ${b:.2f}\nDD: {dd:.1f}% ({halt})\nPos: {len(SHARED_STATE['active_positions'])}/{MAX_POS}")
                            
                            # 🔴 NEW: Positions with 1-Click Close Commands
                            elif txt in ("📈 Pos", "/pos"):
                                if not SHARED_STATE["active_positions"]:
                                    await self.send("📭 No positions")
                                    continue
                                msg = "🏦 <b>Active Positions</b>\n<i>Click the ❌ command to close.</i>\n\n"
                                for p in SHARED_STATE["active_positions"].values():
                                    base = p['symbol'].split('/')[0]
                                    msg += f"• {p['symbol']} {p['side'].upper()}\n  En: {p['entry']:.4f} 👉 ❌ /close_{base}\n\n"
                                await self.send(msg)
                            
                            # 🔴 NEW: Intercept Close Commands (e.g., /close_BTC)
                            elif txt.startswith("/close_"):
                                base_coin = txt.split("_")[1].upper()
                                target_sym = f"{base_coin}/USDT"
                                pid_to_close = None
                                for pid, p in SHARED_STATE["active_positions"].items():
                                    if p['symbol'] == target_sym: pid_to_close = pid; break
                                
                                if pid_to_close:
                                    await self.send(f"⏳ Closing {target_sym}...")
                                    await self.engine.force_close_position(pid_to_close, "Closed via Telegram")
                                else:
                                    await self.send(f"⚠️ {target_sym} is not active.")

            except Exception as e: log.debug(f"TG error: {e}")
            await asyncio.sleep(1)

# ============================================================================
# 5. CORE QUANT ENGINE (Micro-Trade, Smart Sync, DD Handling)
# ============================================================================
class QuantEngine:
    def __init__(self):
        self.db = AsyncDB()
        self.strategy = StrategyEngine()
        self.tg = AsyncTelegram(self)
        
        self.ex = ccxt.phemex({'apiKey': API_KEY, 'secret': API_SECRET, 'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
        self.ex.set_sandbox_mode(TESTNET)
        self.prices = {}
        self.markets_cache = {}
        self.loop_count = 0

    async def start(self):
        await self.db.init_db()
        await self.db.update_analytics()
        
        try:
            self.markets_cache = await self.ex.load_markets()
            for sym in SYMBOLS:
                try: await self.ex.set_leverage(LEVERAGE, sym)
                except: pass
        except: pass

        for t in await self.db.get_open_trades():
            SHARED_STATE["active_positions"][t['id']] = t
            
        await self.tg.send("🚀 <b>Master Quant V9.1 (Pro Management) Started</b>")
        
        await asyncio.gather(
            self.price_loop(),
            self.scan_loop(),
            self.watchdog_loop(),
            self.tg.poll()
        )

    async def price_loop(self):
        while True:
            try:
                tickers = await self.ex.fetch_tickers(SYMBOLS)
                for s, d in tickers.items():
                    if d.get('last'): self.prices[s] = d['last']
                    
                bal = await self.ex.fetch_balance()
                current_bal = bal.get('USDT', {}).get('total', 0.0)
                SHARED_STATE["balance"] = current_bal
                
                if current_bal > SHARED_STATE["peak_balance"]:
                    SHARED_STATE["peak_balance"] = current_bal
                
                if SHARED_STATE["peak_balance"] > 0:
                    dd = ((SHARED_STATE["peak_balance"] - current_bal) / SHARED_STATE["peak_balance"]) * 100
                    SHARED_STATE["current_dd"] = dd
                    
                    if dd >= MAX_DD and not SHARED_STATE["dd_halted"]:
                        SHARED_STATE["dd_halted"] = True
                        await self.tg.send(f"🛑 <b>NEW TRADES HALTED</b>\nDrawdown: {dd:.1f}%.\n<i>Watchdog is still managing open trades.</i>")
                    elif dd < MAX_DD * 0.8 and SHARED_STATE["dd_halted"]:
                        SHARED_STATE["dd_halted"] = False
                        await self.tg.send("✅ <b>TRADING RESUMED</b>\nDrawdown improved.")
            except: pass
            await asyncio.sleep(2)

    async def scan_loop(self):
        """ 🔴 Note: scan_loop pauses on Drawdown, but watchdog_loop continues! """
        while True:
            self.loop_count += 1
            
            # Manual Sync Every 20 loops
            if self.loop_count % 20 == 0: await self.smart_sync_positions()

            if not SHARED_STATE["is_active"] or SHARED_STATE["dd_halted"]:
                await asyncio.sleep(5); continue
                
            if len(SHARED_STATE["active_positions"]) >= MAX_POS:
                await asyncio.sleep(5); continue

            SHARED_STATE["last_scan"] = time.strftime("%H:%M:%S")

            for sym in SYMBOLS:
                if any(p['symbol'] == sym for p in SHARED_STATE["active_positions"].values()): continue
                
                try:
                    raw = await self.ex.fetch_ohlcv(sym, TIMEFRAME, limit=100)
                    if not raw: continue
                    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
                    
                    sig = self.strategy.analyze(df)
                    if sig['action'] != 'neutral':
                        await self.execute_trade(sym, sig)
                except: pass
                await asyncio.sleep(1)
            await asyncio.sleep(30)

    async def execute_trade(self, sym: str, sig: Dict):
        price = self.prices.get(sym)
        if not price: return
        bal = SHARED_STATE["balance"]
        
        if bal < 5.0: return 

        risk_amount = bal * (RISK_PCT / 100)
        dist = abs(price - sig['sl'])
        if dist == 0: return
        
        raw_qty = risk_amount / dist
        
        market = self.markets_cache.get(sym, {})
        min_qty = market.get('limits', {}).get('amount', {}).get('min', 0.001)
        qty = max(raw_qty, min_qty) 
        
        try: qty = float(self.ex.amount_to_precision(sym, qty))
        except: qty = round(qty, 3)

        side = sig['action']
        try:
            order = await self.ex.create_market_order(sym, side, qty)
            fill_price = order.get('average') or price
            pid = f"pos_{uuid.uuid4().hex[:8]}"
            
            pos = {
                "id": pid, "symbol": sym, "side": side, "strategy": sig['strat'],
                "entry": fill_price, "qty": qty, "sl": sig['sl'], "tp": sig['tp'],
                "tp1": sig['tp1'], "is_partial": 0, "highest_pnl_pct": 0
            }
            
            SHARED_STATE["active_positions"][pid] = pos
            await self.db.insert_trade(pos)
            await self.tg.send(f"✅ <b>Entry {side.upper()}</b>\n{sym} @ {fill_price:.4f}\nQty: {qty}")
            
            sl_side = 'sell' if side == 'buy' else 'buy'
            await self.ex.create_order(sym, 'stop', sl_side, qty, sig['sl'], params={'stopPrice': sig['sl'], 'reduceOnly': True})
        except Exception as e: log.error(f"Exec {sym}: {e}")

    async def watchdog_loop(self):
        """ 🔴 Note: Watchdog never sleeps, even if Drawdown is halted! """
        while True:
            for pid, pos in list(SHARED_STATE["active_positions"].items()):
                price = self.prices.get(pos['symbol'])
                if not price: continue

                pnl_pct = ((price - pos['entry']) / pos['entry'] * 100) if pos['side'] == 'buy' else ((pos['entry'] - price) / pos['entry'] * 100)
                
                # Trailing Stop
                if pnl_pct > TRAIL_ACT:
                    if pnl_pct > pos['highest_pnl_pct']:
                        pos['highest_pnl_pct'] = pnl_pct
                        new_sl = price - (price * (TRAIL_STEP/100)) if pos['side'] == 'buy' else price + (price * (TRAIL_STEP/100))
                        
                        if (pos['side'] == 'buy' and new_sl > pos['sl']) or (pos['side'] == 'sell' and new_sl < pos['sl']):
                            pos['sl'] = new_sl
                            await self.db.update_trade(pid, pos['qty'], pos['sl'], pos['is_partial'], pos['highest_pnl_pct'])
                            log.info(f"📐 Trailed SL for {pos['symbol']} -> {new_sl:.4f}")

                # Partial TP
                if PARTIAL_TP and pos['is_partial'] == 0:
                    hit_tp1 = (pos['side'] == 'buy' and price >= pos['tp1']) or (pos['side'] == 'sell' and price <= pos['tp1'])
                    if hit_tp1:
                        close_side = 'sell' if pos['side'] == 'buy' else 'buy'
                        half_qty = pos['qty'] / 2
                        try:
                            half_qty = float(self.ex.amount_to_precision(pos['symbol'], half_qty))
                            if half_qty > 0:
                                await self.ex.create_market_order(pos['symbol'], close_side, half_qty, params={'reduceOnly': True})
                                pos['qty'] -= half_qty
                                pos['is_partial'] = 1
                                pos['sl'] = pos['entry']
                                await self.db.update_trade(pid, pos['qty'], pos['sl'], 1, pos['highest_pnl_pct'])
                                await self.tg.send(f"✂️ <b>Partial TP Hit</b>\n{pos['symbol']} 50% Closed. SL -> BE.")
                        except: pass

                # Hard SL/TP
                sl_hit = (pos['side'] == 'buy' and price <= pos['sl']) or (pos['side'] == 'sell' and price >= pos['sl'])
                tp_hit = (pos['side'] == 'buy' and price >= pos['tp']) or (pos['side'] == 'sell' and price <= pos['tp'])

                if sl_hit or tp_hit:
                    await self.force_close_position(pid, "SL/Trailing" if sl_hit else "TP")
            await asyncio.sleep(1)

    # 🔴 NEW: Master Close Function (Called by Watchdog & Telegram)
    async def force_close_position(self, pid: str, reason: str):
        pos = SHARED_STATE["active_positions"].get(pid)
        if not pos: return
        
        price = self.prices.get(pos['symbol'], pos['entry'])
        close_side = 'sell' if pos['side'] == 'buy' else 'buy'
        try:
            await self.ex.create_market_order(pos['symbol'], close_side, pos['qty'], params={'reduceOnly': True})
            pnl = (price - pos['entry']) * pos['qty'] * (1 if pos['side'] == 'buy' else -1)
            await self.db.close_trade(pid, pnl)
            del SHARED_STATE["active_positions"][pid]
            await self.db.update_analytics()
            icon = "🟢" if pnl > 0 else "🔴"
            if "Telegram" in reason: icon = "👋"
            await self.tg.send(f"{icon} <b>Closed ({reason})</b>\n{pos['symbol']} | PnL: ${pnl:.2f}")
        except Exception as e: log.error(f"Force Close {pid}: {e}")

    # 🔴 NEW: Smart Sync (Adopts orphaned and manual trades)
    async def smart_sync_positions(self):
        try:
            remote = await self.ex.fetch_positions(SYMBOLS)
            active_remote_syms = []
            
            for p in remote:
                contracts = float(p.get('contracts', 0) or 0)
                if contracts > 0:
                    sym = p['symbol']
                    active_remote_syms.append(sym)
                    
                    # If exchange has it, but bot doesn't -> ADOPT IT!
                    if not any(pos['symbol'] == sym for pos in SHARED_STATE["active_positions"].values()):
                        pid = f"sync_{uuid.uuid4().hex[:8]}"
                        entry = float(p.get('entryPrice', 0))
                        side = 'buy' if p['side'] == 'long' else 'sell'
                        price = self.prices.get(sym, entry)
                        
                        # Set wide safety nets (10% SL/TP) to let Trailing logic take over
                        pos_data = {
                            "id": pid, "symbol": sym, "side": side, "strategy": "Manual/Adopted",
                            "entry": entry, "qty": contracts,
                            "sl": entry * 0.9 if side == 'buy' else entry * 1.1,
                            "tp": entry * 1.1 if side == 'buy' else entry * 0.9,
                            "tp1": entry * 1.05 if side == 'buy' else entry * 0.95,
                            "is_partial": 0, "highest_pnl_pct": ((price - entry)/entry*100) if side=='buy' else ((entry-price)/entry*100)
                        }
                        SHARED_STATE["active_positions"][pid] = pos_data
                        await self.db.insert_trade(pos_data)
                        await self.tg.send(f"🔄 <b>Adopted Position</b>\n{sym} was opened manually/restarted. Bot is now managing it.")

            # If bot has it, but exchange doesn't -> DELETE IT
            for pid, pos in list(SHARED_STATE["active_positions"].items()):
                if pos['symbol'] not in active_remote_syms:
                    await self.db.close_trade(pid, 0.0)
                    del SHARED_STATE["active_positions"][pid]
                    
        except Exception as e: log.debug(f"Sync error: {e}")

# ============================================================================
# 6. WEB DASHBOARD
# ============================================================================
app = Flask(__name__)
auth = HTTPBasicAuth()

@auth.verify_password
def verify(u, p): return u == WEB_USER and p == WEB_PASS

@app.route("/api/status")
@auth.login_required
def api_status(): return jsonify(SHARED_STATE)

@app.route("/")
@auth.login_required
def dashboard():
    html = """
    <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Quant V9.1 Dashboard</title>
    <style>
        body { font-family: Tahoma; background: #0d1117; color: #c9d1d9; padding: 20px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .card { background: #161b22; border: 1px solid #30363d; padding: 20px; border-radius: 10px; }
        h2 { color: #58a6ff; margin-top: 0; } .val { font-size: 1.5em; color: #3fb950; }
    </style>
    <script>
        async function update() {
            let r = await fetch('/api/status'); let d = await r.json();
            document.getElementById('bal').innerText = '$' + d.balance.toFixed(2);
            document.getElementById('dd').innerText = d.current_dd.toFixed(1) + '%';
            if(d.dd_halted) document.getElementById('dd').style.color = "#f85149";
            document.getElementById('pnl').innerText = '$' + d.stats.total_pnl;
            
            let pDiv = document.getElementById('pos'); pDiv.innerHTML = '';
            for (let id in d.active_positions) {
                let p = d.active_positions[id];
                pDiv.innerHTML += `<div style="background:#21262d; margin:5px; padding:10px; border-radius:5px; border-left:4px solid #58a6ff;">
                <b>${p.symbol}</b> (${p.side.toUpperCase()})<br>Strategy: ${p.strategy} | Entry: ${p.entry.toFixed(4)} | Qty: ${p.qty}</div>`;
            }
            if(Object.keys(d.active_positions).length === 0) pDiv.innerHTML = "<span style='color:#8b949e'>No positions</span>";
        } setInterval(update, 2000); window.onload = update;
    </script></head><body>
    <h1 style="text-align:center">🤖 Master Quant Engine V9.1</h1>
    <div class="grid">
        <div class="card"><h2>System</h2><p>Balance: <span id="bal" class="val"></span></p><p>Drawdown: <span id="dd" class="val" style="color:#f0883e"></span></p></div>
        <div class="card"><h2>Stats</h2><p>Total Trades: <span class="val"></span></p><p>Total PnL: <span id="pnl" class="val"></span></p></div>
        <div class="card" style="grid-column: 1 / -1;"><h2>Live Positions</h2><div id="pos"></div></div>
    </div></body></html>
    """
    return render_template_string(html)

def run_web(): app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False)

if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    engine = QuantEngine()
    try: asyncio.run(engine.start())
    except KeyboardInterrupt: log.info("🛑 Shutting down V9.1...")
    finally: asyncio.run(engine.ex.close())
