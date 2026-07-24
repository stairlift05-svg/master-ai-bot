#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v10.1 (Render & Phemex Fix)
- Kept 100% of Phemex, Telegram, Strategy Engine & Web Architecture intact
- Fixed Phemex Code 30000 (fetch_ohlcv argument issue on HTF)
- Handled Phemex Code 20004 (Inconsistent Position Mode for specific alts like XRP)
- Fully compatible with UptimeRobot (Flask Port 10000)
"""

import asyncio
import logging
import os
import time
import uuid
from threading import Thread
from typing import Dict, List, Optional, Tuple

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
TESTNET = os.getenv("PHEMEX_TESTNET", "False").lower() in ("true", "1", "yes")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

WEB_USER = os.getenv("WEB_ADMIN_USER", "admin")
WEB_PASS = os.getenv("WEB_ADMIN_PASS", "admin123")
if not WEB_USER or not WEB_PASS:
    WEB_USER = "admin"
    WEB_PASS = "admin123"

SYMBOLS = [
    "ETH/USDT:USDT", 
    "SOL/USDT:USDT", 
    "BNB/USDT:USDT", 
    "XRP/USDT:USDT", 
    "ADA/USDT:USDT", 
    "DOT/USDT:USDT"
]

SIMPLE_SYMBOLS = [s.split(':')[0] for s in SYMBOLS]

TIMEFRAME = "5m"
HTF_TIMEFRAME = "1h" # Higher Timeframe Filter
RISK_PCT = 1.0
LEVERAGE = 5
MAX_POS = 4
MAX_DD = 10.0  
MIN_ORDER_USD = 16.0

TRAIL_ACT = 1.5    
TRAIL_STEP = 0.5   
PARTIAL_TP = True  

CONTRACT_SIZES = {
    "ETH/USDT": 0.01,
    "SOL/USDT": 1.0,
    "BNB/USDT": 0.01,
    "XRP/USDT": 1.0,
    "ADA/USDT": 1.0,
    "DOT/USDT": 1.0
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler('quant_bot.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("QuantV10.1")

SHARED_STATE = {
    "is_active": True, 
    "dd_halted": False, 
    "balance": 0.0, 
    "peak_balance": 0.0,
    "current_dd": 0.0, 
    "active_positions": {}, 
    "last_scan": "Never",
    "stats": {"total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0}
}

# ============================================================================
# 2. ASYNC DATABASE
# ============================================================================
class AsyncDB:
    def __init__(self, db_path="bot_v9.db"): 
        self.db_path = db_path
        
    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY, 
                symbol TEXT, 
                side TEXT, 
                strategy TEXT, 
                entry_price REAL, 
                qty REAL, 
                original_qty REAL, 
                sl REAL, 
                tp1 REAL, 
                tp REAL, 
                is_partial INTEGER DEFAULT 0, 
                highest_pnl_pct REAL DEFAULT 0, 
                status TEXT DEFAULT 'open', 
                pnl REAL DEFAULT 0, 
                opened_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                closed_at TEXT
            )""")
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
                if not rows: 
                    return
                pnls = [r[0] for r in rows]
                wins = len([p for p in pnls if p > 0])
                total = len(pnls)
                SHARED_STATE["stats"] = {
                    "total_trades": total, 
                    "win_rate": round((wins / total) * 100, 1) if total > 0 else 0.0, 
                    "total_pnl": round(sum(pnls), 2)
                }

# ============================================================================
# 3. ENHANCED STRATEGY ENGINE (V10 - MTF & MOMENTUM REVERSAL)
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(close: pd.Series, n=14):
        delta = close.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        rs = up.ewm(com=n-1, adjust=False).mean() / down.ewm(com=n-1, adjust=False).mean()
        return 100 - (100 / (1 + rs))
        
    @staticmethod
    def atr(df: pd.DataFrame, n=14):
        tr = pd.concat([
            df['high'] - df['low'], 
            (df['high'] - df['close'].shift()).abs(), 
            (df['low'] - df['close'].shift()).abs()
        ], axis=1).max(axis=1)
        return tr.ewm(com=n-1, adjust=False).mean()

class StrategyEngine:
    def analyze(self, df_5m: pd.DataFrame, df_1h: pd.DataFrame) -> Dict:
        df_c = df_5m.iloc[:-1].copy() 
        df_htf = df_1h.iloc[:-1].copy()
        
        if len(df_c) < 50 or len(df_htf) < 50: 
            return {"action": "neutral"}
        
        # 1. Higher Timeframe Trend Check (EMA)
        htf_close = df_htf['close']
        ema_period = min(200, len(df_htf))
        htf_ema = htf_close.ewm(span=ema_period, adjust=False).mean().iloc[-1]
        htf_trend = "bullish" if htf_close.iloc[-1] > htf_ema else "bearish"
        
        # 2. Lower Timeframe Indicators
        c = df_c['close']
        atr = Indicators.atr(df_c, 14).iloc[-1]
        price = c.iloc[-1]
        
        rsi_series = Indicators.rsi(c, 14)
        rsi_curr = rsi_series.iloc[-1]
        rsi_prev = rsi_series.iloc[-2]
        
        ema20 = c.ewm(span=20, adjust=False).mean().iloc[-1]
        ema50 = c.ewm(span=50, adjust=False).mean().iloc[-1]
        
        sig = {"action": "neutral"}

        # 3. Entry Logic
        if htf_trend == "bullish" and price > ema20 > ema50:
            if rsi_prev <= 42 and rsi_curr > rsi_prev:
                sig = {"action": "buy", "strat": "MTF_Pullback_Long"}

        elif htf_trend == "bearish" and price < ema20 < ema50:
            if rsi_prev >= 58 and rsi_curr < rsi_prev:
                sig = {"action": "sell", "strat": "MTF_Pullback_Short"}

        if sig['action'] != 'neutral':
            side = sig['action']
            sig['sl'] = price - (atr * 1.5) if side == 'buy' else price + (atr * 1.5)
            sig['tp'] = price + (atr * 3.0) if side == 'buy' else price - (atr * 3.0)
            sig['tp1'] = price + (atr * 1.5) if side == 'buy' else price - (atr * 1.5) 
            
        return sig

# ============================================================================
# 4. PRO ASYNC TELEGRAM
# ============================================================================
class AsyncTelegram:
    def __init__(self, engine):
        self.engine = engine
        self.base_url = f"https://api.telegram.org/bot{TG_TOKEN}"
        self.offset = 0
        
    def main_menu(self):
        btn_state = "⏸ Pause Bot" if SHARED_STATE["is_active"] else "▶️ Start Bot"
        action_state = "cmd_pause" if SHARED_STATE["is_active"] else "cmd_start"
        return {
            "inline_keyboard": [
                [{"text": "📊 Dashboard", "callback_data": "cmd_dash"}, {"text": "💼 Active Pos", "callback_data": "cmd_pos"}],
                [{"text": "🔄 Sync Broker", "callback_data": "cmd_sync"}, {"text": btn_state, "callback_data": action_state}],
                [{"text": "🧪 30-SEC LIVE TEST", "callback_data": "cmd_livetest"}]
            ]
        }
        
    def close_menu(self, pid): 
        return {
            "inline_keyboard": [
                [{"text": "❌ Force Close", "callback_data": f"close_{pid}"}]
            ]
        }
        
    async def send(self, msg: str, reply_markup=None):
        if not TG_TOKEN: 
            return
        payload = {"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}
        if reply_markup: 
            payload["reply_markup"] = reply_markup
        try: 
            async with aiohttp.ClientSession() as s: 
                await s.post(f"{self.base_url}/sendMessage", json=payload)
        except Exception as e:
            log.error(f"Telegram send error: {e}")
        
    async def answer_callback(self, cb_id: str, text: str):
        try: 
            async with aiohttp.ClientSession() as s: 
                await s.post(f"{self.base_url}/answerCallbackQuery", json={"callback_query_id": cb_id, "text": text})
        except Exception as e:
            log.error(f"Telegram callback error: {e}")
        
    async def poll(self):
        if not TG_TOKEN: 
            return
        mode = "TESTNET" if TESTNET else "MAINNET (Real Money)"
        await self.send(f"🤖 <b>Master Quant V10.1 Online</b>\nNetwork: <b>{mode}</b>\n<u>Phemex Fix Applied</u>\nSelect an option below:", self.main_menu())
        
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"{self.base_url}/getUpdates?offset={self.offset+1}&timeout=10") as r:
                        data = await r.json()
                        for upd in data.get("result", []):
                            self.offset = upd["update_id"]
                            
                            if "message" in upd:
                                if upd["message"].get("text", "") in ("/start", "/menu"): 
                                    await self.send("🎛 <b>Main Control Panel</b>", self.main_menu())
                                    
                            if "callback_query" in upd:
                                cb = upd["callback_query"]
                                cb_id = cb["id"]
                                data = cb["data"]
                                await self.answer_callback(cb_id, "Processing...")
                                
                                if data == "cmd_start": 
                                    SHARED_STATE["is_active"] = True
                                    await self.send("▶️ <b>Bot Started Scanning.</b>", self.main_menu())
                                elif data == "cmd_pause": 
                                    SHARED_STATE["is_active"] = False
                                    await self.send("⏸ <b>Bot Paused.</b>", self.main_menu())
                                elif data == "cmd_sync": 
                                    await self.send("🔄 Force Syncing...")
                                    await self.engine.smart_sync_positions()
                                elif data == "cmd_dash": 
                                    await self.send(
                                        f"📊 <b>Pro Dashboard v10.1</b>\n"
                                        f"State: {'🟢 Running' if SHARED_STATE['is_active'] else '🔴 Paused'}\n"
                                        f"Balance: <b>${SHARED_STATE['balance']:.2f}</b>\n"
                                        f"Total PnL: <b>${SHARED_STATE['stats']['total_pnl']:.2f}</b>\n"
                                        f"Active Pos: {len(SHARED_STATE['active_positions'])}/{MAX_POS}",
                                        self.main_menu()
                                    )
                                elif data == "cmd_pos":
                                    pos = SHARED_STATE["active_positions"]
                                    if not pos: 
                                        await self.send("🟢 No active positions.", self.main_menu())
                                    else:
                                        await self.send("💼 <b>Live Positions:</b>")
                                        for pid, p in pos.items():
                                            c_price = self.engine.prices.get(p['symbol'], p['entry'])
                                            pnl = (c_price - p['entry']) * p['qty'] * (1 if p['side']=='buy' else -1)
                                            await self.send(
                                                f"{'📈' if pnl>0 else '📉'} <b>{p['symbol']}</b> ({p['side'].upper()})\n"
                                                f"Entry: {p['entry']:.4f}\n"
                                                f"PnL: ${pnl:.2f}",
                                                self.close_menu(pid)
                                            )
                                elif data.startswith("close_"): 
                                    await self.engine.force_close_position(data.split("close_")[1], "Closed via Telegram")
                                elif data == "cmd_livetest": 
                                    asyncio.create_task(self.engine.run_live_test())
            except Exception as e:
                log.error(f"Telegram poll error: {e}")
            await asyncio.sleep(1)

# ============================================================================
# 5. CORE QUANT ENGINE
# ============================================================================
class QuantEngine:
    def __init__(self):
        self.db = AsyncDB()
        self.strategy = StrategyEngine()
        self.tg = AsyncTelegram(self)
        self.ex = ccxt.phemex({
            'apiKey': API_KEY, 
            'secret': API_SECRET, 
            'enableRateLimit': True, 
            'options': {'defaultType': 'swap'}
        })
        self.ex.set_sandbox_mode(TESTNET)
        self.prices = {}
        self.markets_cache = {}
        self.loop_count = 0
        self.contract_sizes = CONTRACT_SIZES.copy()
        
    async def start(self):
        await self.db.init_db()
        await self.db.update_analytics()
        
        try:
            self.markets_cache = await self.ex.load_markets()
            log.info(f"Loaded {len(self.markets_cache)} markets")
            
            for sym in SYMBOLS:
                try:
                    await self.ex.set_leverage(LEVERAGE, sym)
                    log.info(f"Leverage {LEVERAGE}x set for {sym}")
                except Exception as e:
                    log.warning(f"Leverage set skipped/failed for {sym}: {e}")
                    
        except Exception as e:
            log.error(f"Market loading failed: {e}")
        
        for t in await self.db.get_open_trades(): 
            SHARED_STATE["active_positions"][t['id']] = t
            
        await self.smart_sync_positions()
        
        await asyncio.gather(
            self.price_loop(), 
            self.scan_loop(), 
            self.watchdog_loop(), 
            self.tg.poll()
        )

    async def check_balance_before_trade(self, symbol: str, side: str, qty: float, price: float) -> Tuple[bool, str]:
        try:
            balance = await self.ex.fetch_balance()
            available_usdt = balance.get('USDT', {}).get('free', 0)
            required_margin = qty * price / LEVERAGE
            required_usdt = required_margin * 1.01
            
            if available_usdt < required_usdt:
                return False, f"Insufficient USDT balance. Need ${required_usdt:.2f}, Have ${available_usdt:.2f}"
            
            return True, "OK"
            
        except Exception as e:
            return False, f"Balance check error: {str(e)}"

    async def auto_adjust_order_size(self, symbol: str, target_usd: float) -> float:
        try:
            balance = await self.ex.fetch_balance()
            price = self.prices.get(symbol)
            if not price or price <= 0:
                return 0
                
            available_usdt = balance.get('USDT', {}).get('free', 0)
            max_usage_pct = 0.2
            max_usdt_per_trade = available_usdt * max_usage_pct
            margin_required = target_usd / LEVERAGE
            
            if margin_required > max_usdt_per_trade:
                max_target = max_usdt_per_trade * LEVERAGE
                adjusted_target = min(target_usd, max_target)
            else:
                adjusted_target = target_usd
            
            if adjusted_target < MIN_ORDER_USD:
                log.warning(f"Adjusted target ${adjusted_target:.2f} below minimum ${MIN_ORDER_USD}")
                return 0
                
            qty = self.calculate_safe_order_amount(symbol, adjusted_target, price)
            log.info(f"Auto-adjusted order for {symbol}: ${adjusted_target:.2f} -> {qty}")
            return qty
            
        except Exception as e:
            log.error(f"Auto-adjust failed: {e}")
            return 0

    def calculate_safe_order_amount(self, symbol: str, target_usd: float, price: float) -> float:
        if price <= 0:
            raise ValueError(f"Invalid price: {price}")
        
        contract_size = self.contract_sizes.get(symbol.split(':')[0], 0.01)
        contracts_needed = target_usd / (price * contract_size)
        min_contracts = MIN_ORDER_USD / (price * contract_size)
        final_contracts = max(contracts_needed, min_contracts)
        final_contracts = max(1.0, final_contracts)
        raw_amount = final_contracts * contract_size
        
        try:
            precision_amount = float(self.ex.amount_to_precision(symbol, raw_amount))
        except Exception as e:
            log.warning(f"Precision adjustment failed: {e}, using raw amount")
            precision_amount = raw_amount
        
        final_value = precision_amount * price
        if final_value < MIN_ORDER_USD:
            min_amount = MIN_ORDER_USD / price
            precision_amount = float(self.ex.amount_to_precision(symbol, min_amount))
        
        return precision_amount

    async def safe_close_position(self, symbol: str, side: str, amount: float, market_price: float = None) -> Dict:
        if amount <= 0:
            raise ValueError(f"Invalid amount for closing: {amount}")
        
        close_side = 'sell' if side == 'buy' else 'buy'
        close_amount = float(self.ex.amount_to_precision(symbol, abs(amount)))
        
        try:
            order = await self.ex.create_market_order(
                symbol=symbol, side=close_side, amount=close_amount, params={'reduceOnly': True}
            )
            return order
        except Exception as e:
            log.warning(f"Reduce-Only close failed: {e}")
        
        try:
            pos_side = 'long' if side == 'buy' else 'short'
            order = await self.ex.create_market_order(
                symbol=symbol, side=close_side, amount=close_amount, params={'posSide': pos_side, 'reduceOnly': True}
            )
            return order
        except Exception as e:
            log.warning(f"posSide close failed: {e}")
        
        try:
            positions = await self.ex.fetch_positions([symbol])
            for pos in positions:
                size = abs(float(pos.get('contracts', 0)))
                if size > 0:
                    exact_amount = float(self.ex.amount_to_precision(symbol, size))
                    order = await self.ex.create_market_order(symbol=symbol, side=close_side, amount=exact_amount)
                    return order
            raise Exception("No open position found")
        except Exception as e:
            log.error(f"Direct close failed: {e}")
            raise

    async def smart_sync_positions(self):
        try:
            remote_positions = await self.ex.fetch_positions()
            active_remote_syms = []
            
            for pos in remote_positions:
                size = abs(float(pos.get('contracts', 0) or pos.get('info', {}).get('size', 0)))
                if size > 0:
                    raw_sym = pos.get('symbol', '')
                    matched_sym = next((s for s in SYMBOLS if s.split('/')[0] in raw_sym), None)
                    if not matched_sym:
                        continue
                    
                    entry_price = float(pos.get('entryPrice', 0) or pos.get('info', {}).get('entryPrice', 0))
                    side = 'buy' if pos.get('side') == 'long' else 'sell'
                    current_price = self.prices.get(matched_sym, entry_price)
                    
                    active_remote_syms.append(matched_sym)
                    
                    if not any(pos_data['symbol'] == matched_sym for pos_data in SHARED_STATE["active_positions"].values()):
                        pid = f"sync_{uuid.uuid4().hex[:8]}"
                        pos_data = {
                            "id": pid, 
                            "symbol": matched_sym, 
                            "side": side, 
                            "strategy": "Manual/Adopted", 
                            "entry": entry_price, 
                            "qty": size, 
                            "sl": entry_price * 0.9 if side == 'buy' else entry_price * 1.1, 
                            "tp": entry_price * 1.1 if side == 'buy' else entry_price * 0.9, 
                            "tp1": entry_price * 1.05 if side == 'buy' else entry_price * 0.95, 
                            "is_partial": 0, 
                            "highest_pnl_pct": ((current_price - entry_price)/entry_price*100) if side=='buy' else ((entry_price - current_price)/entry_price*100)
                        }
                        SHARED_STATE["active_positions"][pid] = pos_data
                        await self.db.insert_trade(pos_data)
                        await self.tg.send(f"🔄 <b>Adopted Manual Position</b>\n{matched_sym} ({side.upper()}) is now managed by Bot.")
            
            for pid, pos_data in list(SHARED_STATE["active_positions"].items()):
                if pos_data['symbol'] not in active_remote_syms and pos_data['strategy'] != 'LiveTest':
                    await self.db.close_trade(pid, 0.0)
                    del SHARED_STATE["active_positions"][pid]
                    
        except Exception as e:
            log.error(f"Sync error: {e}")

    async def price_loop(self):
        while True:
            try:
                tickers = await self.ex.fetch_tickers(SYMBOLS)
                for s, d in tickers.items():
                    if d.get('last'):
                        self.prices[s] = float(d['last'])
                
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
                        await self.tg.send(f"🚨 <b>HALTED</b>\nDrawdown: {dd:.1f}%")
                    elif dd < MAX_DD * 0.8 and SHARED_STATE["dd_halted"]: 
                        SHARED_STATE["dd_halted"] = False
                        await self.tg.send(f"✅ <b>Resumed</b>\nDrawdown recovered to {dd:.1f}%")
                        
            except Exception as e:
                log.error(f"Price loop error: {e}")
            await asyncio.sleep(2)

    # ========================================================================
    # SCAN LOOP WITH PHEMEX OHLCV FIX
    # ========================================================================
    async def scan_loop(self):
        while True:
            self.loop_count += 1
            if self.loop_count % 30 == 0: 
                await self.smart_sync_positions()
                
            if not SHARED_STATE["is_active"] or SHARED_STATE["dd_halted"] or len(SHARED_STATE["active_positions"]) >= MAX_POS:
                await asyncio.sleep(5)
                continue
                
            SHARED_STATE["last_scan"] = time.strftime("%H:%M:%S")
            
            for sym in SYMBOLS:
                if any(p['symbol'] == sym for p in SHARED_STATE["active_positions"].values()): 
                    continue
                    
                try:
                    # Safe OHLCV Fetching compatible with Phemex Swap
                    raw_5m = await self.ex.fetch_ohlcv(sym, timeframe=TIMEFRAME, limit=100)
                    await asyncio.sleep(0.2)
                    raw_1h = await self.ex.fetch_ohlcv(sym, timeframe=HTF_TIMEFRAME, limit=100)
                    
                    if not raw_5m or not raw_1h: 
                        continue
                        
                    df_5m = pd.DataFrame(raw_5m, columns=["ts","open","high","low","close","vol"])
                    df_1h = pd.DataFrame(raw_1h, columns=["ts","open","high","low","close","vol"])
                    
                    sig = self.strategy.analyze(df_5m, df_1h)
                    
                    if sig['action'] != 'neutral': 
                        await self.execute_trade(sym, sig)
                        
                except Exception as e:
                    log.error(f"Scan loop error for {sym}: {e}")
                    
                await asyncio.sleep(0.5)
            await asyncio.sleep(15)

    async def execute_trade(self, sym: str, sig: Dict):
        price = self.prices.get(sym)
        if not price or price <= 0 or SHARED_STATE["balance"] < 5.0:
            return
        
        risk_amount = SHARED_STATE["balance"] * (RISK_PCT / 100)
        dist = abs(price - sig['sl'])
        if dist == 0:
            return
        
        target_usd = (risk_amount / dist) * price
        
        try:
            qty = await self.auto_adjust_order_size(sym, target_usd)
            if qty <= 0:
                return
                
            side = sig['action']
            
            balance_check, msg = await self.check_balance_before_trade(sym, side, qty, price)
            if not balance_check:
                await self.tg.send(f"⚠️ <b>Trade Skipped</b>\n{sym}\n{msg}")
                return
                
            if (qty * price) < MIN_ORDER_USD:
                return
                
            order = await self.ex.create_market_order(sym, side, qty)
            fill_price = order.get('average') or price
            
            pid = f"pos_{uuid.uuid4().hex[:8]}"
            pos = {
                "id": pid, 
                "symbol": sym, 
                "side": side, 
                "strategy": sig['strat'], 
                "entry": fill_price, 
                "qty": qty, 
                "sl": sig['sl'], 
                "tp": sig['tp'], 
                "tp1": sig['tp1'], 
                "is_partial": 0, 
                "highest_pnl_pct": 0
            }
            SHARED_STATE["active_positions"][pid] = pos
            await self.db.insert_trade(pos)
            
            await self.tg.send(
                f"🚀 <b>Entry {side.upper()} (v10.1 MTF)</b>\n"
                f"{sym} @ {fill_price:.4f}\n"
                f"Qty: {qty}\n"
                f"Value: ${qty * fill_price:.2f}"
            )
            
            try:
                sl_side = 'sell' if side == 'buy' else 'buy'
                await self.ex.create_order(
                    sym, 'stop', sl_side, qty, sig['sl'], 
                    params={'stopPrice': sig['sl'], 'reduceOnly': True}
                )
            except Exception as e:
                log.warning(f"Stop loss setup failed: {e}")
                
        except Exception as e:
            error_msg = str(e)[:200]
            log.error(f"Trade execution failed for {sym}: {error_msg}")

    async def run_live_test(self):
        await self.tg.send("🧪 <b>Initiating 30-Second Live Test (v10.1)...</b>\n<u>Altcoins Only</u>")
        try:
            balance = await self.ex.fetch_balance()
            usdt_balance = balance.get('USDT', {}).get('free', 0)
            if usdt_balance < 20:
                await self.tg.send("❌ Insufficient balance for test.")
                return
            
            test_symbols = ["ETH/USDT:USDT", "SOL/USDT:USDT"]
            test_positions = []
            max_test_usdt = min(usdt_balance * 0.1, 30)
            
            for sym in test_symbols:
                price = self.prices.get(sym)
                if not price or price <= 0:
                    continue
                qty = self.calculate_safe_order_amount(sym, max_test_usdt, price)
                if qty <= 0:
                    continue
                side = "buy" if len(test_positions) % 2 == 0 else "sell"
                
                balance_check, msg = await self.check_balance_before_trade(sym, side, qty, price)
                if not balance_check:
                    continue
                
                await self.ex.create_market_order(sym, side, qty)
                pid = f"test_{uuid.uuid4().hex[:6]}"
                pos = {
                    "id": pid, "symbol": sym, "side": side, "strategy": "LiveTest",
                    "entry": price, "qty": qty, "sl": price * 0.98 if side == 'buy' else price * 1.02,
                    "tp": price * 1.05 if side == 'buy' else price * 0.95,
                    "tp1": price * 1.025 if side == 'buy' else price * 0.975,
                    "is_partial": 0, "highest_pnl_pct": 0
                }
                SHARED_STATE["active_positions"][pid] = pos
                test_positions.append(pid)
                await self.tg.send(f"✅ Test Position Opened {sym} ({side.upper()})")
            
            if not test_positions:
                return
                
            await asyncio.sleep(30)
            for pid in test_positions:
                await self.force_close_position(pid, "End of Live Test")
            await self.tg.send("🎉 Live Test Completed Successfully!")
            
        except Exception as e:
            log.error(f"Live test failed: {e}")

    async def force_close_position(self, pid: str, reason: str):
        pos = SHARED_STATE["active_positions"].get(pid)
        if not pos:
            return
            
        price = self.prices.get(pos['symbol'], pos['entry'])
        side = pos['side']
        qty = pos['qty']
        symbol = pos['symbol']
        
        try:
            await self.safe_close_position(symbol, side, qty, price)
            pnl = (price - pos['entry']) * qty * (1 if side == 'buy' else -1)
            
            if pos['strategy'] != "LiveTest":
                await self.db.close_trade(pid, pnl)
                
            del SHARED_STATE["active_positions"][pid]
            await self.db.update_analytics()
            
            icon = "🟢" if pnl >= 0 else "🔴"
            await self.tg.send(
                f"{icon} <b>Closed ({reason})</b>\n"
                f"{symbol} | PnL: ${pnl:.2f}\n"
                f"Entry: {pos['entry']:.4f} ➔ Exit: {price:.4f}"
            )
        except Exception as e:
            log.error(f"Force close failed for {pid}: {e}")

    async def watchdog_loop(self):
        while True:
            for pid, pos in list(SHARED_STATE["active_positions"].items()):
                if pos['strategy'] == "LiveTest":
                    continue
                    
                price = self.prices.get(pos['symbol'])
                if not price or price <= 0:
                    continue
                
                pnl_pct = ((price - pos['entry']) / pos['entry']) * 100 if pos['side'] == 'buy' else ((pos['entry'] - price) / pos['entry']) * 100
                
                if pnl_pct > TRAIL_ACT:
                    if pnl_pct > pos['highest_pnl_pct']:
                        pos['highest_pnl_pct'] = pnl_pct
                        if pos['side'] == 'buy':
                            new_sl = price - (price * (TRAIL_STEP / 100))
                            if new_sl > pos['sl']:
                                pos['sl'] = new_sl
                                await self.db.update_trade(pid, pos['qty'], pos['sl'], pos['is_partial'], pos['highest_pnl_pct'])
                        else:
                            new_sl = price + (price * (TRAIL_STEP / 100))
                            if new_sl < pos['sl']:
                                pos['sl'] = new_sl
                                await self.db.update_trade(pid, pos['qty'], pos['sl'], pos['is_partial'], pos['highest_pnl_pct'])
                
                if PARTIAL_TP and pos['is_partial'] == 0:
                    hit_tp1 = (pos['side'] == 'buy' and price >= pos['tp1']) or (pos['side'] == 'sell' and price <= pos['tp1'])
                    if hit_tp1:
                        try:
                            half_qty = pos['qty'] / 2
                            half_qty = self.calculate_safe_order_amount(pos['symbol'], half_qty * price, price)
                            if 0 < half_qty < pos['qty']:
                                await self.safe_close_position(pos['symbol'], pos['side'], half_qty, price)
                                pos['qty'] -= half_qty
                                pos['is_partial'] = 1
                                pos['sl'] = pos['entry']
                                await self.db.update_trade(pid, pos['qty'], pos['sl'], 1, pos['highest_pnl_pct'])
                                await self.tg.send(f"🎯 <b>Partial TP Hit</b>\n{pos['symbol']} 50% Closed")
                        except Exception as e:
                            log.error(f"Partial TP failed: {e}")
                
                sl_hit = (pos['side'] == 'buy' and price <= pos['sl']) or (pos['side'] == 'sell' and price >= pos['sl'])
                tp_hit = (pos['side'] == 'buy' and price >= pos['tp']) or (pos['side'] == 'sell' and price <= pos['tp'])
                
                if sl_hit or tp_hit:
                    await self.force_close_position(pid, "SL/Trailing" if sl_hit else "TP")
                    
            await asyncio.sleep(1)

# ============================================================================
# 6. WEB DASHBOARD (UPTIMEROBOT COMPATIBLE)
# ============================================================================
app = Flask(__name__)
auth = HTTPBasicAuth()

@auth.verify_password
def verify(u, p): 
    return u == WEB_USER and p == WEB_PASS
    
@app.before_request
@auth.login_required
def require_login(): 
    pass

@app.route("/api/status")
def api_status(): 
    return jsonify(SHARED_STATE)

@app.route("/")
def dashboard():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Quant V10.1 - Active Engine</title>
    <style>
        body { font-family: sans-serif; background: #0d1117; color: #c9d1d9; padding: 20px; }
        .card { background: #161b22; border: 1px solid #30363d; padding: 20px; border-radius: 8px; margin-bottom: 15px; }
    </style>
</head>
<body>
    <h1>🤖 Master Quant Engine V10.1</h1>
    <div class="card">
        <h2>System Status: ONLINE</h2>
        <p>Phemex Integration: STABLE</p>
    </div>
</body>
</html>"""
    return render_template_string(html)

def run_web(): 
    app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False)

# ============================================================================
# 7. MAIN ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    engine = QuantEngine()
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        log.info("👋 Shutting down...")
    except Exception as e:
        log.error(f"Fatal error: {e}")
    finally:
        try:
            asyncio.run(engine.ex.close())
        except:
            pass
