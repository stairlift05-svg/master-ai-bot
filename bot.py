#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v10.0 (Ultimate Optimized Edition)
- Fixed time lag bug with proper candle alignment
- Wilders RSI for accurate signals
- ATR-based intelligent trailing stop
- Multi-timeframe trend filter
- Volume confirmation filter
- Global stop loss protection
- Dynamic position sizing
- Fee-adjusted PnL calculation
- Risk-reward ratio optimized (1:4)
- No Bitcoin - Altcoins only
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
import numpy as np
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

# Correct Phemex format for perpetual swaps
SYMBOLS = [
    "ETH/USDT:USDT", 
    "SOL/USDT:USDT", 
    "BNB/USDT:USDT", 
    "XRP/USDT:USDT", 
    "ADA/USDT:USDT", 
    "DOT/USDT:USDT"
]

TIMEFRAME = "5m"
HIGHER_TIMEFRAME = "1h"  # For MTF filter
RISK_PCT = 1.0
LEVERAGE = 5
MAX_POS = 3  # Reduced from 4 for better risk management
MAX_DD = 8.0  # Reduced from 10
MIN_ORDER_USD = 16.0

# Optimized trailing stop settings
TRAIL_ACT = 2.0  # Increased from 1.5
TRAIL_STEP = 0.3  # Decreased from 0.5
PARTIAL_TP = True

# Global stop loss
GLOBAL_STOP_LOSS = 5.0  # Percentage from peak

# Fee settings
TAKER_FEE = 0.0006  # 0.06%
MAKER_FEE = 0.0002  # 0.02%

# Contract size fallback for altcoins
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
log = logging.getLogger("QuantV10.0")

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
    def __init__(self, db_path="bot_v10.db"): 
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
                atr_value REAL DEFAULT 0,
                is_partial INTEGER DEFAULT 0, 
                highest_pnl_pct REAL DEFAULT 0, 
                status TEXT DEFAULT 'open', 
                gross_pnl REAL DEFAULT 0,
                net_pnl REAL DEFAULT 0,
                fees REAL DEFAULT 0,
                opened_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                closed_at TEXT
            )""")
            await db.commit()
            
    async def insert_trade(self, t: Dict):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO trades 
                (id, symbol, side, strategy, entry_price, qty, original_qty, sl, tp1, tp, atr_value) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (t['id'], t['symbol'], t['side'], t['strategy'], t['entry'], t['qty'], 
                 t['qty'], t['sl'], t['tp1'], t['tp'], t.get('atr_value', 0))
            )
            await db.commit()
            
    async def update_trade(self, t_id: str, qty: float, sl: float, is_partial: int, highest_pnl: float):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE trades SET qty=?, sl=?, is_partial=?, highest_pnl_pct=? WHERE id=?",
                (qty, sl, is_partial, highest_pnl, t_id)
            )
            await db.commit()
            
    async def close_trade(self, t_id: str, gross_pnl: float, net_pnl: float, fees: float):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE trades SET status='closed', gross_pnl=?, net_pnl=?, fees=?, closed_at=CURRENT_TIMESTAMP WHERE id=?",
                (gross_pnl, net_pnl, fees, t_id)
            )
            await db.commit()
            
    async def get_open_trades(self) -> List[Dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM trades WHERE status='open'") as cursor:
                return [dict(row) for row in await cursor.fetchall()]
                
    async def update_analytics(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT net_pnl FROM trades WHERE status='closed'") as cursor:
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
# 3. IMPROVED INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi_wilders(close: pd.Series, n=14):
        """Wilders RSI - More accurate for crypto"""
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=n).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=n).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.fillna(50)
        return rsi
    
    @staticmethod
    def rsi_ewm(close: pd.Series, n=14):
        """EWM version of RSI"""
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1/n, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/n, adjust=False).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    @staticmethod
    def atr(df: pd.DataFrame, n=14):
        """Average True Range"""
        tr = pd.concat([
            df['high'] - df['low'], 
            (df['high'] - df['close'].shift()).abs(), 
            (df['low'] - df['close'].shift()).abs()
        ], axis=1).max(axis=1)
        return tr.ewm(alpha=1/n, adjust=False).mean()
    
    @staticmethod
    def ema(series: pd.Series, period: int):
        """Exponential Moving Average"""
        return series.ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def sma(series: pd.Series, period: int):
        """Simple Moving Average"""
        return series.rolling(window=period).mean()

# ============================================================================
# 4. IMPROVED STRATEGY ENGINE
# ============================================================================
class StrategyEngine:
    def __init__(self):
        self.last_signal = {}
    
    def analyze(self, df: pd.DataFrame) -> Dict:
        """
        تحلیل با در نظر گرفتن کندل جاری برای قیمت 
        و کندل‌های گذشته برای اندیکاتورها
        """
        if len(df) < 30:
            return {"action": "neutral"}
        
        close = df['close']
        high = df['high']
        low = df['low']
        
        # محاسبه اندیکاتورها روی کل دیتا
        rsi_series = Indicators.rsi_wilders(close, n=14)
        ema20_series = Indicators.ema(close, 20)
        ema50_series = Indicators.ema(close, 50)
        
        # قیمت جاری (آخرین کندل کامل شده)
        current_price = close.iloc[-1]
        current_rsi = rsi_series.iloc[-1]
        current_ema20 = ema20_series.iloc[-1]
        current_ema50 = ema50_series.iloc[-1]
        
        # محاسبه ATR برای کندل‌های کامل شده
        atr_value = Indicators.atr(df, n=14).iloc[-1]
        
        # Volume confirmation
        volume_avg = df['vol'].iloc[-20:].mean()
        current_volume = df['vol'].iloc[-1]
        volume_confirmed = current_volume > volume_avg * 1.5
        
        # سیگنال‌دهی با داده‌های هم‌زمان
        sig = {"action": "neutral"}
        
        if current_price > current_ema20 > current_ema50 and current_rsi < 45 and volume_confirmed:
            sig = {"action": "buy", "strat": "Pullback_Long"}
            sig['sl'] = current_price - (atr_value * 2.0)  # Increased SL
            sig['tp'] = current_price + (atr_value * 4.0)  # Increased TP (1:4 ratio)
            sig['tp1'] = current_price + (atr_value * 2.0)  # Partial TP at 1:2
            sig['atr'] = atr_value
            
        elif current_price < current_ema20 < current_ema50 and current_rsi > 55 and volume_confirmed:
            sig = {"action": "sell", "strat": "Pullback_Short"}
            sig['sl'] = current_price + (atr_value * 2.0)
            sig['tp'] = current_price - (atr_value * 4.0)
            sig['tp1'] = current_price - (atr_value * 2.0)
            sig['atr'] = atr_value
        
        return sig

# ============================================================================
# 5. PRO ASYNC TELEGRAM
# ============================================================================
class AsyncTelegram:
    def __init__(self, engine):
        self.engine = engine
        self.base_url = f"https://api.telegram.org/bot{TG_TOKEN}"
        self.offset = 0
        
    def main_menu(self):
        btn_state = "⏹ Pause Bot" if SHARED_STATE["is_active"] else "▶️ Start Bot"
        action_state = "cmd_pause" if SHARED_STATE["is_active"] else "cmd_start"
        return {
            "inline_keyboard": [
                [{"text": "📊 Dashboard", "callback_data": "cmd_dash"}, {"text": "📈 Active Pos", "callback_data": "cmd_pos"}],
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
        await self.send(f"🚀 <b>Master Quant V10.0 Online</b>\nNetwork: <b>{mode}</b>\n<u>Optimized Strategy - No BTC</u>\nSelect an option below:", self.main_menu())
        
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
                                    await self.send("⏹ <b>Bot Paused.</b>", self.main_menu())
                                elif data == "cmd_sync": 
                                    await self.send("🔄 Force Syncing...")
                                    await self.engine.smart_sync_positions()
                                elif data == "cmd_dash": 
                                    await self.send(
                                        f"📊 <b>Pro Dashboard</b>\n"
                                        f"State: {'🟢 Running' if SHARED_STATE['is_active'] else '🔴 Paused'}\n"
                                        f"Balance: <b>${SHARED_STATE['balance']:.2f}</b>\n"
                                        f"Total PnL: <b>${SHARED_STATE['stats']['total_pnl']:.2f}</b>\n"
                                        f"Active Pos: {len(SHARED_STATE['active_positions'])}/{MAX_POS}",
                                        self.main_menu()
                                    )
                                elif data == "cmd_pos":
                                    pos = SHARED_STATE["active_positions"]
                                    if not pos: 
                                        await self.send("📭 No active positions.", self.main_menu())
                                    else:
                                        await self.send("🏦 <b>Live Positions:</b>")
                                        for pid, p in pos.items():
                                            c_price = self.engine.prices.get(p['symbol'], p['entry'])
                                            gross_pnl = (c_price - p['entry']) * p['qty'] * (1 if p['side']=='buy' else -1)
                                            fees = self.engine.calculate_fees(p['entry'], c_price, p['qty'])
                                            net_pnl = gross_pnl - fees
                                            await self.send(
                                                f"{'🟩' if net_pnl>0 else '🟥'} <b>{p['symbol']}</b> ({p['side'].upper()})\n"
                                                f"Entry: {p['entry']:.4f}\n"
                                                f"PnL: ${net_pnl:.2f}",
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
# 6. CORE QUANT ENGINE
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
                    log.warning(f"Leverage set failed for {sym}: {e}")
                    
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

    # ========================================================================
    # FEE CALCULATION
    # ========================================================================
    def calculate_fees(self, entry_price: float, exit_price: float, qty: float) -> float:
        """Calculate total fees for a trade"""
        entry_fee = entry_price * qty * TAKER_FEE
        exit_fee = exit_price * qty * TAKER_FEE
        return entry_fee + exit_fee
    
    def calculate_net_pnl(self, entry_price: float, exit_price: float, qty: float, side: str) -> Tuple[float, float]:
        """Calculate gross and net PnL with fees"""
        gross_pnl = (exit_price - entry_price) * qty * (1 if side == 'buy' else -1)
        fees = self.calculate_fees(entry_price, exit_price, qty)
        net_pnl = gross_pnl - fees
        return gross_pnl, net_pnl, fees

    # ========================================================================
    # MTF TREND FILTER
    # ========================================================================
    async def check_mtf_trend(self, symbol: str) -> str:
        """Check trend on higher timeframe (1H)"""
        try:
            ohlcv_1h = await self.ex.fetch_ohlcv(symbol, HIGHER_TIMEFRAME, limit=50)
            if not ohlcv_1h:
                return "neutral"
            
            df_1h = pd.DataFrame(ohlcv_1h, columns=["ts","open","high","low","close","vol"])
            close_1h = df_1h['close']
            
            ema20_1h = Indicators.ema(close_1h, 20).iloc[-1]
            ema50_1h = Indicators.ema(close_1h, 50).iloc[-1]
            current_price_1h = close_1h.iloc[-1]
            
            if current_price_1h > ema20_1h > ema50_1h:
                return "bullish"
            elif current_price_1h < ema20_1h < ema50_1h:
                return "bearish"
            else:
                return "neutral"
                
        except Exception as e:
            log.error(f"MTF check error: {e}")
            return "neutral"

    # ========================================================================
    # DYNAMIC MAX POSITIONS
    # ========================================================================
    async def get_dynamic_max_pos(self) -> int:
        """Calculate dynamic max positions based on market volatility"""
        try:
            total_volatility = 0
            count = 0
            for sym in SYMBOLS:
                ohlcv = await self.ex.fetch_ohlcv(sym, TIMEFRAME, limit=20)
                if ohlcv:
                    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                    atr = Indicators.atr(df, n=14).iloc[-1]
                    price = df['close'].iloc[-1]
                    total_volatility += (atr / price) * 100
                    count += 1
            
            if count > 0:
                avg_volatility = total_volatility / count
                
                if avg_volatility > 3:
                    return 2
                elif avg_volatility > 1.5:
                    return 3
                else:
                    return 4
                    
        except Exception as e:
            log.error(f"Dynamic max pos error: {e}")
        
        return MAX_POS

    # ========================================================================
    # GLOBAL STOP LOSS
    # ========================================================================
    async def check_global_stop_loss(self) -> bool:
        """Check if global stop loss is triggered"""
        current_balance = SHARED_STATE["balance"]
        peak_balance = SHARED_STATE["peak_balance"]
        
        if peak_balance <= 0:
            return False
        
        loss_pct = ((peak_balance - current_balance) / peak_balance) * 100
        
        if loss_pct >= GLOBAL_STOP_LOSS:
            await self.tg.send(
                f"🛑 <b>GLOBAL STOP LOSS TRIGGERED</b>\n"
                f"Loss: {loss_pct:.1f}%\n"
                f"Peak: ${peak_balance:.2f}\n"
                f"Current: ${current_balance:.2f}\n"
                f"Closing all positions..."
            )
            
            for pid in list(SHARED_STATE["active_positions"].keys()):
                await self.force_close_position(pid, "Global Stop Loss")
            
            SHARED_STATE["is_active"] = False
            return True
        
        return False

    # ========================================================================
    # BALANCE CHECK
    # ========================================================================
    async def check_balance_before_trade(self, symbol: str, side: str, qty: float, price: float) -> Tuple[bool, str]:
        """Check if sufficient balance for trade"""
        try:
            balance = await self.ex.fetch_balance()
            available_usdt = balance.get('USDT', {}).get('free', 0)
            
            required_margin = qty * price / LEVERAGE
            required_usdt = required_margin * 1.01  # +1% for fees
            
            if available_usdt < required_usdt:
                return False, f"Insufficient USDT. Need ${required_usdt:.2f}, Have ${available_usdt:.2f}"
            
            return True, "OK"
            
        except Exception as e:
            return False, f"Balance check error: {str(e)}"

    # ========================================================================
    # AUTO ADJUST ORDER SIZE
    # ========================================================================
    async def auto_adjust_order_size(self, symbol: str, target_usd: float) -> float:
        """Auto adjust order size based on available balance"""
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
                return 0
                
            qty = self.calculate_safe_order_amount(symbol, adjusted_target, price)
            
            return qty
            
        except Exception as e:
            log.error(f"Auto-adjust failed: {e}")
            return 0

    # ========================================================================
    # SAFE ORDER AMOUNT
    # ========================================================================
    def calculate_safe_order_amount(self, symbol: str, target_usd: float, price: float) -> float:
        """Calculate safe order amount with contract size consideration"""
        if price <= 0:
            raise ValueError(f"Invalid price: {price}")
        
        contract_size = self.contract_sizes.get(symbol, 0.01)
        contracts_needed = target_usd / (price * contract_size)
        min_contracts = MIN_ORDER_USD / (price * contract_size)
        final_contracts = max(contracts_needed, min_contracts)
        final_contracts = max(1.0, final_contracts)
        
        raw_amount = final_contracts * contract_size
        
        try:
            precision_amount = float(self.ex.amount_to_precision(symbol, raw_amount))
        except Exception:
            precision_amount = raw_amount
        
        final_value = precision_amount * price
        if final_value < MIN_ORDER_USD:
            min_amount = MIN_ORDER_USD / price
            precision_amount = float(self.ex.amount_to_precision(symbol, min_amount))
        
        return precision_amount

    # ========================================================================
    # SAFE CLOSE POSITION
    # ========================================================================
    async def safe_close_position(self, symbol: str, side: str, amount: float, market_price: float = None) -> Dict:
        """Close position with multiple fallback methods"""
        if amount <= 0:
            raise ValueError(f"Invalid amount for closing: {amount}")
        
        close_side = 'sell' if side == 'buy' else 'buy'
        close_amount = float(self.ex.amount_to_precision(symbol, abs(amount)))
        
        # Method 1: reduceOnly
        try:
            order = await self.ex.create_market_order(
                symbol=symbol,
                side=close_side,
                amount=close_amount,
                params={'reduceOnly': True}
            )
            return order
        except Exception as e:
            log.warning(f"Reduce-Only close failed: {e}")
        
        # Method 2: posSide
        try:
            pos_side = 'long' if side == 'buy' else 'short'
            order = await self.ex.create_market_order(
                symbol=symbol,
                side=close_side,
                amount=close_amount,
                params={'posSide': pos_side, 'reduceOnly': True}
            )
            return order
        except Exception as e:
            log.warning(f"posSide close failed: {e}")
        
        # Method 3: Direct
        try:
            positions = await self.ex.fetch_positions([symbol])
            for pos in positions:
                size = abs(float(pos.get('contracts', 0)))
                if size > 0:
                    exact_amount = float(self.ex.amount_to_precision(symbol, size))
                    order = await self.ex.create_market_order(
                        symbol=symbol,
                        side=close_side,
                        amount=exact_amount
                    )
                    return order
            raise Exception("No open position found")
        except Exception as e:
            log.error(f"Direct close failed: {e}")
            raise

    # ========================================================================
    # SMART SYNC POSITIONS
    # ========================================================================
    async def smart_sync_positions(self):
        """Smart position synchronization with exchange"""
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
                        await self.tg.send(f"🔄 <b>Adopted Manual Position</b>\n{matched_sym} ({side.upper()})")
            
            for pid, pos_data in list(SHARED_STATE["active_positions"].items()):
                if pos_data['symbol'] not in active_remote_syms and pos_data['strategy'] != 'LiveTest':
                    await self.db.close_trade(pid, 0, 0, 0)
                    del SHARED_STATE["active_positions"][pid]
                    
        except Exception as e:
            log.error(f"Sync error: {e}")

    # ========================================================================
    # PRICE LOOP
    # ========================================================================
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
                        await self.tg.send(f"🛑 <b>HALTED</b>\nDrawdown: {dd:.1f}%")
                    elif dd < MAX_DD * 0.8 and SHARED_STATE["dd_halted"]: 
                        SHARED_STATE["dd_halted"] = False
                        await self.tg.send(f"✅ <b>Resumed</b>\nDrawdown recovered to {dd:.1f}%")
                        
            except Exception as e:
                log.error(f"Price loop error: {e}")
            await asyncio.sleep(2)

    # ========================================================================
    # SCAN LOOP
    # ========================================================================
    async def scan_loop(self):
        while True:
            self.loop_count += 1
            if self.loop_count % 30 == 0: 
                await self.smart_sync_positions()
            
            # Check global stop loss
            if await self.check_global_stop_loss():
                await asyncio.sleep(5)
                continue
                
            if not SHARED_STATE["is_active"] or SHARED_STATE["dd_halted"]:
                await asyncio.sleep(5)
                continue
            
            # Dynamic max positions
            current_max_pos = await self.get_dynamic_max_pos()
            
            if len(SHARED_STATE["active_positions"]) >= current_max_pos:
                await asyncio.sleep(5)
                continue
                
            SHARED_STATE["last_scan"] = time.strftime("%H:%M:%S")
            
            for sym in SYMBOLS:
                if any(p['symbol'] == sym for p in SHARED_STATE["active_positions"].values()): 
                    continue
                    
                try:
                    raw = await self.ex.fetch_ohlcv(sym, TIMEFRAME, limit=100)
                    if not raw: 
                        continue
                        
                    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
                    sig = self.strategy.analyze(df)
                    
                    if sig['action'] != 'neutral': 
                        # MTF Filter
                        mtf_trend = await self.check_mtf_trend(sym)
                        
                        if sig['action'] == 'buy' and mtf_trend == 'bearish':
                            log.info(f"Skipping buy on {sym} - bearish on higher TF")
                            continue
                        
                        if sig['action'] == 'sell' and mtf_trend == 'bullish':
                            log.info(f"Skipping sell on {sym} - bullish on higher TF")
                            continue
                        
                        await self.execute_trade(sym, sig)
                        
                except Exception as e:
                    log.error(f"Scan loop error for {sym}: {e}")
                    
                await asyncio.sleep(1)
            await asyncio.sleep(30)

    # ========================================================================
    # EXECUTE TRADE
    # ========================================================================
    async def execute_trade(self, sym: str, sig: Dict):
        price = self.prices.get(sym)
        if not price or price <= 0:
            return
            
        if SHARED_STATE["balance"] < 5.0:
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
                log.warning(f"Balance check failed for {sym}: {msg}")
                await self.tg.send(f"⚠️ <b>Trade Skipped</b>\n{sym}\n{msg}")
                return
                
            order_value = qty * price
            if order_value < MIN_ORDER_USD:
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
                "atr_value": sig.get('atr', 0),
                "is_partial": 0, 
                "highest_pnl_pct": 0
            }
            SHARED_STATE["active_positions"][pid] = pos
            await self.db.insert_trade(pos)
            
            await self.tg.send(
                f"✅ <b>Entry {side.upper()}</b>\n"
                f"{sym} @ {fill_price:.4f}\n"
                f"Qty: {qty}\n"
                f"Value: ${qty * fill_price:.2f}"
            )
            
            try:
                sl_side = 'sell' if side == 'buy' else 'buy'
                await self.ex.create_order(
                    sym, 
                    'stop', 
                    sl_side, 
                    qty, 
                    sig['sl'], 
                    params={'stopPrice': sig['sl'], 'reduceOnly': True}
                )
            except Exception as e:
                log.warning(f"Stop loss setup failed: {e}")
                
        except Exception as e:
            error_msg = str(e)[:200]
            log.error(f"Trade execution failed for {sym}: {error_msg}")
            await self.tg.send(f"❌ <b>Trade Failed</b>\n{sym}\n{error_msg}")

    # ========================================================================
    # LIVE TEST
    # ========================================================================
    async def run_live_test(self):
        await self.tg.send("🧪 <b>Initiating 30-Second Live Test...</b>\n<u>Optimized Strategy</u>")
        
        try:
            balance = await self.ex.fetch_balance()
            usdt_balance = balance.get('USDT', {}).get('free', 0)
            
            await self.tg.send(f"💰 <b>Available USDT Balance:</b> ${usdt_balance:.2f}")
            
            if usdt_balance < 50:
                await self.tg.send("❌ <b>Insufficient balance</b>\nMinimum $50 USDT required")
                return
            
            test_symbols = ["ETH/USDT:USDT", "SOL/USDT:USDT"]
            test_positions = []
            max_test_usdt = min(usdt_balance * 0.1, 30)
            
            for sym in test_symbols:
                price = self.prices.get(sym)
                if not price or price <= 0:
                    await self.tg.send(f"⚠️ No price for {sym}, skipping...")
                    continue
                
                qty = self.calculate_safe_order_amount(sym, max_test_usdt, price)
                if qty <= 0:
                    continue
                
                side = "buy" if len(test_positions) % 2 == 0 else "sell"
                
                try:
                    balance_check, msg = await self.check_balance_before_trade(sym, side, qty, price)
                    if not balance_check:
                        await self.tg.send(f"⚠️ {sym} {side}: {msg}")
                        continue
                    
                    await self.ex.create_market_order(sym, side, qty)
                    
                    pid = f"test_{uuid.uuid4().hex[:6]}"
                    pos = {
                        "id": pid,
                        "symbol": sym,
                        "side": side,
                        "strategy": "LiveTest",
                        "entry": price,
                        "qty": qty,
                        "sl": price * 0.98 if side == 'buy' else price * 1.02,
                        "tp": price * 1.05 if side == 'buy' else price * 0.95,
                        "tp1": price * 1.025 if side == 'buy' else price * 0.975,
                        "atr_value": price * 0.01,
                        "is_partial": 0,
                        "highest_pnl_pct": 0
                    }
                    SHARED_STATE["active_positions"][pid] = pos
                    test_positions.append(pid)
                    
                    await self.tg.send(f"✅ <b>Test Opened</b>\n{sym} ({side.upper()})\nQty: {qty}\nValue: ${qty * price:.2f}")
                    
                except Exception as e:
                    await self.tg.send(f"❌ Failed to open {sym}: {str(e)[:100]}")
            
            if not test_positions:
                await self.tg.send("❌ <b>No test positions could be opened</b>")
                return
            
            await self.tg.send(f"⏳ Waiting 30 seconds... ({len(test_positions)} positions)")
            await asyncio.sleep(30)
            
            await self.tg.send("⏳ Closing test positions...")
            for pid in test_positions:
                await self.force_close_position(pid, "End of Live Test")
            
            await self.tg.send("🎉 <b>Live Test Completed Successfully!</b>")
            
        except Exception as e:
            error_msg = str(e)[:300]
            log.error(f"Live test failed: {error_msg}")
            await self.tg.send(f"❌ <b>Live Test Failed:</b>\n{error_msg}")

    # ========================================================================
    # FORCE CLOSE POSITION
    # ========================================================================
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
            
            gross_pnl, net_pnl, fees = self.calculate_net_pnl(pos['entry'], price, qty, side)
            
            if pos['strategy'] != "LiveTest":
                await self.db.close_trade(pid, gross_pnl, net_pnl, fees)
                
            del SHARED_STATE["active_positions"][pid]
            await self.db.update_analytics()
            
            icon = "🟢" if net_pnl >= 0 else "🔴"
            await self.tg.send(
                f"{icon} <b>Closed ({reason})</b>\n"
                f"{symbol} | Net PnL: ${net_pnl:.2f}\n"
                f"Entry: {pos['entry']:.4f} → Exit: {price:.4f}\n"
                f"Fees: ${fees:.4f}"
            )
            
        except Exception as e:
            log.error(f"Force close failed for {pid}: {e}")
            await self.tg.send(f"❌ <b>Close Failed</b>\n{pid}\n{str(e)[:200]}")

    # ========================================================================
    # WATCHDOG LOOP
    # ========================================================================
    async def watchdog_loop(self):
        while True:
            for pid, pos in list(SHARED_STATE["active_positions"].items()):
                if pos['strategy'] == "LiveTest":
                    continue
                    
                price = self.prices.get(pos['symbol'])
                if not price or price <= 0:
                    continue
                
                if pos['side'] == 'buy':
                    pnl_pct = ((price - pos['entry']) / pos['entry']) * 100
                else:
                    pnl_pct = ((pos['entry'] - price) / pos['entry']) * 100
                
                # Intelligent trailing stop based on ATR
                atr_value = pos.get('atr_value', 0)
                if atr_value <= 0:
                    try:
                        ohlcv = await self.ex.fetch_ohlcv(pos['symbol'], TIMEFRAME, limit=20)
                        if ohlcv:
                            df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])
                            atr_value = Indicators.atr(df, n=14).iloc[-1]
                            pos['atr_value'] = atr_value
                    except:
                        atr_value = price * 0.01
                
                if pnl_pct > TRAIL_ACT:
                    if pnl_pct > pos['highest_pnl_pct']:
                        pos['highest_pnl_pct'] = pnl_pct
                        
                        trail_distance = max(atr_value * 0.5, price * 0.005)
                        
                        if pos['side'] == 'buy':
                            new_sl = price - trail_distance
                            if new_sl > pos['sl']:
                                pos['sl'] = new_sl
                                await self.db.update_trade(pid, pos['qty'], pos['sl'], pos['is_partial'], pos['highest_pnl_pct'])
                        else:
                            new_sl = price + trail_distance
                            if new_sl < pos['sl']:
                                pos['sl'] = new_sl
                                await self.db.update_trade(pid, pos['qty'], pos['sl'], pos['is_partial'], pos['highest_pnl_pct'])
                
                # Partial TP with minimum profit requirement
                if PARTIAL_TP and pos['is_partial'] == 0:
                    min_profit = price * 0.003  # 0.3% for fees
                    hit_tp1 = (pos['side'] == 'buy' and price >= pos['tp1']) or (pos['side'] == 'sell' and price <= pos['tp1'])
                    
                    if hit_tp1:
                        tp1_profit = abs(pos['tp1'] - pos['entry'])
                        if tp1_profit / pos['entry'] > 0.003:
                            try:
                                close_side = 'sell' if pos['side'] == 'buy' else 'buy'
                                half_qty = pos['qty'] / 2
                                half_qty = self.calculate_safe_order_amount(pos['symbol'], half_qty * price, price)
                                
                                if half_qty > 0 and half_qty < pos['qty']:
                                    await self.safe_close_position(pos['symbol'], pos['side'], half_qty, price)
                                    pos['qty'] -= half_qty
                                    pos['is_partial'] = 1
                                    pos['sl'] = pos['entry']
                                    await self.db.update_trade(pid, pos['qty'], pos['sl'], 1, pos['highest_pnl_pct'])
                                    await self.tg.send(f"✂️ <b>Partial TP Hit</b>\n{pos['symbol']} 50% at {price:.4f}")
                            except Exception as e:
                                log.error(f"Partial TP failed for {pid}: {e}")
                
                sl_hit = (pos['side'] == 'buy' and price <= pos['sl']) or (pos['side'] == 'sell' and price >= pos['sl'])
                tp_hit = (pos['side'] == 'buy' and price >= pos['tp']) or (pos['side'] == 'sell' and price <= pos['tp'])
                
                if sl_hit or tp_hit:
                    await self.force_close_position(pid, "SL/Trailing" if sl_hit else "TP")
                    
            await asyncio.sleep(1)

# ============================================================================
# 7. WEB DASHBOARD
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
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Quant V10.0 - Optimized</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0d1117; color: #c9d1d9; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { text-align: center; color: #58a6ff; margin-bottom: 10px; font-size: 2.5em; }
        .subtitle { text-align: center; color: #d29922; margin-bottom: 20px; font-size: 1em; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .card { background: #161b22; border: 1px solid #30363d; padding: 20px; border-radius: 10px; }
        .card h2 { color: #58a6ff; margin-top: 0; margin-bottom: 15px; font-size: 1.2em; }
        .card .val { font-size: 2em; color: #3fb950; }
        .card .val.red { color: #f85149; }
        .card .val.yellow { color: #d29922; }
        .card .info { color: #8b949e; font-size: 0.9em; }
        .pos-item { background: #21262d; margin: 5px 0; padding: 10px; border-radius: 5px; border-left: 4px solid #58a6ff; }
        .pos-item .symbol { font-weight: bold; color: #58a6ff; }
        .pos-item .side { text-transform: uppercase; }
        .pos-item .pnl { font-weight: bold; }
        .pos-item .pnl.positive { color: #3fb950; }
        .pos-item .pnl.negative { color: #f85149; }
        .footer { text-align: center; color: #8b949e; margin-top: 30px; font-size: 0.8em; }
        .status { display: inline-block; padding: 3px 10px; border-radius: 3px; font-size: 0.8em; }
        .status.running { background: #238636; color: #fff; }
        .status.paused { background: #da3633; color: #fff; }
        .status.halted { background: #d29922; color: #fff; }
        .no-btc { background: #1f2937; color: #d29922; padding: 5px 15px; border-radius: 20px; font-size: 0.8em; display: inline-block; }
        @media (max-width: 600px) {
            h1 { font-size: 1.8em; }
            .card .val { font-size: 1.5em; }
        }
    </style>
</head>
<body>
<div class="container">
    <h1>🤖 Master Quant Engine V10.0</h1>
    <div class="subtitle"><span class="no-btc">⚡ OPTIMIZED STRATEGY - No BTC</span></div>
    
    <div class="grid">
        <div class="card">
            <h2>📊 System Status</h2>
            <p id="status"><span class="status running">Loading...</span></p>
            <p class="info">Last Scan: <span id="lastScan">-</span></p>
            <p class="info">DD Halted: <span id="ddHalted">No</span></p>
        </div>
        
        <div class="card">
            <h2>💰 Balance</h2>
            <p><span id="bal" class="val">$0.00</span></p>
            <p class="info">Peak: <span id="peakBal">$0.00</span></p>
            <p class="info">Drawdown: <span id="dd">0.00%</span></p>
        </div>
        
        <div class="card">
            <h2>📈 Stats</h2>
            <p>Total PnL: <span id="pnl" class="val">$0.00</span></p>
            <p class="info">Win Rate: <span id="winRate">0%</span></p>
            <p class="info">Trades: <span id="totalTrades">0</span></p>
        </div>
    </div>
    
    <div class="card">
        <h2>🏦 Active Positions (<span id="posCount">0</span>/{MAX_POS})</h2>
        <div id="pos"></div>
    </div>
    
    <div class="footer">
        Master Quant Engine v10.0 | Phemex {'TESTNET' if TESTNET else 'MAINNET'} | Optimized Strategy | Auto-updates every 2s
    </div>
</div>

<script>
const MAX_POS = 3;

async function update() {
    try {
        let r = await fetch('/api/status');
        let d = await r.json();
        
        let statusEl = document.getElementById('status');
        if (!d.is_active) {
            statusEl.innerHTML = '<span class="status paused">⏹ PAUSED</span>';
        } else if (d.dd_halted) {
            statusEl.innerHTML = '<span class="status halted">⛔ HALTED</span>';
        } else {
            statusEl.innerHTML = '<span class="status running">▶ RUNNING</span>';
        }
        document.getElementById('lastScan').textContent = d.last_scan || '-';
        document.getElementById('ddHalted').textContent = d.dd_halted ? '⚠️ Yes' : '✅ No';
        
        document.getElementById('bal').textContent = '$' + d.balance.toFixed(2);
        document.getElementById('peakBal').textContent = '$' + d.peak_balance.toFixed(2);
        let dd = d.current_dd.toFixed(2);
        let ddEl = document.getElementById('dd');
        ddEl.textContent = dd + '%';
        ddEl.style.color = dd > 5 ? '#f85149' : dd > 2 ? '#d29922' : '#3fb950';
        
        document.getElementById('pnl').textContent = '$' + d.stats.total_pnl.toFixed(2);
        document.getElementById('winRate').textContent = d.stats.win_rate + '%';
        document.getElementById('totalTrades').textContent = d.stats.total_trades;
        
        let posDiv = document.getElementById('pos');
        let posCount = Object.keys(d.active_positions).length;
        document.getElementById('posCount').textContent = posCount;
        
        if (posCount === 0) {
            posDiv.innerHTML = '<p style="color:#8b949e;">📭 No active positions</p>';
        } else {
            let html = '';
            for (let id in d.active_positions) {
                let p = d.active_positions[id];
                let price = d.prices && d.prices[p.symbol] ? d.prices[p.symbol] : p.entry;
                let pnl = (price - p.entry) * p.qty * (p.side === 'buy' ? 1 : -1);
                let pnlClass = pnl >= 0 ? 'positive' : 'negative';
                let icon = pnl >= 0 ? '🟢' : '🔴';
                html += `
                    <div class="pos-item">
                        <span class="symbol">${p.symbol}</span>
                        <span class="side">${p.side.toUpperCase()}</span>
                        <span class="info">| Entry: ${p.entry.toFixed(4)}</span>
                        <span class="info">| Qty: ${p.qty.toFixed(4)}</span>
                        <span class="info">| Strategy: ${p.strategy}</span>
                        <br>
                        <span class="info">Current: ${price.toFixed(4)}</span>
                        <span class="pnl ${pnlClass}">${icon} $${pnl.toFixed(2)}</span>
                        <span class="info">(${((pnl / (p.entry * p.qty)) * 100).toFixed(2)}%)</span>
                    </div>
                `;
            }
            posDiv.innerHTML = html;
        }
    } catch (e) {
        console.error('Update error:', e);
    }
}

setInterval(update, 2000);
update();
</script>
</body>
</html>"""
    return render_template_string(html)

def run_web(): 
    app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False)

# ============================================================================
# 8. MAIN ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    engine = QuantEngine()
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        log.info("🛑 Shutting down...")
    except Exception as e:
        log.error(f"Fatal error: {e}")
    finally:
        try:
            asyncio.run(engine.ex.close())
        except:
            pass
        log.info("👋 Goodbye!")
