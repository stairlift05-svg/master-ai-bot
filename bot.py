#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v15.0 (Full Phemex Native Edition)
═══════════════════════════════════════════════════════════════════
تغییرات اصلی نسبت به v14.0:

🔴 معماری:
  1. حذف کامل وابستگی به Binance
  2. تمام OHLCV از Phemex Testnet مستقیم
  3. Smart Price Validation بدون نیاز به منبع ثانویه
  4. WebSocket-ready price feed از Phemex

📈 گسترش:
  5. 10 جفت‌ارز (از 4 به 10)
  6. MAX_POS از 3 به 10
  7. Dynamic Position Sizing per symbol

📊 گزارش:
  8. گزارش TXT فوق‌کامل (تشخیص ایرادات استراتژی + عملیاتی)
  9. آنالیز Slippage، Fee Impact، Hold Time
  10. Signal Quality Score
  11. Strategy Performance Matrix
  12. Operational Health Report
═══════════════════════════════════════════════════════════════════
"""

import asyncio
import logging
import os
import time
import uuid
import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from threading import Thread, Lock
from typing import Dict, List, Any, Optional, Tuple

import aiohttp
import aiosqlite
import ccxt.async_support as ccxt
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string

# ============================================================================
# 1. CONFIGURATION
# ============================================================================
load_dotenv()

API_KEY    = os.getenv("PHEMEX_API_KEY", "")
API_SECRET = os.getenv("PHEMEX_API_SECRET", "")
TESTNET    = os.getenv("PHEMEX_TESTNET", "True").lower() in ("true", "1", "yes")
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT    = os.getenv("TELEGRAM_CHAT_ID", "")

# ─── 10 جفت‌ارز از Phemex ────────────────────────────────────────────────
SYMBOLS = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "BNB/USDT:USDT",
    "XRP/USDT:USDT",
    "ADA/USDT:USDT",
    "LTC/USDT:USDT",
    "LINK/USDT:USDT",
    "DOGE/USDT:USDT",
    "MATIC/USDT:USDT",
    "AVAX/USDT:USDT",
]

# ─── Per-Symbol Config (بدون Binance) ────────────────────────────────────
SYMBOL_CONFIG = {
    "BTC/USDT:USDT":   {
        "min_atr_pct":   0.05,   # حداقل ATR به درصد قیمت
        "max_atr_pct":   3.0,    # حداکثر ATR
        "min_vol_mult":  1.1,    # حداقل ضریب حجم
        "weight":        1.5,    # وزن در پورتفولیو
        "max_usd_pos":   500.0,  # حداکثر پوزیشن دلاری
        "tick_check":    True,   # چک کردن tick size
    },
    "ETH/USDT:USDT":   {
        "min_atr_pct": 0.08, "max_atr_pct": 4.0,
        "min_vol_mult": 1.1, "weight": 1.3, "max_usd_pos": 300.0,
        "tick_check": True,
    },
    "BNB/USDT:USDT":   {
        "min_atr_pct": 0.1,  "max_atr_pct": 4.5,
        "min_vol_mult": 1.1, "weight": 1.0, "max_usd_pos": 200.0,
        "tick_check": True,
    },
    "XRP/USDT:USDT":   {
        "min_atr_pct": 0.2,  "max_atr_pct": 6.0,
        "min_vol_mult": 1.15,"weight": 0.9, "max_usd_pos": 150.0,
        "tick_check": True,
    },
    "ADA/USDT:USDT":   {
        "min_atr_pct": 0.3,  "max_atr_pct": 7.0,
        "min_vol_mult": 1.15,"weight": 0.9, "max_usd_pos": 150.0,
        "tick_check": True,
    },
    "LTC/USDT:USDT":   {
        "min_atr_pct": 0.15, "max_atr_pct": 5.0,
        "min_vol_mult": 1.2, "weight": 0.85,"max_usd_pos": 150.0,
        "tick_check": True,
    },
    "LINK/USDT:USDT":  {
        "min_atr_pct": 0.2,  "max_atr_pct": 6.0,
        "min_vol_mult": 1.2, "weight": 0.85,"max_usd_pos": 150.0,
        "tick_check": True,
    },
    "DOGE/USDT:USDT":  {
        "min_atr_pct": 0.4,  "max_atr_pct": 8.0,
        "min_vol_mult": 1.3, "weight": 0.7, "max_usd_pos": 100.0,
        "tick_check": True,
    },
    "MATIC/USDT:USDT": {
        "min_atr_pct": 0.4,  "max_atr_pct": 8.0,
        "min_vol_mult": 1.3, "weight": 0.7, "max_usd_pos": 100.0,
        "tick_check": True,
    },
    "AVAX/USDT:USDT":  {
        "min_atr_pct": 0.2,  "max_atr_pct": 6.0,
        "min_vol_mult": 1.2, "weight": 0.85,"max_usd_pos": 150.0,
        "tick_check": True,
    },
}

# ─── Strategy Params (بهینه RR) ──────────────────────────────────────────
STRATEGY_PARAMS = {
    "Breakout_Momentum":   {"sl_m": 1.0, "tp_m": 3.5, "tp1_m": 1.8, "min_rr": 2.5},
    "MTF_Pullback":        {"sl_m": 1.2, "tp_m": 2.8, "tp1_m": 1.4, "min_rr": 2.0},
    "SuperTrend_Pullback": {"sl_m": 1.0, "tp_m": 2.5, "tp1_m": 1.3, "min_rr": 2.0},
    "Volume_Surge":        {"sl_m": 1.1, "tp_m": 2.2, "tp1_m": 1.2, "min_rr": 1.8},
    "EMA_Cross":           {"sl_m": 1.3, "tp_m": 3.0, "tp1_m": 1.6, "min_rr": 2.0},
}

# ─── Risk Management ─────────────────────────────────────────────────────
TIMEFRAME            = "5m"
HTF_TIMEFRAME        = "1h"
RISK_PCT             = 0.5       # درصد ریسک هر معامله از کل بالانس
LEVERAGE             = 5
MAX_POS              = 10        # حداکثر معاملات همزمان
MAX_DD               = 10.0      # حداکثر Drawdown مجاز
MAX_DAILY_LOSS       = 5.0       # حداکثر ضرر روزانه
MIN_ORDER_USD        = 16.0
MAX_EXPOSURE_PCT     = 80.0      # حداکثر کل exposure
MAX_SINGLE_EXPOSURE  = 15.0      # حداکثر exposure هر نماد
TAKER_FEE            = 0.0006
FEE_BUFFER           = 1.2
TRAIL_ACT            = 1.8
TRAIL_STEP           = 0.6
PARTIAL_TP           = True
RELAXED_MODE         = True
TEST_SYMBOL          = "ADA/USDT:USDT"
TEST_USD             = 12.0

# ─── Rate Limit & Timing ─────────────────────────────────────────────────
SCAN_INTERVAL        = 30        # ثانیه بین دورهای اسکن
SYMBOL_DELAY         = 1.0       # ثانیه بین نمادها
PRICE_LOOP_INTERVAL  = 5         # ثانیه بین آپدیت قیمت

# ─── Circuit Breaker ──────────────────────────────────────────────────────
CONSECUTIVE_LOSS_LIMIT = 3
SYMBOL_COOLDOWN_HOURS  = 2
MAX_ERROR_COOLDOWN     = 1800
MAX_SYMBOL_ERRORS      = 5

# ─── Phemex Testnet URLs ─────────────────────────────────────────────────
PHEMEX_TESTNET_URL     = "https://testnet-api.phemex.com"
PHEMEX_WS_TESTNET      = "wss://testnet-api.phemex.com/ws"

# ─── Logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler("quant_v15.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("QuantV15.0")

# ─── Shared State ────────────────────────────────────────────────────────
SHARED_STATE: Dict[str, Any] = {
    "is_active":           True,
    "dd_halted":           False,
    "daily_halted":        False,
    "balance":             0.0,
    "free_balance":        0.0,
    "peak_balance":        0.0,
    "day_start_balance":   0.0,
    "current_dd":          0.0,
    "daily_pnl":           0.0,
    "active_positions":    {},
    "last_scan":           "Never",
    "scan_count":          0,
    "signal_count":        0,
    "rejected_count":      0,
    "consecutive_losses":  {},
    "symbol_cooldowns":    {},
    "symbol_errors":       {},
    "data_source":         "Phemex Native",
    "phemex_status":       "connecting",
    "stats": {
        "total_trades":    0,
        "winning_trades":  0,
        "losing_trades":   0,
        "win_rate":        0.0,
        "total_pnl":       0.0,
        "total_fees":      0.0,
        "avg_hold_min":    0.0,
        "avg_pnl_per_trade": 0.0,
        "max_win":         0.0,
        "max_loss":        0.0,
        "profit_factor":   0.0,
        "sharpe_approx":   0.0,
        "by_symbol":       {},
        "by_strategy":     {},
        "by_hour":         {},
    },
    "operational": {
        "total_api_errors":   0,
        "position_mode_fixes": 0,
        "circuit_breaker_events": 0,
        "sync_count":         0,
        "uptime_start":       time.time(),
    },
    "version": "15.0",
}
STATE_LOCK = Lock()


# ============================================================================
# 2. PHEMEX NATIVE DATA FEED (جایگزین Binance)
# ============================================================================
class PhemexDataFeed:
    """
    دریافت مستقیم همه داده‌ها از Phemex Testnet
    بدون هیچ وابستگی به Binance
    
    روش کار:
    - OHLCV: از Phemex REST API مستقیم
    - Price: از Phemex Ticker
    - Validation: Internal Consistency Check
    """

    def __init__(self, exchange: ccxt.phemex):
        self.ex            = exchange
        self.price_cache:  Dict[str, float]  = {}
        self.ohlcv_cache:  Dict[str, list]   = {}
        self.cache_time:   Dict[str, float]  = {}
        self.cache_ttl     = 4               # ثانیه - TTL کش
        self.error_count:  Dict[str, int]    = defaultdict(int)
        self.last_good_price: Dict[str, float] = {}

    async def fetch_ticker_price(self, symbol: str) -> Optional[float]:
        """دریافت آخرین قیمت از Phemex"""
        try:
            ticker = await self.ex.fetch_ticker(symbol)
            price  = float(ticker.get("last") or ticker.get("close") or 0)
            if price > 0:
                self.last_good_price[symbol] = price
                self.error_count[symbol]     = 0
                return price
        except Exception as e:
            self.error_count[symbol] += 1
            log.warning(f"Ticker {symbol}: {e}")
        return None

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 120
    ) -> Optional[pd.DataFrame]:
        """
        دریافت OHLCV از Phemex با:
        - کش هوشمند
        - Validation داخلی (بدون مقایسه با Binance)
        - Fallback به کش قبلی
        """
        cache_key = f"{symbol}_{timeframe}"
        now       = time.time()

        # چک کش
        if (cache_key in self.ohlcv_cache and
                now - self.cache_time.get(cache_key, 0) < self.cache_ttl):
            return self.ohlcv_cache[cache_key]

        try:
            raw = await self.ex.fetch_ohlcv(
                symbol, timeframe=timeframe, limit=limit)

            if not raw or len(raw) < 40:
                log.warning(f"OHLCV کم {symbol}/{timeframe}: {len(raw) if raw else 0}")
                return self.ohlcv_cache.get(cache_key)

            df = pd.DataFrame(
                raw,
                columns=["ts", "open", "high", "low", "close", "volume"]
            )

            # ─── Validation داخلی (بدون نیاز به Binance) ──────────────
            if not self._validate_ohlcv(df, symbol):
                log.warning(f"OHLCV Invalid: {symbol}")
                return self.ohlcv_cache.get(cache_key)

            # آپدیت کش قیمت از آخرین کندل
            last_close = float(df["close"].iloc[-1])
            if last_close > 0:
                self.price_cache[symbol]      = last_close
                self.last_good_price[symbol]  = last_close

            # ذخیره در کش
            self.ohlcv_cache[cache_key] = df
            self.cache_time[cache_key]  = now
            self.error_count[symbol]    = 0

            return df

        except Exception as e:
            self.error_count[symbol] += 1
            log.error(f"OHLCV {symbol}/{timeframe}: {e}")

            # Fallback به کش
            cached = self.ohlcv_cache.get(cache_key)
            if cached is not None:
                log.info(f"Fallback به کش برای {symbol}")
            return cached

    def _validate_ohlcv(self, df: pd.DataFrame, symbol: str) -> bool:
        """
        اعتبارسنجی داخلی OHLCV بدون نیاز به منبع خارجی
        
        چک‌ها:
        1. High >= Low همیشه
        2. قیمت‌های منفی نباشند
        3. حجم منفی نباشد
        4. تغییرات غیرطبیعی (>50% در یک کندل)
        5. کندل‌های تکراری متوالی
        6. Timestamp به ترتیب باشد
        """
        try:
            if len(df) < 10:
                return False

            # چک 1: High >= Low
            if not (df["high"] >= df["low"]).all():
                log.warning(f"❌ {symbol}: High < Low یافت شد")
                return False

            # چک 2: قیمت‌های مثبت
            if (df[["open", "high", "low", "close"]] <= 0).any().any():
                log.warning(f"❌ {symbol}: قیمت صفر یا منفی")
                return False

            # چک 3: حجم
            if (df["volume"] < 0).any():
                log.warning(f"❌ {symbol}: حجم منفی")
                return False

            # چک 4: تغییرات غیرطبیعی (spike detection)
            pct_change = df["close"].pct_change().abs()
            if (pct_change > 0.5).any():
                spike_idx = pct_change[pct_change > 0.5].index.tolist()
                log.warning(
                    f"⚠️ {symbol}: تغییر >50% در کندل‌های {spike_idx}")
                # فقط warning - داده را رد نمی‌کنیم

            # چک 5: کندل‌های تکراری (frozen market)
            last_5 = df["close"].iloc[-5:]
            if last_5.nunique() == 1:
                log.warning(f"❌ {symbol}: 5 کندل یکسان - frozen market")
                return False

            # چک 6: Timestamp ترتیب صعودی
            if not df["ts"].is_monotonic_increasing:
                log.warning(f"❌ {symbol}: Timestamp ترتیب اشتباه")
                return False

            return True

        except Exception as e:
            log.error(f"Validate OHLCV {symbol}: {e}")
            return False

    def get_validated_price(self, symbol: str) -> Optional[float]:
        """
        دریافت قیمت معتبر از کش
        Internal validation بدون منبع خارجی
        """
        price = self.price_cache.get(symbol)
        if not price or price <= 0:
            return self.last_good_price.get(symbol)

        # چک منطقی قیمت با آخرین قیمت خوب
        last_good = self.last_good_price.get(symbol)
        if last_good and last_good > 0:
            ratio = price / last_good
            if ratio > 3.0 or ratio < 0.33:
                log.error(
                    f"💀 {symbol}: قیمت غیرمنطقی! "
                    f"فعلی={price:.6f} آخرین‌خوب={last_good:.6f}")
                return last_good  # برگشت به آخرین قیمت خوب

        return price

    async def fetch_all_tickers(self) -> Dict[str, float]:
        """دریافت همه قیمت‌ها با یک request"""
        prices = {}
        try:
            tickers = await self.ex.fetch_tickers(SYMBOLS)
            for sym, tick in tickers.items():
                p = float(tick.get("last") or tick.get("close") or 0)
                if p > 0:
                    prices[sym]                  = p
                    self.price_cache[sym]         = p
                    self.last_good_price[sym]     = p
        except Exception as e:
            log.error(f"fetch_all_tickers: {e}")
        return prices

    async def get_market_depth_spread(
        self, symbol: str
    ) -> Tuple[float, float]:
        """
        دریافت Spread از Order Book Phemex
        جایگزین Price Diff با Binance
        برگشت: (spread_pct, mid_price)
        """
        try:
            ob     = await self.ex.fetch_order_book(symbol, limit=5)
            bids   = ob.get("bids", [])
            asks   = ob.get("asks", [])
            if bids and asks:
                best_bid   = float(bids[0][0])
                best_ask   = float(asks[0][0])
                mid        = (best_bid + best_ask) / 2
                spread_pct = (best_ask - best_bid) / mid * 100
                return spread_pct, mid
        except Exception as e:
            log.debug(f"OrderBook {symbol}: {e}")
        return 999.0, 0.0


# ============================================================================
# 3. DATABASE (ارتقا یافته)
# ============================================================================
class Database:
    def __init__(self, path="bot_v15.db"):
        self.path = path

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            # جدول اصلی معاملات
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    symbol TEXT, side TEXT, strategy TEXT,
                    entry_price REAL, qty REAL, original_qty REAL,
                    sl REAL, tp1 REAL, tp REAL,
                    is_partial INTEGER DEFAULT 0,
                    highest_pnl_pct REAL DEFAULT 0,
                    status TEXT DEFAULT 'open',
                    pnl REAL DEFAULT 0,
                    fees_est REAL DEFAULT 0,
                    net_pnl REAL DEFAULT 0,
                    exit_price REAL DEFAULT 0,
                    slippage_est REAL DEFAULT 0,
                    exit_reason TEXT,
                    hold_seconds REAL DEFAULT 0,
                    expected_rr REAL DEFAULT 0,
                    actual_rr REAL DEFAULT 0,
                    signal_quality REAL DEFAULT 0,
                    rsi_at_entry REAL DEFAULT 0,
                    atr_at_entry REAL DEFAULT 0,
                    htf_trend TEXT DEFAULT '',
                    opened_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    closed_at TEXT
                )""")

            # جدول تصمیمات
            await db.execute("""
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT, action TEXT, strategy TEXT,
                    reason TEXT, price REAL,
                    rsi REAL, atr REAL, htf_trend TEXT,
                    signal_quality REAL DEFAULT 0,
                    spread_pct REAL DEFAULT 0,
                    extra TEXT
                )""")

            # جدول Equity
            await db.execute("""
                CREATE TABLE IF NOT EXISTS equity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    balance REAL, free REAL,
                    peak REAL, dd REAL,
                    open_pos INTEGER DEFAULT 0
                )""")

            # جدول Circuit Breaker
            await db.execute("""
                CREATE TABLE IF NOT EXISTS circuit_breaker_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT, event_type TEXT, detail TEXT
                )""")

            # جدول خطاهای عملیاتی
            await db.execute("""
                CREATE TABLE IF NOT EXISTS operational_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    error_type TEXT,
                    symbol TEXT,
                    message TEXT,
                    resolved INTEGER DEFAULT 0
                )""")

            # جدول آمار اسکن
            await db.execute("""
                CREATE TABLE IF NOT EXISTS scan_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbols_scanned INTEGER,
                    signals_found INTEGER,
                    signals_executed INTEGER,
                    rejected_price_diff INTEGER,
                    rejected_no_signal INTEGER,
                    rejected_circuit INTEGER,
                    scan_duration_ms REAL
                )""")

            await db.commit()

    # ─── Trade Operations ─────────────────────────────────────────────────
    async def insert_trade(self, t: dict):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO trades
                (id,symbol,side,strategy,entry_price,qty,original_qty,
                 sl,tp1,tp,expected_rr,signal_quality,
                 rsi_at_entry,atr_at_entry,htf_trend)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t["id"], t["symbol"], t["side"], t["strategy"],
                 t["entry"], t["qty"], t["qty"],
                 t["sl"], t["tp1"], t["tp"],
                 t.get("expected_rr", 0),
                 t.get("signal_quality", 0),
                 t.get("rsi_at_entry", 0),
                 t.get("atr_at_entry", 0),
                 t.get("htf_trend", "")))
            await db.commit()

    async def update_trade(self, tid, qty, sl, partial, hp):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """UPDATE trades
                   SET qty=?,sl=?,is_partial=?,highest_pnl_pct=?
                   WHERE id=?""",
                (qty, sl, partial, hp, tid))
            await db.commit()

    async def close_trade(self, tid, pnl, fees=0.0, reason="",
                           hold=0.0, exit_price=0.0, actual_rr=0.0,
                           slippage=0.0):
        net = pnl - fees
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                UPDATE trades SET
                    status='closed', pnl=?, fees_est=?, net_pnl=?,
                    exit_price=?, slippage_est=?, actual_rr=?,
                    exit_reason=?, hold_seconds=?,
                    closed_at=CURRENT_TIMESTAMP
                WHERE id=?""",
                (pnl, fees, net, exit_price, slippage,
                 actual_rr, reason, hold, tid))
            await db.commit()

    async def log_decision(self, symbol, action, strategy, reason,
                            price=0, rsi=0, atr=0, htf="",
                            signal_quality=0, spread_pct=0, extra=""):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO decisions
                (symbol,action,strategy,reason,price,rsi,atr,
                 htf_trend,signal_quality,spread_pct,extra)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, action, strategy, reason, price,
                 rsi, atr, htf, signal_quality, spread_pct,
                 str(extra)[:400]))
            await db.commit()

    async def log_circuit_breaker(self, symbol, event_type, detail):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """INSERT INTO circuit_breaker_log
                   (symbol,event_type,detail) VALUES (?,?,?)""",
                (symbol, event_type, detail[:300]))
            await db.commit()

    async def log_operational_error(self, error_type, symbol, message):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """INSERT INTO operational_errors
                   (error_type,symbol,message) VALUES (?,?,?)""",
                (error_type, symbol, message[:400]))
            await db.commit()
        with STATE_LOCK:
            SHARED_STATE["operational"]["total_api_errors"] += 1

    async def log_scan_stats(self, scanned, found, executed,
                              rej_price, rej_signal, rej_circuit,
                              duration_ms):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO scan_stats
                (symbols_scanned,signals_found,signals_executed,
                 rejected_price_diff,rejected_no_signal,
                 rejected_circuit,scan_duration_ms)
                VALUES (?,?,?,?,?,?,?)""",
                (scanned, found, executed, rej_price,
                 rej_signal, rej_circuit, duration_ms))
            await db.commit()

    async def log_equity(self, balance, free, peak, dd, open_pos):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """INSERT INTO equity
                   (balance,free,peak,dd,open_pos)
                   VALUES (?,?,?,?,?)""",
                (balance, free, peak, dd, open_pos))
            await db.commit()

    # ─── Queries ──────────────────────────────────────────────────────────
    async def get_open_trades(self) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM trades WHERE status='open'") as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_closed_trades(self, limit=200) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM trades WHERE status='closed'
                ORDER BY closed_at DESC LIMIT ?""", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_recent_decisions(self, limit=500) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM decisions
                ORDER BY id DESC LIMIT ?""", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_operational_errors(self, limit=50) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM operational_errors
                ORDER BY id DESC LIMIT ?""", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_scan_stats(self, limit=100) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM scan_stats
                ORDER BY id DESC LIMIT ?""", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_equity_history(self, limit=200) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM equity
                ORDER BY id DESC LIMIT ?""", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def update_analytics(self):
        """آپدیت کامل آمار"""
        async with aiosqlite.connect(self.path) as db:
            async with db.execute("""
                SELECT symbol, strategy, side, pnl, fees_est,
                       hold_seconds, expected_rr, actual_rr,
                       entry_price, exit_price, slippage_est,
                       rsi_at_entry, opened_at
                FROM trades WHERE status='closed'""") as c:
                rows = await c.fetchall()

        if not rows:
            return

        pnls      = [r[3] for r in rows]
        fees      = [r[4] for r in rows]
        holds     = [r[5] for r in rows if r[5] and r[5] > 0]
        exp_rrs   = [r[6] for r in rows if r[6] and r[6] > 0]
        act_rrs   = [r[7] for r in rows if r[7] and r[7] > 0]
        slippages = [r[10] for r in rows if r[10] and r[10] != 0]

        wins    = [p for p in pnls if p > 0]
        losses  = [p for p in pnls if p < 0]

        # آمار per symbol
        by_symbol   = defaultdict(list)
        by_strategy = defaultdict(list)
        by_hour     = defaultdict(list)

        for r in rows:
            sym, strat, side, pnl = r[0], r[1], r[2], r[3]
            opened_at = r[12] or ""
            by_symbol[sym].append(pnl)
            by_strategy[strat].append(pnl)
            try:
                hr = datetime.fromisoformat(
                    opened_at.replace("Z", "")).hour
                by_hour[str(hr)].append(pnl)
            except Exception:
                pass

        def calc_stats(pnl_list):
            if not pnl_list:
                return {}
            w = [p for p in pnl_list if p > 0]
            l = [p for p in pnl_list if p < 0]
            gross_profit = sum(w) if w else 0
            gross_loss   = abs(sum(l)) if l else 0
            pf = gross_profit / gross_loss if gross_loss > 0 else 999.0
            return {
                "trades":  len(pnl_list),
                "wins":    len(w),
                "losses":  len(l),
                "pnl":     round(sum(pnl_list), 3),
                "wr":      round(len(w) / len(pnl_list) * 100, 1),
                "avg_win": round(sum(w) / len(w), 3) if w else 0,
                "avg_loss":round(sum(l) / len(l), 3) if l else 0,
                "pf":      round(pf, 2),
            }

        # Profit Factor کل
        gross_profit  = sum(wins)  if wins   else 0
        gross_loss    = abs(sum(losses)) if losses else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 999.0

        # Sharpe تقریبی
        if len(pnls) > 1:
            mean_pnl = statistics.mean(pnls)
            std_pnl  = statistics.stdev(pnls)
            sharpe   = mean_pnl / std_pnl if std_pnl > 0 else 0
        else:
            sharpe = 0

        with STATE_LOCK:
            SHARED_STATE["stats"] = {
                "total_trades":      len(pnls),
                "winning_trades":    len(wins),
                "losing_trades":     len(losses),
                "win_rate":          round(len(wins)/len(pnls)*100, 1),
                "total_pnl":         round(sum(pnls), 3),
                "total_fees":        round(sum(fees), 3),
                "net_pnl":           round(sum(pnls) - sum(fees), 3),
                "avg_hold_min":      round(
                    sum(holds)/len(holds)/60, 1) if holds else 0,
                "avg_pnl_per_trade": round(
                    sum(pnls)/len(pnls), 3),
                "max_win":           round(max(pnls), 3),
                "max_loss":          round(min(pnls), 3),
                "profit_factor":     round(profit_factor, 2),
                "sharpe_approx":     round(sharpe, 3),
                "avg_expected_rr":   round(
                    sum(exp_rrs)/len(exp_rrs), 2) if exp_rrs else 0,
                "avg_actual_rr":     round(
                    sum(act_rrs)/len(act_rrs), 2) if act_rrs else 0,
                "avg_slippage":      round(
                    sum(slippages)/len(slippages), 5) if slippages else 0,
                "by_symbol":         {s: calc_stats(v)
                                      for s, v in by_symbol.items()},
                "by_strategy":       {s: calc_stats(v)
                                      for s, v in by_strategy.items()},
                "by_hour":           {h: calc_stats(v)
                                      for h, v in by_hour.items()},
            }

    # ─── گزارش TXT فوق کامل ──────────────────────────────────────────────
    async def generate_full_report(self) -> str:
        decisions  = await self.get_recent_decisions(500)
        closed     = await self.get_closed_trades(200)
        open_trades = await self.get_open_trades()
        op_errors   = await self.get_operational_errors(50)
        scan_stats  = await self.get_scan_stats(50)
        equity_hist = await self.get_equity_history(100)

        lines = []
        W     = 70
        sep   = "═" * W
        sep2  = "─" * W

        def title(text):
            lines.append("")
            lines.append(sep)
            lines.append(f"  {text}")
            lines.append(sep)

        def section(text):
            lines.append("")
            lines.append(f"  ▶ {text}")
            lines.append(sep2)

        def row(label, value, indent=4):
            lines.append(f"{'':>{indent}}{label:<30}: {value}")

        # ══════════════════════════════════════════════════════
        lines.append(sep)
        lines.append(" " * 10 + "MASTER QUANT ENGINE v15.0")
        lines.append(" " * 8 + "AI Think Tank | Full Diagnostic Report")
        lines.append(
            f" " * 8 +
            f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        lines.append(sep)

        with STATE_LOCK:
            st = dict(SHARED_STATE)
            op = dict(st.get("operational", {}))
            stats = dict(st.get("stats", {}))

        uptime_sec = time.time() - op.get("uptime_start", time.time())
        uptime_str = str(timedelta(seconds=int(uptime_sec)))

        # ══ بخش 1: خلاصه اجرایی ═══════════════════════════════
        title("📋 SECTION 1: EXECUTIVE SUMMARY")

        row("نسخه ربات",        "v15.0 | Phemex Native")
        row("منبع داده",         st.get("data_source", "Phemex"))
        row("وضعیت Phemex",      st.get("phemex_status", "?"))
        row("Uptime",            uptime_str)
        row("تعداد نمادها",      str(len(SYMBOLS)))
        row("حداکثر پوزیشن",    str(MAX_POS))
        row("Testnet",           str(TESTNET))
        lines.append("")

        row("موجودی فعلی",       f"${st.get('balance', 0):.2f}")
        row("موجودی آزاد",       f"${st.get('free_balance', 0):.2f}")
        row("Peak Balance",      f"${st.get('peak_balance', 0):.2f}")
        row("Drawdown فعلی",     f"{st.get('current_dd', 0):.2f}%")
        row("PnL روزانه",        f"${st.get('daily_pnl', 0):.2f}")
        row("پوزیشن باز",        str(len(st.get('active_positions', {}))))
        lines.append("")

        row("کل معاملات",        str(stats.get('total_trades', 0)))
        row("معاملات برنده",     str(stats.get('winning_trades', 0)))
        row("معاملات بازنده",    str(stats.get('losing_trades', 0)))
        row("Win Rate",          f"{stats.get('win_rate', 0):.1f}%")
        row("کل PnL (Gross)",    f"${stats.get('total_pnl', 0):.3f}")
        row("کل کارمزد",         f"${stats.get('total_fees', 0):.3f}")
        row("Net PnL",           f"${stats.get('net_pnl', 0):.3f}")
        row("Profit Factor",     str(stats.get('profit_factor', 0)))
        row("Sharpe (تقریبی)",   str(stats.get('sharpe_approx', 0)))
        row("بزرگ‌ترین سود",     f"${stats.get('max_win', 0):.3f}")
        row("بزرگ‌ترین ضرر",     f"${stats.get('max_loss', 0):.3f}")
        row("میانگین Hold",      f"{stats.get('avg_hold_min', 0):.1f} دقیقه")
        row("میانگین PnL/معامله",f"${stats.get('avg_pnl_per_trade', 0):.3f}")
        row("RR مورد انتظار",    str(stats.get('avg_expected_rr', 0)))
        row("RR واقعی",          str(stats.get('avg_actual_rr', 0)))
        row("Slippage متوسط",    f"{stats.get('avg_slippage', 0):.5f}")

        # ══ بخش 2: ارزیابی سلامت استراتژی ════════════════════
        title("🧠 SECTION 2: STRATEGY HEALTH ANALYSIS")

        total_t   = stats.get('total_trades', 0)
        wr        = stats.get('win_rate', 0)
        pf        = stats.get('profit_factor', 0)
        exp_rr    = stats.get('avg_expected_rr', 0)
        act_rr    = stats.get('avg_actual_rr', 0)

        # ─── تشخیص ایرادات استراتژی ─────────────────────────────
        section("تشخیص خودکار ایرادات استراتژی")

        strategy_issues = []
        strategy_warnings = []
        strategy_ok = []

        # 1. Win Rate
        if total_t >= 10:
            be_wr = 100 / (1 + exp_rr) if exp_rr > 0 else 50
            if wr < be_wr:
                strategy_issues.append(
                    f"❌ Win Rate ({wr}%) زیر Breakeven ({be_wr:.1f}%) است\n"
                    f"       → استراتژی در بلندمدت زیانده است\n"
                    f"       → راه‌حل: سخت‌گیرتر کردن فیلترهای ورود")
            elif wr < 40:
                strategy_warnings.append(
                    f"⚠️ Win Rate ({wr}%) پایین است\n"
                    f"       → نیاز به بررسی شرایط ورود")
            else:
                strategy_ok.append(f"✅ Win Rate ({wr}%) قابل قبول")
        else:
            strategy_warnings.append(
                f"⚠️ تعداد معاملات ({total_t}) ناکافی برای ارزیابی دقیق")

        # 2. RR Slippage
        if exp_rr > 0 and act_rr > 0:
            rr_decay = (exp_rr - act_rr) / exp_rr * 100
            if rr_decay > 30:
                strategy_issues.append(
                    f"❌ RR واقعی ({act_rr:.2f}) vs انتظار ({exp_rr:.2f})\n"
                    f"       افت RR: {rr_decay:.1f}%\n"
                    f"       → TP خیلی دور است یا معاملات زود بسته می‌شوند")
            elif rr_decay > 15:
                strategy_warnings.append(
                    f"⚠️ افت RR: {rr_decay:.1f}% (انتظار {exp_rr:.2f} → واقعی {act_rr:.2f})")
            else:
                strategy_ok.append(f"✅ RR واقعی ({act_rr:.2f}) نزدیک به انتظار ({exp_rr:.2f})")

        # 3. Profit Factor
        if total_t >= 5:
            if pf < 1.0:
                strategy_issues.append(
                    f"❌ Profit Factor ({pf}) زیر 1.0\n"
                    f"       → ضررها بیش از سودها هستند\n"
                    f"       → ربات به صورت سیستماتیک زیان می‌دهد")
            elif pf < 1.3:
                strategy_warnings.append(
                    f"⚠️ Profit Factor ({pf}) پایین (هدف: >1.5)")
            elif pf >= 2.0:
                strategy_ok.append(f"✅ Profit Factor عالی ({pf})")
            else:
                strategy_ok.append(f"✅ Profit Factor قابل قبول ({pf})")

        # 4. Consecutive Losses Pattern
        if closed:
            consec = 0
            max_consec = 0
            for t in closed:
                if t["pnl"] < 0:
                    consec += 1
                    max_consec = max(max_consec, consec)
                else:
                    consec = 0
            if max_consec >= 5:
                strategy_issues.append(
                    f"❌ بیشترین ضرر متوالی: {max_consec}\n"
                    f"       → احتمال مشکل سیستماتیک در یک بازه زمانی")
            elif max_consec >= 3:
                strategy_warnings.append(
                    f"⚠️ بیشترین ضرر متوالی: {max_consec}")
            else:
                strategy_ok.append(
                    f"✅ بیشترین ضرر متوالی: {max_consec} (قابل قبول)")

        # 5. Hold Time Analysis
        avg_hold  = stats.get('avg_hold_min', 0)
        if avg_hold > 0:
            if avg_hold < 5:
                strategy_warnings.append(
                    f"⚠️ Hold Time متوسط ({avg_hold:.1f}min) خیلی کوتاه\n"
                    f"       → ممکن است SL خیلی نزدیک باشد")
            elif avg_hold > 480:
                strategy_warnings.append(
                    f"⚠️ Hold Time متوسط ({avg_hold:.1f}min) خیلی طولانی\n"
                    f"       → ممکن است TP خیلی دور باشد")
            else:
                strategy_ok.append(
                    f"✅ Hold Time متوسط: {avg_hold:.1f} دقیقه")

        # نمایش نتایج
        if strategy_issues:
            lines.append("")
            lines.append("  🔴 ایرادات جدی:")
            for issue in strategy_issues:
                lines.append(f"     {issue}")

        if strategy_warnings:
            lines.append("")
            lines.append("  🟡 هشدارها:")
            for warn in strategy_warnings:
                lines.append(f"     {warn}")

        if strategy_ok:
            lines.append("")
            lines.append("  🟢 موارد سالم:")
            for ok in strategy_ok:
                lines.append(f"     {ok}")

        # ─── آمار per-strategy ────────────────────────────────────
        section("عملکرد هر استراتژی")
        by_strat = stats.get("by_strategy", {})
        if by_strat:
            lines.append(
                f"  {'استراتژی':<22} {'معاملات':>8} "
                f"{'WR%':>7} {'PnL':>10} {'PF':>6} "
                f"{'AvgW':>8} {'AvgL':>8}")
            lines.append("  " + "─" * 68)
            for strat, sv in sorted(
                by_strat.items(), key=lambda x: x[1].get("pnl",0), reverse=True
            ):
                lines.append(
                    f"  {strat:<22} {sv['trades']:>8} "
                    f"{sv.get('wr',0):>6.1f}% "
                    f"{sv.get('pnl',0):>+9.3f} "
                    f"{sv.get('pf',0):>6.2f} "
                    f"{sv.get('avg_win',0):>+7.3f} "
                    f"{sv.get('avg_loss',0):>+7.3f}")
        else:
            lines.append("  هنوز داده کافی نیست")

        # ─── آمار per-symbol ─────────────────────────────────────
        section("عملکرد هر نماد")
        by_sym = stats.get("by_symbol", {})
        if by_sym:
            lines.append(
                f"  {'نماد':<20} {'معاملات':>8} "
                f"{'WR%':>7} {'PnL':>10} {'PF':>6}")
            lines.append("  " + "─" * 55)
            for sym, sv in sorted(
                by_sym.items(), key=lambda x: x[1].get("pnl",0), reverse=True
            ):
                base = sym.split("/")[0]
                status_icon = "🔴" if sv.get('pnl',0) < 0 else "🟢"
                lines.append(
                    f"  {status_icon} {base:<18} {sv['trades']:>8} "
                    f"{sv.get('wr',0):>6.1f}% "
                    f"{sv.get('pnl',0):>+9.3f} "
                    f"{sv.get('pf',0):>6.2f}")
        else:
            lines.append("  هنوز داده کافی نیست")

        # ─── بهترین ساعات معامله ─────────────────────────────────
        by_hour = stats.get("by_hour", {})
        if len(by_hour) >= 3:
            section("بهترین ساعات معامله (UTC)")
            sorted_hours = sorted(
                by_hour.items(),
                key=lambda x: x[1].get("pnl", 0), reverse=True)
            lines.append(
                f"  {'ساعت (UTC)':<12} {'معاملات':>8} "
                f"{'WR%':>7} {'PnL':>10}")
            lines.append("  " + "─" * 40)
            for hr, sv in sorted_hours[:8]:
                icon = "🟢" if sv.get('pnl',0) >= 0 else "🔴"
                lines.append(
                    f"  {icon} {hr:0>2}:00{'':<6} "
                    f"{sv['trades']:>8} "
                    f"{sv.get('wr',0):>6.1f}% "
                    f"{sv.get('pnl',0):>+9.3f}")

        # ══ بخش 3: سلامت عملیاتی ══════════════════════════════
        title("⚙️ SECTION 3: OPERATIONAL HEALTH REPORT")

        section("آمار عملیاتی کلی")
        row("کل خطاهای API",       str(op.get('total_api_errors', 0)))
        row("Position Mode Fix",   str(op.get('position_mode_fixes', 0)))
        row("Circuit Breaker Events", str(op.get('circuit_breaker_events', 0)))
        row("تعداد Sync",          str(op.get('sync_count', 0)))
        row("تعداد اسکن",          str(st.get('scan_count', 0)))
        row("کل سیگنال",           str(st.get('signal_count', 0)))
        row("کل رد شده",           str(st.get('rejected_count', 0)))

        # ─── آمار اسکن ───────────────────────────────────────────
        if scan_stats:
            section("آمار اسکن‌های اخیر (50 اسکن آخر)")
            total_scanned  = sum(s.get("symbols_scanned", 0) for s in scan_stats)
            total_found    = sum(s.get("signals_found", 0) for s in scan_stats)
            total_exec     = sum(s.get("signals_executed", 0) for s in scan_stats)
            total_rej_pr   = sum(s.get("rejected_price_diff", 0) for s in scan_stats)
            total_rej_sig  = sum(s.get("rejected_no_signal", 0) for s in scan_stats)
            total_rej_cb   = sum(s.get("rejected_circuit", 0) for s in scan_stats)
            avg_dur        = statistics.mean(
                [s.get("scan_duration_ms", 0) for s in scan_stats])

            row("کل نمادهای اسکن‌شده",  str(total_scanned))
            row("کل سیگنال یافت‌شده",   str(total_found))
            row("کل اجراشده",           str(total_exec))
            row("رد - اختلاف قیمت",     str(total_rej_pr))
            row("رد - بدون سیگنال",      str(total_rej_sig))
            row("رد - Circuit Breaker",  str(total_rej_cb))
            row("میانگین زمان اسکن",    f"{avg_dur:.0f}ms")

            if total_found > 0:
                exec_rate = total_exec / total_found * 100
                row("نرخ اجرا از سیگنال",
                    f"{exec_rate:.1f}%")
                if exec_rate < 20:
                    lines.append(
                        "\n  ⚠️  نرخ اجرای پایین: اکثر سیگنال‌ها رد می‌شوند")
                    lines.append(
                        "     احتمالاً محدودیت‌های ریسک یا Circuit Breaker فعال است")

        # ─── Circuit Breaker Status ───────────────────────────────
        section("وضعیت Circuit Breaker")
        now = time.time()
        with STATE_LOCK:
            cds  = dict(SHARED_STATE["symbol_cooldowns"])
            errs = dict(SHARED_STATE["symbol_errors"])
            cls  = dict(SHARED_STATE["consecutive_losses"])

        active_cds = {s: v for s, v in cds.items() if v > now}
        if active_cds:
            lines.append("  🔴 Cooldown های فعال:")
            for sym, end_ts in active_cds.items():
                rem = int((end_ts - now) / 60)
                lines.append(f"     ⛔ {sym}: {rem} دقیقه باقی‌مانده")
        else:
            lines.append("  ✅ هیچ Cooldown فعالی نیست")

        lines.append("")
        lines.append("  📊 ضررهای متوالی:")
        if cls:
            for sym, v in cls.items():
                last = datetime.fromtimestamp(
                    v.get('last_loss', 0)).strftime('%H:%M') \
                    if v.get('last_loss') else "?"
                lines.append(
                    f"     {sym}: {v['count']} ضرر "
                    f"(آخرین: {last})")
        else:
            lines.append("  ✅ بدون ضرر متوالی")

        # ─── خطاهای عملیاتی اخیر ────────────────────────────────
        if op_errors:
            section("آخرین خطاهای عملیاتی")
            for err in op_errors[:15]:
                ts_str = err.get("ts", "")[:16]
                lines.append(
                    f"  [{ts_str}] {err.get('error_type','?'):<20} "
                    f"{err.get('symbol',''):<18} "
                    f"{err.get('message','')[:50]}")

        # ─── تشخیص ایرادات عملیاتی ───────────────────────────────
        section("تشخیص خودکار ایرادات عملیاتی")

        op_issues   = []
        op_warnings = []
        op_ok       = []

        total_api_err = op.get('total_api_errors', 0)
        scan_cnt      = st.get('scan_count', 0)

        if scan_cnt > 0:
            err_rate = total_api_err / scan_cnt
            if err_rate > 0.5:
                op_issues.append(
                    f"❌ نرخ خطای API بالا: {err_rate:.2f} خطا/اسکن\n"
                    f"       → مشکل اتصال یا Rate Limit\n"
                    f"       → راه‌حل: افزایش SYMBOL_DELAY")
            elif err_rate > 0.1:
                op_warnings.append(
                    f"⚠️ نرخ خطای API: {err_rate:.2f} خطا/اسکن")
            else:
                op_ok.append(f"✅ نرخ خطای API: {err_rate:.3f} (قابل قبول)")

        pm_fixes = op.get('position_mode_fixes', 0)
        if pm_fixes > 3:
            op_issues.append(
                f"❌ {pm_fixes} بار Position Mode Fix شد\n"
                f"       → مشکل مکرر با Hedge Mode\n"
                f"       → راه‌حل: تنظیم دستی در پنل Phemex")
        elif pm_fixes > 0:
            op_warnings.append(f"⚠️ {pm_fixes} بار Position Mode Fix")
        else:
            op_ok.append("✅ Position Mode مشکلی نداشته")

        cb_events = op.get('circuit_breaker_events', 0)
        if cb_events > 10:
            op_issues.append(
                f"❌ {cb_events} رویداد Circuit Breaker\n"
                f"       → نمادها مکرراً ضرر متوالی می‌دهند\n"
                f"       → راه‌حل: بررسی شرایط بازار")
        elif cb_events > 3:
            op_warnings.append(f"⚠️ {cb_events} رویداد Circuit Breaker")
        else:
            op_ok.append(f"✅ Circuit Breaker Events: {cb_events}")

        if op_issues:
            lines.append("\n  🔴 ایرادات عملیاتی:")
            for issue in op_issues:
                lines.append(f"     {issue}")
        if op_warnings:
            lines.append("\n  🟡 هشدارهای عملیاتی:")
            for warn in op_warnings:
                lines.append(f"     {warn}")
        if op_ok:
            lines.append("\n  🟢 موارد سالم:")
            for ok in op_ok:
                lines.append(f"     {ok}")

        # ══ بخش 4: معاملات باز ════════════════════════════════
        title("🟢 SECTION 4: OPEN POSITIONS")
        if open_trades:
            lines.append(
                f"  {'نماد':<20} {'جهت':<6} "
                f"{'ورود':>10} {'Qty':>10} "
                f"{'SL':>10} {'TP':>10} "
                f"{'استراتژی':<22}")
            lines.append("  " + "─" * 90)
            for t in open_trades:
                lines.append(
                    f"  {t['symbol']:<20} "
                    f"{t['side'].upper():<6} "
                    f"{t['entry_price']:>10.5f} "
                    f"{t['qty']:>10.4f} "
                    f"{t['sl']:>10.5f} "
                    f"{t['tp']:>10.5f} "
                    f"{t.get('strategy',''):<22}")
        else:
            lines.append("  هیچ پوزیشن بازی ندارد")

        # ══ بخش 5: معاملات بسته ════════════════════════════════
        title("📈 SECTION 5: CLOSED TRADES (Last 50)")
        if closed:
            lines.append(
                f"  {'نماد':<20} {'جهت':<6} "
                f"{'ورود':>10} {'خروج':>10} "
                f"{'PnL':>9} {'RR':>6} "
                f"{'Hold':>8} {'علت':<15} {'استراتژی'}")
            lines.append("  " + "─" * 100)
            for t in closed[:50]:
                emoji  = "✅" if t["pnl"] > 0 else "❌"
                hold   = int((t.get("hold_seconds", 0) or 0) / 60)
                ep     = t.get("exit_price", 0) or 0
                act_rr = t.get("actual_rr", 0) or 0
                lines.append(
                    f"  {emoji} {t['symbol']:<18} "
                    f"{t['side']:<6} "
                    f"{t['entry_price']:>10.5f} "
                    f"{ep:>10.5f} "
                    f"{t['pnl']:>+8.3f} "
                    f"{act_rr:>6.2f} "
                    f"{hold:>7}m "
                    f"{(t.get('exit_reason','') or ''):<15} "
                    f"{t.get('strategy','')}")
        else:
            lines.append("  هنوز معامله‌ای بسته نشده")

        # ══ بخش 6: تحلیل سیگنال‌ها ════════════════════════════
        title("🔍 SECTION 6: SIGNAL & DECISION ANALYSIS")

        if decisions:
            section("خلاصه تصمیمات")
            actions  = Counter(d["action"] for d in decisions)
            signals  = sum(v for k, v in actions.items() if k != "neutral")
            rejected = actions.get("neutral", 0)
            total_d  = len(decisions)

            row("کل تصمیمات",        str(total_d))
            row("سیگنال ورود",        str(signals))
            row("رد شده",            str(rejected))
            row("نرخ سیگنال",        f"{signals/total_d*100:.1f}%")

            section("دلایل رد شدن")
            reasons = Counter()
            for d in decisions:
                if d["action"] == "neutral":
                    r = (d.get("reason") or "Unknown")
                    # گروه‌بندی دلایل مشابه
                    if "اختلاف قیمت" in r or "spread" in r.lower():
                        reasons["Spread/Price Diff بالا"] += 1
                    elif "سیگنال" in r or "RSI" in r:
                        reasons["بدون سیگنال واضح"] += 1
                    elif "نوسان" in r or "ATR" in r:
                        reasons["نوسان نامناسب"] += 1
                    elif "روند" in r or "HTF" in r:
                        reasons["روند نامشخص"] += 1
                    elif "داده" in r:
                        reasons["داده ناکافی"] += 1
                    elif "Circuit" in r or "Cooldown" in r:
                        reasons["Circuit Breaker"] += 1
                    else:
                        reasons[r[:40]] += 1

            for reason, cnt in reasons.most_common(10):
                pct = cnt / rejected * 100 if rejected > 0 else 0
                bar = "█" * int(pct / 5)
                lines.append(
                    f"  {cnt:>5} ({pct:>5.1f}%)  "
                    f"{reason:<35} {bar}")

            section("آمار سیگنال‌ها per نماد")
            sym_signals  = defaultdict(lambda: {"signals": 0, "neutral": 0})
            for d in decisions:
                s = d.get("symbol", "?")
                if d["action"] != "neutral":
                    sym_signals[s]["signals"] += 1
                else:
                    sym_signals[s]["neutral"] += 1

            lines.append(
                f"  {'نماد':<20} {'سیگنال':>8} "
                f"{'رد':>8} {'نرخ%':>8}")
            lines.append("  " + "─" * 46)
            for sym in SYMBOLS:
                sv = sym_signals.get(sym, {"signals": 0, "neutral": 0})
                total_sym = sv["signals"] + sv["neutral"]
                rate = sv["signals"] / total_sym * 100 if total_sym > 0 else 0
                base = sym.split("/")[0]
                lines.append(
                    f"  {base:<20} {sv['signals']:>8} "
                    f"{sv['neutral']:>8} {rate:>7.1f}%")

            section("15 تصمیم آخر")
            for d in decisions[:15]:
                icon  = "✅" if d["action"] != "neutral" else "⛔"
                ts    = (d.get("ts") or "")[:16]
                rsi   = d.get("rsi", 0) or 0
                lines.append(
                    f"  {icon} [{ts}] "
                    f"{d.get('symbol',''):<20} "
                    f"RSI:{rsi:>5.1f}  "
                    f"{(d.get('reason',''))[:50]}")

        # ══ بخش 7: Equity Curve ════════════════════════════════
        title("📉 SECTION 7: EQUITY ANALYSIS")
        if equity_hist and len(equity_hist) >= 5:
            section("تاریخچه موجودی (20 نقطه آخر)")
            recent_eq = equity_hist[:20]
            for eq in recent_eq:
                ts  = (eq.get("ts") or "")[:16]
                bal = eq.get("balance", 0) or 0
                dd  = eq.get("dd", 0) or 0
                op  = eq.get("open_pos", 0) or 0
                bar = "▓" * int(dd * 2) if dd > 0 else ""
                lines.append(
                    f"  [{ts}] "
                    f"${bal:>8.2f}  "
                    f"DD:{dd:>5.2f}%  "
                    f"Pos:{op}  "
                    f"{bar}")

            # آنالیز Equity
            balances = [e.get("balance", 0) or 0 for e in equity_hist]
            dds      = [e.get("dd", 0) or 0 for e in equity_hist]

            if balances:
                section("آماره‌های Equity")
                row("Min Balance",   f"${min(balances):.2f}")
                row("Max Balance",   f"${max(balances):.2f}")
                row("Max DD مشاهده‌شده", f"{max(dds):.2f}%")
                row("میانگین DD",    f"{sum(dds)/len(dds):.2f}%")

                # بررسی روند
                if len(balances) >= 10:
                    first_half  = sum(balances[:len(balances)//2]) / (len(balances)//2)
                    second_half = sum(balances[len(balances)//2:]) / (len(balances) - len(balances)//2)
                    if second_half > first_half * 1.01:
                        lines.append("\n  📈 روند Equity: صعودی ✅")
                    elif second_half < first_half * 0.99:
                        lines.append("\n  📉 روند Equity: نزولی ⚠️")
                    else:
                        lines.append("\n  📊 روند Equity: خنثی")

        # ══ بخش 8: توصیه‌های نهایی ════════════════════════════
        title("💡 SECTION 8: RECOMMENDATIONS")

        all_issues = strategy_issues + op_issues
        all_warns  = strategy_warnings + op_warnings

        if not all_issues and not all_warns:
            lines.append("  ✅ ربات در وضعیت سالم است")
            lines.append("  ✅ ادامه عملیات توصیه می‌شود")
        else:
            if all_issues:
                lines.append("  🔴 اقدامات فوری لازم:")
                for i, issue in enumerate(all_issues, 1):
                    first_line = issue.split('\n')[0]
                    lines.append(f"     {i}. {first_line}")
                lines.append("")

            if all_warns:
                lines.append("  🟡 بررسی و بهینه‌سازی:")
                for i, warn in enumerate(all_warns, 1):
                    lines.append(f"     {i}. {warn}")
                lines.append("")

            lines.append("  📌 پارامترهای فعلی:")
            row("RISK_PCT",          f"{RISK_PCT}%")
            row("LEVERAGE",          str(LEVERAGE))
            row("MAX_POS",           str(MAX_POS))
            row("TRAIL_ACT",         f"{TRAIL_ACT}%")
            row("CONSECUTIVE_LOSS_LIMIT", str(CONSECUTIVE_LOSS_LIMIT))

        # Footer
        lines.append("")
        lines.append(sep)
        lines.append(
            f"  گزارش تهیه شده در: "
            f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        lines.append(
            f"  Master Quant v15.0 | Phemex Native | AI Think Tank")
        lines.append(sep)

        return "\n".join(lines)


# ============================================================================
# 4. INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(series: pd.Series, n=14) -> pd.Series:
        delta   = series.diff()
        up      = delta.clip(lower=0)
        down    = -delta.clip(upper=0)
        ma_up   = up.ewm(com=n - 1, adjust=False).mean()
        ma_down = down.ewm(com=n - 1, adjust=False).mean()
        rs      = ma_up / ma_down.replace(0, 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def atr(df: pd.DataFrame, n=14) -> pd.Series:
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"]  - df["close"].shift()).abs(),
        ], axis=1).max(axis=1)
        return tr.ewm(com=n - 1, adjust=False).mean()

    @staticmethod
    def ema(series: pd.Series, span: int) -> pd.Series:
        return series.ewm(span=span, adjust=False).mean()

    @staticmethod
    def sma(series: pd.Series, period: int) -> pd.Series:
        return series.rolling(period).mean()

    @staticmethod
    def supertrend(df: pd.DataFrame, period=10, mult=3.0):
        atr_s  = Indicators.atr(df, period)
        hl2    = (df["high"] + df["low"]) / 2
        upper  = hl2 + mult * atr_s
        lower  = hl2 - mult * atr_s
        direction = pd.Series(1, index=df.index)
        for i in range(1, len(df)):
            if df["close"].iloc[i] > upper.iloc[i - 1]:
                direction.iloc[i] = 1
            elif df["close"].iloc[i] < lower.iloc[i - 1]:
                direction.iloc[i] = -1
            else:
                direction.iloc[i] = direction.iloc[i - 1]
                if direction.iloc[i] == 1 and lower.iloc[i] < lower.iloc[i-1]:
                    lower.iloc[i] = lower.iloc[i-1]
                if direction.iloc[i] == -1 and upper.iloc[i] > upper.iloc[i-1]:
                    upper.iloc[i] = upper.iloc[i-1]
        return direction, upper, lower

    @staticmethod
    def macd(series: pd.Series, fast=12, slow=26, sig=9):
        fast_ema = series.ewm(span=fast, adjust=False).mean()
        slow_ema = series.ewm(span=slow, adjust=False).mean()
        macd_line   = fast_ema - slow_ema
        signal_line = macd_line.ewm(span=sig, adjust=False).mean()
        histogram   = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def bollinger(series: pd.Series, period=20, std=2.0):
        mid   = series.rolling(period).mean()
        sigma = series.rolling(period).std()
        upper = mid + std * sigma
        lower = mid - std * sigma
        return upper, mid, lower

    @staticmethod
    def highest(s, p): return s.rolling(p).max()
    @staticmethod
    def lowest(s, p):  return s.rolling(p).min()

    @staticmethod
    def signal_quality(rsi, atr, atr_sma, volume, vol_sma,
                        htf_strength) -> float:
        """
        امتیاز کیفیت سیگنال 0-100
        """
        score = 50.0

        # RSI در ناحیه ایده‌آل
        if 45 <= rsi <= 65:
            score += 10
        elif 40 <= rsi <= 70:
            score += 5
        elif rsi < 30 or rsi > 80:
            score -= 15

        # ATR در محدوده مناسب
        if atr_sma > 0:
            atr_ratio = atr / atr_sma
            if 0.8 <= atr_ratio <= 1.5:
                score += 10
            elif atr_ratio > 2.0 or atr_ratio < 0.5:
                score -= 10

        # Volume
        if vol_sma > 0:
            vol_ratio = volume / vol_sma
            if vol_ratio > 1.5:
                score += 15
            elif vol_ratio > 1.2:
                score += 8
            elif vol_ratio < 0.8:
                score -= 10

        # HTF Strength
        score += htf_strength * 10  # -1 to 1

        return max(0, min(100, score))


# ============================================================================
# 5. STRATEGY ENGINE (ارتقا یافته)
# ============================================================================
class StrategyEngine:

    def analyze(
        self,
        df_5m: pd.DataFrame,
        df_1h: pd.DataFrame,
        symbol: str
    ) -> dict:
        df  = df_5m.iloc[:-1].copy()
        htf = df_1h.iloc[:-1].copy()

        if len(df) < 60 or len(htf) < 40:
            return self._neutral("داده ناکافی")

        # ─── HTF Trend با قدرت‌سنجی ──────────────────────────────────
        hclose   = htf["close"]
        e20h     = Indicators.ema(hclose, 20).iloc[-1]
        e50h     = Indicators.ema(hclose, 50).iloc[-1]
        e200h    = Indicators.ema(hclose, min(200, len(htf))).iloc[-1]
        hp       = float(hclose.iloc[-1])

        # قدرت روند HTF: -1 تا +1
        htf_strength = 0.0
        if hp > e20h:  htf_strength += 0.3
        if hp > e50h:  htf_strength += 0.3
        if e20h > e50h: htf_strength += 0.2
        if e50h > e200h * 0.995: htf_strength += 0.2
        htf_strength -= 1.0  # نرمال‌سازی به -1..1 نسبی

        if hp > e50h and e50h > e200h * 0.998:
            htf_trend = "bullish"
            htf_strength = abs(htf_strength)
        elif hp < e50h and e50h < e200h * 1.002:
            htf_trend = "bearish"
            htf_strength = abs(htf_strength)
        else:
            return self._neutral("روند HTF نامشخص", htf="sideways")

        # ─── اندیکاتورهای اصلی ───────────────────────────────────────
        c      = df["close"]
        high   = df["high"]
        low    = df["low"]
        vol    = df["volume"]
        price  = float(c.iloc[-1])

        atr_s  = Indicators.atr(df, 14)
        atr    = float(atr_s.iloc[-1])
        if atr <= 0:
            return self._neutral("ATR صفر", htf=htf_trend)

        atr_sma  = float(Indicators.sma(atr_s, 20).iloc[-1])
        cfg      = SYMBOL_CONFIG.get(symbol, {})
        min_atr  = price * cfg.get("min_atr_pct", 0.05) / 100
        max_atr  = price * cfg.get("max_atr_pct", 5.0) / 100

        if atr < min_atr or atr > max_atr:
            return self._neutral(
                f"ATR خارج از محدوده ({atr:.5f})",
                atr=atr, htf=htf_trend)

        rsi_s   = Indicators.rsi(c)
        rsi     = float(rsi_s.iloc[-1])
        rsi_p   = float(rsi_s.iloc[-2])

        ema20   = float(Indicators.ema(c, 20).iloc[-1])
        ema50   = float(Indicators.ema(c, 50).iloc[-1])
        ema200  = float(Indicators.ema(c, min(200, len(c))).iloc[-1])

        st_d, st_u, st_l = Indicators.supertrend(df)

        macd_l, macd_sig, macd_hist = Indicators.macd(c)
        macd_cur  = float(macd_l.iloc[-1])
        macd_prev = float(macd_l.iloc[-2])
        macd_h    = float(macd_hist.iloc[-1])

        bb_up, bb_mid, bb_low = Indicators.bollinger(c)
        bb_upper = float(bb_up.iloc[-1])
        bb_lower = float(bb_low.iloc[-1])

        vsma = float(Indicators.sma(vol, 20).iloc[-1])
        vcur = float(vol.iloc[-1])
        h10  = float(Indicators.highest(high, 10).iloc[-1])
        l10  = float(Indicators.lowest(low, 10).iloc[-1])
        h20  = float(Indicators.highest(high, 20).iloc[-1])
        l20  = float(Indicators.lowest(low, 20).iloc[-1])

        min_vol = cfg.get("min_vol_mult", 1.1)

        # ─── Strategy 1: Breakout Momentum ───────────────────────────
        if (htf_trend == "bullish" and
                price > ema20 and price >= h10 * 0.999 and
                48 < rsi < 75 and vcur > vsma * min_vol and
                macd_h > 0):
            sq = Indicators.signal_quality(
                rsi, atr, atr_sma, vcur, vsma, htf_strength)
            return self._build(
                "buy", "Breakout_Momentum",
                price, atr, rsi, htf_trend, sq)

        if (htf_trend == "bearish" and
                price < ema20 and price <= l10 * 1.001 and
                25 < rsi < 52 and vcur > vsma * min_vol and
                macd_h < 0):
            sq = Indicators.signal_quality(
                rsi, atr, atr_sma, vcur, vsma, htf_strength)
            return self._build(
                "sell", "Breakout_Momentum",
                price, atr, rsi, htf_trend, sq)

        # ─── Strategy 2: MTF Pullback ─────────────────────────────────
        if (htf_trend == "bullish" and
                price > ema20 > ema50 * 0.999 and
                rsi_p <= 42 and rsi > rsi_p and rsi < 62 and
                c.iloc[-1] > c.iloc[-2]):
            sq = Indicators.signal_quality(
                rsi, atr, atr_sma, vcur, vsma, htf_strength)
            return self._build(
                "buy", "MTF_Pullback",
                price, atr, rsi, htf_trend, sq)

        if (htf_trend == "bearish" and
                price < ema20 < ema50 * 1.001 and
                rsi_p >= 58 and rsi < rsi_p and rsi > 38 and
                c.iloc[-1] < c.iloc[-2]):
            sq = Indicators.signal_quality(
                rsi, atr, atr_sma, vcur, vsma, htf_strength)
            return self._build(
                "sell", "MTF_Pullback",
                price, atr, rsi, htf_trend, sq)

        # ─── Strategy 3: SuperTrend Pullback ─────────────────────────
        if (htf_trend == "bullish" and
                st_d.iloc[-1] == 1 and
                low.iloc[-1] <= st_l.iloc[-1] * 1.005 and
                c.iloc[-1] > c.iloc[-2] and
                38 < rsi < 65):
            sq = Indicators.signal_quality(
                rsi, atr, atr_sma, vcur, vsma, htf_strength)
            return self._build(
                "buy", "SuperTrend_Pullback",
                price, atr, rsi, htf_trend, sq)

        if (htf_trend == "bearish" and
                st_d.iloc[-1] == -1 and
                high.iloc[-1] >= st_u.iloc[-1] * 0.995 and
                c.iloc[-1] < c.iloc[-2] and
                35 < rsi < 62):
            sq = Indicators.signal_quality(
                rsi, atr, atr_sma, vcur, vsma, htf_strength)
            return self._build(
                "sell", "SuperTrend_Pullback",
                price, atr, rsi, htf_trend, sq)

        # ─── Strategy 4: Volume Surge ────────────────────────────────
        if (htf_trend == "bullish" and
                price > ema20 and vcur > vsma * 1.5 and
                c.iloc[-1] > c.iloc[-2] and
                48 < rsi < 70 and price > bb_mid):
            sq = Indicators.signal_quality(
                rsi, atr, atr_sma, vcur, vsma, htf_strength)
            return self._build(
                "buy", "Volume_Surge",
                price, atr, rsi, htf_trend, sq)

        if (htf_trend == "bearish" and
                price < ema20 and vcur > vsma * 1.5 and
                c.iloc[-1] < c.iloc[-2] and
                30 < rsi < 52 and price < bb_mid):
            sq = Indicators.signal_quality(
                rsi, atr, atr_sma, vcur, vsma, htf_strength)
            return self._build(
                "sell", "Volume_Surge",
                price, atr, rsi, htf_trend, sq)

        # ─── Strategy 5: EMA Cross ───────────────────────────────────
        ema20_prev = float(Indicators.ema(c, 20).iloc[-2])
        ema50_prev = float(Indicators.ema(c, 50).iloc[-2])
        cross_up   = ema20_prev <= ema50_prev and ema20 > ema50
        cross_down = ema20_prev >= ema50_prev and ema20 < ema50

        if (htf_trend == "bullish" and cross_up and
                rsi > 45 and vcur > vsma * 0.9):
            sq = Indicators.signal_quality(
                rsi, atr, atr_sma, vcur, vsma, htf_strength)
            return self._build(
                "buy", "EMA_Cross",
                price, atr, rsi, htf_trend, sq)

        if (htf_trend == "bearish" and cross_down and
                rsi < 55 and vcur > vsma * 0.9):
            sq = Indicators.signal_quality(
                rsi, atr, atr_sma, vcur, vsma, htf_strength)
            return self._build(
                "sell", "EMA_Cross",
                price, atr, rsi, htf_trend, sq)

        return self._neutral(
            f"بدون سیگنال (RSI={rsi:.1f}, HTF={htf_trend})",
            rsi=rsi, atr=atr, htf=htf_trend)

    def _neutral(self, reason, rsi=0, atr=0, htf="") -> dict:
        return {
            "action": "neutral", "reason": reason,
            "strat": "", "rsi": rsi, "atr": atr,
            "htf": htf, "signal_quality": 0,
        }

    def _build(self, side, strat, price, atr, rsi, htf, sq=50) -> dict:
        p = STRATEGY_PARAMS.get(
            strat, {"sl_m": 1.5, "tp_m": 2.8, "tp1_m": 1.4, "min_rr": 2.0})
        sl_m, tp_m, tp1_m = p["sl_m"], p["tp_m"], p["tp1_m"]
        expected_rr       = round(tp_m / sl_m, 2)

        if side == "buy":
            return {
                "action": "buy", "strat": strat,
                "sl":  price - atr * sl_m,
                "tp":  price + atr * tp_m,
                "tp1": price + atr * tp1_m,
                "reason": f"سیگنال {strat} (Q:{sq:.0f})",
                "rsi": rsi, "atr": atr, "htf": htf,
                "expected_rr":    expected_rr,
                "signal_quality": sq,
            }
        return {
            "action": "sell", "strat": strat,
            "sl":  price + atr * sl_m,
            "tp":  price - atr * tp_m,
            "tp1": price - atr * tp1_m,
            "reason": f"سیگنال {strat} (Q:{sq:.0f})",
            "rsi": rsi, "atr": atr, "htf": htf,
            "expected_rr":    expected_rr,
            "signal_quality": sq,
        }


# ============================================================================
# 6. RISK MANAGER (ارتقا یافته)
# ============================================================================
class RiskManager:

    @staticmethod
    def calculate_qty(
        balance: float,
        price:   float,
        sl:      float,
        free:    float,
        symbol:  str,
        exchange,
        signal_quality: float = 50.0
    ) -> float:
        if price <= 0 or balance <= 0:
            return 0.0
        dist = abs(price - sl)
        if dist <= 0:
            return 0.0

        # تنظیم RISK_PCT بر اساس کیفیت سیگنال
        quality_mult = 0.5 + (signal_quality / 100.0)  # 0.5x - 1.5x
        adj_risk_pct = RISK_PCT * quality_mult

        risk_usd = balance * (adj_risk_pct / 100.0)
        qty      = risk_usd / dist

        # محدودیت‌های سرمایه
        cfg           = SYMBOL_CONFIG.get(symbol, {})
        max_usd_sym   = cfg.get("max_usd_pos", 300.0)
        max_by_free   = (free * 0.15 * LEVERAGE) / price
        max_by_exp    = (balance * MAX_SINGLE_EXPOSURE / 100.0) / price
        max_by_config = max_usd_sym / price

        qty = min(qty, max_by_free, max_by_exp, max_by_config)

        # Check کل exposure
        with STATE_LOCK:
            total_pos    = len(SHARED_STATE["active_positions"])
            total_exp_usd = sum(
                p.get("qty", 0) * p.get("entry", 0)
                for p in SHARED_STATE["active_positions"].values()
            )
        max_total_exp = balance * MAX_EXPOSURE_PCT / 100.0
        if total_exp_usd + qty * price > max_total_exp:
            available = max(0, max_total_exp - total_exp_usd) / price
            qty = min(qty, available)

        try:
            qty = float(exchange.amount_to_precision(symbol, qty))
            if qty * price < MIN_ORDER_USD:
                qty = float(exchange.amount_to_precision(
                    symbol, MIN_ORDER_USD / price))
        except Exception:
            return 0.0

        return max(qty, 0.0)

    @staticmethod
    def check_global_risk() -> Tuple[bool, str]:
        """چک ریسک کلی پورتفولیو"""
        with STATE_LOCK:
            st      = dict(SHARED_STATE)
            pos     = dict(st["active_positions"])
            balance = st["balance"]

        if st["dd_halted"]:
            return False, f"DD Halt ({st['current_dd']:.1f}%)"
        if st["daily_halted"]:
            return False, f"Daily Loss Halt"
        if not st["is_active"]:
            return False, "ربات متوقف"
        if len(pos) >= MAX_POS:
            return False, f"MAX_POS ({MAX_POS}) تکمیل شده"
        if balance < 20:
            return False, "موجودی ناکافی"

        return True, ""


# ============================================================================
# 7. CIRCUIT BREAKER
# ============================================================================
class CircuitBreaker:
    def __init__(self, db: Database):
        self.db = db

    def is_symbol_allowed(self, symbol: str) -> Tuple[bool, str]:
        now = time.time()
        with STATE_LOCK:
            cd = SHARED_STATE["symbol_cooldowns"].get(symbol, 0)
            if cd > now:
                return False, f"Cooldown ({int((cd-now)/60)}min)"
            err = SHARED_STATE["symbol_errors"].get(symbol, {})
            if err.get("cooldown_end", 0) > now:
                return False, f"API Error Cooldown ({int((err['cooldown_end']-now)/60)}min)"
        return True, ""

    async def register_api_error(self, symbol: str, error: str):
        with STATE_LOCK:
            errs = SHARED_STATE["symbol_errors"]
            if symbol not in errs:
                errs[symbol] = {"count": 0, "cooldown_end": 0}
            errs[symbol]["count"] += 1
            count    = errs[symbol]["count"]
            cooldown = min(30 * (2 ** (count - 1)), MAX_ERROR_COOLDOWN)
            errs[symbol]["cooldown_end"] = time.time() + cooldown
            errs[symbol]["last_error"]   = error[:200]
        log.warning(f"⚠️ CB | {symbol} | خطا #{count} | {cooldown}s")
        await self.db.log_circuit_breaker(
            symbol, "api_error",
            f"count={count}|{error[:100]}")
        await self.db.log_operational_error(
            "api_error", symbol, error[:200])

    async def register_loss(self, symbol: str, pnl: float) -> bool:
        with STATE_LOCK:
            cl = SHARED_STATE["consecutive_losses"]
            if symbol not in cl:
                cl[symbol] = {"count": 0, "last_loss": 0}
            cl[symbol]["count"]     += 1
            cl[symbol]["last_loss"]  = time.time()
            count = cl[symbol]["count"]

            if count >= CONSECUTIVE_LOSS_LIMIT:
                SHARED_STATE["symbol_cooldowns"][symbol] = (
                    time.time() + SYMBOL_COOLDOWN_HOURS * 3600)
                SHARED_STATE["operational"]["circuit_breaker_events"] += 1

        if count >= CONSECUTIVE_LOSS_LIMIT:
            await self.db.log_circuit_breaker(
                symbol, "consecutive_loss",
                f"count={count}|pnl={pnl:.3f}")
            return True
        return False

    def register_win(self, symbol: str):
        with STATE_LOCK:
            SHARED_STATE["consecutive_losses"].pop(symbol, None)
            if symbol in SHARED_STATE["symbol_errors"]:
                SHARED_STATE["symbol_errors"][symbol]["count"]       = 0
                SHARED_STATE["symbol_errors"][symbol]["cooldown_end"] = 0

    async def fix_position_mode(self, exchange, symbol: str) -> bool:
        try:
            await exchange.set_position_mode(False, symbol)
            with STATE_LOCK:
                SHARED_STATE["operational"]["position_mode_fixes"] += 1
            log.info(f"✅ Position Mode Fixed: {symbol}")
            return True
        except Exception as e:
            log.warning(f"Fix position mode {symbol}: {e}")
        return False


# ============================================================================
# 8. TELEGRAM CONTROLLER
# ============================================================================
class TelegramController:
    def __init__(self, engine):
        self.engine  = engine
        self.base    = f"https://api.telegram.org/bot{TG_TOKEN}"
        self.offset  = 0

    def menu(self):
        btn = "⏸️ Pause" if SHARED_STATE["is_active"] else "▶️ Start"
        act = "cmd_pause" if SHARED_STATE["is_active"] else "cmd_start"
        return {"inline_keyboard": [
            [{"text": "📊 Dashboard",       "callback_data": "cmd_dash"},
             {"text": "💼 Positions",        "callback_data": "cmd_pos"}],
            [{"text": "🔄 Sync",            "callback_data": "cmd_sync"},
             {"text": btn,                 "callback_data": act}],
            [{"text": "📈 Stats",           "callback_data": "cmd_stats"},
             {"text": "🚫 Rejections",      "callback_data": "cmd_rej"}],
            [{"text": "🔴 Circuit Breaker", "callback_data": "cmd_cb"},
             {"text": "⚠️ Op Errors",      "callback_data": "cmd_operr"}],
            [{"text": "⚡ Real Test",       "callback_data": "cmd_realtest"}],
            [{"text": "📄 Full Report TXT", "callback_data": "cmd_txt"}],
        ]}

    async def send(self, text: str, markup=None):
        if not TG_TOKEN:
            return
        if len(text) > 4000:
            text = text[:3900] + "\n..."
        payload = {
            "chat_id":    TG_CHAT,
            "text":       text,
            "parse_mode": "HTML"
        }
        if markup:
            payload["reply_markup"] = markup
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(
                    f"{self.base}/sendMessage",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=12))
        except Exception as e:
            log.error(f"TG: {e}")

    async def send_document(self, path: str, caption=""):
        if not os.path.exists(path):
            await self.send("❌ فایل یافت نشد")
            return
        try:
            with open(path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("chat_id",  TG_CHAT)
                form.add_field("caption",  caption[:1024])
                form.add_field("document", f,
                               filename=os.path.basename(path))
                async with aiohttp.ClientSession() as s:
                    await s.post(
                        f"{self.base}/sendDocument",
                        data=form,
                        timeout=aiohttp.ClientTimeout(total=60))
        except Exception as e:
            await self.send(f"❌ خطا ارسال فایل: {e}")

    async def poll(self):
        if not TG_TOKEN:
            return
        await self.send(
            "🚀 <b>Master Quant v15.0 Online</b>\n"
            "Phemex Native | 10 Pairs | MAX_POS=10\n"
            "✅ No Binance | ✅ Full Report | ✅ Circuit Breaker",
            self.menu())

        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{self.base}/getUpdates"
                        f"?offset={self.offset+1}&timeout=8",
                        timeout=aiohttp.ClientTimeout(total=15)
                    ) as r:
                        data = await r.json()

                for u in data.get("result", []):
                    self.offset = u["update_id"]
                    if "callback_query" not in u:
                        continue
                    cb = u["callback_query"]
                    try:
                        async with aiohttp.ClientSession() as ss:
                            await ss.post(
                                f"{self.base}/answerCallbackQuery",
                                json={"callback_query_id": cb["id"],
                                      "text": "⏳"},
                                timeout=aiohttp.ClientTimeout(total=4))
                    except Exception:
                        pass
                    await self._handle(cb["data"])

            except Exception as e:
                log.error(f"TG poll: {e}")
            await asyncio.sleep(1)

    async def _handle(self, cmd: str):
        eng = self.engine

        if cmd == "cmd_start":
            with STATE_LOCK:
                SHARED_STATE["is_active"] = True
            await self.send("▶️ ربات فعال شد", self.menu())

        elif cmd == "cmd_pause":
            with STATE_LOCK:
                SHARED_STATE["is_active"] = False
            await self.send("⏸️ ربات متوقف شد", self.menu())

        elif cmd == "cmd_dash":
            with STATE_LOCK:
                st = dict(SHARED_STATE)
            await self.send(
                f"📊 <b>Dashboard v15.0</b>\n"
                f"منبع: {st.get('data_source','?')} | "
                f"Phemex: {st.get('phemex_status','?')}\n\n"
                f"💰 Balance: <b>${st['balance']:.2f}</b>\n"
                f"📉 DD: {st['current_dd']:.2f}% | "
                f"📅 Daily: ${st['daily_pnl']:.2f}\n"
                f"📦 Positions: {len(st['active_positions'])}/{MAX_POS}\n"
                f"🎯 WR: {st['stats']['win_rate']}% | "
                f"PnL: ${st['stats']['total_pnl']:.2f}\n"
                f"🕐 اسکن: {st['last_scan']}",
                self.menu())

        elif cmd == "cmd_pos":
            with STATE_LOCK:
                pos = dict(SHARED_STATE["active_positions"])
            if not pos:
                await self.send("💤 هیچ پوزیشنی نیست", self.menu())
            else:
                msg = (f"💼 <b>پوزیشن‌های باز "
                       f"({len(pos)}/{MAX_POS}):</b>\n\n")
                for p in pos.values():
                    pr  = eng.data_feed.get_validated_price(p["symbol"])
                    pr  = pr or p["entry"]
                    pnl = ((pr - p["entry"]) * p["qty"]
                           * (1 if p["side"] == "buy" else -1))
                    e = "🟢" if pnl >= 0 else "🔴"
                    msg += (f"{e} <b>{p['symbol']}</b> "
                            f"{p['side'].upper()}\n"
                            f"   Entry:{p['entry']:.4f} | "
                            f"PnL:${pnl:.2f}\n"
                            f"   {p.get('strategy','?')}\n\n")
                await self.send(msg, self.menu())

        elif cmd == "cmd_sync":
            await eng.smart_sync()
            with STATE_LOCK:
                SHARED_STATE["operational"]["sync_count"] += 1
            await self.send("🔄 Sync انجام شد", self.menu())

        elif cmd == "cmd_stats":
            with STATE_LOCK:
                stats = dict(SHARED_STATE.get("stats", {}))
            msg = (
                f"📈 <b>آمار کامل v15.0:</b>\n\n"
                f"معاملات: {stats.get('total_trades',0)}\n"
                f"برنده: {stats.get('winning_trades',0)} | "
                f"بازنده: {stats.get('losing_trades',0)}\n"
                f"WR: {stats.get('win_rate',0):.1f}%\n"
                f"Gross PnL: ${stats.get('total_pnl',0):.3f}\n"
                f"کارمزد: ${stats.get('total_fees',0):.3f}\n"
                f"Net PnL: ${stats.get('net_pnl',0):.3f}\n"
                f"PF: {stats.get('profit_factor',0):.2f}\n"
                f"Sharpe: {stats.get('sharpe_approx',0):.3f}\n"
                f"RR (exp/act): "
                f"{stats.get('avg_expected_rr',0):.2f}/"
                f"{stats.get('avg_actual_rr',0):.2f}\n"
                f"Hold: {stats.get('avg_hold_min',0):.0f}min")
            await self.send(msg, self.menu())

        elif cmd == "cmd_rej":
            decs = await eng.db.get_recent_decisions(15)
            msg  = "🚫 <b>آخرین تصمیم‌ها:</b>\n\n"
            for d in decs:
                icon = "✅" if d["action"] != "neutral" else "⛔"
                sq   = d.get("signal_quality", 0) or 0
                msg += (f"{icon} <b>{d.get('symbol','')}</b> "
                        f"Q:{sq:.0f}\n"
                        f"   {(d.get('reason',''))[:60]}\n\n")
            await self.send(msg, self.menu())

        elif cmd == "cmd_cb":
            now = time.time()
            with STATE_LOCK:
                cds = dict(SHARED_STATE["symbol_cooldowns"])
                cls = dict(SHARED_STATE["consecutive_losses"])
            msg  = "🔴 <b>Circuit Breaker v15.0</b>\n\n"
            ac   = {s: v for s, v in cds.items() if v > now}
            if ac:
                msg += "⛔ <b>Active Cooldowns:</b>\n"
                for s, end in ac.items():
                    msg += f"   {s}: {int((end-now)/60)}min\n"
            else:
                msg += "✅ No active cooldowns\n"
            msg += "\n📊 <b>Consecutive Losses:</b>\n"
            if cls:
                for s, v in cls.items():
                    msg += f"   {s}: {v['count']}\n"
            else:
                msg += "✅ All clear\n"
            await self.send(msg, self.menu())

        elif cmd == "cmd_operr":
            errors = await eng.db.get_operational_errors(10)
            msg    = "⚠️ <b>آخرین خطاهای عملیاتی:</b>\n\n"
            for e in errors:
                msg += (f"[{(e.get('ts',''))[:16]}] "
                        f"{e.get('error_type','')}\n"
                        f"   {e.get('symbol','')} | "
                        f"{e.get('message','')[:60]}\n\n")
            if not errors:
                msg += "✅ هیچ خطایی ثبت نشده"
            await self.send(msg, self.menu())

        elif cmd == "cmd_realtest":
            asyncio.create_task(eng.real_test_trade())

        elif cmd == "cmd_txt":
            await self.send("⏳ در حال تهیه گزارش کامل...")
            report = await eng.db.generate_full_report()
            fname  = (f"quant_v15_report_"
                      f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt")
            with open(fname, "w", encoding="utf-8") as f:
                f.write(report)
            size_kb = os.path.getsize(fname) // 1024
            await self.send_document(
                fname,
                f"📄 گزارش کامل v15.0 | {size_kb}KB | "
                f"{datetime.utcnow().strftime('%H:%M UTC')}")


# ============================================================================
# 9. QUANT ENGINE (اصلی)
# ============================================================================
class QuantEngine:
    def __init__(self):
        self.db        = Database()
        self.strategy  = StrategyEngine()
        self.risk      = RiskManager()
        self.cb        = CircuitBreaker(self.db)
        self.analytics = Analytics(self.db) if False else None

        # ─── Phemex Exchange (تنها منبع داده و معامله) ──────────────
        phemex_config = {
            "apiKey":          API_KEY,
            "secret":          API_SECRET,
            "enableRateLimit": True,
            "options":         {"defaultType": "swap"},
            "timeout":         30000,
        }
        if TESTNET:
            phemex_config["urls"] = {
                "api": {
                    "public":  PHEMEX_TESTNET_URL,
                    "private": PHEMEX_TESTNET_URL,
                }
            }

        self.ex = ccxt.phemex(phemex_config)
        self.ex.set_sandbox_mode(TESTNET)

        # ─── Data Feed از Phemex (بدون Binance) ─────────────────────
        self.data_feed = PhemexDataFeed(self.ex)
        self.tg        = TelegramController(self)

        self.prices:      Dict[str, float] = {}
        self.open_times:  Dict[str, float] = {}

        with STATE_LOCK:
            SHARED_STATE["data_source"]   = "Phemex Testnet" if TESTNET else "Phemex Live"
            SHARED_STATE["phemex_status"] = "initializing"

    async def start(self):
        await self.db.init()
        log.info("🚀 Master Quant v15.0 | Phemex Native")
        log.info(f"   Mode: {'TESTNET' if TESTNET else 'LIVE'}")
        log.info(f"   Pairs: {len(SYMBOLS)} | MAX_POS: {MAX_POS}")

        # ─── اتصال به Phemex ─────────────────────────────────────────
        try:
            await self.ex.load_markets()
            with STATE_LOCK:
                SHARED_STATE["phemex_status"] = "connected"
            log.info("✅ Phemex Markets Loaded")
        except Exception as e:
            with STATE_LOCK:
                SHARED_STATE["phemex_status"] = "error"
            log.error(f"Market load: {e}")

        # ─── Position Mode & Leverage ─────────────────────────────────
        await self._init_exchange()

        # ─── Load Open Trades ─────────────────────────────────────────
        for t in await self.db.get_open_trades():
            with STATE_LOCK:
                SHARED_STATE["active_positions"][t["id"]] = {
                    "id":              t["id"],
                    "symbol":          t["symbol"],
                    "side":            t["side"],
                    "strategy":        t["strategy"],
                    "entry":           t["entry_price"],
                    "qty":             t["qty"],
                    "sl":              t["sl"],
                    "tp":              t["tp"],
                    "tp1":             t["tp1"],
                    "is_partial":      t.get("is_partial", 0),
                    "highest_pnl_pct": t.get("highest_pnl_pct", 0),
                    "expected_rr":     t.get("expected_rr", 0),
                    "signal_quality":  t.get("signal_quality", 0),
                }
                self.open_times[t["id"]] = time.time()

        log.info(
            f"✅ {len(SHARED_STATE['active_positions'])} معامله باز")

        await self.smart_sync()
        await self.update_balance()
        await self.db.update_analytics()

        # ─── اجرای موازی ─────────────────────────────────────────────
        await asyncio.gather(
            self.price_loop(),
            self.scan_loop(),
            self.watchdog_loop(),
            self.equity_logger(),
            self.tg.poll()
        )

    async def _init_exchange(self):
        """تنظیم اولیه Exchange"""
        log.info("🔧 تنظیم Position Mode...")
        try:
            await self.ex.set_position_mode(False)
            log.info("✅ Global One-Way Mode")
        except Exception as e:
            log.warning(f"Global PM: {e}")
            for sym in SYMBOLS:
                try:
                    await self.ex.set_position_mode(False, sym)
                except Exception:
                    pass
                await asyncio.sleep(0.2)

        log.info("🔧 تنظیم Leverage...")
        for sym in SYMBOLS:
            try:
                await self.ex.set_leverage(LEVERAGE, sym)
                log.info(f"   ✅ {LEVERAGE}x → {sym.split('/')[0]}")
            except Exception as e:
                log.warning(f"   Leverage {sym}: {e}")
            await asyncio.sleep(0.3)

    async def update_balance(self):
        try:
            bal   = await self.ex.fetch_balance()
            total = float(bal.get("USDT", {}).get("total", 0) or 0)
            free  = float(bal.get("USDT", {}).get("free",  0) or 0)
            with STATE_LOCK:
                SHARED_STATE["balance"]      = total
                SHARED_STATE["free_balance"] = free
                if total > SHARED_STATE["peak_balance"]:
                    SHARED_STATE["peak_balance"] = total
                if SHARED_STATE["day_start_balance"] <= 0:
                    SHARED_STATE["day_start_balance"] = total
            return total, free
        except Exception as e:
            log.error(f"Balance: {e}")
            return 0.0, 0.0

    async def price_loop(self):
        """آپدیت قیمت‌ها از Phemex مستقیم"""
        while True:
            try:
                prices = await self.data_feed.fetch_all_tickers()
                self.prices.update(prices)
                with STATE_LOCK:
                    SHARED_STATE["phemex_status"] = "live"

                bal, free = await self.update_balance()
                with STATE_LOCK:
                    peak = SHARED_STATE["peak_balance"]
                    if peak > 0 and bal > 0:
                        dd = (peak - bal) / peak * 100
                        SHARED_STATE["current_dd"] = dd
                        SHARED_STATE["dd_halted"]  = dd >= MAX_DD

                    day_start = SHARED_STATE["day_start_balance"]
                    if day_start > 0:
                        dpnl = bal - day_start
                        SHARED_STATE["daily_pnl"]    = dpnl
                        SHARED_STATE["daily_halted"] = (
                            dpnl / day_start * 100 <= -MAX_DAILY_LOSS)

            except Exception as e:
                log.error(f"price_loop: {e}")
                with STATE_LOCK:
                    SHARED_STATE["phemex_status"] = "error"

            await asyncio.sleep(PRICE_LOOP_INTERVAL)

    async def equity_logger(self):
        """لاگ منظم Equity"""
        while True:
            await asyncio.sleep(60)
            with STATE_LOCK:
                bal  = SHARED_STATE["balance"]
                free = SHARED_STATE["free_balance"]
                peak = SHARED_STATE["peak_balance"]
                dd   = SHARED_STATE["current_dd"]
                npos = len(SHARED_STATE["active_positions"])
            await self.db.log_equity(bal, free, peak, dd, npos)

    async def scan_loop(self):
        """اسکن نمادها"""
        while True:
            # چک ریسک کلی
            ok, reason = self.risk.check_global_risk()
            if not ok:
                log.debug(f"Scan skip: {reason}")
                await asyncio.sleep(12)
                continue

            scan_start = time.time()
            with STATE_LOCK:
                SHARED_STATE["last_scan"] = time.strftime("%H:%M:%S")
                SHARED_STATE["scan_count"] += 1

            # آمار اسکن
            s_scanned = s_found = s_exec = 0
            s_rej_price = s_rej_sig = s_rej_cb = 0

            for sym in SYMBOLS:
                # چک پوزیشن موجود
                with STATE_LOCK:
                    if any(p["symbol"] == sym
                           for p in SHARED_STATE["active_positions"].values()):
                        continue

                s_scanned += 1

                # Circuit Breaker
                allowed, cb_reason = self.cb.is_symbol_allowed(sym)
                if not allowed:
                    s_rej_cb += 1
                    continue

                try:
                    # ─── دریافت داده از Phemex ──────────────────────
                    df5 = await self.data_feed.fetch_ohlcv(
                        sym, TIMEFRAME, 120)
                    await asyncio.sleep(SYMBOL_DELAY)

                    df1 = await self.data_feed.fetch_ohlcv(
                        sym, HTF_TIMEFRAME, 80)
                    await asyncio.sleep(0.5)

                    if df5 is None or len(df5) < 50:
                        continue
                    if df1 is None or len(df1) < 20:
                        df1 = df5.copy()

                    # ─── تحلیل استراتژی ─────────────────────────────
                    sig = self.strategy.analyze(df5, df1, sym)

                    # ─── قیمت از Phemex (بدون مقایسه Binance) ───────
                    phemex_price = self.data_feed.get_validated_price(sym)
                    if not phemex_price or phemex_price <= 0:
                        phemex_price = self.prices.get(sym)
                    if not phemex_price or phemex_price <= 0:
                        continue

                    # ─── Spread Check از Order Book ──────────────────
                    spread_pct, ob_price = (
                        await self.data_feed.get_market_depth_spread(sym))
                    await asyncio.sleep(0.3)

                    # آستانه spread per-symbol
                    max_spread = SYMBOL_CONFIG.get(
                        sym, {}).get("max_atr_pct", 5.0) * 0.3
                    max_spread = max(0.3, min(max_spread, 2.0))

                    if spread_pct > max_spread and spread_pct < 999:
                        s_rej_price += 1
                        await self.db.log_decision(
                            sym, "neutral", "",
                            f"Spread بالا ({spread_pct:.2f}%)",
                            price=phemex_price,
                            spread_pct=spread_pct)
                        continue

                    # ─── Log Decision ────────────────────────────────
                    await self.db.log_decision(
                        sym, sig["action"],
                        sig.get("strat", ""),
                        sig.get("reason", ""),
                        phemex_price,
                        sig.get("rsi", 0),
                        sig.get("atr", 0),
                        sig.get("htf", ""),
                        sig.get("signal_quality", 0),
                        spread_pct)

                    if sig["action"] != "neutral":
                        s_found += 1
                        with STATE_LOCK:
                            SHARED_STATE["signal_count"] += 1

                        # بازمحاسبه SL/TP با قیمت Phemex واقعی
                        atr = sig.get("atr", 0)
                        if atr > 0:
                            strat = sig.get("strat", "")
                            p = STRATEGY_PARAMS.get(
                                strat,
                                {"sl_m":1.5,"tp_m":2.8,"tp1_m":1.4})
                            if sig["action"] == "buy":
                                sig["sl"]  = phemex_price - atr * p["sl_m"]
                                sig["tp"]  = phemex_price + atr * p["tp_m"]
                                sig["tp1"] = phemex_price + atr * p["tp1_m"]
                            else:
                                sig["sl"]  = phemex_price + atr * p["sl_m"]
                                sig["tp"]  = phemex_price - atr * p["tp_m"]
                                sig["tp1"] = phemex_price - atr * p["tp1_m"]

                        executed = await self.execute_trade(sym, sig)
                        if executed:
                            s_exec += 1
                    else:
                        s_rej_sig += 1
                        with STATE_LOCK:
                            SHARED_STATE["rejected_count"] += 1

                except Exception as e:
                    log.error(f"scan {sym}: {e}")
                    await self.cb.register_api_error(sym, str(e))

                await asyncio.sleep(SYMBOL_DELAY)

            # ذخیره آمار اسکن
            scan_dur = (time.time() - scan_start) * 1000
            await self.db.log_scan_stats(
                s_scanned, s_found, s_exec,
                s_rej_price, s_rej_sig, s_rej_cb, scan_dur)

            log.info(
                f"📡 اسکن #{SHARED_STATE['scan_count']} | "
                f"⏱️{scan_dur:.0f}ms | "
                f"✅{s_exec}/{s_found} | "
                f"❌{s_rej_sig}sig {s_rej_price}spread {s_rej_cb}cb")

            await asyncio.sleep(SCAN_INTERVAL)

    async def execute_trade(self, sym: str, sig: dict) -> bool:
        """اجرای معامله - برگشت True اگر موفق"""
        price = self.data_feed.get_validated_price(sym) or self.prices.get(sym)
        with STATE_LOCK:
            bal  = SHARED_STATE["balance"]
            free = SHARED_STATE["free_balance"]

        if not price or bal < 20 or free < 15:
            return False

        try:
            qty = self.risk.calculate_qty(
                bal, price, sig["sl"], free, sym, self.ex,
                sig.get("signal_quality", 50))

            if qty <= 0:
                await self.db.log_decision(
                    sym, "rejected",
                    sig.get("strat", ""), "حجم صفر")
                return False

            # ارسال سفارش
            order = await self.ex.create_market_order(
                sym, sig["action"], qty)

            fill    = float(order.get("average") or price)
            slip    = abs(fill - price)
            pid     = f"pos_{uuid.uuid4().hex[:8]}"

            # محاسبه RR واقعی بر اساس fill
            if sig["action"] == "buy":
                act_rr = (sig["tp"] - fill) / max(fill - sig["sl"], 0.0001)
            else:
                act_rr = (fill - sig["tp"]) / max(sig["sl"] - fill, 0.0001)

            pos = {
                "id":              pid,
                "symbol":          sym,
                "side":            sig["action"],
                "strategy":        sig.get("strat", ""),
                "entry":           fill,
                "qty":             qty,
                "sl":              sig["sl"],
                "tp":              sig["tp"],
                "tp1":             sig["tp1"],
                "is_partial":      0,
                "highest_pnl_pct": 0.0,
                "expected_rr":     sig.get("expected_rr", 0),
                "actual_rr":       round(act_rr, 2),
                "signal_quality":  sig.get("signal_quality", 0),
                "rsi_at_entry":    sig.get("rsi", 0),
                "atr_at_entry":    sig.get("atr", 0),
                "htf_trend":       sig.get("htf", ""),
                "slippage":        slip,
            }

            with STATE_LOCK:
                SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()

            await self.db.insert_trade(pos)
            self.cb.register_win(sym)

            await self.tg.send(
                f"🎯 <b>{sig['action'].upper()}</b> "
                f"| {sym.split('/')[0]}\n"
                f"Strat: {sig.get('strat','')} | "
                f"Q:{sig.get('signal_quality',0):.0f}\n"
                f"Fill: {fill:.5f} | Slip: {slip:.5f}\n"
                f"SL: {sig['sl']:.5f} | TP: {sig['tp']:.5f}\n"
                f"RR: {act_rr:.2f}x | Qty: {qty}")

            return True

        except Exception as e:
            err = str(e)
            log.error(f"execute {sym}: {err}")

            if "20004" in err or "INCONSISTENT" in err.upper():
                fixed = await self.cb.fix_position_mode(self.ex, sym)
                await self.tg.send(
                    f"🔧 Position Mode {'Fixed' if fixed else 'Fix Failed'}: "
                    f"{sym.split('/')[0]}")
            else:
                await self.cb.register_api_error(sym, err)

            await self.db.log_decision(
                sym, "rejected",
                sig.get("strat",""), err[:120])
            return False

    async def real_test_trade(self):
        await self.tg.send("⚡ شروع تست واقعی...")
        try:
            bal, free = await self.update_balance()
            if bal < 20:
                await self.tg.send("❌ موجودی ناکافی")
                return
            price = self.data_feed.get_validated_price(TEST_SYMBOL)
            if not price:
                await self.tg.send("❌ قیمت در دسترس نیست")
                return
            qty = float(self.ex.amount_to_precision(
                TEST_SYMBOL, min(TEST_USD, bal * 0.05) / price))
            order = await self.ex.create_market_order(
                TEST_SYMBOL, "buy", qty)
            fill  = float(order.get("average") or price)
            pid   = f"test_{uuid.uuid4().hex[:6]}"
            pos   = {
                "id": pid, "symbol": TEST_SYMBOL,
                "side": "buy", "strategy": "RealTest",
                "entry": fill, "qty": qty,
                "sl": fill * 0.97, "tp": fill * 1.03,
                "tp1": fill * 1.015,
                "is_partial": 0, "highest_pnl_pct": 0.0,
                "expected_rr": 1.0, "signal_quality": 50,
            }
            with STATE_LOCK:
                SHARED_STATE["active_positions"][pid] = pos
            self.open_times[pid] = time.time()
            await self.tg.send(
                f"🧪 تست باز @ {fill:.5f} | Qty:{qty}\n30 ثانیه...")
            await asyncio.sleep(30)
            await self.force_close(pid, "RealTest done")
            await self.tg.send("✅ تست بسته شد")
        except Exception as e:
            await self.tg.send(f"❌ خطا: {e}")

    async def smart_sync(self):
        """همگام‌سازی با وضعیت واقعی Exchange"""
        try:
            remote = await self.ex.fetch_positions()
            active = set()
            for p in remote:
                if abs(float(p.get("contracts") or 0)) > 0:
                    raw     = p.get("symbol", "")
                    matched = next(
                        (s for s in SYMBOLS
                         if s.split("/")[0] in raw), None)
                    if matched:
                        active.add(matched)

            with STATE_LOCK:
                to_del = [
                    pid for pid, p in SHARED_STATE["active_positions"].items()
                    if p["symbol"] not in active
                    and p["strategy"] != "RealTest"
                ]
            for pid in to_del:
                await self.db.close_trade(
                    pid, 0.0, reason="remote close")
                with STATE_LOCK:
                    SHARED_STATE["active_positions"].pop(pid, None)

            log.info(f"🔄 Sync | {len(active)} پوزیشن در exchange")
        except Exception as e:
            log.error(f"sync: {e}")

    async def force_close(self, pid: str, reason: str):
        with STATE_LOCK:
            pos = SHARED_STATE["active_positions"].get(pid)
        if not pos:
            return

        price = (self.data_feed.get_validated_price(pos["symbol"])
                 or pos["entry"])
        hold  = time.time() - self.open_times.get(pid, time.time())

        try:
            cs = "sell" if pos["side"] == "buy" else "buy"
            order = await self.ex.create_market_order(
                pos["symbol"], cs, pos["qty"],
                params={"reduceOnly": True})

            exit_price = float(order.get("average") or price)
            raw_pnl    = ((exit_price - pos["entry"]) * pos["qty"]
                          * (1 if pos["side"] == "buy" else -1))
            fees       = abs(raw_pnl) * TAKER_FEE * 2 * FEE_BUFFER
            net        = raw_pnl - fees
            slip       = abs(exit_price - price)

            # RR واقعی
            if pos["side"] == "buy":
                dist_sl = pos["entry"] - pos["sl"]
                dist_pnl = exit_price - pos["entry"]
            else:
                dist_sl  = pos["sl"] - pos["entry"]
                dist_pnl = pos["entry"] - exit_price
            act_rr = (dist_pnl / dist_sl) if dist_sl > 0 else 0

            if pos["strategy"] != "RealTest":
                await self.db.close_trade(
                    pid, raw_pnl, fees, reason, hold,
                    exit_price, act_rr, slip)

                if net < 0:
                    cb_act = await self.cb.register_loss(
                        pos["symbol"], net)
                    if cb_act:
                        await self.tg.send(
                            f"🛑 <b>Circuit Breaker</b>\n"
                            f"{pos['symbol'].split('/')[0]}: "
                            f"{CONSECUTIVE_LOSS_LIMIT} ضرر متوالی\n"
                            f"Cooldown: {SYMBOL_COOLDOWN_HOURS}h")
                else:
                    self.cb.register_win(pos["symbol"])

            with STATE_LOCK:
                SHARED_STATE["active_positions"].pop(pid, None)
            self.open_times.pop(pid, None)
            await self.db.update_analytics()

            emoji = "🟢" if net >= 0 else "🔴"
            await self.tg.send(
                f"{emoji} <b>بسته شد</b> ({reason})\n"
                f"{pos['symbol'].split('/')[0]} | "
                f"{pos['side'].upper()}\n"
                f"PnL: ${net:.3f} | "
                f"RR: {act_rr:.2f}x | "
                f"Hold: {int(hold/60)}min")

        except Exception as e:
            log.error(f"force_close {pid}: {e}")

    async def watchdog_loop(self):
        """نظارت مستمر بر پوزیشن‌ها"""
        while True:
            with STATE_LOCK:
                items = list(
                    SHARED_STATE["active_positions"].items())

            for pid, pos in items:
                if pos["strategy"] == "RealTest":
                    continue

                price = (
                    self.data_feed.get_validated_price(pos["symbol"])
                    or self.prices.get(pos["symbol"]))
                if not price:
                    continue

                pnl_pct = (
                    (price - pos["entry"]) / pos["entry"] * 100
                    if pos["side"] == "buy"
                    else (pos["entry"] - price) / pos["entry"] * 100)

                # Trailing Stop
                if pnl_pct > TRAIL_ACT:
                    if pnl_pct > pos["highest_pnl_pct"]:
                        pos["highest_pnl_pct"] = pnl_pct
                        if pos["side"] == "buy":
                            new_sl = price * (1 - TRAIL_STEP / 100)
                            if new_sl > pos["sl"]:
                                pos["sl"] = new_sl
                        else:
                            new_sl = price * (1 + TRAIL_STEP / 100)
                            if new_sl < pos["sl"]:
                                pos["sl"] = new_sl
                        await self.db.update_trade(
                            pid, pos["qty"], pos["sl"],
                            pos["is_partial"],
                            pos["highest_pnl_pct"])

                # Partial TP
                if PARTIAL_TP and pos["is_partial"] == 0:
                    tp1_hit = (
                        (pos["side"] == "buy"  and price >= pos["tp1"])
                        or
                        (pos["side"] == "sell" and price <= pos["tp1"]))
                    if tp1_hit:
                        try:
                            half = float(
                                self.ex.amount_to_precision(
                                    pos["symbol"], pos["qty"] / 2))
                            if half > 0:
                                cs = ("sell" if pos["side"] == "buy"
                                      else "buy")
                                await self.ex.create_market_order(
                                    pos["symbol"], cs, half,
                                    params={"reduceOnly": True})
                                pos["qty"]       -= half
                                pos["is_partial"] = 1
                                pos["sl"]         = pos["entry"]
                                await self.db.update_trade(
                                    pid, pos["qty"], pos["sl"],
                                    1, pos["highest_pnl_pct"])
                                await self.tg.send(
                                    f"🔹 Partial TP → BE\n"
                                    f"{pos['symbol'].split('/')[0]} "
                                    f"@ {price:.4f}")
                        except Exception as e:
                            log.error(f"partial_tp: {e}")

                # SL / TP
                sl_hit = (
                    (pos["side"] == "buy"  and price <= pos["sl"])
                    or
                    (pos["side"] == "sell" and price >= pos["sl"]))
                tp_hit = (
                    (pos["side"] == "buy"  and price >= pos["tp"])
                    or
                    (pos["side"] == "sell" and price <= pos["tp"]))

                if sl_hit or tp_hit:
                    await self.force_close(
                        pid, "SL/Trail" if sl_hit else "TP")

            await asyncio.sleep(2.0)


# ============================================================================
# 10. ANALYTICS (helper)
# ============================================================================
class Analytics:
    def __init__(self, db: Database):
        self.db = db

    async def full_report(self) -> str:
        return await self.db.generate_full_report()


# ============================================================================
# 11. WEB DASHBOARD
# ============================================================================
app = Flask(__name__)

DASH_HTML = """
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Quant v15.0 | Phemex Native</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui;background:#0d1117;color:#c9d1d9;
  padding:16px;direction:rtl}
h1{color:#58a6ff;font-size:1.4rem;margin-bottom:4px}
.sub{color:#8b949e;font-size:.8rem;margin-bottom:16px}
.bar{background:#161b22;border:1px solid #30363d;border-radius:8px;
  padding:8px 14px;display:flex;gap:16px;flex-wrap:wrap;
  font-size:.82rem;margin-bottom:12px;align-items:center}
.dot{width:9px;height:9px;border-radius:50%;display:inline-block;
  margin-left:5px}
.g{background:#3fb950}.r{background:#f85149}.y{background:#d29922}
.alert{background:#1a0f0f;border:1px solid #f85149;border-radius:8px;
  padding:8px 14px;margin-bottom:10px;color:#f85149;font-size:.82rem}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));
  gap:10px;margin:12px 0}
.card{background:#161b22;border:1px solid #30363d;border-radius:9px;
  padding:13px}
.label{font-size:.74rem;color:#8b949e;margin-bottom:5px}
.val{font-size:1.28rem;font-weight:700;color:#58a6ff}
.val.gn{color:#3fb950}.val.rd{color:#f85149}
h3{color:#8b949e;font-size:.85rem;margin:16px 0 6px;
  text-transform:uppercase;letter-spacing:.4px}
table{width:100%;border-collapse:collapse;font-size:.82rem}
th{background:#21262d;padding:7px 10px;text-align:right;color:#8b949e}
td{padding:7px 10px;border-bottom:1px solid #21262d}
tr:hover td{background:#161b22}
.b{display:inline-block;padding:2px 7px;border-radius:10px;
  font-size:.72rem;font-weight:700}
.bb{background:#0d2818;color:#3fb950}
.bs{background:#2d1317;color:#f85149}
.bst{background:#0c2a63;color:#58a6ff}
</style>
</head>
<body>
<h1>🚀 Master Quant v15.0</h1>
<p class="sub">Phemex Native | 10 Pairs | No Binance | MAX_POS=10</p>

<div class="bar">
  <span><span class="dot" id="d1"></span>
    <span id="st">—</span></span>
  <span>📡 Phemex: <span id="ps">—</span></span>
  <span>🕐 اسکن: <span id="sc">—</span></span>
  <span>📊 اسکن #<span id="sn">0</span></span>
</div>

<div id="alerts"></div>

<div class="grid">
  <div class="card"><div class="label">موجودی</div>
    <div class="val" id="bal">—</div></div>
  <div class="card"><div class="label">موجودی آزاد</div>
    <div class="val" id="fre">—</div></div>
  <div class="card"><div class="label">پوزیشن باز</div>
    <div class="val" id="pos">0/10</div></div>
  <div class="card"><div class="label">Net PnL</div>
    <div class="val" id="pnl">—</div></div>
  <div class="card"><div class="label">Win Rate</div>
    <div class="val" id="wr">—</div></div>
  <div class="card"><div class="label">Drawdown</div>
    <div class="val rd" id="dd">—</div></div>
  <div class="card"><div class="label">Profit Factor</div>
    <div class="val" id="pf">—</div></div>
  <div class="card"><div class="label">کل معاملات</div>
    <div class="val" id="tr">—</div></div>
</div>

<h3>📦 پوزیشن‌های باز</h3>
<table><thead><tr>
  <th>نماد</th><th>جهت</th><th>استراتژی</th>
  <th>ورود</th><th>SL</th><th>TP</th><th>Qty</th><th>Q</th>
</tr></thead>
<tbody id="ptb">
  <tr><td colspan="8" style="text-align:center;color:#8b949e">
    هیچ پوزیشنی نیست</td></tr>
</tbody></table>

<script>
async function r(){
  try{
    const d=await(await fetch('/api/status')).json();
    const a=d.is_active&&!d.dd_halted&&!d.daily_halted;
    document.getElementById('d1').className=
      'dot '+(a?'g':d.dd_halted?'r':'y');
    document.getElementById('st').textContent=
      d.dd_halted?'DD Halt':d.daily_halted?'Daily Halt':
      d.is_active?'فعال':'متوقف';
    document.getElementById('ps').textContent=
      d.phemex_status||'?';
    document.getElementById('sc').textContent=d.last_scan||'—';
    document.getElementById('sn').textContent=d.scan_count||0;

    const b=d.balance||0,f=d.free_balance||0;
    document.getElementById('bal').textContent='$'+b.toFixed(2);
    document.getElementById('fre').textContent='$'+f.toFixed(2);

    const pos=Object.values(d.active_positions||{});
    document.getElementById('pos').textContent=
      pos.length+'/10';

    const s=d.stats||{};
    const np=s.net_pnl||s.total_pnl||0;
    const pe=document.getElementById('pnl');
    pe.textContent='$'+np.toFixed(2);
    pe.className='val '+(np>=0?'gn':'rd');

    document.getElementById('wr').textContent=
      (s.win_rate||0).toFixed(1)+'%';
    const dd=d.current_dd||0;
    document.getElementById('dd').textContent=
      dd.toFixed(2)+'%';
    document.getElementById('pf').textContent=
      (s.profit_factor||0).toFixed(2);
    document.getElementById('tr').textContent=
      s.total_trades||0;

    // Positions
    const tb=document.getElementById('ptb');
    if(pos.length===0){
      tb.innerHTML='<tr><td colspan="8" style="text-align:center;'+
        'color:#8b949e">هیچ پوزیشنی نیست</td></tr>';
    }else{
      tb.innerHTML=pos.map(p=>`<tr>
        <td>${p.symbol.split('/')[0]}</td>
        <td><span class="b ${p.side=='buy'?'bb':'bs'}">
          ${p.side.toUpperCase()}</span></td>
        <td><span class="b bst">${p.strategy||'?'}</span></td>
        <td>${(p.entry||0).toFixed(4)}</td>
        <td style="color:#f85149">${(p.sl||0).toFixed(4)}</td>
        <td style="color:#3fb950">${(p.tp||0).toFixed(4)}</td>
        <td>${p.qty||0}</td>
        <td>${(p.signal_quality||0).toFixed(0)}</td>
      </tr>`).join('');
    }

    // Circuit Breaker Alerts
    const cds=d.symbol_cooldowns||{};
    const now=Date.now()/1000;
    const ac=Object.entries(cds).filter(([,v])=>v>now);
    document.getElementById('alerts').innerHTML=
      ac.length?'<div class="alert">🔴 <b>Circuit Breaker:</b> '+
      ac.map(([s,v])=>s.split('/')[0]+
        ' ('+Math.round((v-now)/60)+'min)').join(' | ')+
      '</div>':'';
  }catch(e){console.error(e)}
}
r();setInterval(r,4000);
</script>
</body></html>
"""

@app.route("/")
def dashboard():
    return render_template_string(DASH_HTML)

@app.route("/api/status")
def api_status():
    with STATE_LOCK:
        return jsonify(dict(SHARED_STATE))

@app.route("/api/report")
def api_report():
    return jsonify({"status": "use /api/status or TG cmd_txt"})

def run_web():
    app.run(host="0.0.0.0", port=10000, debug=False, use_reloader=False)


# ============================================================================
# 12. MAIN
# ============================================================================
if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    log.info("🌐 Dashboard: http://0.0.0.0:10000")
    log.info(f"🔗 منبع داده: Phemex {'Testnet' if TESTNET else 'Live'}")
    log.info(f"📊 {len(SYMBOLS)} نماد | MAX_POS={MAX_POS}")

    engine = QuantEngine()
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        log.info("👋 Shutdown")
    except Exception as e:
        log.error(f"💥 Fatal: {e}")
        raise
