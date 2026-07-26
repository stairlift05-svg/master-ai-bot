#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v16.0 (Orphan Position Manager + Phemex Fix)
═══════════════════════════════════════════════════════════════════
اصلاحات کامل بر اساس لاگ واقعی:

🔴 بحرانی:
  1. فرمت Timeframe Phemex: "5m"→"5m" ولی باید با market.timeframes چک شود
  2. OHLCV آرگومان اشتباه → استفاده از since timestamp
  3. مدیریت پوزیشن یتیم (Orphan Position Manager)
  4. MATIC حذف شد (TE_SYMBOL_NOT_IN_RANGE)
  5. setPositionMode per-symbol اجباری

🟢 جدید:
  6. Orphan Position Scanner → شناسایی و مدیریت همه پوزیشن‌های باز
  7. Auto-adopt پوزیشن‌های دستی
  8. Phemex OHLCV format fix
  9. Symbol validator در startup
"""

import asyncio
import logging
import os
import time
import uuid
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from threading import Thread, Lock
from typing import Dict, List, Any, Optional, Tuple

import aiohttp
import aiosqlite
import ccxt.async_support as ccxt
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string

# ============================================================================
# 1. CONFIGURATION
# ============================================================================
load_dotenv()

API_KEY    = os.getenv("PHEMEX_API_KEY", "")
API_SECRET = os.getenv("PHEMEX_API_SECRET", "")
TESTNET    = os.getenv("PHEMEX_TESTNET", "True").lower() in ("true","1","yes")
TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT    = os.getenv("TELEGRAM_CHAT_ID", "")

# ─── Phemex Testnet URLs ──────────────────────────────────────────────────
PHEMEX_TESTNET_REST = "https://testnet-api.phemex.com"
PHEMEX_LIVE_REST    = "https://api.phemex.com"

# ─── نمادهای معتبر Phemex (MATIC حذف شد) ─────────────────────────────────
# فرمت صحیح برای Phemex Perpetual Swap
SYMBOLS_CANDIDATE = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "BNB/USDT:USDT",
    "XRP/USDT:USDT",
    "ADA/USDT:USDT",
    "LTC/USDT:USDT",
    "LINK/USDT:USDT",
    "DOGE/USDT:USDT",
    "AVAX/USDT:USDT",
    "SOL/USDT:USDT",
]

# لیست نهایی بعد از validation در startup
SYMBOLS: List[str] = []

# ─── Phemex timeframe map ─────────────────────────────────────────────────
# Phemex از این فرمت‌ها پشتیبانی می‌کند
PHEMEX_TF_MAP = {
    "1m":  60,
    "3m":  180,
    "5m":  300,
    "15m": 900,
    "30m": 1800,
    "1h":  3600,
    "2h":  7200,
    "4h":  14400,
    "6h":  21600,
    "12h": 43200,
    "1d":  86400,
    "1w":  604800,
}

TIMEFRAME     = "5m"
HTF_TIMEFRAME = "1h"

# ─── Per-Symbol Config ────────────────────────────────────────────────────
BASE_SYMBOL_CONFIG = {
    "BTC/USDT:USDT":  {"min_atr_pct":0.03,"max_atr_pct":3.0,"min_vol_mult":1.1,"weight":1.5,"max_usd_pos":500.0},
    "ETH/USDT:USDT":  {"min_atr_pct":0.05,"max_atr_pct":4.0,"min_vol_mult":1.1,"weight":1.3,"max_usd_pos":300.0},
    "BNB/USDT:USDT":  {"min_atr_pct":0.08,"max_atr_pct":4.5,"min_vol_mult":1.1,"weight":1.0,"max_usd_pos":200.0},
    "XRP/USDT:USDT":  {"min_atr_pct":0.15,"max_atr_pct":6.0,"min_vol_mult":1.15,"weight":0.9,"max_usd_pos":150.0},
    "ADA/USDT:USDT":  {"min_atr_pct":0.2, "max_atr_pct":7.0,"min_vol_mult":1.15,"weight":0.9,"max_usd_pos":150.0},
    "LTC/USDT:USDT":  {"min_atr_pct":0.1, "max_atr_pct":5.0,"min_vol_mult":1.2,"weight":0.85,"max_usd_pos":150.0},
    "LINK/USDT:USDT": {"min_atr_pct":0.15,"max_atr_pct":6.0,"min_vol_mult":1.2,"weight":0.85,"max_usd_pos":150.0},
    "DOGE/USDT:USDT": {"min_atr_pct":0.3, "max_atr_pct":8.0,"min_vol_mult":1.3,"weight":0.7,"max_usd_pos":100.0},
    "AVAX/USDT:USDT": {"min_atr_pct":0.15,"max_atr_pct":6.0,"min_vol_mult":1.2,"weight":0.85,"max_usd_pos":150.0},
    "SOL/USDT:USDT":  {"min_atr_pct":0.08,"max_atr_pct":5.0,"min_vol_mult":1.15,"weight":1.0,"max_usd_pos":200.0},
}
SYMBOL_CONFIG: Dict[str, dict] = {}  # پر می‌شود بعد از validation

# ─── Strategy Params ──────────────────────────────────────────────────────
STRATEGY_PARAMS = {
    "Breakout_Momentum":   {"sl_m":1.0,"tp_m":3.5,"tp1_m":1.8},
    "MTF_Pullback":        {"sl_m":1.2,"tp_m":2.8,"tp1_m":1.4},
    "SuperTrend_Pullback": {"sl_m":1.0,"tp_m":2.5,"tp1_m":1.3},
    "Volume_Surge":        {"sl_m":1.1,"tp_m":2.2,"tp1_m":1.2},
    "EMA_Cross":           {"sl_m":1.3,"tp_m":3.0,"tp1_m":1.6},
    "Orphan_Adopted":      {"sl_m":2.0,"tp_m":2.0,"tp1_m":1.0},
}

# ─── Risk ─────────────────────────────────────────────────────────────────
RISK_PCT             = 0.5
LEVERAGE             = 5
MAX_POS              = 10
MAX_DD               = 10.0
MAX_DAILY_LOSS       = 5.0
MIN_ORDER_USD        = 16.0
MAX_EXPOSURE_PCT     = 80.0
MAX_SINGLE_EXPOSURE  = 15.0
TAKER_FEE            = 0.0006
FEE_BUFFER           = 1.2
TRAIL_ACT            = 1.8
TRAIL_STEP           = 0.6
PARTIAL_TP           = True

# ─── Orphan Management ────────────────────────────────────────────────────
ORPHAN_SCAN_INTERVAL = 30      # ثانیه بین بررسی یتیم‌ها
ORPHAN_SL_PCT        = 3.0    # SL پیش‌فرض برای یتیم‌ها (درصد از entry)
ORPHAN_TP_PCT        = 4.0    # TP پیش‌فرض برای یتیم‌ها
ORPHAN_TP1_PCT       = 2.0    # TP1 پیش‌فرض

# ─── Timing ───────────────────────────────────────────────────────────────
SCAN_INTERVAL        = 45
SYMBOL_DELAY         = 1.2
PRICE_LOOP_INTERVAL  = 6
EQUITY_LOG_INTERVAL  = 60

# ─── Circuit Breaker ──────────────────────────────────────────────────────
CONSECUTIVE_LOSS_LIMIT = 3
SYMBOL_COOLDOWN_HOURS  = 2
MAX_ERROR_COOLDOWN     = 1800
TEST_SYMBOL_FALLBACK   = "ADA/USDT:USDT"

# ─── Logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler("quant_v16.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("QuantV16.0")

# ─── Shared State ─────────────────────────────────────────────────────────
SHARED_STATE: Dict[str, Any] = {
    "is_active":          True,
    "dd_halted":          False,
    "daily_halted":       False,
    "balance":            0.0,
    "free_balance":       0.0,
    "peak_balance":       0.0,
    "day_start_balance":  0.0,
    "current_dd":         0.0,
    "daily_pnl":          0.0,
    "active_positions":   {},
    "orphan_positions":   {},   # پوزیشن‌های یتیم شناسایی‌شده
    "last_scan":          "Never",
    "scan_count":         0,
    "signal_count":       0,
    "rejected_count":     0,
    "valid_symbols":      [],
    "consecutive_losses": {},
    "symbol_cooldowns":   {},
    "symbol_errors":      {},
    "phemex_status":      "init",
    "data_source":        "Phemex Native",
    "stats": {
        "total_trades":0,"winning_trades":0,"losing_trades":0,
        "win_rate":0.0,"total_pnl":0.0,"total_fees":0.0,
        "net_pnl":0.0,"avg_hold_min":0.0,"max_win":0.0,
        "max_loss":0.0,"profit_factor":0.0,"sharpe_approx":0.0,
        "avg_expected_rr":0.0,"avg_actual_rr":0.0,
        "orphans_adopted":0,"orphans_closed":0,
        "by_symbol":{},"by_strategy":{},
    },
    "operational": {
        "total_api_errors":0,"position_mode_fixes":0,
        "circuit_breaker_events":0,"sync_count":0,
        "orphan_detections":0,"uptime_start":time.time(),
    },
    "version": "16.0",
}
STATE_LOCK = Lock()


# ============================================================================
# 2. SYMBOL VALIDATOR
# ============================================================================
class SymbolValidator:
    """
    اعتبارسنجی نمادها در Phemex قبل از شروع
    - چک وجود در بازار
    - چک OHLCV قابل دریافت بودن
    - چک Leverage قابل تنظیم بودن
    """

    def __init__(self, exchange: ccxt.phemex):
        self.ex = exchange

    async def validate_all(self, candidates: List[str]) -> List[str]:
        valid   = []
        invalid = []

        log.info(f"🔍 اعتبارسنجی {len(candidates)} نماد...")

        for sym in candidates:
            ok, reason = await self._check_symbol(sym)
            if ok:
                valid.append(sym)
                SYMBOL_CONFIG[sym] = BASE_SYMBOL_CONFIG.get(sym, {
                    "min_atr_pct":0.1,"max_atr_pct":6.0,
                    "min_vol_mult":1.2,"weight":0.8,"max_usd_pos":100.0
                })
                log.info(f"   ✅ {sym.split('/')[0]}")
            else:
                invalid.append(sym)
                log.warning(f"   ❌ {sym.split('/')[0]}: {reason}")
            await asyncio.sleep(0.8)

        log.info(
            f"✅ نمادهای معتبر: {len(valid)} | "
            f"❌ حذف‌شده: {len(invalid)}")
        return valid

    async def _check_symbol(self, sym: str) -> Tuple[bool, str]:
        """چک یک نماد"""
        # 1. چک وجود در بازار
        try:
            markets = self.ex.markets or {}
            if sym not in markets:
                return False, "در market list نیست"
        except Exception:
            pass

        # 2. چک OHLCV
        try:
            since = int((time.time() - 3600) * 1000)
            candles = await self.ex.fetch_ohlcv(
                sym, "5m", since=since, limit=5)
            if not candles or len(candles) < 2:
                return False, "OHLCV خالی"
        except Exception as e:
            err = str(e)
            if "30000" in err or "input arguments" in err:
                return False, f"OHLCV خطا: {err[:80]}"
            if "11070" in err or "NOT_IN_RANGE" in err:
                return False, "نماد در Testnet موجود نیست"
            return False, f"خطا: {err[:60]}"

        # 3. چک Leverage
        try:
            await self.ex.set_leverage(LEVERAGE, sym)
        except Exception as e:
            err = str(e)
            if "11070" in err or "NOT_IN_RANGE" in err:
                return False, "Leverage خطا - نماد موجود نیست"

        return True, "OK"


# ============================================================================
# 3. PHEMEX DATA FEED (اصلاح شده)
# ============================================================================
class PhemexDataFeed:
    """
    دریافت داده مستقیم از Phemex
    اصلاح اصلی: استفاده از since parameter برای OHLCV
    """

    def __init__(self, exchange: ccxt.phemex):
        self.ex           = exchange
        self.price_cache: Dict[str, float] = {}
        self.ohlcv_cache: Dict[str, pd.DataFrame] = {}
        self.cache_time:  Dict[str, float] = {}
        self.cache_ttl    = 8
        self.last_good:   Dict[str, float] = {}

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 120
    ) -> Optional[pd.DataFrame]:
        """
        دریافت OHLCV از Phemex
        
        کلید اصلاح: Phemex نیاز به since دارد
        بدون since → خطای 30000
        """
        cache_key = f"{symbol}_{timeframe}"
        now       = time.time()

        # چک کش
        if (cache_key in self.ohlcv_cache and
                now - self.cache_time.get(cache_key, 0) < self.cache_ttl):
            return self.ohlcv_cache[cache_key]

        # محاسبه since بر اساس timeframe
        tf_sec = PHEMEX_TF_MAP.get(timeframe, 300)
        since  = int((now - tf_sec * limit) * 1000)

        try:
            raw = await self.ex.fetch_ohlcv(
                symbol,
                timeframe=timeframe,
                since=since,
                limit=limit,
                params={}
            )

            if not raw or len(raw) < 10:
                cached = self.ohlcv_cache.get(cache_key)
                return cached

            df = pd.DataFrame(
                raw,
                columns=["ts","open","high","low","close","volume"]
            )
            df = df.astype({
                "ts":"int64","open":"float64","high":"float64",
                "low":"float64","close":"float64","volume":"float64"
            })

            if not self._validate(df, symbol):
                return self.ohlcv_cache.get(cache_key)

            # آپدیت کش قیمت
            last_close = float(df["close"].iloc[-1])
            if last_close > 0:
                self.price_cache[symbol] = last_close
                self.last_good[symbol]   = last_close

            self.ohlcv_cache[cache_key] = df
            self.cache_time[cache_key]  = now
            return df

        except Exception as e:
            err = str(e)
            log.error(f"OHLCV {symbol}/{timeframe}: {err[:120]}")

            # اگر خطای آرگومان بود، since را تغییر بده
            if "30000" in err or "input arguments" in err:
                return await self._fetch_fallback(symbol, timeframe, limit)

            return self.ohlcv_cache.get(cache_key)

    async def _fetch_fallback(
        self, symbol: str, timeframe: str, limit: int
    ) -> Optional[pd.DataFrame]:
        """
        روش جایگزین برای Phemex:
        بدون since، با params مختلف
        """
        cache_key = f"{symbol}_{timeframe}"
        attempts  = [
            # تلاش 1: بدون since، فقط limit
            {"limit": limit},
            # تلاش 2: با end time
            {"limit": limit, "end": int(time.time() * 1000)},
            # تلاش 3: limit کمتر
            {"limit": 50},
        ]

        for params in attempts:
            try:
                await asyncio.sleep(0.5)
                raw = await self.ex.fetch_ohlcv(
                    symbol, timeframe=timeframe, params=params)
                if raw and len(raw) >= 10:
                    df = pd.DataFrame(
                        raw,
                        columns=["ts","open","high","low","close","volume"])
                    df = df.astype({
                        "ts":"int64","open":"float64","high":"float64",
                        "low":"float64","close":"float64","volume":"float64"
                    })
                    if self._validate(df, symbol):
                        self.ohlcv_cache[cache_key] = df
                        self.cache_time[cache_key]  = time.time()
                        last_close = float(df["close"].iloc[-1])
                        if last_close > 0:
                            self.price_cache[symbol] = last_close
                        log.info(f"✅ Fallback OHLCV موفق: {symbol}/{timeframe}")
                        return df
            except Exception as e2:
                log.debug(f"Fallback attempt {params}: {e2}")

        return self.ohlcv_cache.get(cache_key)

    def _validate(self, df: pd.DataFrame, symbol: str) -> bool:
        try:
            if len(df) < 5:
                return False
            if not (df["high"] >= df["low"]).all():
                return False
            if (df[["open","high","low","close"]] <= 0).any().any():
                return False
            if (df["volume"] < 0).any():
                return False
            # چک کندل‌های frozen
            if len(df) >= 5 and df["close"].iloc[-5:].nunique() == 1:
                log.warning(f"⚠️ {symbol}: frozen market")
                return False
            return True
        except Exception:
            return False

    async def fetch_all_tickers(self) -> Dict[str, float]:
        prices = {}
        try:
            syms    = SYMBOLS if SYMBOLS else SYMBOLS_CANDIDATE[:5]
            tickers = await self.ex.fetch_tickers(syms)
            for sym, tick in tickers.items():
                p = float(tick.get("last") or tick.get("close") or 0)
                if p > 0:
                    prices[sym]          = p
                    self.price_cache[sym] = p
                    self.last_good[sym]   = p
        except Exception as e:
            log.error(f"fetch_all_tickers: {e}")
        return prices

    def get_price(self, symbol: str) -> Optional[float]:
        price    = self.price_cache.get(symbol)
        last_good = self.last_good.get(symbol)

        if not price or price <= 0:
            return last_good

        if last_good and last_good > 0:
            ratio = price / last_good
            if ratio > 3.0 or ratio < 0.33:
                log.error(f"💀 Spike detected {symbol}: {price} vs {last_good}")
                return last_good

        return price

    async def get_spread_pct(self, symbol: str) -> float:
        try:
            ob   = await self.ex.fetch_order_book(symbol, limit=5)
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
            if bids and asks:
                bid = float(bids[0][0])
                ask = float(asks[0][0])
                mid = (bid + ask) / 2
                return (ask - bid) / mid * 100 if mid > 0 else 999.0
        except Exception:
            pass
        return 0.0


# ============================================================================
# 4. ORPHAN POSITION MANAGER (جدید - اصلاح اصلی)
# ============================================================================
class OrphanPositionManager:
    """
    مدیریت پوزیشن‌های یتیم (Orphan Positions)
    
    پوزیشن یتیم = پوزیشنی که:
    - در Exchange وجود دارد
    - در دیتابیس ربات ثبت نیست
    - یا دستی باز شده
    - یا ربات crash کرده و از دست رفته
    
    روش کار:
    1. هر N ثانیه همه پوزیشن‌های Exchange را بررسی
    2. مقایسه با پوزیشن‌های شناخته‌شده ربات
    3. یتیم‌ها را شناسایی و Adopt کن
    4. SL/TP پیش‌فرض تعیین کن
    5. تحت نظارت Watchdog قرار بده
    """

    def __init__(self, db, tg, exchange: ccxt.phemex,
                 data_feed: PhemexDataFeed):
        self.db         = db
        self.tg         = tg
        self.ex         = exchange
        self.data_feed  = data_feed
        self.open_times: Dict[str, float] = {}

    async def scan_and_adopt(self, known_positions: Dict) -> Dict[str, dict]:
        """
        اسکن Exchange و پیدا کردن یتیم‌ها
        برگشت: dict از پوزیشن‌های یتیم جدید
        """
        new_orphans = {}
        try:
            remote_positions = await self.ex.fetch_positions()
        except Exception as e:
            log.error(f"OrphanScan fetch_positions: {e}")
            return new_orphans

        for rp in remote_positions:
            contracts = float(rp.get("contracts") or
                              rp.get("size") or 0)
            if abs(contracts) < 0.0001:
                continue

            raw_sym  = rp.get("symbol", "")
            side_raw = rp.get("side", "") or ""
            entry    = float(rp.get("entryPrice") or
                             rp.get("entry_price") or 0)

            # پیدا کردن نماد استاندارد
            std_sym = self._normalize_symbol(raw_sym)
            if not std_sym:
                log.warning(f"OrphanScan: نماد ناشناخته {raw_sym}")
                continue

            side = "buy" if side_raw.lower() in ("buy","long") else "sell"

            # آیا این پوزیشن را می‌شناسیم؟
            is_known = any(
                p["symbol"] == std_sym and p["side"] == side
                for p in known_positions.values()
            )
            if is_known:
                continue

            # ─── یتیم پیدا شد! ─────────────────────────────────────
            log.warning(
                f"🔍 یتیم شناسایی شد: {std_sym} | "
                f"{side.upper()} | qty={contracts} | entry={entry}")

            # قیمت فعلی
            current_price = self.data_feed.get_price(std_sym) or entry
            if entry <= 0:
                entry = current_price

            # SL/TP پیش‌فرض بر اساس entry
            if side == "buy":
                sl  = entry * (1 - ORPHAN_SL_PCT / 100)
                tp  = entry * (1 + ORPHAN_TP_PCT / 100)
                tp1 = entry * (1 + ORPHAN_TP1_PCT / 100)
            else:
                sl  = entry * (1 + ORPHAN_SL_PCT / 100)
                tp  = entry * (1 - ORPHAN_TP_PCT / 100)
                tp1 = entry * (1 - ORPHAN_TP1_PCT / 100)

            pid = f"orphan_{uuid.uuid4().hex[:8]}"

            orphan = {
                "id":              pid,
                "symbol":          std_sym,
                "side":            side,
                "strategy":        "Orphan_Adopted",
                "entry":           entry,
                "qty":             abs(contracts),
                "sl":              sl,
                "tp":              tp,
                "tp1":             tp1,
                "is_partial":      0,
                "highest_pnl_pct": 0.0,
                "expected_rr":     ORPHAN_TP_PCT / ORPHAN_SL_PCT,
                "signal_quality":  30.0,
                "is_orphan":       True,
                "adopted_at":      time.time(),
                "raw_symbol":      raw_sym,
            }

            new_orphans[pid] = orphan
            self.open_times[pid] = time.time()

            # ذخیره در DB
            await self.db.insert_trade(orphan)

            # اطلاع‌رسانی
            pnl_est = ((current_price - entry) * abs(contracts)
                       * (1 if side == "buy" else -1))
            await self.tg.send(
                f"🔍 <b>پوزیشن یتیم Adopt شد</b>\n"
                f"نماد: {std_sym}\n"
                f"جهت: {side.upper()} | Qty: {abs(contracts)}\n"
                f"Entry: {entry:.5f}\n"
                f"PnL تخمینی: ${pnl_est:.2f}\n"
                f"SL: {sl:.5f} | TP: {tp:.5f}\n"
                f"⚠️ SL/TP پیش‌فرض {ORPHAN_SL_PCT}%/{ORPHAN_TP_PCT}% اعمال شد")

            with STATE_LOCK:
                SHARED_STATE["stats"]["orphans_adopted"] += 1
                SHARED_STATE["operational"]["orphan_detections"] += 1

            await self.db.log_circuit_breaker(
                std_sym, "orphan_adopted",
                f"qty={contracts}|entry={entry}|side={side}")

        return new_orphans

    def _normalize_symbol(self, raw: str) -> Optional[str]:
        """تبدیل نماد خام Phemex به فرمت استاندارد ccxt"""
        # نمونه‌های خام: "BTCUSDT", "BTC/USDT", "BTCUSD"
        raw = raw.upper().strip()

        # اگر قبلاً استاندارد بود
        all_syms = SYMBOLS or SYMBOLS_CANDIDATE
        if raw in all_syms:
            return raw

        # جستجو بر اساس base currency
        for std in all_syms:
            base = std.split("/")[0]
            if base in raw or raw.startswith(base):
                return std

        return None

    async def close_orphan(self, pid: str, pos: dict,
                            reason: str, engine) -> None:
        """بستن یتیم"""
        await engine.force_close(pid, f"Orphan-{reason}")
        with STATE_LOCK:
            SHARED_STATE["stats"]["orphans_closed"] += 1
            SHARED_STATE["orphan_positions"].pop(pid, None)


# ============================================================================
# 5. DATABASE
# ============================================================================
class Database:
    def __init__(self, path="bot_v16.db"):
        self.path = path

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    symbol TEXT, side TEXT, strategy TEXT,
                    entry_price REAL, qty REAL, original_qty REAL,
                    sl REAL, tp1 REAL, tp REAL,
                    is_partial INTEGER DEFAULT 0,
                    highest_pnl_pct REAL DEFAULT 0,
                    is_orphan INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'open',
                    pnl REAL DEFAULT 0, fees_est REAL DEFAULT 0,
                    net_pnl REAL DEFAULT 0, exit_price REAL DEFAULT 0,
                    slippage_est REAL DEFAULT 0, actual_rr REAL DEFAULT 0,
                    exit_reason TEXT, hold_seconds REAL DEFAULT 0,
                    expected_rr REAL DEFAULT 0, signal_quality REAL DEFAULT 0,
                    rsi_at_entry REAL DEFAULT 0, atr_at_entry REAL DEFAULT 0,
                    htf_trend TEXT DEFAULT '',
                    opened_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT, action TEXT, strategy TEXT,
                    reason TEXT, price REAL, rsi REAL, atr REAL,
                    htf_trend TEXT, signal_quality REAL DEFAULT 0,
                    spread_pct REAL DEFAULT 0, extra TEXT
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS equity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    balance REAL, free REAL, peak REAL,
                    dd REAL, open_pos INTEGER DEFAULT 0,
                    orphan_pos INTEGER DEFAULT 0
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS circuit_breaker_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT, event_type TEXT, detail TEXT
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS operational_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    error_type TEXT, symbol TEXT, message TEXT,
                    resolved INTEGER DEFAULT 0
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS scan_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbols_scanned INTEGER, signals_found INTEGER,
                    signals_executed INTEGER, rejected_spread INTEGER,
                    rejected_no_signal INTEGER, rejected_circuit INTEGER,
                    orphans_found INTEGER DEFAULT 0,
                    scan_duration_ms REAL
                )""")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS symbol_validation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT, status TEXT, reason TEXT
                )""")
            await db.commit()

    # ─── Trade ────────────────────────────────────────────────────────────
    async def insert_trade(self, t: dict):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT OR IGNORE INTO trades
                (id,symbol,side,strategy,entry_price,qty,original_qty,
                 sl,tp1,tp,is_orphan,expected_rr,signal_quality,
                 rsi_at_entry,atr_at_entry,htf_trend)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t["id"],t["symbol"],t["side"],t["strategy"],
                 t["entry"],t["qty"],t["qty"],
                 t["sl"],t["tp1"],t["tp"],
                 int(t.get("is_orphan",False)),
                 t.get("expected_rr",0), t.get("signal_quality",0),
                 t.get("rsi_at_entry",0), t.get("atr_at_entry",0),
                 t.get("htf_trend","")))
            await db.commit()

    async def update_trade(self, tid, qty, sl, partial, hp):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE trades SET qty=?,sl=?,is_partial=?,"
                "highest_pnl_pct=? WHERE id=?",
                (qty,sl,partial,hp,tid))
            await db.commit()

    async def close_trade(self, tid, pnl, fees=0.0, reason="",
                           hold=0.0, exit_price=0.0,
                           actual_rr=0.0, slippage=0.0):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                UPDATE trades SET status='closed',
                pnl=?,fees_est=?,net_pnl=?,exit_price=?,
                slippage_est=?,actual_rr=?,exit_reason=?,
                hold_seconds=?,closed_at=CURRENT_TIMESTAMP
                WHERE id=?""",
                (pnl,fees,pnl-fees,exit_price,slippage,
                 actual_rr,reason,hold,tid))
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
                (symbol,action,strategy,reason,price,rsi,atr,
                 htf,signal_quality,spread_pct,str(extra)[:400]))
            await db.commit()

    async def log_circuit_breaker(self, symbol, event_type, detail):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO circuit_breaker_log "
                "(symbol,event_type,detail) VALUES (?,?,?)",
                (symbol,event_type,detail[:300]))
            await db.commit()

    async def log_operational_error(self, error_type, symbol, message):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO operational_errors "
                "(error_type,symbol,message) VALUES (?,?,?)",
                (error_type,symbol,message[:400]))
            await db.commit()
        with STATE_LOCK:
            SHARED_STATE["operational"]["total_api_errors"] += 1

    async def log_scan_stats(self, scanned, found, executed,
                              rej_sp, rej_sig, rej_cb,
                              orphans, dur_ms):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO scan_stats
                (symbols_scanned,signals_found,signals_executed,
                 rejected_spread,rejected_no_signal,rejected_circuit,
                 orphans_found,scan_duration_ms)
                VALUES (?,?,?,?,?,?,?,?)""",
                (scanned,found,executed,rej_sp,rej_sig,rej_cb,
                 orphans,dur_ms))
            await db.commit()

    async def log_equity(self, balance, free, peak, dd,
                          open_pos, orphan_pos):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO equity
                (balance,free,peak,dd,open_pos,orphan_pos)
                VALUES (?,?,?,?,?,?)""",
                (balance,free,peak,dd,open_pos,orphan_pos))
            await db.commit()

    async def log_symbol_validation(self, symbol, status, reason):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO symbol_validation "
                "(symbol,status,reason) VALUES (?,?,?)",
                (symbol,status,reason))
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
            async with db.execute(
                "SELECT * FROM trades WHERE status='closed' "
                "ORDER BY closed_at DESC LIMIT ?", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_recent_decisions(self, limit=500) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM decisions ORDER BY id DESC LIMIT ?",
                (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_operational_errors(self, limit=50) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM operational_errors "
                "ORDER BY id DESC LIMIT ?", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_scan_stats(self, limit=50) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM scan_stats ORDER BY id DESC LIMIT ?",
                (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_equity_history(self, limit=200) -> List[dict]:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM equity ORDER BY id DESC LIMIT ?",
                (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def update_analytics(self):
        async with aiosqlite.connect(self.path) as db:
            async with db.execute("""
                SELECT symbol,strategy,pnl,fees_est,hold_seconds,
                       expected_rr,actual_rr,entry_price,exit_price,
                       slippage_est,opened_at,is_orphan
                FROM trades WHERE status='closed'""") as c:
                rows = await c.fetchall()
        if not rows:
            return

        pnls   = [r[2] for r in rows]
        fees   = [r[3] for r in rows]
        holds  = [r[4] for r in rows if r[4] and r[4] > 0]
        wins   = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        by_sym = defaultdict(list)
        by_str = defaultdict(list)

        for r in rows:
            by_sym[r[0]].append(r[2])
            by_str[r[1]].append(r[2])

        gp = sum(wins)  if wins   else 0
        gl = abs(sum(losses)) if losses else 0
        pf = gp/gl if gl > 0 else 999.0

        sharpe = 0.0
        if len(pnls) > 1:
            try:
                m = statistics.mean(pnls)
                s = statistics.stdev(pnls)
                sharpe = m/s if s > 0 else 0
            except Exception:
                pass

        def cs(lst):
            if not lst: return {}
            w = [p for p in lst if p > 0]
            l = [p for p in lst if p < 0]
            gp2 = sum(w) if w else 0
            gl2 = abs(sum(l)) if l else 0
            return {
                "trades": len(lst),
                "pnl":    round(sum(lst),3),
                "wr":     round(len(w)/len(lst)*100,1),
                "pf":     round(gp2/gl2,2) if gl2 > 0 else 999.0,
                "avg_win":round(sum(w)/len(w),3) if w else 0,
                "avg_loss":round(sum(l)/len(l),3) if l else 0,
            }

        with STATE_LOCK:
            prev = SHARED_STATE["stats"]
            SHARED_STATE["stats"].update({
                "total_trades":   len(pnls),
                "winning_trades": len(wins),
                "losing_trades":  len(losses),
                "win_rate":       round(len(wins)/len(pnls)*100,1),
                "total_pnl":      round(sum(pnls),3),
                "total_fees":     round(sum(fees),3),
                "net_pnl":        round(sum(pnls)-sum(fees),3),
                "avg_hold_min":   round(sum(holds)/len(holds)/60,1) if holds else 0,
                "max_win":        round(max(pnls),3),
                "max_loss":       round(min(pnls),3),
                "profit_factor":  round(pf,2),
                "sharpe_approx":  round(sharpe,3),
                "by_symbol":      {s:cs(v) for s,v in by_sym.items()},
                "by_strategy":    {s:cs(v) for s,v in by_str.items()},
                # حفظ آمار یتیم
                "orphans_adopted": prev.get("orphans_adopted",0),
                "orphans_closed":  prev.get("orphans_closed",0),
            })

    # ─── Full Report ──────────────────────────────────────────────────────
    async def generate_full_report(self) -> str:
        decisions   = await self.get_recent_decisions(500)
        closed      = await self.get_closed_trades(200)
        open_trades = await self.get_open_trades()
        op_errors   = await self.get_operational_errors(50)
        scan_stats  = await self.get_scan_stats(50)
        equity_hist = await self.get_equity_history(100)

        W   = 72
        sep = "═" * W
        sep2= "─" * W
        lines = []

        def T(txt):
            lines.append("")
            lines.append(sep)
            lines.append(f"  {txt}")
            lines.append(sep)

        def S(txt):
            lines.append("")
            lines.append(f"  ▶ {txt}")
            lines.append(sep2)

        def R(label, value, indent=4):
            lines.append(f"{'':>{indent}}{label:<32}: {value}")

        # Header
        lines.append(sep)
        lines.append(" "*8 + "MASTER QUANT ENGINE v16.0 - FULL DIAGNOSTIC")
        lines.append(" "*8 + f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        lines.append(sep)

        with STATE_LOCK:
            st    = dict(SHARED_STATE)
            stats = dict(st.get("stats",{}))
            op    = dict(st.get("operational",{}))

        up_sec = time.time() - op.get("uptime_start", time.time())

        # ══ 1. Executive Summary ══════════════════════════════════
        T("📋 SECTION 1: EXECUTIVE SUMMARY")
        R("نسخه",          "v16.0 | Phemex Native | Orphan Manager")
        R("Uptime",        str(timedelta(seconds=int(up_sec))))
        R("Mode",          "TESTNET" if TESTNET else "LIVE")
        R("نمادهای معتبر", str(len(SYMBOLS)) + ": " + " ".join(s.split("/")[0] for s in SYMBOLS))
        R("MAX_POS",       str(MAX_POS))
        lines.append("")
        R("موجودی",        f"${st.get('balance',0):.2f}")
        R("موجودی آزاد",   f"${st.get('free_balance',0):.2f}")
        R("Peak",          f"${st.get('peak_balance',0):.2f}")
        R("Drawdown",      f"{st.get('current_dd',0):.2f}%")
        R("Daily PnL",     f"${st.get('daily_pnl',0):.2f}")
        R("پوزیشن‌های باز", str(len(st.get('active_positions',{}))))
        R("پوزیشن یتیم",   str(len(st.get('orphan_positions',{}))))
        lines.append("")
        R("کل معاملات",    str(stats.get('total_trades',0)))
        R("Win Rate",      f"{stats.get('win_rate',0):.1f}%")
        R("Net PnL",       f"${stats.get('net_pnl',0):.3f}")
        R("Profit Factor", str(stats.get('profit_factor',0)))
        R("Sharpe",        str(stats.get('sharpe_approx',0)))
        R("Max Win",       f"${stats.get('max_win',0):.3f}")
        R("Max Loss",      f"${stats.get('max_loss',0):.3f}")
        R("یتیم Adopt",    str(stats.get('orphans_adopted',0)))
        R("یتیم بسته",     str(stats.get('orphans_closed',0)))

        # ══ 2. Orphan Position Report ════════════════════════════
        T("👻 SECTION 2: ORPHAN POSITION REPORT")

        orphans_open = [t for t in open_trades if t.get("is_orphan")]
        orphans_cls  = [t for t in closed if t.get("strategy") == "Orphan_Adopted"]

        S("یتیم‌های فعال")
        if orphans_open:
            lines.append(
                f"  {'نماد':<20} {'جهت':<6} {'ورود':>10} "
                f"{'Qty':>10} {'SL':>10} {'TP':>10}")
            lines.append("  " + "─" * 68)
            for t in orphans_open:
                lines.append(
                    f"  {t['symbol']:<20} {t['side'].upper():<6} "
                    f"{t['entry_price']:>10.5f} {t['qty']:>10.4f} "
                    f"{t['sl']:>10.5f} {t['tp']:>10.5f}")
        else:
            lines.append("  ✅ هیچ یتیم فعالی نیست")

        S("یتیم‌های بسته‌شده")
        if orphans_cls:
            for t in orphans_cls[:20]:
                e = "✅" if t["pnl"] > 0 else "❌"
                lines.append(
                    f"  {e} {t['symbol']:<20} {t['side']:<6} "
                    f"PnL:${t['pnl']:>+8.3f}  "
                    f"علت: {t.get('exit_reason','')}")
        else:
            lines.append("  هنوز یتیمی بسته نشده")

        S("تشخیص ایرادات یتیم")
        if stats.get('orphans_adopted',0) > 5:
            lines.append("  🔴 تعداد زیاد یتیم → ربات مکرراً crash می‌کند")
            lines.append("     یا معاملات دستی زیادی انجام می‌شود")
        elif stats.get('orphans_adopted',0) > 0:
            lines.append("  ⚠️  پوزیشن یتیم شناسایی شد")
            lines.append("     بررسی کنید چرا ربات این پوزیشن را نمی‌شناخت")
        else:
            lines.append("  ✅ هیچ پوزیشن یتیمی شناسایی نشده")

        # ══ 3. Symbol Validation ════════════════════════════════
        T("🔍 SECTION 3: SYMBOL VALIDATION")
        R("نمادهای معتبر",  str(len(SYMBOLS)))
        R("نمادهای رد‌شده",
          str(len(SYMBOLS_CANDIDATE) - len(SYMBOLS)))
        lines.append("")
        lines.append("  نمادهای معتبر:")
        for s in SYMBOLS:
            lines.append(f"    ✅ {s}")
        lines.append("")
        rejected = [s for s in SYMBOLS_CANDIDATE if s not in SYMBOLS]
        if rejected:
            lines.append("  نمادهای رد‌شده:")
            for s in rejected:
                lines.append(f"    ❌ {s}")

        # ══ 4. Strategy Analysis ════════════════════════════════
        T("🧠 SECTION 4: STRATEGY HEALTH")

        S("عملکرد استراتژی‌ها")
        by_str = stats.get("by_strategy",{})
        if by_str:
            lines.append(
                f"  {'استراتژی':<25} {'معاملات':>8} "
                f"{'WR%':>7} {'PnL':>10} {'PF':>6} "
                f"{'AvgW':>8} {'AvgL':>8}")
            lines.append("  " + "─" * 75)
            for strat,sv in sorted(
                by_str.items(),
                key=lambda x:x[1].get("pnl",0),reverse=True):
                icon = "🟢" if sv.get("pnl",0)>=0 else "🔴"
                lines.append(
                    f"  {icon} {strat:<23} {sv['trades']:>8} "
                    f"{sv.get('wr',0):>6.1f}% "
                    f"{sv.get('pnl',0):>+9.3f} "
                    f"{sv.get('pf',0):>6.2f} "
                    f"{sv.get('avg_win',0):>+7.3f} "
                    f"{sv.get('avg_loss',0):>+7.3f}")
        else:
            lines.append("  هنوز معامله بسته‌شده‌ای نیست")

        S("تشخیص خودکار")
        total_t = stats.get('total_trades',0)
        wr      = stats.get('win_rate',0)
        pf      = stats.get('profit_factor',0)

        issues = []
        warns  = []
        oks    = []

        if total_t >= 10:
            if wr < 35:
                issues.append(f"❌ Win Rate {wr}% خیلی پایین (هدف >40%)")
            elif wr < 45:
                warns.append(f"⚠️ Win Rate {wr}% پایین")
            else:
                oks.append(f"✅ Win Rate {wr}%")

            if pf < 1.0:
                issues.append(f"❌ Profit Factor {pf} < 1.0 → زیانده")
            elif pf < 1.3:
                warns.append(f"⚠️ Profit Factor {pf} پایین")
            else:
                oks.append(f"✅ Profit Factor {pf}")
        else:
            warns.append(f"⚠️ {total_t} معامله - داده ناکافی")

        for x in issues: lines.append(f"  {x}")
        for x in warns:  lines.append(f"  {x}")
        for x in oks:    lines.append(f"  {x}")

        # ══ 5. Operational Health ════════════════════════════════
        T("⚙️ SECTION 5: OPERATIONAL HEALTH")

        S("آمار کلی")
        R("کل خطاهای API",    str(op.get('total_api_errors',0)))
        R("Position Mode Fix",str(op.get('position_mode_fixes',0)))
        R("Circuit Breaker",  str(op.get('circuit_breaker_events',0)))
        R("Orphan Detection", str(op.get('orphan_detections',0)))
        R("Sync Count",       str(op.get('sync_count',0)))
        R("Scan Count",       str(st.get('scan_count',0)))

        # آمار OHLCV errors از scan_stats
        if scan_stats:
            S("آمار اسکن‌ها")
            t_sc   = sum(s.get("symbols_scanned",0) for s in scan_stats)
            t_fn   = sum(s.get("signals_found",0)   for s in scan_stats)
            t_ex   = sum(s.get("signals_executed",0) for s in scan_stats)
            t_rsp  = sum(s.get("rejected_spread",0)  for s in scan_stats)
            t_rsig = sum(s.get("rejected_no_signal",0) for s in scan_stats)
            t_rcb  = sum(s.get("rejected_circuit",0) for s in scan_stats)
            t_orp  = sum(s.get("orphans_found",0)    for s in scan_stats)
            durs   = [s.get("scan_duration_ms",0) for s in scan_stats if s.get("scan_duration_ms")]
            avg_d  = sum(durs)/len(durs) if durs else 0

            R("کل اسکن",          str(len(scan_stats)))
            R("نمادهای اسکن‌شده", str(t_sc))
            R("سیگنال یافت",      str(t_fn))
            R("اجراشده",          str(t_ex))
            R("رد - Spread",       str(t_rsp))
            R("رد - بدون سیگنال",  str(t_rsig))
            R("رد - Circuit",      str(t_rcb))
            R("یتیم‌ها",           str(t_orp))
            R("میانگین زمان اسکن",f"{avg_d:.0f}ms")

        # ══ 6. Open Positions ════════════════════════════════════
        T("🟢 SECTION 6: OPEN POSITIONS")
        regular = [t for t in open_trades if not t.get("is_orphan")]
        if regular:
            for t in regular:
                icon = "🟢" if t["side"] == "buy" else "🔴"
                lines.append(
                    f"  {icon} {t['symbol']:<20} "
                    f"{t['side'].upper():<6} "
                    f"Entry:{t['entry_price']:.5f} "
                    f"Qty:{t['qty']:.4f} "
                    f"Strat:{t.get('strategy','?')}")
        else:
            lines.append("  هیچ پوزیشن منظمی باز نیست")

        # ══ 7. Closed Trades ═════════════════════════════════════
        T("📈 SECTION 7: CLOSED TRADES (Last 50)")
        if closed:
            lines.append(
                f"  {'نماد':<20} {'جهت':<5} "
                f"{'PnL':>9} {'RR':>6} "
                f"{'Hold':>7} {'علت':<15} "
                f"{'استراتژی':<22} {'یتیم?'}")
            lines.append("  " + "─" * 90)
            for t in closed[:50]:
                e    = "✅" if t["pnl"] > 0 else "❌"
                hold = int((t.get("hold_seconds") or 0)/60)
                orp  = "👻" if t.get("is_orphan") else ""
                lines.append(
                    f"  {e} {t['symbol']:<18} "
                    f"{t['side']:<5} "
                    f"{t['pnl']:>+8.3f} "
                    f"{t.get('actual_rr',0) or 0:>6.2f} "
                    f"{hold:>6}m "
                    f"{(t.get('exit_reason','') or ''):<15} "
                    f"{t.get('strategy',''):<22} {orp}")
        else:
            lines.append("  هنوز معامله‌ای بسته نشده")

        # ══ 8. Decisions ══════════════════════════════════════════
        T("🔍 SECTION 8: SIGNAL ANALYSIS")
        if decisions:
            actions   = Counter(d["action"] for d in decisions)
            signals   = sum(v for k,v in actions.items() if k!="neutral")
            rejected  = actions.get("neutral",0)
            total_d   = len(decisions)

            S("خلاصه تصمیمات")
            R("کل تصمیمات",  str(total_d))
            R("سیگنال ورود",  str(signals))
            R("رد شده",       str(rejected))
            R("نرخ سیگنال",   f"{signals/total_d*100:.1f}%" if total_d else "0%")

            S("دلایل رد شدن")
            reasons = Counter()
            for d in decisions:
                if d["action"] == "neutral":
                    r = (d.get("reason") or "unknown")[:45]
                    reasons[r] += 1
            for reason,cnt in reasons.most_common(12):
                pct = cnt/rejected*100 if rejected else 0
                bar = "█"*int(pct/4)
                lines.append(
                    f"  {cnt:>5} ({pct:>5.1f}%)  "
                    f"{reason:<40} {bar}")

        # ══ 9. Errors ══════════════════════════════════════════════
        T("⚠️ SECTION 9: OPERATIONAL ERRORS")
        if op_errors:
            for e in op_errors[:20]:
                ts = (e.get("ts",""))[:16]
                lines.append(
                    f"  [{ts}] {e.get('error_type','?'):<20} "
                    f"{e.get('symbol',''):<18} "
                    f"{e.get('message','')[:55]}")
        else:
            lines.append("  ✅ هیچ خطای عملیاتی ثبت نشده")

        # ══ 10. Recommendations ════════════════════════════════════
        T("💡 SECTION 10: RECOMMENDATIONS")
        all_issues = issues[:]
        if op.get("total_api_errors",0) > 20:
            all_issues.append("❌ خطاهای API زیاد - Rate Limit یا اتصال")
        if op.get("orphan_detections",0) > 5:
            all_issues.append("❌ یتیم‌های زیاد - ربات crash می‌کند")
        if op.get("position_mode_fixes",0) > 3:
            all_issues.append("❌ Position Mode مکرراً اشتباه است")

        if not all_issues:
            lines.append("  ✅ ربات در وضعیت سالم")
        else:
            for i,iss in enumerate(all_issues,1):
                lines.append(f"  {i}. {iss}")

        # Footer
        lines.append("")
        lines.append(sep)
        lines.append(f"  v16.0 | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        lines.append(sep)
        return "\n".join(lines)


# ============================================================================
# 6. INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(s,n=14):
        d=s.diff();u=d.clip(lower=0);dn=-d.clip(upper=0)
        mu=u.ewm(com=n-1,adjust=False).mean()
        md=dn.ewm(com=n-1,adjust=False).mean()
        return 100-(100/(1+mu/md.replace(0,1e-10)))

    @staticmethod
    def atr(df,n=14):
        tr=pd.concat([
            df["high"]-df["low"],
            (df["high"]-df["close"].shift()).abs(),
            (df["low"]-df["close"].shift()).abs()
        ],axis=1).max(axis=1)
        return tr.ewm(com=n-1,adjust=False).mean()

    @staticmethod
    def ema(s,span):
        return s.ewm(span=span,adjust=False).mean()

    @staticmethod
    def sma(s,p): return s.rolling(p).mean()

    @staticmethod
    def supertrend(df,period=10,mult=3.0):
        atr=Indicators.atr(df,period)
        hl2=(df["high"]+df["low"])/2
        up=hl2+mult*atr; lo=hl2-mult*atr
        d=pd.Series(1,index=df.index)
        for i in range(1,len(df)):
            if df["close"].iloc[i]>up.iloc[i-1]: d.iloc[i]=1
            elif df["close"].iloc[i]<lo.iloc[i-1]: d.iloc[i]=-1
            else:
                d.iloc[i]=d.iloc[i-1]
                if d.iloc[i]==1 and lo.iloc[i]<lo.iloc[i-1]: lo.iloc[i]=lo.iloc[i-1]
                if d.iloc[i]==-1 and up.iloc[i]>up.iloc[i-1]: up.iloc[i]=up.iloc[i-1]
        return d,up,lo

    @staticmethod
    def macd(s,fast=12,slow=26,sig=9):
        m=s.ewm(span=fast,adjust=False).mean()-s.ewm(span=slow,adjust=False).mean()
        sl=m.ewm(span=sig,adjust=False).mean()
        return m,sl,m-sl

    @staticmethod
    def highest(s,p): return s.rolling(p).max()
    @staticmethod
    def lowest(s,p):  return s.rolling(p).min()


# ============================================================================
# 7. STRATEGY ENGINE
# ============================================================================
class StrategyEngine:
    def analyze(self, df5: pd.DataFrame, df1: pd.DataFrame, sym: str) -> dict:
        df  = df5.iloc[:-1].copy()
        htf = df1.iloc[:-1].copy()
        if len(df)<60 or len(htf)<30:
            return self._n("داده ناکافی")

        hc   = htf["close"]
        e50h = Indicators.ema(hc,50).iloc[-1]
        e200h= Indicators.ema(hc,min(200,len(htf))).iloc[-1]
        hp   = float(hc.iloc[-1])

        if   hp>e50h and e50h>e200h*0.998: htf_t="bullish"
        elif hp<e50h and e50h<e200h*1.002: htf_t="bearish"
        else: return self._n("HTF نامشخص",htf="sideways")

        c    = df["close"]; high=df["high"]; low=df["low"]; vol=df["volume"]
        px   = float(c.iloc[-1])
        atr_s= Indicators.atr(df,14)
        atr  = float(atr_s.iloc[-1])
        if atr<=0: return self._n("ATR صفر",htf=htf_t)

        atr_sma = float(Indicators.sma(atr_s,20).iloc[-1])
        cfg      = SYMBOL_CONFIG.get(sym,{})
        mn       = px*cfg.get("min_atr_pct",0.05)/100
        mx       = px*cfg.get("max_atr_pct",5.0)/100
        if atr<mn or atr>mx:
            return self._n(f"ATR خارج محدوده",atr=atr,htf=htf_t)

        rsi  = Indicators.rsi(c)
        rv   = float(rsi.iloc[-1]); rp=float(rsi.iloc[-2])
        e20  = float(Indicators.ema(c,20).iloc[-1])
        e50  = float(Indicators.ema(c,50).iloc[-1])
        st_d,st_u,st_l = Indicators.supertrend(df)
        _,_,mh = Indicators.macd(c)
        vsma = float(Indicators.sma(vol,20).iloc[-1])
        vc   = float(vol.iloc[-1])
        h10  = float(Indicators.highest(high,10).iloc[-1])
        l10  = float(Indicators.lowest(low,10).iloc[-1])
        mv   = cfg.get("min_vol_mult",1.1)

        # S1: Breakout
        if htf_t=="bullish" and px>e20 and px>=h10*0.999 and 48<rv<75 and vc>vsma*mv and float(mh.iloc[-1])>0:
            return self._b("buy","Breakout_Momentum",px,atr,rv,htf_t)
        if htf_t=="bearish" and px<e20 and px<=l10*1.001 and 25<rv<52 and vc>vsma*mv and float(mh.iloc[-1])<0:
            return self._b("sell","Breakout_Momentum",px,atr,rv,htf_t)

        # S2: Pullback
        if htf_t=="bullish" and px>e20>e50*0.999 and rp<=42 and rv>rp and rv<62:
            return self._b("buy","MTF_Pullback",px,atr,rv,htf_t)
        if htf_t=="bearish" and px<e20<e50*1.001 and rp>=58 and rv<rp and rv>38:
            return self._b("sell","MTF_Pullback",px,atr,rv,htf_t)

        # S3: SuperTrend
        if htf_t=="bullish" and st_d.iloc[-1]==1 and low.iloc[-1]<=st_l.iloc[-1]*1.005 and c.iloc[-1]>c.iloc[-2] and 38<rv<65:
            return self._b("buy","SuperTrend_Pullback",px,atr,rv,htf_t)
        if htf_t=="bearish" and st_d.iloc[-1]==-1 and high.iloc[-1]>=st_u.iloc[-1]*0.995 and c.iloc[-1]<c.iloc[-2] and 35<rv<62:
            return self._b("sell","SuperTrend_Pullback",px,atr,rv,htf_t)

        # S4: Volume
        if htf_t=="bullish" and px>e20 and vc>vsma*1.5 and c.iloc[-1]>c.iloc[-2] and 48<rv<70:
            return self._b("buy","Volume_Surge",px,atr,rv,htf_t)
        if htf_t=="bearish" and px<e20 and vc>vsma*1.5 and c.iloc[-1]<c.iloc[-2] and 30<rv<52:
            return self._b("sell","Volume_Surge",px,atr,rv,htf_t)

        # S5: EMA Cross
        e20p = float(Indicators.ema(c,20).iloc[-2])
        e50p = float(Indicators.ema(c,50).iloc[-2])
        if htf_t=="bullish" and e20p<=e50p and e20>e50 and rv>45:
            return self._b("buy","EMA_Cross",px,atr,rv,htf_t)
        if htf_t=="bearish" and e20p>=e50p and e20<e50 and rv<55:
            return self._b("sell","EMA_Cross",px,atr,rv,htf_t)

        return self._n(f"بدون سیگنال RSI={rv:.1f}",rsi=rv,atr=atr,htf=htf_t)

    def _n(self,reason,rsi=0,atr=0,htf=""):
        return {"action":"neutral","reason":reason,"strat":"","rsi":rsi,"atr":atr,"htf":htf,"signal_quality":0}

    def _b(self,side,strat,price,atr,rsi,htf):
        p  = STRATEGY_PARAMS.get(strat,{"sl_m":1.5,"tp_m":2.8,"tp1_m":1.4})
        sm,tm,t1m = p["sl_m"],p["tp_m"],p["tp1_m"]
        rr = round(tm/sm,2)
        if side=="buy":
            return {"action":"buy","strat":strat,
                    "sl":price-atr*sm,"tp":price+atr*tm,"tp1":price+atr*t1m,
                    "reason":f"سیگنال {strat}","rsi":rsi,"atr":atr,"htf":htf,
                    "expected_rr":rr,"signal_quality":55.0}
        return {"action":"sell","strat":strat,
                "sl":price+atr*sm,"tp":price-atr*tm,"tp1":price-atr*t1m,
                "reason":f"سیگنال {strat}","rsi":rsi,"atr":atr,"htf":htf,
                "expected_rr":rr,"signal_quality":55.0}


# ============================================================================
# 8. RISK MANAGER
# ============================================================================
class RiskManager:
    @staticmethod
    def calculate_qty(balance,price,sl,free,symbol,exchange,sq=50):
        if price<=0 or balance<=0: return 0.0
        dist=abs(price-sl)
        if dist<=0: return 0.0
        qm   = 0.5+(sq/100.0)
        risk = balance*(RISK_PCT/100.0)*qm
        qty  = risk/dist
        cfg  = SYMBOL_CONFIG.get(symbol,{})
        m1   = (free*0.15*LEVERAGE)/price
        m2   = (balance*MAX_SINGLE_EXPOSURE/100.0)/price
        m3   = cfg.get("max_usd_pos",200.0)/price
        qty  = min(qty,m1,m2,m3)
        try:
            qty=float(exchange.amount_to_precision(symbol,qty))
            if qty*price<MIN_ORDER_USD:
                qty=float(exchange.amount_to_precision(symbol,MIN_ORDER_USD/price))
        except Exception:
            return 0.0
        return max(qty,0.0)

    @staticmethod
    def check_global() -> Tuple[bool,str]:
        with STATE_LOCK:
            s=dict(SHARED_STATE)
        if s["dd_halted"]:     return False,f"DD Halt {s['current_dd']:.1f}%"
        if s["daily_halted"]:  return False,"Daily Loss Halt"
        if not s["is_active"]: return False,"ربات متوقف"
        if len(s["active_positions"])>=MAX_POS: return False,f"MAX_POS={MAX_POS}"
        if s["balance"]<20:    return False,"موجودی ناکافی"
        return True,""


# ============================================================================
# 9. CIRCUIT BREAKER
# ============================================================================
class CircuitBreaker:
    def __init__(self,db):
        self.db=db

    def is_allowed(self,symbol:str)->Tuple[bool,str]:
        now=time.time()
        with STATE_LOCK:
            if SHARED_STATE["symbol_cooldowns"].get(symbol,0)>now:
                rem=int((SHARED_STATE["symbol_cooldowns"][symbol]-now)/60)
                return False,f"Loss Cooldown {rem}min"
            err=SHARED_STATE["symbol_errors"].get(symbol,{})
            if err.get("cooldown_end",0)>now:
                rem=int((err["cooldown_end"]-now)/60)
                return False,f"API Cooldown {rem}min"
        return True,""

    async def reg_error(self,symbol,error):
        with STATE_LOCK:
            e=SHARED_STATE["symbol_errors"]
            if symbol not in e: e[symbol]={"count":0,"cooldown_end":0}
            e[symbol]["count"]+=1
            n=e[symbol]["count"]
            cd=min(30*(2**(n-1)),MAX_ERROR_COOLDOWN)
            e[symbol]["cooldown_end"]=time.time()+cd
            e[symbol]["last_error"]=error[:100]
        await self.db.log_circuit_breaker(symbol,"api_error",f"n={n}|{error[:80]}")
        await self.db.log_operational_error("api_error",symbol,error[:200])

    async def reg_loss(self,symbol,pnl)->bool:
        with STATE_LOCK:
            cl=SHARED_STATE["consecutive_losses"]
            if symbol not in cl: cl[symbol]={"count":0,"last_loss":0}
            cl[symbol]["count"]+=1
            cl[symbol]["last_loss"]=time.time()
            n=cl[symbol]["count"]
            if n>=CONSECUTIVE_LOSS_LIMIT:
                SHARED_STATE["symbol_cooldowns"][symbol]=time.time()+SYMBOL_COOLDOWN_HOURS*3600
                SHARED_STATE["operational"]["circuit_breaker_events"]+=1
        if n>=CONSECUTIVE_LOSS_LIMIT:
            await self.db.log_circuit_breaker(symbol,"consec_loss",f"n={n}|pnl={pnl:.3f}")
            return True
        return False

    def reg_win(self,symbol):
        with STATE_LOCK:
            SHARED_STATE["consecutive_losses"].pop(symbol,None)
            if symbol in SHARED_STATE["symbol_errors"]:
                SHARED_STATE["symbol_errors"][symbol]["count"]=0
                SHARED_STATE["symbol_errors"][symbol]["cooldown_end"]=0

    async def fix_pm(self,exchange,symbol)->bool:
        try:
            await exchange.set_position_mode(False,symbol)
            with STATE_LOCK:
                SHARED_STATE["operational"]["position_mode_fixes"]+=1
            return True
        except Exception as e:
            log.warning(f"fix_pm {symbol}: {e}")
        return False


# ============================================================================
# 10. TELEGRAM
# ============================================================================
class TelegramController:
    def __init__(self,engine):
        self.engine=engine
        self.base=f"https://api.telegram.org/bot{TG_TOKEN}"
        self.offset=0

    def menu(self):
        btn="⏸️ Pause" if SHARED_STATE["is_active"] else "▶️ Start"
        act="cmd_pause" if SHARED_STATE["is_active"] else "cmd_start"
        return {"inline_keyboard":[
            [{"text":"📊 Dashboard","callback_data":"cmd_dash"},
             {"text":"💼 Positions","callback_data":"cmd_pos"}],
            [{"text":"👻 Orphans","callback_data":"cmd_orphan"},
             {"text":"🔄 Sync","callback_data":"cmd_sync"}],
            [{"text":btn,"callback_data":act},
             {"text":"📈 Stats","callback_data":"cmd_stats"}],
            [{"text":"🔴 Circuit","callback_data":"cmd_cb"},
             {"text":"⚠️ Errors","callback_data":"cmd_err"}],
            [{"text":"⚡ Real Test","callback_data":"cmd_test"}],
            [{"text":"📄 Full Report","callback_data":"cmd_txt"}],
        ]}

    async def send(self,text,markup=None):
        if not TG_TOKEN: return
        if len(text)>4000: text=text[:3900]+"\n..."
        p={"chat_id":TG_CHAT,"text":text,"parse_mode":"HTML"}
        if markup: p["reply_markup"]=markup
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"{self.base}/sendMessage",json=p,
                             timeout=aiohttp.ClientTimeout(total=12))
        except Exception as e:
            log.error(f"TG: {e}")

    async def send_doc(self,path,caption=""):
        if not os.path.exists(path):
            await self.send("❌ فایل نبود"); return
        try:
            with open(path,"rb") as f:
                form=aiohttp.FormData()
                form.add_field("chat_id",TG_CHAT)
                form.add_field("caption",caption[:1000])
                form.add_field("document",f,filename=os.path.basename(path))
                async with aiohttp.ClientSession() as s:
                    await s.post(f"{self.base}/sendDocument",data=form,
                                 timeout=aiohttp.ClientTimeout(total=60))
        except Exception as e:
            await self.send(f"❌ ارسال فایل: {e}")

    async def poll(self):
        if not TG_TOKEN: return
        await self.send(
            "🚀 <b>Master Quant v16.0</b>\n"
            "Phemex Native | Orphan Manager | Symbol Validator\n"
            f"✅ {len(SYMBOLS)} نماد معتبر | MAX_POS={MAX_POS}",
            self.menu())

        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{self.base}/getUpdates?offset={self.offset+1}&timeout=8",
                        timeout=aiohttp.ClientTimeout(total=15)) as r:
                        data=await r.json()
                for u in data.get("result",[]):
                    self.offset=u["update_id"]
                    if "callback_query" not in u: continue
                    cb=u["callback_query"]
                    try:
                        async with aiohttp.ClientSession() as ss:
                            await ss.post(f"{self.base}/answerCallbackQuery",
                                json={"callback_query_id":cb["id"],"text":"⏳"},
                                timeout=aiohttp.ClientTimeout(total=4))
                    except Exception: pass
                    await self._cmd(cb["data"])
            except Exception as e:
                log.error(f"TG poll: {e}")
            await asyncio.sleep(1)

    async def _cmd(self,cmd):
        eng=self.engine
        if cmd=="cmd_start":
            with STATE_LOCK: SHARED_STATE["is_active"]=True
            await self.send("▶️ فعال",self.menu())
        elif cmd=="cmd_pause":
            with STATE_LOCK: SHARED_STATE["is_active"]=False
            await self.send("⏸️ متوقف",self.menu())
        elif cmd=="cmd_dash":
            with STATE_LOCK: st=dict(SHARED_STATE)
            orp_cnt=len(st.get("orphan_positions",{}))
            await self.send(
                f"📊 <b>Dashboard v16.0</b>\n"
                f"💰 ${st['balance']:.2f} | آزاد:${st['free_balance']:.2f}\n"
                f"📉 DD:{st['current_dd']:.2f}% | Daily:${st['daily_pnl']:.2f}\n"
                f"📦 Pos:{len(st['active_positions'])}/{MAX_POS} | Orphan:🔍{orp_cnt}\n"
                f"🎯 WR:{st['stats']['win_rate']}% | PnL:${st['stats']['net_pnl']:.2f}\n"
                f"📡 اسکن:{st['last_scan']} #{st['scan_count']}",
                self.menu())
        elif cmd=="cmd_pos":
            with STATE_LOCK:
                pos=dict(SHARED_STATE["active_positions"])
                orp=dict(SHARED_STATE["orphan_positions"])
            msg="💼 <b>پوزیشن‌ها:</b>\n\n"
            all_pos={**pos,**orp}
            if not all_pos:
                msg+="💤 هیچ پوزیشنی نیست"
            else:
                for p in all_pos.values():
                    pr=eng.data_feed.get_price(p["symbol"]) or p["entry"]
                    pnl=((pr-p["entry"])*p["qty"]*(1 if p["side"]=="buy" else -1))
                    ico="🟢" if pnl>=0 else "🔴"
                    orp_tag="👻" if p.get("is_orphan") else ""
                    msg+=(f"{ico}{orp_tag} <b>{p['symbol'].split('/')[0]}</b> "
                          f"{p['side'].upper()}\n"
                          f"  Entry:{p['entry']:.4f} PnL:${pnl:.2f}\n"
                          f"  SL:{p['sl']:.4f} TP:{p['tp']:.4f}\n\n")
            await self.send(msg,self.menu())
        elif cmd=="cmd_orphan":
            with STATE_LOCK:
                orp=dict(SHARED_STATE["orphan_positions"])
                stats=dict(SHARED_STATE["stats"])
            msg=(f"👻 <b>Orphan Manager</b>\n\n"
                 f"فعال: {len(orp)}\n"
                 f"Adopt شده: {stats.get('orphans_adopted',0)}\n"
                 f"بسته شده: {stats.get('orphans_closed',0)}\n\n")
            if orp:
                for p in orp.values():
                    msg+=f"• {p['symbol'].split('/')[0]} {p['side'].upper()} {p['qty']}\n"
            else:
                msg+="✅ هیچ یتیمی نیست"
            await self.send(msg,self.menu())
        elif cmd=="cmd_sync":
            await eng.smart_sync()
            with STATE_LOCK:
                SHARED_STATE["operational"]["sync_count"]+=1
            await self.send("🔄 Sync OK",self.menu())
        elif cmd=="cmd_stats":
            with STATE_LOCK: s=dict(SHARED_STATE["stats"])
            await self.send(
                f"📈 <b>Stats v16.0</b>\n"
                f"معاملات: {s.get('total_trades',0)}\n"
                f"WR: {s.get('win_rate',0):.1f}%\n"
                f"Net PnL: ${s.get('net_pnl',0):.3f}\n"
                f"PF: {s.get('profit_factor',0):.2f}\n"
                f"Sharpe: {s.get('sharpe_approx',0):.3f}\n"
                f"یتیم Adopt: {s.get('orphans_adopted',0)}",
                self.menu())
        elif cmd=="cmd_cb":
            now=time.time()
            with STATE_LOCK:
                cds=dict(SHARED_STATE["symbol_cooldowns"])
                cls=dict(SHARED_STATE["consecutive_losses"])
            msg="🔴 <b>Circuit Breaker</b>\n\n"
            ac={s:v for s,v in cds.items() if v>now}
            msg+="Active:\n"+(
                "\n".join(f"  ⛔{s.split('/')[0]}: {int((v-now)/60)}min"
                           for s,v in ac.items()) or "  ✅ None")+"\n\n"
            msg+="Losses:\n"+(
                "\n".join(f"  {s.split('/')[0]}: {v['count']}"
                           for s,v in cls.items()) or "  ✅ Clear")
            await self.send(msg,self.menu())
        elif cmd=="cmd_err":
            errs=await eng.db.get_operational_errors(10)
            msg="⚠️ <b>آخرین خطاها:</b>\n\n"
            for e in errs:
                msg+=f"[{(e.get('ts',''))[:16]}] {e.get('message','')[:60]}\n\n"
            if not errs: msg+="✅ هیچ خطایی نیست"
            await self.send(msg,self.menu())
        elif cmd=="cmd_test":
            asyncio.create_task(eng.real_test_trade())
        elif cmd=="cmd_txt":
            await self.send("⏳ در حال تهیه گزارش...")
            report=await eng.db.generate_full_report()
            fname=f"quant_v16_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(fname,"w",encoding="utf-8") as f:
                f.write(report)
            await self.send_doc(fname,f"📄 v16.0 | {os.path.getsize(fname)//1024}KB")


# ============================================================================
# 11. QUANT ENGINE
# ============================================================================
class QuantEngine:
    def __init__(self):
        self.db       = Database()
        self.strategy = StrategyEngine()
        self.risk     = RiskManager()
        self.cb       = CircuitBreaker(self.db)

        # ─── Phemex (تنها منبع) ──────────────────────────────────────
        cfg = {
            "apiKey":API_KEY,"secret":API_SECRET,
            "enableRateLimit":True,
            "options":{"defaultType":"swap"},
            "timeout":30000,
        }
        if TESTNET:
            cfg["urls"] = {
                "api":{
                    "public":PHEMEX_TESTNET_REST,
                    "private":PHEMEX_TESTNET_REST,
                }
            }

        self.ex        = ccxt.phemex(cfg)
        self.ex.set_sandbox_mode(TESTNET)

        self.data_feed = PhemexDataFeed(self.ex)
        self.validator = SymbolValidator(self.ex)
        self.tg        = TelegramController(self)
        self.orphan_mgr= None  # بعد از init

        self.prices:     Dict[str,float] = {}
        self.open_times: Dict[str,float] = {}

        with STATE_LOCK:
            SHARED_STATE["data_source"]   = ("Phemex Testnet"
                                              if TESTNET else "Phemex Live")
            SHARED_STATE["phemex_status"] = "init"

    async def start(self):
        global SYMBOLS
        await self.db.init()
        log.info("🚀 Master Quant v16.0")
        log.info(f"   Mode: {'TESTNET' if TESTNET else 'LIVE'}")

        # ─── اتصال ───────────────────────────────────────────────────
        try:
            await self.ex.load_markets()
            with STATE_LOCK:
                SHARED_STATE["phemex_status"]="connected"
            log.info("✅ Phemex connected")
        except Exception as e:
            log.error(f"Connect: {e}")
            with STATE_LOCK:
                SHARED_STATE["phemex_status"]="error"

        # ─── اعتبارسنجی نمادها ───────────────────────────────────────
        SYMBOLS = await self.validator.validate_all(SYMBOLS_CANDIDATE)
        with STATE_LOCK:
            SHARED_STATE["valid_symbols"] = SYMBOLS

        if not SYMBOLS:
            log.error("❌ هیچ نماد معتبری پیدا نشد!")
            await self.tg.send("❌ هیچ نماد معتبری پیدا نشد!")
            return

        log.info(f"✅ {len(SYMBOLS)} نماد معتبر: "
                 f"{[s.split('/')[0] for s in SYMBOLS]}")

        # ─── Position Mode per-symbol ─────────────────────────────────
        for sym in SYMBOLS:
            try:
                await self.ex.set_position_mode(False, sym)
                log.info(f"   ✅ One-Way: {sym.split('/')[0]}")
            except Exception as e:
                log.warning(f"   PM {sym.split('/')[0]}: {e}")
            await asyncio.sleep(0.3)

        # ─── Orphan Manager ───────────────────────────────────────────
        self.orphan_mgr = OrphanPositionManager(
            self.db, self.tg, self.ex, self.data_feed)

        # ─── Load DB trades ───────────────────────────────────────────
        for t in await self.db.get_open_trades():
            if t["symbol"] not in SYMBOLS:
                log.warning(f"نماد نامعتبر در DB: {t['symbol']}")
                continue
            pos = {
                "id":t["id"],"symbol":t["symbol"],"side":t["side"],
                "strategy":t["strategy"],"entry":t["entry_price"],
                "qty":t["qty"],"sl":t["sl"],"tp":t["tp"],"tp1":t["tp1"],
                "is_partial":t.get("is_partial",0),
                "highest_pnl_pct":t.get("highest_pnl_pct",0),
                "expected_rr":t.get("expected_rr",0),
                "signal_quality":t.get("signal_quality",0),
                "is_orphan":bool(t.get("is_orphan",0)),
            }
            with STATE_LOCK:
                if t.get("is_orphan"):
                    SHARED_STATE["orphan_positions"][t["id"]] = pos
                else:
                    SHARED_STATE["active_positions"][t["id"]] = pos
            self.open_times[t["id"]] = time.time()

        log.info(f"✅ DB: {len(SHARED_STATE['active_positions'])} عادی | "
                 f"{len(SHARED_STATE['orphan_positions'])} یتیم")

        # ─── اولین Sync + اسکن یتیم ──────────────────────────────────
        await self.smart_sync()
        await self.update_balance()

        # ─── اجرا ────────────────────────────────────────────────────
        await asyncio.gather(
            self.price_loop(),
            self.scan_loop(),
            self.orphan_scan_loop(),
            self.watchdog_loop(),
            self.equity_logger(),
            self.tg.poll()
        )

    async def update_balance(self):
        try:
            bal=await self.ex.fetch_balance()
            total=float(bal.get("USDT",{}).get("total",0) or 0)
            free =float(bal.get("USDT",{}).get("free",0)  or 0)
            with STATE_LOCK:
                SHARED_STATE["balance"]=total
                SHARED_STATE["free_balance"]=free
                if total>SHARED_STATE["peak_balance"]:
                    SHARED_STATE["peak_balance"]=total
                if SHARED_STATE["day_start_balance"]<=0:
                    SHARED_STATE["day_start_balance"]=total
            return total,free
        except Exception as e:
            log.error(f"Balance: {e}")
            return 0.0,0.0

    async def price_loop(self):
        while True:
            try:
                prices=await self.data_feed.fetch_all_tickers()
                self.prices.update(prices)
                with STATE_LOCK:
                    SHARED_STATE["phemex_status"]="live"
                bal,free=await self.update_balance()
                with STATE_LOCK:
                    peak=SHARED_STATE["peak_balance"]
                    if peak>0 and bal>0:
                        dd=(peak-bal)/peak*100
                        SHARED_STATE["current_dd"]=dd
                        SHARED_STATE["dd_halted"]=dd>=MAX_DD
                    ds=SHARED_STATE["day_start_balance"]
                    if ds>0:
                        dpnl=bal-ds
                        SHARED_STATE["daily_pnl"]=dpnl
                        SHARED_STATE["daily_halted"]=(dpnl/ds*100<=-MAX_DAILY_LOSS)
            except Exception as e:
                log.error(f"price_loop: {e}")
                with STATE_LOCK:
                    SHARED_STATE["phemex_status"]="error"
            await asyncio.sleep(PRICE_LOOP_INTERVAL)

    async def equity_logger(self):
        while True:
            await asyncio.sleep(EQUITY_LOG_INTERVAL)
            with STATE_LOCK:
                bal=SHARED_STATE["balance"]
                free=SHARED_STATE["free_balance"]
                peak=SHARED_STATE["peak_balance"]
                dd=SHARED_STATE["current_dd"]
                npos=len(SHARED_STATE["active_positions"])
                norp=len(SHARED_STATE["orphan_positions"])
            await self.db.log_equity(bal,free,peak,dd,npos,norp)

    # ─── Orphan Scan Loop (جدید) ──────────────────────────────────────
    async def orphan_scan_loop(self):
        """
        هر ORPHAN_SCAN_INTERVAL ثانیه یکبار:
        1. همه پوزیشن‌های Exchange را بگیر
        2. یتیم‌ها را شناسایی کن
        3. Adopt کن و مدیریت کن
        """
        await asyncio.sleep(15)  # صبر برای راه‌اندازی اولیه

        while True:
            try:
                with STATE_LOCK:
                    known = {
                        **dict(SHARED_STATE["active_positions"]),
                        **dict(SHARED_STATE["orphan_positions"])
                    }

                # اسکن و adopt
                new_orphans = await self.orphan_mgr.scan_and_adopt(known)

                if new_orphans:
                    log.info(f"👻 {len(new_orphans)} یتیم جدید Adopt شد")
                    with STATE_LOCK:
                        SHARED_STATE["orphan_positions"].update(new_orphans)
                    self.open_times.update(
                        self.orphan_mgr.open_times)

            except Exception as e:
                log.error(f"orphan_scan_loop: {e}")

            await asyncio.sleep(ORPHAN_SCAN_INTERVAL)

    async def scan_loop(self):
        while True:
            ok,reason=self.risk.check_global()
            if not ok:
                await asyncio.sleep(12)
                continue

            t0=time.time()
            with STATE_LOCK:
                SHARED_STATE["last_scan"]=time.strftime("%H:%M:%S")
                SHARED_STATE["scan_count"]+=1

            s_sc=s_fn=s_ex=s_rsp=s_rsig=s_rcb=0

            for sym in SYMBOLS:
                with STATE_LOCK:
                    all_pos={
                        **SHARED_STATE["active_positions"],
                        **SHARED_STATE["orphan_positions"]
                    }
                    if any(p["symbol"]==sym for p in all_pos.values()):
                        continue

                s_sc+=1
                ok2,cb_r=self.cb.is_allowed(sym)
                if not ok2:
                    s_rcb+=1
                    continue

                try:
                    df5=await self.data_feed.fetch_ohlcv(sym,TIMEFRAME,120)
                    await asyncio.sleep(SYMBOL_DELAY)
                    df1=await self.data_feed.fetch_ohlcv(sym,HTF_TIMEFRAME,80)
                    await asyncio.sleep(0.5)

                    if df5 is None or len(df5)<50: continue
                    if df1 is None or len(df1)<20: df1=df5.copy()

                    sig=self.strategy.analyze(df5,df1,sym)

                    px=self.data_feed.get_price(sym)
                    if not px or px<=0: continue

                    # Spread check
                    spread=await self.data_feed.get_spread_pct(sym)
                    await asyncio.sleep(0.3)
                    max_sp=2.0
                    if spread>max_sp and spread<999:
                        s_rsp+=1
                        await self.db.log_decision(
                            sym,"neutral","",
                            f"Spread {spread:.2f}%",
                            price=px,spread_pct=spread)
                        continue

                    await self.db.log_decision(
                        sym,sig["action"],sig.get("strat",""),
                        sig.get("reason",""),px,
                        sig.get("rsi",0),sig.get("atr",0),
                        sig.get("htf",""),sig.get("signal_quality",0),spread)

                    if sig["action"]!="neutral":
                        s_fn+=1
                        with STATE_LOCK:
                            SHARED_STATE["signal_count"]+=1
                        atr=sig.get("atr",0)
                        if atr>0:
                            p=STRATEGY_PARAMS.get(sig.get("strat",""),
                                {"sl_m":1.5,"tp_m":2.8,"tp1_m":1.4})
                            if sig["action"]=="buy":
                                sig["sl"]=px-atr*p["sl_m"]
                                sig["tp"]=px+atr*p["tp_m"]
                                sig["tp1"]=px+atr*p["tp1_m"]
                            else:
                                sig["sl"]=px+atr*p["sl_m"]
                                sig["tp"]=px-atr*p["tp_m"]
                                sig["tp1"]=px-atr*p["tp1_m"]
                        done=await self.execute_trade(sym,sig)
                        if done: s_ex+=1
                    else:
                        s_rsig+=1
                        with STATE_LOCK:
                            SHARED_STATE["rejected_count"]+=1

                except Exception as e:
                    log.error(f"scan {sym}: {e}")
                    await self.cb.reg_error(sym,str(e))

                await asyncio.sleep(SYMBOL_DELAY)

            dur=(time.time()-t0)*1000
            await self.db.log_scan_stats(
                s_sc,s_fn,s_ex,s_rsp,s_rsig,s_rcb,
                len(SHARED_STATE.get("orphan_positions",{})),dur)
            log.info(
                f"📡 اسکن #{SHARED_STATE['scan_count']} "
                f"⏱️{dur:.0f}ms ✅{s_ex}/{s_fn} "
                f"❌{s_rsig}sig {s_rsp}sp {s_rcb}cb")

            await asyncio.sleep(SCAN_INTERVAL)

    async def execute_trade(self,sym,sig)->bool:
        px=self.data_feed.get_price(sym) or self.prices.get(sym)
        with STATE_LOCK:
            bal=SHARED_STATE["balance"]
            free=SHARED_STATE["free_balance"]
        if not px or bal<20 or free<15: return False

        try:
            qty=self.risk.calculate_qty(
                bal,px,sig["sl"],free,sym,self.ex,
                sig.get("signal_quality",50))
            if qty<=0:
                await self.db.log_decision(sym,"rejected",
                    sig.get("strat",""),"حجم صفر")
                return False

            order=await self.ex.create_market_order(sym,sig["action"],qty)
            fill=float(order.get("average") or px)
            pid=f"pos_{uuid.uuid4().hex[:8]}"

            if sig["action"]=="buy":
                act_rr=(sig["tp"]-fill)/max(fill-sig["sl"],0.0001)
            else:
                act_rr=(fill-sig["tp"])/max(sig["sl"]-fill,0.0001)

            pos={
                "id":pid,"symbol":sym,"side":sig["action"],
                "strategy":sig.get("strat",""),"entry":fill,"qty":qty,
                "sl":sig["sl"],"tp":sig["tp"],"tp1":sig["tp1"],
                "is_partial":0,"highest_pnl_pct":0.0,
                "expected_rr":sig.get("expected_rr",0),
                "actual_rr":round(act_rr,2),
                "signal_quality":sig.get("signal_quality",0),
                "rsi_at_entry":sig.get("rsi",0),
                "atr_at_entry":sig.get("atr",0),
                "htf_trend":sig.get("htf",""),
                "is_orphan":False,
            }
            with STATE_LOCK:
                SHARED_STATE["active_positions"][pid]=pos
            self.open_times[pid]=time.time()
            await self.db.insert_trade(pos)
            self.cb.reg_win(sym)

            await self.tg.send(
                f"🎯 <b>{sig['action'].upper()}</b> "
                f"{sym.split('/')[0]}\n"
                f"Strat:{sig.get('strat','')} Q:{sig.get('signal_quality',0):.0f}\n"
                f"Fill:{fill:.5f} SL:{sig['sl']:.5f} TP:{sig['tp']:.5f}\n"
                f"RR:{act_rr:.2f}x Qty:{qty}")
            return True

        except Exception as e:
            err=str(e)
            log.error(f"execute {sym}: {err}")
            if "20004" in err or "INCONSISTENT" in err.upper():
                await self.cb.fix_pm(self.ex,sym)
            else:
                await self.cb.reg_error(sym,err)
            await self.db.log_decision(sym,"rejected",
                sig.get("strat",""),err[:100])
            return False

    async def real_test_trade(self):
        await self.tg.send("⚡ تست واقعی...")
        sym=next((s for s in SYMBOLS
                   if "ADA" in s or "XRP" in s or "DOGE" in s),
                  SYMBOLS[0] if SYMBOLS else None)
        if not sym:
            await self.tg.send("❌ نماد مناسب نیافت"); return
        try:
            bal,free=await self.update_balance()
            if bal<20:
                await self.tg.send("❌ موجودی ناکافی"); return
            px=self.data_feed.get_price(sym)
            if not px:
                await self.tg.send("❌ قیمت نبود"); return
            amt=min(15.0,bal*0.04)/px
            qty=float(self.ex.amount_to_precision(sym,amt))
            order=await self.ex.create_market_order(sym,"buy",qty)
            fill=float(order.get("average") or px)
            pid=f"test_{uuid.uuid4().hex[:6]}"
            pos={
                "id":pid,"symbol":sym,"side":"buy",
                "strategy":"RealTest","entry":fill,"qty":qty,
                "sl":fill*0.97,"tp":fill*1.03,"tp1":fill*1.015,
                "is_partial":0,"highest_pnl_pct":0.0,
                "expected_rr":1.0,"signal_quality":50,"is_orphan":False,
            }
            with STATE_LOCK:
                SHARED_STATE["active_positions"][pid]=pos
            self.open_times[pid]=time.time()
            await self.tg.send(
                f"🧪 تست باز {sym.split('/')[0]} @ {fill:.5f}\n30s...")
            await asyncio.sleep(30)
            await self.force_close(pid,"RealTest")
            await self.tg.send("✅ تست بسته شد")
        except Exception as e:
            await self.tg.send(f"❌ {e}")

    async def smart_sync(self):
        try:
            remote=await self.ex.fetch_positions()
            active=set()
            for p in remote:
                if abs(float(p.get("contracts") or 0))>0:
                    raw=p.get("symbol","")
                    m=next((s for s in SYMBOLS if s.split("/")[0] in raw),None)
                    if m: active.add(m)

            with STATE_LOCK:
                to_del=[
                    pid for pid,p in SHARED_STATE["active_positions"].items()
                    if p["symbol"] not in active
                    and p.get("strategy")!="RealTest"]
            for pid in to_del:
                await self.db.close_trade(pid,0.0,reason="remote closed")
                with STATE_LOCK:
                    SHARED_STATE["active_positions"].pop(pid,None)

            log.info(f"🔄 Sync | Exchange:{len(active)} | DB closed:{len(to_del)}")
        except Exception as e:
            log.error(f"sync: {e}")

    async def force_close(self,pid,reason):
        with STATE_LOCK:
            pos=(SHARED_STATE["active_positions"].get(pid) or
                 SHARED_STATE["orphan_positions"].get(pid))
        if not pos: return

        px=self.data_feed.get_price(pos["symbol"]) or pos["entry"]
        hold=time.time()-self.open_times.get(pid,time.time())

        try:
            cs="sell" if pos["side"]=="buy" else "buy"
            order=await self.ex.create_market_order(
                pos["symbol"],cs,pos["qty"],params={"reduceOnly":True})
            exit_px=float(order.get("average") or px)
            raw_pnl=((exit_px-pos["entry"])*pos["qty"]
                     *(1 if pos["side"]=="buy" else -1))
            fees=abs(raw_pnl)*TAKER_FEE*2*FEE_BUFFER
            net=raw_pnl-fees

            dist_sl=abs(pos["entry"]-pos["sl"])
            dist_pnl=abs(exit_px-pos["entry"])
            act_rr=(dist_pnl/dist_sl) if dist_sl>0 else 0
            if pos["side"]=="sell" and exit_px<pos["entry"]: act_rr=abs(act_rr)
            elif pos["side"]=="buy" and exit_px<pos["entry"]: act_rr=-abs(act_rr)

            if pos.get("strategy")!="RealTest":
                await self.db.close_trade(
                    pid,raw_pnl,fees,reason,hold,
                    exit_px,act_rr,abs(exit_px-px))
                if net<0:
                    cb=await self.cb.reg_loss(pos["symbol"],net)
                    if cb:
                        await self.tg.send(
                            f"🛑 Circuit Breaker\n"
                            f"{pos['symbol'].split('/')[0]} "
                            f"{CONSECUTIVE_LOSS_LIMIT} ضرر متوالی")
                else:
                    self.cb.reg_win(pos["symbol"])

            with STATE_LOCK:
                SHARED_STATE["active_positions"].pop(pid,None)
                SHARED_STATE["orphan_positions"].pop(pid,None)
            self.open_times.pop(pid,None)
            await self.db.update_analytics()

            e="🟢" if net>=0 else "🔴"
            orp="👻" if pos.get("is_orphan") else ""
            await self.tg.send(
                f"{e}{orp} <b>بسته</b> ({reason})\n"
                f"{pos['symbol'].split('/')[0]} {pos['side'].upper()}\n"
                f"PnL:${net:.3f} RR:{act_rr:.2f}x Hold:{int(hold/60)}min")

        except Exception as e:
            log.error(f"force_close {pid}: {e}")

    async def watchdog_loop(self):
        while True:
            with STATE_LOCK:
                all_pos={
                    **dict(SHARED_STATE["active_positions"]),
                    **dict(SHARED_STATE["orphan_positions"])
                }

            for pid,pos in all_pos.items():
                if pos.get("strategy")=="RealTest": continue
                px=self.data_feed.get_price(pos["symbol"]) or self.prices.get(pos["symbol"])
                if not px: continue

                pnl_pct=((px-pos["entry"])/pos["entry"]*100
                          if pos["side"]=="buy"
                          else (pos["entry"]-px)/pos["entry"]*100)

                # Trailing
                if pnl_pct>TRAIL_ACT and pnl_pct>pos["highest_pnl_pct"]:
                    pos["highest_pnl_pct"]=pnl_pct
                    if pos["side"]=="buy":
                        nsl=px*(1-TRAIL_STEP/100)
                        if nsl>pos["sl"]: pos["sl"]=nsl
                    else:
                        nsl=px*(1+TRAIL_STEP/100)
                        if nsl<pos["sl"]: pos["sl"]=nsl
                    await self.db.update_trade(
                        pid,pos["qty"],pos["sl"],
                        pos["is_partial"],pos["highest_pnl_pct"])

                # Partial TP
                if PARTIAL_TP and pos["is_partial"]==0:
                    hit=((pos["side"]=="buy" and px>=pos["tp1"]) or
                         (pos["side"]=="sell" and px<=pos["tp1"]))
                    if hit:
                        try:
                            half=float(self.ex.amount_to_precision(
                                pos["symbol"],pos["qty"]/2))
                            if half>0:
                                cs="sell" if pos["side"]=="buy" else "buy"
                                await self.ex.create_market_order(
                                    pos["symbol"],cs,half,
                                    params={"reduceOnly":True})
                                pos["qty"]-=half
                                pos["is_partial"]=1
                                pos["sl"]=pos["entry"]
                                await self.db.update_trade(
                                    pid,pos["qty"],pos["sl"],1,
                                    pos["highest_pnl_pct"])
                                await self.tg.send(
                                    f"🔹 Partial TP → BE\n"
                                    f"{pos['symbol'].split('/')[0]} @ {px:.4f}")
                        except Exception as e:
                            log.error(f"partial_tp: {e}")

                # SL/TP
                sl_hit=((pos["side"]=="buy" and px<=pos["sl"]) or
                        (pos["side"]=="sell" and px>=pos["sl"]))
                tp_hit=((pos["side"]=="buy" and px>=pos["tp"]) or
                        (pos["side"]=="sell" and px<=pos["tp"]))
                if sl_hit or tp_hit:
                    await self.force_close(pid,"SL/Trail" if sl_hit else "TP")

            await asyncio.sleep(2.0)


# ============================================================================
# 12. WEB DASHBOARD
# ============================================================================
app=Flask(__name__)

@app.route("/")
def dashboard():
    return render_template_string("""
<!DOCTYPE html><html lang="fa" dir="rtl">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Quant v16.0</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui;background:#0d1117;color:#c9d1d9;padding:16px;direction:rtl}
h1{color:#58a6ff;font-size:1.4rem}
.sub{color:#8b949e;font-size:.8rem;margin-bottom:14px}
.bar{background:#161b22;border:1px solid #30363d;border-radius:8px;
  padding:8px 14px;display:flex;gap:14px;flex-wrap:wrap;font-size:.8rem;margin-bottom:10px}
.dot{width:9px;height:9px;border-radius:50%;display:inline-block;margin-left:5px}
.g{background:#3fb950}.r{background:#f85149}.y{background:#d29922}
.alert{background:#1a0f0f;border:1px solid #f85149;border-radius:8px;
  padding:8px;margin-bottom:8px;color:#f85149;font-size:.8rem}
.orphan-alert{background:#1a1200;border:1px solid #d29922;border-radius:8px;
  padding:8px;margin-bottom:8px;color:#d29922;font-size:.8rem}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin:10px 0}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px}
.label{font-size:.72rem;color:#8b949e;margin-bottom:4px}
.val{font-size:1.2rem;font-weight:700;color:#58a6ff}
.gn{color:#3fb950}.rd{color:#f85149}.yw{color:#d29922}
h3{color:#8b949e;font-size:.82rem;margin:14px 0 5px;text-transform:uppercase}
table{width:100%;border-collapse:collapse;font-size:.8rem}
th{background:#21262d;padding:6px 8px;text-align:right;color:#8b949e}
td{padding:6px 8px;border-bottom:1px solid #21262d}
.b{display:inline-block;padding:2px 6px;border-radius:8px;font-size:.7rem;font-weight:700}
.bb{background:#0d2818;color:#3fb950}.bs{background:#2d1317;color:#f85149}
.borp{background:#1a1200;color:#d29922}
</style></head><body>
<h1>🚀 Master Quant v16.0</h1>
<p class="sub">Phemex Native | Orphan Manager | Symbol Validator | No Binance</p>
<div class="bar">
  <span><span class="dot" id="d1"></span><span id="st">—</span></span>
  <span>📡 <span id="ps">—</span></span>
  <span>🕐 <span id="sc">—</span> #<span id="sn">0</span></span>
  <span>✅ <span id="syms">—</span> نماد</span>
</div>
<div id="al"></div><div id="oa"></div>
<div class="grid">
  <div class="card"><div class="label">موجودی</div><div class="val" id="bal">—</div></div>
  <div class="card"><div class="label">آزاد</div><div class="val" id="fre">—</div></div>
  <div class="card"><div class="label">پوزیشن</div><div class="val" id="pos">0/10</div></div>
  <div class="card"><div class="label">یتیم</div><div class="val yw" id="orp">0</div></div>
  <div class="card"><div class="label">Net PnL</div><div class="val" id="pnl">—</div></div>
  <div class="card"><div class="label">Win Rate</div><div class="val" id="wr">—</div></div>
  <div class="card"><div class="label">DD</div><div class="val rd" id="dd">—</div></div>
  <div class="card"><div class="label">PF</div><div class="val" id="pf">—</div></div>
</div>
<h3>📦 همه پوزیشن‌ها (عادی + یتیم)</h3>
<table><thead><tr><th>نماد</th><th>نوع</th><th>جهت</th><th>استراتژی</th>
<th>ورود</th><th>SL</th><th>TP</th><th>Qty</th></tr></thead>
<tbody id="ptb"><tr><td colspan="8" style="text-align:center;color:#8b949e">—</td></tr></tbody></table>
<script>
async function r(){
  try{
    const d=await(await fetch('/api/status')).json();
    const a=d.is_active&&!d.dd_halted&&!d.daily_halted;
    document.getElementById('d1').className='dot '+(a?'g':d.dd_halted?'r':'y');
    document.getElementById('st').textContent=d.dd_halted?'DD Halt':d.daily_halted?'Daily Halt':d.is_active?'فعال':'متوقف';
    document.getElementById('ps').textContent=d.phemex_status||'?';
    document.getElementById('sc').textContent=d.last_scan||'—';
    document.getElementById('sn').textContent=d.scan_count||0;
    document.getElementById('syms').textContent=(d.valid_symbols||[]).length;
    document.getElementById('bal').textContent='$'+(d.balance||0).toFixed(2);
    document.getElementById('fre').textContent='$'+(d.free_balance||0).toFixed(2);
    const reg=Object.values(d.active_positions||{});
    const orp=Object.values(d.orphan_positions||{});
    document.getElementById('pos').textContent=reg.length+'/10';
    const oe=document.getElementById('orp');
    oe.textContent=orp.length;
    oe.className='val '+(orp.length>0?'yw':'gn');
    const s=d.stats||{};
    const np=s.net_pnl||0;
    const pe=document.getElementById('pnl');
    pe.textContent='$'+np.toFixed(2);pe.className='val '+(np>=0?'gn':'rd');
    document.getElementById('wr').textContent=(s.win_rate||0).toFixed(1)+'%';
    document.getElementById('dd').textContent=(d.current_dd||0).toFixed(2)+'%';
    document.getElementById('pf').textContent=(s.profit_factor||0).toFixed(2);
    // Alerts
    const cds=d.symbol_cooldowns||{},now=Date.now()/1000;
    const ac=Object.entries(cds).filter(([,v])=>v>now);
    document.getElementById('al').innerHTML=ac.length?
      '<div class="alert">🔴 CB: '+ac.map(([s,v])=>s.split('/')[0]+'('+Math.round((v-now)/60)+'m)').join(' ')+'</div>':'';
    document.getElementById('oa').innerHTML=orp.length?
      '<div class="orphan-alert">👻 '+orp.length+' پوزیشن یتیم فعال: '+orp.map(p=>p.symbol.split('/')[0]+' '+p.side.toUpperCase()).join(' | ')+'</div>':'';
    // Table
    const all=[...reg.map(p=>({...p,is_orphan:false})),...orp.map(p=>({...p,is_orphan:true}))];
    const tb=document.getElementById('ptb');
    tb.innerHTML=all.length?all.map(p=>`<tr>
      <td>${p.symbol.split('/')[0]}</td>
      <td>${p.is_orphan?'<span class="b borp">👻 ORPHAN</span>':'<span class="b bb">NORMAL</span>'}</td>
      <td><span class="b ${p.side=='buy'?'bb':'bs'}">${p.side.toUpperCase()}</span></td>
      <td style="font-size:.72rem">${p.strategy||'?'}</td>
      <td>${(p.entry||0).toFixed(4)}</td>
      <td style="color:#f85149">${(p.sl||0).toFixed(4)}</td>
      <td style="color:#3fb950">${(p.tp||0).toFixed(4)}</td>
      <td>${p.qty||0}</td>
    </tr>`).join(''):'<tr><td colspan="8" style="text-align:center;color:#8b949e">هیچ پوزیشنی نیست</td></tr>';
  }catch(e){console.error(e)}
}
r();setInterval(r,4000);
</script></body></html>""")

@app.route("/api/status")
def api_status():
    with STATE_LOCK:
        return jsonify(dict(SHARED_STATE))

def run_web():
    app.run(host="0.0.0.0",port=10000,debug=False,use_reloader=False)


# ============================================================================
# 13. MAIN
# ============================================================================
if __name__=="__main__":
    Thread(target=run_web,daemon=True).start()
    log.info("🌐 Dashboard: http://0.0.0.0:10000")
    engine=QuantEngine()
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        log.info("👋 Shutdown")
    except Exception as e:
        log.error(f"💥 Fatal: {e}")
        raise
