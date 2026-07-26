#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master Quant Engine v17.0 - Phemex Native Fixed
═══════════════════════════════════════════════════════
اصلاحات بحرانی v17.0:

1. OHLCV Phemex: استفاده از resolution (عدد) نه timeframe (رشته)
2. Symbol format: تشخیص خودکار فرمت صحیح از market info
3. Orphan Manager: شناسایی و مدیریت پوزیشن‌های قدیمی
4. Validator بدون OHLCV: فقط market existence check
5. Fallback OHLCV با چندین روش
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

PHEMEX_TESTNET_URL = "https://testnet-api.phemex.com"
PHEMEX_LIVE_URL    = "https://api.phemex.com"

# ─── نمادهای کاندید ────────────────────────────────────────────────────────
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

# لیست نهایی بعد از validation
SYMBOLS: List[str] = []

# ─── Phemex Resolution Map ────────────────────────────────────────────────
# Phemex از resolution عددی (ثانیه) استفاده می‌کند
PHEMEX_RESOLUTION = {
    "1m":   60,
    "3m":   180,
    "5m":   300,
    "15m":  900,
    "30m":  1800,
    "1h":   3600,
    "2h":   7200,
    "4h":   14400,
    "6h":   21600,
    "12h":  43200,
    "1d":   86400,
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
SYMBOL_CONFIG: Dict[str, dict] = {}

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
RISK_PCT            = 0.5
LEVERAGE            = 5
MAX_POS             = 10
MAX_DD              = 10.0
MAX_DAILY_LOSS      = 5.0
MIN_ORDER_USD       = 16.0
MAX_EXPOSURE_PCT    = 80.0
MAX_SINGLE_EXP      = 15.0
TAKER_FEE           = 0.0006
FEE_BUFFER          = 1.2
TRAIL_ACT           = 1.8
TRAIL_STEP          = 0.6
PARTIAL_TP          = True

# ─── Orphan ───────────────────────────────────────────────────────────────
ORPHAN_SL_PCT       = 3.0
ORPHAN_TP_PCT       = 4.0
ORPHAN_TP1_PCT      = 2.0
ORPHAN_SCAN_SEC     = 30

# ─── Timing ───────────────────────────────────────────────────────────────
SCAN_INTERVAL       = 45
SYMBOL_DELAY        = 1.0
PRICE_LOOP_SEC      = 6
EQUITY_LOG_SEC      = 60

# ─── Circuit Breaker ──────────────────────────────────────────────────────
CONSEC_LOSS_LIMIT   = 3
COOLDOWN_HOURS      = 2
MAX_ERR_COOLDOWN    = 1800

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler("quant_v17.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("QuantV17")

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
    "orphan_positions":   {},
    "last_scan":          "Never",
    "scan_count":         0,
    "signal_count":       0,
    "rejected_count":     0,
    "valid_symbols":      [],
    "ohlcv_method":       "unknown",
    "consecutive_losses": {},
    "symbol_cooldowns":   {},
    "symbol_errors":      {},
    "phemex_status":      "init",
    "stats": {
        "total_trades":0,"winning_trades":0,"losing_trades":0,
        "win_rate":0.0,"total_pnl":0.0,"total_fees":0.0,"net_pnl":0.0,
        "avg_hold_min":0.0,"max_win":0.0,"max_loss":0.0,
        "profit_factor":0.0,"sharpe_approx":0.0,
        "orphans_adopted":0,"orphans_closed":0,
        "by_symbol":{},"by_strategy":{},
    },
    "operational": {
        "total_api_errors":0,"position_mode_fixes":0,
        "circuit_breaker_events":0,"sync_count":0,
        "orphan_detections":0,"uptime_start":time.time(),
    },
    "version": "17.0",
}
STATE_LOCK = Lock()


# ============================================================================
# 2. PHEMEX OHLCV ENGINE - اصلاح اصلی
# ============================================================================
class PhemexOHLCV:
    """
    کلاس اختصاصی دریافت OHLCV از Phemex
    
    مشکل اصلی: Phemex Swap از timeframe رشته‌ای پشتیبانی نمی‌کند
    و باید از resolution عددی یا params خاص استفاده شود
    
    روش‌های امتحان به ترتیب:
    1. ccxt standard با timeframe string
    2. params با resolution عددی  
    3. params با period
    4. REST API مستقیم
    """

    def __init__(self, exchange: ccxt.phemex, base_url: str):
        self.ex       = exchange
        self.base_url = base_url
        self.method   = None   # روش موفق کشف‌شده
        self.cache:   Dict[str, pd.DataFrame] = {}
        self.cache_ts: Dict[str, float]       = {}
        self.cache_ttl = 10
        self.last_price: Dict[str, float]     = {}

    async def discover_method(self, symbol: str) -> Optional[str]:
        """
        کشف روش صحیح OHLCV برای این Exchange
        یک بار اجرا می‌شود و روش را ذخیره می‌کند
        """
        log.info(f"🔍 کشف روش OHLCV برای Phemex ({symbol})...")
        methods = [
            ("ccxt_standard",     self._try_ccxt_standard),
            ("ccxt_resolution",   self._try_ccxt_resolution),
            ("ccxt_period",       self._try_ccxt_period),
            ("rest_direct",       self._try_rest_direct),
        ]

        for name, fn in methods:
            try:
                df = await fn(symbol, "5m", 10)
                if df is not None and len(df) >= 3:
                    log.info(f"✅ روش OHLCV: {name}")
                    self.method = name
                    with STATE_LOCK:
                        SHARED_STATE["ohlcv_method"] = name
                    return name
                log.debug(f"   روش {name}: ناموفق")
            except Exception as e:
                log.debug(f"   روش {name}: {e}")
            await asyncio.sleep(0.5)

        log.error("❌ هیچ روش OHLCV کار نکرد!")
        return None

    async def fetch(self, symbol: str, timeframe: str,
                    limit: int = 120) -> Optional[pd.DataFrame]:
        """دریافت OHLCV با روش کشف‌شده"""
        cache_key = f"{symbol}_{timeframe}"
        now = time.time()

        # کش
        if (cache_key in self.cache and
                now - self.cache_ts.get(cache_key, 0) < self.cache_ttl):
            return self.cache[cache_key]

        df = None
        # اگر روش هنوز کشف نشده
        if self.method is None:
            await self.discover_method(symbol)

        if self.method == "ccxt_standard":
            df = await self._try_ccxt_standard(symbol, timeframe, limit)
        elif self.method == "ccxt_resolution":
            df = await self._try_ccxt_resolution(symbol, timeframe, limit)
        elif self.method == "ccxt_period":
            df = await self._try_ccxt_period(symbol, timeframe, limit)
        elif self.method == "rest_direct":
            df = await self._try_rest_direct(symbol, timeframe, limit)

        # Fallback اگر روش اصلی شکست خورد
        if df is None or len(df) < 5:
            df = await self._fallback_all(symbol, timeframe, limit)

        if df is not None and len(df) >= 5:
            # آپدیت کش
            self.cache[cache_key] = df
            self.cache_ts[cache_key] = now
            px = float(df["close"].iloc[-1])
            if px > 0:
                self.last_price[symbol] = px

        return df if (df is not None and len(df) >= 5) else self.cache.get(cache_key)

    # ─── روش 1: ccxt استاندارد با since ──────────────────────────────────
    async def _try_ccxt_standard(self, symbol, tf, limit) -> Optional[pd.DataFrame]:
        res     = PHEMEX_RESOLUTION.get(tf, 300)
        since   = int((time.time() - res * limit * 1.5) * 1000)
        raw     = await self.ex.fetch_ohlcv(symbol, tf, since=since, limit=limit)
        return self._to_df(raw)

    # ─── روش 2: با resolution param ──────────────────────────────────────
    async def _try_ccxt_resolution(self, symbol, tf, limit) -> Optional[pd.DataFrame]:
        res     = PHEMEX_RESOLUTION.get(tf, 300)
        end_ts  = int(time.time())
        start_ts= end_ts - res * limit
        raw     = await self.ex.fetch_ohlcv(
            symbol, tf,
            params={
                "resolution": res,
                "from":       start_ts,
                "to":         end_ts,
            })
        return self._to_df(raw)

    # ─── روش 3: با period param ───────────────────────────────────────────
    async def _try_ccxt_period(self, symbol, tf, limit) -> Optional[pd.DataFrame]:
        raw = await self.ex.fetch_ohlcv(
            symbol, tf,
            params={"period": tf, "limit": limit})
        return self._to_df(raw)

    # ─── روش 4: REST API مستقیم ──────────────────────────────────────────
    async def _try_rest_direct(self, symbol, tf, limit) -> Optional[pd.DataFrame]:
        """
        فراخوانی مستقیم Phemex REST API
        مسیر: /exchange/public/md/v2/kline/last
        """
        res = PHEMEX_RESOLUTION.get(tf, 300)

        # تبدیل symbol به فرمت Phemex
        phemex_sym = self._to_phemex_sym(symbol)
        if not phemex_sym:
            return None

        url    = f"{self.base_url}/exchange/public/md/v2/kline/last"
        params = {
            "symbol":     phemex_sym,
            "resolution": res,
            "limit":      limit,
        }

        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, params=params,
                                 timeout=aiohttp.ClientTimeout(total=15)) as r:
                    data = await r.json()

            rows = (data.get("data", {}).get("rows") or
                    data.get("rows") or
                    data.get("result", {}).get("rows") or [])

            if not rows:
                # تلاش با endpoint قدیمی
                url2    = f"{self.base_url}/md/kline"
                params2 = {"symbol": phemex_sym, "resolution": res, "limit": limit}
                async with aiohttp.ClientSession() as s:
                    async with s.get(url2, params=params2,
                                     timeout=aiohttp.ClientTimeout(total=15)) as r2:
                        data2 = await r2.json()
                rows = data2.get("result", {}).get("rows") or []

            if not rows:
                return None

            # فرمت Phemex: [timestamp, open, high, low, close, volume, turnover]
            # قیمت‌ها ممکن است ×10^8 باشند (scaled)
            records = []
            for row in rows:
                if len(row) < 6:
                    continue
                ts   = int(row[0]) * 1000 if int(row[0]) < 1e12 else int(row[0])
                o, h, l, c, v = (float(x) for x in row[1:6])
                # تشخیص scaled price
                if c > 1e6 and symbol not in ("BTC/USDT:USDT",):
                    o /= 1e8; h /= 1e8; l /= 1e8; c /= 1e8
                records.append([ts, o, h, l, c, v])

            return self._to_df(records)

        except Exception as e:
            log.debug(f"REST direct {symbol}: {e}")
            return None

    async def _fallback_all(self, symbol, tf, limit) -> Optional[pd.DataFrame]:
        """امتحان همه روش‌ها"""
        fns = [
            self._try_ccxt_standard,
            self._try_ccxt_resolution,
            self._try_ccxt_period,
            self._try_rest_direct,
        ]
        for fn in fns:
            try:
                df = await fn(symbol, tf, limit)
                if df is not None and len(df) >= 5:
                    return df
            except Exception:
                pass
            await asyncio.sleep(0.3)
        return None

    def _to_df(self, raw) -> Optional[pd.DataFrame]:
        if not raw or len(raw) < 3:
            return None
        try:
            df = pd.DataFrame(
                raw, columns=["ts","open","high","low","close","volume"])
            df = df.astype(float)
            df["ts"] = df["ts"].astype(int)
            df = df.dropna()
            df = df[df["close"] > 0]
            df = df[df["high"] >= df["low"]]
            df = df.sort_values("ts").reset_index(drop=True)
            return df if len(df) >= 3 else None
        except Exception as e:
            log.debug(f"_to_df: {e}")
            return None

    def _to_phemex_sym(self, ccxt_sym: str) -> Optional[str]:
        """تبدیل BTC/USDT:USDT → BTCUSDT"""
        try:
            base = ccxt_sym.split("/")[0]
            return f"{base}USDT"
        except Exception:
            return None

    def get_price(self, symbol: str) -> Optional[float]:
        return self.last_price.get(symbol)


# ============================================================================
# 3. PHEMEX DATA FEED
# ============================================================================
class PhemexDataFeed:
    def __init__(self, exchange: ccxt.phemex, ohlcv_engine: PhemexOHLCV):
        self.ex          = exchange
        self.ohlcv_eng   = ohlcv_engine
        self.price_cache: Dict[str, float] = {}
        self.last_good:   Dict[str, float] = {}

    async def fetch_ohlcv(self, symbol: str, tf: str,
                           limit: int = 120) -> Optional[pd.DataFrame]:
        return await self.ohlcv_eng.fetch(symbol, tf, limit)

    async def fetch_all_tickers(self) -> Dict[str, float]:
        prices = {}
        try:
            syms    = SYMBOLS[:10] if SYMBOLS else []
            if not syms:
                return prices
            tickers = await self.ex.fetch_tickers(syms)
            for sym, tick in tickers.items():
                p = float(tick.get("last") or tick.get("close") or 0)
                if p > 0:
                    prices[sym]              = p
                    self.price_cache[sym]    = p
                    self.last_good[sym]      = p
                    self.ohlcv_eng.last_price[sym] = p
        except Exception as e:
            log.error(f"fetch_all_tickers: {e}")
            # Fallback: ticker یک به یک
            for sym in (SYMBOLS[:5] if SYMBOLS else []):
                try:
                    t = await self.ex.fetch_ticker(sym)
                    p = float(t.get("last") or 0)
                    if p > 0:
                        prices[sym]           = p
                        self.price_cache[sym] = p
                        self.last_good[sym]   = p
                except Exception:
                    pass
                await asyncio.sleep(0.3)
        return prices

    def get_price(self, sym: str) -> Optional[float]:
        p  = self.price_cache.get(sym)
        lg = self.last_good.get(sym) or self.ohlcv_eng.get_price(sym)
        if not p or p <= 0:
            return lg
        if lg and lg > 0:
            r = p / lg
            if r > 3.0 or r < 0.33:
                log.warning(f"Spike {sym}: {p} vs {lg}")
                return lg
        return p

    async def get_spread(self, sym: str) -> float:
        try:
            ob   = await self.ex.fetch_order_book(sym, limit=5)
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
            if bids and asks:
                bid = float(bids[0][0])
                ask = float(asks[0][0])
                mid = (bid + ask) / 2
                return (ask - bid) / mid * 100 if mid > 0 else 0.0
        except Exception:
            pass
        return 0.0


# ============================================================================
# 4. SYMBOL VALIDATOR (اصلاح شده - بدون OHLCV)
# ============================================================================
class SymbolValidator:
    """
    اعتبارسنجی نمادها بدون نیاز به OHLCV
    فقط چک می‌کند نماد در market list وجود دارد
    """

    def __init__(self, exchange: ccxt.phemex):
        self.ex = exchange

    async def validate_all(self, candidates: List[str]) -> List[str]:
        valid   = []
        markets = self.ex.markets or {}

        log.info(f"🔍 اعتبارسنجی {len(candidates)} نماد (Market Check)...")
        log.info(f"   تعداد کل بازارها: {len(markets)}")

        for sym in candidates:
            ok, reason = self._check_market(sym, markets)
            if ok:
                valid.append(sym)
                SYMBOL_CONFIG[sym] = BASE_SYMBOL_CONFIG.get(sym, {
                    "min_atr_pct":0.1,"max_atr_pct":6.0,
                    "min_vol_mult":1.2,"weight":0.8,"max_usd_pos":100.0
                })
                log.info(f"   ✅ {sym.split('/')[0]}")
            else:
                log.warning(f"   ⚠️  {sym.split('/')[0]}: {reason}")
                # اضافه کردن با هشدار اگر ticker کار می‌کند
                ticker_ok = await self._check_ticker(sym)
                if ticker_ok:
                    valid.append(sym)
                    SYMBOL_CONFIG[sym] = BASE_SYMBOL_CONFIG.get(sym, {
                        "min_atr_pct":0.1,"max_atr_pct":6.0,
                        "min_vol_mult":1.2,"weight":0.8,"max_usd_pos":100.0
                    })
                    log.info(f"   ✅ {sym.split('/')[0]} (ticker OK)")
            await asyncio.sleep(0.3)

        if not valid:
            log.warning("⚠️  Market check ناموفق → استفاده از همه کاندیدها")
            for sym in candidates:
                SYMBOL_CONFIG[sym] = BASE_SYMBOL_CONFIG.get(sym, {
                    "min_atr_pct":0.1,"max_atr_pct":6.0,
                    "min_vol_mult":1.2,"weight":0.8,"max_usd_pos":100.0
                })
            return candidates

        log.info(f"✅ {len(valid)} نماد معتبر | ❌ {len(candidates)-len(valid)} حذف‌شده")
        return valid

    def _check_market(self, sym: str, markets: dict) -> Tuple[bool, str]:
        if sym in markets:
            m = markets[sym]
            if not m.get("active", True):
                return False, "غیرفعال"
            return True, "OK"

        # جستجوی alternative format
        base = sym.split("/")[0]
        alts = [
            f"{base}/USDT:USDT",
            f"{base}USDT",
            f"{base}/USD:USD",
        ]
        for alt in alts:
            if alt in markets:
                return True, f"یافت شد به عنوان {alt}"

        return False, "در market list نیست"

    async def _check_ticker(self, sym: str) -> bool:
        try:
            t = await self.ex.fetch_ticker(sym)
            p = float(t.get("last") or t.get("close") or 0)
            return p > 0
        except Exception:
            return False


# ============================================================================
# 5. ORPHAN POSITION MANAGER (کامل و اصلاح‌شده)
# ============================================================================
class OrphanManager:
    """
    مدیریت پوزیشن‌های یتیم
    
    پوزیشن یتیم = در Exchange هست ولی ربات نمی‌شناسد
    علت: restart ربات، crash، معامله دستی
    
    عملکرد:
    1. fetch_positions از Exchange
    2. مقایسه با active_positions + orphan_positions
    3. یتیم‌های جدید را adopt کن
    4. SL/TP پیش‌فرض بده
    5. زیر نظارت watchdog قرار بده
    """

    def __init__(self, db, tg, ex: ccxt.phemex, feed: PhemexDataFeed):
        self.db   = db
        self.tg   = tg
        self.ex   = ex
        self.feed = feed
        self.open_times: Dict[str, float] = {}

    async def scan(self, known: Dict[str, dict]) -> Dict[str, dict]:
        """اسکن و adopt یتیم‌ها"""
        new_orphans: Dict[str, dict] = {}

        try:
            remote = await self.ex.fetch_positions()
        except Exception as e:
            log.error(f"OrphanScan: {e}")
            return new_orphans

        for rp in remote:
            qty_raw = (rp.get("contracts") or rp.get("size") or
                       rp.get("info", {}).get("size") or 0)
            try:
                qty = abs(float(qty_raw))
            except Exception:
                qty = 0.0

            if qty < 1e-8:
                continue

            raw_sym  = rp.get("symbol", "")
            side_raw = (rp.get("side") or
                        rp.get("info", {}).get("side") or "").lower()
            entry    = float(rp.get("entryPrice") or
                             rp.get("info", {}).get("avgEntryPrice") or 0)

            std_sym = self._normalize(raw_sym)
            if not std_sym:
                log.debug(f"Orphan: نماد ناشناخته {raw_sym}")
                continue

            side = "buy" if side_raw in ("buy","long","bid") else "sell"

            # چک آیا می‌شناسیم
            is_known = any(
                p["symbol"] == std_sym and p["side"] == side
                for p in known.values()
            )
            if is_known:
                continue

            # ─── یتیم پیدا شد ──────────────────────────────────────
            log.warning(
                f"👻 یتیم: {std_sym} | {side.upper()} | "
                f"qty={qty:.4f} | entry={entry:.5f}")

            px = self.feed.get_price(std_sym) or entry or 1.0
            if entry <= 0:
                entry = px

            # SL/TP پیش‌فرض
            if side == "buy":
                sl  = entry * (1 - ORPHAN_SL_PCT / 100)
                tp  = entry * (1 + ORPHAN_TP_PCT / 100)
                tp1 = entry * (1 + ORPHAN_TP1_PCT / 100)
            else:
                sl  = entry * (1 + ORPHAN_SL_PCT / 100)
                tp  = entry * (1 - ORPHAN_TP_PCT / 100)
                tp1 = entry * (1 - ORPHAN_TP1_PCT / 100)

            pnl_est = (px - entry) * qty * (1 if side == "buy" else -1)
            pid     = f"orphan_{uuid.uuid4().hex[:8]}"

            orphan = {
                "id":              pid,
                "symbol":          std_sym,
                "side":            side,
                "strategy":        "Orphan_Adopted",
                "entry":           entry,
                "qty":             qty,
                "sl":              sl,
                "tp":              tp,
                "tp1":             tp1,
                "is_partial":      0,
                "highest_pnl_pct": 0.0,
                "expected_rr":     ORPHAN_TP_PCT / ORPHAN_SL_PCT,
                "signal_quality":  25.0,
                "is_orphan":       True,
                "raw_symbol":      raw_sym,
                "rsi_at_entry":    0,
                "atr_at_entry":    0,
                "htf_trend":       "",
            }

            new_orphans[pid] = orphan
            self.open_times[pid] = time.time()

            # ذخیره DB
            await self.db.insert_trade(orphan)

            with STATE_LOCK:
                SHARED_STATE["stats"]["orphans_adopted"] += 1
                SHARED_STATE["operational"]["orphan_detections"] += 1

            await self.tg.send(
                f"👻 <b>پوزیشن یتیم Adopt شد</b>\n"
                f"نماد: <b>{std_sym}</b> | {side.upper()}\n"
                f"Qty: {qty:.4f} | Entry: {entry:.5f}\n"
                f"PnL تخمینی: ${pnl_est:.2f}\n"
                f"SL: {sl:.5f} | TP: {tp:.5f}\n"
                f"⚠️ SL {ORPHAN_SL_PCT}% / TP {ORPHAN_TP_PCT}% اعمال شد")

        return new_orphans

    def _normalize(self, raw: str) -> Optional[str]:
        """تبدیل فرمت نماد خام به استاندارد"""
        raw = raw.strip()

        # اگر قبلاً استاندارد بود
        all_syms = SYMBOLS or SYMBOLS_CANDIDATE
        if raw in all_syms:
            return raw

        # جستجوی base currency
        for std in all_syms:
            base = std.split("/")[0]
            if raw.upper().startswith(base) or base in raw.upper():
                return std

        # تلاش برای ساخت
        raw_up = raw.upper()
        for std in all_syms:
            base = std.split("/")[0]
            quota = std.split(":")[0]  # BTC/USDT
            if raw_up == quota.replace("/","") or raw_up == f"{base}USDT":
                return std

        return None


# ============================================================================
# 6. DATABASE
# ============================================================================
class Database:
    def __init__(self, path="bot_v17.db"):
        self.path = path

    async def init(self):
        async with aiosqlite.connect(self.path) as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY, symbol TEXT, side TEXT,
                    strategy TEXT, entry_price REAL, qty REAL,
                    original_qty REAL, sl REAL, tp1 REAL, tp REAL,
                    is_partial INTEGER DEFAULT 0,
                    highest_pnl_pct REAL DEFAULT 0,
                    is_orphan INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'open',
                    pnl REAL DEFAULT 0, fees_est REAL DEFAULT 0,
                    net_pnl REAL DEFAULT 0, exit_price REAL DEFAULT 0,
                    slippage_est REAL DEFAULT 0, actual_rr REAL DEFAULT 0,
                    exit_reason TEXT, hold_seconds REAL DEFAULT 0,
                    expected_rr REAL DEFAULT 0,
                    signal_quality REAL DEFAULT 0,
                    rsi_at_entry REAL DEFAULT 0,
                    atr_at_entry REAL DEFAULT 0,
                    htf_trend TEXT DEFAULT '',
                    opened_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    closed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT, action TEXT, strategy TEXT,
                    reason TEXT, price REAL, rsi REAL, atr REAL,
                    htf_trend TEXT, signal_quality REAL DEFAULT 0,
                    spread_pct REAL DEFAULT 0, extra TEXT
                );
                CREATE TABLE IF NOT EXISTS equity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    balance REAL, free REAL, peak REAL,
                    dd REAL, open_pos INTEGER DEFAULT 0,
                    orphan_pos INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS circuit_breaker_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT, event_type TEXT, detail TEXT
                );
                CREATE TABLE IF NOT EXISTS operational_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    error_type TEXT, symbol TEXT, message TEXT
                );
                CREATE TABLE IF NOT EXISTS scan_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT DEFAULT CURRENT_TIMESTAMP,
                    symbols_scanned INTEGER, signals_found INTEGER,
                    signals_executed INTEGER, rejected_spread INTEGER,
                    rejected_no_signal INTEGER, rejected_circuit INTEGER,
                    orphans_found INTEGER DEFAULT 0,
                    scan_duration_ms REAL
                );
            """)
            await db.commit()

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
                 t.get("expected_rr",0),t.get("signal_quality",0),
                 t.get("rsi_at_entry",0),t.get("atr_at_entry",0),
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
                           hold=0.0, exit_price=0.0, actual_rr=0.0):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                UPDATE trades SET status='closed',pnl=?,fees_est=?,
                net_pnl=?,exit_price=?,actual_rr=?,exit_reason=?,
                hold_seconds=?,closed_at=CURRENT_TIMESTAMP WHERE id=?""",
                (pnl,fees,pnl-fees,exit_price,actual_rr,reason,hold,tid))
            await db.commit()

    async def log_decision(self, symbol, action, strategy, reason,
                            price=0, rsi=0, atr=0, htf="",
                            sq=0, spread=0, extra=""):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO decisions
                (symbol,action,strategy,reason,price,rsi,atr,
                 htf_trend,signal_quality,spread_pct,extra)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol,action,strategy,reason,price,rsi,atr,
                 htf,sq,spread,str(extra)[:400]))
            await db.commit()

    async def log_cb(self, sym, evt, detail):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO circuit_breaker_log (symbol,event_type,detail) "
                "VALUES (?,?,?)", (sym,evt,detail[:300]))
            await db.commit()

    async def log_err(self, etype, sym, msg):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO operational_errors (error_type,symbol,message) "
                "VALUES (?,?,?)", (etype,sym,msg[:400]))
            await db.commit()
        with STATE_LOCK:
            SHARED_STATE["operational"]["total_api_errors"] += 1

    async def log_scan(self, sc,fn,ex,rs,rsig,rcb,orp,dur):
        async with aiosqlite.connect(self.path) as db:
            await db.execute("""
                INSERT INTO scan_stats
                (symbols_scanned,signals_found,signals_executed,
                 rejected_spread,rejected_no_signal,rejected_circuit,
                 orphans_found,scan_duration_ms)
                VALUES (?,?,?,?,?,?,?,?)""",
                (sc,fn,ex,rs,rsig,rcb,orp,dur))
            await db.commit()

    async def log_equity(self, bal, free, peak, dd, npos, norp):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "INSERT INTO equity (balance,free,peak,dd,open_pos,orphan_pos) "
                "VALUES (?,?,?,?,?,?)",(bal,free,peak,dd,npos,norp))
            await db.commit()

    async def get_open_trades(self):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM trades WHERE status='open'") as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_closed_trades(self, limit=200):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM trades WHERE status='closed' "
                "ORDER BY closed_at DESC LIMIT ?", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_decisions(self, limit=500):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM decisions ORDER BY id DESC LIMIT ?",
                (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def get_errors(self, limit=50):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM operational_errors "
                "ORDER BY id DESC LIMIT ?", (limit,)) as c:
                return [dict(r) for r in await c.fetchall()]

    async def update_analytics(self):
        async with aiosqlite.connect(self.path) as db:
            async with db.execute(
                "SELECT symbol,strategy,pnl,fees_est,hold_seconds "
                "FROM trades WHERE status='closed'") as c:
                rows = await c.fetchall()
        if not rows: return
        pnls  = [r[2] for r in rows]
        fees  = [r[3] for r in rows]
        holds = [r[4] for r in rows if r[4] and r[4]>0]
        wins  = [p for p in pnls if p>0]
        losses= [p for p in pnls if p<0]
        by_sym = defaultdict(list)
        by_str = defaultdict(list)
        for r in rows:
            by_sym[r[0]].append(r[2])
            by_str[r[1]].append(r[2])
        gp = sum(wins) if wins else 0
        gl = abs(sum(losses)) if losses else 0
        pf = gp/gl if gl>0 else 999.0
        sh = 0.0
        if len(pnls)>1:
            try:
                m=statistics.mean(pnls); s=statistics.stdev(pnls)
                sh=m/s if s>0 else 0
            except Exception: pass
        def cs(lst):
            if not lst: return {}
            w=[p for p in lst if p>0]; l=[p for p in lst if p<0]
            g2=sum(w) if w else 0; l2=abs(sum(l)) if l else 0
            return {"trades":len(lst),"pnl":round(sum(lst),3),
                    "wr":round(len(w)/len(lst)*100,1),
                    "pf":round(g2/l2,2) if l2>0 else 999.0}
        with STATE_LOCK:
            prev = dict(SHARED_STATE["stats"])
            SHARED_STATE["stats"].update({
                "total_trades":len(pnls),"winning_trades":len(wins),
                "losing_trades":len(losses),
                "win_rate":round(len(wins)/len(pnls)*100,1) if pnls else 0,
                "total_pnl":round(sum(pnls),3),
                "total_fees":round(sum(fees),3),
                "net_pnl":round(sum(pnls)-sum(fees),3),
                "avg_hold_min":round(sum(holds)/len(holds)/60,1) if holds else 0,
                "max_win":round(max(pnls),3) if pnls else 0,
                "max_loss":round(min(pnls),3) if pnls else 0,
                "profit_factor":round(pf,2),
                "sharpe_approx":round(sh,3),
                "by_symbol":{s:cs(v) for s,v in by_sym.items()},
                "by_strategy":{s:cs(v) for s,v in by_str.items()},
                "orphans_adopted":prev.get("orphans_adopted",0),
                "orphans_closed":prev.get("orphans_closed",0),
            })

    async def generate_report(self) -> str:
        decisions   = await self.get_decisions(500)
        closed      = await self.get_closed_trades(200)
        open_trades = await self.get_open_trades()
        op_errors   = await self.get_errors(50)

        W   = 70
        sep = "═"*W
        lines = []

        def T(t): lines.extend(["",sep,f"  {t}",sep])
        def S(t): lines.extend(["",f"  ▶ {t}","─"*W])
        def R(l,v): lines.append(f"    {l:<30}: {v}")

        lines.append(sep)
        lines.append(" "*6+"MASTER QUANT ENGINE v17.0 - FULL REPORT")
        lines.append(f" "*6+f"UTC: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(sep)

        with STATE_LOCK:
            st    = dict(SHARED_STATE)
            stats = dict(st.get("stats",{}))
            op    = dict(st.get("operational",{}))

        up = time.time()-op.get("uptime_start",time.time())

        T("📋 SECTION 1: OVERVIEW")
        R("Version",         "v17.0 | Phemex Native + Orphan Manager")
        R("Uptime",          str(timedelta(seconds=int(up))))
        R("Mode",            "TESTNET" if TESTNET else "LIVE")
        R("OHLCV Method",    st.get("ohlcv_method","?"))
        R("Valid Symbols",   str(len(SYMBOLS))+": "+
          " ".join(s.split("/")[0] for s in SYMBOLS))
        R("Balance",         f"${st.get('balance',0):.2f}")
        R("Free",            f"${st.get('free_balance',0):.2f}")
        R("DD",              f"{st.get('current_dd',0):.2f}%")
        R("Daily PnL",       f"${st.get('daily_pnl',0):.2f}")
        R("Positions",       str(len(st.get('active_positions',{}))))
        R("Orphans",         str(len(st.get('orphan_positions',{}))))
        R("Total Trades",    str(stats.get('total_trades',0)))
        R("Win Rate",        f"{stats.get('win_rate',0):.1f}%")
        R("Net PnL",         f"${stats.get('net_pnl',0):.3f}")
        R("Profit Factor",   str(stats.get('profit_factor',0)))
        R("Orphans Adopted", str(stats.get('orphans_adopted',0)))

        T("👻 SECTION 2: ORPHAN POSITIONS")
        orphs_open = [t for t in open_trades if t.get("is_orphan")]
        orphs_cls  = [t for t in closed if t.get("strategy")=="Orphan_Adopted"]

        S("یتیم‌های فعال")
        if orphs_open:
            lines.append(f"  {'نماد':<20}{'جهت':<6}{'ورود':>10}{'Qty':>10}{'SL':>10}{'TP':>10}")
            lines.append("  "+"─"*66)
            for t in orphs_open:
                lines.append(
                    f"  {t['symbol']:<20}{t['side'].upper():<6}"
                    f"{t['entry_price']:>10.5f}{t['qty']:>10.4f}"
                    f"{t['sl']:>10.5f}{t['tp']:>10.5f}")
        else:
            lines.append("  ✅ هیچ یتیم فعالی نیست")

        S("یتیم‌های بسته‌شده")
        for t in orphs_cls[:15]:
            e="✅" if t["pnl"]>0 else "❌"
            lines.append(f"  {e} {t['symbol']:<20} {t['side']:<5} "
                         f"PnL:${t['pnl']:>+8.3f}  {t.get('exit_reason','')}")

        S("تحلیل ایراد یتیم")
        na = stats.get('orphans_adopted',0)
        if na > 10:
            lines.append("  🔴 بیش از 10 یتیم → ربات مکرراً restart می‌شود")
        elif na > 3:
            lines.append("  ⚠️  چندین یتیم → بررسی stability ربات")
        elif na > 0:
            lines.append("  ⚠️  یتیم شناسایی شد → احتمالاً restart قبلی")
        else:
            lines.append("  ✅ هیچ یتیمی شناسایی نشده - stability خوب")

        T("🧠 SECTION 3: STRATEGY ANALYSIS")
        S("عملکرد استراتژی‌ها")
        by_str = stats.get("by_strategy",{})
        if by_str:
            lines.append(f"  {'استراتژی':<25}{'معاملات':>8}{'WR%':>7}{'PnL':>10}{'PF':>6}")
            lines.append("  "+"─"*56)
            for s,sv in sorted(by_str.items(),key=lambda x:x[1].get("pnl",0),reverse=True):
                ic="🟢" if sv.get("pnl",0)>=0 else "🔴"
                lines.append(f"  {ic} {s:<23}{sv['trades']:>8}"
                             f"{sv.get('wr',0):>6.1f}%{sv.get('pnl',0):>+9.3f}"
                             f"{sv.get('pf',0):>6.2f}")
        else:
            lines.append("  هنوز معامله‌ای ثبت نشده")

        S("تشخیص خودکار ایرادات")
        wr  = stats.get('win_rate',0)
        pf  = stats.get('profit_factor',0)
        tot = stats.get('total_trades',0)
        if tot >= 10:
            if wr < 35:   lines.append(f"  🔴 Win Rate {wr}% → زیر breakeven")
            elif wr < 45: lines.append(f"  ⚠️  Win Rate {wr}% → پایین")
            else:          lines.append(f"  ✅ Win Rate {wr}%")
            if pf < 1.0:  lines.append(f"  🔴 PF {pf} < 1.0 → سیستماتیک زیانده")
            elif pf < 1.3:lines.append(f"  ⚠️  PF {pf} پایین")
            else:          lines.append(f"  ✅ PF {pf}")
        else:
            lines.append(f"  ⚠️  فقط {tot} معامله - داده ناکافی")

        T("⚙️ SECTION 4: OPERATIONAL HEALTH")
        R("API Errors",      str(op.get('total_api_errors',0)))
        R("Position Mode Fix",str(op.get('position_mode_fixes',0)))
        R("Circuit Breaker", str(op.get('circuit_breaker_events',0)))
        R("Orphan Detect",   str(op.get('orphan_detections',0)))
        R("Scan Count",      str(st.get('scan_count',0)))

        T("🟢 SECTION 5: OPEN POSITIONS")
        reg = [t for t in open_trades if not t.get("is_orphan")]
        if reg:
            for t in reg:
                ic="🟢" if t["side"]=="buy" else "🔴"
                lines.append(f"  {ic} {t['symbol']:<20}{t['side'].upper():<6}"
                             f"Entry:{t['entry_price']:.5f} Qty:{t['qty']:.4f} "
                             f"Strat:{t.get('strategy','?')}")
        else:
            lines.append("  هیچ پوزیشن عادی باز نیست")

        T("📈 SECTION 6: CLOSED TRADES (Last 30)")
        if closed:
            lines.append(f"  {'نماد':<20}{'جهت':<5}{'PnL':>9}{'Hold':>7}{'علت':<15}{'Orphan'}")
            lines.append("  "+"─"*60)
            for t in closed[:30]:
                e="✅" if t["pnl"]>0 else "❌"
                hold=int((t.get("hold_seconds") or 0)/60)
                orp="👻" if t.get("is_orphan") else ""
                lines.append(f"  {e} {t['symbol']:<18}{t['side']:<5}"
                             f"{t['pnl']:>+8.3f}{hold:>6}m "
                             f"{(t.get('exit_reason','') or ''):<15} {orp}")
        else:
            lines.append("  هنوز معامله‌ای بسته نشده")

        T("🔍 SECTION 7: SIGNAL ANALYSIS")
        if decisions:
            acts     = Counter(d["action"] for d in decisions)
            sigs     = sum(v for k,v in acts.items() if k!="neutral")
            rejs     = acts.get("neutral",0)
            reasons  = Counter()
            for d in decisions:
                if d["action"]=="neutral":
                    reasons[(d.get("reason") or "?")[:45]] += 1
            R("کل تصمیمات", str(len(decisions)))
            R("سیگنال‌ها",  str(sigs))
            R("رد شده",     str(rejs))
            lines.append("")
            lines.append("  دلایل رد:")
            for r,n in reasons.most_common(10):
                pct=n/rejs*100 if rejs else 0
                lines.append(f"  {n:>5} ({pct:>5.1f}%)  {r}")

        T("⚠️ SECTION 8: RECENT ERRORS")
        if op_errors:
            for e in op_errors[:15]:
                lines.append(f"  [{(e.get('ts',''))[:16]}] "
                             f"{e.get('error_type','?'):<18} "
                             f"{e.get('symbol',''):<16} "
                             f"{e.get('message','')[:50]}")
        else:
            lines.append("  ✅ هیچ خطایی ثبت نشده")

        lines.extend(["",sep,
            f"  v17.0 | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",sep])
        return "\n".join(lines)


# ============================================================================
# 7. INDICATORS
# ============================================================================
class Indicators:
    @staticmethod
    def rsi(s,n=14):
        d=s.diff();u=d.clip(lower=0);dn=-d.clip(upper=0)
        return 100-(100/(1+u.ewm(com=n-1,adjust=False).mean()/
                         dn.ewm(com=n-1,adjust=False).mean().replace(0,1e-10)))

    @staticmethod
    def atr(df,n=14):
        tr=pd.concat([df["high"]-df["low"],
            (df["high"]-df["close"].shift()).abs(),
            (df["low"]-df["close"].shift()).abs()],axis=1).max(axis=1)
        return tr.ewm(com=n-1,adjust=False).mean()

    @staticmethod
    def ema(s,span): return s.ewm(span=span,adjust=False).mean()
    @staticmethod
    def sma(s,p): return s.rolling(p).mean()

    @staticmethod
    def supertrend(df,period=10,mult=3.0):
        a=Indicators.atr(df,period)
        hl2=(df["high"]+df["low"])/2
        up=hl2+mult*a; lo=hl2-mult*a
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
    def macd(s,f=12,sl=26,sg=9):
        m=s.ewm(span=f,adjust=False).mean()-s.ewm(span=sl,adjust=False).mean()
        return m,m.ewm(span=sg,adjust=False).mean(),m-m.ewm(span=sg,adjust=False).mean()

    @staticmethod
    def highest(s,p): return s.rolling(p).max()
    @staticmethod
    def lowest(s,p): return s.rolling(p).min()


# ============================================================================
# 8. STRATEGY ENGINE
# ============================================================================
class StrategyEngine:
    def analyze(self, df5, df1, sym):
        df  = df5.iloc[:-1].copy()
        htf = df1.iloc[:-1].copy()
        if len(df)<50 or len(htf)<20:
            return self._n("داده ناکافی")

        hc=htf["close"]
        e50h=Indicators.ema(hc,min(50,len(hc))).iloc[-1]
        e200h=Indicators.ema(hc,min(200,len(hc))).iloc[-1]
        hp=float(hc.iloc[-1])

        if hp>e50h and e50h>e200h*0.998: htf_t="bullish"
        elif hp<e50h and e50h<e200h*1.002: htf_t="bearish"
        else: return self._n("HTF نامشخص",htf="sideways")

        c=df["close"]; h=df["high"]; l=df["low"]; v=df["volume"]
        px=float(c.iloc[-1])
        atr_s=Indicators.atr(df,14)
        atr=float(atr_s.iloc[-1])
        if atr<=0: return self._n("ATR صفر",htf=htf_t)

        atr_sma=float(Indicators.sma(atr_s,20).iloc[-1])
        cfg=SYMBOL_CONFIG.get(sym,{})
        mn=px*cfg.get("min_atr_pct",0.05)/100
        mx=px*cfg.get("max_atr_pct",5.0)/100
        if atr<mn or atr>mx: return self._n("ATR خارج",atr=atr,htf=htf_t)

        rsi_s=Indicators.rsi(c)
        rv=float(rsi_s.iloc[-1]); rp=float(rsi_s.iloc[-2])
        e20=float(Indicators.ema(c,20).iloc[-1])
        e50=float(Indicators.ema(c,50).iloc[-1])
        st_d,st_u,st_l=Indicators.supertrend(df)
        _,_,mh=Indicators.macd(c)
        vsma=float(Indicators.sma(v,20).iloc[-1])
        vc=float(v.iloc[-1])
        h10=float(Indicators.highest(h,10).iloc[-1])
        l10=float(Indicators.lowest(l,10).iloc[-1])
        mv=cfg.get("min_vol_mult",1.1)

        if htf_t=="bullish" and px>e20 and px>=h10*0.999 and 48<rv<75 and vc>vsma*mv and float(mh.iloc[-1])>0:
            return self._b("buy","Breakout_Momentum",px,atr,rv,htf_t)
        if htf_t=="bearish" and px<e20 and px<=l10*1.001 and 25<rv<52 and vc>vsma*mv and float(mh.iloc[-1])<0:
            return self._b("sell","Breakout_Momentum",px,atr,rv,htf_t)
        if htf_t=="bullish" and px>e20>e50*0.999 and rp<=42 and rv>rp and rv<62:
            return self._b("buy","MTF_Pullback",px,atr,rv,htf_t)
        if htf_t=="bearish" and px<e20<e50*1.001 and rp>=58 and rv<rp and rv>38:
            return self._b("sell","MTF_Pullback",px,atr,rv,htf_t)
        if htf_t=="bullish" and st_d.iloc[-1]==1 and l.iloc[-1]<=st_l.iloc[-1]*1.005 and c.iloc[-1]>c.iloc[-2] and 38<rv<65:
            return self._b("buy","SuperTrend_Pullback",px,atr,rv,htf_t)
        if htf_t=="bearish" and st_d.iloc[-1]==-1 and h.iloc[-1]>=st_u.iloc[-1]*0.995 and c.iloc[-1]<c.iloc[-2] and 35<rv<62:
            return self._b("sell","SuperTrend_Pullback",px,atr,rv,htf_t)
        if htf_t=="bullish" and px>e20 and vc>vsma*1.5 and c.iloc[-1]>c.iloc[-2] and 48<rv<70:
            return self._b("buy","Volume_Surge",px,atr,rv,htf_t)
        if htf_t=="bearish" and px<e20 and vc>vsma*1.5 and c.iloc[-1]<c.iloc[-2] and 30<rv<52:
            return self._b("sell","Volume_Surge",px,atr,rv,htf_t)

        e20p=float(Indicators.ema(c,20).iloc[-2])
        e50p=float(Indicators.ema(c,50).iloc[-2])
        if htf_t=="bullish" and e20p<=e50p and e20>e50 and rv>45:
            return self._b("buy","EMA_Cross",px,atr,rv,htf_t)
        if htf_t=="bearish" and e20p>=e50p and e20<e50 and rv<55:
            return self._b("sell","EMA_Cross",px,atr,rv,htf_t)

        return self._n(f"بدون سیگنال RSI={rv:.1f}",rsi=rv,atr=atr,htf=htf_t)

    def _n(self,r,rsi=0,atr=0,htf=""):
        return {"action":"neutral","reason":r,"strat":"",
                "rsi":rsi,"atr":atr,"htf":htf,"signal_quality":0}

    def _b(self,side,strat,price,atr,rsi,htf):
        p=STRATEGY_PARAMS.get(strat,{"sl_m":1.5,"tp_m":2.8,"tp1_m":1.4})
        sm,tm,t1=p["sl_m"],p["tp_m"],p["tp1_m"]
        if side=="buy":
            return {"action":"buy","strat":strat,
                    "sl":price-atr*sm,"tp":price+atr*tm,"tp1":price+atr*t1,
                    "reason":f"سیگنال {strat}","rsi":rsi,"atr":atr,"htf":htf,
                    "expected_rr":round(tm/sm,2),"signal_quality":55.0}
        return {"action":"sell","strat":strat,
                "sl":price+atr*sm,"tp":price-atr*tm,"tp1":price-atr*t1,
                "reason":f"سیگنال {strat}","rsi":rsi,"atr":atr,"htf":htf,
                "expected_rr":round(tm/sm,2),"signal_quality":55.0}


# ============================================================================
# 9. RISK + CIRCUIT BREAKER
# ============================================================================
class RiskManager:
    @staticmethod
    def calc_qty(bal,px,sl,free,sym,ex,sq=50):
        if px<=0 or bal<=0: return 0.0
        dist=abs(px-sl)
        if dist<=0: return 0.0
        qm=0.6+(sq/100.0)*0.4
        risk=bal*(RISK_PCT/100.0)*qm
        qty=risk/dist
        cfg=SYMBOL_CONFIG.get(sym,{})
        m1=(free*0.15*LEVERAGE)/px
        m2=(bal*MAX_SINGLE_EXP/100.0)/px
        m3=cfg.get("max_usd_pos",200.0)/px
        qty=min(qty,m1,m2,m3)
        try:
            qty=float(ex.amount_to_precision(sym,qty))
            if qty*px<MIN_ORDER_USD:
                qty=float(ex.amount_to_precision(sym,MIN_ORDER_USD/px))
        except Exception: return 0.0
        return max(qty,0.0)

    @staticmethod
    def check_global():
        with STATE_LOCK: s=dict(SHARED_STATE)
        if s["dd_halted"]:     return False,f"DD {s['current_dd']:.1f}%"
        if s["daily_halted"]:  return False,"Daily Loss"
        if not s["is_active"]: return False,"Paused"
        if len(s["active_positions"])>=MAX_POS: return False,f"MAX_POS={MAX_POS}"
        if s["balance"]<20:    return False,"Balance Low"
        return True,""


class CircuitBreaker:
    def __init__(self,db): self.db=db

    def is_ok(self,sym):
        now=time.time()
        with STATE_LOCK:
            if SHARED_STATE["symbol_cooldowns"].get(sym,0)>now:
                return False,f"Loss CD {int((SHARED_STATE['symbol_cooldowns'][sym]-now)/60)}m"
            e=SHARED_STATE["symbol_errors"].get(sym,{})
            if e.get("cooldown_end",0)>now:
                return False,f"Err CD {int((e['cooldown_end']-now)/60)}m"
        return True,""

    async def reg_err(self,sym,err):
        with STATE_LOCK:
            e=SHARED_STATE["symbol_errors"]
            if sym not in e: e[sym]={"count":0,"cooldown_end":0}
            e[sym]["count"]+=1
            n=e[sym]["count"]
            cd=min(30*(2**(n-1)),MAX_ERR_COOLDOWN)
            e[sym]["cooldown_end"]=time.time()+cd
        await self.db.log_err("api_error",sym,err[:200])

    async def reg_loss(self,sym,pnl):
        with STATE_LOCK:
            cl=SHARED_STATE["consecutive_losses"]
            if sym not in cl: cl[sym]={"count":0}
            cl[sym]["count"]+=1; n=cl[sym]["count"]
            if n>=CONSEC_LOSS_LIMIT:
                SHARED_STATE["symbol_cooldowns"][sym]=time.time()+COOLDOWN_HOURS*3600
                SHARED_STATE["operational"]["circuit_breaker_events"]+=1
        if n>=CONSEC_LOSS_LIMIT:
            await self.db.log_cb(sym,"consec_loss",f"n={n}|pnl={pnl:.3f}")
            return True
        return False

    def reg_win(self,sym):
        with STATE_LOCK:
            SHARED_STATE["consecutive_losses"].pop(sym,None)
            if sym in SHARED_STATE["symbol_errors"]:
                SHARED_STATE["symbol_errors"][sym]["count"]=0
                SHARED_STATE["symbol_errors"][sym]["cooldown_end"]=0

    async def fix_pm(self,ex,sym):
        try:
            await ex.set_position_mode(False,sym)
            with STATE_LOCK:
                SHARED_STATE["operational"]["position_mode_fixes"]+=1
            return True
        except Exception as e:
            log.warning(f"fix_pm {sym}: {e}")
        return False


# ============================================================================
# 10. TELEGRAM
# ============================================================================
class TG:
    def __init__(self,engine):
        self.eng=engine
        self.base=f"https://api.telegram.org/bot{TG_TOKEN}"
        self.offset=0

    def menu(self):
        btn="⏸️ Pause" if SHARED_STATE["is_active"] else "▶️ Start"
        act="cmd_pause" if SHARED_STATE["is_active"] else "cmd_start"
        return {"inline_keyboard":[
            [{"text":"📊 Dash","callback_data":"cmd_dash"},
             {"text":"💼 Positions","callback_data":"cmd_pos"}],
            [{"text":"👻 Orphans","callback_data":"cmd_orphan"},
             {"text":"🔄 Sync","callback_data":"cmd_sync"}],
            [{"text":btn,"callback_data":act},
             {"text":"📈 Stats","callback_data":"cmd_stats"}],
            [{"text":"🔴 Circuit","callback_data":"cmd_cb"},
             {"text":"⚠️ Errors","callback_data":"cmd_err"}],
            [{"text":"⚡ Test","callback_data":"cmd_test"},
             {"text":"📄 Report","callback_data":"cmd_txt"}],
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

    async def send_doc(self,path,cap=""):
        if not os.path.exists(path): await self.send("❌ فایل نبود"); return
        try:
            with open(path,"rb") as f:
                form=aiohttp.FormData()
                form.add_field("chat_id",TG_CHAT)
                form.add_field("caption",cap[:1000])
                form.add_field("document",f,filename=os.path.basename(path))
                async with aiohttp.ClientSession() as s:
                    await s.post(f"{self.base}/sendDocument",data=form,
                                 timeout=aiohttp.ClientTimeout(total=60))
        except Exception as e: await self.send(f"❌ {e}")

    async def poll(self):
        if not TG_TOKEN: return
        await self.send(
            f"🚀 <b>Quant v17.0</b>\n"
            f"Phemex Native | Orphan Manager\n"
            f"✅ {len(SYMBOLS)} نماد | OHLCV:{SHARED_STATE.get('ohlcv_method','?')}",
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
            except Exception as e: log.error(f"TG: {e}")
            await asyncio.sleep(1)

    async def _cmd(self,cmd):
        eng=self.eng
        if cmd=="cmd_start":
            with STATE_LOCK: SHARED_STATE["is_active"]=True
            await self.send("▶️ فعال",self.menu())
        elif cmd=="cmd_pause":
            with STATE_LOCK: SHARED_STATE["is_active"]=False
            await self.send("⏸️ متوقف",self.menu())
        elif cmd=="cmd_dash":
            with STATE_LOCK: st=dict(SHARED_STATE)
            await self.send(
                f"📊 <b>v17.0</b>\n"
                f"💰 ${st['balance']:.2f} | آزاد:${st['free_balance']:.2f}\n"
                f"DD:{st['current_dd']:.2f}% Daily:${st['daily_pnl']:.2f}\n"
                f"Pos:{len(st['active_positions'])}/{MAX_POS} Orphan:👻{len(st['orphan_positions'])}\n"
                f"WR:{st['stats']['win_rate']}% PnL:${st['stats']['net_pnl']:.2f}\n"
                f"OHLCV:{st.get('ohlcv_method','?')} Scan:{st['last_scan']}",
                self.menu())
        elif cmd=="cmd_pos":
            with STATE_LOCK:
                all_p={**SHARED_STATE["active_positions"],**SHARED_STATE["orphan_positions"]}
            if not all_p:
                await self.send("💤 هیچ پوزیشنی نیست",self.menu()); return
            msg="💼 <b>پوزیشن‌ها:</b>\n\n"
            for p in all_p.values():
                px=eng.feed.get_price(p["symbol"]) or p["entry"]
                pnl=(px-p["entry"])*p["qty"]*(1 if p["side"]=="buy" else -1)
                ic="🟢" if pnl>=0 else "🔴"
                ot="👻" if p.get("is_orphan") else ""
                msg+=(f"{ic}{ot} <b>{p['symbol'].split('/')[0]}</b> {p['side'].upper()}\n"
                      f"  Entry:{p['entry']:.4f} PnL:${pnl:.2f}\n"
                      f"  SL:{p['sl']:.4f} TP:{p['tp']:.4f}\n\n")
            await self.send(msg,self.menu())
        elif cmd=="cmd_orphan":
            with STATE_LOCK:
                orp=dict(SHARED_STATE["orphan_positions"])
                st=dict(SHARED_STATE["stats"])
            msg=(f"👻 <b>Orphan Manager</b>\n\n"
                 f"فعال: {len(orp)}\n"
                 f"Adopt شده: {st.get('orphans_adopted',0)}\n"
                 f"بسته‌شده: {st.get('orphans_closed',0)}\n\n")
            for p in orp.values():
                msg+=f"• {p['symbol'].split('/')[0]} {p['side'].upper()} {p['qty']:.4f}\n"
            if not orp: msg+="✅ هیچ یتیمی نیست"
            await self.send(msg,self.menu())
        elif cmd=="cmd_sync":
            await eng.smart_sync()
            await self.send("🔄 Sync OK",self.menu())
        elif cmd=="cmd_stats":
            with STATE_LOCK: s=dict(SHARED_STATE["stats"])
            await self.send(
                f"📈 <b>Stats</b>\nمعاملات:{s.get('total_trades',0)}\n"
                f"WR:{s.get('win_rate',0):.1f}% PF:{s.get('profit_factor',0):.2f}\n"
                f"Net PnL:${s.get('net_pnl',0):.3f}\n"
                f"Orphan Adopt:{s.get('orphans_adopted',0)}",self.menu())
        elif cmd=="cmd_cb":
            now=time.time()
            with STATE_LOCK:
                cds=dict(SHARED_STATE["symbol_cooldowns"])
                cls=dict(SHARED_STATE["consecutive_losses"])
            ac={s:v for s,v in cds.items() if v>now}
            msg=("🔴 <b>Circuit Breaker</b>\n\n"
                 +(("\n".join(f"⛔{s.split('/')[0]}:{int((v-now)/60)}m" for s,v in ac.items())) or "✅ None")
                 +"\n\nLosses:\n"
                 +(("\n".join(f"{s.split('/')[0]}:{v['count']}" for s,v in cls.items())) or "✅ Clear"))
            await self.send(msg,self.menu())
        elif cmd=="cmd_err":
            errs=await eng.db.get_errors(10)
            msg="⚠️ <b>Errors:</b>\n\n"
            for e in errs:
                msg+=f"[{(e.get('ts',''))[:16]}] {e.get('message','')[:60]}\n\n"
            if not errs: msg+="✅ None"
            await self.send(msg,self.menu())
        elif cmd=="cmd_test":
            asyncio.create_task(eng.real_test())
        elif cmd=="cmd_txt":
            await self.send("⏳ در حال تهیه گزارش...")
            r=await eng.db.generate_report()
            fn=f"v17_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(fn,"w",encoding="utf-8") as f: f.write(r)
            await self.send_doc(fn,f"📄 v17.0 | {os.path.getsize(fn)//1024}KB")


# ============================================================================
# 11. QUANT ENGINE
# ============================================================================
class QuantEngine:
    def __init__(self):
        self.db       = Database()
        self.strategy = StrategyEngine()
        self.risk     = RiskManager()
        self.cb       = CircuitBreaker(self.db)

        url = PHEMEX_TESTNET_URL if TESTNET else PHEMEX_LIVE_URL
        self.ex = ccxt.phemex({
            "apiKey":API_KEY,"secret":API_SECRET,
            "enableRateLimit":True,
            "options":{"defaultType":"swap"},
            "timeout":30000,
            "urls":{"api":{"public":url,"private":url}},
        })
        self.ex.set_sandbox_mode(TESTNET)

        self.ohlcv_eng  = PhemexOHLCV(self.ex, url)
        self.feed       = PhemexDataFeed(self.ex, self.ohlcv_eng)
        self.validator  = SymbolValidator(self.ex)
        self.tg         = TG(self)
        self.orphan_mgr: Optional[OrphanManager] = None
        self.open_times: Dict[str,float] = {}

    async def start(self):
        global SYMBOLS
        await self.db.init()
        log.info("🚀 Master Quant v17.0")

        # ─── اتصال ───────────────────────────────────────────────────
        try:
            await self.ex.load_markets()
            with STATE_LOCK: SHARED_STATE["phemex_status"]="connected"
            log.info(f"✅ Phemex connected | {len(self.ex.markets)} markets")
        except Exception as e:
            log.error(f"Connect: {e}")
            with STATE_LOCK: SHARED_STATE["phemex_status"]="error"

        # ─── اعتبارسنجی نمادها (فقط market check) ──────────────────
        SYMBOLS = await self.validator.validate_all(SYMBOLS_CANDIDATE)
        with STATE_LOCK: SHARED_STATE["valid_symbols"]=SYMBOLS
        log.info(f"✅ نمادهای معتبر: {[s.split('/')[0] for s in SYMBOLS]}")

        # ─── کشف روش OHLCV ───────────────────────────────────────────
        if SYMBOLS:
            await self.ohlcv_eng.discover_method(SYMBOLS[0])

        # ─── Position Mode per-symbol ─────────────────────────────────
        for sym in SYMBOLS:
            try:
                await self.ex.set_position_mode(False, sym)
            except Exception as e:
                log.debug(f"PM {sym.split('/')[0]}: {e}")
            await asyncio.sleep(0.2)

        # ─── Orphan Manager ───────────────────────────────────────────
        self.orphan_mgr = OrphanManager(
            self.db, self.tg, self.ex, self.feed)

        # ─── Load DB ──────────────────────────────────────────────────
        for t in await self.db.get_open_trades():
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
                    SHARED_STATE["orphan_positions"][t["id"]]=pos
                else:
                    SHARED_STATE["active_positions"][t["id"]]=pos
            self.open_times[t["id"]]=time.time()

        log.info(f"DB: {len(SHARED_STATE['active_positions'])} عادی | "
                 f"{len(SHARED_STATE['orphan_positions'])} یتیم")

        # ─── اولین Sync + Orphan Scan ────────────────────────────────
        await self.smart_sync()
        await self.update_balance()

        # ─── اسکن اولیه یتیم‌ها ──────────────────────────────────────
        await self._do_orphan_scan()

        await asyncio.gather(
            self.price_loop(),
            self.scan_loop(),
            self.orphan_loop(),
            self.watchdog_loop(),
            self.equity_loop(),
            self.tg.poll()
        )

    async def update_balance(self):
        try:
            b=await self.ex.fetch_balance()
            total=float(b.get("USDT",{}).get("total",0) or 0)
            free =float(b.get("USDT",{}).get("free",0)  or 0)
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
                prices=await self.feed.fetch_all_tickers()
                with STATE_LOCK:
                    SHARED_STATE["phemex_status"]="live"
                bal,_=await self.update_balance()
                with STATE_LOCK:
                    peak=SHARED_STATE["peak_balance"]
                    if peak>0 and bal>0:
                        dd=(peak-bal)/peak*100
                        SHARED_STATE["current_dd"]=dd
                        SHARED_STATE["dd_halted"]=dd>=MAX_DD
                    ds=SHARED_STATE["day_start_balance"]
                    if ds>0:
                        dp=bal-ds
                        SHARED_STATE["daily_pnl"]=dp
                        SHARED_STATE["daily_halted"]=(dp/ds*100<=-MAX_DAILY_LOSS)
            except Exception as e:
                log.error(f"price_loop: {e}")
                with STATE_LOCK: SHARED_STATE["phemex_status"]="error"
            await asyncio.sleep(PRICE_LOOP_SEC)

    async def equity_loop(self):
        while True:
            await asyncio.sleep(EQUITY_LOG_SEC)
            with STATE_LOCK:
                b=SHARED_STATE["balance"];f=SHARED_STATE["free_balance"]
                p=SHARED_STATE["peak_balance"];d=SHARED_STATE["current_dd"]
                n=len(SHARED_STATE["active_positions"])
                o=len(SHARED_STATE["orphan_positions"])
            await self.db.log_equity(b,f,p,d,n,o)

    async def _do_orphan_scan(self):
        """اسکن یتیم‌ها"""
        if not self.orphan_mgr: return
        with STATE_LOCK:
            known={**dict(SHARED_STATE["active_positions"]),
                   **dict(SHARED_STATE["orphan_positions"])}
        new=await self.orphan_mgr.scan(known)
        if new:
            with STATE_LOCK:
                SHARED_STATE["orphan_positions"].update(new)
            self.open_times.update(self.orphan_mgr.open_times)
            log.info(f"👻 {len(new)} یتیم adopt شد")

    async def orphan_loop(self):
        await asyncio.sleep(20)
        while True:
            try:
                await self._do_orphan_scan()
            except Exception as e:
                log.error(f"orphan_loop: {e}")
            await asyncio.sleep(ORPHAN_SCAN_SEC)

    async def scan_loop(self):
        while True:
            ok,r=self.risk.check_global()
            if not ok:
                await asyncio.sleep(12); continue

            t0=time.time()
            with STATE_LOCK:
                SHARED_STATE["last_scan"]=time.strftime("%H:%M:%S")
                SHARED_STATE["scan_count"]+=1

            sc=fn=ex=rs=rsig=rcb=0
            for sym in SYMBOLS:
                with STATE_LOCK:
                    ap={**SHARED_STATE["active_positions"],**SHARED_STATE["orphan_positions"]}
                    if any(p["symbol"]==sym for p in ap.values()): continue
                sc+=1
                ok2,_=self.cb.is_ok(sym)
                if not ok2: rcb+=1; continue
                try:
                    df5=await self.feed.fetch_ohlcv(sym,TIMEFRAME,120)
                    await asyncio.sleep(SYMBOL_DELAY)
                    df1=await self.feed.fetch_ohlcv(sym,HTF_TIMEFRAME,80)
                    await asyncio.sleep(0.4)
                    if df5 is None or len(df5)<50: continue
                    if df1 is None or len(df1)<15: df1=df5.copy()

                    sig=self.strategy.analyze(df5,df1,sym)
                    px=self.feed.get_price(sym)
                    if not px or px<=0: continue

                    spread=await self.feed.get_spread(sym)
                    await asyncio.sleep(0.3)
                    if spread>2.5 and spread<999:
                        rs+=1
                        await self.db.log_decision(sym,"neutral","",
                            f"Spread {spread:.2f}%",price=px,spread=spread)
                        continue

                    await self.db.log_decision(sym,sig["action"],
                        sig.get("strat",""),sig.get("reason",""),
                        px,sig.get("rsi",0),sig.get("atr",0),
                        sig.get("htf",""),sig.get("signal_quality",0),spread)

                    if sig["action"]!="neutral":
                        fn+=1
                        with STATE_LOCK: SHARED_STATE["signal_count"]+=1
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
                        done=await self.execute(sym,sig)
                        if done: ex+=1
                    else:
                        rsig+=1
                        with STATE_LOCK: SHARED_STATE["rejected_count"]+=1
                except Exception as e:
                    log.error(f"scan {sym}: {e}")
                    await self.cb.reg_err(sym,str(e))
                await asyncio.sleep(SYMBOL_DELAY)

            dur=(time.time()-t0)*1000
            await self.db.log_scan(sc,fn,ex,rs,rsig,rcb,
                len(SHARED_STATE.get("orphan_positions",{})),dur)
            log.info(f"📡 #{SHARED_STATE['scan_count']} ⏱️{dur:.0f}ms "
                     f"✅{ex}/{fn} ❌{rsig}sig {rs}sp {rcb}cb")
            await asyncio.sleep(SCAN_INTERVAL)

    async def execute(self,sym,sig)->bool:
        px=self.feed.get_price(sym)
        with STATE_LOCK: bal=SHARED_STATE["balance"]; free=SHARED_STATE["free_balance"]
        if not px or bal<20 or free<15: return False
        try:
            qty=self.risk.calc_qty(bal,px,sig["sl"],free,sym,self.ex,
                                    sig.get("signal_quality",50))
            if qty<=0:
                await self.db.log_decision(sym,"rejected",sig.get("strat",""),"حجم صفر")
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
            with STATE_LOCK: SHARED_STATE["active_positions"][pid]=pos
            self.open_times[pid]=time.time()
            await self.db.insert_trade(pos)
            self.cb.reg_win(sym)
            await self.tg.send(
                f"🎯 <b>{sig['action'].upper()}</b> {sym.split('/')[0]}\n"
                f"Fill:{fill:.5f} SL:{sig['sl']:.5f} TP:{sig['tp']:.5f}\n"
                f"RR:{act_rr:.2f}x Qty:{qty}")
            return True
        except Exception as e:
            err=str(e)
            log.error(f"execute {sym}: {err}")
            if "20004" in err or "INCONSISTENT" in err.upper():
                await self.cb.fix_pm(self.ex,sym)
            else:
                await self.cb.reg_err(sym,err)
            return False

    async def smart_sync(self):
        try:
            remote=await self.ex.fetch_positions()
            active=set()
            for p in remote:
                if abs(float(p.get("contracts") or 0))>0:
                    raw=p.get("symbol","")
                    m=next((s for s in SYMBOLS if s.split("/")[0] in raw.upper()),None)
                    if m: active.add(m)
            with STATE_LOCK:
                to_del=[pid for pid,p in SHARED_STATE["active_positions"].items()
                        if p["symbol"] not in active and p.get("strategy")!="RealTest"]
            for pid in to_del:
                await self.db.close_trade(pid,0.0,reason="remote_closed")
                with STATE_LOCK: SHARED_STATE["active_positions"].pop(pid,None)
            with STATE_LOCK: SHARED_STATE["operational"]["sync_count"]+=1
            log.info(f"🔄 Sync | Exchange:{len(active)} closed:{len(to_del)}")
        except Exception as e:
            log.error(f"sync: {e}")

    async def real_test(self):
        await self.tg.send("⚡ تست...")
        sym=next((s for s in SYMBOLS if "ADA" in s or "XRP" in s or "DOGE" in s),
                  SYMBOLS[0] if SYMBOLS else None)
        if not sym: await self.tg.send("❌ نماد نیافت"); return
        try:
            bal,_=await self.update_balance()
            if bal<20: await self.tg.send("❌ موجودی"); return
            px=self.feed.get_price(sym)
            if not px: await self.tg.send("❌ قیمت"); return
            qty=float(self.ex.amount_to_precision(sym,min(12,bal*0.04)/px))
            order=await self.ex.create_market_order(sym,"buy",qty)
            fill=float(order.get("average") or px)
            pid=f"test_{uuid.uuid4().hex[:6]}"
            pos={"id":pid,"symbol":sym,"side":"buy","strategy":"RealTest",
                 "entry":fill,"qty":qty,"sl":fill*0.97,"tp":fill*1.03,
                 "tp1":fill*1.015,"is_partial":0,"highest_pnl_pct":0.0,
                 "expected_rr":1.0,"signal_quality":50,"is_orphan":False}
            with STATE_LOCK: SHARED_STATE["active_positions"][pid]=pos
            self.open_times[pid]=time.time()
            await self.tg.send(f"🧪 {sym.split('/')[0]} @ {fill:.5f} | 30s...")
            await asyncio.sleep(30)
            await self.force_close(pid,"RealTest")
            await self.tg.send("✅ تست بسته شد")
        except Exception as e: await self.tg.send(f"❌ {e}")

    async def force_close(self,pid,reason):
        with STATE_LOCK:
            pos=(SHARED_STATE["active_positions"].get(pid) or
                 SHARED_STATE["orphan_positions"].get(pid))
        if not pos: return
        px=self.feed.get_price(pos["symbol"]) or pos["entry"]
        hold=time.time()-self.open_times.get(pid,time.time())
        try:
            cs="sell" if pos["side"]=="buy" else "buy"
            order=await self.ex.create_market_order(
                pos["symbol"],cs,pos["qty"],params={"reduceOnly":True})
            ep=float(order.get("average") or px)
            raw=(ep-pos["entry"])*pos["qty"]*(1 if pos["side"]=="buy" else -1)
            fees=abs(raw)*TAKER_FEE*2*FEE_BUFFER
            net=raw-fees
            dsl=abs(pos["entry"]-pos["sl"])
            dp=abs(ep-pos["entry"])
            rr=(dp/dsl) if dsl>0 else 0
            if pos.get("strategy")!="RealTest":
                await self.db.close_trade(pid,raw,fees,reason,hold,ep,rr)
                if net<0:
                    cb=await self.cb.reg_loss(pos["symbol"],net)
                    if cb: await self.tg.send(f"🛑 Circuit {pos['symbol'].split('/')[0]}")
                else: self.cb.reg_win(pos["symbol"])
                if pos.get("is_orphan"):
                    with STATE_LOCK: SHARED_STATE["stats"]["orphans_closed"]+=1
            with STATE_LOCK:
                SHARED_STATE["active_positions"].pop(pid,None)
                SHARED_STATE["orphan_positions"].pop(pid,None)
            self.open_times.pop(pid,None)
            await self.db.update_analytics()
            ic="🟢" if net>=0 else "🔴"
            ot="👻" if pos.get("is_orphan") else ""
            await self.tg.send(f"{ic}{ot} بسته ({reason})\n"
                               f"{pos['symbol'].split('/')[0]} {pos['side'].upper()}\n"
                               f"PnL:${net:.3f} RR:{rr:.2f}x Hold:{int(hold/60)}m")
        except Exception as e: log.error(f"force_close: {e}")

    async def watchdog_loop(self):
        while True:
            with STATE_LOCK:
                all_p={**dict(SHARED_STATE["active_positions"]),
                       **dict(SHARED_STATE["orphan_positions"])}
            for pid,pos in all_p.items():
                if pos.get("strategy")=="RealTest": continue
                px=self.feed.get_price(pos["symbol"])
                if not px: continue
                pct=((px-pos["entry"])/pos["entry"]*100
                     if pos["side"]=="buy"
                     else (pos["entry"]-px)/pos["entry"]*100)
                if pct>TRAIL_ACT and pct>pos["highest_pnl_pct"]:
                    pos["highest_pnl_pct"]=pct
                    if pos["side"]=="buy":
                        nsl=px*(1-TRAIL_STEP/100)
                        if nsl>pos["sl"]: pos["sl"]=nsl
                    else:
                        nsl=px*(1+TRAIL_STEP/100)
                        if nsl<pos["sl"]: pos["sl"]=nsl
                    await self.db.update_trade(pid,pos["qty"],pos["sl"],
                        pos["is_partial"],pos["highest_pnl_pct"])
                if PARTIAL_TP and pos["is_partial"]==0:
                    hit=((pos["side"]=="buy" and px>=pos["tp1"]) or
                         (pos["side"]=="sell" and px<=pos["tp1"]))
                    if hit:
                        try:
                            h=float(self.ex.amount_to_precision(pos["symbol"],pos["qty"]/2))
                            if h>0:
                                cs="sell" if pos["side"]=="buy" else "buy"
                                await self.ex.create_market_order(pos["symbol"],cs,h,
                                    params={"reduceOnly":True})
                                pos["qty"]-=h; pos["is_partial"]=1; pos["sl"]=pos["entry"]
                                await self.db.update_trade(pid,pos["qty"],pos["sl"],1,
                                    pos["highest_pnl_pct"])
                                await self.tg.send(f"🔹 Partial TP {pos['symbol'].split('/')[0]}")
                        except Exception as e: log.error(f"partial_tp: {e}")
                sl_hit=((pos["side"]=="buy" and px<=pos["sl"]) or
                        (pos["side"]=="sell" and px>=pos["sl"]))
                tp_hit=((pos["side"]=="buy" and px>=pos["tp"]) or
                        (pos["side"]=="sell" and px<=pos["tp"]))
                if sl_hit or tp_hit:
                    await self.force_close(pid,"SL" if sl_hit else "TP")
            await asyncio.sleep(2.0)


# ============================================================================
# 12. WEB
# ============================================================================
app=Flask(__name__)

@app.route("/")
def index():
    return render_template_string("""<!DOCTYPE html>
<html lang="fa" dir="rtl"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Quant v17.0</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui;background:#0d1117;color:#c9d1d9;padding:14px;direction:rtl}
h1{color:#58a6ff;font-size:1.3rem;margin-bottom:3px}
.sub{color:#8b949e;font-size:.75rem;margin-bottom:12px}
.bar{background:#161b22;border:1px solid #30363d;border-radius:7px;
  padding:7px 12px;display:flex;gap:12px;flex-wrap:wrap;font-size:.78rem;margin-bottom:9px}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-left:4px}
.g{background:#3fb950}.r{background:#f85149}.y{background:#d29922}
.alert{background:#1a0f0f;border:1px solid #f85149;border-radius:7px;
  padding:7px;margin-bottom:7px;color:#f85149;font-size:.78rem}
.oa{background:#1a1200;border:1px solid #d29922;border-radius:7px;
  padding:7px;margin-bottom:7px;color:#d29922;font-size:.78rem}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));
  gap:8px;margin:9px 0}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:11px}
.lbl{font-size:.7rem;color:#8b949e;margin-bottom:3px}
.val{font-size:1.15rem;font-weight:700;color:#58a6ff}
.gn{color:#3fb950}.rd{color:#f85149}.yw{color:#d29922}
h3{color:#8b949e;font-size:.8rem;margin:12px 0 5px;text-transform:uppercase}
table{width:100%;border-collapse:collapse;font-size:.78rem}
th{background:#21262d;padding:5px 7px;text-align:right;color:#8b949e}
td{padding:5px 7px;border-bottom:1px solid #21262d}
.b{display:inline-block;padding:1px 5px;border-radius:6px;font-size:.68rem;font-weight:700}
.bb{background:#0d2818;color:#3fb950}.bs{background:#2d1317;color:#f85149}
.bo{background:#1a1200;color:#d29922}
</style></head><body>
<h1>🚀 Master Quant v17.0</h1>
<p class="sub">Phemex Native | Orphan Manager | Multi-Method OHLCV</p>
<div class="bar">
  <span><span class="dot" id="d1"></span><span id="st">—</span></span>
  <span>📡 <span id="ps">—</span></span>
  <span>OHLCV: <span id="om">—</span></span>
  <span>🕐 <span id="sc">—</span> #<span id="sn">0</span></span>
  <span>✅ <span id="syms">0</span> نماد</span>
</div>
<div id="al"></div><div id="oa"></div>
<div class="grid">
  <div class="card"><div class="lbl">موجودی</div><div class="val" id="bal">—</div></div>
  <div class="card"><div class="lbl">آزاد</div><div class="val" id="fre">—</div></div>
  <div class="card"><div class="lbl">پوزیشن</div><div class="val" id="pos">0/10</div></div>
  <div class="card"><div class="lbl">👻 یتیم</div><div class="val yw" id="orp">0</div></div>
  <div class="card"><div class="lbl">Net PnL</div><div class="val" id="pnl">—</div></div>
  <div class="card"><div class="lbl">Win Rate</div><div class="val" id="wr">—</div></div>
  <div class="card"><div class="lbl">DD</div><div class="val rd" id="dd">—</div></div>
  <div class="card"><div class="lbl">PF</div><div class="val" id="pf">—</div></div>
</div>
<h3>📦 همه پوزیشن‌ها</h3>
<table><thead><tr><th>نماد</th><th>نوع</th><th>جهت</th>
<th>استراتژی</th><th>ورود</th><th>SL</th><th>TP</th><th>Qty</th></tr></thead>
<tbody id="ptb"><tr><td colspan="8" style="text-align:center;color:#8b949e">—</td></tr></tbody>
</table>
<script>
async function r(){
  try{
    const d=await(await fetch('/api/status')).json();
    const a=d.is_active&&!d.dd_halted&&!d.daily_halted;
    document.getElementById('d1').className='dot '+(a?'g':d.dd_halted?'r':'y');
    document.getElementById('st').textContent=d.dd_halted?'DD Halt':d.daily_halted?'Daily Halt':d.is_active?'فعال':'متوقف';
    document.getElementById('ps').textContent=d.phemex_status||'?';
    document.getElementById('om').textContent=d.ohlcv_method||'?';
    document.getElementById('sc').textContent=d.last_scan||'—';
    document.getElementById('sn').textContent=d.scan_count||0;
    document.getElementById('syms').textContent=(d.valid_symbols||[]).length;
    document.getElementById('bal').textContent='$'+(d.balance||0).toFixed(2);
    document.getElementById('fre').textContent='$'+(d.free_balance||0).toFixed(2);
    const reg=Object.values(d.active_positions||{});
    const orp=Object.values(d.orphan_positions||{});
    document.getElementById('pos').textContent=reg.length+'/10';
    const oe=document.getElementById('orp');
    oe.textContent=orp.length; oe.className='val '+(orp.length?'yw':'gn');
    const s=d.stats||{};const np=s.net_pnl||0;
    const pe=document.getElementById('pnl');
    pe.textContent='$'+np.toFixed(2);pe.className='val '+(np>=0?'gn':'rd');
    document.getElementById('wr').textContent=(s.win_rate||0).toFixed(1)+'%';
    document.getElementById('dd').textContent=(d.current_dd||0).toFixed(2)+'%';
    document.getElementById('pf').textContent=(s.profit_factor||0).toFixed(2);
    const cds=d.symbol_cooldowns||{},now=Date.now()/1000;
    const ac=Object.entries(cds).filter(([,v])=>v>now);
    document.getElementById('al').innerHTML=ac.length?
      '<div class="alert">🔴 CB: '+ac.map(([s,v])=>s.split('/')[0]+'('+Math.round((v-now)/60)+'m)').join(' | ')+'</div>':'';
    document.getElementById('oa').innerHTML=orp.length?
      '<div class="oa">👻 '+orp.length+' یتیم: '+orp.map(p=>p.symbol.split('/')[0]+' '+p.side.toUpperCase()).join(' | ')+'</div>':'';
    const all=[...reg.map(p=>({...p,_orp:false})),...orp.map(p=>({...p,_orp:true}))];
    const tb=document.getElementById('ptb');
    tb.innerHTML=all.length?all.map(p=>`<tr>
      <td>${p.symbol.split('/')[0]}</td>
      <td>${p._orp?'<span class="b bo">👻 ORPHAN</span>':'<span class="b bb">NORMAL</span>'}</td>
      <td><span class="b ${p.side=='buy'?'bb':'bs'}">${p.side.toUpperCase()}</span></td>
      <td style="font-size:.7rem">${p.strategy||'?'}</td>
      <td>${(p.entry||0).toFixed(4)}</td>
      <td style="color:#f85149">${(p.sl||0).toFixed(4)}</td>
      <td style="color:#3fb950">${(p.tp||0).toFixed(4)}</td>
      <td>${p.qty||0}</td></tr>`).join('')
    :'<tr><td colspan="8" style="text-align:center;color:#8b949e">هیچ پوزیشنی نیست</td></tr>';
  }catch(e){console.error(e)}
}
r();setInterval(r,4000);
</script></body></html>""")

@app.route("/api/status")
def api_status():
    with STATE_LOCK: return jsonify(dict(SHARED_STATE))

def run_web():
    app.run(host="0.0.0.0",port=10000,debug=False,use_reloader=False)

# ============================================================================
# 13. MAIN
# ============================================================================
if __name__=="__main__":
    Thread(target=run_web,daemon=True).start()
    log.info("🌐 http://0.0.0.0:10000")
    engine=QuantEngine()
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        log.info("👋 Shutdown")
    except Exception as e:
        log.error(f"💥 {e}"); raise
