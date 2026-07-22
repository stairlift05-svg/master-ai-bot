#!/usr/bin/env python3
# -*- coding: utf-8 -*- 
"""
Master-AI Quant Bot v4.6 - FINAL FIXED VERSION
نسخه,نهایی با رفع کامل مشکل داده
"""

import json
import logging
import os
import sys
import threading
import time
import uuid
import concurrent.futures
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import ccxt
import numpy as np
import pandas as pd
import requests
from flask import Flask

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("MasterQuant_v4.6")


# ============================================================================
# CONFIGURATION
# ============================================================================
class Cfg:
    @staticmethod
    def s(k: str, d: str = "") -> str:
        return os.getenv(k, d).strip()

    @staticmethod
    def f(k: str, d: float) -> float:
        try:
            return float(os.getenv(k, str(d)).strip())
        except Exception:
            return d

    @staticmethod
    def i(k: str, d: int) -> int:
        try:
            return int(os.getenv(k, str(d)).strip())
        except Exception:
            return d

    @staticmethod
    def b(k: str, d: bool = False) -> bool:
        return os.getenv(k, "true" if d else "false").strip().lower() in (
            "1", "true", "yes", "on",
        )


API_KEY = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT = Cfg.s("TELEGRAM_CHAT_ID")

SYMBOLS = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
    "BNB/USDT:USDT",
    "DOGE/USDT:USDT",
    "ADA/USDT:USDT",
    "AVAX/USDT:USDT",
    "DOT/USDT:USDT",
    "LINK/USDT:USDT",
]

# 🔥 تنظیمات جدید
RISK_PCT = Cfg.f("RISK_PER_TRADE", 0.5)
MAX_DD = Cfg.f("MAX_DRAWDOWN", 15.0)
MAX_POS = Cfg.i("MAX_POSITIONS", 2)
LEVERAGE = Cfg.i("LEVERAGE", 5)
TESTNET = Cfg.b("PHEMEX_TESTNET", True)
PORT = Cfg.i("PORT", 10000)
SCAN_INTERVAL = Cfg.i("SCAN_INTERVAL", 90)  # 🔥 افزایش به ۹۰ ثانیه
MIN_CONFIDENCE = Cfg.i("MIN_CONFIDENCE", 75)
SCAN_BATCH_SIZE = Cfg.i("SCAN_BATCH_SIZE", 1)  # 🔥 کاهش به ۱
REQUEST_TIMEOUT = Cfg.i("REQUEST_TIMEOUT", 45)  # 🔥 افزایش به ۴۵

# contract_size
CONTRACT_SIZE_MAP = {
    "BTC": 0.001,
    "ETH": 0.01,
    "SOL": 0.1,
    "XRP": 1.0,
    "BNB": 0.01,
    "DOGE": 10.0,
    "ADA": 1.0,
    "AVAX": 0.1,
    "DOT": 0.1,
    "LINK": 0.1,
}

MIN_SL_PCT = 0.05


# ============================================================================
# TECHNICAL INDICATORS
# ============================================================================
class Indicators:

    @staticmethod
    def rsi(close: pd.Series, n: int = 14) -> pd.Series:
        delta = close.diff()
        up = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        rs = up.ewm(com=n - 1, adjust=False).mean() / (
            down.ewm(com=n - 1, adjust=False).mean() + 1e-10
        )
        return 100 - (100 / (1 + rs))

    @staticmethod
    def ema(close: pd.Series, n: int) -> pd.Series:
        return close.ewm(span=n, adjust=False).mean()

    @staticmethod
    def atr(high: pd.Series, low: pd.Series,
            close: pd.Series, n: int = 14) -> pd.Series:
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ], axis=1).max(axis=1)
        return tr.ewm(com=n - 1, adjust=False).mean()

    @staticmethod
    def macd(close: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9):
        e_fast = close.ewm(span=fast, adjust=False).mean()
        e_slow = close.ewm(span=slow, adjust=False).mean()
        line = e_fast - e_slow
        signal = line.ewm(span=sig, adjust=False).mean()
        hist = line - signal
        return line, signal, hist

    @staticmethod
    def adx(high: pd.Series, low: pd.Series,
            close: pd.Series, n: int = 14) -> pd.Series:
        up = high.diff()
        down = -low.diff()
        plus_dm = np.where((up > down) & (up > 0), up, 0.0)
        minus_dm = np.where((down > up) & (down > 0), down, 0.0)
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ], axis=1).max(axis=1)
        atr_val = tr.ewm(com=n - 1, adjust=False).mean()
        plus_di = 100 * (
            pd.Series(plus_dm).ewm(com=n - 1, adjust=False).mean()
            / (atr_val + 1e-10)
        )
        minus_di = 100 * (
            pd.Series(minus_dm).ewm(com=n - 1, adjust=False).mean()
            / (atr_val + 1e-10)
        )
        dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10))
        return dx.ewm(com=n - 1, adjust=False).mean()

    @staticmethod
    def bbands(close: pd.Series, n: int = 20, std: float = 2.0):
        mid = close.rolling(n).mean()
        sd = close.rolling(n).std()
        return mid - std * sd, mid, mid + std * sd

    @staticmethod
    def safe(s, idx: int = -1) -> float:
        try:
            if s is None:
                return 0.0
            v = s.iloc[idx]
            return float(v) if not (v != v) else 0.0
        except Exception:
            return 0.0

    @staticmethod
    def trend_strength(close: pd.Series, fast: int = 10, slow: int = 30) -> float:
        ema_fast = Indicators.ema(close, fast)
        ema_slow = Indicators.ema(close, slow)
        ratio = (ema_fast / ema_slow - 1) * 100
        return float(ratio.iloc[-1]) if len(ratio) > 0 else 0.0


IND = Indicators()


# ============================================================================
# DATABASE
# ============================================================================
class DB:
    _SCHEMA = [
        """CREATE TABLE IF NOT EXISTS trades (
            id              TEXT PRIMARY KEY,
            symbol          TEXT NOT NULL,
            side            TEXT NOT NULL,
            entry_price     REAL NOT NULL,
            fill_price      REAL,
            exit_price      REAL,
            quantity         REAL NOT NULL,
            filled_quantity  REAL DEFAULT 0,
            stop_loss       REAL NOT NULL,
            take_profit     REAL NOT NULL,
            status          TEXT DEFAULT 'open',
            strategy        TEXT,
            confidence      INTEGER DEFAULT 0,
            pnl             REAL DEFAULT 0,
            pnl_pct         REAL DEFAULT 0,
            is_partial      INTEGER DEFAULT 0,
            exit_reason     TEXT,
            exchange_order_id TEXT,
            sl_order_id     TEXT,
            contracts       INTEGER DEFAULT 0,
            opened_at       TEXT DEFAULT CURRENT_TIMESTAMP,
            closed_at       TEXT,
            is_real         INTEGER DEFAULT 1
        )"""
    ]

    def __init__(self):
        self._lock = threading.Lock()
        self._path = "bot_v4.db"
        self._boot()

    def _boot(self):
        import sqlite3
        with self._lock:
            c = sqlite3.connect(self._path)
            for s in self._SCHEMA:
                c.execute(s)
            c.commit()
            c.close()

    def _cx(self):
        import sqlite3
        return sqlite3.connect(self._path, timeout=15)

    def run(self, sql: str, p: tuple = ()) -> Optional[List]:
        try:
            with self._lock:
                c = self._cx()
                cur = c.cursor()
                cur.execute(sql, p)
                c.commit()
                if sql.strip().upper().startswith("SELECT"):
                    res = cur.fetchall()
                    c.close()
                    return res
                c.close()
        except Exception as e:
            log.error("DB Error: %s", e)
        return None

    def open_trades(self) -> List[Dict]:
        rows = self.run(
            "SELECT id,symbol,side,entry_price,fill_price,quantity,"
            "filled_quantity,stop_loss,take_profit,strategy,confidence,"
            "is_partial,exchange_order_id,sl_order_id,contracts "
            "FROM trades WHERE status='open'"
        )
        if not rows:
            return []
        keys = [
            "id", "symbol", "side", "entry", "fill_price", "qty",
            "filled_qty", "sl", "tp", "strategy", "conf",
            "is_partial", "exchange_order_id", "sl_order_id", "contracts",
        ]
        return [dict(zip(keys, r)) for r in rows]

    def insert(self, t: Dict):
        self.run(
            "INSERT OR IGNORE INTO trades "
            "(id,symbol,side,entry_price,fill_price,quantity,filled_quantity,"
            "stop_loss,take_profit,strategy,confidence,exchange_order_id,"
            "sl_order_id,contracts,is_real) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                t["id"], t["symbol"], t["side"], t["entry"],
                t.get("fill_price", t["entry"]),
                t["qty"], t.get("filled_qty", t["qty"]),
                t["sl"], t["tp"], t["strategy"], t["conf"],
                t.get("exchange_order_id", ""),
                t.get("sl_order_id", ""),
                t.get("contracts", 0),
                1,
            ),
        )

    def update_partial(self, tid: str, new_qty: float, new_sl: float):
        self.run(
            "UPDATE trades SET quantity=?, stop_loss=?, is_partial=1 WHERE id=?",
            (new_qty, new_sl, tid),
        )

    def update_sl_order(self, tid: str, sl_order_id: str):
        self.run(
            "UPDATE trades SET sl_order_id=? WHERE id=?",
            (sl_order_id or "", tid),
        )

    def close(self, tid: str, ep: float, pnl: float,
              pct: float, reason: str):
        self.run(
            "UPDATE trades SET status='closed',exit_price=?,pnl=?,"
            "pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?",
            (ep, pnl, pct, reason, tid),
        )

    def get_analytics(self) -> Dict:
        rows = self.run(
            "SELECT pnl, pnl_pct FROM trades WHERE status='closed' AND is_real=1"
        )
        if not rows:
            return {
                "total_trades": 0, "win_rate": 0.0,
                "total_pnl": 0.0, "profit_factor": 0.0,
                "wins_count": 0, "losses_count": 0,
                "avg_win": 0.0, "avg_loss": 0.0,
                "largest_win": 0.0, "largest_loss": 0.0,
            }
        pnls = [r[0] for r in rows]
        wins = [p for p in pnls if p > 0]
        losses = [abs(p) for p in pnls if p < 0]
        total = len(pnls)
        return {
            "total_trades": total,
            "wins_count": len(wins),
            "losses_count": len(losses),
            "win_rate": round(len(wins) / total * 100, 1) if total else 0.0,
            "total_pnl": round(sum(pnls), 2),
            "profit_factor": round(
                sum(wins) / sum(losses), 2
            ) if sum(losses) > 0 else round(sum(wins), 2),
            "avg_win": round(sum(wins) / len(wins), 2) if wins else 0.0,
            "avg_loss": round(sum(losses) / len(losses), 2) if losses else 0.0,
            "largest_win": round(max(wins), 2) if wins else 0.0,
            "largest_loss": round(max(losses), 2) if losses else 0.0,
        }


database = DB()


# ============================================================================
# EXCHANGE ENGINE - با کش کردن داده
# ============================================================================
class Exchange:

    def __init__(self):
        self._ex: Optional[ccxt.phemex] = None
        self._binance: Optional[ccxt.binance] = None
        self._markets_info: Dict = {}
        self._connected = False
        self._data_cache: Dict = {}  # 🔥 کش داده
        self._cache_time: Dict = {}  # 🔥 زمان کش
        self._connect()

    def _connect(self):
        if not API_KEY or not API_SECRET:
            log.error("❌ کليدهاي API تنظيم نشده!")
            return
        try:
            self._ex = ccxt.phemex({
                "apiKey": API_KEY,
                "secret": API_SECRET,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
                "timeout": REQUEST_TIMEOUT * 1000,
            })
            if TESTNET:
                self._ex.set_sandbox_mode(True)
                log.warning("⚠️  حالت TESTNET فعال است!")

            self._ex.load_markets()
            self._cache_market_info()
            self._set_leverage_all()
            self._connected = True

            try:
                self._binance = ccxt.binance({
                    "enableRateLimit": True,
                    "options": {"defaultType": "swap"},
                    "timeout": REQUEST_TIMEOUT * 1000,
                })
                log.info("✅ اتصال به Binance (fallback) برقرار شد")
            except Exception as e:
                log.warning(f"⚠️ Binance fallback: {e}")

            mode = "TESTNET" if TESTNET else "MAINNET"
            log.info("✅ اتصال به Phemex %s برقرار شد.", mode)
        except Exception as e:
            log.error("❌ خطاي اتصال: %s", e)

    def _cache_market_info(self):
        if not self._ex:
            return
        for sym in SYMBOLS:
            if sym in self._ex.markets:
                mkt = self._ex.markets[sym]
                symbol_base = sym.split("/")[0]
                contract_size = CONTRACT_SIZE_MAP.get(symbol_base, 0.001)
                
                self._markets_info[sym] = {
                    "min_amount": mkt.get("limits", {}).get("amount", {}).get("min", 0.001),
                    "min_cost": mkt.get("limits", {}).get("cost", {}).get("min", 0.5),
                    "precision_amount": mkt.get("precision", {}).get("amount", 0.001),
                    "precision_price": mkt.get("precision", {}).get("price", 0.01),
                    "contract_size": contract_size,
                }

    def _set_leverage_all(self):
        if not self._ex:
            return
        for sym in SYMBOLS:
            try:
                self._ex.set_leverage(LEVERAGE, sym)
            except Exception as e:
                log.warning("⚠️  لوريج %s: %s", sym, e)

    @property
    def is_connected(self) -> bool:
        return self._connected and self._ex is not None

    def get_contract_size(self, sym: str) -> float:
        symbol_base = sym.split("/")[0]
        return CONTRACT_SIZE_MAP.get(symbol_base, 0.001)

    def fetch_ohlcv_safe(self, sym: str, tf: str = "5m",
                         limit: int = 60, max_retries: int = 3) -> Optional[pd.DataFrame]:
        if not self.is_connected:
            return None

        for attempt in range(max_retries):
            try:
                raw = self._ex.fetch_ohlcv(sym, tf, limit=limit)
                if raw and len(raw) >= 25:
                    df = pd.DataFrame(
                        raw, columns=["ts", "open", "high", "low", "close", "vol"]
                    )
                    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
                    if not df["close"].isna().any():
                        return df

                if attempt == max_retries - 1:
                    return self._fetch_from_binance(sym, tf, limit)

                time.sleep(1 * (attempt + 1))

            except ccxt.RateLimitExceeded:
                log.warning(f"⚠️ Rate Limit {sym} {tf}, waiting...")
                time.sleep(3 * (attempt + 1))
            except ccxt.NetworkError:
                log.warning(f"⚠️ Network Error {sym} {tf}, retrying...")
                time.sleep(2 * (attempt + 1))
            except Exception as e:
                if attempt == max_retries - 1:
                    log.error(f"❌ OHLCV Error [{sym} {tf}]: {e}")
                    return self._fetch_from_binance(sym, tf, limit)
                time.sleep(1 * (attempt + 1))

        return None

    def _fetch_from_binance(self, sym: str, tf: str, limit: int) -> Optional[pd.DataFrame]:
        if not self._binance:
            return None
        try:
            binance_sym = sym.replace("/USDT:USDT", "/USDT")
            raw = self._binance.fetch_ohlcv(binance_sym, tf, limit=limit)
            if not raw or len(raw) < 25:
                return None
            df = pd.DataFrame(
                raw, columns=["ts", "open", "high", "low", "close", "vol"]
            )
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            log.info(f"✅ {sym} از Binance دریافت شد")
            return df
        except Exception as e:
            log.debug(f"Binance fallback failed: {e}")
            return None

    def fetch_multi_ohlcv(self, sym: str) -> Dict[str, pd.DataFrame]:
        """🔥 با اولویت تایم‌فریم بزرگتر"""
        result = {}
        
        # 🔥 اولویت با تایم‌فریم بزرگتر
        timeframes = ["15m", "5m", "1m"]
        
        for tf in timeframes:
            df = self.fetch_ohlcv_safe(sym, tf, limit=60, max_retries=2)
            
            if df is not None and len(df) >= 25:
                result[tf] = df
                time.sleep(0.5)  # 🔥 فاصله بیشتر
            else:
                # 🔥 اگر ۱۵ دقیقه failed، کل عملیات را متوقف کن
                if tf == "15m":
                    log.warning(f"⚠️ {sym}: داده ۱۵ دقیقه دريافت نشد")
                    return {}
                # 🔥 برای بقیه، سعی کن از داده قبلی استفاده کنی
                if result:
                    log.warning(f"⚠️ {sym}: {tf} دريافت نشد، استفاده از داده موجود")
                    continue
                return {}
        
        # 🔥 اگر ۱ دقیقه نبود، از ۵ دقیقه کپی کن
        if "1m" not in result and "5m" in result:
            log.warning(f"⚠️ {sym}: استفاده از ۵ دقیقه به جای ۱ دقیقه")
            result["1m"] = result["5m"].copy()
        
        # 🔥 اگر ۵ دقیقه نبود، از ۱۵ دقیقه کپی کن
        if "5m" not in result and "15m" in result:
            log.warning(f"⚠️ {sym}: استفاده از ۱۵ دقیقه به جای ۵ دقیقه")
            result["5m"] = result["15m"].copy()
        
        return result

    def fetch_multi_ohlcv_cached(self, sym: str) -> Dict[str, pd.DataFrame]:
        """🔥 با کش کردن داده"""
        now = time.time()
        cache_duration = 120  # 🔥 ۲ دقیقه کش
        
        if sym in self._data_cache and (now - self._cache_time.get(sym, 0)) < cache_duration:
            log.info(f"📦 {sym}: استفاده از داده کش شده")
            return self._data_cache[sym]
        
        data = self.fetch_multi_ohlcv(sym)
        if data:
            self._data_cache[sym] = data
            self._cache_time[sym] = now
        
        return data

    def get_current_price(self, sym: str) -> Optional[float]:
        if not self.is_connected:
            return None
        try:
            ticker = self._ex.fetch_ticker(sym)
            return float(ticker.get("last", 0))
        except Exception:
            try:
                if self._binance:
                    binance_sym = sym.replace("/USDT:USDT", "/USDT")
                    ticker = self._binance.fetch_ticker(binance_sym)
                    return float(ticker.get("last", 0))
            except Exception:
                pass
            return None

    def fetch_real_positions(self) -> List[Dict]:
        if not self.is_connected:
            return []
        try:
            positions = self._ex.fetch_positions()
            active = []
            for p in positions:
                contracts = float(p.get("contracts", 0) or 0)
                if contracts > 0:
                    active.append({
                        "symbol": p.get("symbol"),
                        "side": p.get("side", "long"),
                        "qty": contracts,
                        "entry": float(p.get("entryPrice", 0) or 0),
                        "unrealized_pnl": float(p.get("unrealizedPnl", 0) or 0),
                        "liquidation": float(p.get("liquidationPrice", 0) or 0),
                    })
            return active
        except Exception as e:
            log.error("Fetch Positions Error: %s", e)
            return []

    def balance(self) -> float:
        if not self.is_connected:
            return 0.0
        try:
            b = self._ex.fetch_balance()
            return float(b.get("USDT", {}).get("free", 0.0))
        except Exception:
            return 0.0

    def total_equity(self) -> float:
        if not self.is_connected:
            return 0.0
        try:
            b = self._ex.fetch_balance()
            return float(b.get("USDT", {}).get("total", 0.0))
        except Exception:
            return 0.0

    def place_order(self, sym: str, side: str, qty: float,
                    is_close: bool = False) -> Optional[Dict]:
        if not self.is_connected:
            log.error("❌ صرافي متصل نيست!")
            return None

        try:
            current_price = self.get_current_price(sym)
            if not current_price:
                log.error("❌ قيمت دريافت نشد: %s", sym)
                return None

            contract_size = self.get_contract_size(sym)
            contracts = qty / contract_size
            contracts = int(round(contracts))
            
            if contracts < 1:
                contracts = 1
                qty = contracts * contract_size

            log.info(
                f"📤 ارسال سفارش | {side.upper()} {sym} | "
                f"{contracts} قرارداد = {qty:.6f} سکه"
            )

            params = {}
            if is_close:
                params["reduceOnly"] = True

            if side.lower() == "buy":
                result = self._ex.create_market_buy_order(sym, contracts, params=params)
            else:
                result = self._ex.create_market_sell_order(sym, contracts, params=params)

            fill_price = float(result.get("average") or result.get("price") or current_price)
            filled_contracts = float(result.get("filled") or result.get("amount") or contracts)
            filled_qty = filled_contracts * contract_size

            return {
                "id": result.get("id"),
                "fill_price": fill_price,
                "filled_qty": filled_qty,
                "filled_contracts": filled_contracts,
                "status": result.get("status"),
            }

        except ccxt.InsufficientFunds:
            log.error("❌ موجودي کافي نيست [%s %s]", side, sym)
            return None
        except ccxt.InvalidOrder as e:
            log.error("❌ سفارش نامعتبر [%s %s]: %s", side, sym, e)
            return None
        except Exception as e:
            log.error("❌ خطاي سفارش [%s %s]: %s", side, sym, e)
            return None

    def place_stop_loss(self, sym: str, pos_side: str,
                        qty: float, stop_price: float) -> Optional[str]:
        if not self.is_connected:
            return None
        try:
            contract_size = self.get_contract_size(sym)
            contracts = int(round(qty / contract_size))
            if contracts < 1:
                contracts = 1
            
            sl_side = "sell" if pos_side == "long" else "buy"
            fmt_price = float(self._ex.price_to_precision(sym, stop_price))

            params = {
                "stopPrice": fmt_price,
                "reduceOnly": True,
                "triggerType": "ByLastPrice",
            }

            result = self._ex.create_order(
                sym, "market", sl_side, contracts, None, params=params,
            )

            return result.get("id")
        except Exception as e:
            log.warning(f"⚠️ خطا در SL [%s]: %s", sym, e)
            return None

    def cancel_order_safe(self, sym: str, order_id: str):
        if not self.is_connected or not order_id:
            return
        try:
            self._ex.cancel_order(order_id, sym)
        except Exception as e:
            log.debug("Cancel order [%s]: %s", order_id, e)


EX = Exchange()


# ============================================================================
# STRATEGY ENGINE (خلاصه شده برای اختصار)
# ============================================================================
@dataclass
class Signal:
    action: str = "neutral"
    strategy: str = ""
    confidence: int = 0
    reason: str = ""
    sl: float = 0.0
    tp: float = 0.0
    entry_estimate: float = 0.0
    debug_info: str = ""


class StrategyEngine:
    # ... (همان استراتژی‌های قبلی با MIN_SL_PCT = 0.05)
    pass


STRATEGY = StrategyEngine()


# ============================================================================
# TELEGRAM HANDLER (خلاصه شده)
# ============================================================================
class TelegramHandler:
    # ... (همان کد قبلی)
    pass


# ============================================================================
# CORE ENGINE - با استفاده از کش
# ============================================================================
class Engine:

    def __init__(self):
        self._pos: Dict[str, Dict] = {}
        self._lock = threading.RLock()
        self.is_active = True
        self.is_dd_halted = False
        self.current_dd = 0.0
        self.peak_balance = None
        self.tg: Optional[TelegramHandler] = None
        self._cycle_count = 0
        self._sync_on_boot()

    def _sync_on_boot(self):
        equity = EX.total_equity()
        self.peak_balance = equity if equity > 0 else None
        for t in database.open_trades():
            self._pos[t["id"]] = t

        real_positions = EX.fetch_real_positions()
        for rp in real_positions:
            already = any(p["symbol"] == rp["symbol"] for p in self._pos.values())
            if not already:
                pid = f"sync_{uuid.uuid4().hex[:6]}"
                entry = rp["entry"]
                contract_size = EX.get_contract_size(rp["symbol"])
                pos = {
                    "id": pid,
                    "symbol": rp["symbol"],
                    "side": rp["side"],
                    "entry": entry,
                    "fill_price": entry,
                    "qty": rp["qty"] * contract_size,
                    "filled_qty": rp["qty"] * contract_size,
                    "sl": entry * 0.95 if rp["side"] == "long" else entry * 1.05,
                    "tp": entry * 1.075 if rp["side"] == "long" else entry * 0.925,
                    "strategy": "Synced",
                    "conf": 100,
                    "is_partial": 0,
                    "exchange_order_id": "",
                    "sl_order_id": "",
                    "contracts": int(rp["qty"]),
                }
                self._pos[pid] = pos
                database.insert(pos)

    def run_loop(self):
        log.info("🚀 موتور v4.6 شروع شد - با کش داده")
        while True:
            try:
                self._cycle_count += 1
                if not EX.is_connected:
                    log.warning("⚠️ صرافي متصل نيست...")
                    time.sleep(30)
                    continue

                equity = EX.total_equity()
                if equity > 0:
                    self._check_drawdown(equity)

                self._manage_positions()

                if self._cycle_count % 20 == 0:
                    self._check_sync()

                # 🔥 فقط هر ۲ سیکل یکبار اسکن کن
                if self.is_active and not self.is_dd_halted and self._cycle_count % 2 == 0:
                    with self._lock:
                        pos_count = len(self._pos)
                    if pos_count < MAX_POS:
                        self._scan_markets(equity)

                time.sleep(SCAN_INTERVAL)
            except Exception as e:
                log.error("❌ Engine Error: %s", e)
                time.sleep(SCAN_INTERVAL)

    def _check_drawdown(self, equity: float):
        if self.peak_balance is None or equity > self.peak_balance:
            self.peak_balance = equity
        if self.peak_balance and self.peak_balance > 0:
            self.current_dd = ((self.peak_balance - equity) / self.peak_balance * 100)
            if self.current_dd >= MAX_DD and not self.is_dd_halted:
                self.is_dd_halted = True
                log.critical("🛑 DRAWDOWN! DD=%.1f%%", self.current_dd)
                if self.tg:
                    self.tg.send(f"🛑 هشدار افت! {self.current_dd:.1f}%")
            elif self.current_dd < MAX_DD * 0.7 and self.is_dd_halted:
                self.is_dd_halted = False
                log.info("✅ افت بهبود يافت")

    def _check_sync(self):
        real = EX.fetch_real_positions()
        real_syms = {p["symbol"] for p in real}
        with self._lock:
            db_syms = {p["symbol"] for p in self._pos.values()}
        orphans = db_syms - real_syms
        for pid, pos in list(self._pos.items()):
            if pos["symbol"] in orphans:
                price = EX.get_current_price(pos["symbol"]) or pos["entry"]
                self._close_position(pid, pos, price, "Sync_Orphan")

    def _scan_markets(self, balance: float):
        """🔥 استفاده از کش داده"""
        with self._lock:
            active_syms = [p["symbol"] for p in self._pos.values()]
        
        symbols_to_scan = [s for s in SYMBOLS if s not in active_syms]
        max_scan = min(len(symbols_to_scan), SCAN_BATCH_SIZE)
        
        for sym in symbols_to_scan[:max_scan]:
            try:
                with self._lock:
                    if len(self._pos) >= MAX_POS:
                        return
                
                short_name = sym.split("/")[0]
                log.info(f"📊 اسکن {short_name}...")
                
                # 🔥 استفاده از کش
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(EX.fetch_multi_ohlcv_cached, sym)
                    try:
                        dfs = future.result(timeout=REQUEST_TIMEOUT)
                    except concurrent.futures.TimeoutError:
                        log.warning(f"⏰ Timeout {short_name}")
                        continue
                
                if not dfs:
                    log.warning(f"⚠️ داده {short_name} دريافت نشد")
                    continue
                
                signal = STRATEGY.analyze(sym, dfs)
                
                if signal.action == "neutral":
                    log.debug(f"[{short_name}] {signal.debug_info[:50]}")
                    continue
                
                if signal.confidence < MIN_CONFIDENCE:
                    log.info(f"[{short_name}] اطمينان {signal.confidence}% < {MIN_CONFIDENCE}%")
                    continue
                
                log.info(f"✅ [{short_name}] سيگنال: {signal.action.upper()} ({signal.strategy})")
                self._execute_signal(sym, signal, balance)
                time.sleep(2)
                
            except Exception as e:
                log.error(f"[{sym}] Scan Error: {e}")

    def _execute_signal(self, sym: str, sig: Signal, balance: float):
        short_name = sym.split("/")[0]
        
        sl_dist = sig.entry_estimate * MIN_SL_PCT
        risk_amount = balance * (RISK_PCT / 100.0) * 0.3
        qty = risk_amount / sl_dist
        
        max_notional = balance * 0.05
        if (qty * sig.entry_estimate) > max_notional:
            qty = max_notional / sig.entry_estimate
        
        contract_size = EX.get_contract_size(sym)
        contracts = qty / contract_size
        
        if contracts < 1:
            contracts = 1
        contracts = int(round(contracts))
        qty = contracts * contract_size
        
        log.info(
            f"[{short_name}] 📊 {contracts} قرارداد = {qty:.6f} سکه | SL: 5%"
        )
        
        side = "buy" if sig.action == "buy" else "sell"
        order_result = EX.place_order(sym, side, qty, is_close=False)
        
        if not order_result:
            log.warning(f"❌ [{short_name}] سفارش اجرا نشد")
            return
        
        fill_price = order_result["fill_price"]
        filled_qty = order_result["filled_qty"]
        
        pos_side = "long" if sig.action == "buy" else "short"
        
        if pos_side == "long":
            real_sl = fill_price * 0.95
            real_tp = fill_price * 1.075
        else:
            real_sl = fill_price * 1.05
            real_tp = fill_price * 0.925
        
        sl_order_id = EX.place_stop_loss(sym, pos_side, filled_qty, real_sl)
        
        pid = f"p_{uuid.uuid4().hex[:8]}"
        pos = {
            "id": pid,
            "symbol": sym,
            "side": pos_side,
            "entry": fill_price,
            "fill_price": fill_price,
            "qty": filled_qty,
            "filled_qty": filled_qty,
            "sl": real_sl,
            "tp": real_tp,
            "strategy": sig.strategy,
            "conf": sig.confidence,
            "is_partial": 0,
            "exchange_order_id": order_result["id"] or "",
            "sl_order_id": sl_order_id or "",
            "contracts": contracts,
        }
        
        with self._lock:
            self._pos[pid] = pos
        database.insert(pos)
        
        log.info(
            f"✅ [{short_name}] پوزيشن باز | ورود: {fill_price:.4f} | SL: 5% | {contracts} قرارداد"
        )

    def _manage_positions(self):
        with self._lock:
            snap = dict(self._pos)
        for pid, pos in snap.items():
            try:
                price = EX.get_current_price(pos["symbol"])
                if not price:
                    continue
                side = pos["side"]
                sl_hit = ((side == "long" and price <= pos["sl"]) or (side == "short" and price >= pos["sl"]))
                if sl_hit:
                    log.info(f"🛑 [{pos['symbol']}] SL خورد!")
                    self._close_position(pid, pos, price, "StopLoss")
                    continue
                if not pos.get("is_partial", 0):
                    tp_hit = ((side == "long" and price >= pos["tp"]) or (side == "short" and price <= pos["tp"]))
                    if tp_hit:
                        log.info(f"🎯 [{pos['symbol']}] TP خورد!")
                        self._close_position(pid, pos, price, "TakeProfit")
            except Exception as e:
                log.error(f"Manage Error: {e}")

    def _close_position(self, pid: str, pos: Dict, price: float, reason: str):
        close_side = "sell" if pos["side"] == "long" else "buy"
        result = EX.place_order(pos["symbol"], close_side, pos["qty"], is_close=True)
        actual_price = result["fill_price"] if result else price
        if pos.get("sl_order_id"):
            EX.cancel_order_safe(pos["symbol"], pos["sl_order_id"])
        entry = pos.get("fill_price", pos["entry"])
        pnl = ((actual_price - entry) * pos["qty"] if pos["side"] == "long" else (entry - actual_price) * pos["qty"])
        pct = ((actual_price - entry) / entry * 100 if pos["side"] == "long" else (entry - actual_price) / entry * 100)
        database.close(pid, actual_price, pnl, pct, reason)
        with self._lock:
            self._pos.pop(pid, None)
        emoji = "✅" if pnl >= 0 else "❌"
        log.info(f"{emoji} [{pos['symbol']}] بسته شد | PnL: {pnl:+.2f}$ ({pct:+.2f}%)")
        if self.tg:
            self.tg.send(f"{emoji} <b>بسته شد ({reason})</b>\n{pos['symbol']}\nPnL: {pnl:+.2f}$ ({pct:+.2f}%)")


# ============================================================================
# WEB SERVER
# ============================================================================
app = Flask(__name__)
engine_instance: Optional[Engine] = None


@app.route("/")
def home():
    stats = database.get_analytics()
    bal = EX.balance()
    equity = EX.total_equity()
    pos_count = len(engine_instance._pos) if engine_instance else 0
    active = engine_instance.is_active if engine_instance else False
    connected = EX.is_connected
    mode = "TESTNET" if TESTNET else "MAINNET"
    dd = engine_instance.current_dd if engine_instance else 0

    return f"""
    <!DOCTYPE html>
    <html dir="rtl" lang="fa">
    <head>
        <meta charset="UTF-8">
        <title>Quant Bot v4.6</title>
        <meta http-equiv="refresh" content="30">
        <style>
            body {{ font-family: Tahoma, sans-serif; background: #0d1117; color: #c9d1d9; padding: 20px; text-align: center; }}
            .card {{ background: #161b22; border: 1px solid #30363d; padding: 12px; margin: 6px; border-radius: 8px; display: inline-block; min-width: 130px; }}
            .warn {{ background: #3d1f00; border-color: #f0883e; color: #f0883e; }}
            .ok {{ border-color: #3fb950; }}
            h1 {{ color: #58a6ff; }}
            .badge {{ background: #238636; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; }}
        </style>
    </head>
    <body>
        <h1>🤖 Master-AI Quant Bot v4.6</h1>
        <span class="badge">✅ نهایی - با کش داده</span>
        <div class="status">
            وضعيت: <b>{'▶️ فعال' if active else '⏹ متوقف'}</b> |
            اتصال: <b>{'✅' if connected else '❌'}</b> |
            شبکه: <b>{mode}</b> |
            پوزيشن: <b>{pos_count}/{MAX_POS}</b>
        </div>
        <div class="card"><h3>💰 موجودي</h3><p>${bal:,.2f}</p></div>
        <div class="card"><h3>💎 ارزش کل</h3><p>${equity:,.2f}</p></div>
        <div class="card {'ok' if stats['total_pnl'] >= 0 else 'warn'}">
            <h3>📈 PnL</h3><p>{stats['total_pnl']:+,.2f}$</p>
        </div>
        <div class="card"><h3>🎯 WR</h3><p>{stats['win_rate']}%</p></div>
        <div class="card"><h3>🛡️ DD</h3><p>{dd:.1f}%</p></div>
        <br><br>
        <div class="card" style="min-width: 200px;">
            <h3>🔧 تنظیمات v4.6</h3>
            <p>ریسک: {RISK_PCT}% | Min SL: 5%</p>
            <p>Max Pos: {MAX_POS} | Scan: {SCAN_INTERVAL}s</p>
            <p style="color: #3fb950;">✅ کش داده فعال</p>
            <p style="color: #3fb950;">✅ Fallback به Binance</p>
        </div>
    </body>
    </html>
    """


@app.route("/health")
def health():
    return {
        "status": "ok",
        "version": "4.6",
        "connected": EX.is_connected,
        "testnet": TESTNET,
        "active": engine_instance.is_active if engine_instance else False,
        "positions": len(engine_instance._pos) if engine_instance else 0,
    }


@app.route("/debug")
def api_debug():
    results = {}
    for sym in SYMBOLS:
        short = sym.split("/")[0]
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(EX.fetch_multi_ohlcv_cached, sym)
                dfs = future.result(timeout=REQUEST_TIMEOUT)
            if not dfs:
                results[short] = {"error": "no data"}
                continue
            sig = STRATEGY.analyze(sym, dfs)
            results[short] = {
                "action": sig.action,
                "strategy": sig.strategy,
                "confidence": sig.confidence,
                "debug": sig.debug_info,
            }
        except Exception as e:
            results[short] = {"error": str(e)[:50]}
    return results


# ============================================================================
# MAIN
# ============================================================================
def main():
    global engine_instance

    log.info("=" * 60)
    log.info("  🤖 Master-AI Quant Bot v4.6 (FINAL)")
    log.info("  ✅ کش داده فعال")
    log.info("  ✅ Min SL: 5%")
    log.info("  ✅ Fallback به Binance")
    log.info("  🌐 Mode: %s", "TESTNET" if TESTNET else "MAINNET")
    log.info("  🔗 Connected: %s", EX.is_connected)
    log.info("  🎯 Risk: %.1f%% | Min SL: 5%%", RISK_PCT)
    log.info("  📊 Max Pos: %d | Scan: %ds", MAX_POS, SCAN_INTERVAL)
    log.info("=" * 60)

    if not EX.is_connected:
        log.critical("❌ اتصال به صرافي برقرار نشد!")

    engine_instance = Engine()
    tg = TelegramHandler(engine_instance)
    engine_instance.tg = tg

    if TG_TOKEN and TG_CHAT:
        tg.send(
            f"🚀 <b>ربات v4.6 (نهایی) شروع شد</b>\n"
            f"{'═' * 28}\n"
            f"✅ کش داده فعال\n"
            f"✅ Min SL: 5%\n"
            f"✅ Fallback به Binance\n"
            f"🛡️ Max DD: {MAX_DD}%\n"
            f"📊 Max Pos: {MAX_POS} | Scan: {SCAN_INTERVAL}s\n"
            f"🌐 {'🧪 TESTNET' if TESTNET else '💰 MAINNET'}\n"
            f"{'═' * 28}\n"
            f"✅ ربات آماده اسکن است",
            reply_markup=tg._keyboard(),
        )

    threading.Thread(target=engine_instance.run_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)


if __name__ == "__main__":
    main()