#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Quant Bot v5.0 - FULLY OPTIMIZED
نسخه کاملاً بهینه‌شده با اصلاح تمام مشکلات شناسایی‌شده
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
log = logging.getLogger("MasterQuant_v5.0")


# ============================================================================
# CONFIGURATION - اصلاح‌شده
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

# 🔧 اصلاح #10: ریسک واقعی‌تر
RISK_PCT = Cfg.f("RISK_PER_TRADE", 1.0)          # از 0.5 به 1.0
MAX_DD = Cfg.f("MAX_DRAWDOWN", 15.0)
# 🔧 اصلاح #11: ظرفیت بیشتر
MAX_POS = Cfg.i("MAX_POSITIONS", 4)               # از 2 به 4
LEVERAGE = Cfg.i("LEVERAGE", 5)
TESTNET = Cfg.b("PHEMEX_TESTNET", True)
PORT = Cfg.i("PORT", 10000)
# 🔧 اصلاح #9: اسکن سریع‌تر
SCAN_INTERVAL = Cfg.i("SCAN_INTERVAL", 45)        # از 90 به 45
# 🔧 اصلاح #1: حداقل اطمینان پایین‌تر
MIN_CONFIDENCE = Cfg.i("MIN_CONFIDENCE", 55)      # از 70 به 55
# 🔧 اصلاح #6: اسکن بیشتر در هر سیکل
SCAN_BATCH_SIZE = Cfg.i("SCAN_BATCH_SIZE", 5)     # از 1 به 5
REQUEST_TIMEOUT = Cfg.i("REQUEST_TIMEOUT", 45)

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

# 🔧 اصلاح SL/TP تطبیقی
ATR_MULTIPLIER_SL = 1.5
ATR_MULTIPLIER_TP = 3.0

# 🔧 اصلاح #5: تنظیمات Trailing Stop
TRAILING_ACTIVATE_PCT = 1.5   # فعال‌سازی بعد از 1.5% سود
TRAILING_STEP_PCT = 0.5       # فاصله trail

# 🔧 اصلاح #8: Partial Take Profit
PARTIAL_TP_ENABLED = True
PARTIAL_TP_RATIO = 0.5        # 50% در TP1
PARTIAL_TP1_MULTIPLIER = 2.0  # TP1 = 2*ATR


# ============================================================================
# TECHNICAL INDICATORS - بهبودیافته
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
    def sma(close: pd.Series, n: int) -> pd.Series:
        return close.rolling(window=n).mean()

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
            pd.Series(plus_dm, index=high.index).ewm(com=n - 1, adjust=False).mean()
            / (atr_val + 1e-10)
        )
        minus_di = 100 * (
            pd.Series(minus_dm, index=high.index).ewm(com=n - 1, adjust=False).mean()
            / (atr_val + 1e-10)
        )
        dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10))
        return dx.ewm(com=n - 1, adjust=False).mean()

    @staticmethod
    def macd(close: pd.Series, fast: int = 12, slow: int = 26,
             signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def bollinger_bands(close: pd.Series, n: int = 20,
                        std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        sma = close.rolling(window=n).mean()
        std = close.rolling(window=n).std()
        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)
        return upper, sma, lower

    @staticmethod
    def stochastic(high: pd.Series, low: pd.Series,
                   close: pd.Series, k_period: int = 14,
                   d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
        lowest = low.rolling(window=k_period).min()
        highest = high.rolling(window=k_period).max()
        k = 100 * (close - lowest) / (highest - lowest + 1e-10)
        d = k.rolling(window=d_period).mean()
        return k, d

    @staticmethod
    def volume_profile(vol: pd.Series, n: int = 20) -> Tuple[float, float]:
        """نسبت حجم فعلی به میانگین و روند حجم"""
        avg = vol.rolling(window=n).mean()
        current = vol.iloc[-1] if len(vol) > 0 else 0
        avg_val = avg.iloc[-1] if len(avg) > 0 else 1
        ratio = current / (avg_val + 1e-10)
        # روند حجم (آیا حجم در حال افزایش است)
        vol_trend = 0
        if len(vol) >= 5:
            recent_avg = vol.iloc[-5:].mean()
            older_avg = vol.iloc[-10:-5].mean() if len(vol) >= 10 else avg_val
            vol_trend = (recent_avg - older_avg) / (older_avg + 1e-10)
        return ratio, vol_trend

    @staticmethod
    def safe(s, idx: int = -1) -> float:
        try:
            if s is None:
                return 0.0
            v = s.iloc[idx]
            return float(v) if not (v != v) else 0.0
        except Exception:
            return 0.0


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
            is_real         INTEGER DEFAULT 1,
            trailing_active INTEGER DEFAULT 0,
            trailing_sl     REAL DEFAULT 0,
            highest_pnl_pct REAL DEFAULT 0
        )"""
    ]

    def __init__(self):
        self._lock = threading.Lock()
        self._path = "bot_v5.db"
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

    def update_sl(self, tid: str, new_sl: float):
        self.run(
            "UPDATE trades SET stop_loss=? WHERE id=?",
            (new_sl, tid),
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
# EXCHANGE ENGINE
# ============================================================================
class Exchange:

    def __init__(self):
        self._ex: Optional[ccxt.phemex] = None
        self._binance: Optional[ccxt.binance] = None
        self._markets_info: Dict = {}
        self._connected = False
        self._data_cache: Dict = {}
        self._cache_time: Dict = {}
        self._connect()

    def _connect(self):
        if not API_KEY or not API_SECRET:
            log.error("❌ کلیدهای API تنظیم نشده!")
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
                log.warning("⚠️ حالت TESTNET فعال است!")

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
            log.error("❌ خطای اتصال: %s", e)

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
                log.warning("⚠️ لوریج %s: %s", sym, e)

    @property
    def is_connected(self) -> bool:
        return self._connected and self._ex is not None

    def get_contract_size(self, sym: str) -> float:
        symbol_base = sym.split("/")[0]
        return CONTRACT_SIZE_MAP.get(symbol_base, 0.001)

    def fetch_ohlcv_safe(self, sym: str, tf: str = "5m",
                         limit: int = 100, max_retries: int = 3) -> Optional[pd.DataFrame]:
        if not self.is_connected:
            return None

        for attempt in range(max_retries):
            try:
                raw = self._ex.fetch_ohlcv(sym, tf, limit=limit)
                if raw and len(raw) >= 20:
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
            if not raw or len(raw) < 20:
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
        """🔧 اصلاح #2: دریافت تایم‌فریم‌های 5m, 15m, 1h"""
        result = {}
        timeframes = {
            "1h": 200,    # برای EMA200 نیاز به 200 کندل ساعتی
            "15m": 100,
            "5m": 60,
        }

        for tf, limit in timeframes.items():
            df = self.fetch_ohlcv_safe(sym, tf, limit=limit, max_retries=2)
            if df is not None and len(df) >= 20:
                result[tf] = df
                time.sleep(0.3)
            else:
                log.warning(f"⚠️ {sym}: داده {tf} دریافت نشد")
                # اگر 1h نداشتیم از 15m استفاده می‌کنیم
                if tf == "1h" and "15m" not in result:
                    continue

        # حداقل باید 5m یا 15m داشته باشیم
        if "5m" not in result and "15m" not in result:
            return {}

        # اگر 5m نداشتیم از 15m کپی کن
        if "5m" not in result and "15m" in result:
            result["5m"] = result["15m"].copy()

        return result

    def fetch_multi_ohlcv_cached(self, sym: str) -> Dict[str, pd.DataFrame]:
        now = time.time()
        # 🔧 اصلاح #12: کش کوتاه‌تر
        cache_duration = 60  # از 120 به 60

        if sym in self._data_cache and (now - self._cache_time.get(sym, 0)) < cache_duration:
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
            log.error("❌ صرافی متصل نیست!")
            return None
        try:
            current_price = self.get_current_price(sym)
            if not current_price:
                log.error("❌ قیمت دریافت نشد: %s", sym)
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
            log.error("❌ موجودی کافی نیست [%s %s]", side, sym)
            return None
        except ccxt.InvalidOrder as e:
            log.error("❌ سفارش نامعتبر [%s %s]: %s", side, sym, e)
            return None
        except Exception as e:
            log.error("❌ خطای سفارش [%s %s]: %s", side, sym, e)
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
            log.warning(f"⚠️ خطا در SL [{sym}]: {e}")
            return None

    def cancel_order_safe(self, sym: str, order_id: str):
        if not self.is_connected or not order_id:
            return
        try:
            self._ex.cancel_order(order_id, sym)
        except Exception as e:
            log.debug("Cancel order [%s]: %s", order_id, e)

    def update_stop_loss(self, sym: str, pos_side: str,
                         qty: float, old_sl_id: str,
                         new_sl_price: float) -> Optional[str]:
        """به‌روزرسانی حد ضرر (کنسل قبلی + ثبت جدید)"""
        self.cancel_order_safe(sym, old_sl_id)
        return self.place_stop_loss(sym, pos_side, qty, new_sl_price)


EX = Exchange()


# ============================================================================
# STRATEGY ENGINE - کاملاً بازنویسی‌شده با ۵ استراتژی
# ============================================================================
@dataclass
class Signal:
    action: str = "neutral"
    strategy: str = ""
    confidence: int = 0
    reason: str = ""
    sl: float = 0.0
    tp: float = 0.0
    tp1: float = 0.0          # 🔧 Partial TP
    entry_estimate: float = 0.0
    debug_info: str = ""
    atr_value: float = 0.0    # برای Trailing Stop


class StrategyEngine:

    def analyze(self, sym: str, dfs: Dict[str, pd.DataFrame]) -> Signal:
        """
        🔧 اصلاح اصلی: ۵ استراتژی مستقل که هر کدام
        به‌تنهایی می‌توانند سیگنال بدهند
        """
        signals = []

        # استراتژی ۱: Trend Breakout
        sig = self._strategy_breakout(sym, dfs)
        if sig.action != "neutral":
            signals.append(sig)

        # استراتژی ۲: Pullback به EMA
        sig = self._strategy_pullback(sym, dfs)
        if sig.action != "neutral":
            signals.append(sig)

        # استراتژی ۳: RSI Divergence + Trend
        sig = self._strategy_rsi_trend(sym, dfs)
        if sig.action != "neutral":
            signals.append(sig)

        # استراتژی ۴: MACD Cross + ADX
        sig = self._strategy_macd_adx(sym, dfs)
        if sig.action != "neutral":
            signals.append(sig)

        # استراتژی ۵: Bollinger Squeeze Breakout
        sig = self._strategy_bollinger(sym, dfs)
        if sig.action != "neutral":
            signals.append(sig)

        # انتخاب بهترین سیگنال
        if not signals:
            return Signal(debug_info="هیچ استراتژی سیگنال نداد")

        # مرتب‌سازی بر اساس اطمینان
        signals.sort(key=lambda s: s.confidence, reverse=True)
        best = signals[0]

        # اگر چند استراتژی هم‌جهت هستند، اطمینان را افزایش بده
        same_dir = [s for s in signals if s.action == best.action]
        if len(same_dir) >= 2:
            best.confidence = min(95, best.confidence + 10)
            best.reason += f" | {len(same_dir)} استراتژی هم‌جهت"

        return best

    def _get_trend_context(self, dfs: Dict[str, pd.DataFrame]) -> Dict:
        """تشخیص روند از تایم‌فریم بالاتر"""
        context = {
            "trend": "neutral",
            "strength": 0,
            "adx": 0,
        }

        # 🔧 اصلاح #2: استفاده از 1h برای روند اگر موجود باشد
        if "1h" in dfs and len(dfs["1h"]) >= 50:
            df = dfs["1h"]
        elif "15m" in dfs and len(dfs["15m"]) >= 50:
            df = dfs["15m"]
        else:
            return context

        close = df["close"]
        high = df["high"]
        low = df["low"]

        ema20 = IND.ema(close, 20)
        ema50 = IND.ema(close, 50)
        adx = IND.adx(high, low, close, 14)

        price = IND.safe(close)
        ema20_val = IND.safe(ema20)
        ema50_val = IND.safe(ema50)
        adx_val = IND.safe(adx)

        context["adx"] = adx_val

        # 🔧 اصلاح #2: حذف EMA200 از شرط اجباری (نیاز به ۲۰۰ کندل)
        # به جایش از EMA20 > EMA50 استفاده می‌کنیم
        if price > ema20_val > ema50_val:
            context["trend"] = "up"
            context["strength"] = min(100, adx_val)
        elif price < ema20_val < ema50_val:
            context["trend"] = "down"
            context["strength"] = min(100, adx_val)
        elif price > ema50_val:
            context["trend"] = "weak_up"
            context["strength"] = min(50, adx_val)
        elif price < ema50_val:
            context["trend"] = "weak_down"
            context["strength"] = min(50, adx_val)

        return context

    def _get_atr_levels(self, df: pd.DataFrame,
                        price: float, side: str) -> Tuple[float, float, float]:
        """محاسبه SL, TP, TP1 بر اساس ATR"""
        atr = IND.atr(df["high"], df["low"], df["close"], 14)
        atr_val = IND.safe(atr)

        if atr_val <= 0:
            atr_val = price * 0.01  # fallback 1%

        if side == "buy":
            sl = price - (ATR_MULTIPLIER_SL * atr_val)
            tp = price + (ATR_MULTIPLIER_TP * atr_val)
            tp1 = price + (PARTIAL_TP1_MULTIPLIER * atr_val)
        else:
            sl = price + (ATR_MULTIPLIER_SL * atr_val)
            tp = price - (ATR_MULTIPLIER_TP * atr_val)
            tp1 = price - (PARTIAL_TP1_MULTIPLIER * atr_val)

        return sl, tp, tp1

    # ================================================================
    # استراتژی ۱: Breakout (اصلاح‌شده)
    # ================================================================
    def _strategy_breakout(self, sym: str,
                           dfs: Dict[str, pd.DataFrame]) -> Signal:
        if "5m" not in dfs:
            return Signal()

        df5 = dfs["5m"]
        if len(df5) < 25:
            return Signal()

        ctx = self._get_trend_context(dfs)
        close = df5["close"]
        high = df5["high"]
        low = df5["low"]
        vol = df5["vol"]

        price = IND.safe(close)
        # 🔧 اصلاح #3: شکست ۱۰ کندلی به جای ۲۰
        high_10 = IND.safe(high.rolling(10).max(), -2)  # سقف قبل از کندل فعلی
        low_10 = IND.safe(low.rolling(10).min(), -2)

        vol_ratio, vol_trend = IND.volume_profile(vol)

        if high_10 <= 0 or low_10 <= 0:
            return Signal()

        # شرط حجم ملایم‌تر
        # 🔧 اصلاح: حجم ۱.۲ برابر به جای ۱.۵ برابر
        vol_ok = vol_ratio > 1.2

        # Breakout صعودی
        if price > high_10 and vol_ok:
            if ctx["trend"] in ("up", "weak_up", "neutral"):
                conf = 65
                if ctx["trend"] == "up":
                    conf = 75
                if ctx["adx"] > 25:
                    conf += 5
                if vol_ratio > 2.0:
                    conf += 5

                sl, tp, tp1 = self._get_atr_levels(df5, price, "buy")
                atr_val = IND.safe(IND.atr(high, low, close, 14))

                return Signal(
                    action="buy",
                    strategy="Breakout",
                    confidence=min(90, conf),
                    reason=f"شکست سقف ۱۰ کندل | حجم {vol_ratio:.1f}x | روند: {ctx['trend']}",
                    sl=sl, tp=tp, tp1=tp1,
                    entry_estimate=price,
                    atr_value=atr_val,
                    debug_info=f"✅ Breakout BUY | Vol={vol_ratio:.1f}x ADX={ctx['adx']:.0f}"
                )

        # Breakout نزولی
        if price < low_10 and vol_ok:
            if ctx["trend"] in ("down", "weak_down", "neutral"):
                conf = 65
                if ctx["trend"] == "down":
                    conf = 75
                if ctx["adx"] > 25:
                    conf += 5
                if vol_ratio > 2.0:
                    conf += 5

                sl, tp, tp1 = self._get_atr_levels(df5, price, "sell")
                atr_val = IND.safe(IND.atr(high, low, close, 14))

                return Signal(
                    action="sell",
                    strategy="Breakout",
                    confidence=min(90, conf),
                    reason=f"شکست کف ۱۰ کندل | حجم {vol_ratio:.1f}x | روند: {ctx['trend']}",
                    sl=sl, tp=tp, tp1=tp1,
                    entry_estimate=price,
                    atr_value=atr_val,
                    debug_info=f"✅ Breakout SELL | Vol={vol_ratio:.1f}x ADX={ctx['adx']:.0f}"
                )

        return Signal(debug_info=f"Breakout: شرط ورود برقرار نیست")

    # ================================================================
    # استراتژی ۲: Pullback (اصلاح‌شده)
    # ================================================================
    def _strategy_pullback(self, sym: str,
                           dfs: Dict[str, pd.DataFrame]) -> Signal:
        if "15m" not in dfs:
            return Signal()

        df15 = dfs["15m"]
        if len(df15) < 30:
            return Signal()

        ctx = self._get_trend_context(dfs)
        close = df15["close"]
        high = df15["high"]
        low = df15["low"]

        price = IND.safe(close)
        ema20 = IND.ema(close, 20)
        ema20_val = IND.safe(ema20)
        rsi = IND.rsi(close, 14)
        rsi_val = IND.safe(rsi)

        if ema20_val <= 0:
            return Signal()

        # فاصله قیمت از EMA20
        dist_pct = (price - ema20_val) / ema20_val * 100

        # 🔧 اصلاح #4: رنج وسیع‌تر (۲٪ به جای ۱٪)
        # Pullback صعودی: قیمت نزدیک EMA20 و RSI بالای ۴۰
        if ctx["trend"] in ("up", "weak_up"):
            if -2.0 < dist_pct < 0.5 and rsi_val > 40 and rsi_val < 70:
                conf = 60
                if ctx["trend"] == "up":
                    conf = 70
                if ctx["adx"] > 20:
                    conf += 5
                if -1.0 < dist_pct < 0.2:
                    conf += 5  # نزدیک‌تر = بهتر

                sl, tp, tp1 = self._get_atr_levels(df15, price, "buy")
                atr_val = IND.safe(IND.atr(high, low, close, 14))

                return Signal(
                    action="buy",
                    strategy="Pullback",
                    confidence=min(85, conf),
                    reason=f"برگشت به EMA20 ({dist_pct:+.1f}%) | RSI={rsi_val:.0f}",
                    sl=sl, tp=tp, tp1=tp1,
                    entry_estimate=price,
                    atr_value=atr_val,
                    debug_info=f"✅ Pullback BUY | Dist={dist_pct:.1f}% RSI={rsi_val:.0f}"
                )

        # Pullback نزولی
        if ctx["trend"] in ("down", "weak_down"):
            if -0.5 < dist_pct < 2.0 and rsi_val < 60 and rsi_val > 30:
                conf = 60
                if ctx["trend"] == "down":
                    conf = 70
                if ctx["adx"] > 20:
                    conf += 5
                if -0.2 < dist_pct < 1.0:
                    conf += 5

                sl, tp, tp1 = self._get_atr_levels(df15, price, "sell")
                atr_val = IND.safe(IND.atr(high, low, close, 14))

                return Signal(
                    action="sell",
                    strategy="Pullback",
                    confidence=min(85, conf),
                    reason=f"برگشت به EMA20 ({dist_pct:+.1f}%) | RSI={rsi_val:.0f}",
                    sl=sl, tp=tp, tp1=tp1,
                    entry_estimate=price,
                    atr_value=atr_val,
                    debug_info=f"✅ Pullback SELL | Dist={dist_pct:.1f}% RSI={rsi_val:.0f}"
                )

        return Signal(debug_info=f"Pullback: روند={ctx['trend']} فاصله={dist_pct:.1f}%")

    # ================================================================
    # استراتژی ۳: RSI + Trend (جدید)
    # ================================================================
    def _strategy_rsi_trend(self, sym: str,
                            dfs: Dict[str, pd.DataFrame]) -> Signal:
        if "5m" not in dfs:
            return Signal()

        df5 = dfs["5m"]
        if len(df5) < 30:
            return Signal()

        ctx = self._get_trend_context(dfs)
        close = df5["close"]
        high = df5["high"]
        low = df5["low"]

        price = IND.safe(close)
        rsi = IND.rsi(close, 14)
        rsi_val = IND.safe(rsi)
        rsi_prev = IND.safe(rsi, -2)

        ema20 = IND.ema(close, 20)
        ema20_val = IND.safe(ema20)

        # RSI از منطقه اشباع فروش خارج می‌شود + روند صعودی
        if ctx["trend"] in ("up", "weak_up"):
            if rsi_prev < 35 and rsi_val > 35 and price > ema20_val:
                conf = 65
                if ctx["trend"] == "up":
                    conf = 72
                if rsi_prev < 30:
                    conf += 5

                sl, tp, tp1 = self._get_atr_levels(df5, price, "buy")
                atr_val = IND.safe(IND.atr(high, low, close, 14))

                return Signal(
                    action="buy",
                    strategy="RSI_Trend",
                    confidence=min(85, conf),
                    reason=f"RSI از اشباع فروش خارج شد ({rsi_prev:.0f}→{rsi_val:.0f})",
                    sl=sl, tp=tp, tp1=tp1,
                    entry_estimate=price,
                    atr_value=atr_val,
                    debug_info=f"✅ RSI_Trend BUY | RSI={rsi_val:.0f}"
                )

        # RSI از منطقه اشباع خرید خارج می‌شود + روند نزولی
        if ctx["trend"] in ("down", "weak_down"):
            if rsi_prev > 65 and rsi_val < 65 and price < ema20_val:
                conf = 65
                if ctx["trend"] == "down":
                    conf = 72
                if rsi_prev > 70:
                    conf += 5

                sl, tp, tp1 = self._get_atr_levels(df5, price, "sell")
                atr_val = IND.safe(IND.atr(high, low, close, 14))

                return Signal(
                    action="sell",
                    strategy="RSI_Trend",
                    confidence=min(85, conf),
                    reason=f"RSI از اشباع خرید خارج شد ({rsi_prev:.0f}→{rsi_val:.0f})",
                    sl=sl, tp=tp, tp1=tp1,
                    entry_estimate=price,
                    atr_value=atr_val,
                    debug_info=f"✅ RSI_Trend SELL | RSI={rsi_val:.0f}"
                )

        return Signal(debug_info=f"RSI_Trend: RSI={rsi_val:.0f} روند={ctx['trend']}")

    # ================================================================
    # استراتژی ۴: MACD + ADX (جدید)
    # ================================================================
    def _strategy_macd_adx(self, sym: str,
                           dfs: Dict[str, pd.DataFrame]) -> Signal:
        if "15m" not in dfs:
            return Signal()

        df15 = dfs["15m"]
        if len(df15) < 35:
            return Signal()

        close = df15["close"]
        high = df15["high"]
        low = df15["low"]

        price = IND.safe(close)
        macd_line, signal_line, histogram = IND.macd(close)
        adx = IND.adx(high, low, close, 14)

        macd_val = IND.safe(macd_line)
        macd_prev = IND.safe(macd_line, -2)
        signal_val = IND.safe(signal_line)
        signal_prev = IND.safe(signal_line, -2)
        hist_val = IND.safe(histogram)
        hist_prev = IND.safe(histogram, -2)
        adx_val = IND.safe(adx)

        # MACD Cross Up + ADX بالا
        if macd_prev < signal_prev and macd_val > signal_val and adx_val > 20:
            conf = 62
            if adx_val > 30:
                conf = 70
            if hist_val > hist_prev:  # هیستوگرام در حال رشد
                conf += 5

            sl, tp, tp1 = self._get_atr_levels(df15, price, "buy")
            atr_val = IND.safe(IND.atr(high, low, close, 14))

            return Signal(
                action="buy",
                strategy="MACD_ADX",
                confidence=min(85, conf),
                reason=f"MACD Cross Up | ADX={adx_val:.0f}",
                sl=sl, tp=tp, tp1=tp1,
                entry_estimate=price,
                atr_value=atr_val,
                debug_info=f"✅ MACD_ADX BUY | ADX={adx_val:.0f}"
            )

        # MACD Cross Down + ADX بالا
        if macd_prev > signal_prev and macd_val < signal_val and adx_val > 20:
            conf = 62
            if adx_val > 30:
                conf = 70
            if hist_val < hist_prev:
                conf += 5

            sl, tp, tp1 = self._get_atr_levels(df15, price, "sell")
            atr_val = IND.safe(IND.atr(high, low, close, 14))

            return Signal(
                action="sell",
                strategy="MACD_ADX",
                confidence=min(85, conf),
                reason=f"MACD Cross Down | ADX={adx_val:.0f}",
                sl=sl, tp=tp, tp1=tp1,
                entry_estimate=price,
                atr_value=atr_val,
                debug_info=f"✅ MACD_ADX SELL | ADX={adx_val:.0f}"
            )

        return Signal(debug_info=f"MACD_ADX: ADX={adx_val:.0f}")

    # ================================================================
    # استراتژی ۵: Bollinger Squeeze Breakout (جدید)
    # ================================================================
    def _strategy_bollinger(self, sym: str,
                            dfs: Dict[str, pd.DataFrame]) -> Signal:
        if "5m" not in dfs:
            return Signal()

        df5 = dfs["5m"]
        if len(df5) < 25:
            return Signal()

        ctx = self._get_trend_context(dfs)
        close = df5["close"]
        high = df5["high"]
        low = df5["low"]
        vol = df5["vol"]

        price = IND.safe(close)
        upper, mid, lower = IND.bollinger_bands(close, 20, 2.0)

        upper_val = IND.safe(upper)
        lower_val = IND.safe(lower)
        mid_val = IND.safe(mid)

        if upper_val <= 0 or lower_val <= 0 or mid_val <= 0:
            return Signal()

        # عرض باند (squeeze detection)
        band_width = (upper_val - lower_val) / mid_val * 100

        # باند عرض‌های ۲۰ کندل اخیر
        band_widths = ((upper - lower) / mid * 100).dropna()
        if len(band_widths) < 10:
            return Signal()

        avg_width = band_widths.iloc[-20:].mean() if len(band_widths) >= 20 else band_widths.mean()

        # Squeeze: باند فعلی < 80% میانگین
        is_squeeze = band_width < avg_width * 0.8

        vol_ratio, _ = IND.volume_profile(vol)

        # Breakout از باند بالایی بعد از Squeeze
        if price > upper_val and (is_squeeze or vol_ratio > 1.3):
            if ctx["trend"] not in ("down",):  # ضد روند نباشد
                conf = 60
                if is_squeeze:
                    conf += 10
                if vol_ratio > 1.5:
                    conf += 5
                if ctx["trend"] in ("up", "weak_up"):
                    conf += 5

                sl, tp, tp1 = self._get_atr_levels(df5, price, "buy")
                atr_val = IND.safe(IND.atr(high, low, close, 14))

                return Signal(
                    action="buy",
                    strategy="BB_Squeeze",
                    confidence=min(85, conf),
                    reason=f"شکست باند بالا | Squeeze={is_squeeze} | BW={band_width:.1f}%",
                    sl=sl, tp=tp, tp1=tp1,
                    entry_estimate=price,
                    atr_value=atr_val,
                    debug_info=f"✅ BB_Squeeze BUY | BW={band_width:.1f}%"
                )

        # Breakout از باند پایینی
        if price < lower_val and (is_squeeze or vol_ratio > 1.3):
            if ctx["trend"] not in ("up",):
                conf = 60
                if is_squeeze:
                    conf += 10
                if vol_ratio > 1.5:
                    conf += 5
                if ctx["trend"] in ("down", "weak_down"):
                    conf += 5

                sl, tp, tp1 = self._get_atr_levels(df5, price, "sell")
                atr_val = IND.safe(IND.atr(high, low, close, 14))

                return Signal(
                    action="sell",
                    strategy="BB_Squeeze",
                    confidence=min(85, conf),
                    reason=f"شکست باند پایین | Squeeze={is_squeeze} | BW={band_width:.1f}%",
                    sl=sl, tp=tp, tp1=tp1,
                    entry_estimate=price,
                    atr_value=atr_val,
                    debug_info=f"✅ BB_Squeeze SELL | BW={band_width:.1f}%"
                )

        return Signal(debug_info=f"BB: BW={band_width:.1f}% Squeeze={is_squeeze}")


STRATEGY = StrategyEngine()


# ============================================================================
# TELEGRAM HANDLER
# ============================================================================
class TelegramHandler:

    def __init__(self, engine_ref):
        self.engine = engine_ref
        self.last_update_id = 0
        if TG_TOKEN and TG_CHAT:
            threading.Thread(target=self._poll_loop, daemon=True).start()
            log.info("🤖 تلگرام متصل شد.")

    def send(self, msg: str, reply_markup=None):
        if not TG_TOKEN or not TG_CHAT:
            return
        try:
            data = {
                "chat_id": TG_CHAT,
                "text": msg,
                "parse_mode": "HTML",
            }
            if reply_markup:
                data["reply_markup"] = json.dumps(reply_markup)
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data=data, timeout=10,
            )
        except Exception as e:
            log.warning("TG Error: %s", e)

    def _keyboard(self):
        return {
            "keyboard": [
                [{"text": "📊 داشبورد"}, {"text": "📈 پوزیشن‌ها"}],
                [{"text": "📜 تاریخچه"}, {"text": "⚙️ وضعیت"}],
                [{"text": "▶️ شروع"}, {"text": "⏹ توقف"}],
                [{"text": "🔍 دیباگ اسکن"}],
            ],
            "resize_keyboard": True,
        }

    def _poll_loop(self):
        while True:
            try:
                url = (
                    f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
                    f"?offset={self.last_update_id + 1}&timeout=10"
                )
                res = requests.get(url, timeout=15).json()
                if res.get("ok"):
                    for upd in res.get("result", []):
                        self.last_update_id = upd["update_id"]
                        txt = upd.get("message", {}).get("text", "").strip()
                        if txt:
                            self._handle(txt)
            except Exception:
                pass
            time.sleep(2)

    def _handle(self, cmd: str):
        kb = self._keyboard()
        if cmd in ("/start", "▶️ شروع"):
            self.engine.is_active = True
            self.send("▶️ <b>ربات فعال شد!</b>", reply_markup=kb)
        elif cmd in ("/stop", "⏹ توقف"):
            self.engine.is_active = False
            self.send("⏹ <b>ربات متوقف شد</b>", reply_markup=kb)
        elif cmd in ("/dashboard", "📊 داشبورد"):
            self._send_dashboard()
        elif cmd in ("/positions", "📈 پوزیشن‌ها"):
            self._send_positions()
        elif cmd in ("/history", "📜 تاریخچه"):
            self._send_history()
        elif cmd in ("/status", "⚙️ وضعیت"):
            self._send_status()
        elif cmd in ("/debug", "🔍 دیباگ اسکن"):
            self._send_debug_scan()

    def _send_dashboard(self):
        stats = database.get_analytics()
        bal = EX.balance()
        equity = EX.total_equity()
        db_count = len(self.engine._pos)
        status = "▶️ فعال" if self.engine.is_active else "⏹ متوقف"
        mode = "🧪 TESTNET" if TESTNET else "💰 MAINNET"

        msg = (
            f"📊 <b>داشبورد ربات v5.0</b>\n"
            f"{'═' * 28}\n"
            f"⚡ وضعیت: {status}\n"
            f"🌐 شبکه: {mode}\n"
            f"🔗 اتصال: {'✅' if EX.is_connected else '❌'}\n"
            f"{'═' * 28}\n"
            f"💰 موجودی: ${bal:,.2f}\n"
            f"💎 ارزش کل: ${equity:,.2f}\n"
            f"📈 PnL: {stats['total_pnl']:+,.2f}$\n"
            f"{'═' * 28}\n"
            f"📊 پوزیشن: {db_count}/{MAX_POS}\n"
            f"🎯 Win Rate: {stats['win_rate']}%\n"
            f"🏆 معاملات: {stats['total_trades']}\n"
            f"🛡️ DD: {self.engine.current_dd:.1f}%\n"
            f"{'═' * 28}\n"
            f"🧠 ۵ استراتژی فعال\n"
            f"📐 Trailing Stop + Partial TP\n"
            f"⏱️ اسکن: {SCAN_INTERVAL}s | Batch: {SCAN_BATCH_SIZE}\n"
            f"🔧 نسخه: v5.0"
        )
        self.send(msg, reply_markup=self._keyboard())

    def _send_positions(self):
        real_pos = EX.fetch_real_positions()
        db_pos = list(self.engine._pos.values())
        if not real_pos and not db_pos:
            self.send("📭 <b>هیچ پوزیشنی نیست</b>", reply_markup=self._keyboard())
            return
        msg = "🏦 <b>پوزیشن‌ها:</b>\n"
        if real_pos:
            for p in real_pos:
                msg += (
                    f"\n📌 {p['symbol']} ({p['side'].upper()}) | "
                    f"ورود: {p['entry']:.4f} | "
                    f"PnL: {p['unrealized_pnl']:+.2f}$\n"
                )
        # نمایش وضعیت trailing
        for pid, pos in self.engine._pos.items():
            if pos.get("trailing_active"):
                msg += f"  📐 Trailing SL فعال: {pos['sl']:.4f}\n"
            if pos.get("is_partial"):
                msg += f"  ✂️ Partial TP انجام شده\n"
        self.send(msg, reply_markup=self._keyboard())

    def _send_history(self):
        stats = database.get_analytics()
        msg = (
            f"📜 <b>آمار معاملات</b>\n"
            f"{'═' * 28}\n"
            f"📊 کل: {stats['total_trades']}\n"
            f"✅ برد: {stats['wins_count']} | ❌ باخت: {stats['losses_count']}\n"
            f"🎯 Win Rate: {stats['win_rate']}%\n"
            f"💰 PnL: {stats['total_pnl']:+.2f}$\n"
            f"📈 PF: {stats['profit_factor']}\n"
            f"🏆 بهترین: +{stats['largest_win']:.2f}$\n"
            f"💔 بدترین: -{stats['largest_loss']:.2f}$\n"
        )
        self.send(msg, reply_markup=self._keyboard())

    def _send_status(self):
        connected = EX.is_connected
        mode = "TESTNET" if TESTNET else "MAINNET"
        bal = EX.balance() if connected else 0
        msg = (
            f"⚙️ <b>وضعیت v5.0</b>\n"
            f"{'═' * 28}\n"
            f"🔗 صرافی: {'✅' if connected else '❌'}\n"
            f"🌐 شبکه: {mode}\n"
            f"💰 موجودی: ${bal:,.2f}\n"
            f"🎯 ریسک: {RISK_PCT}% | SL: {ATR_MULTIPLIER_SL}*ATR\n"
            f"🎯 TP: {ATR_MULTIPLIER_TP}*ATR | TP1: {PARTIAL_TP1_MULTIPLIER}*ATR\n"
            f"📊 Max Pos: {MAX_POS} | Scan: {SCAN_INTERVAL}s\n"
            f"📐 Trailing: فعال ({TRAILING_ACTIVATE_PCT}%)\n"
            f"✂️ Partial TP: {'فعال' if PARTIAL_TP_ENABLED else 'غیرفعال'}\n"
            f"🧠 استراتژی‌ها: Breakout, Pullback, RSI, MACD, BB\n"
            f"🔍 Min Conf: {MIN_CONFIDENCE}%\n"
            f"📦 Batch: {SCAN_BATCH_SIZE}"
        )
        self.send(msg, reply_markup=self._keyboard())

    def _send_debug_scan(self):
        if not EX.is_connected:
            self.send("❌ صرافی متصل نیست", reply_markup=self._keyboard())
            return

        msg = "🔍 <b>دیباگ اسکن v5.0:</b>\n"
        bal = EX.balance()
        msg += f"💰 موجودی: ${bal:,.2f}\n"
        msg += f"📊 پوزیشن: {len(self.engine._pos)}/{MAX_POS}\n"
        msg += f"🧠 ۵ استراتژی فعال\n\n"

        active_syms = [p["symbol"] for p in self.engine._pos.values()]

        for sym in SYMBOLS:
            short_name = sym.split("/")[0]
            if sym in active_syms:
                msg += f"📌 <b>{short_name}</b>: پوزیشن باز\n"
                continue
            if len(self.engine._pos) >= MAX_POS:
                msg += f"⛔ <b>{short_name}</b>: ظرفیت پر\n"
                continue

            try:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(EX.fetch_multi_ohlcv_cached, sym)
                    dfs = future.result(timeout=REQUEST_TIMEOUT)

                if not dfs:
                    msg += f"❌ <b>{short_name}</b>: داده دریافت نشد\n"
                    continue

                sig = STRATEGY.analyze(sym, dfs)
                if sig.action == "neutral":
                    msg += f"⏸️ <b>{short_name}</b>: {sig.debug_info[:60]}\n"
                else:
                    sl_pct = abs(sig.sl - sig.entry_estimate) / sig.entry_estimate * 100
                    tp_pct = abs(sig.tp - sig.entry_estimate) / sig.entry_estimate * 100
                    msg += (
                        f"✅ <b>{short_name}</b>: {sig.action.upper()} "
                        f"({sig.strategy}) C={sig.confidence}% "
                        f"SL={sl_pct:.1f}% TP={tp_pct:.1f}%\n"
                    )
            except concurrent.futures.TimeoutError:
                msg += f"⏰ <b>{short_name}</b>: Timeout\n"
            except Exception as e:
                msg += f"❌ <b>{short_name}</b>: {str(e)[:30]}\n"

        self.send(msg, reply_markup=self._keyboard())


# ============================================================================
# CORE ENGINE - بازنویسی با Trailing Stop + Partial TP
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
        self._last_signal_time: Dict[str, float] = {}  # جلوگیری از سیگنال تکراری
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
                    "tp1": entry * 1.05 if rp["side"] == "long" else entry * 0.95,
                    "strategy": "Synced",
                    "conf": 100,
                    "is_partial": 0,
                    "exchange_order_id": "",
                    "sl_order_id": "",
                    "contracts": int(rp["qty"]),
                    "trailing_active": False,
                    "atr_value": entry * 0.01,
                    "highest_pnl_pct": 0,
                }
                self._pos[pid] = pos
                database.insert(pos)

    def run_loop(self):
        log.info("🚀 موتور v5.0 شروع شد - ۵ استراتژی فعال")
        while True:
            try:
                self._cycle_count += 1
                if not EX.is_connected:
                    log.warning("⚠️ صرافی متصل نیست...")
                    time.sleep(30)
                    continue

                equity = EX.total_equity()
                if equity > 0:
                    self._check_drawdown(equity)

                # 🔧 اصلاح #9: مدیریت پوزیشن‌ها هر سیکل
                self._manage_positions()

                if self._cycle_count % 20 == 0:
                    self._check_sync()

                # 🔧 اصلاح #7: اسکن هر سیکل (نه فقط زوج‌ها)
                if self.is_active and not self.is_dd_halted:
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
            self.current_dd = (self.peak_balance - equity) / self.peak_balance * 100
            if self.current_dd >= MAX_DD and not self.is_dd_halted:
                self.is_dd_halted = True
                log.critical("🛑 DRAWDOWN! DD=%.1f%%", self.current_dd)
                if self.tg:
                    self.tg.send(f"🛑 هشدار افت! {self.current_dd:.1f}%")
            elif self.current_dd < MAX_DD * 0.7 and self.is_dd_halted:
                self.is_dd_halted = False
                log.info("✅ افت بهبود یافت")

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
        with self._lock:
            active_syms = [p["symbol"] for p in self._pos.values()]

        symbols_to_scan = [s for s in SYMBOLS if s not in active_syms]

        # 🔧 اصلاح #6: اسکن بیشتر
        max_scan = min(len(symbols_to_scan), SCAN_BATCH_SIZE)

        for sym in symbols_to_scan[:max_scan]:
            try:
                with self._lock:
                    if len(self._pos) >= MAX_POS:
                        return

                # جلوگیری از سیگنال تکراری در بازه کوتاه
                now = time.time()
                last_sig = self._last_signal_time.get(sym, 0)
                if now - last_sig < 300:  # حداقل ۵ دقیقه بین سیگنال‌ها
                    continue

                short_name = sym.split("/")[0]
                log.info(f"📊 اسکن {short_name}...")

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(EX.fetch_multi_ohlcv_cached, sym)
                    try:
                        dfs = future.result(timeout=REQUEST_TIMEOUT)
                    except concurrent.futures.TimeoutError:
                        log.warning(f"⏰ Timeout {short_name}")
                        continue

                if not dfs:
                    log.warning(f"⚠️ داده {short_name} دریافت نشد")
                    continue

                signal = STRATEGY.analyze(sym, dfs)

                if signal.action == "neutral":
                    log.debug(f"[{short_name}] {signal.debug_info[:60]}")
                    continue

                if signal.confidence < MIN_CONFIDENCE:
                    log.info(
                        f"[{short_name}] اطمینان {signal.confidence}% < {MIN_CONFIDENCE}%"
                    )
                    continue

                log.info(
                    f"✅ [{short_name}] سیگنال: {signal.action.upper()} "
                    f"({signal.strategy}) Conf={signal.confidence}%"
                )
                self._execute_signal(sym, signal, balance)
                self._last_signal_time[sym] = now
                time.sleep(1)

            except Exception as e:
                log.error(f"[{sym}] Scan Error: {e}")

    def _execute_signal(self, sym: str, sig: Signal, balance: float):
        short_name = sym.split("/")[0]

        # حجم بر اساس حد ضرر پویا (ATR)
        sl_dist = abs(sig.entry_estimate - sig.sl)
        if sl_dist <= 0:
            log.warning(f"[{short_name}] حد ضرر نامعتبر")
            return

        # 🔧 اصلاح #10: ریسک واقعی‌تر
        risk_amount = balance * (RISK_PCT / 100.0)
        qty = risk_amount / sl_dist

        max_notional = balance * 0.10  # از 5% به 10%
        if (qty * sig.entry_estimate) > max_notional:
            qty = max_notional / sig.entry_estimate

        contract_size = EX.get_contract_size(sym)
        contracts = qty / contract_size

        if contracts < 1:
            contracts = 1
        contracts = int(round(contracts))
        qty = contracts * contract_size

        log.info(
            f"[{short_name}] 📊 {contracts} قرارداد = {qty:.6f} سکه | "
            f"SL: {sl_dist / sig.entry_estimate * 100:.1f}% | "
            f"TP: {abs(sig.tp - sig.entry_estimate) / sig.entry_estimate * 100:.1f}%"
        )

        side = "buy" if sig.action == "buy" else "sell"
        order_result = EX.place_order(sym, side, qty, is_close=False)

        if not order_result:
            log.warning(f"❌ [{short_name}] سفارش اجرا نشد")
            return

        fill_price = order_result["fill_price"]
        filled_qty = order_result["filled_qty"]

        # محاسبه SL و TP بر اساس Fill Price
        sl_ratio = abs(sig.entry_estimate - sig.sl) / sig.entry_estimate
        tp_ratio = abs(sig.entry_estimate - sig.tp) / sig.entry_estimate
        tp1_ratio = abs(sig.entry_estimate - sig.tp1) / sig.entry_estimate if sig.tp1 else tp_ratio * 0.5

        pos_side = "long" if sig.action == "buy" else "short"

        if pos_side == "long":
            real_sl = fill_price * (1 - sl_ratio)
            real_tp = fill_price * (1 + tp_ratio)
            real_tp1 = fill_price * (1 + tp1_ratio)
        else:
            real_sl = fill_price * (1 + sl_ratio)
            real_tp = fill_price * (1 - tp_ratio)
            real_tp1 = fill_price * (1 - tp1_ratio)

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
            "original_qty": filled_qty,  # برای partial TP
            "sl": real_sl,
            "tp": real_tp,
            "tp1": real_tp1,
            "strategy": sig.strategy,
            "conf": sig.confidence,
            "is_partial": 0,
            "exchange_order_id": order_result["id"] or "",
            "sl_order_id": sl_order_id or "",
            "contracts": contracts,
            "original_contracts": contracts,
            "trailing_active": False,
            "atr_value": sig.atr_value,
            "highest_pnl_pct": 0,
        }

        with self._lock:
            self._pos[pid] = pos
        database.insert(pos)

        sl_pct = abs(real_sl - fill_price) / fill_price * 100
        tp_pct = abs(real_tp - fill_price) / fill_price * 100
        log.info(
            f"✅ [{short_name}] پوزیشن باز | ورود: {fill_price:.4f} | "
            f"SL: {sl_pct:.1f}% | TP: {tp_pct:.1f}% | {contracts} قرارداد"
        )

        if self.tg:
            self.tg.send(
                f"🚀 <b>پوزیشن جدید ({sig.strategy})</b>\n"
                f"{sym} | {pos_side.upper()}\n"
                f"ورود: {fill_price:.4f}\n"
                f"SL: {real_sl:.4f} ({sl_pct:.1f}%)\n"
                f"TP1: {real_tp1:.4f} | TP: {real_tp:.4f} ({tp_pct:.1f}%)\n"
                f"{contracts} قرارداد | اطمینان: {sig.confidence}%\n"
                f"📐 Trailing Stop آماده"
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
                entry = pos.get("fill_price", pos["entry"])

                # محاسبه PnL فعلی
                if side == "long":
                    pnl_pct = (price - entry) / entry * 100
                else:
                    pnl_pct = (entry - price) / entry * 100

                # ============================================
                # 🔧 اصلاح #5: Trailing Stop
                # ============================================
                if pnl_pct > TRAILING_ACTIVATE_PCT and not pos.get("trailing_active"):
                    pos["trailing_active"] = True
                    log.info(
                        f"📐 [{pos['symbol']}] Trailing Stop فعال شد | PnL={pnl_pct:.1f}%"
                    )
                    if self.tg:
                        self.tg.send(
                            f"📐 Trailing فعال | {pos['symbol']} | PnL={pnl_pct:+.1f}%"
                        )

                if pos.get("trailing_active"):
                    # بالاترین سود تا کنون
                    if pnl_pct > pos.get("highest_pnl_pct", 0):
                        pos["highest_pnl_pct"] = pnl_pct

                        # جابجایی SL
                        atr = pos.get("atr_value", entry * 0.01)
                        if side == "long":
                            new_sl = price - (TRAILING_STEP_PCT / 100.0 * price)
                            new_sl = max(new_sl, price - atr)  # حداقل ۱ ATR فاصله
                            if new_sl > pos["sl"]:
                                old_sl = pos["sl"]
                                pos["sl"] = new_sl
                                # به‌روزرسانی SL در صرافی
                                new_sl_id = EX.update_stop_loss(
                                    pos["symbol"], side, pos["qty"],
                                    pos.get("sl_order_id", ""), new_sl
                                )
                                if new_sl_id:
                                    pos["sl_order_id"] = new_sl_id
                                database.update_sl(pid, new_sl)
                                log.info(
                                    f"📐 [{pos['symbol']}] SL جابجا: "
                                    f"{old_sl:.4f} → {new_sl:.4f}"
                                )
                        else:
                            new_sl = price + (TRAILING_STEP_PCT / 100.0 * price)
                            new_sl = min(new_sl, price + atr)
                            if new_sl < pos["sl"]:
                                old_sl = pos["sl"]
                                pos["sl"] = new_sl
                                new_sl_id = EX.update_stop_loss(
                                    pos["symbol"], side, pos["qty"],
                                    pos.get("sl_order_id", ""), new_sl
                                )
                                if new_sl_id:
                                    pos["sl_order_id"] = new_sl_id
                                database.update_sl(pid, new_sl)
                                log.info(
                                    f"📐 [{pos['symbol']}] SL جابجا: "
                                    f"{old_sl:.4f} → {new_sl:.4f}"
                                )

                # ============================================
                # 🔧 اصلاح #8: Partial Take Profit
                # ============================================
                if PARTIAL_TP_ENABLED and not pos.get("is_partial", 0):
                    tp1 = pos.get("tp1", 0)
                    if tp1 > 0:
                        tp1_hit = (
                            (side == "long" and price >= tp1) or
                            (side == "short" and price <= tp1)
                        )
                        if tp1_hit:
                            self._partial_close(pid, pos, price)

                # ============================================
                # SL Check
                # ============================================
                sl_hit = (
                    (side == "long" and price <= pos["sl"]) or
                    (side == "short" and price >= pos["sl"])
                )
                if sl_hit:
                    log.info(f"🛑 [{pos['symbol']}] SL خورد!")
                    self._close_position(pid, pos, price, "StopLoss")
                    continue

                # ============================================
                # TP Check (TP کامل)
                # ============================================
                tp_hit = (
                    (side == "long" and price >= pos["tp"]) or
                    (side == "short" and price <= pos["tp"])
                )
                if tp_hit:
                    log.info(f"🎯 [{pos['symbol']}] TP خورد!")
                    self._close_position(pid, pos, price, "TakeProfit")
                    continue

                # به‌روزرسانی در حافظه
                with self._lock:
                    if pid in self._pos:
                        self._pos[pid] = pos

            except Exception as e:
                log.error(f"Manage Error [{pos.get('symbol', '?')}]: {e}")

    def _partial_close(self, pid: str, pos: Dict, price: float):
        """بستن بخشی از پوزیشن در TP1"""
        original_qty = pos.get("original_qty", pos["qty"])
        close_qty = original_qty * PARTIAL_TP_RATIO

        if close_qty <= 0:
            return

        close_side = "sell" if pos["side"] == "long" else "buy"
        result = EX.place_order(pos["symbol"], close_side, close_qty, is_close=True)

        if result:
            remaining_qty = pos["qty"] - result["filled_qty"]
            if remaining_qty <= 0:
                remaining_qty = pos["qty"] * (1 - PARTIAL_TP_RATIO)

            # جابجایی SL به نقطه ورود (breakeven)
            new_sl = pos.get("fill_price", pos["entry"])

            pos["qty"] = remaining_qty
            pos["sl"] = new_sl
            pos["is_partial"] = 1

            # به‌روزرسانی SL در صرافی
            new_sl_id = EX.update_stop_loss(
                pos["symbol"], pos["side"], remaining_qty,
                pos.get("sl_order_id", ""), new_sl
            )
            if new_sl_id:
                pos["sl_order_id"] = new_sl_id

            database.update_partial(pid, remaining_qty, new_sl)

            entry = pos.get("fill_price", pos["entry"])
            pnl_partial = (
                (price - entry) * close_qty if pos["side"] == "long"
                else (entry - price) * close_qty
            )

            log.info(
                f"✂️ [{pos['symbol']}] Partial TP | "
                f"{PARTIAL_TP_RATIO * 100:.0f}% بسته شد | "
                f"PnL: {pnl_partial:+.2f}$ | SL→BE"
            )

            if self.tg:
                self.tg.send(
                    f"✂️ <b>Partial TP</b>\n"
                    f"{pos['symbol']} | {PARTIAL_TP_RATIO * 100:.0f}% بسته شد\n"
                    f"PnL: {pnl_partial:+.2f}$\n"
                    f"SL به BreakEven جابجا شد ✅"
                )

            with self._lock:
                if pid in self._pos:
                    self._pos[pid] = pos

    def _close_position(self, pid: str, pos: Dict, price: float, reason: str):
        close_side = "sell" if pos["side"] == "long" else "buy"
        result = EX.place_order(pos["symbol"], close_side, pos["qty"], is_close=True)
        actual_price = result["fill_price"] if result else price

        if pos.get("sl_order_id"):
            EX.cancel_order_safe(pos["symbol"], pos["sl_order_id"])

        entry = pos.get("fill_price", pos["entry"])
        pnl = (
            (actual_price - entry) * pos["qty"] if pos["side"] == "long"
            else (entry - actual_price) * pos["qty"]
        )
        pct = (
            (actual_price - entry) / entry * 100 if pos["side"] == "long"
            else (entry - actual_price) / entry * 100
        )

        database.close(pid, actual_price, pnl, pct, reason)

        with self._lock:
            self._pos.pop(pid, None)

        emoji = "✅" if pnl >= 0 else "❌"
        log.info(
            f"{emoji} [{pos['symbol']}] بسته شد ({reason}) | "
            f"PnL: {pnl:+.2f}$ ({pct:+.2f}%)"
        )

        if self.tg:
            trail_info = " | 📐 Trailing" if pos.get("trailing_active") else ""
            partial_info = " | ✂️ Partial" if pos.get("is_partial") else ""
            self.tg.send(
                f"{emoji} <b>بسته شد ({reason})</b>\n"
                f"{pos['symbol']} | {pos['side'].upper()}\n"
                f"PnL: {pnl:+.2f}$ ({pct:+.2f}%)"
                f"{trail_info}{partial_info}"
            )


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

    # نمایش پوزیشن‌های فعلی
    positions_html = ""
    if engine_instance:
        for pid, pos in engine_instance._pos.items():
            price = EX.get_current_price(pos["symbol"])
            if price:
                entry = pos.get("fill_price", pos["entry"])
                if pos["side"] == "long":
                    pnl_pct = (price - entry) / entry * 100
                else:
                    pnl_pct = (entry - price) / entry * 100
                trail = "📐" if pos.get("trailing_active") else ""
                partial = "✂️" if pos.get("is_partial") else ""
                color = "#3fb950" if pnl_pct >= 0 else "#f85149"
                positions_html += f"""
                <div class="card" style="border-color: {color}; min-width: 200px;">
                    <h4>{pos['symbol'].split('/')[0]} {pos['side'].upper()} {trail}{partial}</h4>
                    <p>ورود: {entry:.4f}</p>
                    <p style="color: {color};">PnL: {pnl_pct:+.2f}%</p>
                    <p>SL: {pos['sl']:.4f} | TP: {pos['tp']:.4f}</p>
                    <p style="font-size:0.8em;">{pos['strategy']}</p>
                </div>"""

    return f"""
    <!DOCTYPE html>
    <html dir="rtl" lang="fa">
    <head>
        <meta charset="UTF-8">
        <title>Quant Bot v5.0</title>
        <meta http-equiv="refresh" content="20">
        <style>
            body {{ font-family: Tahoma, sans-serif; background: #0d1117;
                   color: #c9d1d9; padding: 20px; text-align: center; }}
            .card {{ background: #161b22; border: 1px solid #30363d;
                    padding: 12px; margin: 6px; border-radius: 8px;
                    display: inline-block; min-width: 130px;
                    vertical-align: top; }}
            .warn {{ background: #3d1f00; border-color: #f0883e; color: #f0883e; }}
            .ok {{ border-color: #3fb950; }}
            h1 {{ color: #58a6ff; }}
            .badge {{ background: #238636; padding: 2px 8px;
                     border-radius: 4px; font-size: 0.8em; }}
            .section {{ margin: 15px 0; }}
        </style>
    </head>
    <body>
        <h1>🤖 Master-AI Quant Bot v5.0</h1>
        <span class="badge">✅ ۵ استراتژی + Trailing + Partial TP</span>
        <div class="section">
            وضعیت: <b>{'▶️ فعال' if active else '⏹ متوقف'}</b> |
            اتصال: <b>{'✅' if connected else '❌'}</b> |
            شبکه: <b>{mode}</b> |
            پوزیشن: <b>{pos_count}/{MAX_POS}</b>
        </div>

        <div class="section">
            <div class="card"><h3>💰 موجودی</h3><p>${bal:,.2f}</p></div>
            <div class="card"><h3>💎 ارزش کل</h3><p>${equity:,.2f}</p></div>
            <div class="card {'ok' if stats['total_pnl'] >= 0 else 'warn'}">
                <h3>📈 PnL</h3><p>{stats['total_pnl']:+,.2f}$</p>
            </div>
            <div class="card"><h3>🎯 WR</h3><p>{stats['win_rate']}%</p></div>
            <div class="card"><h3>🛡️ DD</h3><p>{dd:.1f}%</p></div>
            <div class="card"><h3>📊 معاملات</h3><p>{stats['total_trades']}</p></div>
        </div>

        <div class="section">
            <h2>📈 پوزیشن‌های فعال</h2>
            {positions_html if positions_html else '<p>هیچ پوزیشنی نیست</p>'}
        </div>

        <div class="section">
            <div class="card" style="min-width: 280px;">
                <h3>🧠 تنظیمات</h3>
                <p>استراتژی: Breakout, Pullback, RSI, MACD, BB</p>
                <p>SL: {ATR_MULTIPLIER_SL}×ATR | TP: {ATR_MULTIPLIER_TP}×ATR</p>
                <p>📐 Trailing: {TRAILING_ACTIVATE_PCT}% | ✂️ Partial: {PARTIAL_TP_RATIO*100:.0f}%</p>
                <p>Scan: {SCAN_INTERVAL}s | Batch: {SCAN_BATCH_SIZE} | MinConf: {MIN_CONFIDENCE}%</p>
            </div>
        </div>
    </body>
    </html>
    """


@app.route("/health")
def health():
    return {
        "status": "ok",
        "version": "5.0",
        "connected": EX.is_connected,
        "testnet": TESTNET,
        "active": engine_instance.is_active if engine_instance else False,
        "positions": len(engine_instance._pos) if engine_instance else 0,
        "strategies": ["Breakout", "Pullback", "RSI_Trend", "MACD_ADX", "BB_Squeeze"],
        "features": ["trailing_stop", "partial_tp", "5_strategies"],
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
                "reason": sig.reason,
                "debug": sig.debug_info,
                "sl_pct": round(
                    abs(sig.sl - sig.entry_estimate) / sig.entry_estimate * 100, 2
                ) if sig.entry_estimate else 0,
                "tp_pct": round(
                    abs(sig.tp - sig.entry_estimate) / sig.entry_estimate * 100, 2
                ) if sig.entry_estimate else 0,
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
    log.info("  🤖 Master-AI Quant Bot v5.0 (FULLY OPTIMIZED)")
    log.info("  ✅ ۵ استراتژی: Breakout, Pullback, RSI, MACD, BB")
    log.info("  ✅ Trailing Stop + Partial Take Profit")
    log.info("  ✅ حد ضرر پویا بر اساس ATR")
    log.info("  ✅ Fallback به Binance")
    log.info("  🌐 Mode: %s", "TESTNET" if TESTNET else "MAINNET")
    log.info("  🔗 Connected: %s", EX.is_connected)
    log.info("  🎯 SL: %.1f*ATR | TP: %.1f*ATR", ATR_MULTIPLIER_SL, ATR_MULTIPLIER_TP)
    log.info("  📐 Trail: %.1f%% | Partial: %.0f%%",
             TRAILING_ACTIVATE_PCT, PARTIAL_TP_RATIO * 100)
    log.info("  📊 Max Pos: %d | Scan: %ds | Batch: %d",
             MAX_POS, SCAN_INTERVAL, SCAN_BATCH_SIZE)
    log.info("  🔍 Min Confidence: %d%%", MIN_CONFIDENCE)
    log.info("=" * 60)

    if not EX.is_connected:
        log.critical("❌ اتصال به صرافی برقرار نشد!")

    engine_instance = Engine()
    tg = TelegramHandler(engine_instance)
    engine_instance.tg = tg

    if TG_TOKEN and TG_CHAT:
        tg.send(
            f"🚀 <b>ربات v5.0 شروع شد</b>\n"
            f"{'═' * 28}\n"
            f"🧠 ۵ استراتژی فعال:\n"
            f"  1️⃣ Breakout (شکست سطوح)\n"
            f"  2️⃣ Pullback (برگشت به EMA)\n"
            f"  3️⃣ RSI + Trend\n"
            f"  4️⃣ MACD + ADX\n"
            f"  5️⃣ Bollinger Squeeze\n"
            f"{'═' * 28}\n"
            f"📐 Trailing Stop: فعال ({TRAILING_ACTIVATE_PCT}%)\n"
            f"✂️ Partial TP: {PARTIAL_TP_RATIO * 100:.0f}% در {PARTIAL_TP1_MULTIPLIER}×ATR\n"
            f"🎯 SL: {ATR_MULTIPLIER_SL}×ATR | TP: {ATR_MULTIPLIER_TP}×ATR\n"
            f"🛡️ Max DD: {MAX_DD}%\n"
            f"📊 Max Pos: {MAX_POS} | Scan: {SCAN_INTERVAL}s\n"
            f"🔍 Min Conf: {MIN_CONFIDENCE}%\n"
            f"🌐 {'🧪 TESTNET' if TESTNET else '💰 MAINNET'}\n"
            f"{'═' * 28}\n"
            f"✅ ربات آماده اسکن است",
            reply_markup=tg._keyboard(),
        )

    threading.Thread(target=engine_instance.run_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)


if __name__ == "__main__":
    main()
