#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Quant Bot v4.2 - FINAL FIXED VERSION
نسخه نهایی با رفع کامل مشکل دریافت داده
- افزایش timeout و retry
- اسکن گروهی نمادها
- Fallback به Binance
- بهینه‌سازی Rate Limit
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
log = logging.getLogger("MasterQuant_v4.2")


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

# 🔥 تنظیمات بهینه‌شده
RISK_PCT = Cfg.f("RISK_PER_TRADE", 0.5)
MAX_DD = Cfg.f("MAX_DRAWDOWN", 10.0)
MAX_POS = Cfg.i("MAX_POSITIONS", 2)  # 🔥 کاهش به ۲
LEVERAGE = Cfg.i("LEVERAGE", 5)
TESTNET = Cfg.b("PHEMEX_TESTNET", True)
PORT = Cfg.i("PORT", 10000)
SCAN_INTERVAL = Cfg.i("SCAN_INTERVAL", 45)  # 🔥 افزایش به ۴۵
MIN_CONFIDENCE = Cfg.i("MIN_CONFIDENCE", 70)
SCAN_BATCH_SIZE = Cfg.i("SCAN_BATCH_SIZE", 3)  # 🔥 حداکثر ۳ نماد در هر اسکن
REQUEST_TIMEOUT = Cfg.i("REQUEST_TIMEOUT", 30)


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
    def sma(close: pd.Series, n: int) -> pd.Series:
        return close.rolling(n).mean()

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
            "is_partial,exchange_order_id,sl_order_id "
            "FROM trades WHERE status='open'"
        )
        if not rows:
            return []
        keys = [
            "id", "symbol", "side", "entry", "fill_price", "qty",
            "filled_qty", "sl", "tp", "strategy", "conf",
            "is_partial", "exchange_order_id", "sl_order_id",
        ]
        return [dict(zip(keys, r)) for r in rows]

    def insert(self, t: Dict):
        self.run(
            "INSERT OR IGNORE INTO trades "
            "(id,symbol,side,entry_price,fill_price,quantity,filled_quantity,"
            "stop_loss,take_profit,strategy,confidence,exchange_order_id,"
            "sl_order_id,is_real) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                t["id"], t["symbol"], t["side"], t["entry"],
                t.get("fill_price", t["entry"]),
                t["qty"], t.get("filled_qty", t["qty"]),
                t["sl"], t["tp"], t["strategy"], t["conf"],
                t.get("exchange_order_id", ""),
                t.get("sl_order_id", ""),
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
# EXCHANGE ENGINE - نسخه اصلاح‌شده با fallback
# ============================================================================
class Exchange:

    def __init__(self):
        self._ex: Optional[ccxt.phemex] = None
        self._binance: Optional[ccxt.binance] = None
        self._markets_info: Dict = {}
        self._connected = False
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

            # 🔥 اتصال به Binance برای fallback
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
                self._markets_info[sym] = {
                    "min_amount": mkt.get("limits", {}).get(
                        "amount", {}
                    ).get("min", 0.001),
                    "min_cost": mkt.get("limits", {}).get(
                        "cost", {}
                    ).get("min", 0.5),
                    "precision_amount": mkt.get("precision", {}).get(
                        "amount", 0.001
                    ),
                    "precision_price": mkt.get("precision", {}).get(
                        "price", 0.01
                    ),
                    "contract_size": mkt.get("contractSize", 1),
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

    def fetch_ohlcv_safe(self, sym: str, tf: str = "5m",
                         limit: int = 80, max_retries: int = 5) -> Optional[pd.DataFrame]:
        """🔥 دریافت داده با retry و fallback به Binance"""
        if not self.is_connected:
            return None

        for attempt in range(max_retries):
            try:
                # 🔥 تلاش با Phemex
                raw = self._ex.fetch_ohlcv(sym, tf, limit=limit)
                if raw and len(raw) >= 30:
                    df = pd.DataFrame(
                        raw, columns=["ts", "open", "high", "low", "close", "vol"]
                    )
                    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
                    
                    if not df["close"].isna().any():
                        return df

                if attempt == max_retries - 1:
                    # 🔥 Fallback به Binance
                    log.warning(f"⚠️ Fallback به Binance برای {sym} {tf}")
                    return self._fetch_from_binance(sym, tf, limit)

                time.sleep(0.5 * (attempt + 1))

            except ccxt.RateLimitExceeded:
                log.warning(f"⚠️ Rate Limit {sym} {tf}, waiting...")
                time.sleep(2 * (attempt + 1))

            except ccxt.NetworkError:
                log.warning(f"⚠️ Network Error {sym} {tf}, retrying...")
                time.sleep(1 * (attempt + 1))

            except Exception as e:
                if attempt == max_retries - 1:
                    log.error(f"❌ OHLCV Error [{sym} {tf}]: {e}")
                    return self._fetch_from_binance(sym, tf, limit)
                time.sleep(0.5 * (attempt + 1))

        return None

    def _fetch_from_binance(self, sym: str, tf: str, limit: int) -> Optional[pd.DataFrame]:
        """🔥 دریافت داده از Binance"""
        if not self._binance:
            return None

        try:
            # تبدیل نام نماد برای Binance
            binance_sym = sym.replace("/USDT:USDT", "/USDT")
            
            raw = self._binance.fetch_ohlcv(binance_sym, tf, limit=limit)
            if not raw or len(raw) < 30:
                return None

            df = pd.DataFrame(
                raw, columns=["ts", "open", "high", "low", "close", "vol"]
            )
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            
            log.info(f"✅ داده {sym} از Binance دریافت شد")
            return df

        except Exception as e:
            log.debug(f"Binance fallback failed: {e}")
            return None

    def fetch_multi_ohlcv(self, sym: str) -> Dict[str, pd.DataFrame]:
        """🔥 دریافت داده با مدیریت بهتر"""
        result = {}
        timeframes = ["1m", "3m", "5m", "15m"]

        for tf in timeframes:
            df = self.fetch_ohlcv_safe(sym, tf, limit=80, max_retries=3)

            if df is None or len(df) < 30:
                log.debug(f"⚠️ داده ناکافي {sym} {tf} (len={len(df) if df is not None else 0})")
                
                if tf == "1m":
                    time.sleep(1)
                    df = self.fetch_ohlcv_safe(sym, tf, limit=80, max_retries=3)
                    if df is None or len(df) < 30:
                        log.error(f"❌ داده اصلی {sym} دريافت نشد")
                        return {}
                else:
                    if result:
                        log.warning(f"⚠️ {sym} {tf} داده ناقص، ادامه با داده موجود")
                        continue
                    return {}

            result[tf] = df
            time.sleep(0.3)  # 🔥 فاصله بین درخواست‌ها

        # 🔥 بررسی وجود داده‌های ضروری
        required = ["1m", "5m", "15m"]
        for tf in required:
            if tf not in result:
                log.error(f"❌ داده {tf} برای {sym} موجود نیست")
                return {}

        return result

    def get_current_price(self, sym: str) -> Optional[float]:
        if not self.is_connected:
            return None
        try:
            ticker = self._ex.fetch_ticker(sym)
            return float(ticker.get("last", 0))
        except Exception as e:
            log.error("Ticker Error [%s]: %s", sym, e)
            # 🔥 Fallback به Binance
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
                        "unrealized_pnl": float(
                            p.get("unrealizedPnl", 0) or 0
                        ),
                        "liquidation": float(
                            p.get("liquidationPrice", 0) or 0
                        ),
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

    def validate_order_size(self, sym: str, qty: float,
                            price: float) -> Tuple[bool, float, str]:
        info = self._markets_info.get(sym, {})
        min_amount = info.get("min_amount", 0.001)
        min_cost = info.get("min_cost", 0.5)

        try:
            formatted_qty = float(self._ex.amount_to_precision(sym, qty))
        except Exception:
            formatted_qty = qty

        if formatted_qty < min_amount:
            return (False, 0, f"حجم {formatted_qty} کمتر از حداقل {min_amount}")

        cost = formatted_qty * price / LEVERAGE
        if cost < min_cost:
            return (False, 0, f"ارزش {cost:.2f}$ کمتر از حداقل {min_cost}$")

        return True, formatted_qty, "OK"

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

            valid, fmt_qty, msg = self.validate_order_size(sym, qty, current_price)
            if not valid:
                log.warning("⚠️  سفارش نامعتبر [%s]: %s", sym, msg)
                return None

            params = {}
            if is_close:
                params["reduceOnly"] = True

            log.info("📤 ارسال سفارش | %s %s | حجم: %s", side.upper(), sym, fmt_qty)

            if side.lower() == "buy":
                result = self._ex.create_market_buy_order(sym, fmt_qty, params=params)
            else:
                result = self._ex.create_market_sell_order(sym, fmt_qty, params=params)

            fill_price = float(result.get("average") or result.get("price") or current_price)
            filled_qty = float(result.get("filled") or result.get("amount") or fmt_qty)

            log.info("✅ سفارش اجرا شد | %s %s | حجم: %s | قيمت: %s",
                     side.upper(), sym, filled_qty, fill_price)

            return {
                "id": result.get("id"),
                "fill_price": fill_price,
                "filled_qty": filled_qty,
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
            sl_side = "sell" if pos_side == "long" else "buy"
            fmt_qty = float(self._ex.amount_to_precision(sym, qty))
            fmt_price = float(self._ex.price_to_precision(sym, stop_price))

            params = {
                "stopPrice": fmt_price,
                "reduceOnly": True,
                "triggerType": "ByLastPrice",
            }

            result = self._ex.create_order(
                sym, "market", sl_side, fmt_qty, None, params=params,
            )

            log.info("🛑 SL ثبت شد | %s | قيمت: %s", sym, fmt_price)
            return result.get("id")
        except Exception as e:
            log.warning("⚠️  خطا در SL [%s]: %s", sym, e)
            return None

    def cancel_order_safe(self, sym: str, order_id: str):
        if not self.is_connected or not order_id:
            return
        try:
            self._ex.cancel_order(order_id, sym)
            log.info("❌ سفارش %s لغو شد", order_id)
        except Exception as e:
            log.debug("Cancel order [%s]: %s", order_id, e)


EX = Exchange()


# ============================================================================
# STRATEGY ENGINE
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

    def analyze(self, sym: str, dfs: Dict[str, pd.DataFrame]) -> Signal:
        required = ["1m", "3m", "5m", "15m"]
        
        if not dfs:
            return Signal(debug_info="داده دريافت نشد")
        
        for tf in required:
            if tf not in dfs:
                return Signal(debug_info=f"{tf} موجود نیست")
            if len(dfs[tf]) < 30:
                return Signal(debug_info=f"{tf} داده ناکافی")
        
        df1m = dfs["1m"]
        df3m = dfs["3m"]
        df5m = dfs["5m"]
        df15m = dfs["15m"]
        
        adx15 = IND.safe(IND.adx(df15m["high"], df15m["low"], df15m["close"]))
        trend_str = IND.trend_strength(df15m["close"])
        
        # استراتژی‌های اصلی
        if adx15 > 20:
            sig = self._momentum_scalp_optimized(df1m, df3m, df15m, adx15, trend_str)
            if sig.action != "neutral" and sig.confidence >= MIN_CONFIDENCE:
                return sig
        
        if adx15 <= 28:
            sig = self._mean_reversion_optimized(df1m, df5m, df15m, adx15, trend_str)
            if sig.action != "neutral" and sig.confidence >= MIN_CONFIDENCE:
                return sig
        
        sig = self._breakout_optimized(df1m, df5m, df15m, adx15, trend_str)
        if sig.action != "neutral" and sig.confidence >= MIN_CONFIDENCE:
            return sig
        
        # استراتژی‌های جدید
        sig = self._pullback_ema(df1m, df5m, df15m, adx15)
        if sig.action != "neutral" and sig.confidence >= MIN_CONFIDENCE:
            return sig
        
        sig = self._supertrend_volume(df1m, df5m, df15m)
        if sig.action != "neutral" and sig.confidence >= MIN_CONFIDENCE:
            return sig
        
        sig = self._double_pattern(df1m, df5m, df15m)
        if sig.action != "neutral" and sig.confidence >= MIN_CONFIDENCE:
            return sig
        
        sig = self._atr_breakout(df1m, df5m, df15m)
        if sig.action != "neutral" and sig.confidence >= MIN_CONFIDENCE:
            return sig
        
        sig = self._rsi_divergence(df1m, df5m, df15m)
        if sig.action != "neutral" and sig.confidence >= MIN_CONFIDENCE:
            return sig
        
        sig = self._opening_range(df1m, df5m, df15m)
        if sig.action != "neutral" and sig.confidence >= MIN_CONFIDENCE:
            return sig
        
        return Signal(
            debug_info=f"ADX15={adx15:.1f} Trend={trend_str:.2f} - هيچ سيگنالي"
        )

    def _pullback_ema(self, df1m, df5m, df15m, adx15) -> Signal:
        ema20 = IND.safe(IND.ema(df15m["close"], 20))
        ema50 = IND.safe(IND.ema(df15m["close"], 50))
        ema200 = IND.safe(IND.ema(df15m["close"], 200))
        price = IND.safe(df15m["close"])
        rsi = IND.safe(IND.rsi(df15m["close"], 14))
        
        if price > ema50 > ema200 and adx15 > 25:
            if price <= ema20 * 1.005 and price >= ema20 * 0.995:
                if 40 < rsi < 50:
                    c1 = IND.safe(df1m["close"])
                    ema9_1 = IND.safe(IND.ema(df1m["close"], 9))
                    if c1 > ema9_1:
                        atr15 = IND.safe(IND.atr(df15m["high"], df15m["low"], df15m["close"]))
                        sl_dist = max(atr15 * 2.0, c1 * 0.025)
                        sl = c1 - sl_dist
                        tp = c1 + sl_dist * 1.5
                        return Signal(
                            action="buy",
                            strategy="PullbackEMA",
                            confidence=72,
                            reason=f"Pullback EMA RSI={rsi:.0f}",
                            sl=sl, tp=tp, entry_estimate=c1,
                            debug_info="✅ Pullback EMA"
                        )
        
        if price < ema50 < ema200 and adx15 > 25:
            if price >= ema20 * 0.995 and price <= ema20 * 1.005:
                if 50 < rsi < 60:
                    c1 = IND.safe(df1m["close"])
                    ema9_1 = IND.safe(IND.ema(df1m["close"], 9))
                    if c1 < ema9_1:
                        atr15 = IND.safe(IND.atr(df15m["high"], df15m["low"], df15m["close"]))
                        sl_dist = max(atr15 * 2.0, c1 * 0.025)
                        sl = c1 + sl_dist
                        tp = c1 - sl_dist * 1.5
                        return Signal(
                            action="sell",
                            strategy="PullbackEMA",
                            confidence=72,
                            reason=f"Pullback EMA RSI={rsi:.0f}",
                            sl=sl, tp=tp, entry_estimate=c1,
                            debug_info="✅ Pullback EMA (شورت)"
                        )
        return Signal(debug_info="PullbackEMA: شرايط برقرار نيست")

    def _supertrend_volume(self, df1m, df5m, df15m) -> Signal:
        atr = IND.safe(IND.atr(df15m["high"], df15m["low"], df15m["close"], 10))
        multiplier = 3.0
        hl2 = (df15m["high"] + df15m["low"]) / 2
        
        upper_band = IND.safe(hl2 + multiplier * atr)
        lower_band = IND.safe(hl2 - multiplier * atr)
        
        close = IND.safe(df15m["close"])
        prev_close = IND.safe(df15m["close"], -2)
        
        vol = IND.safe(df15m["vol"])
        avg_vol = IND.safe(df15m["vol"].rolling(20).mean())
        vol_surge = vol > avg_vol * 1.3
        
        if not vol_surge:
            return Signal(debug_info="SuperTrend: حجم کافی نیست")
        
        if close > upper_band and prev_close <= upper_band:
            c1 = IND.safe(df1m["close"])
            atr1 = IND.safe(IND.atr(df1m["high"], df1m["low"], df1m["close"]))
            sl_dist = max(atr1 * 2.0, c1 * 0.025)
            sl = c1 - sl_dist
            tp = c1 + sl_dist * 1.8
            return Signal(
                action="buy",
                strategy="SuperTrend_Vol",
                confidence=78,
                reason=f"SuperTrend تغییر + حجم {vol/avg_vol:.1f}x",
                sl=sl, tp=tp, entry_estimate=c1,
                debug_info="✅ SuperTrend BUY"
            )
        
        if close < lower_band and prev_close >= lower_band:
            c1 = IND.safe(df1m["close"])
            atr1 = IND.safe(IND.atr(df1m["high"], df1m["low"], df1m["close"]))
            sl_dist = max(atr1 * 2.0, c1 * 0.025)
            sl = c1 + sl_dist
            tp = c1 - sl_dist * 1.8
            return Signal(
                action="sell",
                strategy="SuperTrend_Vol",
                confidence=78,
                reason=f"SuperTrend تغییر + حجم {vol/avg_vol:.1f}x",
                sl=sl, tp=tp, entry_estimate=c1,
                debug_info="✅ SuperTrend SELL"
            )
        return Signal(debug_info="SuperTrend: بدون تغییر روند")

    def _double_pattern(self, df1m, df5m, df15m) -> Signal:
        high = df5m["high"].values
        low = df5m["low"].values
        close = df5m["close"].values
        
        troughs = []
        peaks = []
        
        for i in range(5, len(close) - 5):
            if low[i] < low[i-1] and low[i] < low[i+1]:
                troughs.append((i, low[i]))
            if high[i] > high[i-1] and high[i] > high[i+1]:
                peaks.append((i, high[i]))
        
        if len(peaks) >= 2:
            last_peak = peaks[-1]
            prev_peak = peaks[-2]
            
            diff_pct = abs(last_peak[1] - prev_peak[1]) / prev_peak[1] * 100
            if diff_pct < 2.0:
                neck = min(low[prev_peak[0]:last_peak[0]])
                current_price = IND.safe(df5m["close"])
                
                if current_price < neck:
                    vol = IND.safe(df5m["vol"])
                    avg_vol = IND.safe(df5m["vol"].rolling(20).mean())
                    if vol > avg_vol * 1.1:
                        c1 = IND.safe(df1m["close"])
                        if c1 <= 0:
                            c1 = current_price
                        
                        atr15 = IND.safe(IND.atr(df15m["high"], df15m["low"], df15m["close"]))
                        sl_dist = max(atr15 * 2.0, c1 * 0.025)
                        
                        sl = prev_peak[1] * 1.02
                        tp = c1 - (prev_peak[1] - neck) * 1.2
                        
                        if tp < c1 - c1 * 0.01:
                            return Signal(
                                action="sell",
                                strategy="DoubleTop",
                                confidence=75,
                                reason=f"Double Top {diff_pct:.1f}%",
                                sl=sl, tp=tp, entry_estimate=c1,
                                debug_info="✅ Double Top"
                            )
        
        if len(troughs) >= 2:
            last_trough = troughs[-1]
            prev_trough = troughs[-2]
            
            diff_pct = abs(last_trough[1] - prev_trough[1]) / prev_trough[1] * 100
            if diff_pct < 2.0:
                neck = max(high[prev_trough[0]:last_trough[0]])
                current_price = IND.safe(df5m["close"])
                
                if current_price > neck:
                    vol = IND.safe(df5m["vol"])
                    avg_vol = IND.safe(df5m["vol"].rolling(20).mean())
                    if vol > avg_vol * 1.1:
                        c1 = IND.safe(df1m["close"])
                        if c1 <= 0:
                            c1 = current_price
                        
                        atr15 = IND.safe(IND.atr(df15m["high"], df15m["low"], df15m["close"]))
                        sl_dist = max(atr15 * 2.0, c1 * 0.025)
                        
                        sl = prev_trough[1] * 0.98
                        tp = c1 + (neck - prev_trough[1]) * 1.2
                        
                        if tp > c1 + c1 * 0.01:
                            return Signal(
                                action="buy",
                                strategy="DoubleBottom",
                                confidence=75,
                                reason=f"Double Bottom {diff_pct:.1f}%",
                                sl=sl, tp=tp, entry_estimate=c1,
                                debug_info="✅ Double Bottom"
                            )
        return Signal(debug_info="DoublePattern: الگويي يافت نشد")

    def _atr_breakout(self, df1m, df5m, df15m) -> Signal:
        high = df5m["high"]
        low = df5m["low"]
        close = df5m["close"]
        
        atr = IND.safe(IND.atr(high, low, close, 14))
        upper_band = IND.safe(high.rolling(20).max()) + atr * 0.5
        lower_band = IND.safe(low.rolling(20).min()) - atr * 0.5
        
        current_price = IND.safe(close)
        vol = IND.safe(df5m["vol"])
        avg_vol = IND.safe(df5m["vol"].rolling(20).mean())
        
        if current_price > upper_band and vol > avg_vol * 1.1:
            c1 = IND.safe(df1m["close"])
            sl_dist = max(atr * 2.0, c1 * 0.025)
            sl = c1 - sl_dist
            tp = c1 + sl_dist * 1.8
            return Signal(
                action="buy",
                strategy="ATR_Breakout",
                confidence=72,
                reason=f"ATR Breakout بالا",
                sl=sl, tp=tp, entry_estimate=c1,
                debug_info="✅ ATR Breakout UP"
            )
        
        if current_price < lower_band and vol > avg_vol * 1.1:
            c1 = IND.safe(df1m["close"])
            sl_dist = max(atr * 2.0, c1 * 0.025)
            sl = c1 + sl_dist
            tp = c1 - sl_dist * 1.8
            return Signal(
                action="sell",
                strategy="ATR_Breakout",
                confidence=72,
                reason=f"ATR Breakout پایین",
                sl=sl, tp=tp, entry_estimate=c1,
                debug_info="✅ ATR Breakout DOWN"
            )
        return Signal(debug_info="ATR_Breakout: بدون شکست")

    def _rsi_divergence(self, df1m, df5m, df15m) -> Signal:
        close = df15m["close"]
        rsi = IND.rsi(close, 14)
        
        price_lows = []
        rsi_lows = []
        
        for i in range(10, len(close) - 1):
            if close.iloc[i] < close.iloc[i-1] and close.iloc[i] < close.iloc[i+1]:
                price_lows.append((i, close.iloc[i]))
            if rsi.iloc[i] < rsi.iloc[i-1] and rsi.iloc[i] < rsi.iloc[i+1]:
                rsi_lows.append((i, rsi.iloc[i]))
        
        if len(price_lows) >= 2 and len(rsi_lows) >= 2:
            last_price_low = price_lows[-1]
            prev_price_low = price_lows[-2]
            last_rsi_low = rsi_lows[-1]
            prev_rsi_low = rsi_lows[-2]
            
            if last_price_low[1] < prev_price_low[1] and last_rsi_low[1] > prev_rsi_low[1]:
                c1 = IND.safe(df1m["close"])
                atr1 = IND.safe(IND.atr(df1m["high"], df1m["low"], df1m["close"]))
                sl_dist = max(atr1 * 2.0, c1 * 0.025)
                sl = c1 - sl_dist
                tp = c1 + sl_dist * 1.8
                return Signal(
                    action="buy",
                    strategy="RSI_Divergence",
                    confidence=80,
                    reason=f"واگرایی مثبت RSI",
                    sl=sl, tp=tp, entry_estimate=c1,
                    debug_info="✅ RSI Divergence BUY"
                )
        
        price_highs = []
        rsi_highs = []
        for i in range(10, len(close) - 1):
            if close.iloc[i] > close.iloc[i-1] and close.iloc[i] > close.iloc[i+1]:
                price_highs.append((i, close.iloc[i]))
            if rsi.iloc[i] > rsi.iloc[i-1] and rsi.iloc[i] > rsi.iloc[i+1]:
                rsi_highs.append((i, rsi.iloc[i]))
        
        if len(price_highs) >= 2 and len(rsi_highs) >= 2:
            last_price_high = price_highs[-1]
            prev_price_high = price_highs[-2]
            last_rsi_high = rsi_highs[-1]
            prev_rsi_high = rsi_highs[-2]
            
            if last_price_high[1] > prev_price_high[1] and last_rsi_high[1] < prev_rsi_high[1]:
                c1 = IND.safe(df1m["close"])
                atr1 = IND.safe(IND.atr(df1m["high"], df1m["low"], df1m["close"]))
                sl_dist = max(atr1 * 2.0, c1 * 0.025)
                sl = c1 + sl_dist
                tp = c1 - sl_dist * 1.8
                return Signal(
                    action="sell",
                    strategy="RSI_Divergence",
                    confidence=80,
                    reason=f"واگرایی منفی RSI",
                    sl=sl, tp=tp, entry_estimate=c1,
                    debug_info="✅ RSI Divergence SELL"
                )
        return Signal(debug_info="RSI_Divergence: واگرايي يافت نشد")

    def _opening_range(self, df1m, df5m, df15m) -> Signal:
        if len(df5m) < 20:
            return Signal(debug_info="OpeningRange: داده کافی نیست")
        
        first_15 = df5m.iloc[:3]
        range_high = first_15["high"].max()
        range_low = first_15["low"].min()
        
        current_price = IND.safe(df5m["close"])
        vol = IND.safe(df5m["vol"])
        avg_vol = IND.safe(df5m["vol"].rolling(20).mean())
        
        if current_price > range_high and vol > avg_vol * 1.1:
            c1 = IND.safe(df1m["close"])
            atr1 = IND.safe(IND.atr(df1m["high"], df1m["low"], df1m["close"]))
            sl_dist = max(atr1 * 2.0, c1 * 0.025)
            sl = c1 - sl_dist
            tp = c1 + sl_dist * 1.8
            return Signal(
                action="buy",
                strategy="OpeningRange",
                confidence=75,
                reason=f"شکست محدوده بازگشایی",
                sl=sl, tp=tp, entry_estimate=c1,
                debug_info="✅ Opening Range UP"
            )
        
        if current_price < range_low and vol > avg_vol * 1.1:
            c1 = IND.safe(df1m["close"])
            atr1 = IND.safe(IND.atr(df1m["high"], df1m["low"], df1m["close"]))
            sl_dist = max(atr1 * 2.0, c1 * 0.025)
            sl = c1 + sl_dist
            tp = c1 - sl_dist * 1.8
            return Signal(
                action="sell",
                strategy="OpeningRange",
                confidence=75,
                reason=f"شکست محدوده بازگشایی",
                sl=sl, tp=tp, entry_estimate=c1,
                debug_info="✅ Opening Range DOWN"
            )
        return Signal(debug_info="OpeningRange: بدون شکست")

    def _momentum_scalp_optimized(self, df1m, df3m, df15m, adx15, trend_str) -> Signal:
        price15 = IND.safe(df15m["close"])
        ema20_15 = IND.safe(IND.ema(df15m["close"], 20))
        ema50_15 = IND.safe(IND.ema(df15m["close"], 50))
        
        if price15 > ema20_15 and ema20_15 > ema50_15 and trend_str > 0.3:
            trend = "long"
        elif price15 < ema20_15 and ema20_15 < ema50_15 and trend_str < -0.3:
            trend = "short"
        else:
            return Signal(debug_info=f"Momentum: روند ضعيف")
        
        rsi3 = IND.safe(IND.rsi(df3m["close"], 14))
        _, _, m_hist = IND.macd(df3m["close"])
        macd_h = IND.safe(m_hist)
        vol_ratio = IND.safe(df1m["vol"] / df1m["vol"].rolling(20).mean())
        
        if trend == "long":
            pullback = (rsi3 < 45 and rsi3 > 30) and (macd_h > 0.0001)
        else:
            pullback = (rsi3 > 55 and rsi3 < 70) and (macd_h < -0.0001)
        
        if not pullback:
            return Signal(debug_info=f"Momentum: پولبک ضعيف")
        
        c1 = IND.safe(df1m["close"])
        ema9_1 = IND.safe(IND.ema(df1m["close"], 9))
        trigger = (c1 > ema9_1) if trend == "long" else (c1 < ema9_1)
        
        if not trigger or vol_ratio < 0.6:
            return Signal(debug_info=f"Momentum: تريگر ضعيف")
        
        atr15 = IND.safe(IND.atr(df15m["high"], df15m["low"], df15m["close"]))
        sl_dist = max(atr15 * 2.0, c1 * 0.025)
        sl_dist_tp = sl_dist * 1.5
        
        if trend == "long":
            sl = c1 - sl_dist
            tp = c1 + sl_dist_tp
            action = "buy"
        else:
            sl = c1 + sl_dist
            tp = c1 - sl_dist_tp
            action = "sell"
        
        conf = 55
        if adx15 > 30: conf += 8
        if adx15 > 40: conf += 8
        if abs(rsi3 - 50) > 8: conf += 8
        if abs(macd_h) > 0.001: conf += 7
        if vol_ratio > 1.2: conf += 5
        
        return Signal(
            action=action,
            strategy="MomentumScalp",
            confidence=min(conf, 90),
            reason=f"ADX={adx15:.0f} RSI3={rsi3:.0f}",
            sl=sl, tp=tp, entry_estimate=c1,
            debug_info=f"✅ Momentum {trend}"
        )

    def _mean_reversion_optimized(self, df1m, df5m, df15m, adx15, trend_str) -> Signal:
        bb_lo, bb_mid, bb_hi = IND.bbands(df5m["close"], 20, 2.0)
        c5 = IND.safe(df5m["close"])
        rsi5 = IND.safe(IND.rsi(df5m["close"], 14))
        
        if c5 <= 0:
            return Signal(debug_info="MeanRev: قيمت نامعتبر")
        
        if abs(trend_str) > 0.8:
            return Signal(debug_info=f"MeanRev: روند قوي")
        
        bb_lo_val = IND.safe(bb_lo)
        bb_hi_val = IND.safe(bb_hi)
        
        at_lower = c5 <= bb_lo_val and rsi5 < 35
        at_upper = c5 >= bb_hi_val and rsi5 > 65
        
        if not (at_lower or at_upper):
            return Signal(debug_info=f"MeanRev: باند نخورده")
        
        c1 = IND.safe(df1m["close"])
        rsi1 = IND.safe(IND.rsi(df1m["close"], 7))
        
        if c1 <= 0:
            return Signal(debug_info="MeanRev: C1=0")
        
        atr15 = IND.safe(IND.atr(df15m["high"], df15m["low"], df15m["close"]))
        sl_dist = max(atr15 * 2.0, c1 * 0.025)
        
        if at_lower and rsi1 > 30:
            sl = c1 - sl_dist
            tp = c1 + sl_dist * 1.5
            conf = 50
            if rsi5 < 30: conf += 10
            if rsi1 > 35: conf += 8
            vol_ratio = IND.safe(df1m["vol"] / df1m["vol"].rolling(20).mean())
            if vol_ratio > 1.1: conf += 5
            return Signal(
                action="buy",
                strategy="MeanReversion",
                confidence=min(conf, 85),
                reason=f"BB_Low RSI5={rsi5:.0f}",
                sl=sl, tp=tp, entry_estimate=c1,
                debug_info="✅ MeanRev BUY"
            )
        
        if at_upper and rsi1 < 70:
            sl = c1 + sl_dist
            tp = c1 - sl_dist * 1.5
            conf = 50
            if rsi5 > 70: conf += 10
            if rsi1 < 65: conf += 8
            vol_ratio = IND.safe(df1m["vol"] / df1m["vol"].rolling(20).mean())
            if vol_ratio > 1.1: conf += 5
            return Signal(
                action="sell",
                strategy="MeanReversion",
                confidence=min(conf, 85),
                reason=f"BB_High RSI5={rsi5:.0f}",
                sl=sl, tp=tp, entry_estimate=c1,
                debug_info="✅ MeanRev SELL"
            )
        return Signal(debug_info="MeanRev: تأیید نداد")

    def _breakout_optimized(self, df1m, df5m, df15m, adx15, trend_str) -> Signal:
        high5 = df5m["high"].rolling(20).max()
        low5 = df5m["low"].rolling(20).min()
        
        current_high = IND.safe(high5)
        current_low = IND.safe(low5)
        c5 = IND.safe(df5m["close"])
        prev_c5 = IND.safe(df5m["close"], -2)
        vol = IND.safe(df5m["vol"])
        avg_vol = IND.safe(df5m["vol"].rolling(20).mean())
        
        if c5 <= 0 or current_high <= 0 or current_low <= 0:
            return Signal(debug_info="Breakout: داده ناقص")
        
        atr15 = IND.safe(IND.atr(df15m["high"], df15m["low"], df15m["close"]))
        sl_dist = max(atr15 * 2.0, c5 * 0.025)
        rsi1 = IND.safe(IND.rsi(df1m["close"], 7))
        
        if c5 >= current_high and prev_c5 < current_high:
            if vol > avg_vol * 1.2 and rsi1 > 45:
                c1 = IND.safe(df1m["close"])
                if c1 <= 0: c1 = c5
                sl = c1 - sl_dist
                tp = c1 + sl_dist * 1.8
                conf = 60
                if adx15 > 25: conf += 8
                if vol > avg_vol * 1.5: conf += 8
                return Signal(
                    action="buy",
                    strategy="Breakout_High",
                    confidence=min(conf, 85),
                    reason=f"شکست سقف",
                    sl=sl, tp=tp, entry_estimate=c1,
                    debug_info="✅ Breakout UP"
                )
        
        if c5 <= current_low and prev_c5 > current_low:
            if vol > avg_vol * 1.2 and rsi1 < 55:
                c1 = IND.safe(df1m["close"])
                if c1 <= 0: c1 = c5
                sl = c1 + sl_dist
                tp = c1 - sl_dist * 1.8
                conf = 60
                if adx15 > 25: conf += 8
                if vol > avg_vol * 1.5: conf += 8
                return Signal(
                    action="sell",
                    strategy="Breakout_Low",
                    confidence=min(conf, 85),
                    reason=f"شکست کف",
                    sl=sl, tp=tp, entry_estimate=c1,
                    debug_info="✅ Breakout DOWN"
                )
        return Signal(debug_info="Breakout: شکستي رخ نداده")


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
                [{"text": "📊 داشبورد"}, {"text": "📈 پوزيشن‌ها"}],
                [{"text": "📜 تاريخچه"}, {"text": "⚙️ وضعيت"}],
                [{"text": "▶️ شروع"}, {"text": "⏹ توقف"}],
                [{"text": "🔍 ديباگ اسکن"}],
            ],
            "resize_keyboard": True,
        }

    def _poll_loop(self):
        while True:
            try:
                url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?offset={self.last_update_id + 1}&timeout=10"
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
        elif cmd in ("/positions", "📈 پوزيشن‌ها"):
            self._send_positions()
        elif cmd in ("/history", "📜 تاريخچه"):
            self._send_history()
        elif cmd in ("/status", "⚙️ وضعيت"):
            self._send_status()
        elif cmd in ("/debug", "🔍 ديباگ اسکن"):
            self._send_debug_scan()

    def _send_dashboard(self):
        stats = database.get_analytics()
        bal = EX.balance()
        equity = EX.total_equity()
        real_pos = EX.fetch_real_positions()
        db_count = len(self.engine._pos)
        status = "▶️ فعال" if self.engine.is_active else "⏹ متوقف"
        mode = "🧪 TESTNET" if TESTNET else "💰 MAINNET"

        msg = (
            f"📊 <b>داشبورد ربات v4.2</b>\n"
            f"{'═' * 28}\n"
            f"⚡ وضعيت: {status}\n"
            f"🌐 شبکه: {mode}\n"
            f"🔗 اتصال: {'✅' if EX.is_connected else '❌'}\n"
            f"{'═' * 28}\n"
            f"💰 موجودي: ${bal:,.2f}\n"
            f"💎 ارزش کل: ${equity:,.2f}\n"
            f"📈 PnL: {stats['total_pnl']:+,.2f}$\n"
            f"{'═' * 28}\n"
            f"📊 پوزيشن: {db_count}/{MAX_POS}\n"
            f"🎯 Win Rate: {stats['win_rate']}%\n"
            f"🛡️ DD: {self.engine.current_dd:.1f}%\n"
            f"{'═' * 28}\n"
            f"⏱️ اسکن: {SCAN_INTERVAL}s | Min SL: 2.5%\n"
            f"🔧 نسخه: v4.2 (نهایی)"
        )
        self.send(msg, reply_markup=self._keyboard())

    def _send_positions(self):
        real_pos = EX.fetch_real_positions()
        db_pos = list(self.engine._pos.values())
        if not real_pos and not db_pos:
            self.send("📭 <b>هيچ پوزيشني نيست</b>", reply_markup=self._keyboard())
            return
        msg = "🏦 <b>پوزيشن‌ها:</b>\n"
        if real_pos:
            for p in real_pos:
                msg += f"\n📌 {p['symbol']} ({p['side'].upper()}) | ورود: {p['entry']:.4f} | PnL: {p['unrealized_pnl']:+.2f}$\n"
        self.send(msg, reply_markup=self._keyboard())

    def _send_history(self):
        self.send("📜 تاريخچه در داشبورد وب قابل مشاهده است", reply_markup=self._keyboard())

    def _send_status(self):
        connected = EX.is_connected
        mode = "TESTNET" if TESTNET else "MAINNET"
        bal = EX.balance() if connected else 0
        msg = (
            f"⚙️ <b>وضعيت v4.2</b>\n"
            f"{'═' * 28}\n"
            f"🔗 صرافي: {'✅' if connected else '❌'}\n"
            f"🌐 شبکه: {mode}\n"
            f"💰 موجودي: ${bal:,.2f}\n"
            f"🎯 ريسک: {RISK_PCT}% | Min SL: 2.5%\n"
            f"📊 Max Pos: {MAX_POS} | Scan: {SCAN_INTERVAL}s\n"
            f"✅ Fallback به Binance فعال"
        )
        self.send(msg, reply_markup=self._keyboard())

    def _send_debug_scan(self):
        if not EX.is_connected:
            self.send("❌ صرافي متصل نيست", reply_markup=self._keyboard())
            return

        msg = "🔍 <b>ديباگ اسکن v4.2:</b>\n"
        bal = EX.balance()
        msg += f"💰 موجودي: ${bal:,.2f}\n"
        msg += f"📊 پوزيشن: {len(self.engine._pos)}/{MAX_POS}\n"
        msg += f"🎯 Min SL: 2.5%\n\n"

        active_syms = [p["symbol"] for p in self.engine._pos.values()]

        for sym in SYMBOLS:
            short_name = sym.split("/")[0]
            if sym in active_syms:
                msg += f"📌 <b>{short_name}</b>: پوزيشن باز\n"
                continue
            if len(self.engine._pos) >= MAX_POS:
                msg += f"⛔ <b>{short_name}</b>: ظرفيت پر\n"
                continue

            try:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(EX.fetch_multi_ohlcv, sym)
                    dfs = future.result(timeout=REQUEST_TIMEOUT)
                
                if not dfs:
                    msg += f"❌ <b>{short_name}</b>: داده دريافت نشد\n"
                    continue

                sig = STRATEGY.analyze(sym, dfs)
                if sig.action == "neutral":
                    msg += f"⏸️ <b>{short_name}</b>: {sig.debug_info[:50]}\n"
                else:
                    sl_pct = abs(sig.sl - sig.entry_estimate) / sig.entry_estimate * 100
                    msg += f"✅ <b>{short_name}</b>: {sig.action.upper()} ({sig.strategy}) Conf={sig.confidence}% SL={sl_pct:.2f}%\n"
            except concurrent.futures.TimeoutError:
                msg += f"⏰ <b>{short_name}</b>: Timeout\n"
            except Exception as e:
                msg += f"❌ <b>{short_name}</b>: {str(e)[:30]}\n"

        self.send(msg, reply_markup=self._keyboard())


# ============================================================================
# CORE ENGINE
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
                pos = {
                    "id": pid,
                    "symbol": rp["symbol"],
                    "side": rp["side"],
                    "entry": entry,
                    "fill_price": entry,
                    "qty": rp["qty"],
                    "filled_qty": rp["qty"],
                    "sl": entry * 0.975 if rp["side"] == "long" else entry * 1.025,
                    "tp": entry * 1.04 if rp["side"] == "long" else entry * 0.96,
                    "strategy": "Synced",
                    "conf": 100,
                    "is_partial": 0,
                    "exchange_order_id": "",
                    "sl_order_id": "",
                }
                self._pos[pid] = pos
                database.insert(pos)

    def run_loop(self):
        log.info("🚀 موتور v4.2 شروع شد")
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
        """🔥 اسکن گروهی با مدیریت بهتر"""
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
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(EX.fetch_multi_ohlcv, sym)
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
                time.sleep(1)
                
            except Exception as e:
                log.error(f"[{sym}] Scan Error: {e}")

    def _execute_signal(self, sym: str, sig: Signal, balance: float):
        short_name = sym.split("/")[0]
        
        sl_dist = abs(sig.entry_estimate - sig.sl)
        if sl_dist <= 0:
            sl_dist = sig.entry_estimate * 0.025
            if sig.action == "buy":
                sig.sl = sig.entry_estimate - sl_dist
            else:
                sig.sl = sig.entry_estimate + sl_dist
        
        risk_amount = balance * (RISK_PCT / 100.0) * 0.5
        qty = risk_amount / sl_dist
        
        max_notional = balance * 0.08
        if (qty * sig.entry_estimate) > max_notional:
            qty = max_notional / sig.entry_estimate
        
        min_qty = {
            "BTC": 0.001, "ETH": 0.01, "BNB": 0.01,
            "SOL": 0.1, "XRP": 1.0, "DOGE": 10.0,
            "ADA": 1.0, "AVAX": 0.1, "DOT": 0.1, "LINK": 0.1,
        }
        
        symbol_base = sym.split("/")[0]
        if symbol_base in min_qty and qty < min_qty[symbol_base]:
            qty = min_qty[symbol_base]
        
        if qty < 0.0001:
            log.warning(f"[{short_name}] حجم خیلی کم")
            return
        
        min_cost = 0.5
        if (qty * sig.entry_estimate) < min_cost:
            log.warning(f"[{short_name}] ارزش کمتر از {min_cost}$")
            return
        
        log.info(f"[{short_name}] حجم: {qty:.6f} | نوتینال: ${qty * sig.entry_estimate:.2f}")
        
        side = "buy" if sig.action == "buy" else "sell"
        order_result = EX.place_order(sym, side, qty, is_close=False)
        
        if not order_result:
            log.warning(f"❌ [{short_name}] سفارش اجرا نشد")
            if self.tg:
                self.tg.send(f"❌ سفارش رد شد\n{sym}\nحجم: {qty:.6f}")
            return
        
        fill_price = order_result["fill_price"]
        filled_qty = order_result["filled_qty"]
        
        sl_ratio = sl_dist / sig.entry_estimate
        pos_side = "long" if sig.action == "buy" else "short"
        
        if pos_side == "long":
            real_sl = fill_price - (fill_price * sl_ratio)
            real_tp = fill_price + (fill_price * sl_ratio * 1.5)
        else:
            real_sl = fill_price + (fill_price * sl_ratio)
            real_tp = fill_price - (fill_price * sl_ratio * 1.5)
        
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
        }
        
        with self._lock:
            self._pos[pid] = pos
        database.insert(pos)
        
        sl_pct = abs(real_sl - fill_price) / fill_price * 100
        log.info(f"✅ [{short_name}] پوزيشن باز | ورود: {fill_price:.4f} | SL: {sl_pct:.2f}%")
        
        if self.tg:
            self.tg.send(
                f"🚀 <b>پوزيشن جدید ({sig.strategy})</b>\n"
                f"{sym} | {pos_side.upper()}\n"
                f"ورود: {fill_price:.4f} | SL: {sl_pct:.2f}%\n"
                f"اطمينان: {sig.confidence}%"
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
        <title>Quant Bot v4.2</title>
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
        <h1>🤖 Master-AI Quant Bot v4.2</h1>
        <span class="badge">✅ نهایی - Fallback فعال</span>
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
            <h3>🔧 تنظیمات</h3>
            <p>ریسک: {RISK_PCT}% | Min SL: 2.5%</p>
            <p>Max Pos: {MAX_POS} | Scan: {SCAN_INTERVAL}s</p>
            <p style="color: #3fb950;">✅ Fallback به Binance</p>
        </div>
    </body>
    </html>
    """


@app.route("/health")
def health():
    return {
        "status": "ok",
        "version": "4.2",
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
                future = executor.submit(EX.fetch_multi_ohlcv, sym)
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
    log.info("  🤖 Master-AI Quant Bot v4.2 (FINAL)")
    log.info("  ✅ Fallback به Binance فعال")
    log.info("  ✅ اسکن گروهی (هر بار %d نماد)", SCAN_BATCH_SIZE)
    log.info("  🌐 Mode: %s", "TESTNET" if TESTNET else "MAINNET")
    log.info("  🔗 Connected: %s", EX.is_connected)
    log.info("  🎯 Risk: %.1f%% | Min SL: 2.5%%", RISK_PCT)
    log.info("  📊 Max Pos: %d | Scan: %ds", MAX_POS, SCAN_INTERVAL)
    log.info("=" * 60)

    if not EX.is_connected:
        log.critical("❌ اتصال به صرافي برقرار نشد!")

    engine_instance = Engine()
    tg = TelegramHandler(engine_instance)
    engine_instance.tg = tg

    if TG_TOKEN and TG_CHAT:
        tg.send(
            f"🚀 <b>ربات v4.2 (نهایی) شروع شد</b>\n"
            f"{'═' * 28}\n"
            f"✅ Fallback به Binance فعال\n"
            f"🛡️ Min SL: 2.5% | ریسک: {RISK_PCT}%\n"
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