#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Quant Bot v3.1 - OPTIMIZED VERSION
نسخه بهینه‌شده با حفظ تمام استراتژی‌ها
"""

import json
import logging
import os
import sys
import threading
import time
import uuid
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
log = logging.getLogger("MasterQuant_v3.1")


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

RISK_PCT = Cfg.f("RISK_PER_TRADE", 1.0)
MAX_DD = Cfg.f("MAX_DRAWDOWN", 8.0)
MAX_POS = Cfg.i("MAX_POSITIONS", 5)
LEVERAGE = Cfg.i("LEVERAGE", 5)
TESTNET = Cfg.b("PHEMEX_TESTNET", True)
PORT = Cfg.i("PORT", 10000)
SCAN_INTERVAL = Cfg.i("SCAN_INTERVAL", 12)
MIN_CONFIDENCE = Cfg.i("MIN_CONFIDENCE", 65)


# ============================================================================
# TECHNICAL INDICATORS (با اضافات جدید)
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

    # 🔥 استراتژی جدید: فیلتر قدرت روند
    @staticmethod
    def trend_strength(close: pd.Series, fast: int = 10, slow: int = 30) -> float:
        """نسبت EMA‌ها برای اندازه‌گیری قدرت روند"""
        ema_fast = Indicators.ema(close, fast)
        ema_slow = Indicators.ema(close, slow)
        ratio = (ema_fast / ema_slow - 1) * 100
        return float(ratio.iloc[-1]) if len(ratio) > 0 else 0.0


IND = Indicators()


# ============================================================================
# DATABASE (بدون تغییر)
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
        self._path = "bot_v3.db"
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
# EXCHANGE ENGINE (بدون تغییر)
# ============================================================================
class Exchange:

    def __init__(self):
        self._ex: Optional[ccxt.phemex] = None
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
            })
            if TESTNET:
                self._ex.set_sandbox_mode(True)
                log.warning("⚠️  حالت TESTNET فعال است!")

            self._ex.load_markets()
            self._cache_market_info()
            self._set_leverage_all()
            self._connected = True

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
                         limit: int = 100) -> Optional[pd.DataFrame]:
        if not self.is_connected:
            return None
        for attempt in range(3):
            try:
                raw = self._ex.fetch_ohlcv(sym, tf, limit=limit)
                if not raw:
                    return None
                df = pd.DataFrame(
                    raw, columns=["ts", "open", "high", "low", "close", "vol"]
                )
                df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
                return df
            except Exception as e:
                if attempt == 2:
                    log.error("OHLCV Error [%s %s]: %s", sym, tf, e)
                time.sleep(0.5)
        return None

    def fetch_multi_ohlcv(self, sym: str) -> Dict[str, pd.DataFrame]:
        result = {}
        for tf in ["1m", "3m", "5m", "15m"]:
            df = self.fetch_ohlcv_safe(sym, tf, limit=120)  # 🔥 افزایش به ۱۲۰
            if df is None or len(df) < 50:  # 🔥 افزایش حداقل
                log.debug("⚠️  داده ناکافي %s %s (len=%d)",
                          sym, tf, len(df) if df is not None else 0)
                return {}
            result[tf] = df
            time.sleep(0.15)
        return result

    def get_current_price(self, sym: str) -> Optional[float]:
        if not self.is_connected:
            return None
        try:
            ticker = self._ex.fetch_ticker(sym)
            return float(ticker.get("last", 0))
        except Exception as e:
            log.error("Ticker Error [%s]: %s", sym, e)
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
            return (
                False, 0,
                f"حجم {formatted_qty} کمتر از حداقل {min_amount}",
            )

        cost = formatted_qty * price / LEVERAGE
        if cost < min_cost:
            return (
                False, 0,
                f"ارزش {cost:.2f}$ کمتر از حداقل {min_cost}$",
            )

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

            valid, fmt_qty, msg = self.validate_order_size(
                sym, qty, current_price
            )
            if not valid:
                log.warning("⚠️  سفارش نامعتبر [%s]: %s", sym, msg)
                return None

            params = {}
            if is_close:
                params["reduceOnly"] = True

            log.info(
                "📤 ارسال سفارش | %s %s | حجم: %s | قيمت تقريبي: %s | close=%s",
                side.upper(), sym, fmt_qty, current_price, is_close,
            )

            if side.lower() == "buy":
                result = self._ex.create_market_buy_order(
                    sym, fmt_qty, params=params
                )
            else:
                result = self._ex.create_market_sell_order(
                    sym, fmt_qty, params=params
                )

            fill_price = float(
                result.get("average")
                or result.get("price")
                or current_price
            )
            filled_qty = float(
                result.get("filled") or result.get("amount") or fmt_qty
            )

            log.info(
                "✅ سفارش اجرا شد | %s %s | حجم: %s | قيمت: %s | ID: %s",
                side.upper(), sym, filled_qty, fill_price,
                result.get("id"),
            )

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

            log.info(
                "🛑 SL ثبت شد | %s | قيمت: %s | ID: %s",
                sym, fmt_price, result.get("id"),
            )
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

    def fetch_my_trades(self, sym: str, limit: int = 5) -> List[Dict]:
        if not self.is_connected:
            return []
        try:
            trades = self._ex.fetch_my_trades(sym, limit=limit)
            result = []
            for t in trades:
                result.append({
                    "symbol": t.get("symbol"),
                    "side": t.get("side"),
                    "price": t.get("price"),
                    "amount": t.get("amount"),
                    "cost": t.get("cost"),
                    "time": datetime.fromtimestamp(
                        t.get("timestamp", 0) / 1000
                    ).strftime("%m-%d %H:%M"),
                })
            return result
        except Exception:
            return []


EX = Exchange()


# ============================================================================
# STRATEGY ENGINE - 🔥 کاملاً اصلاح‌شده
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

    def analyze(self, sym: str,
                dfs: Dict[str, pd.DataFrame]) -> Signal:
        required = ["1m", "3m", "5m", "15m"]
        if not dfs or any(
            tf not in dfs or len(dfs[tf]) < 50 for tf in required
        ):
            return Signal(debug_info="داده ناکافي")

        df1m = dfs["1m"]
        df3m = dfs["3m"]
        df5m = dfs["5m"]
        df15m = dfs["15m"]

        # 🔥 محاسبه اندیکاتورهای اصلی
        adx15 = IND.safe(
            IND.adx(df15m["high"], df15m["low"], df15m["close"])
        )
        ema20_15 = IND.safe(IND.ema(df15m["close"], 20))
        ema50_15 = IND.safe(IND.ema(df15m["close"], 50))
        price15 = IND.safe(df15m["close"])
        rsi15 = IND.safe(IND.rsi(df15m["close"], 14))
        
        # 🔥 قدرت روند
        trend_str = IND.trend_strength(df15m["close"])

        # =========================================================
        # 1. استراتژی Momentum Scalp (با اصلاحات کامل)
        # =========================================================
        if adx15 > 20:
            sig = self._momentum_scalp(
                sym, df1m, df3m, price15, ema20_15, ema50_15, 
                adx15, trend_str, df15m
            )
            if sig.action != "neutral":
                return sig

        # =========================================================
        # 2. استراتژی Mean Reversion (با اصلاحات کامل)
        # =========================================================
        if adx15 <= 28:  # 🔥 افزایش آستانه رنج
            sig = self._mean_reversion(
                sym, df1m, df5m, df15m, adx15, trend_str
            )
            if sig.action != "neutral":
                return sig

        # =========================================================
        # 3. استراتژی Breakout (با اصلاحات کامل)
        # =========================================================
        sig = self._breakout_strategy(
            sym, df1m, df5m, df15m, adx15, trend_str
        )
        if sig.action != "neutral":
            return sig

        return Signal(
            debug_info=f"ADX15={adx15:.1f} RSI15={rsi15:.1f} "
                       f"Trend={trend_str:.2f} - شرايط ورود برقرار نيست"
        )

    def _momentum_scalp(self, sym, df1m, df3m, price15,
                        ema20_15, ema50_15, adx15, trend_str, df15m) -> Signal:
        """🔥 اصلاح‌شده: فیلترهای قوی‌تر، ATR از ۱۵ دقیقه"""
        
        # تعیین روند با فیلتر قدرت
        if price15 > ema20_15 and ema20_15 > ema50_15 and trend_str > 0.3:
            trend = "long"
        elif price15 < ema20_15 and ema20_15 < ema50_15 and trend_str < -0.3:
            trend = "short"
        else:
            return Signal(
                debug_info=f"Momentum: روند ضعيف "
                           f"Trend={trend_str:.2f}"
            )

        # 🔥 پولبک با فیلترهای قوی‌تر
        rsi3 = IND.safe(IND.rsi(df3m["close"], 14))
        _, _, m_hist = IND.macd(df3m["close"])
        macd_h = IND.safe(m_hist)

        # 🔥 فیلتر حجم (جدید)
        vol_ratio = IND.safe(df1m["vol"] / df1m["vol"].rolling(20).mean())
        
        if trend == "long":
            # 🔥 محدودتر: RSI بین ۳۰-۴۵
            pullback = (rsi3 < 45 and rsi3 > 30) and (macd_h > 0.0001)
        else:
            # 🔥 محدودتر: RSI بین ۵۵-۷۰
            pullback = (rsi3 > 55 and rsi3 < 70) and (macd_h < -0.0001)

        if not pullback:
            return Signal(
                debug_info=f"Momentum: پولبک ضعيف "
                           f"RSI3={rsi3:.1f} MACD_H={macd_h:.6f}"
            )

        # 🔥 تأیید ۱ دقیقه با حجم
        c1 = IND.safe(df1m["close"])
        ema9_1 = IND.safe(IND.ema(df1m["close"], 9))
        trigger = (c1 > ema9_1) if trend == "long" else (c1 < ema9_1)

        if not trigger or vol_ratio < 0.7:  # 🔥 فیلتر حجم
            return Signal(
                debug_info=f"Momentum: تريگر ضعيف ou حجم کم "
                           f"VolRatio={vol_ratio:.2f}"
            )

        # 🔥 ATR از ۱۵ دقیقه (کمتر نویز)
        atr15 = IND.safe(
            IND.atr(df15m["high"], df15m["low"], df15m["close"])
        )
        # 🔥 حداقل فاصله SL = ۰.۸٪ از قیمت
        min_sl_dist = c1 * 0.008
        sl_dist = max(atr15 * 1.5, min_sl_dist)  # 🔥 ضریب افزایش یافته

        if trend == "long":
            sl = c1 - sl_dist
            tp = c1 + (sl_dist * 1.8)  # 🔥 R:R = 1.8
            action = "buy"
        else:
            sl = c1 + sl_dist
            tp = c1 - (sl_dist * 1.8)
            action = "sell"

        # 🔥 محاسبه اطمینان با فیلترهای جدید
        conf = 55
        if adx15 > 30:
            conf += 10
        if adx15 > 40:
            conf += 8
        if abs(rsi3 - 50) > 8:
            conf += 8
        if abs(macd_h) > 0.001:
            conf += 7
        if vol_ratio > 1.2:
            conf += 5
        if abs(trend_str) > 0.5:
            conf += 5

        return Signal(
            action=action,
            strategy="MomentumScalp",
            confidence=min(conf, 92),
            reason=f"ADX={adx15:.0f} RSI3={rsi3:.0f} Vol={vol_ratio:.2f}",
            sl=sl, tp=tp, entry_estimate=c1,
            debug_info=f"✅ Momentum {trend} | SL={sl_dist/c1*100:.2f}%",
        )

    def _mean_reversion(self, sym, df1m, df5m, df15m, adx15, trend_str) -> Signal:
        """🔥 اصلاح‌شده: فیلتر ADX و ضریب ATR بیشتر"""
        
        bb_lo, bb_mid, bb_hi = IND.bbands(df5m["close"], 20, 2.0)
        c5 = IND.safe(df5m["close"])
        rsi5 = IND.safe(IND.rsi(df5m["close"], 14))

        if c5 <= 0:
            return Signal(debug_info="MeanRev: قيمت نامعتبر")

        bb_lo_val = IND.safe(bb_lo)
        bb_hi_val = IND.safe(bb_hi)

        # 🔥 فقط در رنج کامل
        if abs(trend_str) > 0.8:
            return Signal(
                debug_info=f"MeanRev: روند قوي Trend={trend_str:.2f}"
            )

        at_lower = c5 <= bb_lo_val and rsi5 < 32  # 🔥 سخت‌تر
        at_upper = c5 >= bb_hi_val and rsi5 > 68  # 🔥 سخت‌تر

        if not (at_lower or at_upper):
            return Signal(
                debug_info=f"MeanRev: باند نخورده "
                           f"RSI5={rsi5:.1f}"
            )

        c1 = IND.safe(df1m["close"])
        rsi1 = IND.safe(IND.rsi(df1m["close"], 7))

        if c1 <= 0:
            return Signal(debug_info="MeanRev: C1=0")

        # 🔥 ATR از ۱۵ دقیقه
        atr15 = IND.safe(
            IND.atr(df15m["high"], df15m["low"], df15m["close"])
        )
        min_sl_dist = c1 * 0.008
        sl_dist = max(atr15 * 2.0, min_sl_dist)  # 🔥 ضریب ۲

        if at_lower and rsi1 > 28:  # 🔥 سخت‌تر
            sl = c1 - sl_dist
            tp = c1 + (sl_dist * 1.5)
            conf = 50
            if rsi5 < 28:
                conf += 12
            if rsi1 > 35:
                conf += 8
            if vol_ratio := IND.safe(df1m["vol"] / df1m["vol"].rolling(20).mean()) > 1.1:
                conf += 5
            return Signal(
                action="buy",
                strategy="MeanReversion",
                confidence=min(conf, 88),
                reason=f"BB_Low RSI5={rsi5:.0f} RSI1={rsi1:.0f}",
                sl=sl, tp=tp, entry_estimate=c1,
                debug_info="✅ MeanRev BUY",
            )

        if at_upper and rsi1 < 72:  # 🔥 سخت‌تر
            sl = c1 + sl_dist
            tp = c1 - (sl_dist * 1.5)
            conf = 50
            if rsi5 > 72:
                conf += 12
            if rsi1 < 65:
                conf += 8
            if vol_ratio := IND.safe(df1m["vol"] / df1m["vol"].rolling(20).mean()) > 1.1:
                conf += 5
            return Signal(
                action="sell",
                strategy="MeanReversion",
                confidence=min(conf, 88),
                reason=f"BB_High RSI5={rsi5:.0f} RSI1={rsi1:.0f}",
                sl=sl, tp=tp, entry_estimate=c1,
                debug_info="✅ MeanRev SELL",
            )

        return Signal(debug_info="MeanRev: تأیید ۱دقيقه نداد")

    def _breakout_strategy(self, sym, df1m, df5m, df15m, adx15, trend_str) -> Signal:
        """🔥 اصلاح‌شده: تأیید با RSI و افزایش ضریب ATR"""
        
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

        # 🔥 ATR از ۱۵ دقیقه
        atr15 = IND.safe(
            IND.atr(df15m["high"], df15m["low"], df15m["close"])
        )
        min_sl_dist = c5 * 0.01
        sl_dist = max(atr15 * 2.5, min_sl_dist)  # 🔥 ضریب ۲.۵

        # 🔥 تأیید RSI ۱ دقیقه
        rsi1 = IND.safe(IND.rsi(df1m["close"], 7))

        # شکست بالا با تأیید
        if c5 >= current_high and prev_c5 < current_high:
            if vol > avg_vol * 1.3 and rsi1 > 45:  # 🔥 فیلتر RSI
                c1 = IND.safe(df1m["close"])
                if c1 <= 0:
                    c1 = c5
                sl = c1 - sl_dist
                tp = c1 + (sl_dist * 2.0)
                conf = 60
                if adx15 > 25:
                    conf += 8
                if vol > avg_vol * 1.6:
                    conf += 8
                if abs(trend_str) > 0.3:
                    conf += 5
                return Signal(
                    action="buy",
                    strategy="Breakout_High",
                    confidence=min(conf, 90),
                    reason=f"شکست سقف 🚀 Vol={vol/avg_vol:.1f}x",
                    sl=sl, tp=tp, entry_estimate=c1,
                    debug_info="✅ Breakout UP",
                )

        # شکست پایین با تأیید
        if c5 <= current_low and prev_c5 > current_low:
            if vol > avg_vol * 1.3 and rsi1 < 55:  # 🔥 فیلتر RSI
                c1 = IND.safe(df1m["close"])
                if c1 <= 0:
                    c1 = c5
                sl = c1 + sl_dist
                tp = c1 - (sl_dist * 2.0)
                conf = 60
                if adx15 > 25:
                    conf += 8
                if vol > avg_vol * 1.6:
                    conf += 8
                if abs(trend_str) > 0.3:
                    conf += 5
                return Signal(
                    action="sell",
                    strategy="Breakout_Low",
                    confidence=min(conf, 90),
                    reason=f"شکست کف 📉 Vol={vol/avg_vol:.1f}x",
                    sl=sl, tp=tp, entry_estimate=c1,
                    debug_info="✅ Breakout DOWN",
                )

        return Signal(debug_info="Breakout: شکستي رخ نداده")


STRATEGY = StrategyEngine()


# ============================================================================
# TELEGRAM HANDLER (بدون تغییر)
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
                [
                    {"text": "📊 داشبورد"},
                    {"text": "📈 پوزيشن‌ها"},
                ],
                [
                    {"text": "📜 تاريخچه"},
                    {"text": "⚙️ وضعيت"},
                ],
                [
                    {"text": "▶️ شروع"},
                    {"text": "⏹ توقف"},
                ],
                [
                    {"text": "🔍 ديباگ اسکن"},
                ],
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
                        txt = (
                            upd.get("message", {}).get("text", "").strip()
                        )
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
            f"📊 <b>داشبورد ربات v3.1 (بهینه)</b>\n"
            f"{'═' * 25}\n"
            f"⚡ وضعيت: {status}\n"
            f"🌐 شبکه: {mode}\n"
            f"🔗 اتصال: {'✅' if EX.is_connected else '❌'}\n"
            f"{'═' * 25}\n"
            f"💰 موجودي آزاد: ${bal:,.2f}\n"
            f"💎 ارزش کل: ${equity:,.2f}\n"
            f"📈 سود/زيان: {stats['total_pnl']:+,.2f}$\n"
            f"{'═' * 25}\n"
            f"📊 پوزيشن DB: {db_count}/{MAX_POS}\n"
            f"🏦 پوزيشن صرافي: {len(real_pos)}\n"
            f"{'═' * 25}\n"
            f"📊 معاملات: {stats['total_trades']}\n"
            f"✅ برد: {stats['wins_count']} | ❌ باخت: {stats['losses_count']}\n"
            f"🎯 وين‌ريت: {stats['win_rate']}%\n"
            f"⚡ PF: {stats['profit_factor']}\n"
            f"🛡️ افت: {self.engine.current_dd:.1f}% / {MAX_DD}%\n"
            f"{'═' * 25}\n"
            f"⏱️ فاصله اسکن: {SCAN_INTERVAL}s\n"
            f"🎯 حداقل اطمينان: {MIN_CONFIDENCE}%\n"
            f"📊 نمادها: {len(SYMBOLS)}\n"
            f"🔧 نسخه: v3.1 (بهینه)"
        )
        self.send(msg, reply_markup=self._keyboard())

    def _send_positions(self):
        real_pos = EX.fetch_real_positions()
        db_pos = list(self.engine._pos.values())

        if not real_pos and not db_pos:
            self.send(
                "📭 <b>هيچ پوزيشني نيست</b>",
                reply_markup=self._keyboard(),
            )
            return

        msg = "🏦 <b>پوزيشن‌هاي واقعي صرافي:</b>\n"
        if real_pos:
            for p in real_pos:
                msg += (
                    f"\n📌 <b>{p['symbol']}</b> ({p['side'].upper()})\n"
                    f"   ورود: {p['entry']:.4f}\n"
                    f"   حجم: {p['qty']}\n"
                    f"   PnL: {p['unrealized_pnl']:+.2f}$\n"
                    f"   ليکوييد: {p['liquidation']:.2f}\n"
                )
        else:
            msg += "❌ هيچ پوزيشني در صرافي نيست\n"

        if db_pos:
            msg += f"\n{'═' * 25}\n📂 <b>DB ({len(db_pos)}):</b>\n"
            for p in db_pos:
                msg += (
                    f"📌 {p['symbol']} ({p['side']}) "
                    f"@ {p['entry']:.4f} "
                    f"SL={p['sl']:.4f} TP={p['tp']:.4f}\n"
                )

        real_syms = {p["symbol"] for p in real_pos}
        db_syms = {p["symbol"] for p in db_pos}
        if real_syms != db_syms:
            msg += (
                f"\n⚠️ <b>ناهماهنگي!</b>\n"
                f"فقط صرافي: {real_syms - db_syms or 'ندارد'}\n"
                f"فقط DB: {db_syms - real_syms or 'ندارد'}\n"
            )
        else:
            msg += "\n✅ همگام‌سازي صحيح\n"

        self.send(msg, reply_markup=self._keyboard())

    def _send_history(self):
        if not EX.is_connected:
            self.send("❌ صرافي متصل نيست",
                       reply_markup=self._keyboard())
            return
        all_trades = []
        for sym in SYMBOLS[:5]:
            trades = EX.fetch_my_trades(sym, limit=3)
            all_trades.extend(trades)

        if not all_trades:
            self.send(
                "📭 <b>تاريخچه‌اي يافت نشد</b>",
                reply_markup=self._keyboard(),
            )
            return

        msg = "📜 <b>تاريخچه واقعي:</b>\n"
        for t in all_trades[:10]:
            msg += (
                f"\n{t['symbol']} | {t['side'].upper()}\n"
                f"   قيمت: {t['price']} | حجم: {t['amount']}\n"
                f"   هزينه: ${t.get('cost', 0):.2f} | {t['time']}\n"
            )
        self.send(msg, reply_markup=self._keyboard())

    def _send_status(self):
        connected = EX.is_connected
        mode = "TESTNET" if TESTNET else "MAINNET"
        bal = EX.balance() if connected else 0

        msg = (
            f"⚙️ <b>وضعيت سيستم v3.1</b>\n"
            f"{'═' * 25}\n"
            f"🔗 صرافي: {'✅ متصل' if connected else '❌ قطع'}\n"
            f"🌐 شبکه: {mode}\n"
            f"💰 موجودي: ${bal:,.2f}\n"
            f"🤖 ربات: {'▶️ فعال' if self.engine.is_active else '⏹ متوقف'}\n"
            f"⚡ لوريج: {LEVERAGE}x\n"
            f"🎯 ريسک: {RISK_PCT}%\n"
            f"📊 Max Pos: {MAX_POS}\n"
            f"🎯 Min Conf: {MIN_CONFIDENCE}%\n"
            f"⏱️ Scan: هر {SCAN_INTERVAL}s\n"
            f"📊 نمادها: {', '.join(s.split('/')[0] for s in SYMBOLS)}\n"
            f"{'═' * 25}\n"
            f"🔧 اصلاحات v3.1:\n"
            f"• ATR ۱۵ دقيقه براي SL\n"
            f"• حداقل SL ۰.۸٪\n"
            f"• فیلتر حجم و RSI\n"
            f"• R:R بهبود یافته"
        )
        self.send(msg, reply_markup=self._keyboard())

    def _send_debug_scan(self):
        if not EX.is_connected:
            self.send("❌ صرافي متصل نيست",
                       reply_markup=self._keyboard())
            return

        msg = "🔍 <b>ديباگ اسکن نمادها (v3.1):</b>\n"
        bal = EX.balance()
        msg += f"💰 موجودي: ${bal:,.2f}\n"
        msg += f"📊 پوزيشن فعال: {len(self.engine._pos)}/{MAX_POS}\n\n"

        active_syms = [p["symbol"] for p in self.engine._pos.values()]

        for sym in SYMBOLS:
            short_name = sym.split("/")[0]

            if sym in active_syms:
                msg += f"📌 <b>{short_name}</b>: پوزيشن باز دارد\n"
                continue

            if len(self.engine._pos) >= MAX_POS:
                msg += f"⛔ <b>{short_name}</b>: ظرفيت پر\n"
                continue

            try:
                dfs = EX.fetch_multi_ohlcv(sym)
                if not dfs:
                    msg += f"❌ <b>{short_name}</b>: داده دريافت نشد\n"
                    continue

                sig = STRATEGY.analyze(sym, dfs)
                if sig.action == "neutral":
                    msg += f"⏸️ <b>{short_name}</b>: {sig.debug_info[:70]}\n"
                else:
                    sl_pct = abs(sig.sl - sig.entry_estimate) / sig.entry_estimate * 100
                    msg += (
                        f"✅ <b>{short_name}</b>: "
                        f"{sig.action.upper()} "
                        f"({sig.strategy}) "
                        f"Conf={sig.confidence}% "
                        f"SL={sl_pct:.2f}% "
                    )
                    if sig.confidence < MIN_CONFIDENCE:
                        msg += f"⚠️ کمتر از {MIN_CONFIDENCE}%\n"
                    else:
                        msg += "🚀 آماده ورود\n"
            except Exception as e:
                msg += f"❌ <b>{short_name}</b>: خطا: {str(e)[:40]}\n"

        self.send(msg, reply_markup=self._keyboard())


# ============================================================================
# CORE ENGINE - 🔥 اصلاح قیمت ورود
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
            already = any(
                p["symbol"] == rp["symbol"] for p in self._pos.values()
            )
            if not already:
                pid = f"sync_{uuid.uuid4().hex[:6]}"
                entry = rp["entry"]
                atr_est = entry * 0.008
                pos = {
                    "id": pid,
                    "symbol": rp["symbol"],
                    "side": rp["side"],
                    "entry": entry,
                    "fill_price": entry,
                    "qty": rp["qty"],
                    "filled_qty": rp["qty"],
                    "sl": (
                        entry - 1.5 * atr_est
                        if rp["side"] == "long"
                        else entry + 1.5 * atr_est
                    ),
                    "tp": (
                        entry + 2.0 * atr_est
                        if rp["side"] == "long"
                        else entry - 2.0 * atr_est
                    ),
                    "strategy": "Synced",
                    "conf": 100,
                    "is_partial": 0,
                    "exchange_order_id": "",
                    "sl_order_id": "",
                }
                self._pos[pid] = pos
                database.insert(pos)

    def run_loop(self):
        log.info("🚀  موتور اصلي شروع شد")

        while True:
            try:
                self._cycle_count += 1

                if not EX.is_connected:
                    log.warning("⚠️  صرافي متصل نيست...")
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
                    else:
                        if self._cycle_count % 50 == 0:
                            log.info(
                                "📊  ظرفيت پر: %d/%d",
                                pos_count, MAX_POS,
                            )

                time.sleep(SCAN_INTERVAL)

            except Exception as e:
                log.error("❌ Engine Error: %s", e)
                time.sleep(SCAN_INTERVAL)

    def _check_drawdown(self, equity: float):
        if self.peak_balance is None or equity > self.peak_balance:
            self.peak_balance = equity
        if self.peak_balance and self.peak_balance > 0:
            self.current_dd = (
                (self.peak_balance - equity) / self.peak_balance * 100
            )
            if self.current_dd >= MAX_DD and not self.is_dd_halted:
                self.is_dd_halted = True
                log.critical(
                    "🛑 DRAWDOWN LIMIT! DD=%.1f%%", self.current_dd
                )
                if self.tg:
                    self.tg.send(
                        f"🛑 <b>هشدار افت حساب!</b>\n"
                        f"افت: {self.current_dd:.1f}% / {MAX_DD}%\n"
                        f"معاملات جديد متوقف شد!"
                    )
            elif self.current_dd < MAX_DD * 0.7 and self.is_dd_halted:
                self.is_dd_halted = False
                log.info("✅ افت حساب بهبود يافت")

    def _check_sync(self):
        real = EX.fetch_real_positions()
        real_syms = {p["symbol"] for p in real}

        with self._lock:
            db_syms = {p["symbol"] for p in self._pos.values()}

        orphans = db_syms - real_syms
        for pid, pos in list(self._pos.items()):
            if pos["symbol"] in orphans:
                log.warning(
                    "⚠️  Orphan: %s در DB هست ولي در صرافي نيست",
                    pos["symbol"],
                )
                price = EX.get_current_price(pos["symbol"]) or pos["entry"]
                self._close_position(pid, pos, price, "Sync_Orphan")

        for rp in real:
            if rp["symbol"] not in db_syms:
                log.info(
                    "📌 پوزيشن جديد در صرافي: %s - sync مي‌شود",
                    rp["symbol"],
                )
                pid = f"sync_{uuid.uuid4().hex[:6]}"
                entry = rp["entry"]
                atr_est = entry * 0.008
                pos = {
                    "id": pid,
                    "symbol": rp["symbol"],
                    "side": rp["side"],
                    "entry": entry,
                    "fill_price": entry,
                    "qty": rp["qty"],
                    "filled_qty": rp["qty"],
                    "sl": (
                        entry - 1.5 * atr_est
                        if rp["side"] == "long"
                        else entry + 1.5 * atr_est
                    ),
                    "tp": (
                        entry + 2.0 * atr_est
                        if rp["side"] == "long"
                        else entry - 2.0 * atr_est
                    ),
                    "strategy": "Synced",
                    "conf": 100,
                    "is_partial": 0,
                    "exchange_order_id": "",
                    "sl_order_id": "",
                }
                with self._lock:
                    self._pos[pid] = pos
                database.insert(pos)

    def _scan_markets(self, balance: float):
        for sym in SYMBOLS:
            try:
                with self._lock:
                    if len(self._pos) >= MAX_POS:
                        return
                    already_open = any(
                        p["symbol"] == sym for p in self._pos.values()
                    )

                if already_open:
                    continue

                dfs = EX.fetch_multi_ohlcv(sym)
                if not dfs:
                    continue

                signal = STRATEGY.analyze(sym, dfs)
                short_name = sym.split("/")[0]

                if signal.action == "neutral":
                    log.debug(
                        "[%s] neutral: %s",
                        short_name, signal.debug_info[:80],
                    )
                    continue

                if signal.confidence < MIN_CONFIDENCE:
                    log.info(
                        "[%s] سيگنال %s (%s) اطمينان=%d%% < %d%% - رد شد",
                        short_name, signal.action, signal.strategy,
                        signal.confidence, MIN_CONFIDENCE,
                    )
                    continue

                log.info(
                    "✅ [%s] سيگنال: %s | %s | اطمينان: %d%% | دليل: %s",
                    short_name, signal.action.upper(),
                    signal.strategy, signal.confidence, signal.reason,
                )
                self._execute_signal(sym, signal, balance)

            except Exception as e:
                log.error("[%s] Scan Error: %s", sym, e)

    def _execute_signal(self, sym: str, sig: Signal, balance: float):
        """🔥 اصلاح‌شده: SL بر اساس قیمت واقعی Fill"""
        short_name = sym.split("/")[0]

        # محاسبه حجم با فاصله SL از سیگنال
        sl_dist_from_signal = abs(sig.entry_estimate - sig.sl)
        if sl_dist_from_signal <= 0:
            sl_dist_from_signal = sig.entry_estimate * 0.008

        risk_amount = balance * (RISK_PCT / 100.0)
        qty = risk_amount / sl_dist_from_signal

        max_notional = balance * 0.12
        if (qty * sig.entry_estimate) > max_notional:
            qty = max_notional / sig.entry_estimate

        if qty <= 0:
            log.warning("[%s] حجم صفر - رد شد", short_name)
            return

        log.info(
            "[%s] محاسبه حجم: risk=$%.2f sl_dist=%.4f qty=%.6f",
            short_name, risk_amount, sl_dist_from_signal, qty,
        )

        side = "buy" if sig.action == "buy" else "sell"
        order_result = EX.place_order(sym, side, qty, is_close=False)

        if not order_result:
            log.warning("❌  [%s] سفارش اجرا نشد", short_name)
            if self.tg:
                self.tg.send(
                    f"❌ <b>سفارش رد شد</b>\n"
                    f"نماد: {sym}\n"
                    f"جهت: {side}\n"
                    f"حجم: {qty:.6f}"
                )
            return

        fill_price = order_result["fill_price"]
        filled_qty = order_result["filled_qty"]

        # 🔥 اصلاح: محاسبه SL بر اساس قیمت واقعی Fill
        # نسبت فاصله SL به قیمت تخمینی را حفظ می‌کنیم
        sl_ratio = abs(sig.entry_estimate - sig.sl) / sig.entry_estimate
        tp_ratio = abs(sig.entry_estimate - sig.tp) / sig.entry_estimate

        pos_side = "long" if sig.action == "buy" else "short"

        if pos_side == "long":
            real_sl = fill_price - (fill_price * sl_ratio)
            real_tp = fill_price + (fill_price * tp_ratio)
        else:
            real_sl = fill_price + (fill_price * sl_ratio)
            real_tp = fill_price - (fill_price * tp_ratio)

        # 🔥 اطمینان از حداقل فاصله SL
        min_sl_dist = fill_price * 0.008  # ۰.۸٪
        actual_sl_dist = abs(fill_price - real_sl)
        if actual_sl_dist < min_sl_dist:
            if pos_side == "long":
                real_sl = fill_price - min_sl_dist
            else:
                real_sl = fill_price + min_sl_dist
            log.info("[%s] SL به حداقل %0.2f%% رسید", short_name, 0.8)

        # ثبت SL در صرافی
        sl_order_id = EX.place_stop_loss(
            sym, pos_side, filled_qty, real_sl
        )

        # ذخیره
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
        log.info(
            "✅ [%s] پوزيشن باز شد | %s | ورود: %.4f | SL: %.4f (%.2f%%) | TP: %.4f",
            short_name, pos_side.upper(), fill_price, real_sl, sl_pct, real_tp,
        )

        if self.tg:
            self.tg.send(
                f"🚀 <b>پوزيشن جديد ({sig.strategy}) v3.1</b>\n"
                f"{'═' * 25}\n"
                f"📊 نماد: {sym}\n"
                f"📈 جهت: {pos_side.upper()}\n"
                f"💰 ورود: {fill_price:.4f}\n"
                f"🛑 حد ضرر: {real_sl:.4f} ({sl_pct:.2f}%)\n"
                f"🎯 حد سود: {real_tp:.4f}\n"
                f"📊 حجم: {filled_qty}\n"
                f"🎯 اطمينان: {sig.confidence}%\n"
                f"📝 دليل: {sig.reason}\n"
                f"🛡️ SL صرافي: {'✅' if sl_order_id else '❌'}\n"
                f"{'🧪 TESTNET' if TESTNET else '💰 REAL'}"
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

                sl_hit = (
                    (side == "long" and price <= pos["sl"])
                    or (side == "short" and price >= pos["sl"])
                )
                if sl_hit:
                    log.info(
                        "🛑 [%s] SL خورد! price=%.4f sl=%.4f",
                        pos["symbol"], price, pos["sl"],
                    )
                    self._close_position(pid, pos, price, "StopLoss")
                    continue

                if not pos.get("is_partial", 0):
                    tp_hit = (
                        (side == "long" and price >= pos["tp"])
                        or (side == "short" and price <= pos["tp"])
                    )
                    if tp_hit:
                        log.info(
                            "🎯 [%s] TP1 خورد! price=%.4f tp=%.4f",
                            pos["symbol"], price, pos["tp"],
                        )
                        self._partial_exit(pid, pos, price)

            except Exception as e:
                log.error("Manage Error [%s]: %s", pos.get("symbol"), e)

    def _partial_exit(self, pid: str, pos: Dict, price: float):
        half_qty = pos["qty"] / 2.0
        close_side = "sell" if pos["side"] == "long" else "buy"

        result = EX.place_order(
            pos["symbol"], close_side, half_qty, is_close=True
        )
        if not result:
            return

        actual_exit = result["fill_price"]

        if pos.get("sl_order_id"):
            EX.cancel_order_safe(pos["symbol"], pos["sl_order_id"])

        new_sl = pos["entry"]
        new_sl_id = EX.place_stop_loss(
            pos["symbol"], pos["side"], half_qty, new_sl
        )

        with self._lock:
            if pid in self._pos:
                self._pos[pid]["qty"] = half_qty
                self._pos[pid]["sl"] = new_sl
                self._pos[pid]["is_partial"] = 1
                self._pos[pid]["sl_order_id"] = new_sl_id or ""

        database.update_partial(pid, half_qty, new_sl)
        if new_sl_id:
            database.update_sl_order(pid, new_sl_id)

        pnl_half = (
            (actual_exit - pos["entry"]) * half_qty
            if pos["side"] == "long"
            else (pos["entry"] - actual_exit) * half_qty
        )

        if self.tg:
            self.tg.send(
                f"🎯 <b>خروج جزئی (TP1)</b>\n"
                f"نماد: {pos['symbol']}\n"
                f"قيمت خروج: {actual_exit:.4f}\n"
                f"سود جزئي: {pnl_half:+.2f}$\n"
                f"SL جديد: {new_sl:.4f} (Break Even)"
            )

    def _close_position(self, pid: str, pos: Dict,
                        price: float, reason: str):
        close_side = "sell" if pos["side"] == "long" else "buy"

        result = EX.place_order(
            pos["symbol"], close_side, pos["qty"], is_close=True
        )

        actual_price = result["fill_price"] if result else price

        if pos.get("sl_order_id"):
            EX.cancel_order_safe(pos["symbol"], pos["sl_order_id"])

        entry = pos.get("fill_price", pos["entry"])
        pnl = (
            (actual_price - entry) * pos["qty"]
            if pos["side"] == "long"
            else (entry - actual_price) * pos["qty"]
        )
        pct = (
            (actual_price - entry) / entry * 100
            if pos["side"] == "long"
            else (entry - actual_price) / entry * 100
        )

        database.close(pid, actual_price, pnl, pct, reason)

        with self._lock:
            self._pos.pop(pid, None)

        emoji = "✅" if pnl >= 0 else "❌"
        log.info(
            "%s [%s] بسته شد | PnL: %+.2f$ (%+.2f%%) | %s",
            emoji, pos["symbol"], pnl, pct, reason,
        )

        if self.tg:
            self.tg.send(
                f"{emoji} <b>بسته شد ({reason})</b>\n"
                f"نماد: {pos['symbol']}\n"
                f"ورود: {entry:.4f} ➜ خروج: {actual_price:.4f}\n"
                f"سود/زيان: {pnl:+.2f}$ ({pct:+.2f}%)"
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

    return f"""
    <!DOCTYPE html>
    <html dir="rtl" lang="fa">
    <head>
        <meta charset="UTF-8">
        <title>Quant Bot v3.1</title>
        <meta http-equiv="refresh" content="30">
        <style>
            body {{
                font-family: Tahoma, sans-serif;
                background: #0d1117; color: #c9d1d9;
                padding: 20px; text-align: center;
            }}
            .card {{
                background: #161b22;
                border: 1px solid #30363d;
                padding: 12px; margin: 6px;
                border-radius: 8px;
                display: inline-block;
                min-width: 130px;
            }}
            .warn {{ background: #3d1f00; border-color: #f0883e; color: #f0883e; }}
            .ok {{ border-color: #3fb950; }}
            h1 {{ color: #58a6ff; }}
            .status {{ font-size: 1.2em; margin: 10px; }}
            .badge {{ background: #238636; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; }}
        </style>
    </head>
    <body>
        <h1>🤖 Master-AI Quant Bot v3.1</h1>
        <span class="badge">✅ بهینه‌شده</span>

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
        <div class="card"><h3>🎯 وين‌ريت</h3><p>{stats['win_rate']}%</p></div>
        <div class="card"><h3>⚡ PF</h3><p>{stats['profit_factor']}</p></div>
        <div class="card"><h3>📊 معاملات</h3>
            <p>{stats['total_trades']} (W:{stats['wins_count']} L:{stats['losses_count']})</p>
        </div>
        <div class="card"><h3>🛡️ DD</h3><p>{dd:.1f}%</p></div>

        {'<div class="card warn"><h3>🧪</h3><p>TESTNET فعال</p></div>' if TESTNET else ''}

        <br><br>
        <div class="card">
            <h3>🔧 تنظيمات v3.1</h3>
            <p>لوريج: {LEVERAGE}x | ريسک: {RISK_PCT}% | اسکن: {SCAN_INTERVAL}s</p>
            <p>Min Conf: {MIN_CONFIDENCE}% | SL Min: 0.8% | R:R: 1.8</p>
            <p>نمادها: {len(SYMBOLS)}</p>
        </div>
    </body>
    </html>
    """


@app.route("/health")
def health():
    return {
        "status": "ok",
        "version": "3.1",
        "connected": EX.is_connected,
        "testnet": TESTNET,
        "active": engine_instance.is_active if engine_instance else False,
        "positions": len(engine_instance._pos) if engine_instance else 0,
        "cycle": engine_instance._cycle_count if engine_instance else 0,
    }


@app.route("/positions")
def api_positions():
    real = EX.fetch_real_positions()
    db = list(engine_instance._pos.values()) if engine_instance else []
    return {
        "exchange": real,
        "db": [
            {
                "symbol": p["symbol"],
                "side": p["side"],
                "entry": p["entry"],
                "qty": p["qty"],
                "sl": p["sl"],
                "tp": p["tp"],
            }
            for p in db
        ],
        "synced": {p["symbol"] for p in real}
        == {p["symbol"] for p in db},
    }


@app.route("/debug")
def api_debug():
    results = {}
    for sym in SYMBOLS:
        short = sym.split("/")[0]
        try:
            dfs = EX.fetch_multi_ohlcv(sym)
            if not dfs:
                results[short] = "no data"
                continue
            sig = STRATEGY.analyze(sym, dfs)
            results[short] = {
                "action": sig.action,
                "strategy": sig.strategy,
                "confidence": sig.confidence,
                "reason": sig.reason,
                "debug": sig.debug_info,
            }
        except Exception as e:
            results[short] = f"error: {e}"
    return results


# ============================================================================
# MAIN
# ============================================================================
def main():
    global engine_instance

    log.info("=" * 60)
    log.info("  🤖 Master-AI Quant Bot v3.1 (OPTIMIZED)")
    log.info("  🌐 Mode: %s", "TESTNET" if TESTNET else "MAINNET")
    log.info("  🔗 Connected: %s", EX.is_connected)
    log.info("  📊 Symbols: %d", len(SYMBOLS))
    log.info("  🎯 Risk: %.1f%% | Leverage: %dx | MaxPos: %d",
             RISK_PCT, LEVERAGE, MAX_POS)
    log.info("  🎯 MinConfidence: %d%% | ScanInterval: %ds",
             MIN_CONFIDENCE, SCAN_INTERVAL)
    log.info("=" * 60)
    log.info("  🔧 اصلاحات اعمال‌شده:")
    log.info("  • ATR ۱۵ دقيقه براي محاسبه SL")
    log.info("  • حداقل SL ۰.۸٪ از قيمت")
    log.info("  • فیلتر حجم و RSI در تمام استراتژی‌ها")
    log.info("  • محاسبه SL بر اساس قیمت واقعی Fill")
    log.info("  • بهبود R:R به ۱.۸")
    log.info("=" * 60)

    if not EX.is_connected:
        log.critical("❌ اتصال به صرافي برقرار نشد!")

    engine_instance = Engine()
    tg = TelegramHandler(engine_instance)
    engine_instance.tg = tg

    if TG_TOKEN and TG_CHAT:
        tg.send(
            f"🚀 <b>ربات v3.1 (بهینه) شروع شد</b>\n"
            f"{'═' * 25}\n"
            f"🌐 شبکه: {'🧪 TESTNET' if TESTNET else '💰 MAINNET'}\n"
            f"🔗 اتصال: {'✅' if EX.is_connected else '❌'}\n"
            f"📊 نمادها: {len(SYMBOLS)}\n"
            f"⚡ لوريج: {LEVERAGE}x\n"
            f"🎯 ريسک: {RISK_PCT}%\n"
            f"🎯 Min Conf: {MIN_CONFIDENCE}%\n"
            f"⏱️ Scan: {SCAN_INTERVAL}s\n"
            f"{'═' * 25}\n"
            f"🔧 اصلاحات v3.1:\n"
            f"• حداقل SL ۰.۸%\n"
            f"• ATR از ۱۵ دقيقه\n"
            f"• فیلتر حجم و RSI\n"
            f"• R:R بهبود یافته\n"
            f"{'═' * 25}\n"
            f"✅ ربات فعال است و در حال اسکن...\n"
            f"🔍 دکمه ديباگ اسکن براي بررسي وضعيت",
            reply_markup=tg._keyboard(),
        )

    threading.Thread(target=engine_instance.run_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)


if __name__ == "__main__":
    main()
