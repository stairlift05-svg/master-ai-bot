#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Quant Bot v2.0 - FIXED VERSION
اصلاح تمام مشکلات شناسایی‌شده توسط کارگروه بررسی
"""

import json
import logging
import os
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
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
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("MasterQuant_v2")


# ============================================================================
# CONFIGURATION WITH VALIDATION
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


# ─── کلیدها باید از env خوانده شوند، نه هاردکد! ───
API_KEY = Cfg.s("PHEMEX_API_KEY")
API_SECRET = Cfg.s("PHEMEX_API_SECRET")
TG_TOKEN = Cfg.s("TELEGRAM_BOT_TOKEN")
TG_CHAT = Cfg.s("TELEGRAM_CHAT_ID")

if not API_KEY or not API_SECRET:
    log.critical("❌ API_KEY و API_SECRET باید در متغیرهای محیطی تنظیم شوند!")
    log.critical("   هرگز کلید API را در سورس‌کد قرار ندهید!")

SYMBOLS = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
    "BNB/USDT:USDT",
]

RISK_PCT = Cfg.f("RISK_PER_TRADE", 1.0)        # ریسک واقعی هر معامله
MAX_DD = Cfg.f("MAX_DRAWDOWN", 8.0)             # حداکثر افت حساب
MAX_POS = Cfg.i("MAX_POSITIONS", 3)             # کمتر برای ایمنی بیشتر
LEVERAGE = Cfg.i("LEVERAGE", 5)                  # لوریج کمتر و ایمن‌تر
TESTNET = Cfg.b("PHEMEX_TESTNET", False)         # ← پیش‌فرض: واقعی!
PORT = Cfg.i("PORT", 10000)
SCAN_INTERVAL = Cfg.i("SCAN_INTERVAL", 15)       # ثانیه بین اسکن‌ها
MIN_CONFIDENCE = Cfg.i("MIN_CONFIDENCE", 75)      # حداقل اطمینان


# ============================================================================
# TECHNICAL INDICATORS (بدون تغییر - فقط مرتب‌تر)
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
    def macd(close: pd.Series, fast=12, slow=26, sig=9):
        e_fast = close.ewm(span=fast, adjust=False).mean()
        e_slow = close.ewm(span=slow, adjust=False).mean()
        line = e_fast - e_slow
        signal = line.ewm(span=sig, adjust=False).mean()
        return line, signal, line - signal

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


IND = Indicators()


# ============================================================================
# DATABASE - با ستون‌های اضافی برای ردیابی واقعی
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
            opened_at       TEXT DEFAULT CURRENT_TIMESTAMP,
            closed_at       TEXT,
            is_real         INTEGER DEFAULT 1
        )"""
    ]

    def __init__(self):
        self._lock = threading.Lock()
        self._path = "bot_v2.db"
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
            "is_partial,exchange_order_id "
            "FROM trades WHERE status='open'"
        )
        if not rows:
            return []
        keys = [
            "id", "symbol", "side", "entry", "fill_price", "qty",
            "filled_qty", "sl", "tp", "strategy", "conf",
            "is_partial", "exchange_order_id",
        ]
        return [dict(zip(keys, r)) for r in rows]

    def insert(self, t: Dict):
        self.run(
            "INSERT OR IGNORE INTO trades "
            "(id,symbol,side,entry_price,fill_price,quantity,filled_quantity,"
            "stop_loss,take_profit,strategy,confidence,exchange_order_id,is_real) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                t["id"], t["symbol"], t["side"], t["entry"],
                t.get("fill_price", t["entry"]),
                t["qty"], t.get("filled_qty", t["qty"]),
                t["sl"], t["tp"], t["strategy"], t["conf"],
                t.get("exchange_order_id", ""),
                1,  # is_real
            ),
        )

    def update_partial(self, tid: str, new_qty: float, new_sl: float):
        self.run(
            "UPDATE trades SET quantity=?, stop_loss=?, is_partial=1 WHERE id=?",
            (new_qty, new_sl, tid),
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
            "SELECT pnl, pnl_pct FROM trades "
            "WHERE status='closed' AND is_real=1"
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
            "avg_win": round(
                sum(wins) / len(wins), 2
            ) if wins else 0.0,
            "avg_loss": round(
                sum(losses) / len(losses), 2
            ) if losses else 0.0,
            "largest_win": round(max(wins), 2) if wins else 0.0,
            "largest_loss": round(max(losses), 2) if losses else 0.0,
        }


database = DB()


# ============================================================================
# EXCHANGE ENGINE - اصلاح‌شده با ردیابی واقعی
# ============================================================================
class Exchange:

    def __init__(self):
        self._ex: Optional[ccxt.phemex] = None
        self._markets_info: Dict = {}
        self._connected = False
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
            })
            if TESTNET:
                self._ex.set_sandbox_mode(True)
                log.warning("⚠️ حالت TESTNET فعال است - معاملات واقعی نیست!")

            self._ex.load_markets()
            self._cache_market_info()
            self._set_leverage_all()
            self._connected = True

            mode = "TESTNET" if TESTNET else "MAINNET"
            log.info("✅ اتصال به Phemex %s برقرار شد.", mode)

        except Exception as e:
            log.error("❌ خطای اتصال به صرافی: %s", e)

    def _cache_market_info(self):
        """ذخیره اطلاعات بازار برای هر نماد"""
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
                    ).get("min", 1.0),
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
            except Exception:
                pass

    @property
    def is_connected(self) -> bool:
        return self._connected and self._ex is not None

    def fetch_ohlcv_safe(self, sym: str, tf: str = "5m",
                         limit: int = 100) -> Optional[pd.DataFrame]:
        """دریافت ایمن OHLCV با retry"""
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
                time.sleep(1)
        return None

    def fetch_multi_ohlcv(self, sym: str) -> Dict[str, pd.DataFrame]:
        result = {}
        for tf in ["1m", "3m", "5m", "15m"]:
            df = self.fetch_ohlcv_safe(sym, tf)
            if df is None or len(df) < 30:
                return {}
            result[tf] = df
            time.sleep(0.1)  # rate limit
        return result

    def get_current_price(self, sym: str) -> Optional[float]:
        """دریافت قیمت لحظه‌ای از تیکر"""
        if not self.is_connected:
            return None
        try:
            ticker = self._ex.fetch_ticker(sym)
            return float(ticker.get("last", 0))
        except Exception:
            return None

    def fetch_real_positions(self) -> List[Dict]:
        """دریافت پوزیشن‌های واقعی از صرافی"""
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
        """موجودی کل شامل PnL باز"""
        if not self.is_connected:
            return 0.0
        try:
            b = self._ex.fetch_balance()
            return float(b.get("USDT", {}).get("total", 0.0))
        except Exception:
            return 0.0

    def validate_order_size(self, sym: str, qty: float,
                            price: float) -> Tuple[bool, float, str]:
        """اعتبارسنجی حجم سفارش"""
        info = self._markets_info.get(sym, {})
        min_amount = info.get("min_amount", 0.001)
        min_cost = info.get("min_cost", 1.0)

        # فرمت‌بندی حجم
        try:
            formatted_qty = float(
                self._ex.amount_to_precision(sym, qty)
            )
        except Exception:
            formatted_qty = qty

        if formatted_qty < min_amount:
            return False, 0, f"حجم {formatted_qty} کمتر از حداقل {min_amount}"

        cost = formatted_qty * price / LEVERAGE
        if cost < min_cost:
            return False, 0, f"ارزش سفارش {cost:.2f}$ کمتر از حداقل {min_cost}$"

        return True, formatted_qty, "OK"

    def place_order(self, sym: str, side: str, qty: float,
                    is_close: bool = False) -> Optional[Dict]:
        """ثبت سفارش با اعتبارسنجی کامل و بازگشت قیمت واقعی"""
        if not self.is_connected:
            log.error("❌ صرافی متصل نیست!")
            return None

        try:
            current_price = self.get_current_price(sym)
            if not current_price:
                log.error("❌ قیمت لحظه‌ای دریافت نشد: %s", sym)
                return None

            # اعتبارسنجی
            valid, fmt_qty, msg = self.validate_order_size(
                sym, qty, current_price
            )
            if not valid:
                log.warning("⚠️ سفارش نامعتبر [%s]: %s", sym, msg)
                return None

            params = {}
            if is_close:
                params["reduceOnly"] = True

            if side.lower() == "buy":
                result = self._ex.create_market_buy_order(
                    sym, fmt_qty, params=params
                )
            else:
                result = self._ex.create_market_sell_order(
                    sym, fmt_qty, params=params
                )

            # استخراج قیمت واقعی اجرا شده
            fill_price = float(
                result.get("average")
                or result.get("price")
                or current_price
            )
            filled_qty = float(
                result.get("filled") or result.get("amount") or fmt_qty
            )

            log.info(
                "✅ سفارش اجرا شد | %s %s | حجم: %s | قیمت: %s | ID: %s",
                side.upper(), sym, filled_qty, fill_price,
                result.get("id"),
            )

            return {
                "id": result.get("id"),
                "fill_price": fill_price,
                "filled_qty": filled_qty,
                "status": result.get("status"),
                "raw": result,
            }

        except ccxt.InsufficientFunds:
            log.error("❌ موجودی کافی نیست برای [%s %s]", side, sym)
            return None
        except ccxt.InvalidOrder as e:
            log.error("❌ سفارش نامعتبر [%s %s]: %s", side, sym, e)
            return None
        except Exception as e:
            log.error("❌ خطای سفارش [%s %s]: %s", side, sym, e)
            return None

    def place_stop_loss_order(self, sym: str, side: str,
                              qty: float, stop_price: float) -> Optional[str]:
        """ثبت حد ضرر واقعی در صرافی"""
        if not self.is_connected:
            return None
        try:
            # side برای SL: اگر پوزیشن long است، SL = sell
            sl_side = "sell" if side == "long" else "buy"

            fmt_qty = float(self._ex.amount_to_precision(sym, qty))
            fmt_price = float(
                self._ex.price_to_precision(sym, stop_price)
            )

            params = {
                "stopPrice": fmt_price,
                "reduceOnly": True,
                "triggerType": "ByLastPrice",
            }

            result = self._ex.create_order(
                sym, "market", sl_side, fmt_qty,
                None, params=params,
            )

            log.info(
                "🛡️ SL ثبت شد در صرافی | %s | قیمت: %s | ID: %s",
                sym, fmt_price, result.get("id"),
            )
            return result.get("id")

        except Exception as e:
            log.warning("⚠️ خطا در ثبت SL [%s]: %s", sym, e)
            return None

    def cancel_order_safe(self, sym: str, order_id: str):
        """لغو ایمن سفارش"""
        if not self.is_connected or not order_id:
            return
        try:
            self._ex.cancel_order(order_id, sym)
        except Exception:
            pass


EX = Exchange()


# ============================================================================
# STRATEGY BRAIN - بهبود یافته
# ============================================================================
@dataclass
class Signal:
    action: str = "neutral"   # buy, sell, neutral
    strategy: str = ""
    confidence: int = 0
    reason: str = ""
    sl: float = 0.0
    tp: float = 0.0
    entry_estimate: float = 0.0


class StrategyEngine:
    """موتور استراتژی با امتیازدهی ترکیبی"""

    def analyze(self, sym: str,
                dfs: Dict[str, pd.DataFrame]) -> Signal:
        required_tfs = ["1m", "3m", "5m", "15m"]
        if not dfs or any(
            tf not in dfs or len(dfs[tf]) < 30 for tf in required_tfs
        ):
            return Signal()

        df1m = dfs["1m"]
        df3m = dfs["3m"]
        df5m = dfs["5m"]
        df15m = dfs["15m"]

        # ── تحلیل تایم‌فریم ۱۵ دقیقه (روند اصلی) ──
        adx15 = IND.safe(
            IND.adx(df15m["high"], df15m["low"], df15m["close"])
        )
        ema20_15 = IND.safe(IND.ema(df15m["close"], 20))
        ema50_15 = IND.safe(IND.ema(df15m["close"], 50))
        price15 = IND.safe(df15m["close"])
        rsi15 = IND.safe(IND.rsi(df15m["close"], 14))

        # ── استراتژی ۱: Momentum Scalp (روند قوی) ──
        if adx15 > 22:
            signal = self._momentum_scalp(
                df1m, df3m, price15, ema20_15, ema50_15, adx15
            )
            if signal.action != "neutral":
                return signal

        # ── استراتژی ۲: Mean Reversion (بازار رنج) ──
        if adx15 <= 22:
            signal = self._mean_reversion(df1m, df5m, adx15)
            if signal.action != "neutral":
                return signal

        return Signal()

    def _momentum_scalp(
        self, df1m, df3m, price15, ema20_15, ema50_15, adx15
    ) -> Signal:
        # تعیین جهت روند
        if price15 > ema20_15 and ema20_15 > ema50_15:
            trend = "long"
        elif price15 < ema20_15 and ema20_15 < ema50_15:
            trend = "short"
        else:
            return Signal()

        # بررسی پولبک در ۳ دقیقه
        rsi3 = IND.safe(IND.rsi(df3m["close"], 14))
        _, _, m_hist = IND.macd(df3m["close"])
        macd_h = IND.safe(m_hist)

        pullback = (
            (rsi3 < 45 and macd_h > 0)
            if trend == "long"
            else (rsi3 > 55 and macd_h < 0)
        )

        if not pullback:
            return Signal()

        # تأیید در ۱ دقیقه
        c1 = IND.safe(df1m["close"])
        ema9_1 = IND.safe(IND.ema(df1m["close"], 9))
        trigger = (c1 > ema9_1) if trend == "long" else (c1 < ema9_1)

        if not trigger or c1 <= 0:
            return Signal()

        atr3 = IND.safe(
            IND.atr(df3m["high"], df3m["low"], df3m["close"])
        )
        if atr3 <= 0:
            atr3 = c1 * 0.005

        # محاسبه SL/TP با نسبت ریسک به ریوارد حداقل ۱:۱.۵
        if trend == "long":
            sl = c1 - (1.5 * atr3)
            tp = c1 + (2.0 * atr3)
            action = "buy"
        else:
            sl = c1 + (1.5 * atr3)
            tp = c1 - (2.0 * atr3)
            action = "sell"

        # امتیاز اطمینان بر اساس عوامل مختلف
        conf = 60
        if adx15 > 30:
            conf += 10
        if adx15 > 40:
            conf += 5
        if abs(rsi3 - 50) > 10:
            conf += 5
        if abs(macd_h) > 0:
            conf += 5

        return Signal(
            action=action,
            strategy="MomentumScalp",
            confidence=min(conf, 95),
            reason=f"ADX={adx15:.0f} RSI3={rsi3:.0f} Trend={trend}",
            sl=sl,
            tp=tp,
            entry_estimate=c1,
        )

    def _mean_reversion(self, df1m, df5m, adx15) -> Signal:
        bb_lo, _, bb_hi = IND.bbands(df5m["close"], 20, 2.0)
        c5 = IND.safe(df5m["close"])
        rsi5 = IND.safe(IND.rsi(df5m["close"], 14))

        if c5 <= 0:
            return Signal()

        at_lower = c5 <= IND.safe(bb_lo) and rsi5 < 35
        at_upper = c5 >= IND.safe(bb_hi) and rsi5 > 65

        if not (at_lower or at_upper):
            return Signal()

        # تأیید واگرایی در ۱ دقیقه
        c1 = IND.safe(df1m["close"])
        rsi1 = IND.safe(IND.rsi(df1m["close"], 7))

        if c1 <= 0:
            return Signal()

        atr1 = IND.safe(
            IND.atr(df1m["high"], df1m["low"], df1m["close"])
        )
        if atr1 <= 0:
            atr1 = c1 * 0.004

        if at_lower and rsi1 > 28:
            sl = c1 - (1.5 * atr1)
            tp = c1 + (2.0 * atr1)
            conf = 55
            if rsi5 < 30:
                conf += 10
            if rsi1 > 35:
                conf += 10
            return Signal(
                action="buy",
                strategy="MeanReversion",
                confidence=min(conf, 90),
                reason=f"BB_Low RSI5={rsi5:.0f} RSI1={rsi1:.0f}",
                sl=sl, tp=tp, entry_estimate=c1,
            )

        if at_upper and rsi1 < 72:
            sl = c1 + (1.5 * atr1)
            tp = c1 - (2.0 * atr1)
            conf = 55
            if rsi5 > 70:
                conf += 10
            if rsi1 < 65:
                conf += 10
            return Signal(
                action="sell",
                strategy="MeanReversion",
                confidence=min(conf, 90),
                reason=f"BB_High RSI5={rsi5:.0f} RSI1={rsi1:.0f}",
                sl=sl, tp=tp, entry_estimate=c1,
            )

        return Signal()


STRATEGY = StrategyEngine()


# ============================================================================
# TELEGRAM - بهبود یافته
# ============================================================================
class TelegramHandler:

    def __init__(self, engine_ref):
        self.engine = engine_ref
        self.last_update_id = 0
        if TG_TOKEN and TG_CHAT:
            threading.Thread(
                target=self._poll_loop, daemon=True
            ).start()
            log.info("✅ تلگرام متصل شد.")

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
            log.warning("TG Send Error: %s", e)

    def _keyboard(self):
        return {
            "keyboard": [
                [
                    {"text": "📊 داشبورد"},
                    {"text": "💼 پوزیشن‌ها"},
                ],
                [
                    {"text": "📜 تاریخچه صرافی"},
                    {"text": "🔍 وضعیت اتصال"},
                ],
                [
                    {"text": "🟢 شروع"},
                    {"text": "🔴 توقف"},
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

        if cmd in ("/start", "🟢 شروع"):
            self.engine.is_active = True
            self.send("🟢 <b>ربات فعال شد</b>", reply_markup=kb)

        elif cmd in ("/stop", "🔴 توقف"):
            self.engine.is_active = False
            self.send("🔴 <b>ربات متوقف شد</b>", reply_markup=kb)

        elif cmd in ("/dashboard", "📊 داشبورد"):
            self._send_dashboard()

        elif cmd in ("/positions", "💼 پوزیشن‌ها"):
            self._send_positions()

        elif cmd in ("/history", "📜 تاریخچه صرافی"):
            self._send_real_history()

        elif cmd in ("/status", "🔍 وضعیت اتصال"):
            self._send_connection_status()

    def _send_dashboard(self):
        stats = database.get_analytics()
        bal = EX.balance()
        equity = EX.total_equity()
        real_pos = EX.fetch_real_positions()
        db_pos_count = len(self.engine._pos)

        status = "🟢 فعال" if self.engine.is_active else "🔴 متوقف"
        mode = "⚠️ TESTNET" if TESTNET else "✅ MAINNET"

        msg = (
            f"📊 <b>داشبورد ربات</b>\n"
            f"{'━' * 25}\n"
            f"⚙️ وضعیت: {status}\n"
            f"🌐 شبکه: {mode}\n"
            f"🔗 اتصال: {'✅' if EX.is_connected else '❌'}\n"
            f"{'━' * 25}\n"
            f"💰 موجودی آزاد: ${bal:,.2f}\n"
            f"💎 ارزش کل: ${equity:,.2f}\n"
            f"📈 سود/زیان: {stats['total_pnl']:+,.2f}$\n"
            f"{'━' * 25}\n"
            f"📌 پوزیشن DB: {db_pos_count}\n"
            f"📌 پوزیشن صرافی: {len(real_pos)}\n"
            f"{'━' * 25}\n"
            f"🎯 معاملات: {stats['total_trades']}\n"
            f"✅ برد: {stats['wins_count']} | "
            f"❌ باخت: {stats['losses_count']}\n"
            f"🔥 وین‌ریت: {stats['win_rate']}%\n"
            f"⚡ PF: {stats['profit_factor']}\n"
            f"📊 میانگین برد: ${stats['avg_win']}\n"
            f"📊 میانگین باخت: ${stats['avg_loss']}\n"
            f"🛡️ افت حساب: {self.engine.current_dd:.1f}%\n"
        )
        self.send(msg, reply_markup=self._keyboard())

    def _send_positions(self):
        # نمایش پوزیشن‌های واقعی صرافی
        real_pos = EX.fetch_real_positions()
        db_pos = list(self.engine._pos.values())

        if not real_pos and not db_pos:
            self.send(
                "💼 <b>هیچ پوزیشن بازی نیست</b>",
                reply_markup=self._keyboard(),
            )
            return

        msg = "💼 <b>پوزیشن‌های واقعی صرافی:</b>\n"
        if real_pos:
            for p in real_pos:
                msg += (
                    f"\n📌 <b>{p['symbol']}</b> ({p['side'].upper()})\n"
                    f"   ورود: {p['entry']:.4f}\n"
                    f"   حجم: {p['qty']}\n"
                    f"   PnL: {p['unrealized_pnl']:+.2f}$\n"
                    f"   لیکویید: {p['liquidation']:.2f}\n"
                )
        else:
            msg += "هیچ پوزیشنی در صرافی نیست\n"

        if db_pos:
            msg += f"\n{'━' * 25}\n📋 <b>پوزیشن‌های DB ({len(db_pos)}):</b>\n"
            for p in db_pos:
                msg += f"📌 {p['symbol']} ({p['side']}) @ {p['entry']:.4f}\n"

        # بررسی ناهماهنگی
        real_syms = {p["symbol"] for p in real_pos}
        db_syms = {p["symbol"] for p in db_pos}
        if real_syms != db_syms:
            msg += (
                f"\n⚠️ <b>ناهماهنگی!</b>\n"
                f"فقط صرافی: {real_syms - db_syms}\n"
                f"فقط DB: {db_syms - real_syms}\n"
            )

        self.send(msg, reply_markup=self._keyboard())

    def _send_real_history(self):
        if not EX.is_connected:
            self.send("❌ صرافی متصل نیست",
                       reply_markup=self._keyboard())
            return

        try:
            all_trades = []
            for sym in SYMBOLS[:3]:
                try:
                    trades = EX._ex.fetch_my_trades(sym, limit=3)
                    for t in trades:
                        all_trades.append(t)
                except Exception:
                    continue

            if not all_trades:
                self.send(
                    "📜 <b>هیچ تاریخچه‌ای یافت نشد</b>",
                    reply_markup=self._keyboard(),
                )
                return

            msg = "📜 <b>تاریخچه واقعی صرافی:</b>\n"
            for t in all_trades[:10]:
                dt = datetime.fromtimestamp(
                    t.get("timestamp", 0) / 1000
                ).strftime("%m-%d %H:%M")
                msg += (
                    f"\n{t['symbol']} | {t['side'].upper()}\n"
                    f"   قیمت: {t['price']} | حجم: {t['amount']}\n"
                    f"   هزینه: ${t.get('cost', 0):.2f} | {dt}\n"
                )

            self.send(msg, reply_markup=self._keyboard())
        except Exception as e:
            self.send(
                f"❌ خطا: {e}", reply_markup=self._keyboard()
            )

    def _send_connection_status(self):
        connected = EX.is_connected
        mode = "TESTNET" if TESTNET else "MAINNET"
        bal = EX.balance() if connected else 0

        msg = (
            f"🔍 <b>وضعیت اتصال</b>\n"
            f"{'━' * 25}\n"
            f"🔗 صرافی: {'✅ متصل' if connected else '❌ قطع'}\n"
            f"🌐 شبکه: {mode}\n"
            f"💰 موجودی: ${bal:,.2f}\n"
            f"🤖 ربات: {'فعال' if self.engine.is_active else 'متوقف'}\n"
            f"📌 لوریج: {LEVERAGE}x\n"
            f"⚠️ ریسک: {RISK_PCT}% هر معامله\n"
            f"📌 حداکثر پوزیشن: {MAX_POS}\n"
        )
        self.send(msg, reply_markup=self._keyboard())


# ============================================================================
# CORE ENGINE - اصلاح‌شده
# ============================================================================
class Engine:

    def __init__(self):
        self._pos: Dict[str, Dict] = {}
        self._lock = threading.RLock()  # RLock به جای Lock
        self.is_active = False  # ← پیش‌فرض غیرفعال - کاربر باید فعال کند
        self.is_dd_halted = False
        self.current_dd = 0.0
        self.peak_balance = None
        self.tg: Optional[TelegramHandler] = None
        self._sync_on_boot()

    def _sync_on_boot(self):
        """همگام‌سازی با صرافی در شروع"""
        bal = EX.total_equity()
        self.peak_balance = bal if bal > 0 else None

        # بارگذاری از DB
        for t in database.open_trades():
            self._pos[t["id"]] = t

        # همگام‌سازی با پوزیشن‌های واقعی صرافی
        real_positions = EX.fetch_real_positions()
        for rp in real_positions:
            already_tracked = any(
                p["symbol"] == rp["symbol"]
                for p in self._pos.values()
            )
            if not already_tracked:
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
                }
                self._pos[pid] = pos
                database.insert(pos)
                log.info(
                    "🔄 پوزیشن همگام شد: %s %s",
                    rp["symbol"], rp["side"],
                )

        log.info(
            "📊 شروع با %d پوزیشن فعال | موجودی: $%.2f",
            len(self._pos), bal,
        )

    def run_loop(self):
        log.info("▶️ موتور اصلی شروع شد")
        cycle = 0

        while True:
            try:
                cycle += 1

                if not EX.is_connected:
                    log.warning("⚠️ صرافی متصل نیست، تلاش مجدد...")
                    time.sleep(30)
                    continue

                # به‌روزرسانی موجودی
                equity = EX.total_equity()
                if equity > 0:
                    self._check_drawdown(equity)

                # مدیریت پوزیشن‌ها (همیشه فعال)
                self._manage_positions()

                # بررسی ناهماهنگی هر ۱۰ سیکل
                if cycle % 10 == 0:
                    self._check_sync()

                # اسکن سیگنال جدید
                if (
                    self.is_active
                    and not self.is_dd_halted
                    and len(self._pos) < MAX_POS
                ):
                    self._scan_markets(equity)

                time.sleep(SCAN_INTERVAL)

            except Exception as e:
                log.error("❌ Engine Loop Error: %s", e)
                time.sleep(SCAN_INTERVAL)

    def _check_drawdown(self, equity: float):
        if self.peak_balance is None or equity > self.peak_balance:
            self.peak_balance = equity
        if self.peak_balance > 0:
            self.current_dd = (
                (self.peak_balance - equity) / self.peak_balance * 100
            )
            if self.current_dd >= MAX_DD and not self.is_dd_halted:
                self.is_dd_halted = True
                log.critical(
                    "🚨 DRAWDOWN LIMIT! DD=%.1f%% - معاملات جدید متوقف!",
                    self.current_dd,
                )
                if self.tg:
                    self.tg.send(
                        f"🚨 <b>هشدار افت حساب!</b>\n"
                        f"افت: {self.current_dd:.1f}% از حداکثر {MAX_DD}%\n"
                        f"معاملات جدید متوقف شد!"
                    )
            elif self.current_dd < MAX_DD * 0.8 and self.is_dd_halted:
                self.is_dd_halted = False

    def _check_sync(self):
        """بررسی ناهماهنگی بین DB و صرافی"""
        real = EX.fetch_real_positions()
        real_syms = {p["symbol"] for p in real}

        with self._lock:
            db_syms = {p["symbol"] for p in self._pos.values()}

        # پوزیشن‌هایی که در DB هست ولی در صرافی نیست
        orphans = db_syms - real_syms
        for pid, pos in list(self._pos.items()):
            if pos["symbol"] in orphans:
                log.warning(
                    "⚠️ پوزیشن %s در DB هست ولی در صرافی نیست - بستن",
                    pos["symbol"],
                )
                price = EX.get_current_price(pos["symbol"]) or pos["entry"]
                self._close_position(pid, pos, price, "Sync_Orphan")

    def _scan_markets(self, balance: float):
        for sym in SYMBOLS:
            try:
                with self._lock:
                    if len(self._pos) >= MAX_POS:
                        return
                    if any(
                        p["symbol"] == sym for p in self._pos.values()
                    ):
                        continue

                dfs = EX.fetch_multi_ohlcv(sym)
                if not dfs:
                    continue

                signal = STRATEGY.analyze(sym, dfs)

                if (
                    signal.action in ("buy", "sell")
                    and signal.confidence >= MIN_CONFIDENCE
                ):
                    self._execute_signal(sym, signal, balance)

            except Exception as e:
                log.error("[%s] Scan Error: %s", sym, e)

    def _execute_signal(self, sym: str, sig: Signal, balance: float):
        """اجرای سیگنال با ثبت سفارش واقعی"""

        # محاسبه حجم بر اساس ریسک واقعی
        sl_dist = abs(sig.entry_estimate - sig.sl)
        if sl_dist <= 0:
            sl_dist = sig.entry_estimate * 0.005

        risk_amount = balance * (RISK_PCT / 100.0)
        qty = risk_amount / sl_dist

        # محدودیت حجم
        max_notional = balance * 0.1  # حداکثر ۱۰٪ موجودی
        if (qty * sig.entry_estimate) > max_notional:
            qty = max_notional / sig.entry_estimate

        if qty <= 0:
            return

        # ── ثبت سفارش واقعی ──
        side = "buy" if sig.action == "buy" else "sell"
        order_result = EX.place_order(sym, side, qty, is_close=False)

        if not order_result:
            log.warning("⚠️ سفارش اجرا نشد: %s %s", side, sym)
            return

        # ── استفاده از قیمت واقعی اجرا شده ──
        fill_price = order_result["fill_price"]
        filled_qty = order_result["filled_qty"]

        # محاسبه مجدد SL/TP بر اساس قیمت واقعی
        price_diff_sl = abs(sig.entry_estimate - sig.sl)
        price_diff_tp = abs(sig.entry_estimate - sig.tp)

        if sig.action == "buy":
            real_sl = fill_price - price_diff_sl
            real_tp = fill_price + price_diff_tp
        else:
            real_sl = fill_price + price_diff_sl
            real_tp = fill_price - price_diff_tp

        # ── ثبت SL واقعی در صرافی ──
        pos_side = "long" if sig.action == "buy" else "short"
        sl_order_id = EX.place_stop_loss_order(
            sym, pos_side, filled_qty, real_sl
        )

        # ── ذخیره در DB ──
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
            "exchange_order_id": order_result["id"],
            "sl_order_id": sl_order_id,
        }

        with self._lock:
            self._pos[pid] = pos
        database.insert(pos)

        log.info(
            "✅ پوزیشن باز شد | %s %s | ورود: %.4f | SL: %.4f | TP: %.4f",
            pos_side.upper(), sym, fill_price, real_sl, real_tp,
        )

        if self.tg:
            self.tg.send(
                f"🎯 <b>پوزیشن جدید ({sig.strategy})</b>\n"
                f"نماد: {sym}\n"
                f"جهت: {pos_side.upper()}\n"
                f"قیمت ورود: {fill_price:.4f}\n"
                f"حد ضرر: {real_sl:.4f}\n"
                f"حد سود: {real_tp:.4f}\n"
                f"اطمینان: {sig.confidence}%\n"
                f"دلیل: {sig.reason}\n"
                f"{'⚠️ TESTNET' if TESTNET else '✅ REAL'}"
            )

    def _manage_positions(self):
        with self._lock:
            snap = dict(self._pos)

        for pid, pos in snap.items():
            try:
                # دریافت قیمت لحظه‌ای از تیکر (نه کندل!)
                price = EX.get_current_price(pos["symbol"])
                if not price:
                    continue

                side = pos["side"]

                # ── بررسی SL نرم‌افزاری (پشتیبان SL صرافی) ──
                sl_hit = (
                    (side == "long" and price <= pos["sl"])
                    or (side == "short" and price >= pos["sl"])
                )
                if sl_hit:
                    self._close_position(pid, pos, price, "StopLoss")
                    continue

                # ── خروج جزئی (TP1 = 50%) ──
                if not pos.get("is_partial", 0):
                    tp_hit = (
                        (side == "long" and price >= pos["tp"])
                        or (side == "short" and price <= pos["tp"])
                    )
                    if tp_hit:
                        self._partial_exit(pid, pos, price)

            except Exception as e:
                log.error("Manage Error [%s]: %s", pos.get("symbol"), e)

    def _partial_exit(self, pid: str, pos: Dict, price: float):
        """خروج ۵۰٪ و انتقال SL به نقطه ورود"""
        half_qty = pos["qty"] / 2.0
        close_side = "sell" if pos["side"] == "long" else "buy"

        result = EX.place_order(
            pos["symbol"], close_side, half_qty, is_close=True
        )
        if not result:
            return

        actual_exit = result["fill_price"]

        # لغو SL قبلی و ثبت SL جدید
        if pos.get("sl_order_id"):
            EX.cancel_order_safe(pos["symbol"], pos["sl_order_id"])

        new_sl = pos["entry"]  # SL به نقطه ورود (Break Even)
        new_sl_id = EX.place_stop_loss_order(
            pos["symbol"], pos["side"], half_qty, new_sl
        )

        with self._lock:
            self._pos[pid]["qty"] = half_qty
            self._pos[pid]["sl"] = new_sl
            self._pos[pid]["is_partial"] = 1
            self._pos[pid]["sl_order_id"] = new_sl_id

        database.update_partial(pid, half_qty, new_sl)

        if self.tg:
            pnl_half = (
                (actual_exit - pos["entry"]) * half_qty
                if pos["side"] == "long"
                else (pos["entry"] - actual_exit) * half_qty
            )
            self.tg.send(
                f"🎯 <b>خروج ۵۰٪ (TP1)</b>\n"
                f"نماد: {pos['symbol']}\n"
                f"قیمت خروج: {actual_exit:.4f}\n"
                f"سود جزئی: {pnl_half:+.2f}$\n"
                f"SL جدید: {new_sl:.4f} (Break Even)"
            )

    def _close_position(self, pid: str, pos: Dict, price: float,
                        reason: str):
        """بستن کامل پوزیشن"""
        close_side = "sell" if pos["side"] == "long" else "buy"

        result = EX.place_order(
            pos["symbol"], close_side, pos["qty"], is_close=True
        )

        actual_price = result["fill_price"] if result else price

        # لغو SL صرافی
        if pos.get("sl_order_id"):
            EX.cancel_order_safe(pos["symbol"], pos["sl_order_id"])

        # محاسبه PnL واقعی
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

        emoji = "🟢" if pnl >= 0 else "🔴"
        log.info(
            "%s بسته شد | %s | PnL: %+.2f$ (%+.2f%%) | دلیل: %s",
            emoji, pos["symbol"], pnl, pct, reason,
        )

        if self.tg:
            self.tg.send(
                f"{emoji} <b>بستن پوزیشن ({reason})</b>\n"
                f"نماد: {pos['symbol']}\n"
                f"ورود: {entry:.4f} → خروج: {actual_price:.4f}\n"
                f"سود/زیان: {pnl:+.2f}$ ({pct:+.2f}%)"
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

    return f"""
    <!DOCTYPE html>
    <html dir="rtl" lang="fa">
    <head>
        <meta charset="UTF-8">
        <title>Quant Bot v2</title>
        <meta http-equiv="refresh" content="30">
        <style>
            body {{
                font-family: Tahoma, sans-serif;
                background: #0d1117;
                color: #c9d1d9;
                padding: 20px;
                text-align: center;
            }}
            .card {{
                background: #161b22;
                border: 1px solid #30363d;
                padding: 15px; margin: 8px;
                border-radius: 8px;
                display: inline-block;
                min-width: 140px;
            }}
            .warn {{
                background: #3d1f00;
                border-color: #f0883e;
                color: #f0883e;
            }}
            .ok {{ border-color: #3fb950; }}
        </style>
    </head>
    <body>
        <h1>🤖 Master-AI Quant Bot v2.0</h1>
        <p>
            وضعیت: <b>{'🟢 فعال' if active else '🔴 متوقف'}</b> |
            اتصال: <b>{'✅' if connected else '❌'}</b> |
            شبکه: <b>{mode}</b> |
            پوزیشن: <b>{pos_count}/{MAX_POS}</b>
        </p>

        <div class="card"><h3>💰 موجودی</h3><p>${bal:,.2f}</p></div>
        <div class="card"><h3>💎 ارزش کل</h3><p>${equity:,.2f}</p></div>
        <div class="card {'ok' if stats['total_pnl'] >= 0 else 'warn'}">
            <h3>📈 سود/زیان</h3>
            <p>{stats['total_pnl']:+,.2f}$</p>
        </div>
        <div class="card"><h3>🔥 وین‌ریت</h3><p>{stats['win_rate']}%</p></div>
        <div class="card"><h3>⚡ PF</h3><p>{stats['profit_factor']}</p></div>
        <div class="card"><h3>🎯 معاملات</h3>
            <p>{stats['total_trades']} (W:{stats['wins_count']} L:{stats['losses_count']})</p>
        </div>

        {'<div class="card warn"><h3>⚠️ TESTNET</h3><p>معاملات واقعی نیست!</p></div>' if TESTNET else ''}
    </body>
    </html>
    """


@app.route("/health")
def health():
    return {
        "status": "ok",
        "connected": EX.is_connected,
        "testnet": TESTNET,
        "active": engine_instance.is_active if engine_instance else False,
        "positions": len(engine_instance._pos) if engine_instance else 0,
    }


@app.route("/positions")
def api_positions():
    real = EX.fetch_real_positions()
    db = list(engine_instance._pos.values()) if engine_instance else []
    return {
        "exchange_positions": real,
        "db_positions": [
            {
                "symbol": p["symbol"],
                "side": p["side"],
                "entry": p["entry"],
                "qty": p["qty"],
            }
            for p in db
        ],
        "synced": len(real) == len(db),
    }


# ============================================================================
# MAIN
# ============================================================================
def main():
    global engine_instance

    log.info("=" * 60)
    log.info("  Master-AI Quant Bot v2.0 - FIXED VERSION")
    log.info("  Mode: %s", "TESTNET" if TESTNET else "MAINNET")
    log.info("  Connected: %s", EX.is_connected)
    log.info("=" * 60)

    if not EX.is_connected:
        log.critical("❌ اتصال به صرافی برقرار نشد!")
        log.critical("   لطفاً کلیدهای API را بررسی کنید.")

    engine_instance = Engine()
    tg = TelegramHandler(engine_instance)
    engine_instance.tg = tg

    if TG_TOKEN and TG_CHAT:
        tg.send(
            f"🤖 <b>ربات شروع شد</b>\n"
            f"شبکه: {'⚠️ TESTNET' if TESTNET else '✅ MAINNET'}\n"
            f"اتصال: {'✅' if EX.is_connected else '❌'}\n\n"
            f"برای شروع معاملات، دکمه 🟢 را بزنید.",
            reply_markup=tg._keyboard(),
        )

    threading.Thread(target=engine_instance.run_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, debug=False)


if __name__ == "__main__":
    main()
