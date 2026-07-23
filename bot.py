#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Master-AI Quant Bot v4.8.1 - OPTIMIZED & FIXED
نسخه اصلاح‌شده برای افزایش فرصت‌های معاملاتی و رفع خطاهای مانیتورینگ
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
log = logging.getLogger("MasterQuant_v4.8.1")


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

RISK_PCT = Cfg.f("RISK_PER_TRADE", 0.5)
MAX_DD = Cfg.f("MAX_DRAWDOWN", 15.0)
MAX_POS = Cfg.i("MAX_POSITIONS", 3)  # افزایش تعداد پوزیشن‌های مجاز برای پویایی بیشتر
LEVERAGE = Cfg.i("LEVERAGE", 5)
TESTNET = Cfg.b("PHEMEX_TESTNET", True)
PORT = Cfg.i("PORT", 10000)
SCAN_INTERVAL = Cfg.i("SCAN_INTERVAL", 60) # کاهش زمان اسکن برای واکنش سریع‌تر
MIN_CONFIDENCE = Cfg.i("MIN_CONFIDENCE", 65)
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

ATR_MULTIPLIER_SL = 1.5
ATR_MULTIPLIER_TP = 3.0


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
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ], axis=1).max(axis=1)
        return tr.ewm(com=n - 1, adjust=False).mean()

    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
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
        plus_di = 100 * (pd.Series(plus_dm).ewm(com=n - 1, adjust=False).mean() / (atr_val + 1e-10))
        minus_di = 100 * (pd.Series(minus_dm).ewm(com=n - 1, adjust=False).mean() / (atr_val + 1e-10))
        dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10))
        return dx.ewm(com=n - 1, adjust=False).mean()

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
            id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            fill_price REAL,
            exit_price REAL,
            quantity REAL NOT NULL,
            filled_quantity REAL DEFAULT 0,
            stop_loss REAL NOT NULL,
            take_profit REAL NOT NULL,
            status TEXT DEFAULT 'open',
            strategy TEXT,
            confidence INTEGER DEFAULT 0,
            pnl REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0,
            is_partial INTEGER DEFAULT 0,
            exit_reason TEXT,
            exchange_order_id TEXT,
            sl_order_id TEXT,
            contracts INTEGER DEFAULT 0,
            opened_at TEXT DEFAULT CURRENT_TIMESTAMP,
            closed_at TEXT,
            is_real INTEGER DEFAULT 1
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

    def close(self, tid: str, ep: float, pnl: float, pct: float, reason: str):
        self.run(
            "UPDATE trades SET status='closed',exit_price=?,pnl=?,"
            "pnl_pct=?,exit_reason=?,closed_at=CURRENT_TIMESTAMP WHERE id=?",
            (ep, pnl, pct, reason, tid),
        )

    def get_analytics(self) -> Dict:
        rows = self.run("SELECT pnl, pnl_pct FROM trades WHERE status='closed' AND is_real=1")
        if not rows:
            return {"total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0, "profit_factor": 0.0}
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
            "profit_factor": round(sum(wins) / sum(losses), 2) if sum(losses) > 0 else round(sum(wins), 2),
        }


database = DB()


# ============================================================================
# EXCHANGE ENGINE
# ============================================================================
class Exchange:
    def __init__(self):
        self._ex: Optional[ccxt.phemex] = None
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
                "timeout": REQUEST_TIMEOUT * 1000,
            })
            if TESTNET:
                self._ex.set_sandbox_mode(True)
                log.warning("⚠️ حالت TESTNET فعال است!")

            self._ex.load_markets()
            self._set_leverage_all()
            self._connected = True
            log.info("✅ اتصال به Phemex برقرار شد.")
        except Exception as e:
            log.error("❌ خطای اتصال: %s", e)

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

    def fetch_multi_ohlcv(self, sym: str) -> Dict[str, pd.DataFrame]:
        result = {}
        for tf in ["15m", "5m"]:
            try:
                raw = self._ex.fetch_ohlcv(sym, tf, limit=60)
                if raw and len(raw) >= 20:
                    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "vol"])
                    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
                    result[tf] = df
            except Exception as e:
                log.debug(f"Error fetching {sym} {tf}: {e}")
        if "15m" in result and "5m" not in result:
            result["5m"] = result["15m"].copy()
        return result

    def get_current_price(self, sym: str) -> Optional[float]:
        if not self.is_connected:
            return None
        try:
            ticker = self._ex.fetch_ticker(sym)
            return float(ticker.get("last", 0))
        except Exception:
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

    def place_order(self, sym: str, side: str, qty: float, is_close: bool = False) -> Optional[Dict]:
        if not self.is_connected:
            return None
        try:
            current_price = self.get_current_price(sym)
            if not current_price:
                return None

            contract_size = self.get_contract_size(sym)
            contracts = int(round(qty / contract_size))
            if contracts < 1:
                contracts = 1

            params = {"reduceOnly": True} if is_close else {}
            if side.lower() == "buy":
                result = self._ex.create_market_buy_order(sym, contracts, params=params)
            else:
                result = self._ex.create_market_sell_order(sym, contracts, params=params)

            fill_price = float(result.get("average") or result.get("price") or current_price)
            return {
                "id": result.get("id"),
                "fill_price": fill_price,
                "filled_qty": contracts * contract_size,
                "status": result.get("status"),
            }
        except Exception as e:
            log.error("❌ خطای سفارش [%s %s]: %s", side, sym, e)
            return None


EX = Exchange()


# ============================================================================
# STRATEGY ENGINE - OPTIMIZED & RELAXED FILTERS
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
        if "15m" not in dfs or "5m" not in dfs:
            return Signal(debug_info="داده ناکافی")

        df15 = dfs["15m"]
        df5 = dfs["5m"]

        close15 = df15["close"]
        high15 = df15["high"]
        low15 = df15["low"]

        ema20_15 = IND.ema(close15, 20)
        ema50_15 = IND.ema(close15, 50)
        adx15 = IND.adx(high15, low15, close15, 14)
        atr15 = IND.atr(high15, low15, close15, 14)

        price15 = IND.safe(close15)
        ema20_val = IND.safe(ema20_15)
        ema50_val = IND.safe(ema50_15)
        adx_val = IND.safe(adx15)
        atr_val = IND.safe(atr15)

        # اصلاح منطق روند برای سخت‌گیری کمتر و باز شدن دست ربات برای معامله
        uptrend = (price15 > ema50_val) and (adx_val > 18)
        downtrend = (price15 < ema50_val) and (adx_val > 18)

        if not uptrend and not downtrend:
            return Signal(debug_info=f"روند خنثی - ADX={adx_val:.1f}")

        close5 = df5["close"]
        high5 = df5["high"]
        low5 = df5["low"]
        vol5 = df5["vol"]

        current_price = IND.safe(close5)
        high_20 = IND.safe(high5.rolling(20).max())
        low_20 = IND.safe(low5.rolling(20).min())
        avg_vol = IND.safe(vol5.rolling(20).mean())
        current_vol = IND.safe(vol5)

        # استراتژی ۱: Breakout (با شرط حجم ملایم‌تر)
        if uptrend and current_price > high_20 and current_vol > avg_vol * 1.2:
            sl = current_price - (ATR_MULTIPLIER_SL * atr_val)
            tp = current_price + (ATR_MULTIPLIER_TP * atr_val)
            return Signal("buy", "Breakout", 75, "شکست سقف با حجم مناسب", sl, tp, current_price, "OK")

        elif downtrend and current_price < low_20 and current_vol > avg_vol * 1.2:
            sl = current_price + (ATR_MULTIPLIER_SL * atr_val)
            tp = current_price - (ATR_MULTIPLIER_TP * atr_val)
            return Signal("sell", "Breakout", 75, "شکست کف با حجم مناسب", sl, tp, current_price, "OK")

        # استراتژی ۲: Pullback به EMA20
        if uptrend and abs(current_price - ema20_val) / ema20_val < 0.015:
            sl = current_price - (ATR_MULTIPLIER_SL * atr_val)
            tp = current_price + (ATR_MULTIPLIER_TP * atr_val)
            return Signal("buy", "Pullback", 70, "پولبک به EMA20", sl, tp, current_price, "OK")

        elif downtrend and abs(current_price - ema20_val) / ema20_val < 0.015:
            sl = current_price + (ATR_MULTIPLIER_SL * atr_val)
            tp = current_price - (ATR_MULTIPLIER_TP * atr_val)
            return Signal("sell", "Pullback", 70, "پولبک به EMA20", sl, tp, current_price, "OK")

        return Signal(debug_info="سیگنالی یافت نشد")


STRATEGY = StrategyEngine()


# ============================================================================
# MAIN BOT ENGINE LOOP (تکمیل شده)
# ============================================================================
class QuantBot:
    def __init__(self):
        self.is_active = True

    def run_loop(self):
        log.info("🚀 ربات معاملاتی استارت شد...")
        while True:
            if not self.is_active:
                time.sleep(5)
                continue

            try:
                open_pos = database.open_trades()
                if len(open_pos) < MAX_POS:
                    for sym in SYMBOLS:
                        if any(p["symbol"] == sym for p in open_pos):
                            continue
                        
                        dfs = EX.fetch_multi_ohlcv(sym)
                        signal = STRATEGY.analyze(sym, dfs)

                        if signal.action in ["buy", "sell"] and signal.confidence >= MIN_CONFIDENCE:
                            bal = EX.balance()
                            risk_usd = bal * (RISK_PCT / 100) * LEVERAGE
                            qty = risk_usd / signal.entry_estimate if signal.entry_estimate > 0 else 0

                            if qty > 0:
                                side = "buy" if signal.action == "buy" else "sell"
                                order = EX.place_order(sym, side, qty)
                                if order:
                                    trade_id = str(uuid.uuid4())[:8]
                                    database.insert({
                                        "id": trade_id,
                                        "symbol": sym,
                                        "side": signal.action,
                                        "entry": signal.entry_estimate,
                                        "fill_price": order["fill_price"],
                                        "qty": order["filled_qty"],
                                        "sl": signal.sl,
                                        "tp": signal.tp,
                                        "strategy": signal.strategy,
                                        "conf": signal.confidence,
                                        "exchange_order_id": order["id"],
                                        "contracts": int(order["filled_qty"] / EX.get_contract_size(sym))
                                    })
                                    log.info(f"✅ معامله جدید باز شد: {sym} ({signal.action})")
                
                time.sleep(SCAN_INTERVAL)
            except Exception as e:
                log.error(f"خطا در حلقه اصلی: {e}")
                time.sleep(10)


if __name__ == "__main__":
    bot = QuantBot()
    bot.run_loop()
