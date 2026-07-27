#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Master Quant Engine v16.0
Phemex Testnet only — safer data, execution, persistence, and diagnostics.

Required environment variables:
PHEMEX_API_KEY
PHEMEX_API_SECRET
PHEMEX_TESTNET=true
BOT_MODE=testnet

Optional:
AUTO_TRADING=false
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
TELEGRAM_ALLOWED_CHAT_ID=
DASHBOARD_USER=
DASHBOARD_PASSWORD=
PORT=10000
"""

import asyncio
import json
import logging
import os
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import ccxt.async_support as ccxt
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash, generate_password_hash


# =============================================================================
# CONFIGURATION
# =============================================================================

load_dotenv()

APP_VERSION = "16.0"
BOT_MODE = os.getenv("BOT_MODE", "testnet").strip().lower()
TESTNET = os.getenv("PHEMEX_TESTNET", "true").strip().lower() in ("true", "1", "yes")

if BOT_MODE != "testnet" or not TESTNET:
    raise RuntimeError(
        "Safety lock enabled: BOT_MODE must be testnet and PHEMEX_TESTNET must be true."
    )

PHEMEX_API_KEY = os.getenv("PHEMEX_API_KEY", "")
PHEMEX_API_SECRET = os.getenv("PHEMEX_API_SECRET", "")

AUTO_TRADING = os.getenv("AUTO_TRADING", "false").strip().lower() in (
    "true",
    "1",
    "yes",
)

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TG_ALLOWED_CHAT_ID = os.getenv("TELEGRAM_ALLOWED_CHAT_ID", TG_CHAT_ID)

DASHBOARD_USER = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")
DASHBOARD_PASSWORD_HASH = (
    generate_password_hash(DASHBOARD_PASSWORD) if DASHBOARD_PASSWORD else ""
)

PORT = int(os.getenv("PORT", "10000"))

SYMBOLS = [
    "ETH/USDT:USDT",
    "XRP/USDT:USDT",
]

TIMEFRAME = "5m"
HTF_TIMEFRAME = "1h"

CANDLE_LIMIT_5M = 180
CANDLE_LIMIT_1H = 260

SCAN_INTERVAL_SECONDS = 60
PRICE_LOOP_SECONDS = 8
WATCHDOG_SECONDS = 3
SYNC_INTERVAL_SECONDS = 120

LEVERAGE = 3

RISK_PCT_PER_TRADE = 0.25
MAX_OPEN_POSITIONS = 10
MAX_DIRECTIONAL_POSITIONS = 2
MAX_PORTFOLIO_RISK_PCT = 0.60
MAX_NOTIONAL_EXPOSURE_PCT = 20.0

MAX_DRAWDOWN_PCT = 5.0
MAX_DAILY_LOSS_PCT = 2.0
CONSECUTIVE_LOSS_LIMIT = 2
SYMBOL_COOLDOWN_SECONDS = 4 * 60 * 60

MIN_ORDER_USD = 15.0
MAX_SPREAD_PCT = 0.35
MAX_ENTRY_DEVIATION_PCT = 0.45

TAKER_FEE_RATE = 0.0006
FEE_BUFFER = 1.15
ESTIMATED_SLIPPAGE_PCT = 0.03

PARTIAL_TP_ENABLED = True
PARTIAL_CLOSE_FRACTION = 0.50

TRAIL_ACTIVATION_PCT = 1.20
TRAIL_DISTANCE_PCT = 0.55
EMERGENCY_STOP_PCT = 1.25

DATA_DIR = Path("quant_data")
STATE_FILE = DATA_DIR / "state.json"
EVENT_FILE = DATA_DIR / "events.jsonl"
TRADES_FILE = DATA_DIR / "trades.json"

STRATEGY_PARAMS = {
    "Breakout_Momentum": {"sl_atr": 1.15, "tp1_atr": 1.40, "tp_atr": 2.60},
    "MTF_Pullback": {"sl_atr": 1.20, "tp1_atr": 1.30, "tp_atr": 2.40},
    "SuperTrend_Pullback": {"sl_atr": 1.15, "tp1_atr": 1.25, "tp_atr": 2.30},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler("quant_v16.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

log = logging.getLogger("QuantV16")
STATE_LOCK = Lock()

SHARED_STATE: Dict[str, Any] = {
    "version": APP_VERSION,
    "mode": BOT_MODE,
    "auto_trading": AUTO_TRADING,
    "is_active": True,
    "risk_halted": False,
    "daily_halted": False,
    "drawdown_halted": False,
    "balance": 0.0,
    "free_balance": 0.0,
    "peak_balance": 0.0,
    "day_start_balance": 0.0,
    "day_marker": "",
    "daily_pnl": 0.0,
    "daily_pnl_pct": 0.0,
    "current_drawdown_pct": 0.0,
    "active_positions": {},
    "last_scan": "Never",
    "last_sync": "Never",
    "last_error": "",
    "stats": {
        "closed_trades": 0,
        "win_rate": 0.0,
        "net_pnl": 0.0,
        "profit_factor": 0.0,
        "average_win": 0.0,
        "average_loss": 0.0,
    },
}

SYMBOL_COOLDOWNS: Dict[str, float] = {}
SYMBOL_LOSS_STREAKS: Dict[str, int] = {}


# =============================================================================
# HELPERS
# =============================================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def timestamp_now() -> float:
    return time.time()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def side_sign(side: str) -> int:
    return 1 if side.lower() == "buy" else -1


def timeframe_seconds(timeframe: str) -> int:
    values = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "1d": 86400,
    }
    return values.get(timeframe, 3600)


def current_day_marker() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def allowed_telegram_chat(chat_id: Any) -> bool:
    return bool(TG_ALLOWED_CHAT_ID) and str(chat_id) == str(TG_ALLOWED_CHAT_ID)


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}

    if isinstance(value, list):
        return [json_safe(v) for v in value]

    if isinstance(value, tuple):
        return [json_safe(v) for v in value]

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    return str(value)


# =============================================================================
# PERSISTENCE AND REPORTING
# =============================================================================

class Storage:
    def __init__(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def load_state(self) -> Dict[str, Any]:
        if not STATE_FILE.exists():
            return {}

        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception as error:
            log.warning("Could not load saved state: %s", error)
            return {}

    def save_state(self, state: Dict[str, Any]) -> None:
        temporary = STATE_FILE.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(json_safe(state), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(STATE_FILE)

    def load_trades(self) -> List[Dict[str, Any]]:
        if not TRADES_FILE.exists():
            return []

        try:
            data = json.loads(TRADES_FILE.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception as error:
            log.warning("Could not load trade history: %s", error)
            return []

    def save_trades(self, trades: List[Dict[str, Any]]) -> None:
        temporary = TRADES_FILE.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(json_safe(trades), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(TRADES_FILE)

    def add_event(
        self,
        event_type: str,
        symbol: str = "",
        message: str = "",
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        event = {
            "time": utc_now(),
            "type": event_type,
            "symbol": symbol,
            "message": message,
            "details": json_safe(details or {}),
        }

        with EVENT_FILE.open("a", encoding="utf-8") as file:
            file.write(json.dumps(event, ensure_ascii=False) + "\n")

    def recent_events(self, limit: int = 250) -> List[Dict[str, Any]]:
        if not EVENT_FILE.exists():
            return []

        try:
            rows = EVENT_FILE.read_text(encoding="utf-8").splitlines()
            events = [json.loads(row) for row in rows[-limit:]]
            return list(reversed(events))
        except Exception:
            return []

    def calculate_stats(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        closed = [trade for trade in trades if trade.get("status") == "closed"]
        pnls = [safe_float(trade.get("net_pnl")) for trade in closed]

        if not pnls:
            return {
                "closed_trades": 0,
                "win_rate": 0.0,
                "net_pnl": 0.0,
                "profit_factor": 0.0,
                "average_win": 0.0,
                "average_loss": 0.0,
            }

        wins = [pnl for pnl in pnls if pnl > 0]
        losses = [pnl for pnl in pnls if pnl < 0]

        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))

        return {
            "closed_trades": len(closed),
            "win_rate": round(len(wins) / len(closed) * 100.0, 2),
            "net_pnl": round(sum(pnls), 4),
            "profit_factor": round(
                gross_profit / gross_loss if gross_loss > 0 else 0.0,
                3,
            ),
            "average_win": round(sum(wins) / len(wins), 4) if wins else 0.0,
            "average_loss": round(sum(losses) / len(losses), 4) if losses else 0.0,
        }

    def generate_report(self, prices: Dict[str, float]) -> str:
        trades = self.load_trades()
        events = self.recent_events(300)

        with STATE_LOCK:
            state = json.loads(json.dumps(SHARED_STATE))

        stats = self.calculate_stats(trades)

        closed = [trade for trade in trades if trade.get("status") == "closed"]
        open_trades = [trade for trade in trades if trade.get("status") == "open"]

        strategy_results: Dict[str, List[float]] = defaultdict(list)
        symbol_results: Dict[str, List[float]] = defaultdict(list)

        for trade in closed:
            pnl = safe_float(trade.get("net_pnl"))
            strategy_results[trade.get("strategy", "Unknown")].append(pnl)
            symbol_results[trade.get("symbol", "Unknown")].append(pnl)

        rejection_counts = Counter()
        decisions_by_symbol = defaultdict(lambda: {"signals": 0, "rejections": 0})

        for event in events:
            if event.get("type") != "decision":
                continue

            symbol = event.get("symbol", "Unknown")
            action = event.get("details", {}).get("action", "neutral")

            if action in ("buy", "sell"):
                decisions_by_symbol[symbol]["signals"] += 1
            else:
                decisions_by_symbol[symbol]["rejections"] += 1
                rejection_counts[event.get("message", "Unknown")] += 1

        lines: List[str] = []
        lines.append("=" * 78)
        lines.append(f"MASTER QUANT ENGINE v{APP_VERSION} — TESTNET DIAGNOSTIC REPORT")
        lines.append(f"Generated: {utc_now()}")
        lines.append("=" * 78)
        lines.append("")
        lines.append("1. RUNTIME AND SAFETY")
        lines.append(f"Mode: {state['mode']}")
        lines.append(f"Auto trading: {state['auto_trading']}")
        lines.append(f"Bot active: {state['is_active']}")
        lines.append(f"Risk halted: {state['risk_halted']}")
        lines.append(f"Balance: ${state['balance']:.2f}")
        lines.append(f"Free balance: ${state['free_balance']:.2f}")
        lines.append(f"Peak balance: ${state['peak_balance']:.2f}")
        lines.append(f"Drawdown: {state['current_drawdown_pct']:.2f}%")
        lines.append(
            f"Daily PnL: ${state['daily_pnl']:.2f} "
            f"({state['daily_pnl_pct']:.2f}%)"
        )
        lines.append(f"Open positions: {len(state['active_positions'])}/{MAX_OPEN_POSITIONS}")
        lines.append(f"Last scan: {state['last_scan']}")
        lines.append(f"Last sync: {state['last_sync']}")
        lines.append("")

        lines.append("2. PERFORMANCE")
        lines.append(f"Closed trades: {stats['closed_trades']}")
        lines.append(f"Win rate: {stats['win_rate']:.2f}%")
        lines.append(f"Net PnL: ${stats['net_pnl']:.4f}")
        lines.append(f"Profit factor: {stats['profit_factor']:.3f}")
        lines.append(f"Average win: ${stats['average_win']:.4f}")
        lines.append(f"Average loss: ${stats['average_loss']:.4f}")
        lines.append("")

        lines.append("3. OPEN POSITIONS")
        if not open_trades:
            lines.append("No local open positions.")
        else:
            for trade in open_trades:
                symbol = trade["symbol"]
                entry = safe_float(trade["entry"])
                price = safe_float(prices.get(symbol), entry)
                quantity = safe_float(trade["qty"])
                unrealized = (price - entry) * quantity * side_sign(trade["side"])

                lines.append(
                    f"{symbol} | {trade['side'].upper()} | "
                    f"strategy={trade['strategy']}"
                )
                lines.append(
                    f"  entry={entry:.6f} current={price:.6f} qty={quantity:.6f} "
                    f"unrealized=${unrealized:+.4f}"
                )
                lines.append(
                    f"  SL={safe_float(trade['sl']):.6f} "
                    f"TP1={safe_float(trade['tp1']):.6f} "
                    f"TP={safe_float(trade['tp']):.6f}"
                )
                lines.append(
                    f"  exchange stop ID: {trade.get('stop_order_id') or 'not confirmed'}"
                )
        lines.append("")

        lines.append("4. RECENT CLOSED TRADES")
        if not closed:
            lines.append("No closed trades recorded.")
        else:
            for trade in closed[-20:][::-1]:
                lines.append(
                    f"{trade['symbol']} | {trade['side'].upper()} | "
                    f"PnL=${safe_float(trade.get('net_pnl')):+.4f} | "
                    f"fees=${safe_float(trade.get('fees')):.4f}"
                )
                lines.append(
                    f"  strategy={trade.get('strategy')} "
                    f"reason={trade.get('exit_reason')} "
                    f"opened={trade.get('opened_at')} "
                    f"closed={trade.get('closed_at')}"
                )
        lines.append("")

        lines.append("5. PERFORMANCE BY STRATEGY")
        if not strategy_results:
            lines.append("No completed strategy results yet.")
        else:
            for strategy, pnls in sorted(strategy_results.items()):
                wins = len([pnl for pnl in pnls if pnl > 0])
                rate = wins / len(pnls) * 100.0
                lines.append(
                    f"{strategy}: trades={len(pnls)} "
                    f"win_rate={rate:.1f}% net=${sum(pnls):+.4f}"
                )
        lines.append("")

        lines.append("6. PERFORMANCE BY SYMBOL")
        if not symbol_results:
            lines.append("No completed symbol results yet.")
        else:
            for symbol, pnls in sorted(symbol_results.items()):
                wins = len([pnl for pnl in pnls if pnl > 0])
                rate = wins / len(pnls) * 100.0
                lines.append(
                    f"{symbol}: trades={len(pnls)} "
                    f"win_rate={rate:.1f}% net=${sum(pnls):+.4f}"
                )
        lines.append("")

        lines.append("7. DECISION QUALITY")
        lines.append(f"Recorded decision events: {sum(sum(v.values()) for v in decisions_by_symbol.values())}")

        lines.append("Top rejection reasons:")
        if rejection_counts:
            for reason, count in rejection_counts.most_common(12):
                lines.append(f"- {count}x {reason}")
        else:
            lines.append("- No rejections recorded.")

        lines.append("Per-symbol decisions:")
        if decisions_by_symbol:
            for symbol, values in sorted(decisions_by_symbol.items()):
                lines.append(
                    f"- {symbol}: signals={values['signals']} "
                    f"rejections={values['rejections']}"
                )
        else:
            lines.append("- No decision events yet.")
        lines.append("")

        lines.append("8. RECENT ERRORS AND WARNINGS")
        warnings = [
            event
            for event in events
            if event.get("type") in ("error", "warning", "protection_failed")
        ]

        if not warnings:
            lines.append("No recent errors or warnings.")
        else:
            for event in warnings[:15]:
                lines.append(
                    f"- {event['time']} | {event.get('symbol', '-')} | "
                    f"{event['type']} | {event['message']}"
                )

        lines.append("")
        lines.append("=" * 78)

        return "\n".join(lines)


# =============================================================================
# INDICATORS
# =============================================================================

class Indicators:
    @staticmethod
    def rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)

        average_gain = gains.ewm(com=period - 1, adjust=False).mean()
        average_loss = losses.ewm(com=period - 1, adjust=False).mean()

        ratio = average_gain / average_loss.replace(0, 1e-10)
        return 100 - (100 / (1 + ratio))

    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        true_range = pd.concat(
            [
                df["high"] - df["low"],
                (df["high"] - df["close"].shift()).abs(),
                (df["low"] - df["close"].shift()).abs(),
            ],
            axis=1,
        ).max(axis=1)

        return true_range.ewm(com=period - 1, adjust=False).mean()

    @staticmethod
    def supertrend(
        df: pd.DataFrame,
        period: int = 10,
        multiplier: float = 3.0,
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        atr_value = Indicators.atr(df, period)
        midpoint = (df["high"] + df["low"]) / 2

        upper = midpoint + multiplier * atr_value
        lower = midpoint - multiplier * atr_value
        direction = pd.Series(1, index=df.index)

        for index in range(1, len(df)):
            if df["close"].iloc[index] > upper.iloc[index - 1]:
                direction.iloc[index] = 1
            elif df["close"].iloc[index] < lower.iloc[index - 1]:
                direction.iloc[index] = -1
            else:
                direction.iloc[index] = direction.iloc[index - 1]

            if direction.iloc[index] == 1:
                lower.iloc[index] = max(lower.iloc[index], lower.iloc[index - 1])
            else:
                upper.iloc[index] = min(upper.iloc[index], upper.iloc[index - 1])

        return direction, upper, lower


# =============================================================================
# STRATEGY
# =============================================================================

class StrategyEngine:
    def analyze(self, df_5m: pd.DataFrame, df_1h: pd.DataFrame) -> Dict[str, Any]:
        df = df_5m.iloc[:-1].copy()
        htf = df_1h.iloc[:-1].copy()

        if len(df) < 80:
            return self.neutral("Insufficient completed 5m candles")

        if len(htf) < 220:
            return self.neutral("Insufficient completed 1h candles")

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]

        htf_close = htf["close"]
        htf_ema_50 = safe_float(htf_close.ewm(span=50, adjust=False).mean().iloc[-1])
        htf_ema_200 = safe_float(htf_close.ewm(span=200, adjust=False).mean().iloc[-1])
        htf_price = safe_float(htf_close.iloc[-1])

        if htf_price > htf_ema_50 and htf_ema_50 > htf_ema_200:
            htf_trend = "bullish"
        elif htf_price < htf_ema_50 and htf_ema_50 < htf_ema_200:
            htf_trend = "bearish"
        else:
            return self.neutral("HTF trend not aligned", htf_trend="sideways")

        atr_series = Indicators.atr(df, 14)
        atr = safe_float(atr_series.iloc[-1])
        atr_average = safe_float(atr_series.rolling(20).mean().iloc[-1])

        if atr <= 0 or atr_average <= 0:
            return self.neutral("Invalid ATR", atr=atr, htf_trend=htf_trend)

        if atr < atr_average * 0.45 or atr > atr_average * 2.8:
            return self.neutral(
                "Volatility outside allowed range",
                atr=atr,
                htf_trend=htf_trend,
            )

        rsi_series = Indicators.rsi(close, 14)
        rsi = safe_float(rsi_series.iloc[-1])
        previous_rsi = safe_float(rsi_series.iloc[-2])

        price = safe_float(close.iloc[-1])
        previous_close = safe_float(close.iloc[-2])

        ema20 = safe_float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = safe_float(close.ewm(span=50, adjust=False).mean().iloc[-1])

        volume_average = safe_float(volume.rolling(20).mean().iloc[-1], 1e-9)
        current_volume = safe_float(volume.iloc[-1])
        volume_ok = current_volume >= volume_average * 1.12

        highest_12 = safe_float(high.rolling(12).max().iloc[-1])
        lowest_12 = safe_float(low.rolling(12).min().iloc[-1])

        supertrend_direction, supertrend_upper, supertrend_lower = Indicators.supertrend(df)

        if (
            htf_trend == "bullish"
            and price > ema20
            and price >= highest_12 * 0.998
            and 48 < rsi < 74
            and volume_ok
        ):
            return self.build("buy", "Breakout_Momentum", price, atr, rsi, htf_trend)

        if (
            htf_trend == "bearish"
            and price < ema20
            and price <= lowest_12 * 1.002
            and 26 < rsi < 52
            and volume_ok
        ):
            return self.build("sell", "Breakout_Momentum", price, atr, rsi, htf_trend)

        if (
            htf_trend == "bullish"
            and price >= ema20
            and ema20 > ema50
            and previous_rsi <= 48
            and rsi > previous_rsi
            and rsi < 64
        ):
            return self.build("buy", "MTF_Pullback", price, atr, rsi, htf_trend)

        if (
            htf_trend == "bearish"
            and price <= ema20
            and ema20 < ema50
            and previous_rsi >= 52
            and rsi < previous_rsi
            and rsi > 36
        ):
            return self.build("sell", "MTF_Pullback", price, atr, rsi, htf_trend)

        if (
            htf_trend == "bullish"
            and supertrend_direction.iloc[-1] == 1
            and low.iloc[-1] <= supertrend_lower.iloc[-1] * 1.006
            and price > previous_close
            and 40 < rsi < 67
        ):
            return self.build("buy", "SuperTrend_Pullback", price, atr, rsi, htf_trend)

        if (
            htf_trend == "bearish"
            and supertrend_direction.iloc[-1] == -1
            and high.iloc[-1] >= supertrend_upper.iloc[-1] * 0.994
            and price < previous_close
            and 33 < rsi < 60
        ):
            return self.build("sell", "SuperTrend_Pullback", price, atr, rsi, htf_trend)

        return self.neutral(
            f"No qualified signal (RSI={rsi:.1f})",
            rsi=rsi,
            atr=atr,
            htf_trend=htf_trend,
        )

    def neutral(
        self,
        reason: str,
        rsi: float = 0.0,
        atr: float = 0.0,
        htf_trend: str = "",
    ) -> Dict[str, Any]:
        return {
            "action": "neutral",
            "strategy": "",
            "reason": reason,
            "rsi": rsi,
            "atr": atr,
            "htf_trend": htf_trend,
        }

    def build(
        self,
        side: str,
        strategy: str,
        price: float,
        atr: float,
        rsi: float,
        htf_trend: str,
    ) -> Dict[str, Any]:
        params = STRATEGY_PARAMS[strategy]

        stop_distance = atr * params["sl_atr"]
        tp1_distance = atr * params["tp1_atr"]
        target_distance = atr * params["tp_atr"]

        if side == "buy":
            stop_loss = price - stop_distance
            take_profit_1 = price + tp1_distance
            take_profit = price + target_distance
        else:
            stop_loss = price + stop_distance
            take_profit_1 = price - tp1_distance
            take_profit = price - target_distance

        return {
            "action": side,
            "strategy": strategy,
            "reason": f"Qualified {strategy} signal",
            "sl": stop_loss,
            "tp1": take_profit_1,
            "tp": take_profit,
            "rsi": rsi,
            "atr": atr,
            "htf_trend": htf_trend,
            "expected_rr": round(target_distance / stop_distance, 2),
        }


# =============================================================================
# RISK MANAGEMENT
# =============================================================================

class RiskManager:
    @staticmethod
    def calculate_qty(
        exchange: Any,
        symbol: str,
        balance: float,
        free_balance: float,
        entry: float,
        stop_loss: float,
    ) -> Tuple[float, str]:
        if balance <= 0 or free_balance <= 0 or entry <= 0:
            return 0.0, "Invalid balance or entry price"

        stop_distance = abs(entry - stop_loss)

        if stop_distance <= 0:
            return 0.0, "Invalid stop distance"

        risk_budget = balance * (RISK_PCT_PER_TRADE / 100.0)

        estimated_cost_per_unit = stop_distance + entry * (
            TAKER_FEE_RATE * 2 * FEE_BUFFER + ESTIMATED_SLIPPAGE_PCT / 100.0
        )

        raw_qty = risk_budget / estimated_cost_per_unit

        max_by_free_balance = (free_balance * LEVERAGE * 0.15) / entry
        max_by_notional = (balance * MAX_NOTIONAL_EXPOSURE_PCT / 100.0) / entry

        quantity = min(raw_qty, max_by_free_balance, max_by_notional)

        try:
            quantity = safe_float(exchange.amount_to_precision(symbol, quantity))
        except Exception:
            return 0.0, "Quantity precision conversion failed"

        if quantity <= 0:
            return 0.0, "Quantity rounded to zero"

        notional = quantity * entry
        actual_stop_risk = quantity * stop_distance

        if notional < MIN_ORDER_USD:
            return 0.0, "Minimum order would exceed risk budget"

        if actual_stop_risk > risk_budget * 1.05:
            return 0.0, "Actual risk exceeds configured budget"

        return quantity, "OK"

    @staticmethod
    def can_open_position(
        symbol: str,
        side: str,
        entry: float,
        stop_loss: float,
    ) -> Tuple[bool, str]:
        with STATE_LOCK:
            positions = list(SHARED_STATE["active_positions"].values())
            balance = safe_float(SHARED_STATE["balance"])
            halted = SHARED_STATE["risk_halted"]

        if halted:
            return False, "Global risk halt is active"

        if len(positions) >= MAX_OPEN_POSITIONS:
            return False, "Maximum open positions reached"

        if any(position["symbol"] == symbol for position in positions):
            return False, "Position already exists for this symbol"

        same_direction = sum(
            1 for position in positions if position["side"] == side
        )

        if same_direction >= MAX_DIRECTIONAL_POSITIONS:
            return False, "Maximum same-direction positions reached"

        cooldown = SYMBOL_COOLDOWNS.get(symbol, 0)

        if timestamp_now() < cooldown:
            return False, "Symbol is in cooldown"

        current_risk = sum(
            abs(safe_float(position["entry"]) - safe_float(position["sl"]))
            * safe_float(position["qty"])
            for position in positions
        )

        requested_risk = abs(entry - stop_loss)
        portfolio_risk_pct = (
            (current_risk + requested_risk) / balance * 100.0
            if balance > 0
            else 100.0
        )

        if portfolio_risk_pct > MAX_PORTFOLIO_RISK_PCT:
            return False, "Portfolio risk limit would be exceeded"

        return True, "OK"


# =============================================================================
# TELEGRAM
# =============================================================================

class TelegramController:
    def __init__(self, engine: "QuantEngine") -> None:
        self.engine = engine
        self.offset = 0
        self.base_url = f"https://api.telegram.org/bot{TG_TOKEN}" if TG_TOKEN else ""

    def menu(self) -> Dict[str, Any]:
        with STATE_LOCK:
            active = SHARED_STATE["is_active"]

        return {
            "inline_keyboard": [
                [
                    {"text": "📊 Dashboard", "callback_data": "dashboard"},
                    {"text": "💼 Positions", "callback_data": "positions"},
                ],
                [
                    {
                        "text": "⏸ Pause" if active else "▶ Resume",
                        "callback_data": "pause" if active else "resume",
                    },
                    {"text": "🔄 Sync", "callback_data": "sync"},
                ],
                [
                    {"text": "📄 Report", "callback_data": "report"},
                    {"text": "🧠 Decisions", "callback_data": "decisions"},
                ],
            ]
        }

    async def send(self, text: str, markup: Optional[Dict[str, Any]] = None) -> None:
        if not self.base_url or not TG_CHAT_ID:
            return

        payload: Dict[str, Any] = {
            "chat_id": TG_CHAT_ID,
            "text": text[:3900],
            "parse_mode": "HTML",
        }

        if markup:
            payload["reply_markup"] = markup

        try:
            timeout = aiohttp.ClientTimeout(total=15)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                await session.post(
                    f"{self.base_url}/sendMessage",
                    json=payload,
                )
        except Exception as error:
            log.error("Telegram send error: %s", error)

    async def send_document(self, path: str, caption: str) -> None:
        if not self.base_url or not TG_CHAT_ID or not Path(path).exists():
            return

        try:
            timeout = aiohttp.ClientTimeout(total=60)

            form = aiohttp.FormData()
            form.add_field("chat_id", TG_CHAT_ID)
            form.add_field("caption", caption)

            with open(path, "rb") as report_file:
                form.add_field(
                    "document",
                    report_file,
                    filename=Path(path).name,
                )

                async with aiohttp.ClientSession(timeout=timeout) as session:
                    await session.post(
                        f"{self.base_url}/sendDocument",
                        data=form,
                    )
        except Exception as error:
            log.error("Telegram document error: %s", error)

    async def poll(self) -> None:
        if not self.base_url:
            return

        await self.send(
            f"🧪 <b>Master Quant v{APP_VERSION}</b>\n"
            f"Mode: <b>TESTNET</b>\n"
            f"Auto trading: <b>{AUTO_TRADING}</b>",
            self.menu(),
        )

        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=30)

                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(
                        f"{self.base_url}/getUpdates",
                        params={"offset": self.offset + 1, "timeout": 20},
                    ) as response:
                        data = await response.json()

                for update in data.get("result", []):
                    self.offset = update.get("update_id", self.offset)

                    callback = update.get("callback_query")
                    if not callback:
                        continue

                    chat_id = (
                        callback.get("message", {})
                        .get("chat", {})
                        .get("id")
                    )

                    if not allowed_telegram_chat(chat_id):
                        log.warning("Unauthorized Telegram callback ignored.")
                        continue

                    await self.handle_command(callback.get("data", ""))

            except Exception as error:
                log.error("Telegram polling error: %s", error)

            await asyncio.sleep(1)

    async def handle_command(self, command: str) -> None:
        if command == "pause":
            with STATE_LOCK:
                SHARED_STATE["is_active"] = False

            await self.send("⏸️ Bot paused.", self.menu())
            return

        if command == "resume":
            with STATE_LOCK:
                SHARED_STATE["is_active"] = True

            await self.send("▶️ Bot resumed.", self.menu())
            return

        if command == "sync":
            await self.engine.smart_sync()
            await self.send("🔄 Synchronization completed.", self.menu())
            return

        if command == "dashboard":
            with STATE_LOCK:
                state = json.loads(json.dumps(SHARED_STATE))

            await self.send(
                f"📊 <b>Dashboard v{APP_VERSION}</b>\n"
                f"Mode: {state['mode']}\n"
                f"Auto trading: {state['auto_trading']}\n"
                f"Balance: ${state['balance']:.2f}\n"
                f"Drawdown: {state['current_drawdown_pct']:.2f}%\n"
                f"Daily PnL: ${state['daily_pnl']:.2f}\n"
                f"Positions: {len(state['active_positions'])}/{MAX_OPEN_POSITIONS}\n"
                f"Net PnL: ${state['stats']['net_pnl']:.4f}\n"
                f"Win rate: {state['stats']['win_rate']:.2f}%",
                self.menu(),
            )
            return

        if command == "positions":
            with STATE_LOCK:
                positions = list(SHARED_STATE["active_positions"].values())

            if not positions:
                await self.send("💤 No active positions.", self.menu())
                return

            lines = ["💼 <b>Open positions</b>"]

            for position in positions:
                price = safe_float(
                    self.engine.prices.get(position["symbol"]),
                    safe_float(position["entry"]),
                )

                pnl = (
                    (price - safe_float(position["entry"]))
                    * safe_float(position["qty"])
                    * side_sign(position["side"])
                )

                lines.append(
                    f"{position['symbol']} | {position['side'].upper()}\n"
                    f"PnL: ${pnl:+.4f}\n"
                    f"SL: {safe_float(position['sl']):.6f}"
                )

            await self.send("\n\n".join(lines), self.menu())
            return

        if command == "report":
            report = self.engine.storage.generate_report(self.engine.prices)
            report_path = "quant_report_v16.txt"

            Path(report_path).write_text(report, encoding="utf-8")

            await self.send_document(
                report_path,
                f"Master Quant v{APP_VERSION} diagnostic report",
            )
            return

        if command == "decisions":
            events = self.engine.storage.recent_events(40)
            decisions = [event for event in events if event.get("type") == "decision"]

            if not decisions:
                await self.send("No decisions recorded yet.", self.menu())
                return

            lines = ["🧠 <b>Recent decisions</b>"]

            for event in decisions[:10]:
                action = event.get("details", {}).get("action", "neutral")
                icon = "✅" if action in ("buy", "sell") else "⛔"

                lines.append(
                    f"{icon} {event.get('symbol', '-')}\n"
                    f"{action} | {event.get('message', '-')[:100]}"
                )

            await self.send("\n\n".join(lines), self.menu())


# =============================================================================
# ENGINE
# =============================================================================

class QuantEngine:
    def __init__(self) -> None:
        self.storage = Storage()
        self.strategy = StrategyEngine()
        self.telegram = TelegramController(self)

        self.prices: Dict[str, float] = {}
        self.last_price_update: Dict[str, float] = {}
        self.last_sync_at = 0.0

        self.exchange = ccxt.phemex(
            {
                "apiKey": PHEMEX_API_KEY,
                "secret": PHEMEX_API_SECRET,
                "enableRateLimit": True,
                "options": {
                    "defaultType": "swap",
                },
            }
        )

        self.exchange.set_sandbox_mode(True)

    async def start(self) -> None:
        self.restore_saved_state()

        log.info("Master Quant v%s starting in Phemex Testnet mode.", APP_VERSION)

        await self.exchange.load_markets()

        for symbol in SYMBOLS:
            try:
                await self.exchange.set_leverage(LEVERAGE, symbol)
                log.info("Leverage set for %s", symbol)
            except Exception as error:
                log.warning("Leverage setup failed for %s: %s", symbol, error)

        await self.update_balance()
        await self.smart_sync()

        await asyncio.gather(
            self.price_loop(),
            self.scan_loop(),
            self.watchdog_loop(),
            self.telegram.poll(),
        )

    def restore_saved_state(self) -> None:
        saved = self.storage.load_state()

        if not saved:
            return

        with STATE_LOCK:
            for key in (
                "peak_balance",
                "day_start_balance",
                "day_marker",
                "daily_halted",
                "drawdown_halted",
                "risk_halted",
            ):
                if key in saved:
                    SHARED_STATE[key] = saved[key]

            SHARED_STATE["active_positions"] = saved.get("active_positions", {})

        global SYMBOL_COOLDOWNS, SYMBOL_LOSS_STREAKS
        SYMBOL_COOLDOWNS = saved.get("symbol_cooldowns", {})
        SYMBOL_LOSS_STREAKS = saved.get("symbol_loss_streaks", {})

    def persist_state(self) -> None:
        with STATE_LOCK:
            payload = {
                "peak_balance": SHARED_STATE["peak_balance"],
                "day_start_balance": SHARED_STATE["day_start_balance"],
                "day_marker": SHARED_STATE["day_marker"],
                "daily_halted": SHARED_STATE["daily_halted"],
                "drawdown_halted": SHARED_STATE["drawdown_halted"],
                "risk_halted": SHARED_STATE["risk_halted"],
                "active_positions": SHARED_STATE["active_positions"],
                "symbol_cooldowns": SYMBOL_COOLDOWNS,
                "symbol_loss_streaks": SYMBOL_LOSS_STREAKS,
            }

        self.storage.save_state(payload)

    async def update_balance(self) -> None:
        try:
            balance_data = await self.exchange.fetch_balance()
            usdt = balance_data.get("USDT", {})

            total = safe_float(usdt.get("total"))
            free = safe_float(usdt.get("free"))

            marker = current_day_marker()

            with STATE_LOCK:
                if SHARED_STATE["day_marker"] != marker:
                    SHARED_STATE["day_marker"] = marker
                    SHARED_STATE["day_start_balance"] = total
                    SHARED_STATE["daily_halted"] = False

                SHARED_STATE["balance"] = total
                SHARED_STATE["free_balance"] = free

                if SHARED_STATE["peak_balance"] <= 0:
                    SHARED_STATE["peak_balance"] = total

                if total > SHARED_STATE["peak_balance"]:
                    SHARED_STATE["peak_balance"] = total

                peak = safe_float(SHARED_STATE["peak_balance"])
                day_start = safe_float(SHARED_STATE["day_start_balance"])

                drawdown = (peak - total) / peak * 100.0 if peak > 0 else 0.0
                daily_pnl = total - day_start
                daily_pnl_pct = daily_pnl / day_start * 100.0 if day_start > 0 else 0.0

                SHARED_STATE["current_drawdown_pct"] = drawdown
                SHARED_STATE["daily_pnl"] = daily_pnl
                SHARED_STATE["daily_pnl_pct"] = daily_pnl_pct

                SHARED_STATE["drawdown_halted"] = drawdown >= MAX_DRAWDOWN_PCT
                SHARED_STATE["daily_halted"] = daily_pnl_pct <= -MAX_DAILY_LOSS_PCT

                SHARED_STATE["risk_halted"] = (
                    SHARED_STATE["drawdown_halted"]
                    or SHARED_STATE["daily_halted"]
                )

            self.persist_state()

        except Exception as error:
            log.error("Balance update failed: %s", error)

            with STATE_LOCK:
                SHARED_STATE["last_error"] = f"Balance update failed: {error}"

            self.storage.add_event(
                "error",
                message="Balance update failed",
                details={"error": str(error)},
            )

    async def refresh_prices(self) -> None:
        try:
            tickers = await self.exchange.fetch_tickers(SYMBOLS)

            for symbol in SYMBOLS:
                ticker = tickers.get(symbol, {})

                last_price = safe_float(ticker.get("last"))
                bid = safe_float(ticker.get("bid"))
                ask = safe_float(ticker.get("ask"))

                if last_price <= 0:
                    self.storage.add_event(
                        "warning",
                        symbol,
                        "Ticker has no valid last price",
                    )
                    continue

                if bid > 0 and ask > 0:
                    spread_pct = (ask - bid) / ((ask + bid) / 2) * 100.0

                    if spread_pct > MAX_SPREAD_PCT:
                        self.storage.add_event(
                            "warning",
                            symbol,
                            "Spread exceeds allowed threshold",
                            {"spread_pct": round(spread_pct, 4)},
                        )
                        continue

                self.prices[symbol] = last_price
                self.last_price_update[symbol] = timestamp_now()

        except Exception as error:
            log.error("Price refresh failed: %s", error)

            self.storage.add_event(
                "error",
                message="Price refresh failed",
                details={"error": str(error)},
            )

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
    ) -> Tuple[Optional[pd.DataFrame], str]:
        try:
            candles = await self.exchange.fetch_ohlcv(
                symbol,
                timeframe=timeframe,
                limit=limit,
            )

            minimum = 220 if timeframe == HTF_TIMEFRAME else 80

            if not candles:
                return None, f"No {timeframe} candles returned"

            if len(candles) < minimum:
                return None, (
                    f"Insufficient {timeframe} candles: "
                    f"received={len(candles)} required={minimum}"
                )

            df = pd.DataFrame(
                candles,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )

            df["timestamp"] = pd.to_datetime(
                df["timestamp"],
                unit="ms",
                utc=True,
            )

            latest_open_time = df["timestamp"].iloc[-1].timestamp()
            age = timestamp_now() - latest_open_time
            allowed_age = timeframe_seconds(timeframe) + 90

            if age > allowed_age:
                return None, (
                    f"Stale {timeframe} candles: "
                    f"age={age:.0f}s allowed={allowed_age}s"
                )

            return df, "OK"

        except Exception as error:
            message = f"OHLCV fetch failed for {timeframe}: {error}"
            log.warning("%s %s", symbol, message)

            return None, message

    async def price_loop(self) -> None:
        while True:
            await self.refresh_prices()
            await self.update_balance()
            await asyncio.sleep(PRICE_LOOP_SECONDS)

    async def scan_loop(self) -> None:
        while True:
            try:
                with STATE_LOCK:
                    can_scan = (
                        SHARED_STATE["is_active"]
                        and not SHARED_STATE["risk_halted"]
                    )
                    SHARED_STATE["last_scan"] = utc_now()

                if not can_scan:
                    await asyncio.sleep(15)
                    continue

                if timestamp_now() - self.last_sync_at > SYNC_INTERVAL_SECONDS:
                    await self.smart_sync()

                for symbol in SYMBOLS:
                    await self.scan_symbol(symbol)
                    await asyncio.sleep(1.2)

            except Exception as error:
                log.exception("Scan loop failure: %s", error)

                self.storage.add_event(
                    "error",
                    message="Scan loop failure",
                    details={"error": str(error)},
                )

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    async def scan_symbol(self, symbol: str) -> None:
        if timestamp_now() < safe_float(SYMBOL_COOLDOWNS.get(symbol)):
            return

        with STATE_LOCK:
            already_open = any(
                position["symbol"] == symbol
                for position in SHARED_STATE["active_positions"].values()
            )

        if already_open:
            return

        df_5m, reason_5m = await self.fetch_ohlcv(
            symbol,
            TIMEFRAME,
            CANDLE_LIMIT_5M,
        )

        df_1h, reason_1h = await self.fetch_ohlcv(
            symbol,
            HTF_TIMEFRAME,
            CANDLE_LIMIT_1H,
        )

        failed = []

        if df_5m is None:
            failed.append(f"5m: {reason_5m}")

        if df_1h is None:
            failed.append(f"1h: {reason_1h}")

        if failed:
            self.storage.add_event(
                "decision",
                symbol,
                "Phemex candle validation failed: " + " | ".join(failed),
                {
                    "action": "neutral",
                    "timeframe_5m": reason_5m,
                    "timeframe_1h": reason_1h,
                },
            )
            return

        signal = self.strategy.analyze(df_5m, df_1h)

        market_price = safe_float(self.prices.get(symbol))

        if market_price <= 0:
            self.storage.add_event(
                "decision",
                symbol,
                "No current Phemex ticker price",
                {"action": "neutral"},
            )
            return

        candle_price = safe_float(df_5m["close"].iloc[-2])

        deviation_pct = (
            abs(market_price - candle_price) / candle_price * 100.0
            if candle_price > 0
            else 100.0
        )

        if deviation_pct > MAX_ENTRY_DEVIATION_PCT:
            self.storage.add_event(
                "decision",
                symbol,
                "Market price deviates too far from signal candle",
                {
                    "action": "neutral",
                    "market_price": market_price,
                    "signal_price": candle_price,
                    "deviation_pct": round(deviation_pct, 4),
                },
            )
            return

        self.storage.add_event(
            "decision",
            symbol,
            signal["reason"],
            {
                "action": signal["action"],
                "strategy": signal.get("strategy", ""),
                "rsi": signal.get("rsi", 0),
                "atr": signal.get("atr", 0),
                "htf_trend": signal.get("htf_trend", ""),
                "expected_rr": signal.get("expected_rr"),
                "market_price": market_price,
            },
        )

        if signal["action"] not in ("buy", "sell"):
            return

        if not AUTO_TRADING:
            log.info(
                "Signal logged without entry because AUTO_TRADING=false: %s %s",
                symbol,
                signal["action"],
            )
            return

        await self.execute_trade(symbol, signal, market_price)

    async def execute_trade(
        self,
        symbol: str,
        signal: Dict[str, Any],
        market_price: float,
    ) -> None:
        allowed, reason = RiskManager.can_open_position(
            symbol,
            signal["action"],
            market_price,
            safe_float(signal["sl"]),
        )

        if not allowed:
            self.storage.add_event(
                "decision",
                symbol,
                reason,
                {
                    "action": "rejected",
                    "strategy": signal.get("strategy", ""),
                },
            )
            return

        with STATE_LOCK:
            balance = safe_float(SHARED_STATE["balance"])
            free_balance = safe_float(SHARED_STATE["free_balance"])

        quantity, quantity_reason = RiskManager.calculate_qty(
            self.exchange,
            symbol,
            balance,
            free_balance,
            market_price,
            safe_float(signal["sl"]),
        )

        if quantity <= 0:
            self.storage.add_event(
                "decision",
                symbol,
                quantity_reason,
                {
                    "action": "rejected",
                    "strategy": signal.get("strategy", ""),
                },
            )
            return

        try:
            entry_order = await self.exchange.create_market_order(
                symbol,
                signal["action"],
                quantity,
            )

            filled_quantity = safe_float(entry_order.get("filled"), quantity)
            fill_price = safe_float(entry_order.get("average"), market_price)

            if filled_quantity <= 0 or fill_price <= 0:
                raise RuntimeError("Entry returned invalid fill quantity or price")

            trade_id = f"trade_{uuid.uuid4().hex[:16]}"

            trade = {
                "id": trade_id,
                "symbol": symbol,
                "side": signal["action"],
                "strategy": signal["strategy"],
                "entry": fill_price,
                "original_qty": filled_quantity,
                "qty": filled_quantity,
                "sl": safe_float(signal["sl"]),
                "tp1": safe_float(signal["tp1"]),
                "tp": safe_float(signal["tp"]),
                "opened_at": utc_now(),
                "status": "open",
                "entry_order_id": entry_order.get("id"),
                "stop_order_id": None,
                "partial_taken": False,
                "highest_pnl_pct": 0.0,
                "realized_pnl": 0.0,
                "fees": self.estimate_fee(fill_price, filled_quantity),
                "metadata": {
                    "rsi": signal.get("rsi"),
                    "atr": signal.get("atr"),
                    "expected_rr": signal.get("expected_rr"),
                    "entry_signal_price": market_price,
                },
            }

            protection_ok = await self.create_exchange_stop(trade)

            if not protection_ok:
                self.storage.add_event(
                    "protection_failed",
                    symbol,
                    "Protective stop was not confirmed; closing new position",
                    {"trade_id": trade_id},
                )

                await self.close_untracked_position(
                    symbol,
                    signal["action"],
                    filled_quantity,
                    "Protection setup failed",
                )

                await self.telegram.send(
                    f"⚠️ <b>ENTRY REVERSED</b>\n"
                    f"{symbol}\n"
                    f"Reason: exchange protective stop could not be confirmed."
                )
                return

            trades = self.storage.load_trades()
            trades.append(trade)
            self.storage.save_trades(trades)

            with STATE_LOCK:
                SHARED_STATE["active_positions"][trade_id] = trade

            self.persist_state()

            self.storage.add_event(
                "entry",
                symbol,
                "Testnet position opened with protective stop",
                {
                    "trade_id": trade_id,
                    "side": trade["side"],
                    "entry": trade["entry"],
                    "qty": trade["qty"],
                    "sl": trade["sl"],
                    "tp1": trade["tp1"],
                    "tp": trade["tp"],
                    "strategy": trade["strategy"],
                    "entry_order_id": trade["entry_order_id"],
                    "stop_order_id": trade["stop_order_id"],
                },
            )

            await self.telegram.send(
                f"🎯 <b>TESTNET ENTRY</b>\n"
                f"{symbol} | {trade['side'].upper()}\n"
                f"Entry: {trade['entry']:.6f}\n"
                f"Qty: {trade['qty']:.6f}\n"
                f"SL: {trade['sl']:.6f}\n"
                f"TP1: {trade['tp1']:.6f}\n"
                f"TP: {trade['tp']:.6f}\n"
                f"Strategy: {trade['strategy']}"
            )

        except Exception as error:
            log.exception("Entry failed for %s: %s", symbol, error)

            self.storage.add_event(
                "error",
                symbol,
                "Entry execution failed",
                {
                    "error": str(error),
                    "strategy": signal.get("strategy", ""),
                },
            )

            SYMBOL_COOLDOWNS[symbol] = timestamp_now() + 300
            self.persist_state()

    async def create_exchange_stop(self, trade: Dict[str, Any]) -> bool:
        """
        The stop order is created immediately after a successful entry.
        If the exchange rejects this request, the new position is closed.

        Verify the stop order visibly exists in Phemex Testnet after the
        first successful trade before relying on AUTO_TRADING=true long-term.
        """
        opposite_side = "sell" if trade["side"] == "buy" else "buy"

        candidate_params = [
            {
                "reduceOnly": True,
                "stopLossPrice": trade["sl"],
                "triggerType": "ByLastPrice",
            },
            {
                "reduceOnly": True,
                "stopPx": trade["sl"],
                "triggerType": "ByLastPrice",
                "closeOnTrigger": True,
            },
        ]

        for params in candidate_params:
            try:
                stop_order = await self.exchange.create_order(
                    trade["symbol"],
                    "market",
                    opposite_side,
                    trade["qty"],
                    None,
                    params,
                )

                order_id = stop_order.get("id")

                if order_id:
                    trade["stop_order_id"] = order_id

                    self.storage.add_event(
                        "protection_created",
                        trade["symbol"],
                        "Exchange protective stop created",
                        {
                            "trade_id": trade["id"],
                            "stop_order_id": order_id,
                            "stop_price": trade["sl"],
                        },
                    )
                    return True

            except Exception as error:
                log.warning("Protective stop attempt failed: %s", error)

        return False

    async def cancel_order_safely(
        self,
        symbol: str,
        order_id: Optional[str],
    ) -> None:
        if not order_id:
            return

        try:
            await self.exchange.cancel_order(order_id, symbol)

        except Exception as error:
            self.storage.add_event(
                "warning",
                symbol,
                "Could not cancel protective order",
                {
                    "order_id": order_id,
                    "error": str(error),
                },
            )

    async def replace_exchange_stop(self, trade: Dict[str, Any]) -> bool:
        await self.cancel_order_safely(
            trade["symbol"],
            trade.get("stop_order_id"),
        )

        trade["stop_order_id"] = None

        return await self.create_exchange_stop(trade)

    async def close_untracked_position(
        self,
        symbol: str,
        original_side: str,
        quantity: float,
        reason: str,
    ) -> None:
        close_side = "sell" if original_side == "buy" else "buy"

        try:
            await self.exchange.create_market_order(
                symbol,
                close_side,
                quantity,
                params={"reduceOnly": True},
            )

            self.storage.add_event(
                "emergency_close",
                symbol,
                reason,
                {"qty": quantity},
            )

        except Exception as error:
            self.storage.add_event(
                "error",
                symbol,
                "Emergency close failed",
                {
                    "reason": reason,
                    "error": str(error),
                },
            )

    def estimate_fee(self, price: float, quantity: float) -> float:
        return abs(price * quantity) * TAKER_FEE_RATE * FEE_BUFFER

    async def partial_close(self, trade_id: str) -> None:
        with STATE_LOCK:
            trade = SHARED_STATE["active_positions"].get(trade_id)

        if not trade or trade.get("partial_taken"):
            return

        try:
            half_quantity = safe_float(
                self.exchange.amount_to_precision(
                    trade["symbol"],
                    safe_float(trade["qty"]) * PARTIAL_CLOSE_FRACTION,
                )
            )

            if half_quantity <= 0 or half_quantity >= safe_float(trade["qty"]):
                return

            close_side = "sell" if trade["side"] == "buy" else "buy"

            order = await self.exchange.create_market_order(
                trade["symbol"],
                close_side,
                half_quantity,
                params={"reduceOnly": True},
            )

            fill_price = safe_float(
                order.get("average"),
                safe_float(self.prices.get(trade["symbol"])),
            )

            gross_pnl = (
                (fill_price - safe_float(trade["entry"]))
                * half_quantity
                * side_sign(trade["side"])
            )

            fees = self.estimate_fee(fill_price, half_quantity)

            trade["qty"] = safe_float(trade["qty"]) - half_quantity
            trade["realized_pnl"] = safe_float(trade["realized_pnl"]) + gross_pnl - fees
            trade["fees"] = safe_float(trade["fees"]) + fees
            trade["partial_taken"] = True
            trade["sl"] = safe_float(trade["entry"])

            protection_ok = await self.replace_exchange_stop(trade)

            if not protection_ok:
                await self.force_close(
                    trade_id,
                    "Protection replacement failed after partial exit",
                )
                return

            self.update_trade_record(trade)

            self.storage.add_event(
                "partial_exit",
                trade["symbol"],
                "TP1 reached; partial position closed and stop moved to break-even",
                {
                    "trade_id": trade_id,
                    "qty_closed": half_quantity,
                    "fill_price": fill_price,
                    "gross_pnl": gross_pnl,
                    "fees": fees,
                    "remaining_qty": trade["qty"],
                    "new_stop": trade["sl"],
                },
            )

            await self.telegram.send(
                f"🔹 <b>PARTIAL EXIT</b>\n"
                f"{trade['symbol']}\n"
                f"Net partial PnL: ${gross_pnl - fees:+.4f}\n"
                f"Remaining: {trade['qty']:.6f}\n"
                f"Stop moved to break-even."
            )

        except Exception as error:
            self.storage.add_event(
                "error",
                trade["symbol"] if trade else "",
                "Partial exit failed",
                {"error": str(error)},
            )

    async def force_close(self, trade_id: str, reason: str) -> None:
        with STATE_LOCK:
            trade = SHARED_STATE["active_positions"].get(trade_id)

        if not trade:
            return

        try:
            close_side = "sell" if trade["side"] == "buy" else "buy"

            order = await self.exchange.create_market_order(
                trade["symbol"],
                close_side,
                trade["qty"],
                params={"reduceOnly": True},
            )

            exit_price = safe_float(
                order.get("average"),
                safe_float(self.prices.get(trade["symbol"])),
            )

            gross_final = (
                (exit_price - safe_float(trade["entry"]))
                * safe_float(trade["qty"])
                * side_sign(trade["side"])
            )

            final_fees = self.estimate_fee(exit_price, safe_float(trade["qty"]))

            net_pnl = (
                safe_float(trade["realized_pnl"])
                + gross_final
                - final_fees
            )

            total_fees = safe_float(trade["fees"]) + final_fees

            await self.cancel_order_safely(
                trade["symbol"],
                trade.get("stop_order_id"),
            )

            trade["status"] = "closed"
            trade["closed_at"] = utc_now()
            trade["exit_reason"] = reason
            trade["exit_price"] = exit_price
            trade["net_pnl"] = net_pnl
            trade["fees"] = total_fees

            self.update_trade_record(trade)

            with STATE_LOCK:
                SHARED_STATE["active_positions"].pop(trade_id, None)

            symbol = trade["symbol"]

            if net_pnl < 0:
                SYMBOL_LOSS_STREAKS[symbol] = SYMBOL_LOSS_STREAKS.get(symbol, 0) + 1

                if SYMBOL_LOSS_STREAKS[symbol] >= CONSECUTIVE_LOSS_LIMIT:
                    SYMBOL_COOLDOWNS[symbol] = (
                        timestamp_now() + SYMBOL_COOLDOWN_SECONDS
                    )
            else:
                SYMBOL_LOSS_STREAKS.pop(symbol, None)

            self.refresh_statistics()
            self.persist_state()

            self.storage.add_event(
                "final_exit",
                symbol,
                reason,
                {
                    "trade_id": trade_id,
                    "exit_price": exit_price,
                    "gross_final_pnl": gross_final,
                    "fees": total_fees,
                    "net_pnl": net_pnl,
                    "hold_seconds": self.hold_seconds(trade),
                },
            )

            await self.telegram.send(
                f"{'🟢' if net_pnl >= 0 else '🔴'} <b>POSITION CLOSED</b>\n"
                f"{symbol}\n"
                f"Reason: {reason}\n"
                f"Net PnL: ${net_pnl:+.4f}\n"
                f"Fees: ${total_fees:.4f}"
            )

        except Exception as error:
            self.storage.add_event(
                "error",
                trade["symbol"],
                "Final close failed",
                {
                    "trade_id": trade_id,
                    "reason": reason,
                    "error": str(error),
                },
            )

    def hold_seconds(self, trade: Dict[str, Any]) -> float:
        try:
            opened = datetime.fromisoformat(trade["opened_at"])
            return max(
                0.0,
                (datetime.now(timezone.utc) - opened).total_seconds(),
            )
        except Exception:
            return 0.0

    def update_trade_record(self, updated_trade: Dict[str, Any]) -> None:
        trades = self.storage.load_trades()

        for index, trade in enumerate(trades):
            if trade.get("id") == updated_trade.get("id"):
                trades[index] = updated_trade
                break
        else:
            trades.append(updated_trade)

        self.storage.save_trades(trades)

        with STATE_LOCK:
            if updated_trade.get("status") == "open":
                SHARED_STATE["active_positions"][updated_trade["id"]] = updated_trade

        self.persist_state()

    def refresh_statistics(self) -> None:
        stats = self.storage.calculate_stats(self.storage.load_trades())

        with STATE_LOCK:
            SHARED_STATE["stats"] = stats

    async def watchdog_loop(self) -> None:
        while True:
            try:
                with STATE_LOCK:
                    positions = list(SHARED_STATE["active_positions"].items())

                for trade_id, trade in positions:
                    price = safe_float(self.prices.get(trade["symbol"]))

                    if price <= 0:
                        continue

                    entry = safe_float(trade["entry"])
                    pnl_pct = (
                        (price - entry) / entry
                        * 100.0
                        * side_sign(trade["side"])
                        if entry > 0
                        else 0.0
                    )

                    trade["highest_pnl_pct"] = max(
                        safe_float(trade.get("highest_pnl_pct")),
                        pnl_pct,
                    )

                    stop_hit = (
                        price <= safe_float(trade["sl"])
                        if trade["side"] == "buy"
                        else price >= safe_float(trade["sl"])
                    )

                    tp1_hit = (
                        price >= safe_float(trade["tp1"])
                        if trade["side"] == "buy"
                        else price <= safe_float(trade["tp1"])
                    )

                    final_tp_hit = (
                        price >= safe_float(trade["tp"])
                        if trade["side"] == "buy"
                        else price <= safe_float(trade["tp"])
                    )

                    emergency_hit = pnl_pct <= -EMERGENCY_STOP_PCT

                    if (
                        PARTIAL_TP_ENABLED
                        and not trade.get("partial_taken")
                        and tp1_hit
                    ):
                        await self.partial_close(trade_id)
                        continue

                    if pnl_pct >= TRAIL_ACTIVATION_PCT:
                        if trade["side"] == "buy":
                            new_stop = price * (1 - TRAIL_DISTANCE_PCT / 100.0)

                            if new_stop > safe_float(trade["sl"]):
                                trade["sl"] = new_stop
                                await self.replace_exchange_stop(trade)
                                self.update_trade_record(trade)

                        else:
                            new_stop = price * (1 + TRAIL_DISTANCE_PCT / 100.0)

                            if new_stop < safe_float(trade["sl"]):
                                trade["sl"] = new_stop
                                await self.replace_exchange_stop(trade)
                                self.update_trade_record(trade)

                    if emergency_hit:
                        await self.force_close(trade_id, "Emergency loss limit")
                    elif stop_hit:
                        await self.force_close(trade_id, "Stop loss or trailing stop")
                    elif final_tp_hit:
                        await self.force_close(trade_id, "Final take profit")

            except Exception as error:
                log.exception("Watchdog failure: %s", error)

                self.storage.add_event(
                    "error",
                    message="Watchdog failure",
                    details={"error": str(error)},
                )

            await asyncio.sleep(WATCHDOG_SECONDS)

    async def smart_sync(self) -> None:
        """
        The exchange is treated as the live source of truth.
        Existing remote positions missing from local data are recovered with
        conservative emergency levels and marked as Recovered.
        """
        try:
            remote_positions = await self.exchange.fetch_positions(SYMBOLS)
            remote_by_symbol: Dict[str, Dict[str, Any]] = {}

            for position in remote_positions:
                contracts = safe_float(position.get("contracts"))

                if abs(contracts) <= 0:
                    continue

                symbol = position.get("symbol")

                if symbol not in SYMBOLS:
                    continue

                remote_by_symbol[symbol] = {
                    "qty": abs(contracts),
                    "side": "buy" if contracts > 0 else "sell",
                    "entry": safe_float(
                        position.get("entryPrice")
                        or position.get("avgEntryPrice")
                    ),
                }

            with STATE_LOCK:
                local_positions = dict(SHARED_STATE["active_positions"])

            for trade_id, trade in local_positions.items():
                if trade["symbol"] not in remote_by_symbol:
                    trade["status"] = "closed"
                    trade["closed_at"] = utc_now()
                    trade["exit_reason"] = "Not present during exchange synchronization"
                    trade["net_pnl"] = safe_float(trade.get("realized_pnl"))
                    self.update_trade_record(trade)

                    with STATE_LOCK:
                        SHARED_STATE["active_positions"].pop(trade_id, None)

                    self.storage.add_event(
                        "warning",
                        trade["symbol"],
                        "Local position absent on exchange during synchronization",
                        {"trade_id": trade_id},
                    )

            with STATE_LOCK:
                tracked_symbols = {
                    trade["symbol"]
                    for trade in SHARED_STATE["active_positions"].values()
                }

            for symbol, remote in remote_by_symbol.items():
                if symbol in tracked_symbols:
                    continue

                entry = remote["entry"] or safe_float(self.prices.get(symbol))

                if entry <= 0:
                    continue

                distance = entry * (EMERGENCY_STOP_PCT / 100.0)

                if remote["side"] == "buy":
                    stop_loss = entry - distance
                    tp1 = entry + distance
                    take_profit = entry + distance * 2
                else:
                    stop_loss = entry + distance
                    tp1 = entry - distance
                    take_profit = entry - distance * 2

                trade_id = f"recovered_{uuid.uuid4().hex[:16]}"

                recovered = {
                    "id": trade_id,
                    "symbol": symbol,
                    "side": remote["side"],
                    "strategy": "Recovered",
                    "entry": entry,
                    "original_qty": remote["qty"],
                    "qty": remote["qty"],
                    "sl": stop_loss,
                    "tp1": tp1,
                    "tp": take_profit,
                    "opened_at": utc_now(),
                    "status": "open",
                    "entry_order_id": None,
                    "stop_order_id": None,
                    "partial_taken": False,
                    "highest_pnl_pct": 0.0,
                    "realized_pnl": 0.0,
                    "fees": 0.0,
                    "metadata": {
                        "recovered": True,
                        "warning": "Original opening time was unavailable",
                    },
                }

                protection_ok = await self.create_exchange_stop(recovered)

                if not protection_ok:
                    self.storage.add_event(
                        "protection_failed",
                        symbol,
                        "Recovered position has no confirmed protective stop",
                        {"trade_id": trade_id},
                    )

                trades = self.storage.load_trades()
                trades.append(recovered)
                self.storage.save_trades(trades)

                with STATE_LOCK:
                    SHARED_STATE["active_positions"][trade_id] = recovered

                self.storage.add_event(
                    "recovered",
                    symbol,
                    "Position recovered from exchange",
                    {
                        "trade_id": trade_id,
                        "side": remote["side"],
                        "entry": entry,
                        "qty": remote["qty"],
                        "protection_confirmed": protection_ok,
                    },
                )

            self.refresh_statistics()
            self.persist_state()

            self.last_sync_at = timestamp_now()

            with STATE_LOCK:
                SHARED_STATE["last_sync"] = utc_now()

            log.info("Exchange synchronization completed.")

        except Exception as error:
            log.exception("Synchronization failed: %s", error)

            self.storage.add_event(
                "error",
                message="Exchange synchronization failed",
                details={"error": str(error)},
            )


# =============================================================================
# WEB DASHBOARD
# =============================================================================

app = Flask(__name__)
auth = HTTPBasicAuth()


@auth.verify_password
def verify_password(username: str, password: str) -> Optional[str]:
    if (
        DASHBOARD_PASSWORD_HASH
        and username == DASHBOARD_USER
        and check_password_hash(DASHBOARD_PASSWORD_HASH, password)
    ):
        return username

    return None


@app.route("/api/status")
@auth.login_required
def api_status():
    with STATE_LOCK:
        return jsonify(json_safe(SHARED_STATE))


@app.route("/")
@auth.login_required
def dashboard():
    return render_template_string(
        """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Master Quant v16.0</title>
  <style>
    body {
      background:#0d1117;
      color:#c9d1d9;
      font-family:system-ui,sans-serif;
      padding:24px;
      margin:0;
    }
    h1 { color:#58a6ff; margin:0 0 8px; }
    .hint { color:#8b949e; margin-bottom:20px; }
    .grid {
      display:grid;
      grid-template-columns:repeat(auto-fit,minmax(175px,1fr));
      gap:12px;
    }
    .card {
      background:#161b22;
      border:1px solid #30363d;
      border-radius:12px;
      padding:16px;
    }
    .label { color:#8b949e; font-size:.85rem; }
    .value {
      color:#58a6ff;
      font-weight:700;
      font-size:1.35rem;
      margin-top:7px;
    }
  </style>
</head>
<body>
  <h1>Master Quant v16.0</h1>
  <div class="hint">Phemex Testnet monitoring dashboard</div>

  <div class="grid">
    <div class="card"><div class="label">Mode</div><div class="value" id="mode">-</div></div>
    <div class="card"><div class="label">Auto Trading</div><div class="value" id="auto">-</div></div>
    <div class="card"><div class="label">Balance</div><div class="value" id="balance">-</div></div>
    <div class="card"><div class="label">Daily PnL</div><div class="value" id="daily">-</div></div>
    <div class="card"><div class="label">Drawdown</div><div class="value" id="drawdown">-</div></div>
    <div class="card"><div class="label">Positions</div><div class="value" id="positions">-</div></div>
    <div class="card"><div class="label">Net PnL</div><div class="value" id="pnl">-</div></div>
    <div class="card"><div class="label">Win Rate</div><div class="value" id="winrate">-</div></div>
    <div class="card"><div class="label">Risk Status</div><div class="value" id="risk">-</div></div>
  </div>

  <script>
    async function refresh() {
      try {
        const response = await fetch('/api/status');
        const data = await response.json();
        const stats = data.stats || {};

        document.getElementById('mode').textContent = data.mode || '-';
        document.getElementById('auto').textContent = data.auto_trading ? 'ON' : 'OFF';
        document.getElementById('balance').textContent =
          '$' + Number(data.balance || 0).toFixed(2);
        document.getElementById('daily').textContent =
          '$' + Number(data.daily_pnl || 0).toFixed(2);
        document.getElementById('drawdown').textContent =
          Number(data.current_drawdown_pct || 0).toFixed(2) + '%';
        document.getElementById('positions').textContent =
          Object.keys(data.active_positions || {}).length;
        document.getElementById('pnl').textContent =
          '$' + Number(stats.net_pnl || 0).toFixed(4);
        document.getElementById('winrate').textContent =
          Number(stats.win_rate || 0).toFixed(2) + '%';
        document.getElementById('risk').textContent =
          data.risk_halted ? 'HALTED' : 'ACTIVE';
      } catch (_) {}
    }

    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
        """
    )


def run_web() -> None:
    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False,
        use_reloader=False,
    )


# =============================================================================
# MAIN
# =============================================================================

async def main() -> None:
    engine = QuantEngine()

    try:
        await engine.start()
    finally:
        await engine.exchange.close()


if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    asyncio.run(main())
