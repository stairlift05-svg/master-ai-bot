"""Typed domain models shared across all modules.

Using frozen dataclasses for value objects (signals, order results, wallet
state) gives us immutability, cheap equality, and IDE-friendly introspection —
a baseline requirement for maintainable trading software.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Candle:
    """One OHLCV bar. ``ts`` is the open time in milliseconds (UTC)."""

    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float

    @classmethod
    def from_row(cls, row: list) -> "Candle":
        """Build from a ccxt-style row ``[ts, o, h, l, c, v]``."""
        return cls(int(row[0]), float(row[1]), float(row[2]), float(row[3]),
                   float(row[4]), float(row[5]))

    def as_row(self) -> list:
        return [self.ts, self.o, self.h, self.l, self.c, self.v]


class CandleSeries:
    """Lightweight, list-backed OHLCV container.

    Deliberately avoids pandas so the live engine and the backtest harness
    share one dependency-light, fast implementation.
    """

    __slots__ = ("candles",)

    def __init__(self, candles: List[Candle]):
        self.candles: List[Candle] = list(candles)

    @classmethod
    def from_rows(cls, rows: list) -> "CandleSeries":
        return cls([Candle.from_row(r) for r in rows])

    def __len__(self) -> int:
        return len(self.candles)

    @property
    def times(self) -> List[int]:
        return [c.ts for c in self.candles]

    @property
    def opens(self) -> List[float]:
        return [c.o for c in self.candles]

    @property
    def highs(self) -> List[float]:
        return [c.h for c in self.candles]

    @property
    def lows(self) -> List[float]:
        return [c.l for c in self.candles]

    @property
    def closes(self) -> List[float]:
        return [c.c for c in self.candles]

    @property
    def volumes(self) -> List[float]:
        return [c.v for c in self.candles]

    def window(self, n: int) -> "CandleSeries":
        """Return a new series with the last ``n`` candles."""
        return CandleSeries(self.candles[-n:])

    def without_last(self) -> "CandleSeries":
        """Drop the final (possibly still-forming) candle."""
        return CandleSeries(self.candles[:-1])

    def append(self, candle: Candle) -> None:
        self.candles.append(candle)


@dataclass(frozen=True)
class MarketMeta:
    """Exchange symbol metadata used for quantity quantisation."""

    min_qty: float = 0.0
    step: float = 0.0
    price_step: float = 0.0
    funding: float = 0.0  # percent per funding interval (signed)

    @property
    def has_qty_rules(self) -> bool:
        return self.step > 0 or self.min_qty > 0


# ---------------------------------------------------------------------------
# Strategy output
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Signal:
    """A fully specified, ready-to-execute trade proposal."""

    side: str  # "buy" | "sell"
    strategy: str
    reason: str
    entry: float
    sl: float
    tp1: float
    tp: float
    rsi: float = 0.0
    atr: float = 0.0
    htf: str = ""
    confidence: float = 0.5  # 0..1, set by strategy-specific heuristics


@dataclass(frozen=True)
class AnalysisResult:
    """Outcome of one strategy scan over one symbol."""

    action: str  # "buy" | "sell" | "neutral" | "rejected"
    reason: str = ""
    strategy: str = ""
    rsi: float = 0.0
    atr: float = 0.0
    htf: str = ""
    signal: Optional[Signal] = None


# ---------------------------------------------------------------------------
# Exchange / wallet
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WalletState:
    """Normalised dual-wallet (spot + futures) view."""

    equity: float = 0.0            # total account equity
    spot_total: float = 0.0
    spot_free: float = 0.0
    spot_locked: float = 0.0
    futures_total: float = 0.0
    futures_free: float = 0.0
    futures_locked: float = 0.0

    @property
    def free_for_trading(self) -> float:
        """Free margin available for futures orders."""
        if self.futures_free > 0:
            return self.futures_free
        return max(0.0, self.equity)


@dataclass(frozen=True)
class OrderResult:
    """Normalised outcome of a placed order."""

    order_id: str
    symbol: str
    side: str
    qty: float
    avg_price: float
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def notional(self) -> float:
        return self.qty * self.avg_price


@dataclass(frozen=True)
class CloseResult:
    """Outcome of a market close, including realised PnL estimate."""

    order_id: str
    symbol: str
    qty: float
    avg_price: float
    realized_pnl: float
    fees: float
    reason: str
    raw: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------


@dataclass
class Position:
    """A live (open) position managed by the engine."""

    id: str
    symbol: str
    side: str  # "buy" | "sell"
    strategy: str
    entry: float
    qty: float
    sl: float
    tp1: float
    tp: float
    is_partial: int = 0
    highest_pnl_pct: float = 0.0
    opened_at: float = 0.0
    remote_qty: Optional[float] = None  # last seen qty on the exchange
    atr_at_entry: float = 0.0           # ATR used for ATR-based trailing

    # -- derived -------------------------------------------------------
    def notional(self, price: float) -> float:
        return self.qty * price

    def unrealized_pnl(self, price: float) -> float:
        if self.side == "buy":
            return (price - self.entry) * self.qty
        return (self.entry - price) * self.qty

    def pnl_pct(self, price: float) -> float:
        """Return unrealized PnL as notional percentage, safely for zero qty."""
        if self.entry <= 0 or self.qty <= 0:
            return 0.0
        return (self.unrealized_pnl(price) / (self.entry * self.qty)) * 100.0

    def to_dict(self, price: float = 0.0) -> Dict[str, Any]:
        return {
            "id": self.id, "symbol": self.symbol, "side": self.side,
            "strategy": self.strategy, "entry": self.entry, "qty": self.qty,
            "sl": self.sl, "tp1": self.tp1, "tp": self.tp,
            "is_partial": self.is_partial,
            "highest_pnl_pct": self.highest_pnl_pct,
            "opened_at": self.opened_at, "price": price,
            "upnl": self.unrealized_pnl(price) if price > 0 else 0.0,
        }


@dataclass(frozen=True)
class RemotePosition:
    """A position as reported by the exchange (normalised)."""

    symbol: str
    side: str
    qty: float
    entry: float
    raw: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Persistence / reporting
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Decision:
    """One logged scan decision (used by Telegram reports and analytics)."""

    ts: str
    symbol: str
    action: str
    strategy: str
    reason: str
    price: float = 0.0
    rsi: float = 0.0
    atr: float = 0.0
    htf: str = ""
    extra: str = ""


@dataclass(frozen=True)
class Metrics:
    """Aggregate trading statistics computed from the trade history."""

    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    profit_factor: float = 0.0
    expectancy: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    max_dd_pct: float = 0.0
    sharpe: float = 0.0
    avg_hold_s: float = 0.0
