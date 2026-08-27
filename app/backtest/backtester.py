"""Backtesting (#05): event-driven backtester.

Simulates the *exact* live decision pipeline offline:

* same :class:`StrategyEngine` (strategy module),
* same :class:`PositionSizer` + :class:`RiskManager` (risk module),
* same :class:`PortfolioLimits` (optimization module),
* same adaptive-risk scaling and halt logic,
* fees + slippage on every fill,
* intrabar-aware SL/TP/partial/trail simulation.

Only the transport layer (HTTP) is replaced by a synthetic market, so a green
backtest is meaningful evidence about the strategy/risk logic itself.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from app.config import Settings
from app.models import Candle, CandleSeries, Signal
from app.optimization.optimizer import PortfolioLimits
from app.risk.position_sizer import PositionSizer
from app.risk.risk_manager import RiskManager
from app.state import EngineState
from app.strategy.engine import StrategyEngine
from app.backtest.synthetic import resample

MIN_BARS = 260
RESAMPLE_N = {"15m": 3, "1h": 12}   # 5m bars per higher-TF bar
WIN = {"5m": 400, "15m": 300, "1h": 300}   # max bars passed to the engine
SIGNAL_EVERY_N = 3                  # evaluate signals every 3 bars (15m cadence)


@dataclass
class BacktestPosition:
    """A position inside the simulation."""

    symbol: str
    side: str
    strategy: str
    entry: float
    qty: float
    sl: float
    tp1: float
    tp: float
    opened_bar: int
    is_partial: int = 0
    highest_pnl_pct: float = 0.0
    atr_at_entry: float = 0.0

    def unrealized(self, price: float) -> float:
        if self.side == "buy":
            return (price - self.entry) * self.qty
        return (self.entry - price) * self.qty

    def pnl_pct(self, price: float) -> float:
        """Return unrealized PnL percentage without division-by-zero risk."""
        if self.entry <= 0 or self.qty <= 0:
            return 0.0
        return self.unrealized(price) / (self.entry * self.qty) * 100.0


@dataclass
class BacktestTrade:
    symbol: str
    side: str
    strategy: str
    entry: float
    exit_price: float
    qty: float
    pnl: float
    fees: float
    hold_bars: int
    reason: str
    opened_bar: int = 0
    closed_bar: int = 0


@dataclass
class BacktestReport:
    final_equity: float = 0.0
    initial_balance: float = 0.0
    total_return_pct: float = 0.0
    max_dd_pct: float = 0.0
    sharpe: float = 0.0
    trades: List[BacktestTrade] = field(default_factory=list)
    per_strategy: Dict[str, Dict] = field(default_factory=dict)
    decisions: int = 0
    exposure_pct: float = 0.0
    halted_bars: int = 0

    @property
    def n_trades(self) -> int:
        return len(self.trades)

    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        return round(sum(1 for t in self.trades if t.pnl > 0) / len(self.trades) * 100.0, 1)

    def profit_factor(self) -> float:
        gross_win = sum(t.pnl for t in self.trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in self.trades if t.pnl < 0))
        return round(gross_win / gross_loss, 2) if gross_loss > 0 else 0.0

    def net_pnl(self) -> float:
        return round(sum(t.pnl for t in self.trades), 2)

    def to_dict(self) -> Dict:
        return {
            "initial_balance": round(self.initial_balance, 2),
            "final_equity": round(self.final_equity, 2),
            "total_return_pct": round(self.total_return_pct, 2),
            "max_dd_pct": round(self.max_dd_pct, 2),
            "sharpe": round(self.sharpe, 2),
            "n_trades": self.n_trades,
            "win_rate": self.win_rate(),
            "profit_factor": self.profit_factor(),
            "net_pnl": self.net_pnl(),
            "decisions": self.decisions,
            "exposure_pct": round(self.exposure_pct, 1),
            "halted_bars": self.halted_bars,
            "per_strategy": self.per_strategy,
        }


class Backtester:
    """Event-driven simulation of the full decision pipeline."""

    def __init__(self, settings: Settings, initial_balance: float = 500.0,
                 slippage_bps: Optional[float] = None) -> None:
        self.settings = settings
        self.initial_balance = float(initial_balance)
        # Default to the same slippage the strategy cost gate assumes, so the
        # backtest can never be optimistic relative to the live cost model.
        self.slippage = (slippage_bps / 10000.0) if slippage_bps is not None \
            else settings.slippage_pct
        self.state = EngineState()
        self.state.set_many(
            balance=self.initial_balance, peak_balance=self.initial_balance,
            day_start_balance=self.initial_balance, is_active=True,
        )
        self.day_start = self.initial_balance
        self.strategy = StrategyEngine(settings, self.state)
        self.sizer = PositionSizer(settings)
        # Simulation clock: cooldowns advance with simulated time, not wall time.
        self._sim_clock = [0.0]
        self.risk = RiskManager(settings, self.state,
                                clock=lambda: self._sim_clock[0])
        self.limits = PortfolioLimits(settings)
        self.positions: List[BacktestPosition] = []
        self.pending: Dict[str, Signal] = {}
        self.closed: List[BacktestTrade] = []
        self.balance = self.initial_balance
        self.peak = self.initial_balance
        self.equity_history: List[float] = []
        self.exposed_bars = 0
        self.halted_bars = 0
        self.decision_count = 0

    # ------------------------------------------------------------------
    def run(self, market_5m: Dict[str, List[Candle]],
            market_1h: Optional[Dict[str, List[Candle]]] = None) -> BacktestReport:
        """Run the simulation over the given per-symbol 5m series.

        The 15m and 1h series are derived from the 5m data by resampling and
        accumulated incrementally (only *complete* higher-TF bars enter the
        ready list), mirroring how the live feed supplies closed candles.
        """
        symbols = list(market_5m.keys())
        n_bars = min(len(market_5m[s]) for s in symbols)
        htf_15: Dict[str, List[Candle]] = {
            s: resample(market_5m[s], RESAMPLE_N["15m"]) for s in symbols
        }
        htf_1: Dict[str, List[Candle]] = market_1h or {
            s: resample(market_5m[s], RESAMPLE_N["1h"]) for s in symbols
        }
        ready_15: Dict[str, List[Candle]] = {s: [] for s in symbols}
        ready_1: Dict[str, List[Candle]] = {s: [] for s in symbols}
        idx_15: Dict[str, int] = {s: 0 for s in symbols}
        idx_1: Dict[str, int] = {s: 0 for s in symbols}

        for i in range(n_bars):
            self._sim_clock[0] = float(i * 300.0)
            # Daily-loss circuit breaker: roll the day-start at each new day,
            # mirroring the live engine's restart-safe daily-PnL reset.
            if i > 0 and i % 288 == 0:
                self.day_start = self.equity_history[-1] if self.equity_history \
                    else self.day_start
                self.state.set("day_start_balance", self.day_start)
            bar_close = {s: market_5m[s][i].c for s in symbols}

            # ---- 1) execute pending entries at this bar's open ----------
            self._execute_pending(market_5m, i)

            # ---- 2) manage open positions intrabar ----------------------
            for pos in list(self.positions):
                bar = market_5m[pos.symbol][i]
                self._manage_position(pos, bar, i)

            # ---- 3) mark-to-market + halts ------------------------------
            equity = self._equity(bar_close)
            self.peak = max(self.peak, equity)
            ok, _ = self.risk.update_halts(equity, self.peak, self.day_start)
            self.state.set("current_dd",
                           (self.peak - equity) / self.peak * 100.0 if self.peak > 0 else 0.0)
            if not ok:
                self.halted_bars += 1
                self.pending.clear()
            self.equity_history.append(equity)
            if self.positions:
                self.exposed_bars += 1

            # ---- 4) signal generation on closed bars ---------------------
            self._generate_signals(market_5m, htf_15, htf_1, ready_15, ready_1,
                                   idx_15, idx_1, symbols, i)

        # ---- close everything at the final price --------------------------
        final_price = {s: market_5m[s][-1].c for s in symbols}
        for pos in list(self.positions):
            self._close_position(pos, final_price[pos.symbol], n_bars - 1,
                                 "EndOfTest")

        return self._build_report(n_bars)

    # ------------------------------------------------------------------
    # Internal simulation steps
    # ------------------------------------------------------------------
    def _execute_pending(self, market: Dict[str, List[Candle]], i: int) -> None:
        if not self.pending:
            return
        # Respect the drawdown / daily-loss halts exactly like the live engine.
        if self.state.get("dd_halted") or self.state.get("daily_halted") \
                or not self.state.get("is_active", True):
            self.pending.clear()
            return
        open_symbols = {p.symbol for p in self.positions}
        for sym in list(self.pending.keys()):
            if sym in open_symbols:
                self.pending.pop(sym, None)
                continue
            if self.risk.daily_entries_left() <= 0:
                self.pending.clear()
                return
            signal = self.pending.pop(sym)
            bar = market[sym][i]
            fill = bar.o * (1.0 + self.slippage if signal.side == "buy" else 1.0 - self.slippage)
            equity = self._equity({s: market[s][i].c for s in market})
            free = self._free_margin(equity)
            risk_pct = self.risk.adaptive_risk_pct()
            size = self.sizer.compute(sym, fill, signal.sl, free, None, risk_pct)
            cap_err = self.limits.would_exceed(
                {p.symbol for p in self.positions}, len(self.positions),
                sym, fill, size.notional,
                open_notional=sum(p.qty * fill for p in self.positions),
            ) if size.ok else "sizing failed"
            if not size.ok or cap_err:
                continue
            margin = size.notional / self.settings.leverage
            if margin > free:
                continue
            self.risk.mark_entry(sym)
            self.risk.mark_daily_entry()
            self.positions.append(BacktestPosition(
                symbol=sym, side=signal.side, strategy=signal.strategy,
                entry=fill, qty=size.qty, sl=signal.sl, tp1=signal.tp1,
                tp=signal.tp, opened_bar=i, atr_at_entry=signal.atr,
            ))

    def _manage_position(self, pos: BacktestPosition, bar: Candle, i: int) -> None:
        s = self.settings
        hold = (i - pos.opened_bar) * 300.0
        buy = pos.side == "buy"

        # Partial take-profit.
        if (s.partial_tp and pos.is_partial == 0
                and hold >= s.min_hold_partial_s and pos.qty > 0):
            hit = (bar.h >= pos.tp1) if buy else (bar.l <= pos.tp1)
            if hit and pos.pnl_pct(pos.tp1) >= s.min_profit_be_pct:
                half = pos.qty / 2.0
                fill_tp1 = pos.tp1
                self._realize(pos, fill_tp1, half, "PartialTP1")
                pos.qty -= half
                pos.is_partial = 1
                pos.sl = pos.entry  # break-even
                return

        # Trailing stop (ATR-based, ratchet only).
        if hold >= s.min_hold_trail_s and pos.qty > 0:
            extreme = bar.h if buy else bar.l
            pnl_pct = pos.pnl_pct(extreme)
            if pnl_pct > s.trail_act_pct and pnl_pct > pos.highest_pnl_pct:
                pos.highest_pnl_pct = pnl_pct
                step_dist = extreme * s.trail_step_pct / 100.0
                atr_dist = pos.atr_at_entry * s.atr_trail_mult if s.use_atr_trail else 0.0
                dist = max(step_dist, atr_dist)
                if buy:
                    pos.sl = max(pos.sl, extreme - dist)
                else:
                    pos.sl = min(pos.sl, extreme + dist)

        # Hard exits (intrabar aware).
        if buy:
            if bar.l <= pos.sl:
                fill = min(bar.o, pos.sl)  # gap-through protection
                reason = "SL" if pos.highest_pnl_pct <= s.trail_act_pct else "Trail"
                if pos.is_partial == 1 and abs(pos.sl - pos.entry) < 1e-8:
                    reason = "BE"
                self._close_position(pos, fill, i, reason)
                return
            if bar.h >= pos.tp:
                self._close_position(pos, pos.tp, i, "TP")
                return
        else:
            if bar.h >= pos.sl:
                fill = max(bar.o, pos.sl)
                reason = "SL" if pos.highest_pnl_pct <= s.trail_act_pct else "Trail"
                if pos.is_partial == 1 and abs(pos.sl - pos.entry) < 1e-8:
                    reason = "BE"
                self._close_position(pos, fill, i, reason)
                return
            if bar.l <= pos.tp:
                self._close_position(pos, pos.tp, i, "TP")
                return

        # Maximum hold (time stop).
        if hold >= s.max_hold_s:
            self._close_position(pos, bar.c, i, "MaxHold")

    def _realize(self, pos: BacktestPosition, exit_price: float,
                 qty: float, reason: str) -> None:
        fees = (pos.entry + exit_price) * qty * self.settings.taker_fee
        pnl = pos.unrealized(exit_price) * (qty / pos.qty) - fees
        self.balance += pnl
        self.state.bump_loss_streak(pnl > 0)
        self.closed.append(BacktestTrade(
            symbol=pos.symbol, side=pos.side, strategy=pos.strategy,
            entry=pos.entry, exit_price=exit_price, qty=qty, pnl=pnl, fees=fees,
            hold_bars=0, reason=reason,
        ))

    def _close_position(self, pos: BacktestPosition, exit_price: float,
                        bar_idx: int, reason: str) -> None:
        if pos.qty <= 0:
            self.positions.remove(pos)
            return
        slip = 1.0 - self.slippage if pos.side == "buy" else 1.0 + self.slippage
        fill = exit_price * slip
        fees = (pos.entry + fill) * pos.qty * self.settings.taker_fee
        pnl = pos.unrealized(fill) - fees
        self.balance += pnl
        self.state.bump_loss_streak(pnl > 0)
        self.closed.append(BacktestTrade(
            symbol=pos.symbol, side=pos.side, strategy=pos.strategy,
            entry=pos.entry, exit_price=fill, qty=pos.qty, pnl=pnl, fees=fees,
            hold_bars=bar_idx - pos.opened_bar, reason=reason,
            opened_bar=pos.opened_bar, closed_bar=bar_idx,
        ))
        self.positions.remove(pos)

    def _generate_signals(self, market, htf_15, htf_1, ready_15, ready_1,
                          idx_15, idx_1, symbols, i) -> None:
        # Maintain ready 15m and 1h series (complete bars only).
        for sym in symbols:
            if (i + 1) % RESAMPLE_N["15m"] == 0:
                ready_15[sym].append(htf_15[sym][idx_15[sym]])
                idx_15[sym] += 1
            if (i + 1) % RESAMPLE_N["1h"] == 0:
                ready_1[sym].append(htf_1[sym][idx_1[sym]])
                idx_1[sym] += 1

        if i < MIN_BARS or self.pending or len(self.positions) >= self.settings.max_positions:
            return
        if i % SIGNAL_EVERY_N != 0:
            return
        for sym in symbols:
            if sym in {p.symbol for p in self.positions}:
                continue
            allowed, _ = self.risk.can_enter_symbol(sym)
            if not allowed:
                continue
            if i % SIGNAL_EVERY_N != 0:
                continue
            window5 = CandleSeries(market[sym][max(0, i - WIN["5m"] + 1):i + 1])
            window15 = CandleSeries(ready_15[sym][-WIN["15m"]:]) if len(ready_15[sym]) > 30 \
                else window5
            window1 = CandleSeries(ready_1[sym][-WIN["1h"]:]) if len(ready_1[sym]) > 30 \
                else window5
            result = self.strategy.analyze(window5, window15, window1,
                                           symbol=sym, drop_forming=False)
            self.decision_count += 1
            if result.signal is not None:
                self.pending[sym] = result.signal

    # ------------------------------------------------------------------
    # Accounting
    # ------------------------------------------------------------------
    def _equity(self, prices: Dict[str, float]) -> float:
        unrealized = sum(pos.unrealized(prices.get(pos.symbol, pos.entry))
                         for pos in self.positions)
        return self.balance + unrealized

    def _free_margin(self, equity: float) -> float:
        used = sum((p.qty * p.entry) / self.settings.leverage for p in self.positions)
        return max(0.0, equity - used)

    def _build_report(self, n_bars: int) -> BacktestReport:
        final = self.equity_history[-1] if self.equity_history else self.initial_balance
        report = BacktestReport(
            final_equity=final, initial_balance=self.initial_balance,
            total_return_pct=(final / self.initial_balance - 1.0) * 100.0,
            trades=self.closed, decisions=self.decision_count,
        )
        # Max drawdown + Sharpe from the equity curve.
        peak, max_dd = self.initial_balance, 0.0
        for eq in self.equity_history:
            peak = max(peak, eq)
            max_dd = max(max_dd, (peak - eq) / peak * 100.0)
        report.max_dd_pct = max_dd
        if len(self.equity_history) > 1:
            rets = [self.equity_history[k] / self.equity_history[k - 1] - 1.0
                    for k in range(1, len(self.equity_history))
                    if self.equity_history[k - 1] > 0]
            if len(rets) > 2:
                mean = sum(rets) / len(rets)
                var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
                if var > 1e-14:
                    # Annualise per-5m returns to a yearly Sharpe (365d).
                    report.sharpe = (mean / math.sqrt(var)) * math.sqrt(288 * 365)
        report.exposure_pct = self.exposed_bars / n_bars * 100.0 if n_bars else 0.0
        report.halted_bars = self.halted_bars

        per: Dict[str, Dict] = {}
        for t in self.closed:
            bucket = per.setdefault(t.strategy, {"n": 0, "pnl": 0.0, "wins": 0})
            bucket["n"] += 1
            bucket["pnl"] += t.pnl
            if t.pnl > 0:
                bucket["wins"] += 1
        for name, bucket in per.items():
            bucket["pnl"] = round(bucket["pnl"], 2)
            bucket["win_rate"] = round(bucket["wins"] / bucket["n"] * 100.0, 1) \
                if bucket["n"] else 0.0
        report.per_strategy = per
        return report
