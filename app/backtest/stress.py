"""Backtesting (#05): stress scenarios and logic assertions.

Two complementary stress layers:

1. **Market-shock backtests** — mutate the synthetic market (flash crash,
   gap shock, volume drought) and re-run the full pipeline, comparing risk
   metrics against the baseline.
2. **Logic assertions** — unit-level checks of the pure risk/security logic:
   funding-drag gate, data-stall defensive close, position-size caps,
   indicator sanity, and remote-position parsing.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from app.backtest.backtester import Backtester, BacktestReport
from app.backtest.synthetic import build_market
from app.config import Settings
from app.execution.watchdog import PositionWatchdog
from app.models import Candle, Position
from app.risk.position_sizer import PositionSizer
from app.risk.risk_manager import RiskManager
from app.state import EngineState
from app.strategy import indicators as ind

SCENARIOS = ("baseline", "flash_crash", "gap_shock", "volume_drought")


# ---------------------------------------------------------------------------
# Scenario mutations
# ---------------------------------------------------------------------------


def mutate_flash_crash(market: Dict[str, List[Candle]], seed: int) -> None:
    """~-22% crash over 6 bars then a partial recovery, on every symbol."""
    rng = random.Random(seed)
    for sym, candles in market.items():
        i0 = len(candles) // 3
        for k in range(6):
            idx = i0 + k
            if idx >= len(candles):
                break
            c = candles[idx]
            factor = 0.965
            candles[idx] = Candle(c.ts, c.o * factor, c.h * factor,
                                  c.l * factor, c.c * factor, c.v * 3.0)
        for k in range(1, 31):
            idx = i0 + 6 + k
            if idx >= len(candles):
                break
            c = candles[idx]
            factor = 1.0045
            candles[idx] = Candle(c.ts, c.o * factor, c.h * factor,
                                  c.l * factor, c.c * factor, c.v)


def mutate_gap_shock(market: Dict[str, List[Candle]], seed: int) -> None:
    """A +7% gap-up bar followed by a -9% gap-down bar (stop-gap test)."""
    for candles in market.values():
        i0 = len(candles) // 3
        up = candles[i0]
        candles[i0] = Candle(up.ts, up.o, up.h * 1.07, up.l, up.c * 1.07, up.v)
        down = candles[i0 + 1]
        candles[i0 + 1] = Candle(down.ts, down.o, down.h,
                                 down.l * 0.91, down.c * 0.91, down.v)


def mutate_volume_drought(market: Dict[str, List[Candle]], seed: int) -> None:
    """Two hours of near-zero volume (dries up volume-surge signals)."""
    for candles in market.values():
        i0 = len(candles) // 3
        for idx in range(i0, min(i0 + 24, len(candles))):
            c = candles[idx]
            candles[idx] = Candle(c.ts, c.o, c.h, c.l, c.c, c.v * 0.02)


_MUTATORS: Dict[str, Callable] = {
    "flash_crash": mutate_flash_crash,
    "gap_shock": mutate_gap_shock,
    "volume_drought": mutate_volume_drought,
}


# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------


@dataclass
class StressReport:
    baseline: BacktestReport = field(default_factory=BacktestReport)
    scenarios: Dict[str, BacktestReport] = field(default_factory=dict)
    assertions: Dict[str, bool] = field(default_factory=dict)

    def summary_lines(self) -> List[str]:
        lines = ["Scenario            Return%   MaxDD%   Trades   WR%    PF     Surviv",
                 "-" * 70]
        rows = [("baseline", self.baseline)] + [
            (name, rep) for name, rep in self.scenarios.items()
        ]
        for name, rep in rows:
            lines.append(
                f"{name:<18} {rep.total_return_pct:>7.2f}  {rep.max_dd_pct:>6.2f}  "
                f"{rep.n_trades:>5d}  {rep.win_rate():>5.1f}  "
                f"{rep.profit_factor():>5.2f}   {'YES' if rep.final_equity > 0 else 'NO'}"
            )
        return lines


def run_stress(settings: Settings, days: int = 40, seed: int = 42,
               symbols: Optional[List[str]] = None,
               balance: float = 500.0) -> StressReport:
    """Run the baseline backtest plus every market-shock scenario."""
    syms = symbols or list(settings.symbols)[:4]
    base_prices = {"ETHUSD": 1800.0, "SOLUSD": 120.0, "XRPUSD": 0.55,
                   "AVAXUSD": 25.0, "DOTUSD": 5.0, "LINKUSD": 12.0,
                   "ADAUSD": 0.45, "DOGEUSD": 0.08}

    def run(market: Dict[str, List[Candle]]) -> BacktestReport:
        bt = Backtester(settings, initial_balance=balance)
        return bt.run(market)

    baseline_market = build_market(syms, days, seed, base_prices)
    report = StressReport(baseline=run(baseline_market))
    for idx, (name, mutator) in enumerate(_MUTATORS.items()):
        market = build_market(syms, days, seed + 1000 + idx * 13, base_prices)
        mutator(market, seed + 1 + idx)
        report.scenarios[name] = run(market)
    report.assertions = run_logic_assertions(settings)
    return report


# ---------------------------------------------------------------------------
# Logic assertions (pure risk/security checks)
# ---------------------------------------------------------------------------


def run_logic_assertions(settings: Settings) -> Dict[str, bool]:
    """Execute deterministic unit checks; return {name: passed}."""
    checks: Dict[str, bool] = {}
    state = EngineState()

    # 1) Funding-drag gate. ``funding_blocked`` is an *instance* method; the
    # old static-style call raised TypeError and aborted every simulate.py run.
    # The gate is exercised with an explicit threshold so the check is
    # independent of FUNDING_MAX_PCT (0 = gate disabled on the testnet).
    risk_probe = RiskManager(settings, EngineState())
    blocked, why = risk_probe.funding_blocked("buy", 0.50, threshold=0.30)
    checks["funding_gate_blocks_long"] = blocked and "funding" in why
    blocked2, _ = risk_probe.funding_blocked("buy", 0.10, threshold=0.30)
    checks["funding_gate_allows_small"] = not blocked2
    blocked3, _ = risk_probe.funding_blocked("buy", -0.50, threshold=0.30)
    checks["funding_gate_long_ok_when_negative"] = not blocked3

    # 2) Data-stall defensive close.
    dummy_executor = None
    watchdog = PositionWatchdog(settings, state, dummy_executor, None)  # type: ignore[arg-type]
    pos = Position(id="p1", symbol="ETHUSD", side="buy", strategy="T",
                   entry=100.0, qty=0.1, sl=99.0, tp1=101.0, tp=102.0,
                   opened_at=0.0)

    async def _evaluate_probe() -> Optional[str]:
        # Replicate watchdog._evaluate without executor (price provider dead).
        return await watchdog._evaluate(  # noqa: SLF001
            pos, lambda s: (0.0, 99999.0), lambda s: 0.0,
        )
    import asyncio
    checks["data_stall_triggers_close"] = asyncio.run(_evaluate_probe()) == "DataStall"

    # 3) Position sizing caps.
    sizer = PositionSizer(settings)
    size = sizer.compute("ETHUSD", 1800.0, 1790.0, 200.0)
    checks["sizer_ok_with_enough_margin"] = size.ok
    checks["sizer_notional_capped"] = size.notional <= settings.max_notional_usd + 1e-6
    small = sizer.compute("ETHUSD", 1800.0, 1790.0, 3.0)
    checks["sizer_refuses_tiny_balance"] = not small.ok
    huge_risk = sizer.compute("ETHUSD", 1800.0, 1798.0, 200.0)
    checks["sizer_refuses_tight_stop_overflow"] = huge_risk.notional <= settings.max_notional_usd + 1e-6

    # 4) Indicator sanity.
    closes = [100.0 + i * 0.1 for i in range(80)]
    rsi = ind.rsi_wilder(closes, 14)
    checks["rsi_bounds"] = all(v is None or 0 <= v <= 100 for v in rsi)
    checks["rsi_tail_computed"] = rsi[-1] is not None
    highs = [c * 1.01 for c in closes]
    lows = [c * 0.99 for c in closes]
    direction, upper, lower = ind.supertrend(highs, lows, closes, 10, 3.0)
    checks["supertrend_direction_valid"] = direction[-1] in (1, -1)
    checks["supertrend_bands_sane"] = all(
        u is None or l is None or u >= l for u, l in zip(upper, lower)
    )

    # 5) Remote-position parsing (dual shapes).
    payload_a = {"ok": True, "data": [
        {"symbol": "ETHUSD", "side": "buy", "size": 0.5, "entry": 1800.0},
        {"symbol": "SOLUSD", "side": "sell", "size": 2.0, "entry": 120.0},
    ]}
    parsed = QuantEngineStatic.parse_remote(payload_a)
    checks["remote_parse_shape_a"] = (
        parsed.get("ETHUSD", {}).get("qty") == 0.5
        and parsed.get("SOLUSD", {}).get("side") == "sell"
    )
    payload_b = {"ok": True, "data": [
        {"symbol": "XRP/USDT", "qty": -100.0, "entryPrice": 0.55},
    ]}
    parsed_b = QuantEngineStatic.parse_remote(payload_b)
    checks["remote_parse_signed_size"] = (
        parsed_b.get("XRPUSDT", {}).get("side") == "sell"
        and parsed_b.get("XRPUSDT", {}).get("qty") == 100.0
    )

    # 6) Adaptive risk shrinks after drawdown.
    from app.optimization.optimizer import AdaptiveRisk
    state.set("current_dd", 8.0)
    adaptive = AdaptiveRisk(settings, state)
    checks["adaptive_risk_shrinks_on_dd"] = adaptive.profile().factor < 1.0

    return checks


class QuantEngineStatic:
    """Static mirror of engine parsing helpers for assertion tests."""

    @staticmethod
    def parse_remote(data) -> Dict[str, Dict]:
        from app.core.engine import QuantEngine  # type: ignore[attr-defined]

        return QuantEngine._parse_remote_positions(data)  # noqa: SLF001
