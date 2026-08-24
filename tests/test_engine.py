"""Unit tests for the Quant Engine v20 core logic.

Run with:  python tests/run_tests.py   (stdlib unittest, no pytest needed)

Covers the pure-logic layers: indicators, position sizing caps, risk gates,
signal stop/target consistency, wallet parsing, remote-position parsing,
adaptive risk and the shared state snapshot.
"""
from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.capital.margin import MarginManager  # noqa: E402
from app.config import Settings  # noqa: E402
from app.execution.watchdog import PositionWatchdog  # noqa: E402
from app.execution.executor import OrderExecutor  # noqa: E402
from app.data.feed import CandleFeed  # noqa: E402
from app.models import Candle, CandleSeries, Position  # noqa: E402
from app.optimization.optimizer import AdaptiveRisk, PortfolioLimits  # noqa: E402
from app.observability.reporter import build_txt_report  # noqa: E402
from app.risk.position_sizer import PositionSizer  # noqa: E402
from app.risk.risk_manager import RiskManager  # noqa: E402
from app.security.validation import OrderValidator  # noqa: E402
from app.errors import OrderRejectedError  # noqa: E402
from app.state import EngineState  # noqa: E402
from app.strategy import indicators as ind  # noqa: E402

S = Settings()


class TestConfiguration(unittest.TestCase):
    """Cross-module configuration invariants."""

    def test_live_candle_limit_can_satisfy_strategy_warmup(self):
        from app.strategy.engine import MIN_BARS_5M

        self.assertGreaterEqual(S.candle_limit_5m - 1, MIN_BARS_5M)

    def test_rejects_impossible_candle_limit(self):
        from dataclasses import replace
        from app.errors import ConfigError

        with self.assertRaises(ConfigError):
            replace(S, candle_limit_5m=100).validate()

    def test_default_aggregate_cap_fits_all_position_slots(self):
        self.assertGreaterEqual(
            S.max_agg_notional_usd,
            S.max_positions * S.max_notional_usd,
        )


class TestIndicators(unittest.TestCase):
    def test_rsi_bounds_and_tail(self):
        closes = [100.0 + i * 0.1 for i in range(80)]
        rsi = ind.rsi_wilder(closes, 14)
        self.assertTrue(all(v is None or 0 <= v <= 100 for v in rsi))
        self.assertIsNotNone(rsi[-1])

    def test_supertrend_sanity(self):
        closes = [100.0 + i * 0.1 for i in range(80)]
        highs = [c * 1.01 for c in closes]
        lows = [c * 0.99 for c in closes]
        direction, upper, lower = ind.supertrend(highs, lows, closes, 10, 3.0)
        self.assertIn(direction[-1], (1, -1))
        for u, l in zip(upper, lower):
            if u is not None and l is not None:
                self.assertGreaterEqual(u, l)

    def test_quantize(self):
        self.assertEqual(ind.quantize(0.123456789, 0.01), 0.12)
        self.assertEqual(ind.quantize(0.123456789, 0), round(0.123456789, 8))


class TestPositionSizer(unittest.TestCase):
    def _sizer(self):
        return PositionSizer(S)

    def test_caps(self):
        sizer = self._sizer()
        size = sizer.compute("ETHUSD", 1800.0, 1790.0, 200.0)
        self.assertTrue(size.ok)
        self.assertLessEqual(size.notional, S.max_notional_usd + 1e-6)
        self.assertFalse(sizer.compute("ETHUSD", 1800.0, 1790.0, 3.0).ok)

    def test_risk_matches_stop(self):
        """Risk budget must equal |entry - sl| * qty (within tolerance)."""
        sizer = self._sizer()
        # Parameters chosen so the notional cap does not bind: the risk budget
        # is the binding constraint and actual risk must match it.
        size = sizer.compute("ETHUSD", 1800.0, 1790.0, 100.0)
        self.assertTrue(size.ok)
        budgeted = 100.0 * (S.risk_pct / 100.0)  # $0.40
        actual = abs(1800.0 - 1790.0) * size.qty
        self.assertAlmostEqual(actual, budgeted, delta=budgeted * 0.05)

    def test_notional_cap_dominates_on_large_account(self):
        """A large account must still be capped by MAX_NOTIONAL_USD."""
        sizer = self._sizer()
        size = sizer.compute("ETHUSD", 1800.0, 1790.0, 5000.0)
        self.assertTrue(size.ok)
        self.assertLessEqual(size.notional, S.max_notional_usd + 1e-6)


class TestOrderValidation(unittest.TestCase):
    """Verify that malformed financial values cannot bypass hard limits."""

    def test_rejects_non_finite_values(self):
        validator = OrderValidator(S)
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value), self.assertRaises(OrderRejectedError):
                validator.validate("ETHUSD", "buy", value, 100.0, 100.0)

    def test_rejects_inconsistent_notional(self):
        with self.assertRaises(OrderRejectedError):
            OrderValidator(S).validate("ETHUSD", "buy", 2.0, 100.0, 100.0)

    def test_accepts_consistent_order(self):
        OrderValidator(S).validate("ETHUSD", "buy", 0.1, 100.0, 10.0)


class TestRiskGates(unittest.TestCase):
    def test_funding_gate(self):
        self.assertTrue(RiskManager.funding_blocked("buy", 0.5)[0])
        self.assertFalse(RiskManager.funding_blocked("buy", 0.1)[0])
        self.assertFalse(RiskManager.funding_blocked("buy", -0.5)[0])
        self.assertTrue(RiskManager.funding_blocked("sell", -0.5)[0])

    def test_portfolio_limits(self):
        limits = PortfolioLimits(S)
        err = limits.would_exceed({"ETHUSD"}, 1, "SOLUSD", 100.0, 50.0)
        self.assertEqual(err, "")
        self.assertIn("already open",
                      limits.would_exceed({"ETHUSD"}, 1, "ETHUSD", 100.0, 50.0))
        self.assertIn("max positions",
                      limits.would_exceed(set(), 5, "SOLUSD", 100.0, 50.0))

    def test_adaptive_risk(self):
        state = EngineState()
        state.set("current_dd", 8.0)
        self.assertLess(AdaptiveRisk(S, state).profile().factor, 1.0)
        state.set("current_dd", 0.5)
        self.assertEqual(AdaptiveRisk(S, state).profile().factor, 1.0)


class TestSignalConsistencyV2(unittest.TestCase):
    @staticmethod
    def _mk_context(trend: str = "bullish", strength: float = 0.1,
                    rsi15: float = 35.0, price: float = 1800.0,
                    bb_lower_factor: float = 0.98):
        from app.strategy.signals import HtfContext, TFContext

        def tf(label, closes):
            n = len(closes)
            return TFContext(
                label=label, closes=closes,
                highs=[c * 1.01 for c in closes],
                lows=[c * 0.99 for c in closes],
                volumes=[100.0] * n,
                atr=2.0 if label != "1h" else 6.0,
                rsi=rsi15 if label == "15m" else 50.0,
                ema20=closes[-1] * 0.995, ema50=closes[-1] * 0.99,
                ema200=closes[-1] * 0.95 if label == "1h" else None,
                hh=max(closes), ll=min(closes), trend=trend,
                strength=strength, mid=closes[-1],
                bb_upper=closes[-1] * 1.02, bb_lower=closes[-1] * bb_lower_factor,
            )

        closes = [1000.0 + i * 0.5 for i in range(300)]
        return HtfContext(
            symbol="ETHUSD", price=price, tf5=tf("5m", closes),
            tf15=tf("15m", closes[::3]), tf1=tf("1h", closes[::12]),
            candle_bull_5m=True, candle_bear_5m=False,
            min_stop_pct=0.003,
        )

    def test_buy_signal_structure(self):
        from app.strategy.signals import build_strategy
        strat = build_strategy("TrendPullback_HTF", {"sl_m": 2.0, "tp_m": 3.0})
        sig = strat.evaluate(self._mk_context())
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "buy")
        sl_dist = sig.entry - sig.sl
        self.assertGreaterEqual(sl_dist, sig.entry * 0.003 - 1e-9)
        self.assertGreater(sig.tp - sig.sl, sl_dist * 1.5)
        self.assertGreater(sig.tp1, sig.sl)

    def test_mean_reversion_requires_sideways(self):
        from app.strategy.signals import build_strategy
        strat = build_strategy("MeanReversion_BB")
        # Trending 1h -> mean reversion must NOT fire.
        self.assertIsNone(strat.evaluate(self._mk_context(trend="bullish")))
        # Sideways + oversold 15m RSI + price touching the lower band -> fires.
        ctx = self._mk_context(trend="sideways", strength=0.05, rsi15=25.0,
                               bb_lower_factor=1.0)
        sig = strat.evaluate(ctx)
        self.assertIsNotNone(sig)

    def test_unknown_strategy_raises(self):
        from app.strategy.signals import build_strategy
        with self.assertRaises(KeyError):
            build_strategy("Does_Not_Exist")


class TestParsing(unittest.TestCase):
    def test_wallet_dual_shape(self):
        data = {
            "ok": True,
            "equity": 500.0,
            "free_margin": 120.0,
            "balances": {"USDT": 300.0},
            "locks": {"USDT": 10.0},
            "futures": {"balances": {"USDT": 210.0},
                        "locks": {"USDT": 0.0}},
        }
        wallet = MarginManager.parse_wallet(MarginManager(S, None, None), data)  # type: ignore[arg-type]
        self.assertAlmostEqual(wallet.equity, 500.0)
        # The futures wallet balance is authoritative when present.
        self.assertAlmostEqual(wallet.futures_free, 210.0)
        self.assertAlmostEqual(wallet.spot_free, 290.0)
        # Free-margin fallback applies when the futures wallet is absent.
        legacy = {"ok": True, "equity": 500.0, "free_margin": 120.0}
        self.assertAlmostEqual(
            MarginManager.parse_wallet(MarginManager(S, None, None), legacy).futures_free,  # type: ignore[arg-type]
            120.0,
        )

    def test_remote_positions(self):
        from app.core.engine import QuantEngine
        parsed = QuantEngine._parse_remote_positions(
            {"ok": True, "data": [
                {"symbol": "ETHUSD", "side": "buy", "size": 0.5, "entry": 1800.0},
                {"symbol": "XRP/USDT", "qty": -100.0, "entryPrice": 0.55},
            ]})
        self.assertEqual(parsed["ETHUSD"]["qty"], 0.5)
        self.assertEqual(parsed["XRPUSDT"]["side"], "sell")
        self.assertEqual(parsed["XRPUSDT"]["qty"], 100.0)


class TestState(unittest.TestCase):
    def test_snapshot_is_serialisable(self):
        state = EngineState()
        snap = state.snapshot()
        self.assertIn("balance", snap)
        self.assertIn("active_positions", snap)

    def test_position_lifecycle(self):
        state = EngineState()
        pos = Position(id="p1", symbol="ETHUSD", side="buy", strategy="T",
                       entry=100.0, qty=0.5, sl=99.0, tp1=101.0, tp=102.0)
        state.add_position(pos)
        self.assertIsNotNone(state.position("p1"))
        self.assertEqual(pos.unrealized_pnl(101.0), 0.5)
        state.remove_position("p1")
        self.assertIsNone(state.position("p1"))


class TestDataStall(unittest.TestCase):
    def test_stall_triggers_close(self):
        watchdog = PositionWatchdog(S, EngineState(), None, None)  # type: ignore[arg-type]
        pos = Position(id="p1", symbol="ETHUSD", side="buy", strategy="T",
                       entry=100.0, qty=0.1, sl=99.0, tp1=101.0, tp=102.0,
                       opened_at=0.0)
        reason = asyncio.run(watchdog._evaluate(  # noqa: SLF001
            pos, lambda s: (0.0, 99999.0), lambda s: 0.0))
        self.assertEqual(reason, "DataStall")


class TestReporter(unittest.TestCase):
    """Regression tests for sparse feed-health counters."""

    def test_report_accepts_success_only_counter(self):
        from types import SimpleNamespace

        class FakeDatabase:
            async def get_recent_decisions(self, limit):
                return []

            async def get_closed_trades(self, limit):
                return []

            async def compute_metrics(self):
                return SimpleNamespace(
                    total_trades=0, win_rate=0.0, profit_factor=0.0,
                    expectancy=0.0, total_pnl=0.0, max_dd_pct=0.0,
                    sharpe=0.0,
                )

        state = EngineState()
        state.record_fetch("ETHUSD", "5m", True)
        report = asyncio.run(build_txt_report(state, FakeDatabase(), S, {}, {}))
        self.assertIn("5m 1/0", report)
        self.assertIn("1h 0/0", report)


class TestLiveDataCompleteness(unittest.TestCase):
    """Prevent truncated API history from trapping strategies in warm-up."""

    @staticmethod
    def _candles(count):
        return [Candle(1_700_000_000_000 + i * 300_000, 100, 101, 99, 100, 10)
                for i in range(count)]

    def test_rejects_truncated_primary_history(self):
        self.assertFalse(CandleFeed._good(self._candles(100), 300))

    def test_accepts_complete_history(self):
        self.assertTrue(CandleFeed._good(self._candles(300), 300))


class TestExecutionResponseParsing(unittest.TestCase):
    """AriaX may return order fields below data/result envelopes."""

    def test_nested_fill_is_parsed(self):
        response = {"ok": True, "data": {"result": {
            "orderId": "abc", "avgPrice": "123.45", "executedQty": "2.5"
        }}}
        self.assertEqual(OrderExecutor._fill_price(response, "ETHUSD", 2.5), 123.45)
        self.assertEqual(OrderExecutor._fill_qty(response, 1.0), 2.5)
        self.assertEqual(OrderExecutor._order_id(response), "abc")

    def test_missing_fill_is_not_zero_price(self):
        self.assertEqual(OrderExecutor._fill_price({"ok": True}, "ETHUSD", 1), 0.0)


class TestCandleSeries(unittest.TestCase):
    def test_window_and_helpers(self):
        candles = [Candle(1000 + i * 300, 1.0, 1.1, 0.9, 1.05, 10.0)
                   for i in range(10)]
        series = CandleSeries(candles)
        self.assertEqual(len(series), 10)
        self.assertEqual(len(series.window(3)), 3)
        self.assertEqual(series.closes[-1], 1.05)
        self.assertEqual(len(series.without_last()), 9)


if __name__ == "__main__":
    unittest.main(verbosity=2)
