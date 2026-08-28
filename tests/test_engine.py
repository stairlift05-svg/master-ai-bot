"""Unit tests for the Quant Engine v20 core logic.

Run with:  python tests/run_tests.py   (stdlib unittest, no pytest needed)

Covers the pure-logic layers: indicators, position sizing caps, risk gates,
signal stop/target consistency, wallet parsing, remote-position parsing,
adaptive risk and the shared state snapshot.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
import unittest
from dataclasses import replace
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
        rm = RiskManager(S, EngineState())
        # Default: FUNDING_MAX_PCT=0 → gate disabled (testnet placeholder data).
        self.assertFalse(rm.funding_blocked("buy", 0.75)[0])
        self.assertFalse(rm.funding_blocked("sell", -0.75)[0])
        # Explicitly enabled gate behaves as before.
        self.assertTrue(rm.funding_blocked("buy", 0.5, threshold=0.30)[0])
        self.assertFalse(rm.funding_blocked("buy", 0.1, threshold=0.30)[0])
        self.assertFalse(rm.funding_blocked("buy", -0.5, threshold=0.30)[0])
        self.assertTrue(rm.funding_blocked("sell", -0.5, threshold=0.30)[0])

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


# =========================================================================
# v20.4 regression tests: phantom-close loop, error envelopes, pagination
# =========================================================================
class TestV204Regressions(unittest.TestCase):
    """Guards against the v20.3.1 production bugs found in the live audit."""

    def _executor(self):
        from app.execution.executor import OrderExecutor
        st = EngineState()
        rm = RiskManager(S, st)
        sizer = PositionSizer(S)

        class FakeTG:
            menu = lambda self: ""  # noqa: E731
            async def send(self, *a, **k):
                return None

        class FakeDB:
            async def close_trade(self, *a, **k):
                return None

            async def update_analytics(self, *a, **k):
                return None

            async def insert_trade(self, *a, **k):
                return None

        class FakeClient:
            symbol_meta = {}

            async def place_order(self, *a, **k):
                return {"ok": True, "data": {"price": 100.0, "qty": 1}}

            async def get_positions(self):
                return {"ok": True, "data": [
                    {"symbol": "SOLUSD", "side": "buy", "qty": 0.31,
                     "entryPrice": 98.0},
                ]}

        class FakeMargin:
            async def refresh(self):
                return None

        # These regressions exercise the REAL exchange close path, so paper
        # mode (the shipped default) must be switched off for them.
        live = replace(S, paper_mode=False)
        return OrderExecutor(live, FakeClient(), st, rm, sizer, FakeMargin(),
                             FakeDB(), FakeTG(), lambda s: 100.0), st

    def _position(self):
        return Position(id="p1", symbol="SOLUSD", side="buy", strategy="X",
                        entry=98.0, qty=0.31, sl=95.0, tp1=101.0, tp=102.0,
                        opened_at=time.time() - 60)

    def test_error_envelope_detected(self):
        """HTTP-200 business rejections must never be parsed as fills."""
        ex, _ = self._executor()
        bad = {"ok": False, "error": "insufficient balance",
               "data": {"price": 104.3}}
        self.assertNotEqual(ex._error_message(bad), "")
        ok = {"ok": True, "retCode": 0, "result": {"price": 104.3}}
        self.assertEqual(ex._error_message(ok), "")

    def test_unverified_close_keeps_position_and_marks_stuck(self):
        """The infinite recover→close loop must break (SOLUSD incident)."""
        import asyncio
        ex, st = self._executor()
        for cycle in range(3):
            pos = self._position()
            pos.id = f"p{cycle}"
            st.add_position(pos)
            res = asyncio.run(ex.close(pos, "TP"))
            self.assertIsNone(res)                   # close never verified
            self.assertIn(f"p{cycle}", st.positions())  # NOT removed, no PnL
        self.assertIn("SOLUSD", st.get("stuck_symbols", set()))  # flagged

    def test_verified_close_finalizes(self):
        import asyncio
        ex, st = self._executor()

        class GoneClient(ex._client.__class__):
            async def get_positions(self):
                return {"ok": True, "data": []}

        ex._client = GoneClient()
        pos = self._position()
        st.add_position(pos)
        res = asyncio.run(ex.close(pos, "TP"))
        self.assertIsNotNone(res)
        self.assertNotIn("p1", st.positions())
        self.assertGreater(res.realized_pnl, 0)

    def test_pagination_assembles_history(self):
        """fetch_klines pages backwards when the proxy truncates history."""
        from app.api.ariax_client import AriaXClient
        from app.api.signing import RequestSigner

        class PagingClient(AriaXClient):
            def __init__(self):
                super().__init__(S, RequestSigner(S))
                self.calls = []

            async def request(self, method, path, json_body=None):
                self.calls.append(path)
                # First page: newest 51 bars; then full older pages.
                limit = int(path.split("limit=")[1].split("&")[0])
                if "end=" not in path:
                    rows = [[str((1787000000 + i * 3600) * 1000), "1", "2",
                             "0.5", "1.5", "10"] for i in range(51)]
                else:
                    end = int(path.split("end=")[1])
                    rows = [[str(end - i * 3600 * 1000), "1", "2", "0.5",
                             "1.5", "10"] for i in range(1, min(200, limit))]
                return {"retCode": 0, "result": {"list": rows}}

        cl = PagingClient()
        candles = asyncio.run(cl.fetch_klines("ADAUSD", "1h", 220))
        self.assertGreaterEqual(len(candles), 100)   # assembled via paging
        self.assertLessEqual(len(cl.calls), 4)
        ts = [c.ts for c in candles]
        self.assertEqual(ts, sorted(ts))             # oldest -> newest

    def test_http200_ghost_error_resolves_without_pnl(self):
        """A desynced /api/positions record (exchange bug observed live):
        closes answered HTTP 200 + 'qty exceeds position size' for ANY qty.
        Must resolve as ghost — never book phantom PnL."""
        import asyncio
        ex, st = self._executor()

        class GhostClient:
            symbol_meta = {}
            async def place_order(self, *a, **k):
                return {"ok": False, "error": "qty exceeds position size"}
            async def get_positions(self):
                return {"ok": True, "data": [{"symbol": "SOLUSD",
                                              "size": 0.31, "entry": 97.21}]}

        ex._client = GhostClient()
        pos = self._position()
        st.add_position(pos)
        res = asyncio.run(ex.close(pos, "TP"))
        self.assertIsNone(res)
        self.assertNotIn("p1", st.positions())   # ghost-resolved locally

    def test_default_config_is_safe_and_evidence_based(self):
        """v20.5: defaults must reflect the evidence, not the v20.4 claim.

        The screening report rejected every family ("None passed"), and a
        fresh hold-out re-run ranked HTF_Breakout LAST. So it must no longer
        be the shipped default, and real money must be opt-in.
        """
        self.assertNotIn("HTF_Breakout", S.enabled_strategies)
        # v20.6: shorts carry the entire edge in the bear half of the sample
        # (+$61.6 short vs -$6.4 long). Long-only must never be the default.
        self.assertEqual(S.sides, "both")
        self.assertEqual(S.funding_max_pct, 0.0)
        self.assertTrue(S.paper_mode, "real trading must be opt-in")
        self.assertGreaterEqual(S.min_edge_ratio, 1.0, "cost gate must be on")


class TestV205CostGate(unittest.TestCase):
    """The cost gate is the fix for the root cause of every losing version:
    signals whose target could not pay for fees + slippage."""

    def _ctx(self, tp_atr: float, cost_pct: float, edge_ratio: float):
        from app.strategy.signals import HtfContext, TFContext

        closes = [100.0] * 60
        tf = TFContext(label="x", closes=closes, highs=closes, lows=closes,
                       volumes=[1.0] * 60, atr=tp_atr, rsi=50.0, ema20=100.0,
                       ema50=100.0, trend="bullish", strength=1.0)
        return HtfContext(symbol="ETHUSD", price=100.0, tf5=tf, tf15=tf, tf1=tf,
                          candle_bull_5m=True, candle_bear_5m=False,
                          min_stop_pct=0.003, round_trip_cost_pct=cost_pct,
                          min_edge_ratio=edge_ratio)

    def _strategy(self):
        from app.strategy.signals import BaseStrategyV2

        class Probe(BaseStrategyV2):
            name = "Probe"

            def evaluate(self, ctx):
                # Target = 1 ATR, stop = 1 ATR.
                return self._build(ctx, "buy", "probe",
                                   sl_dist=ctx.tf1.atr, tp_dist=ctx.tf1.atr)

        return Probe({})

    def test_rejects_signal_that_cannot_pay_for_costs(self):
        """Target far below the cost floor -> no trade at all."""
        strat = self._strategy()
        # Tiny ATR target vs 1% round-trip friction (need 3% to be worth it).
        ctx = self._ctx(tp_atr=0.05, cost_pct=0.01, edge_ratio=3.0)
        self.assertIsNone(strat.propose(ctx))

    def test_widens_target_that_is_marginally_below_floor(self):
        """A target close to the floor is stretched, not discarded."""
        strat = self._strategy()
        # Target 0.45 (after the 1.5x stop floor) vs required 0.60.
        ctx = self._ctx(tp_atr=0.30, cost_pct=0.002, edge_ratio=3.0)
        sig = strat.propose(ctx)
        self.assertIsNotNone(sig)
        required = 100.0 * 0.002 * 3.0
        self.assertGreaterEqual(sig.tp - sig.entry, required - 1e-9)

    def test_healthy_signal_passes_untouched(self):
        strat = self._strategy()
        ctx = self._ctx(tp_atr=2.0, cost_pct=0.0014, edge_ratio=3.0)
        sig = strat.propose(ctx)
        self.assertIsNotNone(sig)
        # tp = max(atr target, 1.5 x stop) = 3.0, well clear of the 0.42 floor.
        self.assertAlmostEqual(sig.tp - sig.entry, 3.0, places=6)

    def test_gate_disabled_lets_everything_through(self):
        strat = self._strategy()
        ctx = self._ctx(tp_atr=0.05, cost_pct=0.01, edge_ratio=0.0)
        self.assertIsNotNone(strat.propose(ctx))


def _async_price(value: float):
    """Async price provider matching QuantEngine._live_price's signature."""
    async def _price(_symbol: str) -> float:
        return value
    return _price


class TestV205PaperMode(unittest.TestCase):
    """Paper mode must run the whole pipeline without reaching the exchange."""

    def _executor(self, paper: bool):
        from app.persistence.database import Database

        st = EngineState()
        st.set_many(balance=1000.0, free_balance=1000.0)
        settings = replace(S, paper_mode=paper, min_order_usd=1.0)

        class Client:
            symbol_meta = {}

            def __init__(self):
                self.sent = []

            async def place_order(self, *a, **k):
                self.sent.append((a, k))
                return {"ok": True, "avgPrice": 100.0, "qty": k.get("qty", 1)}

            async def get_positions(self):
                return {"ok": True, "data": []}

            async def get_markets(self):
                return {"ok": True, "data": {"ETHUSD": {"funding": 0.0}}}

        class FakeDB:
            async def insert_trade(self, *a, **k): return None
            async def close_trade(self, *a, **k): return None
            async def update_trade(self, *a, **k): return None
            async def log_decision(self, *a, **k): return None
            async def update_analytics(self, *a, **k): return None

        class FakeTG:
            async def send(self, *a, **k): return None
            def menu(self): return None

        class FakeMargin:
            async def refresh(self): return None

        client = Client()
        ex = OrderExecutor(settings, client, st, RiskManager(settings, st),
                           PositionSizer(settings), FakeMargin(), FakeDB(),
                           FakeTG(), _async_price(100.0))
        return ex, client, st

    def test_paper_mode_never_calls_the_exchange(self):
        from app.models import Signal

        ex, client, st = self._executor(paper=True)
        sig = Signal(side="buy", strategy="T", reason="r", entry=100.0,
                     sl=97.0, tp1=103.0, tp=106.0)
        pos = asyncio.run(ex.try_open("ETHUSD", sig))
        self.assertIsNotNone(pos)
        self.assertEqual(client.sent, [], "paper mode must not place orders")
        self.assertIn("ETHUSD", ex.paper_positions())

    def test_paper_fill_is_charged_slippage(self):
        from app.models import Signal

        ex, _, _ = self._executor(paper=True)
        sig = Signal(side="buy", strategy="T", reason="r", entry=100.0,
                     sl=97.0, tp1=103.0, tp=106.0)
        pos = asyncio.run(ex.try_open("ETHUSD", sig))
        # A buy fills ABOVE the mid price by the modelled slippage.
        self.assertGreater(pos.entry, 100.0)

    def test_paper_round_trip_clears_the_simulated_book(self):
        from app.models import Signal

        ex, client, st = self._executor(paper=True)
        sig = Signal(side="buy", strategy="T", reason="r", entry=100.0,
                     sl=97.0, tp1=103.0, tp=106.0)
        pos = asyncio.run(ex.try_open("ETHUSD", sig))
        res = asyncio.run(ex.close(pos, "TP"))
        self.assertIsNotNone(res)
        self.assertEqual(ex.paper_positions(), {})
        self.assertEqual(client.sent, [])

    def test_live_mode_still_reaches_the_exchange(self):
        from app.models import Signal

        ex, client, _ = self._executor(paper=False)
        sig = Signal(side="buy", strategy="T", reason="r", entry=100.0,
                     sl=97.0, tp1=103.0, tp=106.0)
        asyncio.run(ex.try_open("ETHUSD", sig))
        self.assertEqual(len(client.sent), 1)


class TestV205StressHarness(unittest.TestCase):
    def test_logic_assertions_run(self):
        """run_logic_assertions used to raise TypeError, killing simulate.py."""
        from app.backtest.stress import run_logic_assertions

        checks = run_logic_assertions(S)
        self.assertTrue(checks["funding_gate_blocks_long"])
        self.assertTrue(checks["funding_gate_allows_small"])
        self.assertTrue(all(checks.values()), checks)


class TestV206DonchianTrend(unittest.TestCase):
    """Locks in the properties that made Donchian_Trend survive out-of-sample
    testing where all six legacy families failed (analysis/STRATEGY_v20.6.md).
    """

    def _ctx(self, closes, highs=None, lows=None, atr=1.0, ema200=None):
        from app.strategy.signals import HtfContext, TFContext

        n = len(closes)
        highs = highs or list(closes)
        lows = lows or list(closes)
        tf = TFContext(label="1h", closes=closes, highs=highs, lows=lows,
                       volumes=[1.0] * n, atr=atr, rsi=50.0, ema20=closes[-1],
                       ema50=closes[-1],
                       ema200=ema200 if ema200 is not None else 100.0,
                       trend="bullish", strength=1.0)
        return HtfContext(symbol="ETHUSD", price=closes[-1], tf5=tf, tf15=tf,
                          tf1=tf, candle_bull_5m=True, candle_bear_5m=False,
                          min_stop_pct=0.003, round_trip_cost_pct=0.0014,
                          min_edge_ratio=3.0)

    def _strat(self, **over):
        from app.strategy.signals import build_strategy
        return build_strategy("Donchian_Trend", over or None)

    def test_registered_and_is_the_shipped_default(self):
        from app.strategy.signals import _STRATEGY_CLASSES
        self.assertIn("Donchian_Trend", _STRATEGY_CLASSES)
        self.assertEqual(S.enabled_strategies, ("Donchian_Trend",))

    def test_breaks_out_long_above_channel(self):
        closes = [100.0] * 60 + [130.0]
        sig = self._strat().propose(self._ctx(closes, atr=1.0, ema200=100.0))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "buy")

    def test_symmetric_short_below_channel(self):
        """Shorts produced the entire edge; the strategy must take them."""
        closes = [100.0] * 60 + [70.0]
        sig = self._strat().propose(self._ctx(closes, atr=1.0, ema200=100.0))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "sell")

    def test_marginal_poke_through_channel_is_rejected(self):
        """break_atr is the filter that turned the strategy profitable:
        58 stop-outs vs 7 targets without it."""
        closes = [100.0] * 60 + [100.5]     # 0.5 ATR break, needs 1.5
        self.assertIsNone(
            self._strat(break_atr=1.5).propose(
                self._ctx(closes, atr=1.0, ema200=100.0)))

    def test_regime_filter_blocks_counter_trend_breakout(self):
        """Breakout up while price is below the slow EMA -> no trade."""
        closes = [100.0] * 60 + [130.0]
        self.assertIsNone(
            self._strat().propose(self._ctx(closes, atr=1.0, ema200=500.0)))

    def test_no_signal_inside_the_channel(self):
        closes = [100.0 + (i % 5) for i in range(60)] + [102.0]
        self.assertIsNone(
            self._strat().propose(self._ctx(closes, atr=1.0, ema200=100.0)))

    def test_target_is_far_so_winners_can_run(self):
        """Fixed near targets capped winners at ~cost in every losing version."""
        closes = [100.0] * 60 + [130.0]
        sig = self._strat().propose(self._ctx(closes, atr=1.0, ema200=100.0))
        reward = abs(sig.tp - sig.entry)
        risk = abs(sig.entry - sig.sl)
        self.assertGreater(reward / risk, 3.0)

    def test_exit_policy_lets_trends_mature(self):
        """The 4h time stop closed 55/85 trades at ~zero PnL."""
        self.assertGreaterEqual(S.max_hold_s, 100 * 3600)
        self.assertFalse(S.partial_tp)

    def test_primary_timeframe_matches_validation(self):
        self.assertEqual(S.timeframe, "1h")
        self.assertIn(S.mid_timeframe, ("4h", "1h"))


class TestV206WarmupInvariants(unittest.TestCase):
    """The live feed must be able to satisfy the strategy's warm-up.

    Donchian_Trend's regime filter needs the EMA200 of the primary timeframe.
    If the feed can pass its own completeness gate while still delivering
    fewer bars than the strategy requires, the context silently falls back to
    the EMA50 — i.e. the bot would run an unvalidated strategy in production.
    """

    def test_feed_minimum_covers_strategy_warmup(self):
        import math
        from app.strategy.engine import MIN_BARS_5M

        accepted = max(30, math.ceil(S.candle_limit_5m * 0.80))
        # The live scan drops the forming bar before analysing.
        self.assertGreaterEqual(
            accepted - 1, MIN_BARS_5M,
            "feed can accept a batch too small for the strategy warm-up",
        )

    def test_warmup_covers_ema200_regime_filter(self):
        from app.strategy.engine import MIN_BARS_5M

        self.assertGreaterEqual(
            MIN_BARS_5M, 200,
            "EMA200 regime filter would degrade to EMA50",
        )

    def test_ema200_is_present_after_warmup(self):
        """Guard the actual indicator, not just the constant."""
        from app.strategy.engine import StrategyEngine, MIN_BARS_5M
        from app.models import Candle, CandleSeries

        rows = [Candle(ts=i * 3600000, o=100.0, h=101.0, l=99.0,
                       c=100.0 + i * 0.01, v=1.0)
                for i in range(MIN_BARS_5M + 1)]
        series = CandleSeries(rows)
        engine = StrategyEngine(S, EngineState())
        ctx = engine._build_context("ETHUSD", series, series, series)
        self.assertIsNotNone(ctx.tf5.ema200,
                             "regime filter has no EMA200 after warm-up")


# ---------------------------------------------------------------------------
# v21 review fixes: fill re-anchoring (F-03), per-TF signal cadence (F-02),
# live/backtest fee parity (F-09).
# ---------------------------------------------------------------------------
class TestV21ReviewFixes(unittest.TestCase):
    """The three P0/P1 harness-fidelity fixes from the 2026-08-28 review."""

    def _flat_market(self, n: int, gap_open: float = None) -> dict:
        """n flat 100.0 candles; if gap_open is set, the FINAL bar is a
        consistent gap bar (o=h=l=c around the gap open)."""
        from app.models import Candle
        rows = []
        for i in range(n - 1):
            rows.append(Candle(i * 3600_000, 100.0, 100.5, 99.5, 100.0, 10.0))
        if gap_open is None:
            i = n - 1
            rows.append(Candle(i * 3600_000, 100.0, 100.5, 99.5, 100.0, 10.0))
        else:
            i = n - 1
            g = gap_open
            rows.append(Candle(i * 3600_000, g, g * 1.01, g * 0.998,
                                g * 1.002, 10.0))
        return {"ETHUSD": rows}

    def _bt_with_probe(self, signal_side="buy", sl_dist=2.5, tp_dist=5.0,
                       n=280):
        """Backtester whose strategy emits one fixed signal on the last bar."""
        from app.backtest.backtester import Backtester
        from app.models import AnalysisResult, Signal

        class ProbeStrategy:
            name = "probe"

            def __init__(self):
                self.calls = 0

            def analyze(self, df5, df15, df1, symbol="", drop_forming=False):
                self.calls += 1
                # Fire exactly on the second-to-last bar so the order
                # executes at the FINAL (gap) bar's open.
                if len(df5.closes) != n - 1:
                    return AnalysisResult("neutral", "wait")
                price = df5.closes[-1]
                sign = 1.0 if signal_side == "buy" else -1.0
                sig = Signal(
                    side=signal_side, strategy="probe", reason="r",
                    entry=price, sl=price - sign * sl_dist,
                    tp1=price + sign * sl_dist * 0.8,
                    tp=price + sign * tp_dist, rsi=50.0, atr=1.0,
                    htf="bullish", confidence=0.5)
                return AnalysisResult(
                    signal_side, "probe fired", "probe", 50.0, 1.0, "bullish",
                    signal=sig)

        probe = ProbeStrategy()
        # Build with the DEFAULT enabled strategies (probe is not registered),
        # then swap the engine's strategy object AFTER construction.
        bt = Backtester(S, initial_balance=1000.0, base_tf="5m",
                        min_bars=n - 5, signal_every_n=1)
        bt.strategy = probe  # inject the probe in place of StrategyEngine
        return bt

    def _run_and_capture(self, bt, market, side: str):
        """Run the backtest, capturing the position's anchored levels on the
        first management call (the backtest closes everything at EndOfTest)."""
        seen: dict = {}
        orig = bt._manage_position

        def wrapper(pos, bar, i):
            if side == "buy":
                seen.setdefault("sl_dist", pos.entry - pos.sl)
                seen.setdefault("tp_dist", pos.tp - pos.entry)
            else:
                seen.setdefault("sl_dist", pos.sl - pos.entry)
                seen.setdefault("tp_dist", pos.entry - pos.tp)
            seen.setdefault("entry", pos.entry)
            return orig(pos, bar, i)

        bt._manage_position = wrapper
        bt.run(market)
        self.assertEqual(len(bt.closed), 1, "probe should have opened one trade")
        return seen, bt.closed[0]

    def test_backtest_reanchors_levels_to_actual_fill(self):
        """F-03: a gap-open fill must keep the INTENDED stop/target distance.

        The signal is anchored to the flat 100.0 close; the final bar opens at
        104. Without re-anchoring the realized stop distance would be 104-97.5
        = 6.5 (2.6x the budgeted 2.5). With it, entry-sl == 2.5 exactly.
        """
        bt = self._bt_with_probe(signal_side="buy", sl_dist=2.5, tp_dist=5.0, n=281)
        market = self._flat_market(281, gap_open=104.0)
        seen, trade = self._run_and_capture(bt, market, "buy")
        slip = bt.slippage
        expected_fill = 104.0 * (1.0 + slip)
        self.assertAlmostEqual(seen["entry"], expected_fill, places=6)
        self.assertAlmostEqual(seen["sl_dist"], 2.5, places=6,
                               msg="stop distance must equal the intended 2.5")
        self.assertAlmostEqual(seen["tp_dist"], 5.0, places=6,
                               msg="target distance must equal the intended 5.0")

    def test_backtest_reanchors_shorts_too(self):
        bt = self._bt_with_probe(signal_side="sell", sl_dist=3.0, tp_dist=6.0, n=281)
        market = self._flat_market(281, gap_open=96.0)  # gap-down open
        seen, trade = self._run_and_capture(bt, market, "sell")
        self.assertAlmostEqual(seen["sl_dist"], 3.0, places=6)
        self.assertAlmostEqual(seen["tp_dist"], 6.0, places=6)

    def test_cadence_default_is_live_equivalent_per_base_tf(self):
        """F-02: 1h base must evaluate every bar (live scans ~70s), 5m every 3."""
        from app.backtest.backtester import TF_PRESETS, Backtester
        self.assertEqual(TF_PRESETS["5m"][2], 3)
        self.assertEqual(TF_PRESETS["1h"][2], 1)
        self.assertEqual(Backtester(S, base_tf="1h").signal_every_n, 1)
        self.assertEqual(Backtester(S, base_tf="5m").signal_every_n, 3)
        # explicit override still wins
        self.assertEqual(Backtester(S, base_tf="1h",
                                    signal_every_n=2).signal_every_n, 2)

    def test_context_slots_match_live_4h_4h(self):
        """F-05: for a 1h base the mid/HTF context slots are both 4h (4 bars),
        matching the live mid_timeframe=4h / htf_timeframe=4h."""
        from app.backtest.backtester import TF_PRESETS
        self.assertEqual(TF_PRESETS["1h"][1]["15m"], 4)
        self.assertEqual(TF_PRESETS["1h"][1]["1h"], 4)

    def test_backtest_fees_match_live_formula(self):
        """F-09: backtest fees == 2 x fill x qty x taker_fee x fee_buffer,
        identical to executor.close."""
        from app.backtest.backtester import Backtester, BacktestPosition
        bt = Backtester(S, initial_balance=1000.0, base_tf="5m")
        pos = BacktestPosition(symbol="ETHUSD", side="buy", strategy="t",
                               entry=100.0, qty=1.0, sl=97.0, tp1=102.0,
                               tp=105.0, opened_bar=0, atr_at_entry=1.0)
        bt.positions.append(pos)
        bt._close_position(pos, exit_price=105.0, bar_idx=10, reason="TP")
        self.assertEqual(len(bt.closed), 1)
        fill = 105.0 * (1.0 - bt.slippage)  # a buy closes below the touch
        expected = 2.0 * fill * 1.0 * S.taker_fee * S.fee_buffer
        self.assertAlmostEqual(bt.closed[0].fees, expected, places=9)

    def test_executor_reanchors_levels_to_fill_price(self):
        """F-03 (live path): the Position registered by try_open keeps the
        intended SL/TP distances from the ACTUAL fill, not the stale anchor."""
        from app.models import Signal
        from app.persistence.database import Database  # noqa: F401

        st = EngineState()
        st.set_many(balance=1000.0, free_balance=1000.0)
        settings = replace(S, paper_mode=True, min_order_usd=1.0)

        class Client:
            symbol_meta = {}
            def __init__(self):
                self.sent = []
            async def place_order(self, *a, **k):
                self.sent.append((a, k))
                return {"ok": True, "avgPrice": 100.0, "qty": k.get("qty", 1)}
            async def get_positions(self):
                return {"ok": True, "data": []}
            async def get_markets(self):
                return {"ok": True, "data": {"ETHUSD": {"funding": 0.0}}}

        class FakeDB:
            async def insert_trade(self, *a, **k): return None
            async def close_trade(self, *a, **k): return None
            async def update_trade(self, *a, **k): return None
            async def log_decision(self, *a, **k): return None
            async def update_analytics(self, *a, **k): return None

        class FakeTG:
            async def send(self, *a, **k): return None
            def menu(self): return None

        class FakeMargin:
            async def refresh(self): return None

        # Live price moved to 105 while the signal was anchored at 100.
        ex = OrderExecutor(settings, Client(), st, RiskManager(settings, st),
                           PositionSizer(settings), FakeMargin(), FakeDB(),
                           FakeTG(), _async_price(105.0))
        sig = Signal(side="buy", strategy="T", reason="r", entry=100.0,
                     sl=97.5, tp1=102.0, tp=105.0)
        pos = asyncio.run(ex.try_open("ETHUSD", sig))
        self.assertIsNotNone(pos)
        slip = settings.slippage_pct
        self.assertAlmostEqual(pos.entry, 105.0 * (1.0 + slip), places=6)
        self.assertAlmostEqual(pos.entry - pos.sl, 2.5, places=6,
                               msg="stop distance must track the fill, not the anchor")
        self.assertAlmostEqual(pos.tp - pos.entry, 5.0, places=6)

# ======================================================================
# v21.1 hardening cycle (2026-08-28): security gates + verification
# ======================================================================


class TestV211Hardening(unittest.TestCase):
    """Review F-06/F-07/F-08/F-12/F-13/F-15 fixes, unit-verified."""

    # -- F-06: raw secret header is now a config flag -------------------
    def test_secret_header_sent_by_default(self):
        from app.api.signing import RequestSigner

        S_ = Settings()
        self.assertTrue(S_.send_secret_header)  # backward-compatible default
        headers = RequestSigner(S_).headers("GET", "/api/wallet")
        self.assertEqual(headers["X-API-Secret"], S_.arlax_secret)
        self.assertTrue(headers["X-BAPI-SIGNATURE"])

    def test_secret_header_can_be_switched_off(self):
        from app.api.signing import RequestSigner

        off = replace(S, send_secret_header=False)
        headers = RequestSigner(off).headers("GET", "/api/wallet")
        self.assertNotIn("X-API-Secret", headers)
        # The derived HMAC must still be present — auth never silently dies.
        self.assertTrue(headers["X-BAPI-SIGNATURE"])
        self.assertIn("X-API-Key", headers)
        # Signing is untouched by the flag (same key → same HMAC).
        on = RequestSigner(S)
        ts, payload = "1700000000000", "q=1"
        self.assertEqual(on._sign(ts, payload),
                         RequestSigner(off)._sign(ts, payload))

    # -- F-15: CANDLE_LIMIT_PRIMARY alias -------------------------------
    def test_candle_limit_primary_alias(self):
        import os

        saved = {k: os.environ.get(k) for k in ("CANDLE_LIMIT_PRIMARY",
                                                "CANDLE_LIMIT_5M")}
        try:
            os.environ.pop("CANDLE_LIMIT_5M", None)
            os.environ["CANDLE_LIMIT_PRIMARY"] = "320"
            self.assertEqual(Settings.from_env().candle_limit_5m, 320)
            # The legacy name still works and takes precedence.
            os.environ["CANDLE_LIMIT_5M"] = "310"
            self.assertEqual(Settings.from_env().candle_limit_5m, 310)
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    # -- F-12: dead validator removed ------------------------------------
    def test_dead_order_validator_removed(self):
        import app.security.secrets as sec

        self.assertFalse(hasattr(sec, "OrderValidator"))
        from app.security.validation import OrderValidator  # live one intact
        OrderValidator(S).validate("ETHUSD", "buy", 0.1, 100.0, 10.0)

    # -- F-13: close verification tolerance 55% -> 10% -------------------
    def _live_executor(self, remote_qty):
        st = EngineState()
        rm = RiskManager(S, st)
        sizer = PositionSizer(S)

        class FakeTG:
            menu = lambda self: ""  # noqa: E731
            async def send(self, *a, **k):
                return None

        class FakeDB:
            async def close_trade(self, *a, **k):
                return None

            async def update_analytics(self, *a, **k):
                return None

            async def insert_trade(self, *a, **k):
                return None

        class FakeClient:
            async def place_order(self, *a, **k):
                return {"ok": True, "data": {"price": 100.0, "qty": 1}}

            async def get_positions(self):
                if remote_qty is None:
                    return {"ok": True, "data": []}
                return {"ok": True, "data": [
                    {"symbol": "SOLUSD", "side": "buy", "qty": remote_qty,
                     "entryPrice": 98.0},
                ]}

        class FakeMargin:
            async def refresh(self):
                return None

        live = replace(S, paper_mode=False)
        return OrderExecutor(live, FakeClient(), st, rm, sizer, FakeMargin(),
                             FakeDB(), FakeTG(), lambda s: 100.0), st

    def _sol_position(self, pid="p1"):
        return Position(id=pid, symbol="SOLUSD", side="buy", strategy="X",
                        entry=98.0, qty=0.31, sl=95.0, tp1=101.0, tp=102.0,
                        opened_at=time.time() - 60)

    def test_close_rejected_when_45pct_still_open(self):
        """Old 55% threshold would have verified this — it must not."""
        import asyncio

        ex, st = self._live_executor(0.31 * 0.45)
        pos = self._sol_position()
        st.add_position(pos)
        res = asyncio.run(ex.close(pos, "TP"))
        self.assertIsNone(res)
        self.assertIn("p1", st.positions())

    def test_close_verified_when_down_to_dust(self):
        import asyncio

        ex, st = self._live_executor(0.31 * 0.05)  # 5% <= 10% dust floor
        pos = self._sol_position()
        st.add_position(pos)
        res = asyncio.run(ex.close(pos, "TP"))
        self.assertIsNotNone(res)
        self.assertNotIn("p1", st.positions())

    # -- F-07: dashboard auth is ON by default ----------------------------
    def _dash_app(self, dash_token=""):
        from app.server.web import create_app

        class FakeDB:
            async def init(self):
                return None

            async def get_recent_decisions(self, n):
                return []

            async def compute_metrics(self):
                return None

        return create_app(EngineState(), FakeDB(), replace(S, dash_token=dash_token))

    def test_dashboard_requires_token(self):
        app = self._dash_app("sekrit-token-123")
        c = app.test_client()
        self.assertEqual(c.get("/api/status").status_code, 401)
        self.assertEqual(c.get("/").status_code, 401)
        self.assertEqual(c.get("/health").status_code, 200)
        self.assertEqual(
            c.get("/api/status?token=sekrit-token-123").status_code, 200)
        self.assertEqual(
            c.get("/api/status",
                  headers={"X-Dash-Token": "sekrit-token-123"}).status_code,
            200)
        self.assertEqual(c.get("/api/status?token=WRONG").status_code, 401)

    def test_dashboard_autogenerates_token_when_unset(self):
        import logging

        records = []
        handler = logging.Handler()
        handler.emit = lambda record: records.append(record.getMessage())
        target = logging.getLogger("quant.web")
        old_level = target.level
        target.setLevel(logging.INFO)
        target.addHandler(handler)
        try:
            app = self._dash_app("")  # DASH_TOKEN unset
        finally:
            target.removeHandler(handler)
            target.setLevel(old_level)

        # Without the token nobody gets in…
        c = app.test_client()
        self.assertEqual(c.get("/api/status").status_code, 401)
        # …and the auto-generated token is published in the startup log.
        token_line = next(m for m in records
                          if "auto-generated token" in m)
        token = token_line.split("token: ")[1].split(" (")[0]
        self.assertTrue(token)
        self.assertEqual(c.get("/api/status?token=" + token).status_code, 200)

    # -- F-08: real test needs an explicit confirmation tap ---------------
    def test_realtest_requires_confirmation_tap(self):
        import asyncio
        import tempfile
        from unittest import mock

        from app.core.engine import QuantEngine

        st = EngineState()
        st.set("free_balance", 1000.0)
        calls = {"orders": 0, "sends": []}

        class FakeClient:
            symbol_meta = {}

            async def health(self):
                return True

            async def get_markets(self):
                return {}

            async def get_config(self):
                return {}

            async def get_wallet(self):
                return {"data": {}}

            async def get_positions(self):
                return {"ok": True, "data": []}

            async def place_order(self, *a, **k):
                calls["orders"] += 1
                return {"ok": True, "data": {"price": 100.0, "qty": 1}}

            async def close(self):
                return None

        class FakeDB:
            async def close_trade(self, *a, **k):
                return None

            async def update_analytics(self, *a, **k):
                return None

            async def insert_trade(self, *a, **k):
                return None

        db_path = os.path.join(tempfile.mkdtemp(), "t.db")
        eng = QuantEngine(S, st, db_path=db_path, client=FakeClient())
        eng.db = FakeDB()
        eng.executor._db = FakeDB()

        async def _noop():
            return None

        async def _px(_sym):
            return 100.0

        async def _capture(text, markup=None):
            calls["sends"].append((text, markup))

        async def _fast_sleep(*a, **k):
            return None

        eng.margin.refresh = _noop
        eng._live_price = _px
        eng.executor._price = _px  # executor captured the original bound method
        eng.tg.send = _capture

        cbs = eng.tg._callbacks
        self.assertIs(cbs["cmd_realtest"].__func__,
                      QuantEngine._tg_realtest_prompt)
        self.assertIs(cbs["cmd_realtest_yes"].__func__,
                      QuantEngine.real_test_trade)

        async def scenario():
            # Tap 1: the menu button must only open the confirmation gate.
            await cbs["cmd_realtest"]()
            nonlocal_ok = calls["orders"] == 0
            prompt = calls["sends"][-1][1]
            flat = [b["callback_data"]
                    for row in prompt["inline_keyboard"] for b in row]
            self.assertIn("cmd_realtest_yes", flat)
            self.assertTrue(nonlocal_ok, "tap 1 must NOT place an order")
            # Tap 2: explicit YES places exactly one live order.
            with mock.patch("asyncio.sleep", new=_fast_sleep):
                await cbs["cmd_realtest_yes"]()
            self.assertEqual(calls["orders"], 1)

        asyncio.run(scenario())

        # No failure message was sent (the fake order filled cleanly).
        self.assertFalse(any(t.startswith("❌ Test failed")
                             for t, _ in calls["sends"]))
        # Menu button labels the action as real-money.
        menu = eng.tg.menu()
        labels = [b["text"] for row in menu["inline_keyboard"] for b in row]
        self.assertIn("⚡ REAL TEST (real $)", labels)

    # -- F-14 hygiene: funding gate stays OFF, documented ------------------
    def test_funding_gate_default_off(self):
        # The testnet reports a static placeholder funding rate, so the gate
        # must stay disabled (0 = off) — enabled with fake data it would
        # reject every trade.
        self.assertEqual(S.funding_max_pct, 0.0)


if __name__ == "__main__":
    unittest.main()
