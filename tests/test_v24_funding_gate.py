"""v24 sprint 8: the Donchian funding-crowding gate.

Dual-window validated (analysis/runs/v24_sprint8.json): skipping a breakout
entry when our side is the crowded, paying side of the perpetual funding
market improved BOTH windows (A +$188.26 / B +$147.10 vs baseline
+$163.56 / +$143.06; plateau 0.010%-0.020%/8h). Shipped threshold:
±0.01%/8h, the exchange-standard rate. 0 disables; a None rate (data
outage) leaves the gate inert.
"""
import asyncio
import os
import unittest

from app.config import Settings


def _ctx(closes, atr=1.0, ema200=100.0, funding_rate=None):
    from app.strategy.signals import HtfContext, TFContext

    n = len(closes)
    tf = TFContext(label="1h", closes=closes, highs=list(closes),
                   lows=list(closes), volumes=[1.0] * n, atr=atr, rsi=50.0,
                   ema20=closes[-1], ema50=closes[-1], ema200=ema200,
                   trend="bullish", strength=1.0)
    return HtfContext(symbol="ETHUSD", price=closes[-1], tf5=tf, tf15=tf,
                      tf1=tf, candle_bull_5m=True, candle_bear_5m=False,
                      min_stop_pct=0.003, round_trip_cost_pct=0.0014,
                      min_edge_ratio=3.0, funding_rate=funding_rate)


class TestFundingGate(unittest.TestCase):

    def _strat(self, **over):
        from app.strategy.signals import build_strategy
        return build_strategy("Donchian_Trend", over or None)

    def test_shipped_defaults_are_the_validated_gate(self):
        p = Settings.from_env().strategy_params["Donchian_Trend"]
        self.assertEqual(p["funding_max_long"], 0.0001)
        self.assertEqual(p["funding_min_short"], -0.0001)

    def test_env_override_wired_and_can_disable(self):
        os.environ["DONCHIAN_FUNDING_MAX_LONG"] = "0"
        try:
            self.assertEqual(
                Settings.from_env().strategy_params["Donchian_Trend"]["funding_max_long"], 0.0)
        finally:
            del os.environ["DONCHIAN_FUNDING_MAX_LONG"]

    def test_gate_blocks_crowded_long(self):
        """Breakout long while longs pay 0.02%/8h -> skipped."""
        closes = [100.0] * 60 + [130.0]
        sig = self._strat().propose(_ctx(closes, funding_rate=0.0002))
        self.assertIsNone(sig)

    def test_gate_passes_long_at_the_boundary(self):
        """rate == threshold is allowed (block is strictly greater)."""
        closes = [100.0] * 60 + [130.0]
        sig = self._strat().propose(_ctx(closes, funding_rate=0.0001))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "buy")

    def test_gate_blocks_crowded_short(self):
        """Breakdown short while shorts pay 0.02%/8h -> skipped."""
        closes = [100.0] * 60 + [70.0]
        sig = self._strat().propose(_ctx(closes, funding_rate=-0.0002))
        self.assertIsNone(sig)

    def test_gate_passes_short_when_funding_normal(self):
        closes = [100.0] * 60 + [70.0]
        sig = self._strat().propose(_ctx(closes, funding_rate=0.00005))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "sell")

    def test_none_rate_leaves_gate_inert(self):
        """A funding data outage can never block trades."""
        closes = [100.0] * 60 + [130.0]
        sig = self._strat().propose(_ctx(closes, funding_rate=None))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "buy")

    def test_zero_params_disable_the_gate(self):
        closes = [100.0] * 60 + [130.0]
        sig = self._strat(funding_max_long=0.0).propose(
            _ctx(closes, funding_rate=0.005))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "buy")

    def test_zero_short_param_disables_short_gate(self):
        closes = [100.0] * 60 + [70.0]
        sig = self._strat(funding_min_short=0.0).propose(
            _ctx(closes, funding_rate=-0.005))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.side, "sell")


class TestFundingFeed(unittest.TestCase):

    def test_okx_inst_id_mapping(self):
        from app.data.funding import okx_inst_id
        self.assertEqual(okx_inst_id("ETHUSD"), "ETH-USDT-SWAP")
        self.assertEqual(okx_inst_id("BTCUSD"), "BTC-USDT-SWAP")
        self.assertIsNone(okx_inst_id("ETH"))

    def test_get_caches_within_ttl(self):
        from app.data.funding import FundingFeed
        calls = []

        async def fake(sym):
            calls.append(sym)
            return 0.0001

        async def run():
            f = FundingFeed(ttl_sec=60, fetcher=fake)
            a = await f.get("ETHUSD")
            b = await f.get("ETHUSD")
            return a, b, f

        a, b, feed = asyncio.run(run())
        self.assertEqual(a, 0.0001)
        self.assertEqual(b, 0.0001)
        self.assertEqual(len(calls), 1)     # TTL cache hit
        self.assertEqual(feed.snapshot(), {"ETHUSD": 0.0001})

    def test_fetch_error_fails_open_stale_then_none(self):
        from app.data.funding import FundingFeed
        state = {"ok": True}

        async def flaky(sym):
            if state["ok"]:
                return 0.0002
            raise RuntimeError("api down")

        async def run():
            f = FundingFeed(ttl_sec=0.0, fetcher=flaky)  # ttl=0 -> always refetch
            good = await f.get("ETHUSD")
            state["ok"] = False
            stale = await f.get("ETHUSD")      # serves last known value
            never = await f.get("BTCUSD")      # no history -> None
            return good, stale, never

        good, stale, never = asyncio.run(run())
        self.assertEqual(good, 0.0002)
        self.assertEqual(stale, 0.0002)    # fail-open: stale beats unknown
        self.assertIsNone(never)           # never-known symbol -> None (inert)


if __name__ == "__main__":
    unittest.main()
