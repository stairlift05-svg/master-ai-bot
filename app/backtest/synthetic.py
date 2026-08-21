"""Backtesting (#05): synthetic market generation.

Produces deterministic (seeded) 5-minute OHLCV series using a regime-switching
geometric Brownian motion with four regimes (calm / trending up / trending
down / volatile).  Volume is log-normal with occasional surges so the
volume-based strategies behave realistically.

The generator is used by ``simulate.py`` to run the full strategy + risk +
portfolio pipeline offline — no exchange access required.
"""
from __future__ import annotations

import math
import random
from typing import Dict, List

from app.models import Candle

MINUTES_5 = 5 * 60

# Daily volatility parameters calibrated to realistic crypto magnitudes
# (1–5% daily moves). The earlier build used ~0.03% daily vol, which made
# ATR stops smaller than fees — an unrealistic regime that failed everything.
_REGIMES = [
    ("calm", 0.000040, 0.012, 0.45),
    ("trend_up", 0.000600, 0.020, 0.20),
    ("trend_down", -0.000600, 0.020, 0.20),
    ("volatile", 0.000000, 0.048, 0.15),
]


def generate_5m(days: int, base_price: float = 1800.0, seed: int = 42,
                start_ts: int = 1700000000000,
                base_volume: float = 150.0) -> List[Candle]:
    """Generate ``days`` worth of contiguous 5m candles (288/day)."""
    rng = random.Random(seed)
    total = days * 288
    dt = MINUTES_5 / 86400.0
    sqrt_dt = math.sqrt(dt)
    candles: List[Candle] = []
    price = base_price
    regime_remaining = 0
    mu, vol = _REGIMES[0][1], _REGIMES[0][2]

    for i in range(total):
        if regime_remaining <= 0:
            _, mu, vol, _ = rng.choices(
                _REGIMES, weights=[r[3] for r in _REGIMES]
            )[0]
            regime_remaining = rng.randint(1, 2) * 288
        regime_remaining -= 1

        ret = mu * dt + vol * sqrt_dt * rng.gauss(0.0, 1.0)
        open_p = price
        close_p = max(open_p * math.exp(ret), 0.01)
        spread = vol * sqrt_dt * abs(rng.gauss(0.0, 1.0)) * 0.5
        high = max(open_p, close_p) * (1.0 + spread)
        low = min(open_p, close_p) * (1.0 - spread)

        volume = base_volume * max(0.3, rng.lognormvariate(0.0, 0.4))
        if abs(ret) > 1.5 * vol * sqrt_dt:
            volume *= 2.5  # volume surge on strong bars

        candles.append(Candle(start_ts + i * MINUTES_5, open_p, high, low,
                              close_p, volume))
        price = close_p
    return candles


def resample_1h(candles_5m: List[Candle]) -> List[Candle]:
    """Aggregate 5m candles into complete 1h candles (12 bars each)."""
    out: List[Candle] = []
    for i in range(0, len(candles_5m) - 11, 12):
        group = candles_5m[i:i + 12]
        out.append(Candle(
            ts=group[0].ts,
            o=group[0].o,
            h=max(c.h for c in group),
            l=min(c.l for c in group),
            c=group[-1].c,
            v=sum(c.v for c in group),
        ))
    return out


def build_market(symbols: List[str], days: int, seed: int,
                 base_prices: Dict[str, float]) -> Dict[str, List[Candle]]:
    """Generate one 5m series per symbol, all time-aligned."""
    market: Dict[str, List[Candle]] = {}
    for idx, sym in enumerate(symbols):
        market[sym] = generate_5m(
            days, base_price=base_prices.get(sym, 1800.0), seed=seed + idx * 7,
        )
    return market
