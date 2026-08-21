"""Signal generation (#02): dependency-light technical indicators.

All functions are pure Python (no numpy/pandas) so the live engine and the
backtest harness share one implementation that is fast, deterministic and
easy to unit test.  Indicator outputs are aligned with the input length;
positions that cannot be computed yet are ``None``.
"""
from __future__ import annotations

import math
from typing import List, Optional


def sma(values: List[float], period: int) -> List[Optional[float]]:
    """Simple moving average; ``None`` until ``period`` samples exist."""
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0 or len(values) < period:
        return out
    window_sum = sum(values[:period])
    out[period - 1] = window_sum / period
    for i in range(period, len(values)):
        window_sum += values[i] - values[i - period]
        out[i] = window_sum / period
    return out


def ema(values: List[float], period: int) -> List[Optional[float]]:
    """Exponential moving average (standard ``2/(n+1)`` smoothing)."""
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0 or not values:
        return out
    alpha = 2.0 / (period + 1.0)
    current = values[0]
    out[0] = current
    for i in range(1, len(values)):
        current = values[i] * alpha + current * (1.0 - alpha)
        out[i] = current
    return out


def rsi_wilder(values: List[float], period: int = 14) -> List[Optional[float]]:
    """Wilder-smoothed Relative Strength Index.

    Returns ``None`` for the first ``period`` positions.  Values are clamped
    to [0, 100]; a flat series yields 50.0.
    """
    out: List[Optional[float]] = [None] * len(values)
    n = len(values)
    if n <= period:
        return out
    gains = [0.0] * (n - 1)
    losses = [0.0] * (n - 1)
    for i in range(1, n):
        diff = values[i] - values[i - 1]
        gains[i - 1] = max(diff, 0.0)
        losses[i - 1] = max(-diff, 0.0)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, n):
        if i > period:
            avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
        if avg_loss < 1e-12:
            out[i] = 100.0 if avg_gain > 1e-12 else 50.0
        else:
            rs = avg_gain / avg_loss
            out[i] = max(0.0, min(100.0, 100.0 - 100.0 / (1.0 + rs)))
    return out


def atr_wilder(highs: List[float], lows: List[float], closes: List[float],
               period: int = 14) -> List[Optional[float]]:
    """Wilder-smoothed Average True Range (``None`` for early bars)."""
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if n <= period:
        return out
    true_ranges: List[float] = [0.0] * (n - 1)
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        true_ranges[i - 1] = tr
    atr = sum(true_ranges[:period]) / period
    out[period] = atr
    for i in range(period + 1, n):
        atr = (atr * (period - 1) + true_ranges[i - 1]) / period
        out[i] = atr
    return out


def _sliding_extreme(values: List[float], period: int, is_max: bool):
    """O(n) sliding-window extreme using a monotonic deque (amortized O(1)/item)."""
    from collections import deque
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0 or not values:
        return out
    dq: deque = deque()
    for i, v in enumerate(values):
        while dq and ((v >= values[dq[-1]]) if is_max else (v <= values[dq[-1]])):
            dq.pop()
        dq.append(i)
        if dq[0] <= i - period:
            dq.popleft()
        if i >= period - 1:
            out[i] = values[dq[0]]
    return out


def rolling_max(values: List[float], period: int) -> List[Optional[float]]:
    return _sliding_extreme(values, period, True)


def rolling_min(values: List[float], period: int) -> List[Optional[float]]:
    return _sliding_extreme(values, period, False)


def supertrend(highs: List[float], lows: List[float], closes: List[float],
               period: int = 10, multiplier: float = 3.0):
    """Supertrend indicator.

    Returns a tuple ``(direction, upper, lower)`` of lists aligned with the
    input; ``direction`` is 1 (up) / -1 (down), ``None`` before warm-up.
    """
    n = len(closes)
    atr = atr_wilder(highs, lows, closes, period)
    direction: List[Optional[int]] = [None] * n
    upper: List[Optional[float]] = [None] * n
    lower: List[Optional[float]] = [None] * n
    if n <= period:
        return direction, upper, lower

    hl2 = [(h + l) / 2.0 for h, l in zip(highs, lows)]
    up = hl2[period] + multiplier * (atr[period] or 0.0)
    dn = hl2[period] - multiplier * (atr[period] or 0.0)
    upper[period], lower[period] = up, dn
    direction[period] = 1

    for i in range(period + 1, n):
        atr_i = atr[i] or atr[i - 1] or 0.0
        raw_up = hl2[i] + multiplier * atr_i
        raw_dn = hl2[i] - multiplier * atr_i
        if raw_up < (upper[i - 1] or raw_up) or closes[i - 1] > (upper[i - 1] or -1e18):
            upper[i] = raw_up
        else:
            upper[i] = upper[i - 1]
        if raw_dn > (lower[i - 1] or raw_dn) or closes[i - 1] < (lower[i - 1] or 1e18):
            lower[i] = raw_dn
        else:
            lower[i] = lower[i - 1]
        prev_dir = direction[i - 1] or 1
        if closes[i] > (upper[i - 1] or 1e18):
            direction[i] = 1
        elif closes[i] < (lower[i - 1] or -1e18):
            direction[i] = -1
        else:
            direction[i] = prev_dir
    return direction, upper, lower


def detect_rsi_divergence(closes: List[float], rsi_values: List[Optional[float]],
                          lookback: int = 28) -> Optional[str]:
    """Detect classic RSI price/indicator divergence.

    Returns ``"bullish"``, ``"bearish"`` or ``None``.

    Bullish: price makes a lower low while RSI makes a higher low.
    Bearish: price makes a higher high while RSI makes a lower high.
    """
    n = len(closes)
    if n < lookback + 5:
        return None
    window_c = closes[-lookback:]
    window_r = [r for r in rsi_values[-lookback:]]
    if any(r is None for r in window_r):
        return None

    def is_low(idx: int) -> bool:
        if idx < 3 or idx > len(window_c) - 4:
            return False
        c = window_c
        return (c[idx] < c[idx - 1] and c[idx] < c[idx - 2] and c[idx] < c[idx - 3]
                and c[idx] < c[idx + 1] and c[idx] < c[idx + 2])

    def is_high(idx: int) -> bool:
        if idx < 3 or idx > len(window_c) - 4:
            return False
        c = window_c
        return (c[idx] > c[idx - 1] and c[idx] > c[idx - 2] and c[idx] > c[idx - 3]
                and c[idx] > c[idx + 1] and c[idx] > c[idx + 2])

    price_lows = [(i, window_c[i]) for i in range(len(window_c)) if is_low(i)]
    price_highs = [(i, window_c[i]) for i in range(len(window_c)) if is_high(i)]
    rsi_lows = {i: window_r[i] for i, _ in price_lows}
    rsi_highs = {i: window_r[i] for i, _ in price_highs}

    if len(price_lows) >= 2:
        (i1, p1), (i2, p2) = price_lows[-2], price_lows[-1]
        r1, r2 = rsi_lows.get(i1), rsi_lows.get(i2)
        if r1 is not None and r2 is not None:
            if (i2 - i1) >= 4 and p2 < p1 * 0.998 and r2 > r1 + 1.5:
                return "bullish"
    if len(price_highs) >= 2:
        (i1, p1), (i2, p2) = price_highs[-2], price_highs[-1]
        r1, r2 = rsi_highs.get(i1), rsi_highs.get(i2)
        if r1 is not None and r2 is not None:
            if (i2 - i1) >= 4 and p2 > p1 * 1.002 and r2 < r1 - 1.5:
                return "bearish"
    return None


def quantize(value: float, step: float, decimals: int = 8) -> float:
    """Floor-quantize ``value`` to a multiple of ``step``."""
    if step <= 0:
        return max(round(value, decimals), 0.0)
    qty = (math.floor(value / step + 1e-12)) * step
    return max(round(qty, decimals), 0.0)


def bollinger(values: List[float], period: int = 20, num_std: float = 2.0):
    """Bollinger bands via running sums (O(n)).

    Returns (mid, upper, lower) lists aligned to the input; ``None`` before
    ``period`` samples exist.
    """
    n = len(values)
    mid = sma(values, period)
    upper: List[Optional[float]] = [None] * n
    lower: List[Optional[float]] = [None] * n
    if n < period:
        return mid, upper, lower
    sum_x = sum(values[:period])
    sum_x2 = sum(v * v for v in values[:period])
    for i in range(period - 1, n):
        if i > period - 1:
            add = values[i]
            drop = values[i - period]
            sum_x += add - drop
            sum_x2 += add * add - drop * drop
        mean = sum_x / period
        var = max(0.0, sum_x2 / period - mean * mean)
        sd = math.sqrt(var)
        upper[i] = mean + num_std * sd
        lower[i] = mean - num_std * sd
    return mid, upper, lower
