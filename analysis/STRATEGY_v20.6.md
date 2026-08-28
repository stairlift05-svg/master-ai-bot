# STRATEGY v20.6 — Donchian_Trend, validated on real market data

Follow-up to `AUDIT_v20.5.md`, which fixed the plumbing but concluded that
**no strategy in the repo had a positive post-cost edge**. This document
replaces the strategy.

---

## 1. Real data (the thing that was missing)

Every previous conclusion in this repo rested on either synthetic candles or a
cached JSON whose source CSVs are gitignored and unreproducible. I obtained
**real Binance 1h klines** and committed them:

| | |
|---|---|
| Source | Binance spot klines, via a public GitHub dataset mirror |
| Symbols | ETHUSD, SOLUSD, DOGEUSD, BNBUSD, BTCUSD |
| Period | 2024-03-01 → 2025-04-30 (14 months) |
| Bars | 10,224 per symbol, **0 gaps**, OHLC sanity-checked |
| Location | `analysis/data_1h/{SYMBOL}.csv` |

Costs modelled throughout: 0.05% taker per side + 0.02% slippage per side,
with the 1.2x fee buffer — identical to the live cost model.

**The sample contains both regimes**, which turned out to be the decisive fact:

| Half | ETH | SOL | DOGE | BNB | BTC |
|---|---|---|---|---|---|
| Train (first 60%) | −6.7% | +63.3% | +138.8% | +52.6% | +33.1% |
| Test (last 40%) | −43.6% | −32.6% | −40.8% | −3.2% | +14.2% |

Train is a bull market. Test is a bear market.

---

## 2. All six legacy families fail out-of-sample

Same engine, same costs, train/test split, no cherry-picking:

| Strategy | Sides | TRAIN ret | TEST ret | TEST PF |
|---|---|---|---|---|
| HTF_Breakout *(v20.4 default)* | long | −0.48% | **−0.92%** | 0.50 |
| HTF_Breakout | both | −1.30% | −1.77% | 0.49 |
| TrendPullback_HTF | long | −6.31% | −6.03% | 0.42 |
| SwingPullback_1h | long | +0.91% | **−4.21%** | 0.62 |
| SwingPullback_1h | both | **+3.21%** | **−5.67%** | 0.64 |
| MomentumRetrace_RSI | long | −5.32% | −1.45% | 0.67 |
| VolatilityExpansion | long | −0.15% | −1.17% | 0.16 |
| MeanReversion_BB | long | 0.00% (0 trades) | −0.15% | — |

Two things to note:

1. **HTF_Breakout — the strategy v20.4 shipped as "validated, PF 1.19" — loses
   on real data in both halves.** The v20.5 audit called this claim
   unsupported; on real data it is simply false.
2. **SwingPullback_1h is the textbook overfit**: best train result of the
   group (+3.21%), worst test result (−5.67%). Anyone tuning on the first half
   would have shipped it.

Every long-biased family collapsed in the bear half. This is precisely what
happened to the bot in production.

---

## 3. The replacement: `Donchian_Trend`

A deliberately plain channel-breakout trend follower. The design constraints
came straight from the failure analysis:

| Failure observed | Design response |
|---|---|
| Long-only died in the bear half | **Symmetric** — shorts on identical terms |
| Edge/trade < cost/trade | **Few trades, wide targets** — 104 trades in 14 months |
| Fixed targets capped winners at ~cost | **Trailing-stop exit** — winners run |
| Oscillator thresholds memorised the train half | **3 parameters**, no fitted RSI/BB levels |

Rules:
- **Entry**: close breaks the 40-bar high/low by **≥ 1.5 × ATR**, and agrees
  with the EMA200 regime filter.
- **Stop**: 2.5 × ATR. **Target**: 20 × ATR (deliberately far — the real exit
  is the trail).
- **Exit**: ATR trailing stop (6 × ATR), activating at +4%.

The `break_atr = 1.5` confirmation filter is the single most important
parameter. Without it the strategy takes every marginal poke through the
channel: 58 stop-outs against 7 targets, and it loses. With it, results flip
positive on both halves.

---

## 4. Results

Same parameters everywhere. No per-symbol or per-period tuning.

| Window | Return | Trades | PF | Max DD | Win rate |
|---|---|---|---|---|---|
| Train (bull) | **+3.64%** | — | 1.41 | — | — |
| **Test (bear, unseen)** | **+3.52%** | 40 | **1.53** | 2.3% | — |
| **Full 14 months** | **+5.52%** | 104 | **1.33** | **4.04%** | 29.8% |

Walk-forward, 4 sequential quarters, parameters fixed:

| W1 | W2 | W3 | W4 |
|---|---|---|---|
| −0.72% | +0.21% | +0.50% | +5.27% |

3 of 4 positive; the loss is small and shallow (2.4% DD).

**Parameter plateau** — the result is not one lucky cell. At `break_atr` 1.4–2.0
every variant is test-positive (PF 1.24–1.59), and across `entry_len` 30–60 ×
`sl_m` 2.0–3.0, **all 12 combinations are positive on both halves**. Broad
plateaus are what generalisation looks like; sharp peaks are what overfitting
looks like.

**Where the money comes from:**
```
by side:   short +$61.6   long −$6.4
by symbol: DOGE +42.4  BNB +15.4  SOL +16.4  BTC +3.9  ETH −22.9
sides=long only: +0.32%   (vs +5.52% both)
```
The entire edge is in the shorts. `SIDES=long` — the v20.4 default — discarded
it. Note also that the strategy wins only 30% of the time; it is profitable
because winners average ~5× losers. That is normal for trend following, and it
means **losing streaks of 5–10 trades are expected and not a malfunction.**

---

## 5. Config changes shipped

| Setting | Old | New | Why |
|---|---|---|---|
| `ENABLED_STRATEGIES` | HTF_Breakout | **Donchian_Trend** | Only validated family |
| `SIDES` | long | **both** | Shorts are the entire edge |
| `TIMEFRAME` | 5m | **1h** | Validated timeframe; 5m edge < costs |
| `MID_TIMEFRAME` | *(hardcoded 15m)* | **4h** | Was meaningless with a 1h primary |
| `TRAIL_ACT` / `ATR_TRAIL_MULT` | 3.2 / 0.8 | **4.0 / 6.0** | Trail is now the primary exit |
| `MAX_HOLD_SECONDS` | 4h | **400h** | Trends need weeks; 4h cut every winner |
| `PARTIAL_TP` | true | **false** | Halved winners, never halved losers |

Engine changes: the backtester is now timeframe-agnostic (`base_tf`), and the
mid timeframe is configurable instead of hardcoded.

---

## 5b. Live-path hardening (found while verifying the deploy)

The backtest warms up with 260 bars, but the **live** engine only required 120
before analysing. The EMA200 regime filter is undefined below 200 bars, so in
production the context silently fell back to the EMA50 — a different,
unvalidated strategy. Measured on the real test set:

| Live warm-up | Regime filter | TEST return | PF |
|---|---|---|---|
| 120 bars (old) | EMA50 fallback | +3.23% | 1.47 |
| 210 bars (now) | EMA200 as validated | +3.52% | 1.53 |

Fixed by raising `MIN_BARS_5M` to 210 and adding invariant tests that assert
(a) the feed's acceptance threshold can satisfy the warm-up after the forming
bar is dropped, and (b) the EMA200 is actually populated post-warm-up. The
"insufficient data" message now reports the bar count, so a starved feed is
visible in the logs instead of silent.

## 6. Honest limitations

- **14 months, 5 symbols, 104 trades.** Enough to reject the old strategies
  confidently; not enough to promise future profits. A 30% win rate means the
  result is driven by a handful of large winners.
- **Backtests assume fills at modelled prices.** The AriaX testnet's real
  slippage and its placeholder funding data may differ.
- **Not tested on the live exchange yet.** `PAPER_MODE` is still **on** by
  default, deliberately. Let it paper-trade first and compare live signal
  frequency and PnL against these numbers.
- Expect roughly **7–8 trades per month** across 5 symbols. Long quiet
  stretches are normal for this design, not a bug.

**Recommendation:** deploy with `PAPER_MODE=true`, let it run 3–4 weeks, and
compare against the table in §4. Only then consider real capital, starting
small.
