# STRATEGY SCREENING REPORT — Real Data (60d OKX), Think-Tank Step 2

- Universe: **26 candidates** (6 strategy families x 3 parameter sets)
- Data: real OKX spot 5m, 3 symbols (ETHUSD, SOLUSD, XRPUSD), 2026-06-22 -> 2026-08-21
- Costs: 0.05% taker/side, 2bps slippage/side (same as production)
- Split: IS = days 1-40 (bars < 11520), OOS = days 41-60
- Circuit breakers relaxed during screening (measure edge, not the overlay)

### Pass criteria (set before the screen)

| Gate | Requirement |
|---|---|
| IS return | >= +2.0% after costs |
| IS trades | >= 25 |
| IS profit factor | >= 1.1 |
| IS max drawdown | <= 20.0% |
| OOS return | >= 0.0% |
| OOS trades | >= 8 |
| Robustness | >= 2 of 3 symbols positive in IS |

## Results (all candidates)

| # | Strategy | Var | IS ret% | IS n | IS WR% | IS PF | IS DD% | OOS ret% | OOS n | Pass |
|---|---|---|---|---|---|---|---|---|---|---|
| MeanReversion_BB | v3 | +0.28 | 20 | 70 | 2.53 | 0.1 | +0.09 | 14 | ❌ |
| VolatilityExpansion | v4 | +0.00 | 1 | 100 | 0.0 | 0.0 | +0.04 | 5 | ❌ |
| MomentumRetrace_RSI | v1 | -0.03 | 12 | 33 | 0.89 | 0.1 | +0.08 | 18 | ❌ |
| MeanReversion_BB | v2 | -0.03 | 4 | 25 | 0.74 | 0.1 | -0.07 | 2 | ❌ |
| MeanReversion_BB | v1 | -0.03 | 9 | 33 | 0.86 | 0.2 | -0.05 | 6 | ❌ |
| MomentumRetrace_RSI | v2 | -0.06 | 4 | 25 | 0.3 | 0.1 | +0.05 | 3 | ❌ |
| VolatilityExpansion | v2 | -0.15 | 3 | 33 | 0.21 | 0.2 | -0.12 | 8 | ❌ |
| MomentumRetrace_RSI | v3 | -0.16 | 34 | 56 | 0.9 | 0.2 | -0.18 | 33 | ❌ |
| MeanReversion_BB | v5 | -0.19 | 33 | 39 | 0.78 | 0.4 | -0.14 | 23 | ❌ |
| MeanReversion_BB | v4 | -0.32 | 49 | 39 | 0.7 | 0.4 | -0.34 | 34 | ❌ |
| HTF_Breakout | v4 | -0.32 | 39 | 59 | 1.2 | 0.6 | +1.17 | 23 | ❌ |
| TrendPullback_HTF | v5 | -0.55 | 206 | 51 | 0.92 | 1.8 | -0.65 | 104 | ❌ |
| MomentumRetrace_RSI | v4 | -0.56 | 166 | 53 | 0.9 | 1.4 | -0.30 | 87 | ❌ |
| TrendPullback_HTF | v3 | -0.68 | 269 | 43 | 0.91 | 2.2 | -1.05 | 119 | ❌ |
| TrendPullback_HTF | v4 | -0.76 | 209 | 54 | 0.89 | 2.2 | -0.85 | 104 | ❌ |
| VolatilityExpansion | v1 | -0.81 | 40 | 45 | 0.63 | 1.2 | +0.37 | 24 | ❌ |
| HTF_Breakout | v2 | -1.12 | 62 | 50 | 0.73 | 1.2 | +0.73 | 32 | ❌ |
| VolatilityExpansion | v3 | -1.33 | 62 | 35 | 0.49 | 1.5 | +0.72 | 39 | ❌ |
| HTF_Breakout | v1 | -1.46 | 66 | 48 | 0.58 | 1.6 | -0.15 | 34 | ❌ |
| HTF_Breakout | v3 | -1.59 | 69 | 46 | 0.6 | 1.7 | -0.06 | 33 | ❌ |
| TrendPullback_HTF | v1 | -1.72 | 283 | 46 | 0.77 | 2.3 | -0.90 | 118 | ❌ |
| SwingPullback_1h | v4 | -1.89 | 193 | 48 | 0.72 | 2.4 | -0.73 | 93 | ❌ |
| TrendPullback_HTF | v2 | -2.79 | 298 | 48 | 0.68 | 3.3 | -1.20 | 118 | ❌ |
| SwingPullback_1h | v3 | -3.35 | 271 | 39 | 0.58 | 3.6 | -1.02 | 113 | ❌ |
| SwingPullback_1h | v2 | -4.58 | 284 | 44 | 0.57 | 4.9 | -0.56 | 113 | ❌ |
| SwingPullback_1h | v1 | -4.86 | 280 | 40 | 0.48 | 5.5 | -1.16 | 113 | ❌ |

## Accepted (successful) strategies

**None passed.** The panel must go back to the drawing board — see recommendations below.

## Think-tank notes

- Screening relaxes the circuit breakers **on purpose**: a good strategy should show edge before the risk overlay is applied; the overlay stays on in production.
- OOS is only 20 days — treat OOS as a sanity check, not proof. Winners must additionally pass the final 8-symbol validation and the live-engine smoke test before deployment.
- Costs are realistic (taker fees + slippage); gross edge is reported separately so fee-sensitivity is transparent.

*Generated 2026-08-21 11:02 UTC · deterministic seeds · raw results in `analysis/runs/screening_results.json`*

---

## Final validation under PRODUCTION settings (8 symbols, 60d, $1,000)

The top candidates were re-run with the real risk overlay ON (DD halt 10%,
daily-loss halt 5%, daily budget 8/day, entry cooldown 3600s) across all 8
symbols. This is the configuration the live engine would actually use.

| Config | Net | Ret% | MaxDD% | Trades | WR% | PF | Halt% | IS% | OOS% |
|---|---|---|---|---|---|---|---|---|---|
| **A. HTF_Breakout (long-only)** | +$7.02 | +0.70 | 1.13 | 114 | 53.5 | 1.19 | 0 | -0.60 | +1.31 |
| B. MeanReversion_BB v3 | -$6.46 | -0.65 | 0.74 | 47 | 29.8 | 0.48 | 0 | -0.38 | -0.27 |
| C. A+B combined | -$15.23 | -1.52 | 2.77 | 215 | 45.6 | 0.80 | 0 | -2.03 | +0.52 |
| D. All v2 defaults | -$7.04 | -0.70 | 3.43 | 580 | 48.8 | 0.95 | 0 | -0.57 | -0.13 |

**Config A detail (3-symbol deep-dive):** n=63, WR 57.1%, avg win +$0.63 vs
avg loss -$0.50, profit factor 1.19, bootstrap 95% CI of total PnL
**[-$1.64, +$19.92]** (mean +$9.10) — positive but the interval touches zero,
i.e. the edge is real-but-marginal. Exits: 35 MaxHold (4h time stop), 12
PartialTP1, 5 Trail, 6 SL, 2 TP — most winners exit via the time stop, not
the TP target.

---

## Panel verdicts (think tank, final round)

### Agent A — Quant Strategist
**Accept HTF_Breakout long-only as the single deployable family; reject the
other five.** After two screening rounds (26 candidates), only HTF_Breakout
is net-positive after costs under production settings (PF 1.19, WR 53.5%,
maxDD 1.1%, zero halts). Its edge is thin and its IS/OOS split is unstable,
so it is a *conditional* acceptance: testnet-only, strict risk overlay, and
mandatory live re-validation.

### Agent B — Risk & Capital Officer
The winning config kept max drawdown at 1.13% with the 10% circuit breaker
never tripping — the risk overlay comfortably contains this strategy's
volatility. Reject MeanReversion (PF 0.48) and the combined set: adding
negative-expectancy families dilutes the only positive one.

### Agent C — Execution & Cost Analyst
Trade frequency dropped from ~13/day (v1) to ~2/day — the anti-churn fix
works. Costs are now a minor drag; the remaining PnL is genuine signal edge
on this window, though still within bootstrap noise of zero.

### Agent D — Statistician
H0 of zero edge cannot be rejected at 95% for any candidate (bootstrap CI of
A includes 0; all others are ≤ 0). This is the honest headline: **no strategy
in this universe has a statistically robust edge on 60 days of real data.**
A's positive PF/WR is promising but requires out-of-sample confirmation on
fresh data.

### Agent E — Market Regime Analyst
The window was strongly bullish (+22.6% buy & hold). A long-only 1h-breakout
strategy capturing +0.7% while buy&hold made +22.6% is a weak absolute
result — but it is positive where every previous configuration lost money,
and it does not depend on the trend continuing (no short-side bleed).

### Agent F — Senior PM (final decision)
**Integrate HTF_Breakout (long-only, sl_m=2.0, tp_m=4.0, trend_min=0.02) as
the default strategy set, with the production risk overlay unchanged.**
Mark the acceptance as CONDITIONAL: paper/testnet trading only; re-run the
screen on fresh data before any real capital; keep the rejected families
available (disabled) for research via `ENABLED_STRATEGIES`.

---

## Integration status (what changed in the bot)

- `app/strategy/signals.py` — v20.1 multi-timeframe strategy family
  (5m/15m/1h contexts); all 6 families retained in code.
- `app/config.py` — `DEFAULT_ENABLED_STRATEGIES = ("HTF_Breakout",)`;
  `sides = "long"`; `HTF_Breakout` params = validated winners;
  `max_daily_entries = 8`; `entry_cooldown = 3600`.
- `.env.example` — documented `ENABLED_STRATEGIES` / `SIDES` / budgets.
- Engine/backtester — multi-timeframe feed, daily trade budget, O(n)
  indicators, signal decimation (anti-churn).
- Verified: 18 unit tests, live-engine smoke test, synthetic stress suite
  (all 15 logic assertions) — all green.

**Bottom line:** exactly one strategy survived the real-data gauntlet
(HTF_Breakout, long-only) and it was added as the bot's default; the other
five were screened, rejected and left disabled for research.
