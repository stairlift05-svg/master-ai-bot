# AUDIT v20.5 — Full-Bot Review (2026-08-27)

Scope: every module under `app/`, the analysis pipeline, the tests, and the
Render deployment config. Goal, as stated: *"previous versions lost money
consistently; the current version does not trade at all."*

Both symptoms are explained below. They have the **same root cause**, and it
is not a bug in the plumbing — the plumbing is genuinely good.

---

## TL;DR

| # | Finding | Severity | Status |
|---|---|---|---|
| 1 | No strategy has a positive post-cost edge — the shipped default was the *worst* measured config | 🔴 Critical | Defaults corrected + real trading gated behind `PAPER_MODE` |
| 2 | v20.4 config comment cites a "validated PF 1.19" that contradicts the repo's own report | 🔴 Critical | Corrected with evidence in `config.py` |
| 3 | No cost gate: signals were taken whose target could not pay fees + slippage | 🔴 Critical | `MIN_EDGE_RATIO` cost gate added |
| 4 | 4h time stop closed 55/85 trades before targets could be hit (pure fee burn) | 🟠 High | `MAX_HOLD_SECONDS` 4h → 48h |
| 5 | Partial TP halved every winner but never a loser (asymmetric, negative EV) | 🟠 High | `PARTIAL_TP` default off |
| 6 | `run_logic_assertions` raised `TypeError` — **`simulate.py` was 100% broken** | 🟠 High | Fixed |
| 7 | Backtester slippage hardcoded, decoupled from the live cost model | 🟡 Medium | Driven by `SLIPPAGE_PCT` |
| 8 | Dashboard could not distinguish paper from live-money sessions | 🟡 Medium | Mode badge + `/api/status` field |

Tests: **35 → 44**, all passing. `simulate.py` runs end-to-end again.

---

## 1. Why it lost money, and why it then stopped trading

These are two ends of one problem.

The think-tank's own conclusion in `THINK_TANK_REPORT.md` is correct and worth
repeating, because every later version drifted away from it:

> Raw signal edge (pre-friction): **+$15.03** — wiped out by slippage
> (−$50.94) + fees (−$63.67).

The signals are roughly a coin flip. Friction is not. So:

- **Old versions lost money** because they traded a lot (802 entries in 60
  days), and each trade paid ~$0.14 of friction to capture ~$0.02 of edge.
- **v20.4 stopped trading** because the response to that was to tighten
  filters until only one family, on one side, survived — and then the funding
  gate, the 4h time stop and the entry cooldowns squeezed the remainder to
  near zero. Silence was not a fix; it was the same problem with the volume
  turned down.

Tightening entry filters cannot fix a cost problem. Only two things can:
raise the profit per trade above friction, or stop trading. v20.5 does the
first where possible and enforces the second by default.

### The critical finding

`config.py` (v20.4) shipped this claim:

> the think-tank's own 60-day real-data screen rejected every family except
> HTF_Breakout (production config A: long-only, PF 1.19, maxDD 1.13%)

**This is not what the report says.** `STRATEGY_SCREENING_REPORT.md`, in this
repo, states verbatim:

> ## Accepted (successful) strategies
> **None passed.** The panel must go back to the drawing board.

Every HTF_Breakout variant in that table has a **negative** in-sample return
(−0.32%, −1.12%, −1.46%, −1.59%) and a ❌ verdict. There is no PF 1.19
anywhere in the data.

I re-ran the question independently on 5 hold-out seeds that were never used
for tuning (45 days, 4 symbols, $1,000, live cost model):

| Config | Avg return | Seeds positive | Max DD |
|---|---|---|---|
| **HTF_Breakout / long** (v20.4 default) | **−1.25%** | **0 / 5** | 2.51% |
| VolatilityExpansion / long | −0.17% | 2 / 5 | 1.03% |
| MeanReversion_BB / long | −0.09% | 3 / 5 | 0.83% |

The shipped default was the single worst configuration measured — last of
every candidate tested. That is why the bot lost money whenever it did trade.

**Honest bottom line: none of these is profitable.** The best is "loses
slowly". I changed the default to the two least-bad, lowest-drawdown families,
but the responsible conclusion is that this bot is not ready for real money,
which is why `PAPER_MODE` now exists and defaults to on.

---

## 2. Changes made

### 2.1 Cost gate (the actual root-cause fix)

`app/strategy/signals.py` — every signal now passes through a friction check
before it can exist:

```
round_trip = (2 * taker_fee + 2 * slippage) * fee_buffer
required   = round_trip * MIN_EDGE_RATIO        # default 3x
```

- Target below `required` but within reach → widened to `required`.
- Target so small it would need a >2x stretch → **signal discarded**.

This is inherited by all six families via the new `BaseStrategyV2.propose()`,
so no strategy can bypass it. Gate is tunable (`MIN_EDGE_RATIO=0` disables).

### 2.2 Exit policy

Measured on the strategy's own trades, the old exits destroyed the winners:

| Exit reason | Count | PnL |
|---|---|---|
| MaxHold (4h) | 55 | −$0.15 |
| SL | 14 | −$14.53 |
| PartialTP1 | 9 | +$4.97 |
| TP | 5 | +$3.52 |

65% of trades died on the 4h clock at roughly zero gross PnL — paying full
friction for nothing — while only 5 reached a target anchored to the *1h* ATR.
A 1h-ATR target with a 4h deadline is internally inconsistent. Relaxing the
time stop to 48h and disabling partial TP took the same strategy from
PF 0.81 to PF 0.95 on identical data.

### 2.3 `PAPER_MODE` (new safety gate, default **on**)

Full pipeline — scans, sizing, risk gates, watchdog, dashboard, Telegram —
runs unchanged, but orders never leave the process. Fills are simulated at the
live price plus modelled slippage, so paper PnL is charged the same friction as
the backtest. Implemented at `OrderExecutor._submit()`, the single chokepoint
where orders reach the exchange, so open/close/partial are all covered.
The sync loop skips exchange reconciliation in paper mode (it would otherwise
delete the simulated book).

This lets you run the bot on Render continuously and gather real evidence
about whether it makes money — at zero financial risk.

### 2.4 Bug fixes

- **`simulate.py` was completely broken.** `run_logic_assertions()` called
  `RiskManager.funding_blocked()` as a static method → `TypeError` on every
  run. Fixed, and now covered by a test. It also now passes an explicit
  threshold, so the check no longer silently depends on `FUNDING_MAX_PCT=0`.
- Backtester slippage defaults to `settings.slippage_pct` instead of a
  hardcoded 2bps, so research can never be optimistic vs. the live cost model.
- Dashboard shows a **📝 PAPER** / **💵 LIVE MONEY** badge; `/api/status`
  exposes `paper_mode` and `enabled_strategies`.

---

## 3. What I verified as sound

Credit where due — the engineering is genuinely solid, and I found no defects in:

- **Risk layer** — sizing caps, min-qty overflow refusal, aggregate notional,
  DD/daily halts, adaptive risk. The `$40k notional` and `qty × wrong price`
  bugs were already fixed correctly.
- **v20.4 incident fixes** — phantom-PnL close loop, close verification,
  stuck-position policy, HTTP-200 error envelopes, kline pagination. These are
  well-designed and well-tested; I left them alone.
- **State/concurrency** — `RLock` on all mutations, clean snapshot boundary.
- **Security** — validation gate rejects non-finite values and
  notional/qty·price mismatch; secret redaction in logging.

---

## 4. Verification

```
tests/run_tests.py        44 tests, all pass  (was 35)
tests/smoke_live_engine.py PASSED — full engine boots, loops run, paper mode active
simulate.py                runs end-to-end (was crashing)
```

New tests cover: cost-gate rejection / widening / pass-through / disabled,
paper mode isolation (no exchange calls, slippage charged, book cleared, live
mode unaffected), the stress-harness regression, and the corrected defaults.

---

## 5. Recommended next steps

1. **Deploy as-is and let it paper-trade for 2–4 weeks.** This is the only way
   to get trustworthy evidence. Watch `/api/status` and the decision log.
2. **Only if paper PnL is positive over 50+ trades**, set `PAPER_MODE=false`
   with a small `MAX_NOTIONAL_USD`.
3. **The real unlock is not parameter tuning.** All six families are
   variations of "trend/breakout/mean-revert on EMA+RSI+ATR" and all lose to
   costs. Overfitting them further will not help. Worth exploring instead:
   maker/limit entries to cut the taker fee (the largest single cost line),
   higher timeframes so profit per trade dwarfs friction, or an entirely
   different signal source.
4. **Fix the analysis pipeline's data dependency.** `analysis/data/` is
   gitignored and the sandbox has no exchange network access, so the 60-day
   real-data screen cannot currently be reproduced; I used the repo's cached
   `runs/real_60d.json` plus fresh synthetic hold-out seeds. Committing a
   compressed copy of the OHLCV data, or a small fixture, would make the
   headline numbers auditable.

---

## Note on the credentials you shared

The Render API key you posted (`rnd_…`) is unusable from this sandbox — all
outbound network access except PyPI and GitHub is blocked, so I could not
reach `api.render.com`, the AriaX host, or any exchange. Everything above was
done from the code and the repo's cached data.

**Please rotate that key anyway.** It was shared in plain text in a chat
transcript, and revoking it costs you nothing since it was never used here.
No credentials were written to the repository.
