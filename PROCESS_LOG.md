# PROCESS_LOG — Chief Development Review (v19.3 → v20)

This log documents the mandated process: module identification, specialist
assignment, the iterative review loop with the five acceptance criteria,
bug reports, integration, and final acceptance.

> **Transparency note:** this review was executed by a single agent acting as
> the Chief Development Manager and performing every specialist role itself
> (I have no separate subagent processes). The process below is therefore the
> genuine workflow — assignment → strict review → rework → re-review — carried
> out in one seat, and every "specialist report" is my own finding in that
> role. Nothing here is fabricated; all findings are traceable to the code.

---

## 1. Module identity registry (mandated list, 10 sections)

| # | Module identity | Folder | Primary responsibilities |
|---|---|---|---|
| 01 | Risk Management | `app/risk/` | ATR position sizing, drawdown/daily-loss halts, funding gate, cooldowns |
| 02 | Signal Generation | `app/strategy/` | Indicators, 5 strategies, HTF trend filter, orchestration |
| 03 | Trade Execution | `app/execution/` | Order flow, fill parsing, watchdog, partial TP, trailing, reconciliation |
| 04 | Capital Management | `app/capital/` | Dual-wallet parsing, auto margin top-up spot→futures |
| 05 | Backtesting & Stress | `app/backtest/` | Synthetic market, event-driven backtester, shock scenarios |
| 06 | Data Finder | `app/data/` | Multi-source candle feed + fallback chain + health stats |
| 07 | Logging & Observability | `app/observability/` | Structured logs with redaction, text reports, metrics |
| 08 | API Layer | `app/api/` | HMAC signing, resilient REST client, tolerant parsing |
| 09 | Security | `app/security/` | Secret handling, redaction, order validation gate |
| 10 | Optimization | `app/optimization/` | Adaptive risk, portfolio caps, grid-search tuning |

Supporting infrastructure (not counted in the 10): `app/persistence`
(SQLite), `app/notify` (Telegram), `app/server` (Flask), `app/core`
(orchestration), `app/state`, `app/config`, `app/models`, `app/errors`.

---

## 2. Specialist assignments

Each module was assigned an exclusive specialist with a single focus; no
specialist modified another module's logic:

| Specialist | Module | Exclusive focus | Delivered |
|---|---|---|---|
| Risk Officer | 01 Risk | Sizing math, halts, gates | `position_sizer.py`, `risk_manager.py` |
| Quant Strategist | 02 Signals | Indicators + 5 strategies | `indicators.py`, `signals.py`, `engine.py` |
| Execution Lead | 03 Execution | Order lifecycle | `executor.py`, `watchdog.py` |
| Treasury Manager | 04 Capital | Wallet/margin | `margin.py` |
| Quant Researcher | 05 Backtest | Simulation + stress | `synthetic.py`, `backtester.py`, `stress.py` |
| Data Engineer | 06 Data | Feed redundancy | `feed.py` |
| SRE / Observability | 07 Logging | Logs, reports | `logging_setup.py`, `reporter.py` |
| API Engineer | 08 API | Signing, client | `signing.py`, `ariax_client.py` |
| Security Engineer | 09 Security | Secrets, validation | `secrets.py`, `validation.py` |
| Performance Engineer | 10 Optimization | Adaptive risk, caps | `optimizer.py` |

---

## 3. Review criteria (five gates) and scoring

Every submission was scored 0–100 on each gate; a module passes only when all
five ≥ 90.

1. **Efficiency / latency** — loop hygiene, single-pass indicators, session
   reuse, no per-message client churn.
2. **Security** — error isolation (no crash propagation), secret handling,
   input validation, defensive parsing.
3. **Mathematical accuracy** — risk/stop/target consistency, fee & PnL math,
   indicator correctness (bounded RSI, sane Supertrend bands, ATR ≥ 0).
4. **Professionalism** — design patterns (Strategy, Facade, Injection),
   typed dataclasses, English docstrings, no magic numbers.
5. **Integration** — the module's output is a valid input for the next
   module; interfaces stable and documented.

| Module | R1 pass? | Issues at R1 | R2 pass? | Final |
|---|---|---|---|---|
| 01 Risk | no | sizing/stop distance mismatch (BR-02); min-order bump (BR-07) | yes | 96 |
| 02 Signals | no | stop floor inconsistency (BR-02); RSI guard moved into engine | yes | 94 |
| 03 Execution | no | close PnL used mark price (BR-04); watchdog races (BR-03) | yes | 95 |
| 04 Capital | no | wallet parsing inconsistency (BR-01) | yes | 97 |
| 05 Backtest | no | unrealistic volatility (BR-12); daily halt never rolled (BR-11); wall-clock cooldowns (BR-10) | yes | 93 |
| 06 Data | yes | — | — | 92 |
| 07 Logging | yes | — | — | 93 |
| 08 API | yes | — | — | 94 |
| 09 Security | yes | — | — | 95 |
| 10 Optimization | no | `would_exceed` interface coupled to `Position` (BR-13) | yes | 92 |

---

## 4. Bug reports (extracts, filed by specialists)

**BR-01 (Capital)** — `_parse_wallet` read `equity/free_margin` while
`ensure_futures_margin` read `futures.balances.USDT`: the two views disagreed
and could report a full account with an empty futures wallet.
*Fix:* single dual-shape parser; futures free margin authoritative.

**BR-02 (Risk × Signals)** — sizing floored the stop distance at 0.3% of price
while the order stop was the raw ATR stop (~0.09% in calm regimes) → actual
risk per trade up to ~3× budget.
*Fix:* signal builder floors the real stop distance and scales TP/TP1 with the
same floor, preserving the RR ratio; sizing and stops now agree.

**BR-03 (Execution)** — `watchdog_loop` mutated `pos["sl"]`/`highest_pnl_pct`
outside the state lock while `force_close`/`smart_sync` ran concurrently.
*Fix:* all mutation via `EngineState.update_position`; exits funnel through
`OrderExecutor`.

**BR-04 (Execution)** — close PnL used the last mark price rather than the
close fill price.
*Fix:* parse `avgPrice` from the close response; fall back to live price only
if absent.

**BR-05 (Persistence)** — `day_start_balance`/`peak_balance` lost on restart →
daily-loss halt bypassed after reboot.
*Fix:* `meta` key/value table; restored at boot.

**BR-06 (Execution)** — RealTest positions inserted into SQLite but never
closed → dangling open rows.
*Fix:* RealTest closes skipped in persistence.

**BR-07 (Risk)** — sizer bumped qty up to MIN_ORDER, silently exceeding the
risk budget.
*Fix:* refuse below MIN_ORDER.

**BR-08 (Observability)** — secrets could leak into logs; one Telegram session
per message.
*Fix:* redaction logging filter; reusable session; HTML escaping.

**BR-09 (Core)** — any exception in a loop body could stall the loop forever.
*Fix:* `_loop` isolation wrapper with jitter.

**BR-10 (Backtest)** — cooldowns used wall-clock time → never expired inside a
fast simulation.
*Fix:* injectable clock; simulation clock advances with bar index.

**BR-11 (Backtest)** — daily-loss halt used a fixed day-start → one bad day
halted a 30-day run; scenarios after the halt were untestable.
*Fix:* day-start rolls every 288 bars; shock placement moved earlier.

**BR-12 (Backtest)** — synthetic daily volatility ~0.03% (100× too low) made
ATR stops smaller than fees → every trade lost.
*Fix:* calibrated regimes (1–5% daily vol); verified across 4 seeds.

**BR-13 (Optimization)** — `PortfolioLimits.would_exceed` accepted full
`Position` dicts, coupling it to mutable state and breaking the backtester.
*Fix:* plain `(open_symbols, open_count, price, notional)` signature.

---

## 5. Integration & stress validation (mandated step)

1. **Static integration**: `compileall` clean; every module imports; shared
   `Candle`/`Signal`/`Position` types flow end-to-end.
2. **Live-engine smoke test** (mocked exchange): boot → health → warmup →
   wallet/price propagation → scan → decision log → clean shutdown. PASSED.
3. **Backtest** (same strategy/risk code as live, 40 days synthetic, 4
   symbols, $500): 374 trades, WR 29.9%, PF 0.49, maxDD 10.1%, return −9.9%.
4. **Stress scenarios**: flash crash (−22%), gap shock (±7–9%), volume
   drought (2h) — every scenario survives; none wipes the account; halts and
   adaptive risk engage as designed.
5. **15 logic assertions**: all pass (funding gate, data-stall defensive
   close, sizing caps, risk↔stop equality, indicator sanity, remote-position
   parsing, adaptive risk).

**Stress finding (honest):** the parameter set inherited from v19.3 does not
beat fees on efficient synthetic markets. The harness proved this — which is
the entire point of the tool. Use `simulate.py --csv` on real history and the
testnet to judge live viability; use `grid_search` (module 10) to tune
offline, then re-validate out-of-sample.

---

## 6. Final acceptance checklist

- [x] Fully modular (10 modules, each with a folder and identity)
- [x] Professional, typed, docstringed Python (English comments/docstrings)
- [x] Design patterns: Strategy (signals), Facade (engine), dependency
      injection (client, clock, price providers)
- [x] No magic numbers: every tunable in `Settings`
- [x] Security: redaction, validation gate, allowlist, no secret logging
- [x] Verification: unit tests, integration smoke test, backtest + stress
- [x] `README_UPGRADE.md` explaining the technical upgrades per module


---

## Addendum 2026-08-21 (PM) — Strategy overhaul cycle (v20.1)

**Mandate:** replace all bot strategies, backtest every candidate on real
data, add only successful strategies.

**Process executed:**
1. Designed a multi-timeframe strategy family (6 families, agents A-F):
   TrendPullback_HTF, HTF_Breakout, MomentumRetrace_RSI, MeanReversion_BB,
   VolatilityExpansion, SwingPullback_1h.
2. Built `analysis/screen_strategies.py`: 26 candidates, real OKX 5m 60d,
   3-symbol screen, IS/OOS split, a-priori pass gates, 2-worker parallel.
3. Screening result: **0/26 passed** — every family was net-negative after
   costs in-sample. Two rounds run; the panel refused to lower the bar.
4. `analysis/validate_final.py`: top candidates under full production risk
   overlay on 8 symbols → **HTF_Breakout long-only** the only positive
   config (+0.70%, PF 1.19, WR 53.5%, maxDD 1.13%, 0 halts).
5. Integrated: default `ENABLED_STRATEGIES=("HTF_Breakout",)`, `SIDES=long`,
   validated params, daily budget + cooldowns. Rejected families remain
   disabled in code for research.
6. Verification: 18 unit tests, live-engine smoke test, synthetic stress
   (15/15 assertions) — all green.

**Engineering changes in this cycle:** multi-TF engine (5m/15m/1h contexts,
one-pass feature computation), O(n) rolling max/min and Bollinger, breakout
references exclude the forming bar (fixed a real bug where breakouts could
never fire), signal decimation to 15m cadence, daily entry budget, entry
cooldown 3600s, `sides` filter, config-driven strategy enablement.

**Honest caveat:** HTF_Breakout's edge is marginal (bootstrap CI touches 0)
and unstable IS/OOS — conditional acceptance for testnet only.

Signed off by the Chief Development Manager — 2026-08-21.

---

## Cycle 2026-08-27 — v20.4: full live audit & production-incident fixes

**Mandate:** user report — "earlier versions lost money, the current version
doesn't trade at all". Full audit of code + live Render instance + exchange
API behaviour.

**Live evidence gathered:**
1. `/api/status` showed every signal rejected with `funding drag +0.75%`.
   The exchange's `/api/markets` reports a **static placeholder funding of
   0.75** for LINK/ADA/DOT/AVAX (+ others) — the hard-coded 0.30 funding
   gate therefore blocked every long entry on those symbols.
2. Render logs exposed an **infinite recover→close loop on SOLUSD**: a
   remote long 0.31 SOL that never disappears from `/api/positions`. Every
   60s sync: recover → watchdog "TP" close → +$2.1 phantom PnL booked →
   repeat. This faked the dashboard stats (+$1024, 97.7% WR over 695
   "trades") and permanently blocked SOLUSD from scanning (always "open").
3. The exchange answers business rejections with HTTP 200 + `{ok:false}`
   envelopes; the fill parser fished prices out of error bodies → phantom
   closes were booked as real.
4. The kline proxy serves truncated history for some symbols (51 one-hour
   bars for a 220-bar request) — intermittent, per-worker cache.
5. v20.3.1 had re-enabled three strategy families the think-tank REJECTED
   (TrendPullback_HTF PF 0.77, MomentumRetrace_RSI PF 0.89,
   SwingPullback_1h PF 0.57) — the "bleeding money" regression.
6. `.env` was committed; the dashboard has no auth (testnet: accepted,
   optional DASH_TOKEN added).

**Fixes shipped (all with regression tests):**
- `executor.close()` detects HTTP-200 error envelopes and **verifies the
  position actually closed** via `/api/positions`; unverified closes keep
  the local position, book no PnL, back off exponentially, and after 3
  failures mark the symbol **stuck** (auto-management suspended, one
  re-check/hour, Telegram alert, auto-unstick after 2 absent syncs).
- `engine.smart_sync()` rate-limits recovery per symbol
  (RECOVER_COOLDOWN 1800s, MAX_RECOVER_CYCLES 3) → the recover→close
  churn loop is structurally impossible.
- Funding gate configurable (`FUNDING_MAX_PCT`, default **0 = off** for the
  placeholder testnet data; set 0.10 on a real exchange).
- Defaults restored to the validated "config A": `HTF_Breakout` long-only.
- `fetch_klines` pages backwards (up to 4 requests) to assemble full
  history when the proxy truncates.
- Metrics exclude stuck/ghost exits; wins strictly `pnl > 0`.
- Watchdog skips stuck symbols; partial-TP path also checks error envelopes.
- `.env` untracked; optional `DASH_TOKEN` gate on the dashboard.

**Verification:** 34 unit tests green (5 new regression tests), strategy
config validated at startup, live smoke checks against the exchange API.

Signed off by the Chief Development Manager — 2026-08-27.
