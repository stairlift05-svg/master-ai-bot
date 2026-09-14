# v24 — IMBA ALGO Upgrade Program (کار گروه ارتقا)

**Mandate (owner, 2026-09-14):** form a specialist working group, chaired by
the committee coordinator, to fully optimize and upgrade the bot with the goal
of durable profitability.

**Honest charter:** no process can *guarantee* profits. The group's goal is to
maximize the probability of a durable, evidence-backed positive expectancy:
every change ships only with out-of-sample evidence (two-window /
two-independent-halves rule), costs are modelled, and risk stays capped.
Anything that fails evidence is vetoed and documented — that discipline IS
the edge the group controls.

## Working group (virtual specialists, coordinated by the chair)

| Role | Responsibility |
|---|---|
| Quant Research | strategy parameters, walk-forward validation, anti-overfit protocol |
| Execution & Costs | fees/slippage, maker vs taker, fill quality |
| Risk & Portfolio | exposure caps, correlation/regime controls, drawdown governance |
| Data Engineering | feed quality (TRX bug), history depth, gap detection |
| Validation Audit | independent re-runs, harness calibration, veto discipline |
| SRE / Reliability | uptime (keep-alive), disk persistence, incident response |

## Sprint log

### Sprint 1 (2026-09-14) — symbol evidence + cost study ✅

**A) Per-symbol Donchian validation** (each of the 8 never-validated symbols,
two independent 8-month halves of fresh OKX 1h data, 11,800 bars each,
live-equivalent engine; artifact `analysis/runs/v24_sprint1.json`):

| Symbol | H1 (older) | H2 (newer) | Verdict | Action |
|---|---|---|---|---|
| XRP | +10.89 (PF 1.32) | +2.67 (PF 1.11) | PASS | keep |
| AVAX | +23.27 (PF 2.29) | +1.70 (PF 1.07) | PASS | keep |
| DOT | +14.60 (PF 1.48) | +7.36 (PF 1.31) | PASS | keep |
| LINK | +19.04 (PF 1.49) | +0.40 (PF 1.04) | PASS (marginal) | keep |
| BCH | −6.87 | +28.14 (PF 1.88) | MIXED | keep (recent half strong) |
| ADA | +27.68 | **−34.83 (PF 0.31)** | FAIL | **removed** |
| LTC | −1.60 | −1.97 | FAIL | **removed** |
| TRX | +2.31 | **−14.60 (PF 0.24)** | FAIL | removed (owner closed the last position 21:10 UTC, +$0.14) |

Core-5 reference on the same harness (existing windows): **A +160.8 (PF 1.74)
/ B +125.7 (PF 1.44)** — the live strategy's edge is intact on validated
symbols; the live bleed came from regime + unvalidated symbols.

**B) Maker/limit entry study** (core 5, both windows; fill at signal close if
next bar retraces, maker fee 0.02%): A +160.8 → +161.8, B +125.6 → +138.3
(+1 / +12.6 per 16 months; ~5 momentum-failure entries filtered out on B).
Modest for Donchian's low frequency — parked in the backlog until the AriaX
maker fee schedule is confirmed (if maker is actually 0%, the case grows to
+$4.4/+$17).

**Shipped this sprint:** SYMBOL_MAP evidence-based trim (−ADA, −LTC, −TRX —
final: 9 symbols), operator close endpoint POST /api/close/<pid> (PR #24,
token-gated, engine-loop bridge; live-verified 404 path), tests 119/119,
this charter.


### Sprint 2 (2026-09-14) — strategy screening for addition ✅

Owner directive: review other strategies that could be added. Ten candidates
through the live-equivalent two-window harness (artifact
`analysis/runs/v24_sprint2.json`; context builder mirrors the engine's
_tf_context):

| Candidate | A (net / PF) | B (net / PF) | Solo both? | Combo vs Donchian |
|---|---|---|---|---|
| TrendPullback_HTF | −336 / 0.89 | −209 / 0.95 | ✗ | — |
| **HTF_Breakout** | +82 / 1.17 | +14 / 1.07 | ✓ (weak B) | **degrades B** (leg −44) |
| MomentumRetrace_RSI | −153 / 0.82 | −79 / 0.96 | ✗ | — |
| MeanReversion_BB | −71 / 0.37 | −14 / 0.97 | ✗ | — |
| VolatilityExpansion | N/A (volume-gated; windows are price-only) | | — | — |
| SwingPullback_1h | −301 / 0.93 | −347 / 0.92 | ✗ | — |
| EmaCross_Trend | −69 / 0.93 | −32 / 1.02 | ✗ | — |
| Imba_Fib tp4=6 (ref) | +104 / 1.25 | +54 / 1.13 | ✓ | live-rejected 2026-09-14 |
| SuperTrend 3×ATR10 | −112 / 0.87 | −52 / 0.96 | ✗ | — |
| **Keltner 2×ATR** | +106 / 1.14 | +84 / 1.13 | ✓ | substitutes, not complements |

**Verdict: no addition.** Donchian_Trend (A +161 / PF 1.74, B +126 / PF 1.44)
remains strictly best on both windows. The two solo-passers fail the
combination test for opposite reasons: HTF_Breakout's overlapping entries
actively degrade the unseen window; Keltner fires so often it crowd-outs the
incumbent (combo ≡ Keltner solo, which is worse than Donchian solo).
SuperTrend — the popular choice — is firmly negative after costs.

Also shipped: `analysis/live_reports/` — full live-performance archive
(27 Telegram reports + the Persian review log), per the same owner directive.

## Backlog (priority order)

1. **S2 — Walk-forward re-validation of Donchian parameters** (entry_len,
   break_atr, tp_m) on rolling windows instead of two static windows;
   ship only robust plateaus, not peaks.
2. **S3 — Regime/exposure control**: cap concurrent same-direction exposure
   (the live incident: 6 correlated shorts into a bullish grind). Design +
   backtest a portfolio-level rule (e.g., max 3 same-side, or equity-curve
   based de-risking). NOTE: per-trade trend filters (htf_align) were vetoed
   for Imba; a portfolio cap is a different, untested lever.
3. **S4 — Disk persistence on Render** so restarts stop wiping stats/trail
   locks (needs the owner's paid disk decision).
4. **S5 — TRX feed fix or symbol removal** (1h cache = 2 candles inside the
   bot; external API is fine — internal paging/assembly issue).
5. **S6 — Maker execution prototype** (see B above).
6. **S7 — Live evaluation gate**: after ~50 live Donchian trades, compare
   against the B-window profile (WR ~27-38%, PF >1.3 expected, few trades).

## Decision rights

Chair (this committee): research, evidence-backed symbol/parameter changes,
reliability fixes, docs. Owner: strategy swaps, risk budget, paid upgrades,
go/no-go on live gates.
