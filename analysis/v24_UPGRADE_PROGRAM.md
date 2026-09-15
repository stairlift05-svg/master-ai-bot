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


### Sprint 3 (2026-09-14) — portfolio risk control ✅ SHIPPED

Chair decision (owner delegated): the same-direction exposure cap study —
the direct lesson of the live incident — plus a Donchian parameter plateau
check (artifact `analysis/runs/v24_sprint3.json`).

**A) Same-side cap** (concurrent same-direction positions, Donchian, both
windows; baseline peak same-side was 4):

| Cap | A net | B net | Verdict |
|---|---|---|---|
| none | +160.79 | +125.66 | baseline |
| 2 | +114.24 (−29%) | +78.39 (−38%) | veto — DD gain not worth the net loss |
| 3 | +160.78 | +106.86 (−15%) | veto — B degraded |
| **4** | **+163.87** | **+141.75** | **SHIP — improves BOTH windows** |

Shipped: `MAX_SAME_SIDE=4` (config default + env knob, 0=off). It blocks a
fully one-sided book — the exact Sep 8-14 pattern (6 correlated shorts into
a bullish grind) — while leaving the trend-following cluster (≤4) intact.

**B) Parameter plateau** (one-factor sweeps around the shipped centre):
every neighbour holds the centre (entry_len 50 is +7%/+13% but below the
>10%-both-windows ship bar, and raises A's DD — watch item, not a change).
No parameter change shipped — the centre is a plateau, not a peak.

Also: MAX_POS env default aligned with the class default (8; was an
inconsistent 5 left over from v23.7). `/api/status` now exposes
`max_same_side`. Tests 120/120.


### Sprint 4 (2026-09-15) — council of trading masters ✅ (nothing shipped, by evidence)

Owner complaint: the bot makes no trades; directive: convene a top-tier
trading-masters council and upgrade the strategy.

**Diagnosis first** (live): engine healthy, scanning every ~70s, zero
halts, all decisions "no signal — 4h sideways" across all 9 symbols.
Validated Donchian frequency is 0.42-0.48 trades/day → **P(no trade in
18h) = 72%** — the flat gap was a normal draw from the strategy's own
distribution, not a defect. Live trigger-distance scan: 4 symbols (ETH,
BTC, DOGE, SOL) were sitting ON or past their 40-bar breakdown triggers
with the EMA-regime filter short-enabled — the coil was already at the
edge (entries imminent, no code change warranted).

**Three classical master techniques, two-window tested**
(artifact `analysis/runs/v24_sprint4.json`):

| Technique | A (base +160.5) | B (base +127.0) | Verdict |
|---|---|---|---|
| Turtle-style breakout retest entry | +29.3 (n=76) | +47.7 (n=96) | ✗ veto — waiting for retests misses the best trends (they never look back) |
| Volatility-regime gate (ATR pct ≥ 40/50/60%) | +133/+129/+126 | +112/+121/+139 | ✗ veto — every threshold trades away A's net for B's PF |
| Per-side split (info only) | long +68 / short +101 | long +42 / short +96 | no action — longs positive in both windows; the N-04 long gate already handles the weak tail |

**Council verdict:** the shipped Donchian configuration is already at a
local optimum on this evidence. The live lesson of Sep 8-14 cuts both
ways: the last time the bot traded *more*, it *lost* (Imba: 24 trades in
6 days = −$10.7). Patience IS the edge; the remaining upside is
structural (maker fees, disk persistence, live-sample gate), not
parameter churn.

Backlog update: the program's active phase is now the live evaluation
gate (~50 Donchian trades vs the B-window profile).


### Sprint 5 (2026-09-15) — "SMC in combination" (owner challenge) ✅ nothing qualifies

Owner: many traders profit from SMC *combined* with other tools — find that
combination and prove it profitable. External consensus on the combination
styles (LuxAlgo workflow, TradingView SMC scripts guidance, backtrex 2026):
zones as confluence not standalone entries, HTF-trend agreement, liquidity
targets, session timing, stacked setups.

Eight fusion variants, two rounds, two-window rule
(artifact `analysis/runs/v24_sprint5.json`):

| Fusion | A | B | Verdict |
|---|---|---|---|
| F1 ICT 2022 model (sweep→displacement FVG→retest) | 0 trades | 0 trades | void (EMA filter kills sweeps) |
| F1b same, no EMA filter | n=1 | n=1 | void — fires once per window |
| F2 5★ OB + EMA trend + 3R targets | −18.9 (n=90) | −5.1 (n=40) | ✗ veto |
| F3 5★ OB, killzone-only | −23.0 | +5.4 (PF 1.45) | ✗ — A negative |
| F3b killzone + 5★ + trend | +7.8 | +1.0 | combo degrades B ✗ |
| F4 Donchian × OB-confluence filter | n=1 | n=2 | structurally incompatible (breakouts leave zones behind) |
| F4b Donchian × liquidity-pool (EQH/EQL) breakouts | n=3 | n=5 | void — subset of baseline |

**Answer to the challenge:** the combination style could not be proven
profitable — on this data, with this cost model, under the repo's two-window
rule. Across 20+ SMC variants tested in v23.7/v23.8/v24-sprint5, exactly one
element ever showed a whiff of edge (killzone gating, window B only) and it
fails the other window. Claims of profitable SMC+X trading survive on
survivorship, discretionary filtering, unbacktested discretion or markets we
cannot test (volume/flow data). What WOULD reopen the case: (a) exact rules
of a specific claimed-profitable system → tested here, (b) volume-bearing
data, (c) a 4h-timeframe test — logged as backlog options.


### Sprint 6 (2026-09-15) — the masters invent: 10 new styles ✅ 3 validated, 0 shipped

Owner directive: use the council to invent or find NEW styles. Ten tested
across two rounds (artifact `analysis/runs/v24_sprint6.json`):

| Style (school) | A | B | Result |
|---|---|---|---|
| Turtle 20/10 (Dennis/Eckhardt) | −96.5 | −176.1 | ✗ fees vs frequency |
| Turtle + 1×ATR margin | **+26.5** | **+53.5** | ✓ solo-pass |
| Connors RSI-2 pullback | −173.1 | −154.2 | ✗ targets < fees |
| TTM squeeze breakout | +110.9 | −7.4 | ✗ (round 1) |
| Squeeze + ATR-expansion confirm | **+61.6** | **+21.3** | ✓ solo-pass |
| Prior-day high/low break | −11.6 | −8.3 | ✗ |
| Chandelier trail-only (LeBeau) | +88.3 | −22.9 | ✗ (round 1) |
| Chandelier 4×ATR | **+71.8** | **+55.7** | ✓ solo-pass |
| RSI divergence (strict / pure) | n=0 | n=0 / n=4 | void |

**The finding:** three new both-window-positive styles exist — the first
besides Donchian in this repo's history — but every combination with the
incumbent degrades at least one window, and the decomposition shows their
profitable entries are the SAME breakout bars Donchian already takes; their
unique entries lose money. They are replicas of one edge (breakout momentum
+ regime), not independent edges.

**Decision:** nothing ships. The three are recorded as the **validated
bench** — ranked fallbacks (1. Turtle+1ATR, 2. Chandelier-4ATR,
3. Squeeze+confirm) if Donchian_Trend ever fails its live evaluation gate.
Portfolio-diversifying edges (cross-sectional momentum, carry/funding,
volume-based) need data we do not carry — documented as reopening paths.


### Sprint 7 (2026-09-15) — the independent-portfolio review: 24 configurations, 0 shipped

Owner directive: "run the previous and new strategies independently — any
that is profitable alone or alongside ONE other should trade on its own, not
everything fused into one signal." Two independent-operation architectures,
both live-faithful (shared balance, sizing, fees, engine exits, cap=4):

- **ARCH-B waterfall** — one position per symbol, strategies tried in
  priority order (what the live engine does today with two ENABLED_STRATEGIES).
- **ARCH-A independent books** — one position per (strategy, symbol); two
  strategies may hold the same symbol simultaneously; global same-side
  cap shared. The owner's literal proposal. New harness `walk_indep`,
  engine-equivalence-verified against `_walk` (single strategy reproduces
  A +163.56 / B +143.06 exactly).

Candidates = every strategy with a dual-window solo pass on record:
Donchian_Trend (incumbent), Imba_Fib tp4=6 (A +44.2 / B +38.6 under cap —
the only other repo strategy ever dual-positive), and the sprint-6 bench:
Turtle+1ATR (exact reconstruction, bit-for-bit vs record), Chandelier-4ATR
(literal el=50/am=4/tp=30, fresh A +92.6 / B +98.3), Squeeze rng≥1.25×ATR
(representative; the exact round-2 confirm rule was lost with /tmp —
caveated). Vetoed-on-record and NOT re-run: EmaCross_Trend, six legacy
families, all SMC variants.

| Pair | ARCH-B A / B | ARCH-A A / B | beats both? |
|---|---|---|---|
| D+I Imba_Fib | 81.2 / 74.9 | 68.3 / 89.4 | ✗ ✗ |
| D+T Turtle | 49.4 / 122.3 | 116.7 / 110.3 | ✗ ✗ |
| D+C Chandelier | 94.7 / 97.3 | 145.8 / 125.7 | ✗ ✗ |
| D+S Squeeze | 83.2 / 139.1 | 107.9 / 117.0 | ✗ ✗ |
| I+T / I+C / I+S | all ✗ | all ✗ | ✗ |
| T+C / T+S / C+S | all ✗ | all ✗ | ✗ |

Plus 4 flipped-priority waterfalls (partner first): all ✗ — Turtle-priority
starves Donchian completely (result = Turtle solo). **0 of 24 portfolio
configurations beat Donchian solo (A +163.56 / B +143.06) on both windows.**

**The mechanism (decomposition of partner trades vs Donchian activity):**
every partner's profitable trades are the ones where Donchian is ALREADY
holding the symbol (same-bar or d-held). The partners' genuinely independent
("free") trades — no Donchian position on that symbol — are net-negative or
window-inconsistent:

| Partner "free" trades | Window A | Window B |
|---|---|---|
| Imba_Fib | +1.26 (n=224) | **−65.99 (n=252)** |
| Turtle+1ATR | −71.09 | −9.89 |
| Chandelier-4ATR | +128.75 | −22.28 |
| Squeeze rep | −31.75 | +21.30 |

Even Imba_Fib — a different edge family (fib pullback, not breakout) — earns
its B-window profit inside Donchian-held periods (+42.77 d-held) and loses
−$66 on its independent trades. There is one edge in this data; the other
strategies are slow mirrors of it.

**Decision:** no second strategy activated. Single-strategy Donchian_Trend
stays. Independent/portfolio operation is now a **tested** dead end, not an
assumption — reopening requires a strategy whose free trades are net-positive
on BOTH windows (i.e., a genuinely different edge or data source: volume,
funding, cross-sectional). Artifact: `analysis/runs/v24_sprint7.json`.


### Sprint 8 (2026-09-15) — the reopening path executed: NEW DATA (volume + funding) ✅ 1 gate shipped

Owner directive: "apply the best and most successful option, zero to
hundred." With strategy-space exhausted (sprints 5-7), the committee's
documented best option was the reopening path: **different data**.

New data: the **volume** column already in both window CSVs (first use in
repo history) and **funding rates** — Binance USDT-M 8h events
2024-03-01 → 2026-08-31 (2,742/symbol) fetched from the public
data.binance.vision archive (Binance API itself is geo-blocked; OKX's own
history only reaches ~2 months back — the Binance series is the documented
proxy for window B, funding being arb-aligned across venues). Committed to
`analysis/data_funding/`.

Round 1 (artifact `analysis/runs/v24_sprint8.json`):

| Test | A | B | Result |
|---|---|---|---|
| V1 vol>med100 gate | 163.56 | 143.06 | ✗ identical — **tautological** |
| V4 vol<med100 ("quiet breakout") | 0 trades | 0 trades | ✗ no such bar exists |
| FR2 skip crowded p90/10 | 155.26 | 160.59 | ✗ hurts A |
| SV1 VolShock 3× (solo) | +183.13 | +28.81 | free trades B −$51 ✗ mirror |
| SF1 FundTrap contrarian | −45.69 | −21.63 | ✗ loses outright |

**The finding:** volume cannot gate this edge — every breakout bar already
carries above-median volume (the breakout IS the volume event). Funding
can. Round 2 found two both-window passes and a robustness grid confirmed
a genuine plateau, not a peak:

| Gate | A | B | vs baseline 163.56 / 143.06 |
|---|---|---|---|
| FR2b rolling p95 (p92-95 all pass) | +180.2 | +147.0 | ✓ (needs 90d history at runtime) |
| **FR3b absolute ±0.01%/8h (0.010-0.020 all pass)** | **+188.3 PF 2.05** | **+147.1** | ✓ **shipped** |

Standalone funding/volume strategies all failed the sprint-7 free-trade
rule — there is no second independent edge in this data either; but funding
works as a **gate on the incumbent**.

**Shipped (full end-to-end):** the FR3b gate — skip a Donchian breakout
entry when our side is the crowded, paying side of the funding market
(long blocked if funding > +0.01%/8h, short mirrored; the threshold is the
exchange-standard default rate). Stateless (current rate only —
restart-proof), fail-open (a data outage never blocks trades), env
kill-switch (`DONCHIAN_FUNDING_MAX_LONG/MIN_SHORT`, 0 = off). Wiring:
`app/data/funding.py` FundingFeed (OKX public, 30min TTL) →
`HtfContext.funding_rate` → `DonchianTrend.evaluate` gate (blocks logged)
→ `/api/status` exposes `funding_rates`. 132/132 tests green. Gold parity
check: the real class through the live code path reproduces the validated
improvement (gate off A +163.9/B +141.8 → gate on **A +188.6 PF 2.06 /
B +145.8**).

Expected live effect vs the old behaviour: fewer crowded-side entries
(~11% of window-A signals, ~1% of window-B), higher per-trade quality.

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
