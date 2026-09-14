# Strategy v23 — IMBA ALGO final stack (owner-directed replacement)

**Status: shipped as the live strategy per explicit owner directive (2026-08-29).**
Evidence: `analysis/runs/imba_v23_validation.json`

## What shipped

`Imba_Fib` — faithful port of the supplied IMBA module: fibonacci trend
channel on `sensitivity*10` (=180) bars, entries in the top band (≥fib236)
long / bottom band (≤fib786) short, EMA200 regime + RSI guards (long <72,
short >28), stop at the far channel edge, fixed 1/2/3/4% target ladder.
The engine's BE move approximates the module's break-even-after-TP1; the
10/10/10/70 scale-out ladder is documented (the live executor supports a
single partial).

## Two-window validation (same harness as v21/v22)

| Window | Trades | WR | PF | Net | MaxDD |
|---|---|---|---|---|---|
| A (14mo Binance) | 334 | 78.7% | 1.12 | **+$32.17** | 3.18% |
| B (16mo OKX, unseen) | 319 | 70.8% | **0.98** | **−$5.23** | 6.76% |

Reference — the replaced Donchian_Trend v22 on the same windows:
A +$70.55 (PF 1.41), B +$70.64 (PF 1.30).

## Honest read

* High win-rate, thin edge: ~320 trades per window with avg +$0.096 (A) /
  −$0.016 (B) per trade — the fee load of trading ~1.1×/day dominates.
* The unseen window is **net negative after costs** (PF 0.98). By the
  repo's own shipping rule this would NOT have shipped on merit.
* It ships because the owner directed the replacement explicitly. The bot
  remains in PAPER_MODE=true — no real funds are exposed while evidence
  accumulates.
* Rollback is one env var: `ENABLED_STRATEGIES=Donchian_Trend`.

## Live checkpoints (paper)

Watch the 6h Telegram reports: trade frequency should jump to ~1/day
(vs ~0.3/day), win-rate should print high (≈7 in 10) while net PnL
meanders. Re-evaluate after ≥50 paper trades against the B-window profile.

## v23.2 tuning cycle (2026-08-30) — owner-requested, evidence-vetoed

Owner directive: improve the strategy with simple tools/combos, but ship
ONLY what validates positively on real-market backtests. Three quality
filters were implemented and tested one-factor-at-a-time on BOTH windows
(`analysis/runs/imba_v23_tuning.json`):

| Candidate | A (base +$32.17) | B (base −$5.23) | Shipped? |
|---|---|---|---|
| break_margin_atr 0.25 / 0.5 / 1.0 | +23.76 / +25.33 / **−8.00** | −6.48 / −10.87 / +3.19 | ✗ |
| min_ema_dist_atr 0.5 / 1.0 | +32.17 / +30.07 | −5.23 / −5.22 | ✗ (no effect) |
| htf_align 4h | **−7.01** | **−11.92** | ✗ |

**Decision: no change shipped** — the owner's own condition was not met by
any candidate. The only B-improving cell (margin=1.0) destroys window A,
which is the single-window overfit this repo's protocol exists to catch.
Diagnosis: IMBA's thin post-cost edge is structural (trade frequency vs
target size), not an entry-quality problem, so entry filters cannot fix it.
The three parameters stay in the code, default OFF, as research knobs.

## v23.3 proposed addition — the Pine "golden strategy" (evidence-vetoed)

Owner supplied a Pine strategy (EMA9/21 crossover on 4H + EMA200 regime +
ADX(14)>25 + fixed SL1%/TP2%) titled "PF>2". Ported faithfully as
`EmaCross_Trend` (Wilder ADX, crossover on the closed 4H bar, 4h context
slot for live/backtest parity) and tested solo AND combined with Imba_Fib
on both windows (`analysis/runs/emacross_v23_validation.json`):

| Configuration | A net (PF) | B net (PF) | B WR |
|---|---|---|---|
| EmaCross_Trend solo | **−$21.04** (0.86) | **−$58.31** (0.66) | 28.8% |
| Imba_Fib solo (reference) | +$32.17 (1.12) | −$5.23 (0.98) | 70.8% |
| Imba_Fib + EmaCross_Trend | +$30.83 (1.12) | **−$24.32** (0.92) | 66.3% |

**Decision: NOT enabled** — negative solo on both windows, and the combo is
strictly worse than Imba_Fib alone on both windows. A 1:2 bracket needs
>33.3% win-rate before costs; measured 28.8–34.5%. The Pine "PF>2" claim
does not survive multi-symbol real data with real costs (typical origin:
single symbol, hand-picked date range, TV's fill model). The strategy
remains registered (`build_strategy("EmaCross_Trend")`) with unit tests,
default OFF.

## v23.4 — entry/exit trick cycle (2026-08-30): "let winners run" SHIPPED

Diagnosis from trade-level stats: TP winners averaged +$1.27 while SL
losers averaged −$3.53 — the 180-bar fibonacci stop is ~10–15% wide, so
risk-sized positions are small and the 4% cap made the realized R:R ~0.33
(breakeven needs WR > 75%; measured 70.8%).

One-factor tests on both windows (`analysis/runs/imba_v234_tricks.json`):

| Trick | A | B (unseen) | Shipped? |
|---|---|---|---|
| **tp4 4% → 6%** | +11.63 (PF 1.04) | **+50.79 (PF 1.22)** | ✅ |
| tp4 8% | +16.97 | +16.70 | ✗ (weaker total) |
| tighter fib stop .618/.382 | +51.72 | +0.93 | ✗ (A-only) |
| sensitivity 12 / 24 | −4.16 / −11.93 | +10.8 / +11.4 | ✗ (A negative) |

Shipped change: `tp4 = 6.0` (single parameter). The improvement direction
is anti-overfit: it trades some in-sample performance for a large gain on
the unseen window. Charts: `analysis/charts_imba_eth_b.png` (strategy on
chart), `charts_imba_tp4_upgrade.png` (equity before/after).

## v23.5 — ATR-stop hypothesis tested and REJECTED (2026-08-30)

The committee's own proposal (tighten the ~10–15% fib stop to 2–3×ATR to
lift realised R:R) was tested on both windows against the shipped baseline
(tp4=6). Result (`analysis/runs/imba_v235_atr_stop.json`):

| Config | A net | B net | B WR | B maxDD |
|---|---|---|---|---|
| shipped (fib stop) | +11.63 | **+50.79** | 59.3% | 4.84% |
| sl_atr_mult 2.0 | −11.70 | −4.90 | 30.3% | 9.66% |
| sl_atr_mult 2.5 | −32.81 | −29.05 | 33.4% | 10.27% |
| sl_atr_mult 3.0 | −49.62 | −7.13 | 37.5% | 9.51% |

**Rejected on both windows.** The wide fibonacci stop is load-bearing:
IMBA enters at channel extremes where noise is maximal, so a tight stop
is systematically eaten (SL exits: 55 → 405 on A) and win rate collapses.
The knob stays in code (`sl_atr_mult`, default 0) as documented research.

## v23.7 committee review (2026-09-02) — 'ICT Validated SMC v1.8' → REJECTED

Owner submitted a 2,000-line Pine v6 ICT/SMC indicator (order blocks, FVGs,
OTE fib zones, breakers, structure BOS/CHoCH, HTF alignment, killzones,
confluence scoring ≥4/11, cooldown 10 bars) with the directive: review via
the committee protocol and add to the bot **only if profitable**.

Port: `analysis/scripts/ict_validation.py` — full signal engine (the
panel/drawing layers are irrelevant to trading), evaluated on the two-window
protocol with the live-equivalent engine (1h cadence, fees+slippage,
risk sizing, engine exits). Harness calibration: Imba tp4=6 reproduces the
recorded B-window (+53.6 vs +50.8); A runs hotter than the recorded harness
(+104 vs +11.6), so absolute numbers carry a band — every comparison below
is same-harness.

| Test | A (14mo) | B (16mo, unseen) | Verdict |
|---|---|---|---|
| ICT solo (defaults) | n=2069, WR 46.5%, PF 0.89, **−$249.5**, DD 20.2% | n=2513, WR 48.7%, PF 0.90, **−$262.5**, DD 20.5% | ✗✗ |
| ICT solo, before fees | gross ≈ −$56 | gross ≈ −$23 | no alpha at all |
| ICT strict score ≥5 / ≥6 | PF 0.84 / 0.81 | PF 0.89 / 0.80 | ✗ (worse) |
| Combo ICT-first | +$42.4 (vs Imba solo +$104) | +$57.6 (vs +$53.6) | ✗ degrades A |
| Combo Imba-first | +$26.9, ICT leg −$44.3 | +$11.0, ICT leg −$15.1 | ✗ degrades both |

**Committee verdict: REJECT — do not add.** Negative on both windows solo,
negative before fees (so it is not a fee-tuning problem), every combination
ordering leaves the portfolio below Imba-solo on at least one window, and
stricter confluence scores make it *worse*. Root cause: on liquid 1h crypto
the zone-touch conditions fire ~5-7×/day/symbol; the "validated" OB/FVG/OTE
touches are noise, not institutional footprints. Artifact:
`analysis/runs/ict_v237_validation.json`.

## v23.7 capacity expansion (2026-09-02) — SHIPPED

Second half of the same owner directive (more pairs, more trades):

* Symbols 8 → **12**: +BTCUSD (part of both validation windows), +BCHUSD,
  +LTCUSD, +TRXUSD (AriaX-listed majors, no dedicated backtest coverage —
  flagged honestly). Remaining AriaX headroom: AAVE, UNI, XLM.
* `MAX_POS` 5 → **8** and `MAX_AGG_NOTIONAL_USD` 400 → **640** (8 × $80).
  Side effect: the AVAX "max positions reached" rejection spam disappears
  (3 slots of headroom instead of 0).
* Per-trade risk unchanged (0.4%, $80 notional cap, 5x) — margin impact
  ≈ $128 worst case on a $39.9k balance.

## v23.8 follow-up review (2026-09-02) — five-star OBs + SMC کلیات

Owner pushback (fair): on the chart the indicator's five-star (★) OB signals
look completely successful. Re-tested specifically that claim — entries ONLY
at OB touches whose creation confluence score ≥7 (5★) / ≥6 (4★+), SL beyond
the OB, TP at nearest swing, cooldown, HTF gate, full cost model:

| Variant | A | B |
|---|---|---|
| 5★ OBs (score≥7) | n=449, WR 49.0%, PF 0.84, net **−$69.6** (gross −$27.5) | n=551, WR 53.4%, PF 1.03, net **−$18.4** (gross **+$34.2**) |
| 4★+ OBs (score≥6) | n=606, PF 0.86, net −$86.3 | n=717, PF 0.95, net −$55.5 |

Honest nuance: 5★ on the unseen window B is gross-positive — the closest any
ICT variant came to an edge, which matches the owner's visual impression on
recent charts. But it dies after fees and window A is negative even gross.
**Committee verdict stands: not shippable.**

Why charts look better than the backtest: (1) survivorship/hindsight — the
eye anchors on successful touches, forgets failed ones and repeat-touch
failures; (2) chart arrows carry no fees/slippage; (3) ★ measures confluence
at zone creation, not forward return; (4) historical drawings render in
their final state (repaint subtlety).

External review (کلیات SMC): SMC is largely a rebranding of Wyckoff-era
supply/demand and stop-run observations; the *observations* are often real
(liquidity clusters, imbalances refill, session timing), but no rigorous
public study or audited record shows the framework itself carries an edge
(LuxAlgo library; test-max 2026 review). Rule-based backtests of SMC entries
report WR 50-65% / PF >1.5 only on selected markets (e.g. DXY forex) with
defined rules and risk management — consistent with our finding: the raw
signal engine is noise; only a fully-specified, cost-aware variant on the
right market could work, and on 1h crypto it does not.

## v23.9 live performance review (2026-09-14) — 12 days of real trading

Owner report: "عملکرد ربات کاملا ضرر ده". Committee analysis of 24 real
closed trades (Sep 8-14 era) + 27 Telegram reports:

| Metric | Live (24 trades) | Validated profile (B, tp4=6) |
|---|---|---|
| Net | **−$10.71** realized (−$19.8 balance incl. floating) | +$0.12/trade |
| WR | **29.2%** (p=0.003 vs 59% — not chance) | 58.6% |
| PF | **0.36** | 1.22 |
| Avg loss/win | −$1.17 / +$0.79 | — |

Dollar impact tiny (0.05% of balance) thanks to $80 notional caps, but the
edge is negative in this regime. Three causes, ranked:

1. **Service slept Sep 3-8 (free tier)** with live positions open and
   engine-side stops offline — 5-day gap in Telegram reports; DB/stats wiped
   on wake (token rotated). Positions survived by luck. Fixed in v23.9:
   watchdog keep-alive ping every 10 min (pending: GitHub token expired,
   push blocked).
2. **Regime mismatch**: Sep 8-14 was a mild bullish grind (ETH +1.7%, XRP
   +2.3%, BTC +0.1%, SOL −1.0%) while the book ran ~6 shorts; shorts stopped
   out serially. The validated edge was earned shorting a bear leg.
3. **Unvalidated symbols over-represented in losses**: BTC/BCH/TRX/LTC took
   ~37% of trades but ~57% of losses (−$5.8 of −$10.1 identifiable).
   TRX feed is dead inside the bot (1h cache = 2 candles) — open position
   managed via tickers only.

No strategy change made (two-window rule; htf_align already vetoed v23.2).
Owner decisions requested: (a) fresh GitHub token, (b) ride vs pause vs
rollback (Donchian env one-liner), (c) trim the 4 new symbols?, (d) Render
paid upgrade vs 10-min ping.

## v23.9.1 owner decision (2026-09-14 20:42 UTC) — ROLLBACK TO DONCHIAN_TREND

Owner verdict (after reviewing the 24-trade live record, PF 0.36): the
losses come from the strategy, not the sleep incident. Decision menu
presented; owner selected **return to Donchian_Trend**.

Executed per the standing rollback procedure: Render env
`ENABLED_STRATEGIES=Donchian_Trend` + explicit deploy (`5aef37a`).
Verified live: enabled_strategies=['Donchian_Trend'], health ok,
2 positions recovered (TRX buy, BCH sell).

Reference — Donchian_Trend v22 two-window validation (the strategy the
bot returns to): **A +$70.55 (PF 1.41) / B +$70.64 (PF 1.30)**, vs Imba_Fib
A +$32.17 / B −$5.23 (pre-tp4-fix). Honest flags:
* Donchian's validation windows covered 5 symbols (BNB/BTC/DOGE/ETH/SOL);
  the bot now scans 12. The 4 v23.7 additions (BCH/LTC/TRX) remain
  unvalidated for Donchian too — trimming is a standing owner option.
* Imba_Fib stays registered (one env var away, research mode).
