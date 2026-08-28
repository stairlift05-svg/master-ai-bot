# STRATEGY v21.0 — Donchian_Trend, fully re-validated (2026-08-28)

This document **supersedes the headline numbers** in `STRATEGY_v20.6.md` and
records the outcome of the full 2026-08-28 review (see `REVIEW_2026-08-28.md`),
including a **16-month out-of-sample extension the strategy had never seen**.

---

## 1. What changed in the harness (v20.6 → v21)

The 2026-08-28 review found that the shipped backtester did not faithfully
represent the live engine. Three P0/P1 fidelity fixes were implemented,
tested (6 new unit tests, suite now 62), and are part of these numbers:

| Fix | Review ID | What was wrong | What changed |
|---|---|---|---|
| Signal cadence | F-02 | Committed default evaluated signals every 3 bars on a 1h base (= every 3 h), while the live scan loop acts on **every** 1h close → 54 trades backtested vs 114 live-equivalent | `TF_PRESETS` carries a per-base-TF `signal_every_n`: 3 for 5m, **1 for 1h** (default, live-equivalent) |
| Fill re-anchoring | F-03 | SL/TP/TP1 were anchored to the signal bar's close; the fill happens later (next bar open / current market), so realized stop distance — and risk per trade — exceeded the sizer's budget on breakouts | Both `backtester._execute_pending` and `executor.try_open` re-anchor all levels to the **actual fill price**, preserving the intended distances; the executor re-checks the cost gate at the fill |
| Fee parity | F-09 | Backtest booked `(entry+exit)·fee`; live/paper book `2·exit·fee·fee_buffer` → backtest ~17% optimistic on fees | Backtest now uses the live formula exactly |

Also: `simulate.py --csv` auto-detects (or accepts `--tf`) the base
timeframe — 1h data previously ran under the 5m preset, mis-scaling cooldowns
and daily rolls 12× (F-04); the 1h base context slots are now 4h/4h,
matching live (F-05).

**None of these changes touches the strategy logic, the risk layer, or the
default parameters** (`entry_len=40, sl_m=2.5, tp_m=20, break_atr=1.5`,
sides=both, 1h timeframe).

## 2. Data

| Window | Source | Period | Bars × symbols |
|---|---|---|---|
| **A** — validation window | Binance spot 1h (committed, `analysis/data_1h/`) | 2024-03-01 → 2025-04-30 | 10,224 × 5 |
| **B** — unseen extension | OKX spot 1h (fetched 2026-08-28, `analysis/data_1h_oos/`) | 2025-05-01 → 2026-08-28 | 11,633 × 5 |

Costs: 0.05% taker/side + 0.02% slippage/side, fee buffer 1.2 — identical to
the live cost model. Symbols: BTC, ETH, SOL, BNB, DOGE.

Regime context (per-symbol window returns):

| | A (2024-03→2025-04) | B (2025-05→2026-08, unseen) |
|---|---|---|
| BTC | +53.1% | −16.8% |
| ETH | −46.9% | +37.8% |
| SOL | +11.0% | −28.7% |
| BNB | +48.4% | +16.1% |
| DOGE | +44.1% | −50.1% |

Window B is deliberately **hard**: no single direction — three symbols fell,
two rose. A directional bias (like every v20.1 family) would have died here.

## 3. Results (v21 harness, production settings, $1,000)

### Window A — the 14 months used for the v20.6 validation

| Run | Return | Trades | WR | PF | Max DD | Long / Short PnL |
|---|---|---|---|---|---|---|
| **Full** | **+7.06%** | 114 | 32.5% | **1.41** | **3.91%** | +$4.0 / +$66.5 |
| Train (first 60%) | +4.74% | 69 | 31.9% | 1.50 | 3.71% | +$38.4 / +$8.8 |
| Test (last 40%, unseen then) | +3.92% | 42 | 38.1% | 1.59 | 2.71% | −$18.3 / +$57.5 |
| Long only | +3.59% | 68 | 29.4% | 1.34 | 4.27% | — |
| Short only | +7.28% | 56 | 37.5% | 1.93 | 3.26% | — |

### Window B — 16 months of data the strategy has NEVER seen

| Run | Return | Trades | WR | PF | Max DD | Long / Short PnL |
|---|---|---|---|---|---|---|
| **Full (unseen)** | **+6.69%** | 162 | 22.2% | **1.28** | **6.43%** | −$8.5 / +$75.1 |
| First half (2025-05→2026-01) | +4.68% | 72 | 29.2% | 1.45 | 4.83% | +$28.1 / +$18.5 |
| Second half (2026-01→2026-08) | +3.25% | 85 | 20.0% | 1.27 | 6.35% | −$27.5 / +$59.8 |
| Long only | −1.05% | 99 | 20.2% | 0.92 | 6.91% | — |
| Short only | **+10.70%** | 75 | 29.3% | **2.01** | **3.97%** | — |

**30 months combined: ≈ +13.9% on $1,000 (114+162 = 276 trades), max DD
6.43%, both windows independently positive, every 6-month sub-window
positive except none.**

## 4. Reading the evidence

1. **Out-of-sample confirmation, the honest kind.** The strategy was designed
   and validated on window A only. On the 16 months that followed (window B,
   different venue, mixed regime), it returned +6.69% with PF 1.28 and a
   6.43% drawdown. This is the strongest evidence the project has produced —
   and it is reproducible: `python analysis/validate_v21_final.py`.
2. **The win rate (22–33%) is low by design.** Trend following is paid in a
   handful of large winners; losing streaks of 5–10 trades are normal
   behaviour, not a malfunction. Do not "fix" the win rate.
3. **Symmetry is the edge.** On window A both sides are separately positive
   (long +3.59%, short +7.28%). On window B — a net-bearish mix — longs go
   slightly negative (−1.05%) while shorts carry the book (+$75.1).
   `SIDES=both` is mandatory; a long-only deployment would have lost money in
   B.
4. **Risk behaves.** Max drawdown 6.43% over 30 months with a 10% halt and
   0.4%-per-trade risk budget; the halt never needed to engage. Realized risk
   per trade now equals the budgeted risk (F-03, unit-tested).
5. **Honest limitations.**
   * 276 trades over 30 months is a decent but not large sample; window B's
     PF of 1.28 is a comfortable-but-thin edge (roughly a third above the
     cost floor).
   * OKX spot 1h proxies the AriaX testnet futures market; real slippage,
     funding and fill quality on the exchange may differ (placeholder funding
     on the testnet is why `FUNDING_MAX_PCT=0`).
   * Two venues (Binance then OKX) — a splice that could not be avoided;
     window B is self-consistent within one venue.
   * Backtests still assume fills at modelled prices. **Paper mode stays ON**
     until the live paper run tracks these numbers.

## 5. Reproduce

```bash
python analysis/validate_v21_final.py     # both windows, all splits (≈12 min)
python analysis/validate_donchian_1h.py   # window A matrix (cadence, warmup, plateau)
python analysis/fetch_1h_oos.py           # refresh window B data
python tests/run_tests.py                 # 62 tests, incl. v21 invariants
```

Raw output: `analysis/runs/v21_final_validation.json`,
`analysis/runs/donchian_v206_repro.json`.

## 6. What this means for deployment

* The strategy decision is now **evidence-based and reproducible**: keep
  Donchian_Trend, 1h, `SIDES=both`, shipped parameters — validated on
  30 months across two regimes and two venues, with an honest harness.
* `PAPER_MODE` stays **true**. Let the live paper run accumulate ≥ 50 trades
  and compare signal frequency (≈ 7–9/month per 5 symbols) and PnL against
  §3. Only then consider `PAPER_MODE=false` with a small `MAX_NOTIONAL_USD`.
* Before any live-money step, close the P1 items from the review: F-06
  (secret-in-header), F-07 (dashboard auth), F-08 (REAL TEST button), F-14
  (funding gate).
