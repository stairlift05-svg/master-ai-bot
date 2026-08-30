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
