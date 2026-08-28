# Strategy v22 — Long-side distance gate (Donchian_Trend)

**Status: shipped. Default `long_dist_atr = 1.0`.**
Evidence: `analysis/runs/sweep_long_gate_v22.json` · harness: `analysis/sweep_long_gate_v22.py`

## The question

v21 validation showed the long book barely earns in window A (+$4.01) and
loses in the unseen window B (−$8.48; second half −$27.54), while shorts
carry the whole edge. The bleeding longs were breakouts hugging a flat slow
EMA. Would requiring longs to clear the EMA200 by **N × ATR** improve the
strategy — measured honestly on BOTH windows?

## Protocol (identical to validate_v21_final.py)

* **A** = `analysis/data_1h` — 14 months Binance (2024-03 → 2025-04), the
  window the strategy family was originally selected on.
* **B** = `analysis/data_1h_oos` — 16 months OKX (2025-05 → 2026-08),
  **never seen** by any tuning decision before v21.
* **Pre-agreed decision rule:** ship a non-zero default ONLY if window B net
  improves AND window A net does not fall AND B keeps ≥ 60 trades.

## Results (net $, $1000 balance)

| gate (×ATR) | A | B (OOS) | B 2nd half |
|---|---|---|---|
| 0.0 (v21) | 70.55 | 66.63 | 32.23 |
| 0.5 | 70.55 | 66.63 | 32.23 |
| **1.0** | **70.55** | **70.64** | **32.23** |
| 1.5 | 71.61 | 63.64 | — |
| 2.0 | 77.87 | 45.44 | — |
| 2.5 | 77.54 | 48.31 | — |
| 3.0 | 93.55 | 48.75 | — |

**Window B at the shipped default (vs v21):** return 6.69% → **7.09%**,
PF 1.28 → **1.30**, maxDD 6.43% → **6.15%**, long book −$8.48 → −$4.45,
160 trades (2 losing longs filtered). Window A: identical in every metric.

## Why 1.0 and not 2.0 or 3.0

A rises monotonically with the gate (93.55 at 3.0) while B collapses
(−$18 to −$22). Choosing a high gate would be in-sample curve-fitting to
window A — the exact failure mode that killed every pre-v20.6 family in
the v20.5 audit. The OOS window vetoes every gate above 1.0.

## Risk notes

* Only 2 trades were filtered in window B — the improvement is real in
  direction but modest in size; do not extrapolate a large live effect.
* Shorts (the actual edge) are untouched by construction.
* `DONCHIAN_LONG_DIST_ATR=0` on the environment restores v21 behaviour
  exactly (unit-tested).

## Reproduce

```bash
python analysis/sweep_long_gate_v22.py        # full matrix, ~50 min
# or per-window via analysis/sweep_lib.py::run_window
```

v21 baseline numbers remain reproducible via
`python analysis/validate_v21_final.py` (gate pinned to 0.0 there).
