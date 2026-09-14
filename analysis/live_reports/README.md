# Live Performance Archive — IMBA ALGO Engine

Owner directive (2026-09-14): every performance / non-performance report of
the bot must be archived on GitHub. This directory is that archive.

## Contents

* `telegram/` — all 27 Telegram 6h reports received from the bot
  (2026-08-28 → 2026-09-14), renamed to their generation timestamps.
  The raw files as delivered by the owner (numbered `report (NN).txt`)
  are preserved via the `_nNN` suffix.
* `gozaresh-bazbini-final.md` — the complete Persian review log
  (گزارش بازبینی نهایی): every phase from v20 through v24 with evidence,
  decisions and verdicts.

## Timeline of the live account (real money, AriaX testnet)

| Period | Mode | Outcome |
|---|---|---|
| 2026-08-28/29 | paper (pre-go-live) | 0 trades, clean infra |
| 2026-08-30 → 09-02 | LIVE, Imba_Fib | +$1.1 in 3 days (7 trades), small but positive |
| 2026-09-03 → 09-08 | **service asleep (free tier)** | no scans, no reports, no engine-side stops; positions survived by luck |
| 2026-09-08 → 09-14 | LIVE, Imba_Fib, 12 symbols | **−$10.7 realized (24 trades, WR 29.2%, PF 0.36)** — strategy regime mismatch + unvalidated symbols (57% of losses from BTC/BCH/LTC/TRX) |
| 2026-09-14 20:42 | **rollback to Donchian_Trend** (owner verdict: losses are from the strategy) | live since; keep-alive watchdog every 10 min |
| 2026-09-14 21:10 | owner closed last TRX/BCH positions via Telegram | bot flat; symbol set trimmed to 9 evidence-based symbols |

## Key evidence files (elsewhere in the repo)

* `analysis/runs/v24_sprint1.json` — per-symbol validation (ADA/LTC/TRX removed)
* `analysis/runs/v24_sprint2.json` — strategy screening (nothing beats Donchian)
* `analysis/runs/ict_v237_validation.json` — ICT/SMC rejection (v23.7/v23.8)
* `analysis/STRATEGY_v23.md` — the full strategy decision log
* `analysis/v24_UPGRADE_PROGRAM.md` — working-group charter + sprint log

## Standing lesson

The live drawdown of Sep 8-14 was 0.05% of balance — risk caps worked — but
every dollar of it was avoidable: it came from trading an unvalidated symbol
set in the wrong regime. The archive exists so that decision is never made
on vibes again.
