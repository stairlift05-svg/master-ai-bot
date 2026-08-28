# Master Quant Engine v21 — AriaX Testnet (Professional Edition)

**موتور معاملاتی ماژولار برای صرافی آزمایشی AriaX** — بازطراحی کامل از نسخه تک‌فایلی v19.3 به یک سیستم ۱۰ ماژول حرفه‌ای با خودترمیمی، مدیریت ریسک سخت‌گیرانه و هارنس بک‌تست/استرس.

A modular crypto-futures trading engine for the **AriaX testnet**, rebuilt from the single-file v19.3 into a typed, dependency-light, self-healing system with strict risk management and an offline backtest/stress harness.

> **v21.1 (2026-08-28, cycle 2):** security hardening for go-live readiness.
> Dashboard auth is **on by default** (auto-generated token, all routes
> except `/health`); the REAL TEST button now needs an explicit
> confirmation tap before it can spend real money; close verification no
> longer accepts a position that is 45% open (55%→10%); the raw
> `X-API-Secret` header is a config flag (`ARIAX_SEND_SECRET_HEADER`) with a
> documented probe procedure. Add
> [`GO_LIVE_CHECKLIST.md`](GO_LIVE_CHECKLIST.md) — the gate the bot must
> pass before `PAPER_MODE=false`.

> **v21 (2026-08-28):** full character-by-character review + re-validation.
> The backtest harness now matches the live engine (per-bar 1h signal
> cadence, fill re-anchoring, identical fee model), and the strategy
> (Donchian_Trend, 1h, both sides) is validated on **30 months of real data
> including 16 months it had never seen**: +7.06%/PF 1.41 (validation
> window) and +6.69%/PF 1.28 (unseen extension), max DD 6.43%.
> See [`REVIEW_2026-08-28.md`](REVIEW_2026-08-28.md) and
> [`analysis/STRATEGY_v21.0.md`](analysis/STRATEGY_v21.0.md).

---

## Highlights

- **10 specialist modules** — risk, signals, execution, capital, backtest, data, observability, API, security, optimization — each in its own package (`app/<module>/`).
- **Self-healing, zero human intervention**: automatic spot→futures margin top-up, drawdown-halt auto-resume with adaptive risk scaling, ghost-position cleanup, data-stall defensive closes.
- **Redundant candle feed** — AriaX public kline → Bybit → OKX → Binance fallback chain (ccxt optional).
- **Risk-first sizing** — ATR-based stops, risk budget ≡ stop distance, per-position/aggregate notional caps, funding-drag gate, drawdown & daily-loss circuit breakers.
- **Backtest & stress harness** — event-driven simulation reusing the *same* strategy/risk code as live, plus flash-crash / gap / volume-drought scenarios and 15 logic assertions.
- **Verified** — 72 unit tests + full-engine integration smoke test (all green), Flask dashboard, Telegram control.

> ⚠️ **Honest disclaimer:** no trading system can guarantee profitability or a target win-rate. The included backtest on synthetic markets shows this parameter set does **not** beat fees on efficient markets — exactly what a good harness should reveal. Validate on real history (`simulate.py --csv`) and the testnet before risking capital.

---

## Quickstart

```bash
pip install -r requirements.txt        # ccxt recommended for the fallback feed
cp .env.example .env                   # fill ARIAX_KEY / ARIAX_SECRET / ARIAX_BASE
python run.py                          # dashboard → http://0.0.0.0:10000

# offline verification (no credentials needed):
python simulate.py --days 40 --seed 42
python simulate.py --csv ./data        # real 5m CSVs named {SYMBOL}.csv (ts,o,h,l,c,v)
python tests/run_tests.py
```

Telegram (optional): set `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`, then message the
bot for the inline menu (dashboard, positions, sync, report, pause/start, close,
real-test).

---

## Security notes

- Credentials live only in `.env` (git-ignored, **never committed**).
- If a token has ever been pasted into a chat, log, or PR — **revoke and regenerate it**.
- The engine redacts secrets from logs and validates every order locally before
  sending it to the exchange.

## Documentation

- [`README_UPGRADE.md`](README_UPGRADE.md) — full upgrade manifest (10 modules, fixes, migration guide).
- [`PROCESS_LOG.md`](PROCESS_LOG.md) — the mandated review process: module registry, specialist assignments, 13 bug reports, five-gate scoring, integration & stress validation.
- [`legacy/bot_v19.3.py`](legacy/bot_v19.3.py) — the original single-file engine for reference.

## Layout

```
run.py / simulate.py     # live entry + offline harness
app/
  risk/ strategy/ execution/ capital/ backtest/ data/
  observability/ api/ security/ optimization/
  persistence/ notify/ server/ core/ config/ state/ models/ errors/
tests/                   # unit tests + live-engine smoke test
reports/                 # latest backtest/stress outputs
```
