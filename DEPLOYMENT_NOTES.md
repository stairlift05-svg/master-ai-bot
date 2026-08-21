# Deployment notes (v20)

## Entry point
- New engine: `python run.py` (Flask dashboard + asyncio engine).
- Legacy shim kept: `python bot.py` still works and delegates to the new
  engine — Render/Heroku start commands do not need to change.

## Environment
All v19.3 variables keep the same names (`ARIAX_KEY`, `ARIAX_SECRET`,
`ARIAX_BASE`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`). New optional knobs
are documented in `.env.example`. `PORT` (default 10000) is respected, so
Render's injected `PORT` variable works as before.

## What changed
- v19.3 single file -> modular package under `app/` (10 modules).
- Old bot preserved at `legacy/bot_v19.3.py`.
- New: risk-first ATR sizing, adaptive risk, auto margin top-up,
  backtest/stress harness (`simulate.py`), unit tests (`tests/`).

## Verification
- `python tests/run_tests.py`   -> unit tests
- `python tests/smoke_live_engine.py` -> full-engine smoke test (mocked API)
- `python simulate.py`          -> offline backtest + stress scenarios
