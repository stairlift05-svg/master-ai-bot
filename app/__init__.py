"""
Master Quant Engine — AriaX Testnet (Professional Edition)

A modular, typed, event-driven crypto futures trading system designed to run
continuously against the AriaX testnet exchange with a redundant public
candle feed (AriaX -> Bybit -> OKX -> Binance).

Architecture (10 specialist modules + supporting infrastructure):

    01. Risk Management        app/risk/          position sizing, halts, gates
    02. Signal Generation      app/strategy/      indicators, 5 strategies, HTF filter
    03. Trade Execution        app/execution/     order flow, fills, reconciliation
    04. Capital Management     app/capital/       dual-wallet parsing, margin top-up
    05. Backtesting & Stress   app/backtest/      event-driven sim, shock scenarios
    06. Data Finder            app/data/          multi-source candle feed + fallback
    07. Logging & Observability app/observability/ structured logs, metrics, reports
    08. API Layer              app/api/           signed REST client, retries
    09. Security               app/security/      secret handling, validation, redaction
    10. Optimization           app/optimization/  adaptive risk, portfolio caps, tuning

Supporting: app/persistence (SQLite), app/notify (Telegram), app/server (Flask),
app/core (orchestration), app/state (thread-safe shared state).

Disclaimer
----------
This software is an engineering deliverable. No automated system can
*guarantee* a trading win-rate or profitability; backtests are not a promise
of live results. Use on the testnet, validate with ``simulate.py``, and only
then consider capital you can afford to lose.
"""

__version__ = "20.0"
__title__ = "Master Quant Engine (AriaX Testnet) — Professional Edition"
