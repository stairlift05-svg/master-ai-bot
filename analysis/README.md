# Analysis — Real-Market Backtest & Think-Tank Review

Reproducible pipeline (everything deterministic, no credentials needed):

```bash
# 1) Fetch REAL 5m OKX history for the 8 symbols (60 days) -> analysis/data/
python analysis/fetch_real_data.py --days 60

# 2) Run the backtest once, cache raw output -> analysis/runs/real_60d.json
python analysis/run_backtest_real.py

# 3) Compute all diagnostics + render the think-tank report -> THINK_TANK_REPORT.md
python analysis/think_tank.py
```

Alternative one-shot (same code paths as step 2, writes reports/backtest_report.txt):
`python simulate.py --csv analysis/data --balance 1000 --symbols ETHUSD,SOLUSD,XRPUSD,AVAXUSD,DOTUSD,LINKUSD,ADAUSD,DOGEUSD`

## Files

| File | Purpose |
|---|---|
| `THINK_TANK_REPORT.md` | Final panel report (6 agents, evidence-based verdict, recommendations) |
| `equity_curve.svg` | Engine equity vs buy-&-hold benchmark + daily PnL bars (60d) |
| `data/{SYM}.csv` | Real OKX spot 5m OHLCV, 2026-06-22 → 2026-08-21, 17,280 bars/symbol, 0 gaps |
| `runs/real_60d.json` | Cached backtest output (trades, equity, summary) — source of every number |
| `fetch_real_data.py` | OKX downloader (Binance/Bybit are geo-blocked from this host) |
| `run_backtest_real.py` | Backtest runner → JSON cache |
| `think_tank.py` | Diagnostics + report renderer |

## Key numbers (60d, $1,000, 8 symbols, fees 0.05%/side, slippage 2bps/side)

- Net PnL **−$99.58** (−9.96%); max DD 10.07% (halted at configured 10%)
- Raw signal edge (pre-friction): **+$15.03** — wiped out by slippage (−$50.94) + fees (−$63.67)
- 802 entries, 815 realizations, win rate 32.0%, PF 0.57, break-even WR needed 45%
- z-test p ≈ 3.8e-14 · bootstrap 95% CI of total PnL [−$124.68, −$73.65]
- Buy-&-hold benchmark over the same window: **+22.6%**
- 67% of bars spent under the drawdown circuit breaker (which worked as designed)

**Verdict (all 6 agents):** engineering and risk system are sound; the 5m
signal set has an edge per trade smaller than its cost per trade. Do not
deploy as-is; fix trade frequency and stop/target width, then re-validate
out-of-sample and on the AriaX testnet.

*Reproducibility: OKX public API responses are time-stamped; the simulation
itself is fully deterministic (no RNG in the backtester).*
