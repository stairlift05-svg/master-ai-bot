# Render environment cleanup (v20.5)

The public service was inspected on 2026-08-24. `/health` was reachable and
`/api/status` showed an active engine, no recent engine errors, no open
positions, and repeated entries blocked by the funding gate. Render does not
expose its private Environment dashboard publicly, so values cannot be deleted
remotely without account access. Use this file as the authoritative cleanup
list.

## Keep in Render

Only keep these credentials:

```text
ARIAX_KEY
ARIAX_SECRET
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
```

`TELEGRAM_*` may be removed when Telegram control is not required.

The Blueprint also defines this non-secret value:

```text
ARIAX_BASE=https://dryclean-app-1.onrender.com
```

Render injects `PORT`; do not create it manually.

## Remove from Render

Delete all trading, strategy, timing, data, and reporting overrides listed
below. Their validated defaults now live in `app/config.py`; leaving stale
Render values would override fixes deployed in code.

```text
ADAPTIVE_RISK
ADAPT_DD_BAND1
ADAPT_DD_BAND2
ARIAX_CONNECT_TIMEOUT
ARIAX_MAX_RETRIES
ARIAX_RECV_WINDOW
ARIAX_RETRY_BASE
ARIAX_TIMEOUT
ATR_TRAIL_MULT
AUTO_RESUME_DD_RATIO
CANDLE_LIMIT_1H
CANDLE_LIMIT_5M
CLOSE_ON_DATA_STALL
DATA_STALL_GRACE
ENABLED_STRATEGIES
ENTRY_COOLDOWN
ERROR_COOLDOWN_BASE
ERROR_COOLDOWN_MAX
FEE_BUFFER
GHOST_MISS_LIMIT
LEVERAGE
LOG_FILE
LOG_LEVEL
LOSS_STREAK_FACTOR
LOSS_STREAK_SHRINK_AT
MARGIN_RESERVE_FACTOR
MARGIN_UTIL_CAP
MAX_AGG_NOTIONAL_USD
MAX_DAILY_ENTRIES
MAX_DAILY_LOSS
MAX_DD
MAX_HOLD_SECONDS
MAX_NOTIONAL_USD
MAX_POS
MIN_FREE_MARGIN
MIN_HOLD_FOR_PARTIAL
MIN_HOLD_FOR_TRAIL
MIN_ORDER_USD
MIN_PROFIT_FOR_BE
MIN_STOP_PCT
OHLCV_PAUSE
PARTIAL_TP
POST_CLOSE_COOLDOWN
PRICE_INTERVAL
RISK_PCT
SCAN_INTERVAL
SEND_CLIENT_OID
SIDES
SYMBOL_DELAY
SYNC_INTERVAL
TAKER_FEE
TEST_SYMBOL
TEST_USD
TRAIL_ACT
TRAIL_STEP
USE_ATR_TRAIL
```

Also remove obsolete platform selectors such as `PYTHON_VERSION` if present;
`runtime.txt` pins Python 3.12.8. Do not remove Render's own automatically
injected variables.

## Canonical defaults after cleanup

Important defaults include:

```text
RISK_PCT=0.40
LEVERAGE=5
MAX_POS=5
MAX_NOTIONAL_USD=80
MAX_AGG_NOTIONAL_USD=400
ENABLED_STRATEGIES=HTF_Breakout
SIDES=long
CANDLE_LIMIT_5M=300
CANDLE_LIMIT_1H=220
```

The aggregate cap was corrected from $240 to $400 so five $80 slots are
actually usable. This increases capacity, not mandatory risk: stop-distance,
margin, funding, cooldown, and daily-loss gates still apply.

## Apply

1. In Render, open the service, then **Environment**.
2. Delete every manually-created variable except the Keep list.
3. Save changes and perform **Clear build cache & deploy**.
4. Confirm `/api/status` reports a non-zero `day_start_balance` and no
   `recent_errors`.
5. Do not judge inactivity as a failure when decisions say `no signal` or
   `funding drag`; those are intentional risk rejections.
