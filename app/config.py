"""Validated, single-source-of-truth configuration.

Every tunable parameter lives in :class:`Settings` (populated from the
environment) so no module hard-codes magic numbers.  A single ``Settings``
instance is constructed once at startup and injected into every component,
which keeps the system testable and makes optimisation (module #10) trivial.

Environment variables (see ``.env.example``)::

    ARIAX_KEY, ARIAX_SECRET, ARIAX_BASE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    RISK_PCT, MAX_DD_PCT, MAX_DAILY_LOSS_PCT, MAX_NOTIONAL_USD, ... (see below)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, Tuple

from app.errors import ConfigError
from app.strategy.signals import DEFAULT_V2_PARAMS as _V2_DEFAULTS, _STRATEGY_CLASSES

# ---------------------------------------------------------------------------
# Symbol universe
# ---------------------------------------------------------------------------
# AriaX native symbol -> public pair used for the redundant candle feed.
SYMBOL_MAP: Dict[str, str] = {
    "ETHUSD": "ETH/USDT",
    "SOLUSD": "SOL/USDT",
    "XRPUSD": "XRP/USDT",
    "AVAXUSD": "AVAX/USDT",
    "DOTUSD": "DOT/USDT",
    "LINKUSD": "LINK/USDT",
    "ADAUSD": "ADA/USDT",
    "DOGEUSD": "DOGE/USDT",
}
# AriaX symbol -> Bybit-style v5 symbol (ETHUSD -> ETHUSDT) for the public
# /v5/market/kline endpoint served by the exchange itself.
V5_SYMBOL: Dict[str, str] = {k: k[:-3] + "USDT" for k in SYMBOL_MAP}
# Our timeframe name -> Bybit v5 interval code.
TF_V5: Dict[str, str] = {"1m": "1", "5m": "5", "15m": "15", "1h": "60", "4h": "240"}

# Default strategy parameter sets (v20.1 high-timeframe family).
DEFAULT_STRATEGY_PARAMS: Dict[str, Dict[str, float]] = {
    k: dict(v) for k, v in _V2_DEFAULTS.items()
}
# Strategy names enabled in the live engine.
# v20.6 (2026-08-27): Donchian_Trend, validated on 14 months of REAL Binance
# 1h data (2024-03 -> 2025-04, 5 symbols, 0 gaps — analysis/data_1h/).
#
# History of this line, because it matters:
#   * v20.4 shipped HTF_Breakout claiming "validated, PF 1.19". The repo's own
#     screening report says "None passed"; on real data HTF_Breakout returns
#     -0.48% train / -0.92% test. The claim was false.
#   * v20.5 (this audit) tested all six legacy families on the real 14-month
#     series with a train/test split. EVERY one failed out-of-sample.
#     SwingPullback_1h was the clearest trap: +3.21% train -> -5.67% test.
#   * The split is informative: the first half is a bull market, the second a
#     bear market. Every long-biased family died in the second half, which is
#     exactly what happened to this bot in production.
#
# Donchian_Trend is symmetric (long AND short), trades rarely, and lets the
# trailing stop run. Measured, same parameters throughout:
#     train (bull)  +3.64%  PF 1.41
#     test  (bear)  +3.52%  PF 1.53   <- unseen data
#     full 14mo     +5.52%  PF 1.33  maxDD 4.04%  104 trades
#     walk-forward  3 of 4 sequential quarters positive
# It sits on a broad parameter plateau (see analysis/STRATEGY_v20.6.md), not a
# single lucky cell, which is the main reason to believe it generalises.
DEFAULT_ENABLED_STRATEGIES: tuple = ("Donchian_Trend",)


def _env_str(name: str, default: str = "") -> str:
    value = os.getenv(name, "")
    return value.strip() if value else default


def _env_float(
    name: str, default: float, lo: float = -1e15, hi: float = 1e15
) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"Env {name}={raw!r} is not a valid number") from exc
    if not (lo <= value <= hi):
        raise ConfigError(f"Env {name}={value} out of range [{lo}, {hi}]")
    return value


def _env_int(
    name: str, default: int, lo: int = -(2**31), hi: int = 2**31 - 1
) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(float(raw))  # tolerate "5.0"
    except ValueError as exc:
        raise ConfigError(f"Env {name}={raw!r} is not a valid integer") from exc
    if not (lo <= value <= hi):
        raise ConfigError(f"Env {name}={value} out of range [{lo}, {hi}]")
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "y")


@dataclass(frozen=True)
class Settings:
    """Immutable validated engine settings (built once at startup)."""

    # ---- Exchange / API layer (#08) -------------------------------------
    arlax_key: str = ""
    arlax_secret: str = ""
    arlax_base: str = "https://dryclean-app-1.onrender.com"
    recv_window_ms: int = 5000
    request_timeout_s: float = 55.0
    connect_timeout_s: float = 25.0
    max_retries: int = 4
    retry_base_s: float = 4.0

    # ---- Telegram notifications ------------------------------------------
    tg_token: str = ""
    tg_chat_id: str = ""

    # ---- Trading universe ------------------------------------------------
    symbols: Tuple[str, ...] = tuple(SYMBOL_MAP.keys())
    # v20.6: Donchian_Trend was validated on 1h bars; the breakout logic reads
    # the primary timeframe, so the engine must feed it 1h candles.
    timeframe: str = "1h"
    htf_timeframe: str = "4h"
    # Mid timeframe fed to the strategy context. Historically hard-coded to
    # "15m", which is meaningless when the primary timeframe is 1h.
    mid_timeframe: str = "4h"

    # ---- Risk management (#01) -------------------------------------------
    leverage: int = 5
    max_positions: int = 5
    max_dd_pct: float = 10.0
    max_daily_loss_pct: float = 5.0
    risk_pct: float = 0.40
    max_notional_usd: float = 80.0
    # Five $80 slots require $400 aggregate capacity. The old $240 default
    # silently limited MAX_POS=5 to only three fully-sized positions.
    max_agg_notional_usd: float = 400.0
    min_order_usd: float = 8.0
    min_free_margin: float = 15.0
    margin_util_cap: float = 0.85
    margin_reserve_factor: float = 0.65
    min_stop_pct: float = 0.003
    taker_fee: float = 0.0005
    fee_buffer: float = 1.2
    # Expected slippage per side (fraction of price). Used by the strategy
    # cost gate and by the backtester so both price friction identically.
    slippage_pct: float = 0.0002
    # A signal's take-profit must be at least this multiple of the modelled
    # round-trip cost, otherwise it is discarded. Every losing version of this
    # bot took trades whose target barely covered fees; 3.0 is the think-tank
    # floor (target >= 3x friction).
    min_edge_ratio: float = 3.0

    # ---- Strategy (#02) ---------------------------------------------------
    strategy_params: Dict[str, Dict[str, float]] = field(
        default_factory=lambda: {k: dict(v) for k, v in DEFAULT_STRATEGY_PARAMS.items()}
    )
    enabled_strategies: Tuple[str, ...] = DEFAULT_ENABLED_STRATEGIES
    max_daily_entries: int = 12
    # v20.6: MUST be "both". Long-only returned +0.32% over the 14-month
    # sample versus +5.52% for both sides — the entire edge in the bear half
    # comes from shorts. SIDES=long was a primary cause of the live losses.
    sides: str = "both"

    # ---- Funding gate ------------------------------------------------------
    # The AriaX testnet reports a STATIC placeholder funding rate (0.75) for
    # most USD perp symbols (BTC 0.033, ETH -0.038, LINK/ADA/DOT/AVAX 0.75…).
    # The old hard-coded 0.30 threshold therefore blocked every long entry on
    # those symbols ("funding drag +0.75%") while the market was bullish —
    # the #1 reason v20.3.1 stopped trading. 0 disables the gate (testnet
    # placeholder data); set e.g. 0.10 (% per interval) on a real exchange.
    funding_max_pct: float = 0.0

    # ---- Stuck-position recovery (v20.4) -----------------------------------
    # A remote position the exchange keeps reporting after a close was
    # confirmed caused an infinite recover→close→recover loop (one phantom
    # "TP" close + ~$2 fake PnL every sync). These knobs hard-stop that loop.
    recover_cooldown_s: float = 1800.0     # min gap between recoveries of a symbol
    max_recover_cycles: int = 3            # then the symbol is marked stuck
    close_verify_recheck_s: float = 3600.0 # re-try an auto-close of a stuck position at most hourly
    dash_token: str = ""                   # optional shared secret for the dashboard

    # ---- Position management (trailing / partial) -------------------------
    # v20.5 audit: the old exit policy was the second-largest source of loss.
    # Measured over the strategy's own trades (analysis/EXIT_POLICY.md):
    # a 4h time stop closed 55 of 85 trades before the 1h-ATR target could be
    # reached (MaxHold PnL ≈ 0 gross, pure fee burn), while partial TP halved
    # every winner but never halved a loser. Both are now off/relaxed by
    # default, and the trail activates only well beyond the noise band.
    partial_tp: bool = False
    trail_act_pct: float = 4.0
    trail_step_pct: float = 1.0
    use_atr_trail: bool = True
    atr_trail_mult: float = 6.0
    min_hold_partial_s: float = 720.0
    min_hold_trail_s: float = 3600.0
    min_profit_be_pct: float = 0.75
    max_hold_s: float = 400 * 3600.0

    # ---- Timing / cooldowns (#10) ----------------------------------------
    scan_interval_s: float = 70.0
    symbol_delay_s: float = 2.0
    ohlcv_pause_s: float = 1.2
    sync_interval_s: float = 60.0
    price_interval_s: float = 15.0
    entry_cooldown_s: float = 3600.0  # 1 bar on the 1h timeframe
    post_close_cooldown_s: float = 3600.0
    error_cooldown_base_s: float = 120.0
    error_cooldown_max_s: float = 1800.0
    ghost_miss_limit: int = 3

    # ---- Data feed (#06) --------------------------------------------------
    # StrategyEngine drops the forming bar and requires 260 closed 5m bars.
    # Fetch at least 300 so the strategy can ever leave warm-up.
    candle_limit_5m: int = 300
    candle_limit_1h: int = 220
    close_on_data_stall: bool = True
    data_stall_grace_s: float = 600.0

    # ---- Adaptive risk (#10) ----------------------------------------------
    adapt_enabled: bool = True
    adapt_dd_band1_pct: float = 5.0
    adapt_dd_band2_pct: float = 7.5
    adapt_factors: Tuple[float, ...] = (0.5, 0.25)
    loss_streak_shrink_at: int = 3
    loss_streak_factor: float = 0.6
    auto_resume_dd_ratio: float = 0.5

    # ---- Paper mode (v20.5 safety gate) -----------------------------------
    # When True the engine runs the FULL pipeline — scans, signals, sizing,
    # risk gates, position lifecycle, dashboard, Telegram — but never sends
    # an order to the exchange; fills are simulated at the live price plus
    # modelled slippage. This exists because no strategy in this repo has
    # demonstrated a positive post-cost edge yet (see the screening report and
    # analysis/AUDIT_v20.5.md). Set PAPER_MODE=false only after a live paper
    # run shows a positive expectancy over a meaningful sample.
    paper_mode: bool = True

    # ---- Test / misc ------------------------------------------------------
    test_symbol: str = "ETHUSD"
    test_usd: float = 15.0
    send_client_oid: bool = False
    port: int = 10000
    log_file: str = ""
    log_level: str = "INFO"

    # ------------------------------------------------------------------
    def validate(self) -> None:
        """Sanity-check the whole configuration; raise :class:`ConfigError`."""
        if not self.arlax_key or not self.arlax_secret:
            raise ConfigError("ARIAX_KEY / ARIAX_SECRET are required (set them in .env)")
        if not self.arlax_base.startswith("https://"):
            raise ConfigError("ARIAX_BASE must be an https:// URL")
        if "ariax-1.onrender.com" in self.arlax_base:
            raise ConfigError(
                "ariax-1.onrender.com is a dead service; use https://dryclean-app-1.onrender.com"
            )
        if self.leverage < 1 or self.leverage > 100:
            raise ConfigError("LEVERAGE must be within [1, 100]")
        if not (0 < self.risk_pct <= 10):
            raise ConfigError("RISK_PCT must be within (0, 10]")
        if not (0 < self.max_dd_pct <= 100):
            raise ConfigError("MAX_DD_PCT must be within (0, 100]")
        if self.max_notional_usd <= 0 or self.max_agg_notional_usd <= self.max_notional_usd:
            raise ConfigError(
                "MAX_NOTIONAL_USD > 0 and MAX_AGG_NOTIONAL_USD must exceed it"
            )
        if self.max_positions < 1:
            raise ConfigError("MAX_POSITIONS must be >= 1")
        if not self.symbols:
            raise ConfigError("No trading symbols configured")
        for sym in self.symbols:
            if sym not in SYMBOL_MAP:
                raise ConfigError(f"Unknown symbol {sym!r}; allowed: {sorted(SYMBOL_MAP)}")
        if self.mid_timeframe not in TF_V5:
            raise ConfigError(f"Unsupported mid timeframe {self.mid_timeframe}")
        if self.timeframe not in TF_V5 or self.htf_timeframe not in TF_V5:
            raise ConfigError(f"Unsupported timeframes {self.timeframe}/{self.htf_timeframe}")
        if self.sides not in ("both", "long", "short"):
            raise ConfigError("SIDES must be one of: both, long, short")
        if self.enabled_strategies:
            known = set(_STRATEGY_CLASSES)
            unknown = [s for s in self.enabled_strategies if s not in known]
            if unknown:
                raise ConfigError(f"Unknown strategies in ENABLED_STRATEGIES: {unknown}")
        # StrategyEngine needs MIN_BARS_5M closed primary bars and drops the
        # forming candle in live mode. CANDLE_LIMIT_5M is the primary-timeframe
        # fetch size (the name is historical — the primary TF is configurable).
        # Donchian_Trend additionally needs entry_len + EMA200 warm-up.
        if self.candle_limit_5m < 261:
            raise ConfigError("CANDLE_LIMIT_5M must be >= 261 for strategy warm-up")
        if self.candle_limit_1h < 50:
            raise ConfigError("CANDLE_LIMIT_1H must be >= 50")

    # ------------------------------------------------------------------
    @classmethod
    def from_env(cls) -> "Settings":
        """Build :class:`Settings` from the process environment."""
        return cls(
            arlax_key=_env_str("ARIAX_KEY"),
            arlax_secret=_env_str("ARIAX_SECRET"),
            arlax_base=_env_str("ARIAX_BASE", "https://dryclean-app-1.onrender.com"),
            recv_window_ms=_env_int("ARIAX_RECV_WINDOW", 5000, 1000, 60000),
            request_timeout_s=_env_float("ARIAX_TIMEOUT", 55.0, 5, 180),
            connect_timeout_s=_env_float("ARIAX_CONNECT_TIMEOUT", 25.0, 2, 60),
            max_retries=_env_int("ARIAX_MAX_RETRIES", 4, 1, 8),
            retry_base_s=_env_float("ARIAX_RETRY_BASE", 4.0, 0.5, 60),
            tg_token=_env_str("TELEGRAM_BOT_TOKEN"),
            tg_chat_id=_env_str("TELEGRAM_CHAT_ID"),
            leverage=_env_int("LEVERAGE", 5, 1, 100),
            max_positions=_env_int("MAX_POS", 5, 1, 50),
            max_dd_pct=_env_float("MAX_DD", 10.0, 0.5, 100),
            max_daily_loss_pct=_env_float("MAX_DAILY_LOSS", 5.0, 0.5, 100),
            risk_pct=_env_float("RISK_PCT", 0.40, 0.01, 10),
            max_notional_usd=_env_float("MAX_NOTIONAL_USD", 80.0, 5, 1e6),
            max_agg_notional_usd=_env_float("MAX_AGG_NOTIONAL_USD", 400.0, 5, 1e7),
            min_order_usd=_env_float("MIN_ORDER_USD", 8.0, 1, 1e5),
            min_free_margin=_env_float("MIN_FREE_MARGIN", 15.0, 1, 1e6),
            margin_util_cap=_env_float("MARGIN_UTIL_CAP", 0.85, 0.1, 0.99),
            margin_reserve_factor=_env_float("MARGIN_RESERVE_FACTOR", 0.65, 0.1, 0.99),
            min_stop_pct=_env_float("MIN_STOP_PCT", 0.003, 0.0005, 0.05),
            taker_fee=_env_float("TAKER_FEE", 0.0005, 0, 0.01),
            fee_buffer=_env_float("FEE_BUFFER", 1.2, 1.0, 5.0),
            slippage_pct=_env_float("SLIPPAGE_PCT", 0.0002, 0, 0.01),
            min_edge_ratio=_env_float("MIN_EDGE_RATIO", 3.0, 0, 20),
            partial_tp=_env_bool("PARTIAL_TP", False),
            trail_act_pct=_env_float("TRAIL_ACT", 4.0, 0.5, 50),
            trail_step_pct=_env_float("TRAIL_STEP", 1.0, 0.1, 10),
            use_atr_trail=_env_bool("USE_ATR_TRAIL", True),
            atr_trail_mult=_env_float("ATR_TRAIL_MULT", 6.0, 0.1, 20),
            min_hold_partial_s=_env_float("MIN_HOLD_FOR_PARTIAL", 720, 60, 86400),
            min_hold_trail_s=_env_float("MIN_HOLD_FOR_TRAIL", 3600, 60, 86400),
            min_profit_be_pct=_env_float("MIN_PROFIT_FOR_BE", 0.75, 0.1, 10),
            max_hold_s=_env_float("MAX_HOLD_SECONDS", 400 * 3600, 300, 60 * 86400),
            scan_interval_s=_env_float("SCAN_INTERVAL", 70, 10, 3600),
            symbol_delay_s=_env_float("SYMBOL_DELAY", 2.0, 0, 60),
            ohlcv_pause_s=_env_float("OHLCV_PAUSE", 1.2, 0, 30),
            sync_interval_s=_env_float("SYNC_INTERVAL", 60, 10, 3600),
            price_interval_s=_env_float("PRICE_INTERVAL", 15, 3, 300),
            entry_cooldown_s=_env_float("ENTRY_COOLDOWN", 3600, 60, 86400),
            max_daily_entries=_env_int("MAX_DAILY_ENTRIES", 12, 1, 200),
            sides=_env_str("SIDES", "both").lower(),
            funding_max_pct=_env_float("FUNDING_MAX_PCT", 0.0, 0, 100),
            recover_cooldown_s=_env_float("RECOVER_COOLDOWN", 1800, 60, 86400),
            max_recover_cycles=_env_int("MAX_RECOVER_CYCLES", 3, 1, 50),
            close_verify_recheck_s=_env_float("CLOSE_VERIFY_RECHECK", 3600, 60, 86400),
            dash_token=_env_str("DASH_TOKEN"),
            enabled_strategies=tuple(
                s.strip() for s in _env_str(
                    "ENABLED_STRATEGIES", ",".join(DEFAULT_ENABLED_STRATEGIES)
                ).split(",") if s.strip()
            ),
            post_close_cooldown_s=_env_float("POST_CLOSE_COOLDOWN", 3600, 0, 86400),
            error_cooldown_base_s=_env_float("ERROR_COOLDOWN_BASE", 120, 5, 3600),
            error_cooldown_max_s=_env_float("ERROR_COOLDOWN_MAX", 1800, 60, 86400),
            ghost_miss_limit=_env_int("GHOST_MISS_LIMIT", 3, 1, 20),
            candle_limit_5m=_env_int("CANDLE_LIMIT_5M", 300, 261, 1000),
            candle_limit_1h=_env_int("CANDLE_LIMIT_1H", 220, 50, 1000),
            close_on_data_stall=_env_bool("CLOSE_ON_DATA_STALL", True),
            data_stall_grace_s=_env_float("DATA_STALL_GRACE", 600, 60, 86400),
            adapt_enabled=_env_bool("ADAPTIVE_RISK", True),
            adapt_dd_band1_pct=_env_float("ADAPT_DD_BAND1", 5.0, 0.5, 100),
            adapt_dd_band2_pct=_env_float("ADAPT_DD_BAND2", 7.5, 0.5, 100),
            loss_streak_shrink_at=_env_int("LOSS_STREAK_SHRINK_AT", 3, 1, 20),
            loss_streak_factor=_env_float("LOSS_STREAK_FACTOR", 0.6, 0.1, 1.0),
            auto_resume_dd_ratio=_env_float("AUTO_RESUME_DD_RATIO", 0.5, 0.1, 0.9),
            paper_mode=_env_bool("PAPER_MODE", True),
            timeframe=_env_str("TIMEFRAME", "1h"),
            mid_timeframe=_env_str("MID_TIMEFRAME", "4h"),
            htf_timeframe=_env_str("HTF_TIMEFRAME", "4h"),
            test_symbol=_env_str("TEST_SYMBOL", "ETHUSD"),
            test_usd=_env_float("TEST_USD", 15.0, 1, 1e5),
            send_client_oid=_env_bool("SEND_CLIENT_OID", False),
            port=_env_int("PORT", 10000, 1, 65535),
            log_file=_env_str("LOG_FILE"),
            log_level=_env_str("LOG_LEVEL", "INFO").upper(),
        )
