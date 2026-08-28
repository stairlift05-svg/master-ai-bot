"""Logging & observability (#07): human-readable and JSON reports.

``build_txt_report`` renders the classic 7-section ASCII report (summary,
open positions, data-source health, closed trades, decisions, gaps/errors,
latest decisions) — upgraded with the new metrics (profit factor, expectancy,
drawdown) and per-symbol fetch statistics.
"""
from __future__ import annotations

import time
from collections import Counter
from datetime import datetime, timezone
from typing import Dict

from app.config import Settings
from app.models import Position
from app.persistence.database import Database
from app.state import EngineState


def _fmt_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


async def build_txt_report(state: EngineState, db: Database, settings: Settings,
                           prices: Dict[str, float],
                           open_times: Dict[str, float]) -> str:
    """Assemble the full text report shown via Telegram / report file."""
    snapshot = state.snapshot()
    decisions = await db.get_recent_decisions(200)
    closed = await db.get_closed_trades(20)
    metrics = await db.compute_metrics()
    st = snapshot
    now = time.time()

    lines = [
        "=" * 78,
        "     MASTER QUANT ENGINE v22  |  ARIAX TESTNET (PROFESSIONAL)",
        f"     Generated: {_fmt_utc()}",
        "=" * 78, "",
        "┌─ 1. SUMMARY ───────────────────────────────────────────────────────────",
        f"│  Total ${st['balance']:,.2f}  Free ${st['free_balance']:,.2f}",
        f"│  Risk {settings.risk_pct}% (adaptive x{_adaptive_factor(settings, state)})  "
        f"MaxNotional ${settings.max_notional_usd:.0f}  Lev {settings.leverage}x",
        f"│  DD {st['current_dd']:.2f}%  Open {len(st['active_positions'])}/{settings.max_positions}",
        f"│  Trades {metrics.total_trades}  WR {metrics.win_rate}%  "
        f"PF {metrics.profit_factor}  Exp ${metrics.expectancy:.4f}",
        f"│  Net PnL ${metrics.total_pnl:+.2f}  MaxDD {metrics.max_dd_pct:.2f}%  "
        f"Sharpe {metrics.sharpe}",
        f"│  Scan {st['last_scan']}  Sync {st['last_sync']}  "
        f"LossStreak {st['loss_streak']}",
        "└────────────────────────────────────────────────────────────────────────", "",
        "┌─ 2. OPEN ──────────────────────────────────────────────────────────────",
    ]
    active: Dict[str, Position] = st["active_positions"]
    if not active:
        lines.append("│  (flat)")
    else:
        for pid, p in active.items():
            price = prices.get(p["symbol"], p["entry"])
            upnl = (price - p["entry"]) * p["qty"] * (1 if p["side"] == "buy" else -1)
            hold_h = (now - open_times.get(pid, now)) / 3600
            lines.append(f"│  {p['symbol']} {p['side'].upper()} "
                         f"{p.get('strategy','')} ${upnl:+.3f} {hold_h:.1f}h")
    lines += ["└────────────────────────────────────────────────────────────────────────", "",
              "┌─ 3. DATA (candles: AriaX -> Bybit -> OKX -> Binance) ───────────────"]
    for sym in settings.symbols:
        # A symbol may have only successes (or only failures) so its sparse
        # counter need not contain every key. Never index health counters
        # directly: Telegram report generation must remain total.
        s = st["fetch_stats"].get(sym, {})
        lines.append(
            f"│  {sym:<12} {settings.timeframe} {s.get('ok_5m', 0)}/{s.get('fail_5m', 0)}  "
            f"{settings.htf_timeframe} {s.get('ok_1h', 0)}/{s.get('fail_1h', 0)}"
        )
    lines += ["└────────────────────────────────────────────────────────────────────────", "",
              "┌─ 4. CLOSED ────────────────────────────────────────────────────────────"]
    if not closed:
        lines.append("│  (none)")
    else:
        for t in closed[:10]:
            tag = "WIN" if t["pnl"] > 0 else "LOSS"
            lines.append(f"│  [{tag}] {t['symbol']:<12} ${t['pnl']:+.3f}  "
                         f"{(t.get('exit_reason') or '')[:28]}")
    reasons: Counter = Counter()
    sig_count = 0
    for d in decisions:
        if d["action"] in ("neutral", "rejected"):
            reasons[(d["reason"] or "?")[:50]] += 1
        else:
            sig_count += 1
    lines += ["└────────────────────────────────────────────────────────────────────────", "",
              "┌─ 5. DECISIONS ─────────────────────────────────────────────────────────",
              f"│  Total {len(decisions)} | Sig {sig_count} | Rej {len(decisions) - sig_count}"]
    for reason, count in reasons.most_common(8):
        lines.append(f"│    {count:4d} x {reason}")
    lines += ["└────────────────────────────────────────────────────────────────────────", "",
              "┌─ 6. GAPS / ERRORS ─────────────────────────────────────────────────────"]
    gaps = st["signal_but_not_executed"]
    errors = st["recent_errors"]
    for g in gaps:
        lines.append(f"│  {g}")
    for e in errors:
        lines.append(f"│  ERR {e}")
    if not gaps and not errors:
        lines.append("│  (none)")
    lines += ["└────────────────────────────────────────────────────────────────────────", "",
              "┌─ 7. LATEST DECISIONS ──────────────────────────────────────────────────"]
    for d in (decisions or [])[:12]:
        icon = "SIG" if d["action"] not in ("neutral", "rejected") else "REJ"
        lines.append(f"│  [{icon}] {(d.get('ts') or '')[:19]} {d['symbol']:<12} "
                     f"{(d.get('reason') or '')[:40]}")
    lines += ["└────────────────────────────────────────────────────────────────────────",
              "=" * 78]
    return "\n".join(lines)


def _adaptive_factor(settings: Settings, state: EngineState) -> float:
    from app.optimization.optimizer import AdaptiveRisk  # local import

    return round(AdaptiveRisk(settings, state).profile().factor, 2)
