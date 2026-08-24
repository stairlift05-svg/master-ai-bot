"""Core orchestration: the QuantEngine.

The engine is deliberately thin — every policy decision lives in its module
(risk, execution, strategy, capital), and this class only wires components
together and runs the four supervision loops:

* ``price_loop``   — refresh prices + wallet, update halts, log equity.
* ``scan_loop``    — per-symbol strategy scans and order attempts.
* ``watchdog_loop``— position lifecycle (SL/TP/trail/partial/max-hold/stall).
* ``sync_loop``    — reconcile local positions against the exchange.

It also provides the Telegram callbacks, self-healing (margin top-up,
auto-resume from halts, ghost cleanup) and graceful shutdown.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from typing import Awaitable, Callable, Dict, Optional

from app.api.ariax_client import (
    AriaXClient,
    find_market_item,
    market_price,
)
from app.api.signing import RequestSigner
from app.capital.margin import MarginManager
from app.config import Settings
from app.data.feed import CandleFeed
from app.errors import AriaXAPIError, ConfigError, DataUnavailableError
from app.execution.executor import OrderExecutor
from app.execution.watchdog import PositionWatchdog
from app.models import CandleSeries, Position
from app.notify.telegram import TelegramController
from app.observability.reporter import build_txt_report
from app.persistence.database import Database
from app.risk.position_sizer import PositionSizer
from app.risk.risk_manager import RiskManager
from app.state import EngineState
from app.strategy.engine import StrategyEngine

log = logging.getLogger("quant.engine")

_MARGIN_FLOOR_USD = 60.0
_MAX_RECOVERY_ENTRY_ATR_PCT = 0.012


class QuantEngine:
    """Wires every module and runs the four supervision loops."""

    def __init__(self, settings: Settings, state: EngineState,
                 db_path: str = "bot.db",
                 client: Optional[AriaXClient] = None) -> None:
        self.settings = settings
        self.state = state
        self.db = Database(db_path)

        # Exchange + data plumbing (injectable for tests / dry runs).
        self.signer = RequestSigner(settings)
        self.client = client or AriaXClient(settings, self.signer)
        self.margin = MarginManager(settings, self.client, state)
        self.feed = CandleFeed(settings, self.client, state)

        # Strategy + risk.
        self.strategy = StrategyEngine(settings, state)
        self.risk = RiskManager(settings, state)
        self.sizer = PositionSizer(settings)

        # Telegram first (the executor reports through it), then execution.
        self.tg = self._build_telegram()
        self.executor = OrderExecutor(
            settings, self.client, state, self.risk, self.sizer, self.margin,
            self.db, self.tg, self._live_price,
        )
        self.watchdog = PositionWatchdog(settings, state, self.executor, self.db)

        # Live price cache.
        self.prices: Dict[str, float] = {}
        self.last_price_ts: Dict[str, float] = {}
        self._tasks: list = []
        self._miss_count: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        """Initialise DB, check connectivity, warm up, launch loops."""
        await self.db.init()
        self.settings.validate()
        if not self.settings.arlax_key or not self.settings.arlax_secret:
            raise ConfigError("ARIAX_KEY / ARIAX_SECRET missing (see .env.example)")

        log.info("Quant v20 starting | base=%s | symbols=%s",
                 self.settings.arlax_base, list(self.settings.symbols))

        await self._health_check_with_retry()
        await self._warmup()
        await self._restore_daily_state()

        self._tasks = [
            asyncio.create_task(self._loop("price", self.settings.price_interval_s,
                                           self._price_round)),
            asyncio.create_task(self._loop("scan", self.settings.scan_interval_s,
                                           self._scan_round)),
            asyncio.create_task(self._loop("watchdog", 2.0, self._watchdog_round)),
            asyncio.create_task(self._loop("sync", self.settings.sync_interval_s,
                                           self._sync_round)),
            asyncio.create_task(self.tg.run()),
        ]
        log.info("Engine loops started (%d tasks)", len(self._tasks))

    async def shutdown(self) -> None:
        """Cancel loops and close all async resources cleanly."""
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.client.close()
        await self.feed.close()
        await self.tg.close()
        log.info("Engine shut down cleanly")

    # ------------------------------------------------------------------
    # Start-up helpers
    # ------------------------------------------------------------------
    async def _health_check_with_retry(self) -> None:
        for attempt in range(4):
            if await self.client.health():
                log.info("✅ AriaX reachable @ %s", self.settings.arlax_base)
                return
            log.warning("AriaX unreachable; attempt %d/4 — retrying in 15s",
                        attempt + 1)
            await asyncio.sleep(15)
        log.error("=" * 64)
        log.error("❌ AriaX not reachable. Checklist:")
        log.error("  1) ARIAX_BASE must be https://dryclean-app-1.onrender.com")
        log.error("     (the old ariax-1.onrender.com is dead)")
        log.error("  2) ARIAX_KEY / ARIAX_SECRET must be valid")
        log.error("  3) Engine continues and will keep retrying")
        log.error("=" * 64)

    async def _warmup(self) -> None:
        for name, fn in (
            ("markets", self.client.get_markets),
            ("config", self.client.get_config),
            ("wallet", self.client.get_wallet),
        ):
            try:
                data = await fn()
                log.info("AriaX %s OK (%s)", name,
                         list(data.keys())[:8] if isinstance(data, dict) else type(data).__name__)
            except AriaXAPIError as exc:
                log.warning("warmup %s: %s", name, exc)
                await asyncio.sleep(3)
        await self.margin.refresh()
        # Self-healing: guarantee futures margin before the first trade.
        await self.margin.ensure_futures_margin(_MARGIN_FLOOR_USD)

    async def _restore_daily_state(self) -> None:
        """Restore day-start/peak balance across restarts (restart-safe PnL)."""
        today = time.strftime("%Y-%m-%d")
        stored_day = await self.db.load_meta("day_start_date", "")
        stored_balance = await self.db.load_meta("day_start_balance", 0.0)
        peak = await self.db.load_meta("peak_balance", 0.0)

        if stored_day != today or stored_balance <= 0:
            day_start = float(self.state.get("balance", 0.0) or 0.0)
            # Persist and publish atomically from the engine's perspective.
            # Previously only SQLite was updated, leaving the dashboard at
            # day_start_balance=0 and weakening daily-PnL observability.
            self.state.set("day_start_balance", day_start)
            await self.db.save_meta("day_start_date", today)
            await self.db.save_meta("day_start_balance", day_start)
            log.info("New trading day: day_start=$%.2f", day_start)
        else:
            self.state.set("day_start_balance", float(stored_balance))
            log.info("Restored day_start=$%.2f (date %s)", stored_balance, stored_day)

        if peak > 0:
            self.state.set("peak_balance", float(peak))

    # ------------------------------------------------------------------
    # Loop driver
    # ------------------------------------------------------------------
    async def _loop(self, name: str, interval: float, fn: Callable[[], Awaitable]) -> None:
        while True:
            started = time.monotonic()
            try:
                await fn()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - a loop must never die
                log.exception("loop %s crashed: %s", name, exc)
                self.state.record_error(f"loop {name}: {exc}")
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.5, interval + random.uniform(-0.2, 0.2) - elapsed))

    # ------------------------------------------------------------------
    # price_loop
    # ------------------------------------------------------------------
    async def _price_round(self) -> None:
        try:
            data = await self.client.get_markets()
            for sym in self.settings.symbols:
                px = market_price(find_market_item(data, sym))
                if px > 0:
                    self.prices[sym] = px
                    self.last_price_ts[sym] = time.time()
        except AriaXAPIError as exc:
            self.state.record_error(f"markets: {exc}")

        await self.margin.refresh()
        ok, why = self.risk.update_halts(
            self.margin.wallet.equity,
            self.state.get("peak_balance", 0.0) or self.margin.wallet.equity,
            self.state.get("day_start_balance", 0.0) or self.margin.wallet.equity,
        )
        balance = self.margin.wallet.equity
        peak = max(self.state.get("peak_balance", 0.0), balance)
        self.state.set("peak_balance", peak)
        if balance > 0:
            await self.db.log_equity(balance, peak, self.state.get("current_dd", 0.0))
        await self.db.save_meta("peak_balance", peak)
        if not ok:
            log.warning("TRADING HALTED: %s", why)
        # Self-healing margin top-up if the futures wallet runs dry.
        if self.margin.topup_needed():
            await self.margin.ensure_futures_margin(_MARGIN_FLOOR_USD)

    # ------------------------------------------------------------------
    # scan_loop
    # ------------------------------------------------------------------
    async def _scan_round(self) -> None:
        if not self.state.get("is_active", True):
            return
        if self.state.get("dd_halted") or self.state.get("daily_halted"):
            return
        open_symbols = {p.symbol for p in self.state.positions().values()}
        self.state.set("last_scan", time.strftime("%H:%M:%S"))

        for sym in self.settings.symbols:
            if sym in open_symbols:
                continue
            allowed, why = self.risk.can_enter_symbol(sym)
            if not allowed:
                continue
            try:
                await self._scan_symbol(sym)
            except Exception as exc:  # noqa: BLE001
                log.error("scan %s: %s", sym, exc)
                self.state.record_error(f"scan {sym}: {exc}")
            await asyncio.sleep(self.settings.symbol_delay_s)

    async def _scan_symbol(self, sym: str) -> None:
        try:
            candles5 = await self.feed.fetch(sym, self.settings.timeframe,
                                             self.settings.candle_limit_5m)
            candles15 = await self.feed.fetch(sym, "15m", 160)
            candles1 = await self.feed.fetch(sym, self.settings.htf_timeframe,
                                             self.settings.candle_limit_1h)
        except DataUnavailableError:
            self.risk.mark_failure(sym, 1)
            return

        df5 = CandleSeries(candles5)
        df15 = CandleSeries(candles15) if len(candles15) > 30 else df5
        df1 = CandleSeries(candles1) if len(candles1) > 30 else df5
        last_close = candles5[-1].c if candles5 else 0.0
        if last_close > 0:
            self.prices[sym] = last_close
            self.last_price_ts[sym] = time.time()

        result = self.strategy.analyze(df5, df15, df1, symbol=sym,
                                       drop_forming=True)
        await self.db.log_decision(
            sym, result.action, result.strategy, result.reason,
            self.prices.get(sym, 0.0), result.rsi, result.atr, result.htf,
        )
        log.info(
            "SCAN %s action=%s strategy=%s reason=%s bars=%d/%d/%d",
            sym, result.action, result.strategy or "-", result.reason,
            len(candles5), len(candles15), len(candles1),
        )
        if result.signal is not None:
            await self.executor.try_open(sym, result.signal)

    # ------------------------------------------------------------------
    # watchdog_loop
    # ------------------------------------------------------------------
    async def _watchdog_round(self) -> None:
        await self.watchdog.scan_once(self._price_for, self._live_price)

    # ------------------------------------------------------------------
    # sync_loop
    # ------------------------------------------------------------------
    async def _sync_round(self) -> None:
        await self.smart_sync()

    async def smart_sync(self, startup: bool = False) -> None:
        """Reconcile local positions against the exchange truth."""
        try:
            data = await self.client.get_positions()
        except AriaXAPIError as exc:
            self.state.record_error(f"sync: {exc}")
            return

        remote = self._parse_remote_positions(data)
        log.info("remote positions: %s", list(remote.keys()) or "(none)")
        local = self.state.positions()

        # Ghost detection: local position unseen remotely N times in a row.
        for pid, pos in local.items():
            if pos.strategy == "RealTest":
                continue
            if pos.symbol in remote:
                self._miss_count[pid] = 0
                self._check_qty_drift(pos, remote[pos.symbol])
                continue
            self._miss_count[pid] = self._miss_count.get(pid, 0) + 1
            if self._miss_count[pid] >= self.settings.ghost_miss_limit:
                log.warning("GHOST %s (miss %d) — cleaning up",
                            pos.symbol, self._miss_count[pid])
                await self.executor.resolve_ghost(pos, "confirmed")

        # Recovery: remote positions we don't track locally.
        known = {p.symbol for p in local.values()}
        for sym, rpos in remote.items():
            if sym in known:
                continue
            await self._recover_position(sym, rpos)

        if startup:
            await self._cleanup_startup_ghosts(remote)

        self.state.set("last_sync", time.strftime("%H:%M:%S"))
        log.info("sync done: active=%d", len(self.state.positions()))

    # ------------------------------------------------------------------
    # Position recovery / drift
    # ------------------------------------------------------------------
    async def _recover_position(self, sym: str, rpos: Dict) -> None:
        entry = rpos.get("entry") or self.prices.get(sym, 0.0)
        if entry <= 0:
            return
        atr_est = entry * _MAX_RECOVERY_ENTRY_ATR_PCT
        side = rpos["side"]
        if side == "buy":
            sl, tp1, tp = entry - atr_est * 1.5, entry + atr_est * 1.7, entry + atr_est * 3.2
        else:
            sl, tp1, tp = entry + atr_est * 1.5, entry - atr_est * 1.7, entry - atr_est * 3.2
        position = Position(
            id=f"recovered_{uuid.uuid4().hex[:8]}", symbol=sym, side=side,
            strategy="Recovered", entry=entry, qty=rpos["qty"], sl=sl, tp1=tp1,
            tp=tp, opened_at=time.time(), atr_at_entry=atr_est,
        )
        self.state.add_position(position)
        self._miss_count[position.id] = 0
        await self.db.insert_trade(position)
        await self.tg.send(f"🔄 Recovered {sym} ({side}) qty={rpos['qty']}")
        log.info("RECOVERED %s %s qty=%s", sym, side, rpos["qty"])

    def _check_qty_drift(self, pos: Position, rpos: Dict) -> None:
        remote_qty = rpos.get("qty", 0.0)
        if remote_qty <= 0:
            return
        if abs(remote_qty - pos.qty) > max(pos.qty * 0.01, 1e-8):
            log.warning("QTY DRIFT %s local=%s remote=%s — adopting remote",
                        pos.symbol, pos.qty, remote_qty)
            self.state.update_position(pos.id, qty=remote_qty)
            self.db_insert_qty_update(pos.id, remote_qty)

    def db_insert_qty_update(self, pid: str, qty: float) -> None:
        """Schedule a persisted qty fix (fire-and-forget task)."""
        asyncio.create_task(self._persist_qty(pid, qty))

    async def _persist_qty(self, pid: str, qty: float) -> None:
        try:
            pos = self.state.position(pid)
            if pos is not None:
                await self.db.update_trade(pid, qty, pos.sl, pos.is_partial,
                                           pos.highest_pnl_pct)
        except Exception as exc:  # pragma: no cover
            log.warning("persist qty %s: %s", pid, exc)

    async def _cleanup_startup_ghosts(self, remote: Dict) -> None:
        for trade in await self.db.get_open_trades():
            if trade["symbol"] not in remote:
                await self.db.close_trade(trade["id"], 0.0, 0.0,
                                          "startup_ghost", 0.0)

    # ------------------------------------------------------------------
    # Real-test flow (Telegram command)
    # ------------------------------------------------------------------
    async def real_test_trade(self) -> None:
        await self.tg.send("⚡ Real test on AriaX…")
        try:
            await self.margin.refresh()
            free = self.state.get("free_balance") or self.state.get("balance") or 0.0
            await self.tg.send(f"💰 Free ${free:.2f}")
            if free < self.settings.min_free_margin:
                await self.tg.send("❌ insufficient balance")
                return
            sym = self.settings.test_symbol
            price = await self._live_price(sym)
            if price <= 0:
                await self.tg.send("❌ no price")
                return
            size = self.sizer.compute(sym, price, price * 0.99, free)
            qty = min(size.qty, self.settings.test_usd / price)
            await self.tg.send(f"🧪 {sym} qty={qty:.6f} ~${qty * price:.1f}")
            if qty <= 0:
                await self.tg.send("❌ qty=0")
                return
            resp = await self.client.place_order(
                sym, "buy", qty, lev=self.settings.leverage, order_type="market",
            )
            fill = self.executor._fill_price(resp, sym, qty) or price
            position = Position(
                id=f"test_{uuid.uuid4().hex[:6]}", symbol=sym, side="buy",
                strategy="RealTest", entry=fill, qty=qty, sl=fill * 0.97,
                tp1=fill * 1.015, tp=fill * 1.03, opened_at=time.time(),
            )
            self.state.add_position(position)
            await self.tg.send(f"🧪 opened @ {fill:.5f}")
            await asyncio.sleep(12)
            await self.executor.close(position, "RealTest")
            await self.tg.send("✅ test closed", self.tg.menu())
        except Exception as exc:  # noqa: BLE001
            await self.tg.send(f"❌ Test failed:\n{str(exc)[:220]}")
            self.state.record_error(f"real_test: {exc}")

    # ------------------------------------------------------------------
    # Price providers (used by executor + watchdog)
    # ------------------------------------------------------------------
    async def _live_price(self, symbol: str) -> float:
        cached = self.prices.get(symbol, 0.0)
        age = time.time() - self.last_price_ts.get(symbol, 0.0)
        if cached > 0 and age < 60:
            return cached
        try:
            data = await self.client.get_markets()
            px = market_price(find_market_item(data, symbol))
            if px > 0:
                self.prices[symbol] = px
                self.last_price_ts[symbol] = time.time()
            return px or cached
        except AriaXAPIError:
            return cached

    def _price_for(self, symbol: str) -> tuple:
        """(price, age_seconds) — used by the watchdog for stall detection."""
        now = time.time()
        price = self.prices.get(symbol, 0.0)
        age = now - self.last_price_ts.get(symbol, now)
        return price, age

    # ------------------------------------------------------------------
    # Remote position parsing
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_remote_positions(data) -> Dict[str, Dict]:
        out: Dict[str, Dict] = {}
        rows = data
        if isinstance(data, dict):
            rows = (data.get("data") or data.get("positions") or data.get("items")
                    or [])
            if not rows and data.get("symbol"):
                rows = [data]
        if not isinstance(rows, list):
            return out
        for p in rows:
            if not isinstance(p, dict):
                continue
            sym = str(p.get("symbol") or p.get("pair") or "").upper()
            sym = sym.replace("/", "").replace(":", "")
            if not sym:
                continue
            qty = float(p.get("qty") or p.get("size") or p.get("quantity")
                        or p.get("contracts") or 0)
            if abs(qty) < 1e-12:
                continue
            side_raw = str(p.get("side") or p.get("positionSide") or "").lower()
            if side_raw in ("sell", "short", "s"):
                side, qty = "sell", abs(qty)
            elif side_raw in ("buy", "long", "b"):
                side, qty = "buy", abs(qty)
            else:
                side, qty = ("buy", abs(qty)) if qty > 0 else ("sell", abs(qty))
            entry = float(p.get("entry") or p.get("entryPrice") or p.get("avgPrice")
                          or p.get("price") or 0)
            out[sym] = {"symbol": sym, "side": side, "qty": qty, "entry": entry}
        return out

    # ------------------------------------------------------------------
    # Telegram wiring
    # ------------------------------------------------------------------
    def _build_telegram(self) -> TelegramController:
        return TelegramController(self.settings, self.state, {
            "cmd_dash": self._tg_dashboard,
            "cmd_pos": self._tg_positions,
            "cmd_sync": self._tg_sync,
            "cmd_txt": self._tg_report,
            "cmd_rej": self._tg_rejections,
            "cmd_realtest": self.real_test_trade,
            "cmd_start": self._tg_start,
            "cmd_pause": self._tg_pause,
            "on_close": self._tg_close,
        })

    async def _tg_dashboard(self) -> None:
        st = self.state.snapshot()
        await self.tg.send(
            f"📊 <b>Quant v20 AriaX</b>\n"
            f"Total ${st['balance']:.2f}\nFree ${st['free_balance']:.2f}\n"
            f"Pos {len(st['active_positions'])}/{self.settings.max_positions}\n"
            f"DD {st['current_dd']:.2f}% | "
            f"Streak {st['loss_streak']}",
            self.tg.menu(),
        )

    async def _tg_positions(self) -> None:
        positions = self.state.positions()
        if not positions:
            await self.tg.send("💤 flat", self.tg.menu())
            return
        lines = ["💼"]
        for pos in positions.values():
            price = self.prices.get(pos.symbol, pos.entry)
            pnl = pos.unrealized_pnl(price)
            lines.append(f"{pos.symbol} {pos.side} ${pnl:+.2f}")
        await self.tg.send("\n".join(lines), self.tg.menu())

    async def _tg_sync(self) -> None:
        await self.smart_sync()
        await self.tg.send("🔄 Sync done", self.tg.menu())

    async def _tg_report(self) -> None:
        open_times = {pid: p.opened_at for pid, p in self.state.positions().items()}
        report = await build_txt_report(self.state, self.db, self.settings,
                                        self.prices, open_times)
        with open("report.txt", "w", encoding="utf-8") as handle:
            handle.write(report)
        await self.tg.send_document("report.txt", "📄 Quant v20 AriaX")

    async def _tg_rejections(self) -> None:
        decisions = await self.db.get_recent_decisions(12)
        lines = ["🚫"]
        for d in decisions:
            icon = "✅" if d["action"] not in ("neutral", "rejected") else "⛔"
            lines.append(f"{icon} {d['symbol']}\n{(d['reason'] or '')[:60]}")
        await self.tg.send("\n\n".join(lines), self.tg.menu())

    async def _tg_start(self) -> None:
        self.state.set("is_active", True)
        await self.tg.send("▶️ Started", self.tg.menu())

    async def _tg_pause(self) -> None:
        self.state.set("is_active", False)
        await self.tg.send("⏸️ Paused", self.tg.menu())

    async def _tg_close(self, pid: str) -> None:
        position = self.state.position(pid)
        if position is None:
            await self.tg.send("❌ position not found")
            return
        await self.executor.close(position, "Manual_TG")
