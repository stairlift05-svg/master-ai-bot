"""Trade execution (#03): order flow control.

``OrderExecutor`` is the *only* component allowed to touch the exchange order
endpoint.  It enforces, in order:

1. local risk gates + portfolio caps (see ``risk`` / ``optimization``),
2. funding-drag protection,
3. ATR-based position sizing with adaptive risk scaling,
4. input validation (security layer),
5. order placement with idempotency token support,
6. tolerant fill parsing (average price / last price fallback),
7. registration of the position in state + SQLite + Telegram.

Close and partial-TP flows are symmetric, compute realised PnL from the
*actual fill price* (fixing a v19.3 defect that used the last mark price),
and handle "position already closed remotely" gracefully.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, Callable, Dict, Optional, Tuple

from app.api.ariax_client import AriaXClient, find_market_item
from app.capital.margin import MarginManager
from app.config import Settings
from app.errors import AriaXAPIError, OrderRejectedError
from app.models import CloseResult, OrderResult, Position, Signal
from app.optimization.optimizer import AdaptiveRisk, PortfolioLimits
from app.persistence.database import Database
from app.risk.position_sizer import PositionSizer
from app.risk.risk_manager import RiskManager
from app.security.validation import OrderValidator
from app.state import EngineState
from app.strategy.indicators import quantize
from app.notify.telegram import TelegramController

log = logging.getLogger("quant.execution")


class OrderExecutor:
    """Executes and tracks orders end-to-end."""

    def __init__(
        self,
        settings: Settings,
        client: AriaXClient,
        state: EngineState,
        risk: RiskManager,
        sizer: PositionSizer,
        margin: MarginManager,
        db: Database,
        tg: TelegramController,
        price_provider: Callable[[str], float],
    ) -> None:
        self._settings = settings
        self._client = client
        self._state = state
        self._risk = risk
        self._sizer = sizer
        self._margin = margin
        self._db = db
        self._tg = tg
        self._price = price_provider
        self._validator = OrderValidator(settings)
        self._adaptive = AdaptiveRisk(settings, state)
        self._limits = PortfolioLimits(settings)
        self._failure_count: Dict[str, int] = {}
        self._unverified_closes: Dict[str, int] = {}
        self._paper_positions: Dict[str, Dict[str, float]] = {}
        if settings.paper_mode:
            log.warning(
                "PAPER MODE ACTIVE — orders are simulated locally, nothing is "
                "sent to the exchange. Set PAPER_MODE=false to trade for real."
            )

    # ------------------------------------------------------------------
    # Order transport (the single place that reaches the exchange)
    # ------------------------------------------------------------------
    async def _submit(self, symbol: str, side: str, qty: float,
                      strategy: str = "", client_oid: str = "") -> Any:
        """Send an order, or simulate the fill when paper mode is enabled.

        Paper fills use the live price moved against us by the configured
        slippage, so paper PnL is charged the same friction the strategy cost
        gate and the backtester assume. The response mimics the exchange
        envelope so every downstream parser is exercised unchanged.
        """
        if not self._settings.paper_mode:
            return await self._client.place_order(
                symbol, side, qty, lev=self._settings.leverage,
                order_type="market", strategy=strategy, client_oid=client_oid,
            )

        price = await self._price(symbol)
        if price <= 0:
            raise OrderRejectedError(symbol, "paper fill needs a live price")
        slip = 1.0 + self._settings.slippage_pct if side == "buy" \
            else 1.0 - self._settings.slippage_pct
        fill = price * slip
        held = self._paper_positions.get(symbol, {"qty": 0.0, "side": side})
        if held["side"] == side:
            held = {"qty": held["qty"] + qty, "side": side}
        else:
            held = {"qty": max(0.0, held["qty"] - qty), "side": held["side"]}
        if held["qty"] <= 1e-12:
            self._paper_positions.pop(symbol, None)
        else:
            self._paper_positions[symbol] = held
        log.info("PAPER %s %s qty=%s @ %.6f", side.upper(), symbol, qty, fill)
        return {"ok": True, "paper": True,
                "orderId": f"paper_{uuid.uuid4().hex[:10]}",
                "avgPrice": fill, "qty": qty}

    def paper_positions(self) -> Dict[str, Dict[str, float]]:
        """Simulated exchange-side positions (paper mode only)."""
        return {k: dict(v) for k, v in self._paper_positions.items()}

    # ------------------------------------------------------------------
    # Opening
    # ------------------------------------------------------------------
    async def try_open(self, symbol: str, signal: Signal) -> Optional[Position]:
        """Attempt to open a position from a signal. Returns the Position or None."""
        # 1) Local gates.
        allowed, why = self._risk.can_enter_symbol(symbol)
        if not allowed:
            self._state.record_missed_signal(f"{symbol} {signal.strategy} -> {why}")
            return None

        # 2) Live price + funding gate.
        price = await self._price(symbol)
        if price <= 0:
            self._state.record_missed_signal(f"{symbol} {signal.strategy} -> no price")
            return None
        funding = await self._funding_for(symbol)
        blocked, why_f = self._risk.funding_blocked(signal.side, funding)
        if blocked:
            self._state.record_missed_signal(f"{symbol} {signal.strategy} -> {why_f}")
            log.warning("SKIP %s %s: %s", symbol, signal.side, why_f)
            return None

        # 3) Refresh margin and compute size (adaptive risk aware).
        try:
            await self._margin.refresh()
        except AriaXAPIError as exc:
            self._state.record_error(f"margin refresh: {exc}")
            return None
        free = self._state.get("free_balance") or self._state.get("balance") or 0.0
        risk_pct = self._adaptive.effective_risk_pct()
        meta = self._client.symbol_meta.get(symbol)
        size = self._sizer.compute(symbol, price, signal.sl, free, meta, risk_pct)
        if not size.ok:
            self._state.record_missed_signal(
                f"{symbol} {signal.strategy} -> {size.reason}"
            )
            return None

        # 4) Portfolio caps + validation.
        # مهم: open_notional باید با entry هر پوزیشن محاسبه شود،
        # نه با price نماد فعلی (باگ قبلی: qtyِ DOGE × قیمت SOL → $43k جعلی).
        open_positions = self._state.positions()
        open_notional = sum(
            max(0.0, p.qty) * (p.entry if p.entry > 0 else price)
            for p in open_positions.values()
        )
        cap_err = self._limits.would_exceed(
            {p.symbol for p in open_positions.values()}, len(open_positions),
            symbol, price, size.notional,
            open_notional=open_notional,
        )
        if cap_err:
            self._state.record_missed_signal(f"{symbol} {signal.strategy} -> {cap_err}")
            return None
        self._validator.validate(symbol, signal.side, size.qty, price, size.notional)

        # 4b) Daily trade budget.
        if self._risk.daily_entries_left() <= 0:
            self._state.record_missed_signal(f"{symbol} {signal.strategy} -> daily budget reached")
            return None

        # 5) Place the order.
        client_oid = uuid.uuid4().hex[:24] if self._settings.send_client_oid else ""
        try:
            resp = await self._submit(
                symbol, signal.side, size.qty,
                strategy=signal.strategy, client_oid=client_oid,
            )
        except (AriaXAPIError, OrderRejectedError) as exc:
            await self._on_order_failure(symbol, signal, exc)
            return None

        result = self._parse_order_result(resp, symbol, signal.side, size.qty, price)
        if result.qty <= 0 or result.avg_price <= 0:
            await self._on_order_failure(
                symbol, signal,
                OrderRejectedError(symbol, "order response missing fill", resp),
            )
            return None

        # 6) Register the position.
        position = Position(
            id=f"pos_{uuid.uuid4().hex[:8]}",
            symbol=symbol, side=signal.side, strategy=signal.strategy,
            entry=result.avg_price, qty=result.qty,
            sl=signal.sl, tp1=signal.tp1, tp=signal.tp,
            opened_at=time.time(), atr_at_entry=signal.atr,
        )
        self._state.add_position(position)
        self._risk.mark_entry(symbol)
        self._risk.mark_daily_entry()
        self._failure_count.pop(symbol, None)
        await self._db.insert_trade(position)
        await self._tg.send(
            f"🎯 <b>{signal.side.upper()}</b> {signal.strategy}\n"
            f"{symbol} @ {result.avg_price:.4f}\n"
            f"Qty {result.qty} | ~${result.notional:.1f}\n🧪 AriaX Testnet",
            self._tg.menu(),
        )
        log.info("TRADE OPENED %s %s @ %.4f qty=%s", signal.side.upper(),
                 symbol, result.avg_price, result.qty)
        return position

    # ------------------------------------------------------------------
    # Closing
    # ------------------------------------------------------------------
    @staticmethod
    def _error_message(resp: Any) -> str:
        """Return an explicit error string from an API response, if any.

        The AriaX API answers HTTP 200 even for business rejections, and its
        error envelopes can still contain price-ish fields. The old parser
        fished a "price" out of such a body and booked a PHANTOM close with
        made-up PnL (the +$2.1/minute fake "TP" loop came from this).
        """
        for item in OrderExecutor._response_dicts(resp):
            err = str(item.get("error") or item.get("msg") or item.get("message") or "")
            if err and err.lower() not in ("ok", "success", "none", ""):
                if item.get("ok") is False or item.get("success") is False \
                        or item.get("retCode") not in (None, 0) \
                        or "error" in item:
                    return err
        return ""

    async def close(self, position: Position, reason: str) -> Optional[CloseResult]:
        """Close a position at market. Returns the realised result or None."""
        side = "sell" if position.side == "buy" else "buy"
        try:
            resp = await self._submit(position.symbol, side, position.qty)
        except AriaXAPIError as exc:
            if self._looks_ghost(str(exc)):
                await self.resolve_ghost(position, reason)
                return None
            self._state.record_error(f"close {position.symbol}: {exc}")
            return None

        err = self._error_message(resp)
        if err:
            # Business rejection with HTTP 200: treat exactly like an API error.
            if self._looks_ghost(err):
                await self.resolve_ghost(position, f"{reason}_ghostresp")
                return None
            self._state.record_error(f"close {position.symbol}: {err[:120]}")
            log.warning("CLOSE %s rejected by exchange: %s", position.symbol, err[:160])
            # Back off: without this the 2s watchdog would hammer the API.
            count = self._failure_count.get(position.symbol, 0) + 1
            self._failure_count[position.symbol] = count
            self._risk.mark_failure(position.symbol, count)
            return None

        fill = self._fill_price(resp, position.symbol, position.qty)
        if fill <= 0:
            # Some AriaX responses wrap fills in an envelope or acknowledge a
            # market close without returning a fill price. Never interpret a
            # missing price as zero: that records a near-100% notional loss.
            fill = await self._price(position.symbol)
        if fill <= 0:
            self._state.record_error(
                f"close {position.symbol}: exchange returned no fill price"
            )
            log.error("CLOSE %s acknowledged without a usable fill price", position.symbol)
            return None

        # ---- Close verification (v20.4) --------------------------------
        # If the exchange keeps reporting the position at (nearly) the same
        # qty after a "successful" close, the close did NOT really take
        # effect. Booking PnL anyway creates fake statistics and — far worse
        # — removing the local position re-triggers recovery every sync,
        # producing the recover→close→recover churn loop.
        if self._settings.close_verify_recheck_s > 0:
            verified, remote_qty = await self._verify_closed(position)
            if not verified:
                backoff = self._register_unverified_close(position, reason)
                if backoff:
                    return None
                # Position stays locally; watchdog retries after the backoff.
                return None

        raw_pnl = position.unrealized_pnl(fill)
        fees = abs(fill * position.qty) * self._settings.taker_fee * 2 \
            * self._settings.fee_buffer
        net = raw_pnl - fees
        hold = max(0.0, time.time() - position.opened_at)

        result = CloseResult(
            order_id=self._order_id(resp), symbol=position.symbol,
            qty=position.qty, avg_price=fill, realized_pnl=net, fees=fees,
            reason=reason, raw=resp or {},
        )
        await self._finalize_close(position, result, hold)
        return result

    # ------------------------------------------------------------------
    # Close verification helpers (v20.4)
    # ------------------------------------------------------------------
    async def _verify_closed(self, position: Position) -> Tuple[bool, float]:
        """Re-fetch remote positions; True when the symbol is gone/reduced.

        Tolerates the verification endpoint being unavailable: if the call
        fails we must NOT assume the close failed (that would double-sell).
        """
        if self._settings.paper_mode:
            held = self._paper_positions.get(position.symbol)
            return (held is None), (held or {}).get("qty", 0.0)
        try:
            data = await self._client.get_positions()
        except AriaXAPIError as exc:
            log.warning("close verify %s unavailable: %s (assuming closed)",
                        position.symbol, exc)
            return True, 0.0
        remote = {}
        try:
            from app.core.engine import QuantEngine  # local import: avoid cycle
            remote = QuantEngine._parse_remote_positions(data)
        except Exception:  # noqa: BLE001 - defensive
            return True, 0.0
        rpos = remote.get(position.symbol)
        if rpos is None:
            return True, 0.0
        if rpos["qty"] <= position.qty * 0.55:   # meaningfully reduced → OK
            return True, rpos["qty"]
        return False, rpos["qty"]

    def _register_unverified_close(self, position: Position, reason: str) -> bool:
        """Count consecutive unverified closes; space out retries, then flag stuck.

        Returns True when the caller should keep the local position and back
        off (always True here — kept as a hook for future policy).
        """
        sym = position.symbol
        count = self._unverified_closes.get(sym, 0) + 1
        self._unverified_closes[sym] = count
        wait = min(300.0 * (2 ** (count - 1)), self._settings.close_verify_recheck_s)
        # Reuse the risk failure backoff so the watchdog waits before retrying.
        self._risk.mark_failure(sym, count)
        log.warning("CLOSE %s #%d NOT verified remotely (still reported) — "
                    "retry in %.0fs", sym, count, wait)
        self._state.record_error(
            f"close {sym} unverified x{count}; remote still reports position")
        if count >= 3:
            self._mark_stuck(sym, reason)
        return True

    def _mark_stuck(self, symbol: str, reason: str) -> None:
        stuck = set(self._state.get("stuck_symbols", set()) or set())
        if symbol in stuck:
            return
        stuck.add(symbol)
        self._state.set("stuck_symbols", stuck)
        log.error("STUCK POSITION %s: exchange keeps reporting it after closes "
                  "(%s). Auto-close suspended; manual exchange-side check needed.",
                  symbol, reason)
        self._state.record_error(f"STUCK {symbol}: auto-close suspended ({reason})")
        try:
            asyncio.get_running_loop().create_task(
                self._tg.send(
                    f"⚠️ <b>STUCK POSITION</b> {symbol}\n"
                    f"Exchange keeps reporting it after {reason} closes.\n"
                    f"Auto-close suspended; check the exchange dashboard."
                )
            )
        except RuntimeError:
            pass

    # ------------------------------------------------------------------
    # Partial take-profit
    # ------------------------------------------------------------------
    async def partial_tp(self, position: Position) -> bool:
        """Sell half at TP1, move stop to break-even. Returns success."""
        meta = self._client.symbol_meta.get(position.symbol)
        half = quantize(position.qty / 2.0, meta.step if meta else 0.0)
        if half <= 0:
            return False
        side = "sell" if position.side == "buy" else "buy"
        try:
            resp = await self._submit(position.symbol, side, half)
        except AriaXAPIError as exc:
            self._state.record_error(f"partial {position.symbol}: {exc}")
            return False
        if self._error_message(resp):
            self._state.record_error(
                f"partial {position.symbol}: {self._error_message(resp)[:120]}")
            return False
        filled = self._fill_qty(resp, half)
        if filled <= 0:
            return False
        self._state.update_position(
            position.id,
            qty=max(0.0, position.qty - filled),
            is_partial=1,
            sl=position.entry,  # break-even stop after partial
        )
        await self._db.update_trade(
            position.id, max(0.0, position.qty - filled), position.entry, 1,
            position.highest_pnl_pct,
        )
        await self._tg.send(f"🔹 Partial TP {position.symbol} (half closed)")
        log.info("PARTIAL %s filled=%s", position.symbol, filled)
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _on_order_failure(self, symbol: str, signal: Signal, exc: Exception) -> None:
        count = self._failure_count.get(symbol, 0) + 1
        self._failure_count[symbol] = count
        self._risk.mark_failure(symbol, count)
        await self._db.log_decision(symbol, "rejected", signal.strategy,
                                    f"API: {str(exc)[:80]}")
        self._state.record_missed_signal(f"{symbol} {signal.strategy} -> {str(exc)[:80]}")
        self._state.record_error(f"EXECUTE {symbol}: {str(exc)[:80]}")
        await self._tg.send(f"❌ {symbol}\n{str(exc)[:140]}")

    async def _finalize_close(self, position: Position, result: CloseResult,
                              hold: float) -> None:
        self._state.remove_position(position.id)
        self._risk.mark_close(position.symbol)
        if position.strategy != "RealTest":
            await self._db.close_trade(position.id, result.realized_pnl,
                                       result.fees, result.reason, hold)
        self._state.bump_loss_streak(result.realized_pnl > 0)
        await self._db.update_analytics(self._state)
        await self._tg.send(
            f"{'🟢' if result.realized_pnl >= 0 else '🔴'} closed "
            f"({result.reason}) ${result.realized_pnl:.2f}",
            self._tg.menu(),
        )
        log.info("CLOSED %s %s pnl=%+.2f reason=%s", position.symbol,
                 position.side, result.realized_pnl, result.reason)

    async def resolve_ghost(self, position: Position, reason: str) -> None:
        """Position no longer exists remotely; clean up local state."""
        self._state.remove_position(position.id)
        self._unverified_closes.pop(position.symbol, None)
        if position.strategy != "RealTest":
            await self._db.close_trade(position.id, 0.0, 0.0,
                                       f"ghost_{reason}", 0.0)
        self._risk.mark_close(position.symbol)
        await self._db.update_analytics(self._state)

    # -- parsing --------------------------------------------------------
    @staticmethod
    def _order_id(resp: Any) -> str:
        """Extract an order identifier from flat or nested API responses."""
        for item in OrderExecutor._response_dicts(resp):
            value = item.get("orderId") or item.get("order_id") or item.get("oid")
            if value:
                return str(value)
        return ""

    @staticmethod
    def _response_dicts(resp: Any):
        """Yield nested response dictionaries without trusting one envelope shape."""
        queue = [resp]
        seen: set[int] = set()
        while queue:
            item = queue.pop(0)
            if id(item) in seen:
                continue
            seen.add(id(item))
            if isinstance(item, dict):
                yield item
                queue.extend(item.values())
            elif isinstance(item, list):
                queue.extend(item)

    @staticmethod
    def _fill_price(resp: Any, symbol: str, qty: float) -> float:
        """Extract a positive fill price from flat or nested API responses."""
        for item in OrderExecutor._response_dicts(resp):
            for key in ("avgPrice", "avg_price", "fillPrice", "fill_price",
                        "price", "entry", "entryPrice", "execPrice"):
                val = item.get(key)
                try:
                    if val is not None and float(val) > 0:
                        return float(val)
                except (TypeError, ValueError):
                    continue
        return 0.0

    @staticmethod
    def _fill_qty(resp: Any, fallback: float) -> float:
        """Extract executed quantity from flat or nested API responses."""
        for item in OrderExecutor._response_dicts(resp):
            for key in ("qty", "filled", "filledQty", "size", "executedQty",
                        "execQty", "cumExecQty"):
                val = item.get(key)
                try:
                    if val is not None and float(val) > 0:
                        return float(val)
                except (TypeError, ValueError):
                    continue
        return fallback

    def _parse_order_result(self, resp: Any, symbol: str, side: str,
                            qty: float, fallback_price: float) -> OrderResult:
        err = self._error_message(resp)
        if err:
            raise OrderRejectedError(symbol, err, resp)
        fill = self._fill_price(resp, symbol, qty) or fallback_price
        filled = self._fill_qty(resp, qty)
        log.info("ORDER RESULT %s %s fill=%s qty=%s raw=%s", side, symbol,
                 fill, filled, str(resp)[:200])
        return OrderResult(
            order_id=self._order_id(resp), symbol=symbol, side=side,
            qty=filled, avg_price=fill, raw=resp if isinstance(resp, dict) else {},
        )

    async def _funding_for(self, symbol: str) -> float:
        try:
            data = await self._client.get_markets()
        except AriaXAPIError:
            return 0.0
        item = find_market_item(data, symbol)
        if not isinstance(item, dict):
            return 0.0
        try:
            return float(item.get("funding") or item.get("fundingRate") or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _looks_ghost(message: str) -> bool:
        lowered = message.lower()
        # v20.4 live evidence: the AriaX matching engine answers closes of a
        # desynced /api/positions record with "qty exceeds position size"
        # (even for qty << size — the engine has no such position), and dust
        # closes with "notional below minimum … USD". Both mean the position
        # can never be closed via the API: resolve it as a ghost locally
        # (no PnL booked) and let the engine's stuck-position policy take over.
        return any(tag in lowered for tag in
                   ("not found", "no position", "already", "404",
                    "position not exist", "qty exceeds position size",
                    "notional below minimum"))