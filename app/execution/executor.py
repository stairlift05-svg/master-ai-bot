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

import logging
import time
import uuid
from typing import Any, Callable, Dict, Optional

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
        blocked, why_f = RiskManager.funding_blocked(signal.side, funding)
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
            resp = await self._client.place_order(
                symbol, signal.side, size.qty, lev=self._settings.leverage,
                order_type="market", strategy=signal.strategy,
                client_oid=client_oid,
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
            f"Qty {result.qty} | \~${result.notional:.1f}\n🧪 AriaX Testnet",
            self._tg.menu(),
        )
        log.info("TRADE OPENED %s %s @ %.4f qty=%s", signal.side.upper(),
                 symbol, result.avg_price, result.qty)
        return position

    # ------------------------------------------------------------------
    # Closing
    # ------------------------------------------------------------------
    async def close(self, position: Position, reason: str) -> Optional[CloseResult]:
        """Close a position at market. Returns the realised result or None."""
        side = "sell" if position.side == "buy" else "buy"
        try:
            resp = await self._client.place_order(
                position.symbol, side, position.qty, lev=self._settings.leverage,
                order_type="market",
            )
        except AriaXAPIError as exc:
            if self._looks_ghost(str(exc)):
                await self.resolve_ghost(position, reason)
                return None
            self._state.record_error(f"close {position.symbol}: {exc}")
            return None

        fill = self._fill_price(resp, position.symbol, position.qty)
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
            resp = await self._client.place_order(
                position.symbol, side, half, lev=self._settings.leverage,
                order_type="market",
            )
        except AriaXAPIError as exc:
            self._state.record_error(f"partial {position.symbol}: {exc}")
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
        if position.strategy != "RealTest":
            await self._db.close_trade(position.id, 0.0, 0.0,
                                       f"ghost_{reason}", 0.0)
        self._risk.mark_close(position.symbol)
        await self._db.update_analytics(self._state)

    # -- parsing --------------------------------------------------------
    @staticmethod
    def _order_id(resp: Any) -> str:
        if isinstance(resp, dict):
            return str(resp.get("orderId") or resp.get("id") or resp.get("oid") or "")
        return ""

    @staticmethod
    def _fill_price(resp: Any, symbol: str, qty: float) -> float:
        if isinstance(resp, dict):
            for key in ("avgPrice", "fill_price", "price", "entry", "entryPrice"):
                val = resp.get(key)
                try:
                    if val is not None and float(val) > 0:
                        return float(val)
                except (TypeError, ValueError):
                    continue
        return 0.0

    @staticmethod
    def _fill_qty(resp: Any, fallback: float) -> float:
        if isinstance(resp, dict):
            for key in ("qty", "filled", "size", "executedQty"):
                val = resp.get(key)
                try:
                    if val is not None and float(val) > 0:
                        return float(val)
                except (TypeError, ValueError):
                    continue
        return fallback

    def _parse_order_result(self, resp: Any, symbol: str, side: str,
                            qty: float, fallback_price: float) -> OrderResult:
        err = ""
        if isinstance(resp, dict):
            err = str(resp.get("error") or resp.get("msg") or resp.get("message") or "")
            if err.lower() not in ("ok", "success", "none", "") and (
                resp.get("ok") is False or resp.get("success") is False
            ):
                raise OrderRejectedError(symbol, err, resp)
        fill = self._fill_price(resp, symbol, qty) or fallback_price
        filled = self._fill_qty(resp, qty)
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
        return any(tag in lowered for tag in
                   ("not found", "no position", "already", "404", "position not exist"))