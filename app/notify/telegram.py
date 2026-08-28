"""Telegram notification controller.

Design: the controller owns *zero* trading logic.  The engine injects
callbacks (``dashboard``, ``positions``, ``sync``, ``report``, ...) so this
module can never create import cycles and stays trivially testable.

Key properties:

* One reusable ``aiohttp.ClientSession`` (v19.3 opened a session per message).
* HTML escaping of user-derived text to avoid Telegram parse-mode errors.
* Long-polling ``getUpdates`` with callback-query handling (inline buttons).
* Graceful degradation when ``TELEGRAM_BOT_TOKEN`` is unset (silent no-op).
"""
from __future__ import annotations

import asyncio
import html
import logging
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp

from app.config import Settings
from app.state import EngineState

log = logging.getLogger("quant.telegram")


# ---------------------------------------------------------------------------
# v22: rich trade messages. Pure functions so they are unit-testable and
# identical for paper and live (only the mode tag differs).
# ---------------------------------------------------------------------------

def _fmt_hold(seconds: float) -> str:
    s = max(0, int(seconds))
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, _ = divmod(s, 60)
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


def _pct(diff: float, base: float) -> str:
    return f"{(diff / base) * 100.0:+.2f}%" if base > 0 else "n/a"


def _mode_tag(paper: bool) -> str:
    return "📝 PAPER" if paper else "💵 LIVE"


def format_open_message(*, symbol: str, side: str, strategy: str,
                        entry: float, qty: float, sl: float, tp: float,
                        paper: bool, reason: str = "") -> str:
    """The message sent when a position opens (v22)."""
    notional = qty * entry
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    rr = f"{reward / risk:.1f}" if risk > 0 else "n/a"
    lines = [
        f"🎯 OPEN {side.upper()} • {strategy} [{_mode_tag(paper)}]",
        f"{symbol} @ {entry:.6g}",
        f"Qty {qty:.6g} (~${notional:.1f})",
        f"🛑 SL {sl:.6g} ({_pct(sl - entry, entry)}) | "
        f"🎯 TP {tp:.6g} ({_pct(tp - entry, entry)})",
        f"R:R 1:{rr}",
    ]
    if reason:
        lines.append(f"ℹ️ {reason}")
    return "\n".join(lines)


def format_close_message(*, symbol: str, side: str, entry: float,
                         exit_price: float, qty: float, pnl: float,
                         fees: float, reason: str, hold_s: float,
                         paper: bool, balance: float = None) -> str:
    """The message sent when a position closes (v22)."""
    emoji = "🟢" if pnl >= 0 else "🔴"
    invested = entry * qty
    lines = [
        f"{emoji} CLOSE {side.upper()} • {symbol} [{_mode_tag(paper)}]",
        f"Entry {entry:.6g} → Exit {exit_price:.6g} ({_pct(exit_price - entry, entry)})",
        f"💵 PnL {pnl:+.2f}$"
        + (f" ({pnl / invested * 100.0:+.2f}% of ${invested:.0f})" if invested > 0 else ""),
        f"⚖️ fees ${fees:.2f} | 📎 {reason} | ⏱ held {_fmt_hold(hold_s)}",
    ]
    if balance is not None:
        lines.append(f"💰 balance ${balance:.2f}")
    return "\n".join(lines)


class TelegramController:
    """Telegram bot front-end (commands + push notifications)."""

    def __init__(self, settings: Settings, state: EngineState,
                 callbacks: Dict[str, Callable[..., Awaitable[Any]]]) -> None:
        self._settings = settings
        self._state = state
        self._callbacks = callbacks
        self._token = settings.tg_token
        self._chat_id = settings.tg_chat_id
        self._base = f"https://api.telegram.org/bot{self._token}" if self._token else ""
        self._offset = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self._menu_rows: list = []

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------
    @staticmethod
    def escape(text: Any) -> str:
        return html.escape(str(text))

    def menu(self) -> Dict[str, Any]:
        """Inline-keyboard menu (built from current state each time)."""
        st = self._state.snapshot()
        btn_text = "⏸️ Pause" if st["is_active"] else "▶️ Start"
        btn_cmd = "cmd_pause" if st["is_active"] else "cmd_start"
        rows = [
            [{"text": "📊 Dashboard", "callback_data": "cmd_dash"},
             {"text": "💼 Positions", "callback_data": "cmd_pos"}],
            [{"text": "🔄 Sync", "callback_data": "cmd_sync"},
             {"text": btn_text, "callback_data": btn_cmd}],
            [{"text": "📄 Report", "callback_data": "cmd_txt"},
             {"text": "🚫 Rejections", "callback_data": "cmd_rej"}],
            [{"text": "⚡ REAL TEST (real $)", "callback_data": "cmd_realtest"}],
        ]
        for pos in list(st["active_positions"].values())[:5]:
            rows.append([{
                "text": f"❌ Close {pos['symbol']} {pos['side'].upper()}",
                "callback_data": f"close_{pos['id']}",
            }])
        return {"inline_keyboard": rows}

    async def send(self, text: str, markup: Optional[Dict] = None) -> None:
        if not self._token:
            return
        text = self.escape(text) if not text.startswith(("<b>", "🎯")) else text
        if len(text) > 4000:
            text = text[:3900] + "\n..."
        payload: Dict[str, Any] = {
            "chat_id": self._chat_id, "text": text, "parse_mode": "HTML",
        }
        if markup:
            payload["reply_markup"] = markup
        try:
            session = await self._get_session()
            await session.post(f"{self._base}/sendMessage", json=payload, timeout=12)
        except Exception as exc:  # noqa: BLE001 - notifications must never crash
            log.error("TG send: %s", exc)

    async def send_document(self, path: str, caption: str = "") -> None:
        if not self._token:
            return
        try:
            import os
            if not os.path.exists(path):
                return
            form = aiohttp.FormData()
            form.add_field("chat_id", self._chat_id)
            form.add_field("caption", caption)
            form.add_field("document", open(path, "rb"),
                           filename=os.path.basename(path))
            session = await self._get_session()
            await session.post(f"{self._base}/sendDocument", data=form, timeout=60)
        except Exception as exc:  # noqa: BLE001
            log.error("TG doc: %s", exc)

    # ------------------------------------------------------------------
    # Polling loop
    # ------------------------------------------------------------------
    async def run(self) -> None:
        """Startup message + long-poll loop (never returns)."""
        if not self._token:
            log.warning(
                "Telegram notifications DISABLED — set TELEGRAM_BOT_TOKEN and "
                "TELEGRAM_CHAT_ID (Render -> Environment) to receive trade "
                "messages. The engine runs normally without them.")
            while True:
                await asyncio.sleep(60)
        await self.send(
            f"🚀 <b>Quant v22 AriaX (Professional)</b>\n"
            f"Base: {self._settings.arlax_base}\n"
            f"Candles: AriaX → Bybit → OKX → Binance\n"
            f"Risk {self._settings.risk_pct}% | Lev {self._settings.leverage}x | "
            f"Max ${self._settings.max_notional_usd:.0f}",
            self.menu(),
        )
        while True:
            try:
                await self._poll_once()
            except Exception as exc:  # noqa: BLE001
                log.error("TG poll: %s", exc)
            await asyncio.sleep(1)

    async def _poll_once(self) -> None:
        session = await self._get_session()
        url = f"{self._base}/getUpdates?offset={self._offset + 1}&timeout=8"
        async with session.get(url, timeout=12) as resp:
            data = await resp.json()
        for update in data.get("result", []):
            self._offset = update["update_id"]
            if "callback_query" not in update:
                continue
            cb = update["callback_query"]
            data_cb = cb.get("data", "")
            await self._ack(cb["id"])
            await self._handle_callback(data_cb)

    async def _ack(self, callback_id: str) -> None:
        try:
            session = await self._get_session()
            await session.post(
                f"{self._base}/answerCallbackQuery",
                json={"callback_query_id": callback_id, "text": "OK"}, timeout=4,
            )
        except Exception:  # pragma: no cover
            pass

    async def _handle_callback(self, data: str) -> None:
        if data.startswith("close_"):
            pid = data[len("close_"):]
            if "on_close" in self._callbacks:
                await self._callbacks["on_close"](pid)
            await self.send("✅ Close sent", self.menu())
            return
        handler = self._callbacks.get(data)
        if handler is not None:
            try:
                await handler()
            except Exception as exc:  # noqa: BLE001
                log.error("TG callback %s: %s", data, exc)
                await self.send(f"❌ {data}: {exc}")
