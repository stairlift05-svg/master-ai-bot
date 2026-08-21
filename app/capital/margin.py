"""Capital management (#04): dual-wallet supervision and auto top-up.

The AriaX testnet exposes **two** wallets — spot and futures — and the engine
trades futures exclusively.  This module:

* Parses both wallet shapes the API has historically returned (the original
  v19.3 code read ``equity``/``free_margin`` in one place but
  ``futures.balances`` in another — an inconsistency that could report a
  "full" wallet while the futures wallet was empty).
* Automatically transfers spot funds to the futures wallet when free margin
  drops below a safety floor (self-healing, no human intervention).
"""
from __future__ import annotations

import logging
from typing import Any

from app.api.ariax_client import AriaXClient
from app.config import Settings
from app.errors import AriaXAPIError
from app.models import WalletState
from app.state import EngineState

log = logging.getLogger("quant.capital")

_MIN_AUTO_TRANSFER_USD = 10.0
_MAX_AUTO_TRANSFER_USD = 5000.0


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


class MarginManager:
    """Owns wallet state and guarantees futures margin availability."""

    def __init__(self, settings: Settings, client: AriaXClient,
                 state: EngineState) -> None:
        self._settings = settings
        self._client = client
        self._state = state
        self.wallet: WalletState = WalletState()

    # ------------------------------------------------------------------
    def parse_wallet(self, data: Any) -> WalletState:
        """Normalise any historical wallet payload shape into WalletState."""
        if not isinstance(data, dict) or not data.get("ok"):
            return WalletState()

        # Shape A: v2.1 dual-wallet {equity, free_margin, balances, futures}
        futures = data.get("futures") or {}
        spot_balances = data.get("balances") or {}
        spot_locks = data.get("locks") or {}
        fut_balances = futures.get("balances") or {}
        fut_locks = futures.get("locks") or {}

        spot_total = _num(spot_balances.get("USDT"))
        spot_locked = _num(spot_locks.get("USDT"))
        fut_total = _num(fut_balances.get("USDT"))
        fut_locked = _num(fut_locks.get("USDT"))

        equity = _num(data.get("equity") or data.get("balance") or 0)
        if equity <= 0:
            equity = spot_total + fut_total

        free_margin = _num(data.get("free_margin") or data.get("available_margin") or 0)
        futures_free = max(0.0, fut_total - fut_locked)

        return WalletState(
            equity=equity,
            spot_total=spot_total,
            spot_free=max(0.0, spot_total - spot_locked),
            spot_locked=spot_locked,
            futures_total=fut_total,
            futures_free=futures_free if futures_free > 0 else free_margin,
            futures_locked=fut_locked,
        )

    # ------------------------------------------------------------------
    async def refresh(self) -> WalletState:
        """Pull and cache the latest wallet state; raise on failure."""
        data = await self._client.get_wallet()
        self.wallet = self.parse_wallet(data)
        self._state.set_many(
            balance=self.wallet.equity,
            free_balance=self.wallet.free_for_trading,
        )
        return self.wallet

    # ------------------------------------------------------------------
    async def ensure_futures_margin(self, min_free: float) -> bool:
        """Top up the futures wallet from spot if it runs low.

        Returns True if a transfer was performed.
        """
        try:
            await self.refresh()
        except AriaXAPIError as exc:
            log.warning("wallet refresh failed: %s", exc)
            return False

        if self.wallet.futures_free >= min_free:
            return False
        if self.wallet.spot_free < min_free:
            log.warning("spot wallet also low (free=$%.2f); cannot top up",
                        self.wallet.spot_free)
            return False
        move = round(min(self.wallet.spot_free * 0.9, _MAX_AUTO_TRANSFER_USD), 2)
        if move < _MIN_AUTO_TRANSFER_USD:
            return False
        try:
            resp = await self._client.transfer_to_futures(move)
        except AriaXAPIError as exc:
            log.warning("auto transfer failed: %s", exc)
            return False
        ok = bool(resp.get("ok") if isinstance(resp, dict) else False)
        if ok:
            log.info("auto top-up spot->futures $%.2f", move)
        else:
            log.warning("auto top-up rejected: %s",
                        str(resp)[:120] if resp is not None else "no response")
        return ok

    # ------------------------------------------------------------------
    def topup_needed(self) -> bool:
        """True when the futures free margin is below the safety floor."""
        return self.wallet.futures_free < self._settings.min_free_margin * 4
