"""AccountingWriter — single write path for all typed accounting events.

Usage in strategy_runner.py (after ledger save):
    writer = AccountingWriter(state_manager)
    await writer.write(lending_event)

The writer is the only code that touches the accounting_events table.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from almanak.framework.accounting.lp_accounting import LPAccountingEvent
from almanak.framework.accounting.models import (
    AccountingConfidence,
    LendingAccountingEvent,
    PendleAccountingEvent,
    PredictionAccountingEvent,
)
from almanak.framework.accounting.perp_accounting import PerpAccountingEvent
from almanak.framework.accounting.vault_accounting import VaultAccountingEvent

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

AccountingEvent = (
    LendingAccountingEvent
    | PendleAccountingEvent
    | LPAccountingEvent
    | PerpAccountingEvent
    | VaultAccountingEvent
    | PredictionAccountingEvent
)


class AccountingWriter:
    def __init__(self, store: Any) -> None:
        self._store = store

    async def write(self, event: AccountingEvent) -> bool:
        """Persist a typed accounting event to the accounting_events store.

        Fail-closed in LIVE mode: a missing or broken store raises rather than
        silently dropping the accounting record. In non-live modes, errors are
        logged and False is returned so the loop continues.
        """
        is_live = event.identity.execution_mode == "live"
        if not hasattr(self._store, "save_accounting_event"):
            msg = (
                f"Store {type(self._store).__name__} does not support save_accounting_event; "
                "accounting event would be silently dropped"
            )
            if is_live:
                raise RuntimeError(msg)
            logger.warning(msg)
            return False
        try:
            return await self._store.save_accounting_event(event)
        except Exception:
            logger.error("AccountingWriter.write failed", exc_info=True)
            if is_live:
                raise
            return False

    def make_unavailable_lending_event(
        self,
        identity: Any,
        event_type: Any,
        position_key: str,
        market_id: str,
        asset: str,
        reason: str,
    ) -> LendingAccountingEvent:
        from almanak.framework.accounting.models import LendingAccountingEvent

        return LendingAccountingEvent(
            identity=identity,
            event_type=event_type,
            position_key=position_key,
            market_id=market_id,
            asset=asset,
            collateral_value_before_usd=None,
            collateral_value_after_usd=None,
            debt_value_before_usd=None,
            debt_value_after_usd=None,
            net_equity_before_usd=None,
            net_equity_after_usd=None,
            health_factor_before=None,
            health_factor_after=None,
            liquidation_threshold=None,
            lltv=None,
            supply_apr_bps=None,
            borrow_apr_bps=None,
            principal_delta_usd=None,
            interest_delta_usd=None,
            gas_usd=None,
            confidence=AccountingConfidence.UNAVAILABLE,
            unavailable_reason=reason,
        )
