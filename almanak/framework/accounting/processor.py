"""AccountingProcessor — async outbox drain with typed event dispatch (VIB-3467).

Architecture:
  1. Execution hot path writes transaction_ledger + accounting_outbox, then fires
     asyncio.create_task(processor.drain_one(ledger_entry_id)).
  2. drain_one reads the outbox row, reads the ledger row, classifies the intent,
     dispatches to a category handler, and writes the accounting_events row.
  3. On runner startup, drain_pending() drains all pending/failed outbox rows so
     events written before a crash are not permanently lost.

Design constraints:
  - No live chain calls inside the processor.  All inputs come from the ledger row
    (extracted_data_json, price_inputs_json, pre_state_json, post_state_json).
  - Idempotent: drain_one on an already-processed row is a no-op — the outbox row
    is marked processed without re-writing the event or modifying the FIFO store.
  - The processor maintains its own FIFOBasisStore, reconstructed at startup via
    reconstruct_from_events() so REPAY / PT_REDEEM interest attribution is correct
    across restarts.

Idempotency (VIB-3478: _try_write_* legacy writers removed):
  - drain_one checks for an existing accounting_events row keyed on ledger_entry_id.
    If one exists the outbox row is marked processed without re-writing the event or
    modifying the FIFO store.
  - This guarantees the processor's FIFO store stays consistent with accounting_events
    regardless of restart timing.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from almanak.framework.accounting.classifier import AccountingCategory, classify
from almanak.framework.accounting.writer import AccountingWriter

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3


class AccountingProcessor:
    """Drains the accounting_outbox and writes typed accounting_events.

    Instantiated once per strategy runner, tied to a specific deployment_id.
    The FIFOBasisStore is shared with the runner so BORROW lot tracking is
    consistent between the legacy inline writers (during dual-write) and the
    processor path.
    """

    def __init__(
        self,
        state_manager: Any,
        basis_store: FIFOBasisStore,
        deployment_id: str = "",
    ) -> None:
        self._state_manager = state_manager
        self._basis_store = basis_store
        self._deployment_id = deployment_id
        self._writer = AccountingWriter(state_manager)

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    async def drain_one(self, ledger_entry_id: str) -> bool:
        """Process one outbox row end-to-end.

        Returns True when the row was processed (or was already done), False on
        unrecoverable failure (attempts >= MAX_RETRIES).

        Algorithm:
          1. Fetch outbox row by ledger_entry_id.
          2. Skip if already processed.
          3. Set status = 'processing'.
          4. Idempotency check: if accounting_events already has a row for this
             ledger_entry_id, mark processed and return (dual-write safe).
          5. Read ledger row.
          6. Classify intent.
          7. Dispatch to category handler → event (or None).
          8. Write event (if any).
          9. Mark outbox processed.

        Exceptions from category handlers or writer are caught; the row is
        marked 'failed' and retried up to _MAX_RETRIES times.
        """
        if not self._state_manager:
            return False

        try:
            outbox_row = await self._get_outbox_row(ledger_entry_id)
        except Exception:
            logger.warning("drain_one: failed to fetch outbox row for %s", ledger_entry_id, exc_info=True)
            return False

        if outbox_row is None:
            logger.debug("drain_one: no outbox row for ledger_entry_id=%s", ledger_entry_id)
            return False

        outbox_id = outbox_row["id"]
        status = outbox_row.get("status", "pending")

        if status == "processed":
            return True

        attempts = int(outbox_row.get("attempts", 0))
        if status == "failed" and attempts >= _MAX_RETRIES:
            logger.warning("drain_one: giving up on outbox row %s after %d attempts", outbox_id, attempts)
            return False

        # Mark as processing (prevents concurrent drain_one for the same row)
        await self._update_outbox(outbox_id, "processing")

        try:
            # Idempotency: if the legacy writers already wrote this event, skip.
            already_written = await self._has_accounting_event_for_ledger(ledger_entry_id)
            if already_written:
                await self._update_outbox(outbox_id, "processed")
                return True

            ledger_row = await self._get_ledger_row(ledger_entry_id)
            if ledger_row is None:
                raise ValueError(f"ledger row not found: {ledger_entry_id}")

            event = self._dispatch(outbox_row, ledger_row)

            if event is not None:
                ok = await self._writer.write(event)
                if not ok:
                    logger.debug(
                        "drain_one: writer.write returned False for %s (backend may not support write)",
                        ledger_entry_id,
                    )

            await self._update_outbox(outbox_id, "processed")
            return True

        except Exception as exc:
            new_attempts = attempts + 1
            logger.warning(
                "drain_one: error processing outbox row %s (attempt %d/%d): %s",
                outbox_id,
                new_attempts,
                _MAX_RETRIES,
                exc,
                exc_info=True,
            )
            await self._update_outbox(outbox_id, "failed", error=str(exc), attempts=new_attempts)
            return False

    async def drain_pending(self) -> int:
        """Drain all pending/failed outbox rows at startup.

        Processes rows sequentially to avoid flooding the state manager.
        Returns the count of rows successfully drained.
        """
        if not self._state_manager:
            return 0

        try:
            rows = await self._get_pending_rows()
        except Exception:
            logger.warning("drain_pending: failed to fetch pending rows", exc_info=True)
            return 0

        if not rows:
            return 0

        logger.info("drain_pending: found %d pending/failed outbox rows", len(rows))
        drained = 0
        for row in rows:
            ledger_entry_id = row.get("ledger_entry_id", "")
            if not ledger_entry_id:
                continue
            try:
                ok = await self.drain_one(ledger_entry_id)
                if ok:
                    drained += 1
            except Exception:
                logger.warning("drain_pending: uncaught error for ledger_entry_id=%s", ledger_entry_id, exc_info=True)
        if drained:
            logger.info("drain_pending: drained %d/%d rows", drained, len(rows))
        return drained

    # ──────────────────────────────────────────────────────────────────────────
    # Intent dispatch
    # ──────────────────────────────────────────────────────────────────────────

    def _dispatch(self, outbox_row: dict[str, Any], ledger_row: dict[str, Any]) -> Any:
        """Classify the intent and dispatch to the matching category handler."""
        from almanak.framework.accounting.category_handlers.lending_handler import handle_lending
        from almanak.framework.accounting.category_handlers.lp_handler import handle_lp
        from almanak.framework.accounting.category_handlers.pendle_handler import handle_pendle_lp, handle_pendle_pt
        from almanak.framework.accounting.category_handlers.perp_handler import handle_perp
        from almanak.framework.accounting.category_handlers.prediction_handler import handle_prediction
        from almanak.framework.accounting.category_handlers.swap_handler import handle_swap
        from almanak.framework.accounting.category_handlers.vault_handler import handle_vault

        intent_type = ledger_row.get("intent_type") or ""
        protocol = ledger_row.get("protocol") or ""
        token_out = ledger_row.get("token_out") or ""

        category = classify(intent_type, protocol, token_out)
        logger.debug("drain_one: intent_type=%s protocol=%s → category=%s", intent_type, protocol, category)

        if category == AccountingCategory.LENDING:
            return handle_lending(outbox_row, ledger_row, self._basis_store)
        if category == AccountingCategory.PENDLE_LP:
            return handle_pendle_lp(outbox_row, ledger_row)
        if category == AccountingCategory.PENDLE_PT:
            return handle_pendle_pt(outbox_row, ledger_row, self._basis_store)
        if category == AccountingCategory.LP:
            return handle_lp(outbox_row, ledger_row)
        if category == AccountingCategory.PERP:
            return handle_perp(outbox_row, ledger_row)
        if category == AccountingCategory.VAULT:
            return handle_vault(outbox_row, ledger_row)
        if category == AccountingCategory.SWAP:
            return handle_swap(outbox_row, ledger_row, self._basis_store)
        if category == AccountingCategory.PREDICTION:
            return handle_prediction(outbox_row, ledger_row, self._basis_store)
        # NO_ACCOUNTING — no event written
        return None

    # ──────────────────────────────────────────────────────────────────────────
    # State manager helpers
    # ──────────────────────────────────────────────────────────────────────────

    async def _get_outbox_row(self, ledger_entry_id: str) -> dict[str, Any] | None:
        if hasattr(self._state_manager, "get_outbox_by_ledger_id"):
            return await self._call_async(self._state_manager.get_outbox_by_ledger_id, ledger_entry_id)
        return None

    async def _update_outbox(self, outbox_id: str, status: str, error: str = "", attempts: int | None = None) -> None:
        if hasattr(self._state_manager, "update_outbox_entry"):
            await self._call_async(self._state_manager.update_outbox_entry, outbox_id, status, error, attempts)

    async def _has_accounting_event_for_ledger(self, ledger_entry_id: str) -> bool:
        if hasattr(self._state_manager, "has_accounting_events_for_ledger"):
            return bool(await self._call_async(self._state_manager.has_accounting_events_for_ledger, ledger_entry_id))
        return False

    async def _get_ledger_row(self, ledger_entry_id: str) -> dict[str, Any] | None:
        if hasattr(self._state_manager, "get_ledger_entry_by_id"):
            return await self._call_async(self._state_manager.get_ledger_entry_by_id, ledger_entry_id)
        return None

    async def _get_pending_rows(self) -> list[dict[str, Any]]:
        if hasattr(self._state_manager, "get_outbox_pending"):
            rows = await self._call_async(
                self._state_manager.get_outbox_pending,
                self._deployment_id,
                _MAX_RETRIES,
            )
            return rows or []
        return []

    @staticmethod
    async def _call_async(fn: Any, *args: Any) -> Any:
        """Call fn(*args), awaiting if it returns a coroutine."""
        result = fn(*args)
        if asyncio.iscoroutine(result):
            return await result
        return result


# ──────────────────────────────────────────────────────────────────────────────
# Outbox write helper (called from strategy_runner)
# ──────────────────────────────────────────────────────────────────────────────


async def write_outbox_entry(
    state_manager: Any,
    *,
    deployment_id: str,
    strategy_id: str,
    cycle_id: str,
    ledger_entry_id: str,
    intent_type: str,
    wallet_address: str,
    position_key: str = "",
    market_id: str = "",
) -> str | None:
    """Write a row to accounting_outbox and return the outbox row id.

    Best-effort: logs and returns None on all failures (including gateway not
    yet supporting save_outbox_entry — VIB-3482) so the execution path is not
    interrupted. No legacy _try_write_* fallback exists (removed in VIB-3478),
    so a None return means the accounting event will be lost until VIB-3482 ships.
    """
    if not state_manager or not ledger_entry_id:
        return None
    if not hasattr(state_manager, "save_outbox_entry"):
        return None
    now = datetime.now(UTC).isoformat()
    outbox_id = str(uuid.uuid4())
    try:
        fn = state_manager.save_outbox_entry
        result = fn(
            outbox_id,
            deployment_id,
            strategy_id,
            cycle_id,
            ledger_entry_id,
            intent_type,
            wallet_address,
            position_key,
            market_id,
            now,
        )
        if asyncio.iscoroutine(result):
            await result
        return outbox_id
    except NotImplementedError:
        # Re-raise so _write_outbox_and_fire_processor can distinguish
        # "backend not yet deployed" from a real write failure.
        raise
    except Exception as _e:
        from almanak.framework.state.exceptions import AccountingPersistenceError

        if isinstance(_e, AccountingPersistenceError):
            # Propagate with original error details so the runner can surface
            # the gRPC error code and server-side message to the operator.
            # Swallowing here would replace the cause with a generic synthetic message.
            raise
        logger.warning("write_outbox_entry: failed to persist outbox row", exc_info=True)
        return None
