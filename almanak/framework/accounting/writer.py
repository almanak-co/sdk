"""AccountingWriter — single write path for all typed accounting events.

Usage in strategy_runner.py (after ledger save):
    writer = AccountingWriter(state_manager)
    await writer.write(lending_event)

The writer is the only code that touches the accounting_events table.

Augmentation chokepoints (single source of truth):
    Both backends — :func:`SQLiteStore.save_accounting_event` and
    :func:`GatewayStateManager.save_accounting_event` — call
    :func:`augment_accounting_payload` immediately before serialising the
    payload. The writer delegates and never mutates the event instance.
    This module deliberately does *not* monkey-patch ``event.to_payload_json``.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from almanak.framework.accounting.lp_accounting import LPAccountingEvent
from almanak.framework.accounting.models import (
    AccountingConfidence,
    LendingAccountingEvent,
    PendleAccountingEvent,
    PredictionAccountingEvent,
    SwapAccountingEvent,
    TransferAccountingEvent,
)
from almanak.framework.accounting.payload_schemas import (
    FORMULA_VERSION,
    SCHEMA_VERSION,
)
from almanak.framework.accounting.perp_accounting import PerpAccountingEvent
from almanak.framework.accounting.policy import MatchingPolicy, PrimitiveVersion
from almanak.framework.accounting.vault_accounting import VaultAccountingEvent
from almanak.framework.primitives.taxonomy import (
    UnknownIntentTypeError,
    record_for,
)
from almanak.framework.primitives.types import Primitive
from almanak.framework.state.exceptions import (
    AccountingPersistenceError,
    AccountingWriteKind,
)

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
    | SwapAccountingEvent
    | TransferAccountingEvent
)


def augment_accounting_payload(payload_json: str, *, is_live: bool) -> str:
    """Augment an accounting-event payload with the G13/L1/L4 contract.

    Single-point augmentation called by both state backends
    (:meth:`SQLiteStore.save_accounting_event` and
    :meth:`GatewayStateManager.save_accounting_event`) so every accounting
    row ends up with:

    * ``schema_version`` / ``formula_version`` / ``matching_policy_version`` /
      ``primitive_version`` (G13: lot-matching policy declared + versioned;
      VIB-4166: per-primitive contract version stamped before the upcoming
      primitives — Bridge dest-leg, CDP, Liquidate, Vault-async, Claim — land
      so we never need to backfill a missing version across millions of rows).
    * ``principal_repaid_usd`` / ``interest_paid_usd`` projection from
      ``principal_delta_usd`` / ``interest_delta_usd`` for REPAY (L4).
    * ``interest_accrued_usd`` projection from ``interest_delta_usd`` for
      WITHDRAW (L1).

    Failure contract — VIB-3863:

    * **Live mode** (``is_live=True``): malformed JSON or non-dict payloads
      raise :class:`AccountingPersistenceError`. A ``to_payload_json()``
      that emits a non-dict or unparsable string is a bug in the event
      class — silently dropping the version stamps and lending aliases
      would let unaudited rows reach the production books.
    * **Non-live mode** (``is_live=False``): the same conditions log ERROR
      and return the original payload unchanged so paper / dry-run / backtest
      runs do not halt on schema bugs.
    """
    try:
        d = json.loads(payload_json)
    except (json.JSONDecodeError, TypeError) as exc:
        msg = (
            f"augment_accounting_payload: payload_json is not valid JSON "
            f"(type={type(payload_json).__name__}, len={len(payload_json) if isinstance(payload_json, str) else 'n/a'}): {exc}"
        )
        if is_live:
            raise AccountingPersistenceError(
                AccountingWriteKind.ACCOUNTING,
                message=msg,
                cause=exc,
            ) from exc
        logger.error("%s — non-live mode, returning original payload unchanged", msg)
        return payload_json

    if not isinstance(d, dict):
        msg = f"augment_accounting_payload: payload_json must decode to dict, got {type(d).__name__}"
        if is_live:
            raise AccountingPersistenceError(
                AccountingWriteKind.ACCOUNTING,
                message=msg,
            )
        logger.error("%s — non-live mode, returning original payload unchanged", msg)
        return payload_json

    # VIB-4162 (T2) + VIB-4195 (T09) + VIB-4166 (T6): per-primitive version
    # stamping via typed accessors. Look up the primitive for this event_type
    # via the canonical taxonomy ONCE; both `matching_policy_version` AND
    # `primitive_version` are resolved from the same primitive, so a
    # fallback (paper-mode unknown / missing event_type) lands the same
    # fallback primitive in BOTH lookups and an asymmetric stamp is
    # impossible. In live mode an unknown OR missing event_type raises
    # (preserves VIB-3863's mode-aware contract); in non-live the writer
    # logs ERROR and falls back to Primitive.UTILITY for both versions.
    event_type = d.get("event_type")
    if not isinstance(event_type, str) or not event_type:
        msg = "augment_accounting_payload: missing or invalid event_type; cannot resolve per-primitive version stamps"
        if is_live:
            raise AccountingPersistenceError(
                AccountingWriteKind.ACCOUNTING,
                message=msg,
            )
        logger.error(
            "%s — non-live mode, falling back to Primitive.UTILITY for both versions",
            msg,
        )
        primitive = Primitive.UTILITY
    else:
        try:
            primitive = record_for(event_type).primitive
        except UnknownIntentTypeError as exc:
            msg = (
                f"augment_accounting_payload: event_type {event_type!r} has no "
                f"taxonomy row; cannot resolve per-primitive version stamps"
            )
            if is_live:
                raise AccountingPersistenceError(
                    AccountingWriteKind.ACCOUNTING,
                    message=msg,
                    cause=exc,
                ) from exc
            logger.error(
                "%s — non-live mode, falling back to Primitive.UTILITY for both versions",
                msg,
            )
            primitive = Primitive.UTILITY

    d["schema_version"] = SCHEMA_VERSION
    d["formula_version"] = FORMULA_VERSION
    # Both per-primitive stamps land here, against the same `d` and from the
    # same `primitive`, through parallel typed accessors so a future bump
    # propagates uniformly. The AST guard
    # `test_augment_stamps_both_versions_at_every_per_primitive_lookup_site`
    # asserts these two assignments cannot drift apart on a future code path.
    d["matching_policy_version"] = MatchingPolicy.for_primitive(primitive)
    d["primitive_version"] = PrimitiveVersion.for_primitive(primitive)
    _project_lending_aliases(d)
    return json.dumps(d)


def _project_lending_aliases(d: dict[str, Any]) -> None:
    """Project lending payload fields onto Accounting-AttemptNo17 spec names
    so L1 / L4 cells can read them.

    The :class:`LendingAccountingEvent` was authored with
    ``principal_delta_usd`` / ``interest_delta_usd``. The Accountant Test
    §1.2 L4 spec names are ``principal_repaid_usd`` / ``interest_paid_usd``
    (REPAY) and ``interest_accrued_usd`` (WITHDRAW). Rather than rename a
    stable payload schema (which would break every downstream consumer),
    the writer projects the existing fields to the spec names — both shapes
    coexist in the row, and post-hoc auditors can grep either.

    The projection is intent-type aware:

    * REPAY / DELEVERAGE → ``principal_repaid_usd`` ← ``principal_delta_usd``,
      ``interest_paid_usd`` ← ``interest_delta_usd``
    * WITHDRAW → ``interest_accrued_usd`` ← ``interest_delta_usd``
      (supply-side interest accrued before unlock)
    * Other event types pass through unchanged.

    Existing aliases on the payload (if any) win — the projection only
    fills missing keys so a future event that natively writes the spec
    names is not stomped.
    """
    et = d.get("event_type")
    if not isinstance(et, str):
        return

    principal_delta = d.get("principal_delta_usd")
    interest_delta = d.get("interest_delta_usd")

    if et in ("REPAY", "DELEVERAGE"):
        if "principal_repaid_usd" not in d and principal_delta is not None:
            d["principal_repaid_usd"] = principal_delta
        if "interest_paid_usd" not in d and interest_delta is not None:
            d["interest_paid_usd"] = interest_delta
    elif et == "WITHDRAW":
        if "interest_accrued_usd" not in d and interest_delta is not None:
            d["interest_accrued_usd"] = interest_delta


class AccountingWriter:
    """Single delegating writer for typed accounting events.

    The writer's only jobs are mode-aware error semantics and routing to
    the underlying store. Payload augmentation (G13 version stamps + L1/L4
    lending aliases) is the store's responsibility — see
    :func:`augment_accounting_payload`. This division keeps a single
    augmentation chokepoint per backend and avoids any lifetime mutation
    of the in-memory event instance.
    """

    def __init__(self, store: Any) -> None:
        self._store = store

    async def write(self, event: AccountingEvent) -> bool:
        """Persist a typed accounting event to the accounting_events store.

        Fail-closed in LIVE mode: a missing or broken store raises so the
        runner halts with ACCOUNTING_FAILED rather than silently dropping
        the record. In non-live modes, errors are logged and ``False`` is
        returned so the loop continues.
        """
        is_live = event.identity.execution_mode == "live"
        if not hasattr(self._store, "save_accounting_event"):
            msg = (
                f"Store {type(self._store).__name__} does not support "
                f"save_accounting_event; accounting event would be silently dropped"
            )
            if is_live:
                raise AccountingPersistenceError(
                    AccountingWriteKind.ACCOUNTING,
                    strategy_id=getattr(event.identity, "strategy_id", ""),
                    message=msg,
                )
            logger.warning(msg)
            return False
        try:
            return await self._store.save_accounting_event(event)
        except AccountingPersistenceError:
            # Already typed — propagate untouched in live mode, swallow with
            # ERROR log in non-live (the backend's mode-aware augment raised
            # a typed error; we honour its semantics rather than re-wrap).
            if is_live:
                raise
            logger.error("AccountingWriter.write failed (non-live)", exc_info=True)
            return False
        except Exception as exc:
            logger.error("AccountingWriter.write failed", exc_info=True)
            if is_live:
                raise AccountingPersistenceError(
                    AccountingWriteKind.ACCOUNTING,
                    strategy_id=getattr(event.identity, "strategy_id", ""),
                    cause=exc,
                ) from exc
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
