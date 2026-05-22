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
from collections.abc import Callable
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
from almanak.framework.accounting.position_reference import (
    build_legacy_position_reference,
    build_registry_position_reference,
)
from almanak.framework.accounting.vault_accounting import VaultAccountingEvent
from almanak.framework.primitives.taxonomy import (
    UnknownIntentTypeError,
    primitive_for,
    record_for,
)
from almanak.framework.primitives.types import EventKind, Primitive, PrimitiveRecord
from almanak.framework.state.exceptions import (
    AccountingPersistenceError,
    AccountingWriteKind,
)

if TYPE_CHECKING:
    from almanak.framework.accounting.position_reference import PositionReference

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

# VIB-4278: registry-lookup callable shape passed by the SQLite state backend
# so the augment chokepoint can stamp ``source="registry"`` on the
# ``position_reference`` shape when ``position_registry`` has a matching row.
# The chokepoint itself never touches the DB; the callable lives in the state
# backend that owns the connection (per VIB-3862's "writer must not mutate
# event instance" rule and CLAUDE.md's database-schema-ownership rule).
#
# Contract:
#
# * Input args: ``(primitive, event_kind, accounting_category)`` where
#   ``primitive`` is the canonical ``Primitive`` enum **value** (lowercase
#   StrEnum, e.g. ``"lp"``), ``event_kind`` is the ``EventKind`` enum
#   **value** (``"open"`` / ``"close"``), and ``accounting_category`` is
#   the canonical ``AccountingCategory`` enum **value** (e.g. ``"lp"``,
#   ``"pendle_lp"``). The chokepoint resolves all three through
#   ``record_for(event_type)`` before calling. ``accounting_category``
#   was added in PR #2236 round 2: multiple AccountingCategory values
#   can share the same Primitive (UniV3 ``"lp"`` and Pendle ``"pendle_lp"``
#   both have ``Primitive="lp"``), so a lookup keyed only on
#   ``(deployment_id, chain, primitive, tx_hash)`` can legitimately
#   return rows from a different category when a single tx opens
#   positions across categories — threading ``accounting_category``
#   disambiguates the join. All other lookup context (deployment_id,
#   chain, tx_hash) is closed over by the state backend when it builds
#   the callable — the chokepoint stays pure.
# * Returns: ``dict`` row when a matching ``position_registry`` row is
#   found, ``None`` otherwise. The row dict MUST carry at minimum
#   ``physical_identity_hash`` / ``semantic_grouping_key`` /
#   ``grouping_policy_version`` / ``handle`` /
#   ``matching_policy_version``. The chokepoint never inspects extras.
# * MUST NOT raise on "no match" (registry-mode is opt-in per blueprint 28
#   §5; legacy primitives have no registry rows). It MAY raise on actual
#   DB errors; the chokepoint then honors the mode-aware contract (live
#   raises, paper logs ERROR + falls back to legacy).
RegistryLookup = Callable[[str, str, str], dict | None]


def augment_accounting_payload(
    payload_json: str,
    *,
    is_live: bool,
    registry_lookup: RegistryLookup | None = None,
) -> str:
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

    VIB-4278 — registry-lookup hook
    -------------------------------

    The optional ``registry_lookup`` callable lets the SQLite state backend
    stamp ``source="registry"`` (with the registry row's
    ``physical_identity_hash`` / ``semantic_grouping_key`` / ``handle`` /
    ``grouping_policy_version`` / ``matching_policy_version``) instead of the
    Day-1 legacy reference for OPEN/CLOSE events. The chokepoint stays a
    pure function: it never touches the DB; the callable is constructed by
    the state backend that owns the connection and closes over the lookup
    context (deployment_id, chain, tx_hash). See ``RegistryLookup`` above for
    the contract.

    When ``registry_lookup`` is omitted (default ``None``), or returns
    ``None`` (registry row not yet recorded for this event), the chokepoint
    falls back to ``build_legacy_position_reference`` unchanged. This means
    legacy primitives keep emitting ``source="legacy"`` and the
    hosted-Postgres path (T19 / VIB-4205 not yet shipped) continues to emit
    ``source="legacy"`` without behavioural drift. A registry-mode cutover
    flips a primitive's events to ``source="registry"`` once the state
    backend's lookup wiring is in place AND the primitive's atomic
    save_ledger_and_registry path lands the row before the augment runs.
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
    #
    # VIB-4196 (T10): we ALSO need the full PrimitiveRecord on the success
    # path (not just the primitive) so we can stamp `position_reference` on
    # OPEN/CLOSE rows. The fallback path stamps version pairs against
    # Primitive.UTILITY (preserving F5.4) but does NOT emit a
    # `position_reference` — UTILITY has no event_kind, and inventing an
    # OPEN/CLOSE shape on an unknown event_type would silently land
    # malformed pointers in the DB.
    record: PrimitiveRecord | None = None
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
            record = record_for(event_type)
            primitive = record.primitive
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
            record = None
        else:
            # VIB-4477: protocol-aware refinement for parallel LP version
            # streams (V3 vs V4). The plain ``record.primitive`` is
            # ``Primitive.LP`` for every LP event_type regardless of venue;
            # ``primitive_for`` overrides to ``Primitive.LP_V4`` when the
            # payload's protocol is ``uniswap_v4`` so V4 rows stamp from
            # ``PRIMITIVE_VERSIONS[Primitive.LP_V4]`` while V3 / Aerodrome /
            # TraderJoe / Curve continue to stamp from
            # ``PRIMITIVE_VERSIONS[Primitive.LP]``. Other primitives are
            # untouched by the override.
            protocol = d.get("protocol")
            if isinstance(protocol, str) and protocol:
                primitive = primitive_for(event_type, protocol)

    d["schema_version"] = SCHEMA_VERSION
    d["formula_version"] = FORMULA_VERSION
    # Both per-primitive stamps land here, against the same `d` and from the
    # same `primitive`, through parallel typed accessors so a future bump
    # propagates uniformly. The AST guard
    # `test_augment_stamps_both_versions_at_every_per_primitive_lookup_site`
    # asserts these two assignments cannot drift apart on a future code path.
    d["matching_policy_version"] = MatchingPolicy.for_primitive(primitive)
    d["primitive_version"] = PrimitiveVersion.for_primitive(primitive)

    # VIB-4196 (T10): stamp `position_reference` on every OPEN/CLOSE row.
    # Day-1 source = "legacy" for ALL primitives. Cutover tickets (T12 /
    # T16 / T23 / T28) flip per-primitive to "receipt" / "registry" gated
    # by their respective parser audits (T02). Non-OPEN/CLOSE event_kinds
    # (ADJUST, COLLECT, TRANSFER, NONE) leave the column NULL — there is
    # no position lifecycle to point at. The fallback path (record is None)
    # also leaves it NULL: an unknown event_type cannot derive an
    # event_kind, so we cannot guarantee OPEN/CLOSE semantics for it.
    #
    # Anti-smuggling: ALWAYS strip any pre-existing `position_reference` key
    # before deciding whether to emit one ourselves. A connector / category
    # handler that fabricates `{"position_reference": ...}` in
    # `to_payload_json()` would otherwise have its smuggled value survive
    # the augment chokepoint on the non-OPEN/CLOSE / unknown-event_type
    # branches (CodeRabbit on PR #2211 — the writer chokepoint MUST be the
    # only construction site). The `pop` makes the chokepoint exclusive
    # regardless of which branch we take below.
    d.pop("position_reference", None)
    if record is not None and record.event_kind in (EventKind.OPEN, EventKind.CLOSE):
        d["position_reference"] = _resolve_position_reference(
            record,
            registry_lookup=registry_lookup,
            is_live=is_live,
        ).to_dict()

    _project_lending_aliases(d)
    # ``sort_keys=True``: the augmented payload is the canonical bytes that
    # land in ``payload_json`` (Postgres) and feed the SQLite ``position_reference``
    # column extractor. Two equal augmentations must serialize byte-identical
    # so downstream auditors can dedup on payload-checksum equality. The
    # `_extract_position_reference_column` helper and `PositionReference.to_dict`
    # docstring both depend on this invariant — see VIB-4196 / T10.
    return json.dumps(d, sort_keys=True)


def _resolve_position_reference(
    record: PrimitiveRecord,
    *,
    registry_lookup: RegistryLookup | None,
    is_live: bool,
) -> PositionReference:
    """Resolve the ``position_reference`` for an OPEN/CLOSE event.

    VIB-4278 — sits between the augment chokepoint and the two construction
    sites (``build_legacy_position_reference`` and
    ``build_registry_position_reference``) so the chokepoint body stays
    flat and the failure-mode branches are localised:

    * No ``registry_lookup`` supplied → legacy reference (default).
    * Lookup returns ``None`` → legacy reference (no registry row yet for
      this event; registry-mode is opt-in per blueprint 28 §5).
    * Lookup raises → mode-aware (live raises
      :class:`AccountingPersistenceError`; paper logs ERROR + falls back
      to legacy).
    * Lookup returns a row but its ``physical_identity_hash`` is missing /
      empty / whitespace-only → mode-aware (live raises; paper logs ERROR
      + falls back to legacy). Same rule applies if the row's identity
      fields fail the ``Empty ≠ Zero`` shape check in
      :class:`PositionReference.__post_init__`.
    * Lookup returns a row with non-empty identity → registry reference.
    """
    if registry_lookup is None:
        return build_legacy_position_reference(record)

    try:
        row = registry_lookup(
            record.primitive.value,
            record.event_kind.value,
            record.accounting_category.value,
        )
    except Exception as exc:
        msg = (
            f"augment_accounting_payload: registry_lookup raised "
            f"({type(exc).__name__}: {exc}); falling back to legacy reference"
        )
        if is_live:
            raise AccountingPersistenceError(
                AccountingWriteKind.ACCOUNTING,
                message=msg,
                cause=exc,
            ) from exc
        logger.error("%s — non-live mode", msg)
        return build_legacy_position_reference(record)

    if row is None:
        return build_legacy_position_reference(record)

    # Blueprint 28 §3 makes physical_identity_hash / semantic_grouping_key
    # / grouping_policy_version NOT NULL in the registry schema. A row
    # reaching here with any of those missing is a registry-write bug —
    # ``build_registry_position_reference`` fails loud (ValueError) and
    # we honor the mode-aware contract: live raises
    # AccountingPersistenceError so the runner halts; paper / dry_run
    # logs ERROR and falls back to legacy so the loop keeps moving.
    # Previously the writer short-circuited on ``hash_value is None``
    # with a warning + legacy fall-back even in live mode; CodeRabbit
    # PR #2236 round 2 flagged that as too lenient — a None / empty
    # identity field with ``source="registry"`` would lose the L5_22
    # join key silently. The helper is now the single point of
    # validation.
    try:
        return build_registry_position_reference(record, registry_row=row)
    except ValueError as exc:
        msg = (
            f"augment_accounting_payload: registry row failed PositionReference "
            f"shape check ({exc}); per CLAUDE.md 'Empty ≠ Zero' an empty / "
            f"whitespace / None identity field is a parser-write bug, not a value"
        )
        if is_live:
            raise AccountingPersistenceError(
                AccountingWriteKind.ACCOUNTING,
                message=msg,
                cause=exc,
            ) from exc
        logger.error("%s — non-live mode, falling back to legacy reference", msg)
        return build_legacy_position_reference(record)


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
                    deployment_id=event.identity.deployment_id,
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
                    deployment_id=event.identity.deployment_id,
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
