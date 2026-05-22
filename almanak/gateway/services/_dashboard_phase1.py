"""Helpers for VIB-4493 Phase 1 DashboardService RPCs.

Isolated from `dashboard_service.py` (which is already 2818 lines) and from
`_dashboard_helpers.py` (existing helpers for unrelated dashboard surface).
Scope: cutover-state derivation, position-row proto builders, range-history
entry builders, and the per-primitive stub catalogue.

References:
  - Schema:   almanak/framework/state/backends/sqlite.py:720-737 (migration_state),
              :644-663 (position_registry), :428-471 (position_events),
              :476-518 (accounting_events)
  - State manager: almanak/framework/state/gateway_state_manager.py
                   get_migration_state, get_position_registry_open_rows,
                   get_position_events_filtered, get_position_history,
                   get_latest_snapshot
  - Design doc: PortfolioManager/DashboardMay16.md v5
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from almanak.gateway.proto import gateway_pb2

if TYPE_CHECKING:
    from almanak.framework.migration.backfill import MigrationStateRow


# Maps accounting_category to (primitive, cutover_key) for migration_state lookup.
# v1 reality: every LP accounting_category resolves to ('lp', 'lp') because
# the backfill writer only emits cutover_key='lp' today (cutover.py:69).
# Future cutovers (T16 perp, T23 Pendle, T28 Aave) will append typed
# cutover_key values like 'PERP_GMX', 'LP_PENDLE', 'AAVE_COLLATERAL', and
# this mapping will gain entries. Until then, UniV3 LP and Pendle LP share
# one migration_state row by design.
_ACCOUNTING_CATEGORY_TO_CUTOVER_KEY_V1: dict[str, tuple[str, str]] = {
    "LP_UNIV3": ("lp", "lp"),
    "LP_UNIV4": ("lp", "lp"),
    "LP_PANCAKESWAP": ("lp", "lp"),
    "LP_AERODROME": ("lp", "lp"),
    "LP_SUSHISWAP": ("lp", "lp"),
    "LP_PENDLE": ("lp", "lp"),
}

# Per-primitive stub catalogue surfaced when a reconciliation parser or a
# range-history source isn't shipped yet. Renderer paints these as
# explicit "pending VIB-XXXX" cards. Keep tickets accurate — they're shown
# to operators.
PER_PRIMITIVE_STUBS: dict[str, tuple[str, str]] = {
    # primitive -> (ticket, message)
    "perp": ("VIB-4202", "Reconciliation for perp — pending VIB-4202 (T16 — Registry GMX V2 perp end-to-end)"),
    "pendle_lp": ("VIB-4209", "Reconciliation for pendle_lp — pending VIB-4209 (T23 — Registry Pendle LP end-to-end)"),
    "lending": (
        "VIB-4501",
        "Reconciliation for lending — pending VIB-4501 (T24-followup — Aave V3 reconciliation parser)",
    ),
}

# Range-history source-table mapping per primitive.
# LP/PERP → position_events, lending → accounting_events.
# Swap/prediction → no source (history concept doesn't apply); renderer
# shows stub_message.
RANGE_HISTORY_SOURCE_BY_PRIMITIVE: dict[str, str] = {
    "lp": "position_events",
    "perp": "position_events",
    "lending": "accounting_events",
}

RANGE_HISTORY_NA_STUB: str = (
    "Range history doesn't apply to this primitive — swap and prediction "
    "positions are intent-only with no held state. See trade tape for the "
    "underlying intents."
)

# Phase 1 v1 lending range-history stub. Until VIB-4501 ships the
# accounting_events filtered reader on GatewayStateManager, lending range
# history returns this stub. Renderer painters the stub card.
LENDING_RANGE_HISTORY_V1_STUB: str = (
    "Lending range history pending VIB-4501 (T24-followup — Aave V3 "
    "reconciliation parser). Use trade tape and PnL summary for now."
)


@dataclass(frozen=True)
class CutoverDerivation:
    """Materialised cutover signal for one (deployment, accounting_category)."""

    state: gateway_pb2.CutoverState.V
    migration_state_row: MigrationStateRow | None
    last_reconciled_at_block: int
    last_reconciled_unix_seconds: int


def cutover_lookup_key(accounting_category: str) -> tuple[str, str]:
    """Return (primitive, cutover_key) for migration_state lookup.

    Falls back to (accounting_category_lower, accounting_category) for unknown
    categories so future typed cutover rows route correctly without code edits.
    """
    if not accounting_category:
        return ("lp", "lp")
    mapped = _ACCOUNTING_CATEGORY_TO_CUTOVER_KEY_V1.get(accounting_category)
    if mapped is not None:
        return mapped
    return (accounting_category.lower(), accounting_category)


def derive_cutover_state(
    migration_state_row: MigrationStateRow | None,
    *,
    last_reconciled_unix_seconds: int,
    now_unix_seconds: int,
    fresh_threshold_seconds: int = 86400,
) -> gateway_pb2.CutoverState.V:
    """Derive CutoverState enum value from migration_state + reconciliation freshness.

    State machine (per v5 design):
      PRE_BACKFILL          backfill_complete=0 AND backfill_started_at IS NULL
      BACKFILL_IN_PROGRESS  backfill_complete=0 AND backfill_started_at IS NOT NULL
      BACKFILL_COMPLETE     backfill_complete=1 AND reconcile_fresh < 24h
      REGISTRY_AUTHORITATIVE all deployments at BACKFILL_COMPLETE for ≥ N snapshots
                            (the "≥ N" condition is asserted gateway-side across the
                            cutover category; this function only flips the
                            single-row signal — the multi-snapshot stability gate
                            is layered above when known.)

    Phase 1 v1 returns BACKFILL_COMPLETE for the "complete + fresh" combination.
    The REGISTRY_AUTHORITATIVE promotion is intentionally separated and applied
    by the caller when it has multi-snapshot stability evidence (typically via a
    separate aggregation across all deployments for the category).
    """
    if migration_state_row is None:
        return gateway_pb2.CUTOVER_STATE_PRE_BACKFILL

    if not migration_state_row.position_registry_backfill_complete:
        if migration_state_row.backfill_started_at:
            return gateway_pb2.CUTOVER_STATE_BACKFILL_IN_PROGRESS
        return gateway_pb2.CUTOVER_STATE_PRE_BACKFILL

    # backfill_complete=1; check freshness gate.
    if last_reconciled_unix_seconds <= 0:
        # Backfill done but reconciliation has never run — still BACKFILL_COMPLETE
        # because the writer-side gate (rows_synthesized == frozen_denominator)
        # was passed. Renderer shows a freshness warning, not a regression.
        return gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE

    age = now_unix_seconds - last_reconciled_unix_seconds
    if age <= fresh_threshold_seconds:
        return gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE

    # Stale reconciliation: backfill is technically complete but the chain
    # truth check is stale. Keep BACKFILL_COMPLETE (not a regression to
    # IN_PROGRESS) — renderer's freshness pill carries the staleness signal.
    return gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE


def parse_registry_status(status: str) -> gateway_pb2.PositionStatus.V:
    """Convert position_registry.status TEXT to PositionStatus enum."""
    if status == "open":
        return gateway_pb2.POSITION_STATUS_OPEN
    if status == "closed":
        return gateway_pb2.POSITION_STATUS_CLOSED
    if status == "reorg_invalidated":
        return gateway_pb2.POSITION_STATUS_REORG_INVALIDATED
    return gateway_pb2.POSITION_STATUS_UNSPECIFIED


def _str_or_empty(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _int_or_zero(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _bytes_or_empty(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    try:
        return json.dumps(value).encode("utf-8")
    except (TypeError, ValueError):
        return b""


def _iso_or_empty(timestamp_seconds: int) -> str:
    if timestamp_seconds <= 0:
        return ""
    return datetime.fromtimestamp(timestamp_seconds, tz=UTC).isoformat()


def build_position_entry(
    *,
    registry_row: dict[str, Any],
    snapshot_position: dict[str, Any] | None,
    cutover: CutoverDerivation,
    source: gateway_pb2.PositionSource.V,
    confidence: gateway_pb2.PositionConfidence.V,
    value_as_of_unix_seconds: int,
) -> gateway_pb2.PositionEntry:
    """Build a PositionEntry proto from a registry row + matching snapshot row.

    Registry row shape comes from
    ``GatewayStateManager._proto_row_to_registry_dict`` (gateway_state_manager.py:1187).
    Snapshot position shape comes from
    ``PortfolioSnapshot.positions[*]`` (positions_json blob, parsed).

    snapshot_position may be None when the registry has a row but the
    latest snapshot doesn't carry valuation for it (transient mismatch
    surfaced honestly via empty value_* fields + confidence=LOW).
    """
    handle = _str_or_empty(registry_row.get("handle") or registry_row.get("physical_identity_hash"))

    payload_dict = registry_row.get("payload") or {}
    snap_dict = snapshot_position or {}

    primitive_payload: dict[str, Any] = {}
    primitive = _str_or_empty(registry_row.get("primitive"))
    if primitive == "lp":
        primitive_payload = {
            "tick_lower": payload_dict.get("tick_lower"),
            "tick_upper": payload_dict.get("tick_upper"),
            "liquidity": snap_dict.get("liquidity") or payload_dict.get("liquidity"),
            "in_range": snap_dict.get("in_range"),
            "fees_token0": snap_dict.get("fees_token0"),
            "fees_token1": snap_dict.get("fees_token1"),
        }
    elif primitive == "lending":
        primitive_payload = {
            "supply_balance": snap_dict.get("supply_balance"),
            "borrow_balance": snap_dict.get("borrow_balance"),
            "health_factor": snap_dict.get("health_factor"),
            "ltv": snap_dict.get("ltv"),
        }
    elif primitive == "perp":
        primitive_payload = {
            "leverage": snap_dict.get("leverage"),
            "entry_price": snap_dict.get("entry_price"),
            "mark_price": snap_dict.get("mark_price"),
            "unrealized_pnl": snap_dict.get("unrealized_pnl"),
            "is_long": snap_dict.get("is_long"),
        }

    return gateway_pb2.PositionEntry(
        handle=handle,
        physical_identity_hash=_str_or_empty(registry_row.get("physical_identity_hash")),
        deployment_id=_str_or_empty(registry_row.get("deployment_id")),
        chain=_str_or_empty(registry_row.get("chain")),
        primitive=primitive,
        accounting_category=_str_or_empty(registry_row.get("accounting_category")),
        status=parse_registry_status(_str_or_empty(registry_row.get("status"))),
        opened_at_block=_int_or_zero(registry_row.get("opened_at_block")),
        closed_at_block=_int_or_zero(registry_row.get("closed_at_block")),
        opened_tx=_str_or_empty(registry_row.get("opened_tx")),
        closed_tx=_str_or_empty(registry_row.get("closed_tx")),
        value_usd=_str_or_empty(snap_dict.get("value_usd")),
        value_token0=_str_or_empty(snap_dict.get("value_token0")),
        value_token1=_str_or_empty(snap_dict.get("value_token1")),
        source=source,
        confidence=confidence,
        last_reconciled_at_block=_int_or_zero(registry_row.get("last_reconciled_at_block")),
        cutover_state=cutover.state,
        primitive_payload_json=_bytes_or_empty(primitive_payload),
        value_as_of=_iso_or_empty(value_as_of_unix_seconds),
    )


# accounting_category prefix → primitive. Keep in sync with the v1 cutover
# key mapping above and with whatever new categories the backfill writer
# emits when VIB-4202/4209/4501 land.
_ACCOUNTING_CATEGORY_PRIMITIVE_PREFIXES: tuple[tuple[str, str], ...] = (
    ("LP_", "lp"),
    ("PERP_", "perp"),
    ("AAVE_", "lending"),
    ("MORPHO_", "lending"),
    ("COMPOUND_", "lending"),
    ("SWAP_", "swap"),
    ("POLY_", "swap"),
    ("PREDICTION_", "swap"),
)


def infer_primitive_from_accounting_category(accounting_category: str) -> str:
    """Infer the primitive label from an accounting_category string.

    Falls back to "lp" when nothing matches — keeps Phase 1 safe for an
    unknown LP fork that adds a new category before its prefix is
    registered. The renderer's stub message still surfaces if the underlying
    events query comes back empty.
    """
    upper = (accounting_category or "").upper()
    for prefix, primitive in _ACCOUNTING_CATEGORY_PRIMITIVE_PREFIXES:
        if upper.startswith(prefix):
            return primitive
    return "lp"


def _event_unix_seconds(timestamp_value: Any) -> int:
    """Coerce a position_events row's ``timestamp`` into unix seconds.

    Handles the three shapes the SQLite backend produces: ``datetime``
    (post-rowfactory), ISO-8601 ``str`` (legacy), and numeric (rare).
    Returns 0 on any parse failure so the time window filter treats the
    row as "outside any non-zero window" — safe default.
    """
    if isinstance(timestamp_value, datetime):
        return int(timestamp_value.timestamp())
    if isinstance(timestamp_value, str) and timestamp_value:
        try:
            return int(datetime.fromisoformat(timestamp_value).timestamp())
        except ValueError:
            return 0
    if isinstance(timestamp_value, int | float):
        return int(timestamp_value)
    return 0


def build_authoritative_positions(
    *,
    registry_rows: list[dict[str, Any]],
    cutover_by_category: dict[str, CutoverDerivation],
    snapshot_by_id: dict[str, dict[str, Any]],
    snapshot_taken_at_unix: int,
) -> list[gateway_pb2.PositionEntry]:
    """Build PositionEntry rows for the authoritative lane (registry rows).

    Pure function — no I/O. Extracted from DashboardServiceServicer.GetPositions
    so the handler stays under the project's CC=15 complexity gate. Each row:

    * Resolves its category's CutoverDerivation (or defaults to PRE_BACKFILL
      when no derivation matches — defensive against a stale registry row
      whose category lost its migration_state entry).
    * Matches against the snapshot index via handle THEN
      physical_identity_hash — gateway-side LP backfill writes
      handle = position_id today so handle matches first.
    * Picks ``source`` based on cutover state: registry-authoritative
      once backfill is complete, snapshot-derived during transition.
    * Confidence rolls up registry × snapshot × opened_tx presence —
      ``opened_tx`` is a v1 proxy for ledger evidence; a true
      registry × snapshot × transaction_ledger join is deferred (see
      ``derive_confidence`` docstring).
    """
    positions: list[gateway_pb2.PositionEntry] = []
    for row in registry_rows:
        category = row.get("accounting_category", "")
        cutover = cutover_by_category.get(
            category,
            CutoverDerivation(
                state=gateway_pb2.CUTOVER_STATE_PRE_BACKFILL,
                migration_state_row=None,
                last_reconciled_at_block=0,
                last_reconciled_unix_seconds=0,
            ),
        )
        handle = row.get("handle") or row.get("physical_identity_hash") or ""
        snapshot_match = snapshot_by_id.get(handle) or snapshot_by_id.get(row.get("physical_identity_hash", ""))
        source = (
            gateway_pb2.POSITION_SOURCE_REGISTRY
            if cutover.state
            in {
                gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE,
                gateway_pb2.CUTOVER_STATE_REGISTRY_AUTHORITATIVE,
            }
            else gateway_pb2.POSITION_SOURCE_SNAPSHOT
        )
        confidence = derive_confidence(
            registry_present=True,
            snapshot_present=snapshot_match is not None,
            opened_tx_present=bool(row.get("opened_tx")),
        )
        positions.append(
            build_position_entry(
                registry_row=row,
                snapshot_position=snapshot_match,
                cutover=cutover,
                source=source,
                confidence=confidence,
                value_as_of_unix_seconds=snapshot_taken_at_unix,
            )
        )
    return positions


def filter_events_by_time_window(
    events: list[dict[str, Any]],
    *,
    from_unix_seconds: int,
    to_unix_seconds: int,
) -> list[dict[str, Any]]:
    """Return only events whose timestamp lies in ``[from, to]`` (inclusive).

    Zero on either bound disables that side of the filter (open-ended).
    Events with an unparseable timestamp are dropped iff a non-zero window
    bound is in play; with both bounds zero they pass through.
    """
    if from_unix_seconds <= 0 and to_unix_seconds <= 0:
        return list(events)
    out: list[dict[str, Any]] = []
    for event in events:
        event_unix = _event_unix_seconds(event.get("timestamp"))
        # Unparseable timestamps surface as 0 from the helper. When any
        # window bound is active, treat them as unfilterable and drop —
        # otherwise an `from=0, to=T` query silently keeps every corrupt row.
        if event_unix == 0:
            continue
        if from_unix_seconds > 0 and event_unix < from_unix_seconds:
            continue
        if to_unix_seconds > 0 and event_unix > to_unix_seconds:
            continue
        out.append(event)
    return out


def build_cutover_state_entry(
    *,
    accounting_category: str,
    derivation: CutoverDerivation,
) -> gateway_pb2.CutoverStateEntry:
    """Build a CutoverStateEntry proto from a CutoverDerivation."""
    row = derivation.migration_state_row
    if row is None:
        return gateway_pb2.CutoverStateEntry(
            accounting_category=accounting_category,
            state=derivation.state,
            rows_synthesized=0,
            rows_skipped_already_present=0,
            backfill_started_at="",
            backfill_completed_at="",
            backfill_reader_version=0,
            last_reconciled_at_block=derivation.last_reconciled_at_block,
            last_reconciled_unix_seconds=derivation.last_reconciled_unix_seconds,
        )
    return gateway_pb2.CutoverStateEntry(
        accounting_category=accounting_category,
        state=derivation.state,
        rows_synthesized=int(row.rows_synthesized),
        rows_skipped_already_present=int(row.rows_skipped_already_present),
        backfill_started_at=row.backfill_started_at or "",
        backfill_completed_at=row.backfill_completed_at or "",
        backfill_reader_version=int(row.backfill_reader_version),
        last_reconciled_at_block=derivation.last_reconciled_at_block,
        last_reconciled_unix_seconds=derivation.last_reconciled_unix_seconds,
    )


def build_range_history_entry_from_position_event(
    event: dict[str, Any],
) -> gateway_pb2.RangeHistoryEntry:
    """Build a RangeHistoryEntry from a position_events row.

    position_events row shape matches what
    ``GatewayStateManager.get_position_history`` returns
    (gateway_state_manager.py:734-786, which already JSON-decodes the
    attribution payload).
    """
    payload = {
        "tick_lower": event.get("tick_lower"),
        "tick_upper": event.get("tick_upper"),
        "liquidity": event.get("liquidity"),
        "in_range": event.get("in_range"),
        "fees_token0": event.get("fees_token0"),
        "fees_token1": event.get("fees_token1"),
        "leverage": event.get("leverage"),
        "entry_price": event.get("entry_price"),
        "mark_price": event.get("mark_price"),
        "unrealized_pnl": event.get("unrealized_pnl"),
        "is_long": event.get("is_long"),
        "value_usd": event.get("value_usd"),
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    timestamp_unix = 0
    timestamp_raw = event.get("timestamp")
    if isinstance(timestamp_raw, datetime):
        timestamp_unix = int(timestamp_raw.timestamp())
    elif isinstance(timestamp_raw, str) and timestamp_raw:
        try:
            timestamp_unix = int(datetime.fromisoformat(timestamp_raw).timestamp())
        except ValueError:
            pass
    elif isinstance(timestamp_raw, int | float):
        timestamp_unix = int(timestamp_raw)

    return gateway_pb2.RangeHistoryEntry(
        timestamp_unix_seconds=timestamp_unix,
        block_number=_int_or_zero(event.get("block_number")),
        event_type=_str_or_empty(event.get("event_type")),
        source_table="position_events",
        ledger_entry_id=_str_or_empty(event.get("ledger_entry_id")),
        tx_hash=_str_or_empty(event.get("tx_hash")),
        payload_json=_bytes_or_empty(payload),
    )


def build_per_primitive_stubs(missing_primitives: set[str]) -> list[gateway_pb2.PrimitiveCoverageStub]:
    """Return PrimitiveCoverageStub messages for the missing primitives."""
    stubs: list[gateway_pb2.PrimitiveCoverageStub] = []
    for primitive in sorted(missing_primitives):
        stub_info = PER_PRIMITIVE_STUBS.get(primitive)
        if stub_info is None:
            continue
        ticket, message = stub_info
        stubs.append(
            gateway_pb2.PrimitiveCoverageStub(
                primitive=primitive,
                message=message,
                ticket=ticket,
            )
        )
    return stubs


def derive_confidence(
    *,
    registry_present: bool,
    snapshot_present: bool,
    opened_tx_present: bool,
) -> gateway_pb2.PositionConfidence.V:
    """Pick the confidence badge from cross-source agreement.

    Codex review note (medium): the ideal third signal is "row joined to
    ``transaction_ledger.id``" — proof the opening intent was recorded
    in the canonical ledger. v1 does not perform that join (would be a
    per-row gateway-side lookup). Instead we accept the registry row's
    ``opened_tx`` field as the proxy: if Reconcile / backfill could
    back-derive an opening tx hash, that is meaningful evidence the
    position has on-chain provenance, even if we haven't cross-checked
    ``transaction_ledger`` itself.

    Callers should pass ``opened_tx_present=bool(row.get("opened_tx"))``;
    they MUST NOT claim ledger agreement based on this. A future ticket
    (no Linear ID yet) will swap this for an actual ledger join, at
    which point HIGH confidence will mean the strict 3-way agreement
    described in the design doc.
    """
    count = int(registry_present) + int(snapshot_present) + int(opened_tx_present)
    if count >= 3:
        return gateway_pb2.POSITION_CONFIDENCE_HIGH
    if count == 2:
        return gateway_pb2.POSITION_CONFIDENCE_MEDIUM
    if count == 1:
        return gateway_pb2.POSITION_CONFIDENCE_LOW
    return gateway_pb2.POSITION_CONFIDENCE_UNSPECIFIED


# =============================================================================
# Phase 1C reconciliation triad — helpers + preview-token store
#
# Audit findings (A2) that drive the design here:
#   * PositionService.Reconcile has NO idempotency layer — we invent one at
#     DashboardService scope (in-memory dict keyed by preview_token).
#   * PositionService.Reconcile has NO concurrency guard — DashboardService
#     adds an asyncio.Lock per strategy for RefreshRegistryFromChain
#     (Phase 1D). PreviewReconcile / ApplyReconcile do not require a lock
#     because the STATE_DRIFT check catches concurrent state changes.
#   * PositionService.Reconcile partial-apply is per-phantom (not transactional).
#     ApplyReconcileResponse surfaces this via PARTIAL_SUCCESS + primitive_errors.
# =============================================================================


@dataclass(frozen=True)
class StateFingerprint:
    """Counter-based fingerprint used to detect state drift between
    PreviewReconcile and ApplyReconcile. Cheap to compute and compare —
    avoids hashing entire tables on every preview/apply pair.

    The fingerprint covers what matters for reconciliation drift:
      * registry_row_count   — new opens / closes change this
      * registry_max_block   — new reconciliations advance this
      * ledger_max_id        — new ledger entries advance this lex-order
      * source_block_number  — chain head movement (proxy for on-chain drift)

    Two fingerprints with all fields equal mean: registry hasn't changed,
    ledger hasn't grown, AND the chain head sampled in the preview is the
    same one we'd sample at apply time. Any divergence → STATE_DRIFT.
    """

    registry_row_count: int
    registry_max_block: int
    ledger_max_id: str
    source_block_number: int

    def equals(self, other: StateFingerprint) -> bool:
        return (
            self.registry_row_count == other.registry_row_count
            and self.registry_max_block == other.registry_max_block
            and self.ledger_max_id == other.ledger_max_id
            and self.source_block_number == other.source_block_number
        )


def compute_state_fingerprint(
    *,
    registry_rows: list[dict[str, Any]],
    ledger_max_id: str,
    source_block_number: int,
) -> StateFingerprint:
    """Build a fingerprint from a registry snapshot + ledger high-water mark.

    Pure function — no I/O. Callers supply the rows + max ledger id they
    already had to fetch for the reconciliation diff anyway.
    """
    max_block = 0
    for row in registry_rows:
        block = row.get("last_reconciled_at_block") or row.get("opened_at_block") or 0
        try:
            max_block = max(max_block, int(block or 0))
        except (TypeError, ValueError):
            continue
    return StateFingerprint(
        registry_row_count=len(registry_rows),
        registry_max_block=max_block,
        ledger_max_id=ledger_max_id or "",
        source_block_number=source_block_number,
    )


@dataclass
class _PreviewTokenEntry:
    """Stored alongside a preview_token. The reconcile_response is the LP
    diff buckets the operator saw at preview time — replayed at apply time
    if the fingerprint matches."""

    deployment_id: str
    fingerprint: StateFingerprint
    reconcile_response: Any  # gateway_pb2.ReconcileResponse
    expires_at_unix_seconds: int


class PreviewTokenStore:
    """In-memory preview_token registry with TTL.

    NOT cross-process safe — single-gateway scope by design. Multi-gateway
    is unsupported in v1; per-strategy gateway pinning is the 1:1 rule
    (CLAUDE.md, feedback memory: gateway_1to1_hard_rule).

    Token format: ``preview-{uuid4-hex}``. Opaque to clients; only the
    server validates.
    """

    def __init__(self, *, default_ttl_seconds: int = 300) -> None:
        self._tokens: dict[str, _PreviewTokenEntry] = {}
        self._default_ttl_seconds = default_ttl_seconds

    def issue(
        self,
        *,
        deployment_id: str,
        fingerprint: StateFingerprint,
        reconcile_response: Any,
        now_unix_seconds: int,
        ttl_seconds: int | None = None,
    ) -> tuple[str, int]:
        """Issue a new preview_token. Returns (token, expires_at_unix_seconds)."""
        import uuid as _uuid

        token = f"preview-{_uuid.uuid4().hex}"
        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl_seconds
        expires_at = now_unix_seconds + ttl
        self._tokens[token] = _PreviewTokenEntry(
            deployment_id=deployment_id,
            fingerprint=fingerprint,
            reconcile_response=reconcile_response,
            expires_at_unix_seconds=expires_at,
        )
        return token, expires_at

    def consume(
        self,
        *,
        token: str,
        deployment_id: str,
        now_unix_seconds: int,
    ) -> tuple[str, _PreviewTokenEntry | None]:
        """Validate + atomically remove the token.

        Returns ``(status, entry)`` where ``status`` is one of:
          * "OK"          — entry returned, ready for fingerprint compare
          * "NOT_FOUND"   — token does not exist (typo, restart, race)
          * "EXPIRED"     — token TTL elapsed
          * "WRONG_STRATEGY" — token belongs to another strategy (defensive)
        """
        entry = self._tokens.pop(token, None)
        if entry is None:
            return ("NOT_FOUND", None)
        if entry.expires_at_unix_seconds < now_unix_seconds:
            return ("EXPIRED", None)
        if entry.deployment_id != deployment_id:
            # Re-store to avoid silent token loss (defensive — should be impossible
            # with sound client usage but cheap to guard).
            self._tokens[token] = entry
            return ("WRONG_STRATEGY", None)
        return ("OK", entry)

    def gc_expired(self, *, now_unix_seconds: int) -> int:
        """Remove expired entries; returns count purged. Call opportunistically."""
        expired = [t for t, e in self._tokens.items() if e.expires_at_unix_seconds < now_unix_seconds]
        for t in expired:
            self._tokens.pop(t, None)
        return len(expired)


@dataclass
class _ReportCacheEntry:
    response: Any  # gateway_pb2.GetReconciliationReportResponse
    expires_at_unix_seconds: int


class ReconciliationReportCache:
    """5-second TTL cache on GetReconciliationReport responses.

    Keyed only by deployment_id — no semantic invalidation, pure time-based
    expiry. Per v5 design ("simpler, harder to get wrong, bounded staleness").
    """

    def __init__(self, *, ttl_seconds: int = 5) -> None:
        self._entries: dict[str, _ReportCacheEntry] = {}
        self._ttl_seconds = ttl_seconds

    def get(self, deployment_id: str, *, now_unix_seconds: int) -> Any | None:
        entry = self._entries.get(deployment_id)
        if entry is None:
            return None
        if entry.expires_at_unix_seconds < now_unix_seconds:
            self._entries.pop(deployment_id, None)
            return None
        return entry.response

    def put(self, deployment_id: str, response: Any, *, now_unix_seconds: int) -> None:
        self._entries[deployment_id] = _ReportCacheEntry(
            response=response,
            expires_at_unix_seconds=now_unix_seconds + self._ttl_seconds,
        )


def reconcile_response_to_report(
    *,
    reconcile_response: Any,
    now_unix_seconds: int,
) -> gateway_pb2.GetReconciliationReportResponse:
    """Convert a PositionService.ReconcileResponse → dashboard report response.

    Maps diff buckets to ReconciliationFinding rows:
      * matched         → severity=INFO (3-source agreement)
      * phantom_missing → severity=DIVERGED (ledger says yes, registry says no)
      * stranded        → severity=WARN (registry says yes, chain says no)

    Per-primitive stubs are surfaced from PrimitiveError(code='PARSER_UNSUPPORTED').
    """
    findings: list[gateway_pb2.ReconciliationFinding] = []

    for matched in reconcile_response.matched:
        findings.append(
            gateway_pb2.ReconciliationFinding(
                accounting_category=matched.accounting_category,
                physical_identity_hash=matched.physical_identity_hash,
                severity=gateway_pb2.RECONCILIATION_SEVERITY_INFO,
                delta="match",
                ledger_has_row=True,
                snapshot_has_row=True,
                registry_has_row=True,
                suggested_action="",
            )
        )
    for phantom in reconcile_response.phantom_missing:
        findings.append(
            gateway_pb2.ReconciliationFinding(
                accounting_category=phantom.accounting_category,
                physical_identity_hash=phantom.physical_identity_hash,
                severity=gateway_pb2.RECONCILIATION_SEVERITY_DIVERGED,
                delta="on-chain has position; registry does not (GH #2131 case)",
                ledger_has_row=bool(phantom.opened_tx),
                snapshot_has_row=False,
                registry_has_row=False,
                suggested_action="PreviewReconcile then ApplyReconcile to insert the missing registry row",
            )
        )
    for stranded in reconcile_response.stranded:
        findings.append(
            gateway_pb2.ReconciliationFinding(
                accounting_category=stranded.accounting_category,
                physical_identity_hash=stranded.physical_identity_hash,
                severity=gateway_pb2.RECONCILIATION_SEVERITY_WARN,
                delta=f"registry status=open, chain absent: {stranded.absent_reason}",
                ledger_has_row=False,
                snapshot_has_row=False,
                registry_has_row=True,
                suggested_action="inspect with ax lp-info; run teardown if confirmed",
            )
        )

    primitive_stubs: list[gateway_pb2.PrimitiveCoverageStub] = []
    parser_unsupported_primitives = {
        err.primitive for err in reconcile_response.primitive_errors if err.code == "PARSER_UNSUPPORTED"
    }
    primitive_stubs.extend(build_per_primitive_stubs(parser_unsupported_primitives))

    return gateway_pb2.GetReconciliationReportResponse(
        findings=findings,
        primitive_stubs=primitive_stubs,
        reconciliation_id=reconcile_response.reconciliation_id,
        source_block_number=reconcile_response.source_block_number,
        as_of=_iso_or_empty(now_unix_seconds),
    )


def categorize_apply_result(
    *,
    reconcile_response: Any,
    fingerprint_matched: bool,
) -> tuple[str, str]:
    """Classify an apply result into (result_code, detail_message).

    Returns one of: SUCCESS / PARTIAL_SUCCESS / STATE_DRIFT.
    EXPIRED / NOT_FOUND / WRONG_STRATEGY are handled by the caller from the
    PreviewTokenStore.consume() status, never reaching this function.
    """
    if not fingerprint_matched:
        return (
            "STATE_DRIFT",
            "registry / ledger / chain state changed since preview was issued; re-issue PreviewReconcile",
        )
    if reconcile_response.primitive_errors:
        # Partial — some rebuilds succeeded, some failed per primitive_errors.
        return ("PARTIAL_SUCCESS", "some primitives failed to apply; see primitive_errors for details")
    return ("SUCCESS", "all phantom_missing rows inserted into registry")
