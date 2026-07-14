"""Dashboard service facade — two-tier read-only / operator split (Phase 2 / VIB-4494).

This module establishes the **DashboardServiceClient / OperatorDashboardServiceClient**
typing contract for the Phase 1 RPCs (VIB-4493). Renderers MUST type-hint
``DashboardServiceClient`` so they cannot syntactically call mutation RPCs
(``preview_reconcile`` / ``apply_reconcile`` / ``refresh_registry_from_chain``);
those live only on ``OperatorDashboardServiceClient``.

Why two classes instead of one with method conventions:

* **Static enforcement.** A renderer that only takes a ``DashboardServiceClient``
  parameter physically cannot call a mutation method — the attribute does not
  exist. ``mypy`` / Pyright will flag misuse without runtime checks.
* **CI lint compatibility.** Phase 4 (VIB-4496) ships
  ``test_no_bypass.py`` which scans for forbidden imports. The two-class
  shape lets that lint distinguish "renderer imports DashboardServiceClient"
  (OK) from "renderer imports OperatorDashboardServiceClient" (FAIL).
* **Audit semantics.** Mutation methods record the operator who triggered
  them. Keeping them on a separate subclass makes "did this code path ever
  mutate state?" answerable by import grep.

The classes wrap ``GatewayClient`` directly via the ``dashboard`` stub. They
deliberately do NOT inherit from the legacy ``GatewayDashboardClient`` —
that class predates the two-tier split and mixes reads with operator
actions. Existing call sites continue to use ``GatewayDashboardClient``
unchanged; Phase 4 migrates them.

Naming note: this module deliberately avoids the name ``DashboardAPIClient``
because that name is already taken by the per-strategy custom-dashboard
plugin API at ``almanak.framework.dashboard.custom.api_client`` — a different
abstraction (strategy-scoped, injected into user-written UI code).
``DashboardServiceClient`` is named after the gRPC service it wraps
(``DashboardService``) to keep the two unambiguous.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

import grpc

from almanak.framework.gateway_client import GatewayClient, get_gateway_client
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


# =============================================================================
# Typed enums — mirror proto definitions so callers get IDE completion +
# pattern-matching without importing protobuf Enum descriptors.
# =============================================================================


class CutoverState(StrEnum):
    """Per-(strategy, accounting_category) registry cutover progression.

    Derived from ``migration_state.position_registry_backfill_complete``
    + ``migration_state.position_registry_backfill_started_at`` server-side.
    Renderers paint header pills + audit badges based on this value.
    """

    UNSPECIFIED = "UNSPECIFIED"
    PRE_BACKFILL = "PRE_BACKFILL"
    BACKFILL_IN_PROGRESS = "BACKFILL_IN_PROGRESS"
    BACKFILL_COMPLETE = "BACKFILL_COMPLETE"
    REGISTRY_AUTHORITATIVE = "REGISTRY_AUTHORITATIVE"

    @classmethod
    def from_proto(cls, value: int) -> CutoverState:
        return _CUTOVER_STATE_PROTO_TO_ENUM.get(value, cls.UNSPECIFIED)


_CUTOVER_STATE_PROTO_TO_ENUM: dict[int, CutoverState] = {
    gateway_pb2.CUTOVER_STATE_UNSPECIFIED: CutoverState.UNSPECIFIED,
    gateway_pb2.CUTOVER_STATE_PRE_BACKFILL: CutoverState.PRE_BACKFILL,
    gateway_pb2.CUTOVER_STATE_BACKFILL_IN_PROGRESS: CutoverState.BACKFILL_IN_PROGRESS,
    gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE: CutoverState.BACKFILL_COMPLETE,
    gateway_pb2.CUTOVER_STATE_REGISTRY_AUTHORITATIVE: CutoverState.REGISTRY_AUTHORITATIVE,
}


class PositionSource(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    REGISTRY = "REGISTRY"
    SNAPSHOT = "SNAPSHOT"
    LEGACY = "LEGACY"

    @classmethod
    def from_proto(cls, value: int) -> PositionSource:
        return _POSITION_SOURCE_PROTO_TO_ENUM.get(value, cls.UNSPECIFIED)


_POSITION_SOURCE_PROTO_TO_ENUM: dict[int, PositionSource] = {
    gateway_pb2.POSITION_SOURCE_UNSPECIFIED: PositionSource.UNSPECIFIED,
    gateway_pb2.POSITION_SOURCE_REGISTRY: PositionSource.REGISTRY,
    gateway_pb2.POSITION_SOURCE_SNAPSHOT: PositionSource.SNAPSHOT,
    gateway_pb2.POSITION_SOURCE_LEGACY: PositionSource.LEGACY,
}


class PositionConfidence(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    DIVERGED = "DIVERGED"

    @classmethod
    def from_proto(cls, value: int) -> PositionConfidence:
        return _POSITION_CONFIDENCE_PROTO_TO_ENUM.get(value, cls.UNSPECIFIED)


_POSITION_CONFIDENCE_PROTO_TO_ENUM: dict[int, PositionConfidence] = {
    gateway_pb2.POSITION_CONFIDENCE_UNSPECIFIED: PositionConfidence.UNSPECIFIED,
    gateway_pb2.POSITION_CONFIDENCE_HIGH: PositionConfidence.HIGH,
    gateway_pb2.POSITION_CONFIDENCE_MEDIUM: PositionConfidence.MEDIUM,
    gateway_pb2.POSITION_CONFIDENCE_LOW: PositionConfidence.LOW,
    gateway_pb2.POSITION_CONFIDENCE_DIVERGED: PositionConfidence.DIVERGED,
}


class PositionStatus(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    REORG_INVALIDATED = "REORG_INVALIDATED"

    @classmethod
    def from_proto(cls, value: int) -> PositionStatus:
        return _POSITION_STATUS_PROTO_TO_ENUM.get(value, cls.UNSPECIFIED)

    def to_proto(self) -> gateway_pb2.PositionStatus.ValueType:
        # The proto enum's nominal type is ``PositionStatus.ValueType``;
        # underlying value is ``int``. Annotated explicitly so callers
        # passing the result to a proto constructor type-check cleanly.
        return _POSITION_STATUS_ENUM_TO_PROTO.get(self, gateway_pb2.POSITION_STATUS_UNSPECIFIED)


_POSITION_STATUS_PROTO_TO_ENUM: dict[int, PositionStatus] = {
    gateway_pb2.POSITION_STATUS_UNSPECIFIED: PositionStatus.UNSPECIFIED,
    gateway_pb2.POSITION_STATUS_OPEN: PositionStatus.OPEN,
    gateway_pb2.POSITION_STATUS_CLOSED: PositionStatus.CLOSED,
    gateway_pb2.POSITION_STATUS_REORG_INVALIDATED: PositionStatus.REORG_INVALIDATED,
}

# Build the inverse map with an explicit cast — proto ValueType is nominally
# distinct from `int` even though the underlying value is. Building it via a
# typed dict literal (not a comprehension) keeps mypy from collapsing the
# value type to `int` on the way back out.
_POSITION_STATUS_ENUM_TO_PROTO: dict[PositionStatus, gateway_pb2.PositionStatus.ValueType] = {
    PositionStatus.UNSPECIFIED: gateway_pb2.POSITION_STATUS_UNSPECIFIED,
    PositionStatus.OPEN: gateway_pb2.POSITION_STATUS_OPEN,
    PositionStatus.CLOSED: gateway_pb2.POSITION_STATUS_CLOSED,
    PositionStatus.REORG_INVALIDATED: gateway_pb2.POSITION_STATUS_REORG_INVALIDATED,
}


class ReconciliationSeverity(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    INFO = "INFO"
    WARN = "WARN"
    DIVERGED = "DIVERGED"

    @classmethod
    def from_proto(cls, value: int) -> ReconciliationSeverity:
        return _RECON_SEVERITY_PROTO_TO_ENUM.get(value, cls.UNSPECIFIED)


_RECON_SEVERITY_PROTO_TO_ENUM: dict[int, ReconciliationSeverity] = {
    gateway_pb2.RECONCILIATION_SEVERITY_UNSPECIFIED: ReconciliationSeverity.UNSPECIFIED,
    gateway_pb2.RECONCILIATION_SEVERITY_INFO: ReconciliationSeverity.INFO,
    gateway_pb2.RECONCILIATION_SEVERITY_WARN: ReconciliationSeverity.WARN,
    gateway_pb2.RECONCILIATION_SEVERITY_DIVERGED: ReconciliationSeverity.DIVERGED,
}


# =============================================================================
# Dataclasses — Pythonic shapes for the Phase 1 RPC responses.
# =============================================================================


@dataclass(frozen=True)
class CutoverStateEntry:
    """Per-accounting-category cutover snapshot for a strategy."""

    accounting_category: str
    state: CutoverState
    rows_synthesized: int
    rows_skipped_already_present: int
    backfill_started_at: str
    backfill_completed_at: str
    backfill_reader_version: int
    last_reconciled_at_block: int
    last_reconciled_unix_seconds: int

    @property
    def last_reconciled_at(self) -> datetime | None:
        if not self.last_reconciled_unix_seconds:
            return None
        return datetime.fromtimestamp(self.last_reconciled_unix_seconds, tz=UTC)


@dataclass(frozen=True)
class PositionEntry:
    """One position row with provenance + cutover_state for trust badges."""

    handle: str
    physical_identity_hash: str
    deployment_id: str
    chain: str
    primitive: str
    accounting_category: str
    status: PositionStatus
    opened_at_block: int
    closed_at_block: int
    opened_tx: str
    closed_tx: str
    # ``None`` == unmeasured (gateway emitted an empty value string, typically
    # with confidence=LOW) — distinct from ``Decimal("0")`` (a measured zero).
    # Empty≠Zero: renderers show "—" for None, never a fabricated "$0.00".
    value_usd: Decimal | None
    value_token0: Decimal | None
    value_token1: Decimal | None
    source: PositionSource
    confidence: PositionConfidence
    last_reconciled_at_block: int
    cutover_state: CutoverState
    primitive_payload_json: str
    value_as_of: str

    @property
    def is_lp(self) -> bool:
        return self.primitive == "lp"

    @property
    def is_open(self) -> bool:
        return self.status == PositionStatus.OPEN

    @property
    def value_as_of_datetime(self) -> datetime | None:
        if not self.value_as_of:
            return None
        try:
            return datetime.fromisoformat(self.value_as_of.replace("Z", "+00:00"))
        except ValueError:
            return None


@dataclass(frozen=True)
class GetPositionsResult:
    """Bundled response from GetPositions."""

    positions: list[PositionEntry]
    cutover_states: list[CutoverStateEntry]

    def by_accounting_category(self) -> dict[str, list[PositionEntry]]:
        """Group authoritative positions by accounting_category for header bucketing."""
        grouped: dict[str, list[PositionEntry]] = {}
        for p in self.positions:
            grouped.setdefault(p.accounting_category, []).append(p)
        return grouped

    def cutover_for(self, accounting_category: str) -> CutoverStateEntry | None:
        for entry in self.cutover_states:
            if entry.accounting_category == accounting_category:
                return entry
        return None


@dataclass(frozen=True)
class RangeHistoryEntry:
    """One row of LP / lending history from position_events or accounting_events."""

    timestamp_unix_seconds: int
    block_number: int
    event_type: str  # OPEN | ADJUST | COLLECT | CLOSE | TRANSFER | NONE
    source_table: str  # "position_events" | "accounting_events"
    ledger_entry_id: str
    tx_hash: str
    payload_json: str

    @property
    def timestamp(self) -> datetime | None:
        if not self.timestamp_unix_seconds:
            return None
        return datetime.fromtimestamp(self.timestamp_unix_seconds, tz=UTC)


@dataclass(frozen=True)
class GetRangeHistoryResult:
    entries: list[RangeHistoryEntry]
    # Populated for swap/prediction primitives — renderer shows in place of
    # an empty table. Empty entries + empty stub = genuinely no history.
    stub_message: str


@dataclass(frozen=True)
class PrimitiveCoverageStub:
    """Renderer card for primitives without a reconciliation parser."""

    primitive: str
    message: str
    ticket: str


@dataclass(frozen=True)
class ReconciliationFinding:
    """One three-way-diff row (ledger × snapshots × registry)."""

    accounting_category: str
    physical_identity_hash: str
    severity: ReconciliationSeverity
    delta: str
    ledger_has_row: bool
    snapshot_has_row: bool
    registry_has_row: bool
    suggested_action: str


@dataclass(frozen=True)
class ReconciliationReport:
    """Read-only reconciliation report; computed via GetReconciliationReport."""

    findings: list[ReconciliationFinding]
    primitive_stubs: list[PrimitiveCoverageStub]
    reconciliation_id: str
    source_block_number: int
    as_of: str

    @property
    def as_of_datetime(self) -> datetime | None:
        if not self.as_of:
            return None
        try:
            return datetime.fromisoformat(self.as_of.replace("Z", "+00:00"))
        except ValueError:
            return None

    @property
    def diverged_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == ReconciliationSeverity.DIVERGED)

    @property
    def warn_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == ReconciliationSeverity.WARN)


@dataclass(frozen=True)
class MatchedPosition:
    """On-chain ↔ registry agree."""

    physical_identity_hash: str
    primitive: str
    accounting_category: str
    confirmed_at_block: int


@dataclass(frozen=True)
class PhantomMissingPosition:
    """On-chain has a position; registry doesn't (the GH #2131 case)."""

    physical_identity_hash: str
    primitive: str
    accounting_category: str
    semantic_grouping_key: str
    payload_json: str
    opened_at_block: int
    opened_tx: str


@dataclass(frozen=True)
class StrandedRow:
    """Registry has status='open'; chain doesn't have the position anymore."""

    physical_identity_hash: str
    primitive: str
    accounting_category: str
    handle: str
    registry_row_json: str
    confirmed_absent_at_block: int
    absent_reason: str


@dataclass(frozen=True)
class RebuiltRow:
    """A registry row written because phantom-missing fired with apply=true."""

    physical_identity_hash: str
    primitive: str
    accounting_category: str
    source: str
    last_reconciled_at_block: int
    reconciliation_id: str
    registry_row_json: str


@dataclass(frozen=True)
class PrimitiveError:
    """Per-primitive partial failure."""

    primitive: str
    chain: str
    code: str
    message: str
    recoverable: bool


@dataclass(frozen=True)
class PreviewReconcileResult:
    """Idempotent dry-run reconcile result."""

    preview_token: str
    matched: list[MatchedPosition]
    phantom_missing: list[PhantomMissingPosition]
    stranded: list[StrandedRow]
    primitive_stubs: list[PrimitiveCoverageStub]
    reconciliation_id: str
    source_block_number: int
    expires_at_unix_seconds: int

    @property
    def expires_at(self) -> datetime | None:
        if not self.expires_at_unix_seconds:
            return None
        return datetime.fromtimestamp(self.expires_at_unix_seconds, tz=UTC)

    @property
    def has_diff(self) -> bool:
        """True iff there is at least one row to apply or inspect."""
        return bool(self.phantom_missing or self.stranded)


@dataclass(frozen=True)
class ApplyReconcileResult:
    """Apply outcome after consuming a preview_token."""

    result: str  # SUCCESS | PARTIAL_SUCCESS | STATE_DRIFT | EXPIRED | NOT_FOUND
    detail: str
    rebuilt: list[RebuiltRow] = field(default_factory=list)
    primitive_errors: list[PrimitiveError] = field(default_factory=list)
    reconciliation_id: str = ""

    @property
    def is_success(self) -> bool:
        return self.result == "SUCCESS"

    @property
    def needs_retry(self) -> bool:
        """Operator must re-issue PreviewReconcile + ApplyReconcile."""
        return self.result in ("STATE_DRIFT", "EXPIRED", "NOT_FOUND")


@dataclass(frozen=True)
class RefreshRegistryResult:
    """Outcome of an on-demand chain → registry refresh."""

    result: str  # SUCCESS | RATE_LIMITED | FAILED
    detail: str
    positions_refreshed: int
    events_emitted: int
    source_block_number: int
    reconciliation_id: str

    @property
    def is_success(self) -> bool:
        return self.result == "SUCCESS"


# =============================================================================
# Converters — proto → dataclass. Centralised so any new proto field becomes
# a one-line change here, never duplicated at call sites.
# =============================================================================


def _decimal_or_none(s: str) -> Decimal | None:
    """Parse a decimal-string but preserve the Empty≠Zero distinction.

    An empty proto string is the gateway's *unmeasured* sentinel (see the
    ``PositionEntry.value_usd`` proto comment "empty otherwise", and
    ``_dashboard_phase1.build_position_entry`` which emits ``""`` +
    ``confidence=LOW`` when the latest snapshot carries no valuation for a
    registry row). Collapsing that to ``Decimal("0")`` fabricates a measured
    zero and makes the Positions table render a confident ``$0.00`` that
    contradicts the position's real value on every other surface (VIB-5738
    cluster).

    Returns ``None`` for an empty string (unmeasured), an unparseable string
    (untrustworthy ⇒ also unmeasured, never a fake zero), AND a non-finite
    value (``NaN`` / ``±Infinity`` parse cleanly but are not measured values
    and would crash the ``,.2f`` formatter downstream). A literal ``"0"`` is a
    *measured* zero and is preserved as ``Decimal("0")``.
    """
    if not s:
        return None
    try:
        value = Decimal(s)
    except (InvalidOperation, ValueError, TypeError):
        return None
    return value if value.is_finite() else None


def _bytes_to_str(payload: bytes | str | None) -> str:
    """Coerce ``bytes`` proto fields (primitive_payload_json) to ``str`` for renderers."""
    if not payload:
        return ""
    if isinstance(payload, bytes):
        try:
            return payload.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("Non-UTF8 payload bytes encountered; falling back to empty string.")
            return ""
    return payload


def _convert_cutover_state_entry(proto: Any) -> CutoverStateEntry:
    return CutoverStateEntry(
        accounting_category=proto.accounting_category,
        state=CutoverState.from_proto(proto.state),
        rows_synthesized=proto.rows_synthesized,
        rows_skipped_already_present=proto.rows_skipped_already_present,
        backfill_started_at=proto.backfill_started_at,
        backfill_completed_at=proto.backfill_completed_at,
        backfill_reader_version=proto.backfill_reader_version,
        last_reconciled_at_block=proto.last_reconciled_at_block,
        last_reconciled_unix_seconds=proto.last_reconciled_unix_seconds,
    )


def _convert_position_entry(proto: Any) -> PositionEntry:
    return PositionEntry(
        handle=proto.handle,
        physical_identity_hash=proto.physical_identity_hash,
        deployment_id=proto.deployment_id,
        chain=proto.chain,
        primitive=proto.primitive,
        accounting_category=proto.accounting_category,
        status=PositionStatus.from_proto(proto.status),
        opened_at_block=proto.opened_at_block,
        closed_at_block=proto.closed_at_block,
        opened_tx=proto.opened_tx,
        closed_tx=proto.closed_tx,
        value_usd=_decimal_or_none(proto.value_usd),
        value_token0=_decimal_or_none(proto.value_token0),
        value_token1=_decimal_or_none(proto.value_token1),
        source=PositionSource.from_proto(proto.source),
        confidence=PositionConfidence.from_proto(proto.confidence),
        last_reconciled_at_block=proto.last_reconciled_at_block,
        cutover_state=CutoverState.from_proto(proto.cutover_state),
        primitive_payload_json=_bytes_to_str(proto.primitive_payload_json),
        value_as_of=proto.value_as_of,
    )


def _convert_range_history_entry(proto: Any) -> RangeHistoryEntry:
    return RangeHistoryEntry(
        timestamp_unix_seconds=proto.timestamp_unix_seconds,
        block_number=proto.block_number,
        event_type=proto.event_type,
        source_table=proto.source_table,
        ledger_entry_id=proto.ledger_entry_id,
        tx_hash=proto.tx_hash,
        payload_json=_bytes_to_str(proto.payload_json),
    )


def _convert_primitive_stub(proto: Any) -> PrimitiveCoverageStub:
    return PrimitiveCoverageStub(
        primitive=proto.primitive,
        message=proto.message,
        ticket=proto.ticket,
    )


def _convert_finding(proto: Any) -> ReconciliationFinding:
    return ReconciliationFinding(
        accounting_category=proto.accounting_category,
        physical_identity_hash=proto.physical_identity_hash,
        severity=ReconciliationSeverity.from_proto(proto.severity),
        delta=proto.delta,
        ledger_has_row=proto.ledger_has_row,
        snapshot_has_row=proto.snapshot_has_row,
        registry_has_row=proto.registry_has_row,
        suggested_action=proto.suggested_action,
    )


def _convert_matched(proto: Any) -> MatchedPosition:
    return MatchedPosition(
        physical_identity_hash=proto.physical_identity_hash,
        primitive=proto.primitive,
        accounting_category=proto.accounting_category,
        confirmed_at_block=proto.confirmed_at_block,
    )


def _convert_phantom_missing(proto: Any) -> PhantomMissingPosition:
    return PhantomMissingPosition(
        physical_identity_hash=proto.physical_identity_hash,
        primitive=proto.primitive,
        accounting_category=proto.accounting_category,
        semantic_grouping_key=proto.semantic_grouping_key,
        payload_json=_bytes_to_str(proto.payload_json),
        opened_at_block=proto.opened_at_block,
        opened_tx=proto.opened_tx,
    )


def _convert_stranded(proto: Any) -> StrandedRow:
    return StrandedRow(
        physical_identity_hash=proto.physical_identity_hash,
        primitive=proto.primitive,
        accounting_category=proto.accounting_category,
        handle=proto.handle,
        registry_row_json=_bytes_to_str(proto.registry_row_json),
        confirmed_absent_at_block=proto.confirmed_absent_at_block,
        absent_reason=proto.absent_reason,
    )


def _convert_rebuilt(proto: Any) -> RebuiltRow:
    return RebuiltRow(
        physical_identity_hash=proto.physical_identity_hash,
        primitive=proto.primitive,
        accounting_category=proto.accounting_category,
        source=proto.source,
        last_reconciled_at_block=proto.last_reconciled_at_block,
        reconciliation_id=proto.reconciliation_id,
        registry_row_json=_bytes_to_str(proto.registry_row_json),
    )


def _convert_primitive_error(proto: Any) -> PrimitiveError:
    return PrimitiveError(
        primitive=proto.primitive,
        chain=proto.chain,
        code=proto.code,
        message=proto.message,
        recoverable=proto.recoverable,
    )


# =============================================================================
# Errors
# =============================================================================


class DashboardClientError(Exception):
    """Raised on gateway connection or RPC failures from the Phase 2 facade."""


# =============================================================================
# Read-only facade — DashboardAPIClient
# =============================================================================


class DashboardServiceClient:
    """Read-only dashboard client over Phase 1 RPCs.

    Renderers MUST type-hint this class (not ``OperatorDashboardServiceClient``)
    so they cannot syntactically call mutation methods. Phase 4's CI lint
    enforces the import boundary.

    Construction:
        client = DashboardServiceClient()
        client.connect()
        try:
            result = client.get_positions("aave-avax")
        finally:
            client.disconnect()

    Or with a pre-built ``GatewayClient`` (e.g. shared singleton):
        client = DashboardServiceClient(gateway_client=get_gateway_client())
    """

    def __init__(self, gateway_client: GatewayClient | None = None) -> None:
        self._client: GatewayClient | None = gateway_client
        self._owns_client = gateway_client is None

    def connect(self) -> None:
        if self._client is None:
            self._client = get_gateway_client()
        if not self._client.is_connected:
            try:
                self._client.connect()
            except Exception as exc:
                raise DashboardClientError(f"Failed to connect to gateway: {exc}") from exc
        try:
            healthy = self._client.health_check()
        except Exception as exc:
            raise DashboardClientError(f"Gateway health check failed: {exc}") from exc
        if not healthy:
            raise DashboardClientError("Gateway is not healthy")

    def disconnect(self) -> None:
        if self._owns_client and self._client is not None:
            self._client.disconnect()

    @property
    def is_connected(self) -> bool:
        return self._client is not None and self._client.is_connected

    def _stub(self):
        """Return the dashboard service stub; raise if not connected.

        Keeps the connection-check pattern in one place so all RPC methods
        below share the same error semantics.
        """
        if self._client is None or not self._client.is_connected:
            raise DashboardClientError("Not connected to gateway. Call connect() first.")
        return self._client.dashboard

    # -------------------------------------------------------------------------
    # Phase 1 read RPCs
    # -------------------------------------------------------------------------

    def get_positions(
        self,
        deployment_id: str,
        *,
        chain: str = "",
        primitive: str = "",
        accounting_category: str = "",
        status: PositionStatus = PositionStatus.UNSPECIFIED,
    ) -> GetPositionsResult:
        """Authoritative positions feed sourced from ``position_registry``.

        Args:
            deployment_id: Required deployment identifier.
            chain: Optional chain filter (e.g. "ethereum", "avalanche").
            primitive: Optional primitive filter ("lp", "lending", "perp").
            accounting_category: Optional category filter ("LP_UNIV3", ...).
            status: Optional status filter; UNSPECIFIED returns all.

        Returns:
            ``GetPositionsResult`` with positions and per-category cutover
            snapshots for header bucketing.
        """
        request = gateway_pb2.GetPositionsRequest(
            deployment_id=deployment_id,
            chain=chain,
            primitive=primitive,
            accounting_category=accounting_category,
            status=status.to_proto(),
        )
        try:
            response = self._stub().GetPositions(request)
        except grpc.RpcError as exc:
            raise DashboardClientError(f"GetPositions failed: {exc}") from exc
        return GetPositionsResult(
            positions=[_convert_position_entry(p) for p in response.positions],
            cutover_states=[_convert_cutover_state_entry(c) for c in response.cutover_states],
        )

    def get_position_range_history(
        self,
        deployment_id: str,
        *,
        chain: str,
        accounting_category: str,
        handle: str = "",
        physical_identity_hash: str = "",
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> GetRangeHistoryResult:
        """LP / lending history for a single position.

        Exactly one of ``handle`` or ``physical_identity_hash`` is required;
        ``physical_identity_hash`` wins when both are supplied (it is the
        stable primary key — handles can be renamed).
        """
        if not handle and not physical_identity_hash:
            raise ValueError("get_position_range_history requires either handle or physical_identity_hash")
        request = gateway_pb2.GetPositionRangeHistoryRequest(
            deployment_id=deployment_id,
            chain=chain,
            accounting_category=accounting_category,
            handle=handle,
            physical_identity_hash=physical_identity_hash,
            from_unix_seconds=int(from_time.timestamp()) if from_time else 0,
            to_unix_seconds=int(to_time.timestamp()) if to_time else 0,
        )
        try:
            response = self._stub().GetPositionRangeHistory(request)
        except grpc.RpcError as exc:
            raise DashboardClientError(f"GetPositionRangeHistory failed: {exc}") from exc
        return GetRangeHistoryResult(
            entries=[_convert_range_history_entry(e) for e in response.entries],
            stub_message=response.stub_message,
        )

    def get_reconciliation_report(self, deployment_id: str) -> ReconciliationReport:
        """Three-way-diff reconciliation report (LP-only in v1; stubs for others).

        Server-side 5-second TTL cache means callers may call this freely
        from per-page-render code without overloading the registry or
        triggering RPC fanout.
        """
        request = gateway_pb2.GetReconciliationReportRequest(deployment_id=deployment_id)
        try:
            response = self._stub().GetReconciliationReport(request)
        except grpc.RpcError as exc:
            raise DashboardClientError(f"GetReconciliationReport failed: {exc}") from exc
        return ReconciliationReport(
            findings=[_convert_finding(f) for f in response.findings],
            primitive_stubs=[_convert_primitive_stub(s) for s in response.primitive_stubs],
            reconciliation_id=response.reconciliation_id,
            source_block_number=response.source_block_number,
            as_of=response.as_of,
        )


# =============================================================================
# Operator facade — OperatorDashboardClient
# =============================================================================


class OperatorDashboardServiceClient(DashboardServiceClient):
    """Read + write dashboard client. Mutation methods live here only.

    Use this class ONLY in operator-action code paths (toolbar buttons,
    CLI tools that mutate state). Renderers that display data should
    take a ``DashboardServiceClient`` instead — they will not have access
    to ``preview_reconcile`` / ``apply_reconcile`` /
    ``refresh_registry_from_chain``.

    Preview→Apply contract:
        result = operator.preview_reconcile("aave-avax")
        if result.has_diff:
            # Show diff in UI, get operator confirmation
            outcome = operator.apply_reconcile("aave-avax", result.preview_token)
            if outcome.needs_retry:
                # State drifted; re-preview
                ...
    """

    # -------------------------------------------------------------------------
    # Phase 1 mutation RPCs
    # -------------------------------------------------------------------------

    def preview_reconcile(self, deployment_id: str) -> PreviewReconcileResult:
        """Dry-run reconcile; returns a token bound to current state hashes.

        The token expires after a server-side TTL (default 5 minutes) and
        invalidates if the registry or ledger changes between preview and
        apply. Pass to ``apply_reconcile`` to apply this exact preview.
        """
        request = gateway_pb2.PreviewReconcileRequest(deployment_id=deployment_id)
        try:
            response = self._stub().PreviewReconcile(request)
        except grpc.RpcError as exc:
            raise DashboardClientError(f"PreviewReconcile failed: {exc}") from exc
        return PreviewReconcileResult(
            preview_token=response.preview_token,
            matched=[_convert_matched(m) for m in response.matched],
            phantom_missing=[_convert_phantom_missing(p) for p in response.phantom_missing],
            stranded=[_convert_stranded(s) for s in response.stranded],
            primitive_stubs=[_convert_primitive_stub(s) for s in response.primitive_stubs],
            reconciliation_id=response.reconciliation_id,
            source_block_number=response.source_block_number,
            expires_at_unix_seconds=response.expires_at_unix_seconds,
        )

    def apply_reconcile(self, deployment_id: str, preview_token: str) -> ApplyReconcileResult:
        """Apply a previously-issued preview, idempotently.

        Outcomes:
            SUCCESS          — all rows applied.
            PARTIAL_SUCCESS  — some succeeded, some failed; see primitive_errors.
            STATE_DRIFT      — state changed since preview was issued; re-preview.
            EXPIRED          — token TTL elapsed; re-preview.
            NOT_FOUND        — token unrecognized.
        """
        if not preview_token:
            raise ValueError("apply_reconcile requires a non-empty preview_token")
        request = gateway_pb2.ApplyReconcileRequest(
            deployment_id=deployment_id,
            preview_token=preview_token,
        )
        try:
            response = self._stub().ApplyReconcile(request)
        except grpc.RpcError as exc:
            raise DashboardClientError(f"ApplyReconcile failed: {exc}") from exc
        return ApplyReconcileResult(
            result=response.result,
            detail=response.detail,
            rebuilt=[_convert_rebuilt(r) for r in response.rebuilt],
            primitive_errors=[_convert_primitive_error(e) for e in response.primitive_errors],
            reconciliation_id=response.reconciliation_id,
        )

    def refresh_registry_from_chain(self, deployment_id: str) -> RefreshRegistryResult:
        """Force a fresh on-chain read pass for this strategy's registry.

        Gateway enforces a per-strategy lock so two concurrent operator
        clicks coalesce to one chain fanout (the second sees RATE_LIMITED).
        """
        request = gateway_pb2.RefreshRegistryFromChainRequest(deployment_id=deployment_id)
        try:
            response = self._stub().RefreshRegistryFromChain(request)
        except grpc.RpcError as exc:
            raise DashboardClientError(f"RefreshRegistryFromChain failed: {exc}") from exc
        return RefreshRegistryResult(
            result=response.result,
            detail=response.detail,
            positions_refreshed=response.positions_refreshed,
            events_emitted=response.events_emitted,
            source_block_number=response.source_block_number,
            reconciliation_id=response.reconciliation_id,
        )


# =============================================================================
# Singleton accessors. Two-tier: callers ask for read-only or operator.
# =============================================================================


_read_client: DashboardServiceClient | None = None
_operator_client: OperatorDashboardServiceClient | None = None


def get_dashboard_service_client() -> DashboardServiceClient:
    """Shared read-only dashboard client (singleton). Renderer-safe."""
    global _read_client
    if _read_client is None:
        _read_client = DashboardServiceClient()
    return _read_client


def get_operator_dashboard_service_client() -> OperatorDashboardServiceClient:
    """Shared operator dashboard client (singleton). DO NOT pass into renderers."""
    global _operator_client
    if _operator_client is None:
        _operator_client = OperatorDashboardServiceClient()
    return _operator_client


def reset_dashboard_service_clients() -> None:
    """Clear both singletons. Test fixtures should call this in teardown."""
    global _read_client, _operator_client
    if _read_client is not None:
        _read_client.disconnect()
        _read_client = None
    if _operator_client is not None:
        _operator_client.disconnect()
        _operator_client = None
