"""Gateway-backed client for dashboard data access.

This client replaces direct filesystem/database access in the dashboard.
All data is fetched from the gateway via gRPC.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import grpc

from almanak.framework.gateway_client import GatewayClient, get_gateway_client
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


@dataclass
class StrategySummary:
    """Summary of a strategy for dashboard display."""

    deployment_id: str
    name: str
    status: str
    chain: str
    protocol: str
    total_value_usd: Decimal
    pnl_24h_usd: Decimal
    last_action_at: datetime | None
    attention_required: bool
    attention_reason: str
    is_multi_chain: bool
    chains: list[str] = field(default_factory=list)
    execution_mode: str = "live"
    paper_metrics_json: str = ""


@dataclass
class TokenBalance:
    """Token balance information."""

    symbol: str
    balance: Decimal
    value_usd: Decimal


@dataclass
class LPPosition:
    """LP position information."""

    pool: str
    token0: str
    token1: str
    liquidity_usd: Decimal
    range_lower: Decimal
    range_upper: Decimal
    current_price: Decimal
    in_range: bool


@dataclass
class PositionInfo:
    """Position information for a strategy."""

    token_balances: list[TokenBalance] = field(default_factory=list)
    lp_positions: list[LPPosition] = field(default_factory=list)
    total_lp_value_usd: Decimal = Decimal("0")
    health_factor: Decimal | None = None
    leverage: Decimal | None = None


@dataclass
class TimelineEvent:
    """A timeline event."""

    timestamp: datetime | None  # None when timestamp data is missing from source
    event_type: str
    description: str
    tx_hash: str | None = None
    chain: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    cycle_id: str = ""
    phase: str = ""
    # VIB-4041 — typed pointer to transaction_ledger.id when this event narrates
    # an executed intent. Renderers should look up the source-of-truth row
    # rather than reading money-shaped keys from `details`.
    related_ledger_entry_id: str = ""


@dataclass
class StrategyDetails:
    """Detailed strategy information."""

    summary: StrategySummary
    position: PositionInfo
    timeline: list[TimelineEvent] = field(default_factory=list)
    pnl_history: list[dict] = field(default_factory=list)


@dataclass
class LedgerTradeRecord:
    """A trade record from the transaction ledger (protobuf-free)."""

    id: str
    cycle_id: str
    deployment_id: str
    timestamp: datetime | None
    intent_type: str
    token_in: str
    amount_in: str
    token_out: str
    amount_out: str
    effective_price: str
    slippage_bps: float
    gas_used: int
    gas_usd: str
    tx_hash: str
    chain: str
    protocol: str
    success: bool
    error: str


@dataclass
class ActivityFeedItem:
    """One row in the merged activity feed (VIB-4042 / PR3).

    Exactly one of `timeline_event` / `ledger_entry` is populated. The renderer
    switches on `kind` rather than peeking at field presence, which matches
    the gateway's compositor contract.
    """

    kind: str  # "TIMELINE_EVENT" | "LEDGER_ENTRY"
    timestamp: datetime | None
    deployment_id: str
    cycle_id: str
    timeline_event: TimelineEvent | None = None
    ledger_entry: LedgerTradeRecord | None = None


@dataclass
class ActivityFeedResponse:
    """Paginated activity feed response.

    The cursor is composite: pass both ``next_before`` and ``next_before_id``
    back to the next ``get_activity_feed`` call. When two items share the
    boundary timestamp, ``next_before_id`` is the tie-breaker so a tied item
    is never returned twice nor skipped (VIB-4042 / Gemini review).

    ``backfill_truncated`` is True only when the gateway hit
    ``MAX_BACKFILL_ATTEMPTS`` without filling the page AND at least one
    underlying stream still has rows (saturated tie-second). Renderers
    MUST surface this distinct from "end of feed" so operators can spot
    the pathological case (CodeRabbit on PR #2117).
    """

    items: list[ActivityFeedItem]
    has_more: bool
    next_before: datetime | None
    next_before_id: str = ""
    backfill_truncated: bool = False


@dataclass
class PnLSummary:
    """5-second-eyeball card from ``GetPnLSummary`` (VIB-3969).

    Wallet-level money trail + cash buffer + primary-risk gauge. The
    decomposed slice of ``QuantHeaderInfo`` that PnL consumers can pull
    without paying for G6 reconciliation or 21-cell Accountant Test
    posture.
    """

    deployed_usd: Decimal
    nav_usd: Decimal
    lifetime_pnl_usd: Decimal
    lifetime_pnl_pct: Decimal
    net_apr_pct: Decimal
    max_drawdown_pct: Decimal
    current_drawdown_pct: Decimal
    value_confidence: str
    age_days: int
    deployed_capital_usd: Decimal
    available_cash_usd: Decimal
    open_position_count: int
    primary_risk_kind: str
    primary_risk_label: str
    primary_risk_value: str
    primary_risk_color: str


@dataclass
class CostStackInfo:
    """Life-to-date cost / earn decomposition from ``GetCostStack`` (VIB-3969)."""

    cost_gas_usd: Decimal
    cost_protocol_fees_usd: Decimal
    cost_slippage_usd: Decimal
    fees_earned_usd: Decimal
    interest_paid_usd: Decimal
    interest_earned_usd: Decimal
    funding_paid_usd: Decimal
    funding_earned_usd: Decimal
    realized_pnl_usd: Decimal
    il_usd: Decimal
    # VIB-4984: mark-to-market of held directional swap inventory.
    # None = unmeasured (Empty≠Zero), set presence-aware from the proto.
    inventory_unrealized_usd: Decimal | None = None


@dataclass
class AuditPosture:
    """Reconciliation + audit-trail completeness + Accountant Test posture
    from ``GetAuditPosture`` (VIB-3969).

    Server-computed only — the gateway is the single source of truth for
    G6 math. Clients must NOT recompute G6 from sub-fields, or the
    dashboard and the accountant test will drift.
    """

    g6_status: str
    g6_wallet_pnl_usd: Decimal
    g6_component_pnl_usd: Decimal
    g6_gap_usd: Decimal
    g6_epsilon_usd: Decimal
    g6_components: dict[str, Decimal]
    ledger_total: int
    ledger_with_price_inputs: int
    ledger_with_pre_post_state: int
    ledger_with_gas_usd: int
    events_total: int
    events_with_versions: int
    primitive: str
    cells_passed: int
    cells_failed: int
    cells_xfail: int
    cells_total: int
    failing_cells: list[str]
    xfail_cells: list[str]


@dataclass
class TradeTapeRow:
    """One row of the trade tape — ledger × accounting × position event."""

    id: str
    cycle_id: str
    timestamp: datetime | None
    intent_type: str
    token_in: str
    amount_in: str
    token_out: str
    amount_out: str
    effective_price: str
    slippage_bps: float
    gas_used: int
    gas_usd: str
    tx_hash: str
    chain: str
    protocol: str
    success: bool
    error: str
    amount_in_usd: str
    amount_out_usd: str
    extracted_data_json: str
    price_inputs_json: str
    pre_state_json: str
    post_state_json: str
    accounting_payload_json: str
    accounting_event_type: str
    position_key: str
    confidence: str
    unavailable_reason: str
    schema_version: int
    formula_version: int
    matching_policy_version: int
    position_event_json: str
    position_id: str
    position_event_type: str


@dataclass
class TradeTapeResponse:
    rows: list[TradeTapeRow]
    has_more: bool


def _safe_decimal(s: str) -> Decimal:
    if not s:
        return Decimal("0")
    try:
        return Decimal(s)
    except (ValueError, TypeError):
        return Decimal("0")


def _safe_optional_decimal(s: str) -> Decimal | None:
    """Presence-aware proto-string → Decimal (VIB-4984; Empty≠Zero).

    Returns ``None`` for an empty / unparseable proto string (unmeasured),
    NOT ``Decimal("0")``. Use for optional proto fields where "" carries the
    "the gateway did not measure this" signal (e.g. inventory_unrealized_usd).
    """
    if not s:
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError, TypeError):
        return None


def _convert_pnl_summary(proto: gateway_pb2.PnLSummary) -> PnLSummary:
    return PnLSummary(
        deployed_usd=_safe_decimal(proto.deployed_usd),
        nav_usd=_safe_decimal(proto.nav_usd),
        lifetime_pnl_usd=_safe_decimal(proto.lifetime_pnl_usd),
        lifetime_pnl_pct=_safe_decimal(proto.lifetime_pnl_pct),
        net_apr_pct=_safe_decimal(proto.net_apr_pct),
        max_drawdown_pct=_safe_decimal(proto.max_drawdown_pct),
        current_drawdown_pct=_safe_decimal(proto.current_drawdown_pct),
        value_confidence=proto.value_confidence or "UNAVAILABLE",
        age_days=proto.age_days,
        deployed_capital_usd=_safe_decimal(proto.deployed_capital_usd),
        available_cash_usd=_safe_decimal(proto.available_cash_usd),
        open_position_count=proto.open_position_count,
        primary_risk_kind=proto.primary_risk_kind or "none",
        primary_risk_label=proto.primary_risk_label or "No active positions",  # VIB-3925
        primary_risk_value=proto.primary_risk_value or "",
        primary_risk_color=proto.primary_risk_color or "neutral",
    )


def _convert_cost_stack(proto: gateway_pb2.CostStackInfo) -> CostStackInfo:
    return CostStackInfo(
        cost_gas_usd=_safe_decimal(proto.cost_gas_usd),
        cost_protocol_fees_usd=_safe_decimal(proto.cost_protocol_fees_usd),
        cost_slippage_usd=_safe_decimal(proto.cost_slippage_usd),
        fees_earned_usd=_safe_decimal(proto.fees_earned_usd),
        interest_paid_usd=_safe_decimal(proto.interest_paid_usd),
        interest_earned_usd=_safe_decimal(proto.interest_earned_usd),
        funding_paid_usd=_safe_decimal(proto.funding_paid_usd),
        funding_earned_usd=_safe_decimal(proto.funding_earned_usd),
        realized_pnl_usd=_safe_decimal(proto.realized_pnl_usd),
        il_usd=_safe_decimal(proto.il_usd),
        # VIB-4984: presence-aware — "" => None (unmeasured), never Decimal("0").
        inventory_unrealized_usd=_safe_optional_decimal(proto.inventory_unrealized_usd),
    )


def _convert_audit_posture(proto: gateway_pb2.AuditPosture) -> AuditPosture:
    return AuditPosture(
        g6_status=proto.g6_status or "NA",
        g6_wallet_pnl_usd=_safe_decimal(proto.g6_wallet_pnl_usd),
        g6_component_pnl_usd=_safe_decimal(proto.g6_component_pnl_usd),
        g6_gap_usd=_safe_decimal(proto.g6_gap_usd),
        g6_epsilon_usd=_safe_decimal(proto.g6_epsilon_usd),
        g6_components={
            "swap": _safe_decimal(proto.g6_sum_swap),
            "lp": _safe_decimal(proto.g6_sum_lp),
            "perp": _safe_decimal(proto.g6_sum_perp),
            "fees": _safe_decimal(proto.g6_sum_fees),
            "funding": _safe_decimal(proto.g6_sum_funding),
            "interest": _safe_decimal(proto.g6_sum_interest),
            "gas": _safe_decimal(proto.g6_sum_gas),
        },
        ledger_total=proto.ledger_total,
        ledger_with_price_inputs=proto.ledger_with_price_inputs,
        ledger_with_pre_post_state=proto.ledger_with_pre_post_state,
        ledger_with_gas_usd=proto.ledger_with_gas_usd,
        events_total=proto.events_total,
        events_with_versions=proto.events_with_versions,
        primitive=proto.primitive or "mixed",
        cells_passed=proto.cells_passed,
        cells_failed=proto.cells_failed,
        cells_xfail=proto.cells_xfail,
        cells_total=proto.cells_total or 21,
        failing_cells=list(proto.failing_cells),
        xfail_cells=list(proto.xfail_cells),
    )


def _convert_trade_tape_row(proto: gateway_pb2.TradeTapeRow) -> TradeTapeRow:
    return TradeTapeRow(
        id=proto.id,
        cycle_id=proto.cycle_id,
        timestamp=datetime.fromtimestamp(proto.timestamp, tz=UTC) if proto.timestamp else None,
        intent_type=proto.intent_type,
        token_in=proto.token_in,
        amount_in=proto.amount_in,
        token_out=proto.token_out,
        amount_out=proto.amount_out,
        effective_price=proto.effective_price,
        slippage_bps=proto.slippage_bps,
        gas_used=proto.gas_used,
        gas_usd=proto.gas_usd,
        tx_hash=proto.tx_hash,
        chain=proto.chain,
        protocol=proto.protocol,
        success=proto.success,
        error=proto.error,
        amount_in_usd=proto.amount_in_usd,
        amount_out_usd=proto.amount_out_usd,
        extracted_data_json=proto.extracted_data_json,
        price_inputs_json=proto.price_inputs_json,
        pre_state_json=proto.pre_state_json,
        post_state_json=proto.post_state_json,
        accounting_payload_json=proto.accounting_payload_json,
        accounting_event_type=proto.accounting_event_type,
        position_key=proto.position_key,
        confidence=proto.confidence,
        unavailable_reason=proto.unavailable_reason,
        schema_version=proto.schema_version,
        formula_version=proto.formula_version,
        matching_policy_version=proto.matching_policy_version,
        position_event_json=proto.position_event_json,
        position_id=proto.position_id,
        position_event_type=proto.position_event_type,
    )


class GatewayConnectionError(Exception):
    """Raised when gateway connection fails."""

    pass


class GatewayDashboardClient:
    """Client for fetching dashboard data from gateway.

    This client provides a high-level interface for the dashboard to access
    strategy data through the gateway. All data comes via gRPC calls to the
    gateway's DashboardService.

    Usage:
        client = GatewayDashboardClient()
        client.connect()
        try:
            strategies = client.list_strategies()
            details = client.get_strategy_details("my-strategy")
        finally:
            client.disconnect()
    """

    def __init__(self, gateway_client: GatewayClient | None = None):
        """Initialize the dashboard client.

        Args:
            gateway_client: Optional GatewayClient instance. If not provided,
                           uses the default singleton client.
        """
        self._client: GatewayClient | None = gateway_client
        self._owns_client = gateway_client is None

    def connect(self) -> None:
        """Connect to the gateway.

        Raises:
            GatewayConnectionError: If connection fails.
        """
        if self._client is None:
            self._client = get_gateway_client()

        if not self._client.is_connected:
            try:
                self._client.connect()
            except Exception as e:
                raise GatewayConnectionError(f"Failed to connect to gateway: {e}") from e

        # Check if gateway is healthy
        if not self._client.health_check():
            raise GatewayConnectionError("Gateway is not healthy")

    def disconnect(self) -> None:
        """Disconnect from the gateway."""
        if self._owns_client and self._client is not None:
            self._client.disconnect()

    @property
    def is_connected(self) -> bool:
        """Check if connected to gateway."""
        return self._client is not None and self._client.is_connected

    def _ensure_connected(self) -> GatewayClient:
        """Ensure client is connected and return the client.

        Returns:
            The connected GatewayClient instance.

        Raises:
            GatewayConnectionError: If not connected.
        """
        if self._client is None or not self._client.is_connected:
            raise GatewayConnectionError("Not connected to gateway. Call connect() first.")
        return self._client

    def list_strategies(
        self,
        status_filter: str | None = None,
        chain_filter: str | None = None,
        include_position: bool = False,
    ) -> list[StrategySummary]:
        """List executed/running strategies from the instance registry.

        Args:
            status_filter: Source mode ("REGISTRY", "AVAILABLE", "ALL") or
                status filter ("RUNNING", "PAUSED", etc.). Default: "REGISTRY".
            chain_filter: Filter by chain name
            include_position: Include position summary (more expensive)

        Returns:
            List of StrategySummary objects
        """
        client = self._ensure_connected()

        request = gateway_pb2.ListStrategiesRequest(
            status_filter=status_filter or "REGISTRY",
            chain_filter=chain_filter or "",
            include_position=include_position,
        )

        try:
            response = client.dashboard.ListStrategies(request)
        except grpc.RpcError as e:
            logger.exception("Failed to list strategies")
            raise GatewayConnectionError(f"Failed to list strategies: {e}") from e

        return [self._convert_summary(s) for s in response.strategies]

    def list_available_strategies(
        self,
        chain_filter: str | None = None,
    ) -> list[StrategySummary]:
        """List available strategy templates from the filesystem.

        These are strategies with config.json files that haven't been
        executed yet. Used by the Strategy Library page.

        Args:
            chain_filter: Filter by chain name

        Returns:
            List of StrategySummary objects
        """
        client = self._ensure_connected()

        request = gateway_pb2.ListStrategiesRequest(
            status_filter="AVAILABLE",
            chain_filter=chain_filter or "",
            include_position=False,
        )

        try:
            response = client.dashboard.ListStrategies(request)
        except grpc.RpcError as e:
            logger.exception("Failed to list available strategies")
            raise GatewayConnectionError(f"Failed to list available strategies: {e}") from e

        return [self._convert_summary(s) for s in response.strategies]

    def get_strategy_details(
        self,
        deployment_id: str,
        include_timeline: bool = True,
        include_pnl_history: bool = False,
        timeline_limit: int = 20,
    ) -> StrategyDetails:
        """Get detailed information about a strategy.

        Args:
            deployment_id: Deployment identifier
            include_timeline: Include recent timeline events
            include_pnl_history: Include PnL history for charts
            timeline_limit: Maximum number of timeline events

        Returns:
            StrategyDetails object
        """
        client = self._ensure_connected()

        request = gateway_pb2.GetStrategyDetailsRequest(
            deployment_id=deployment_id,
            include_timeline=include_timeline,
            include_pnl_history=include_pnl_history,
            timeline_limit=timeline_limit,
        )

        try:
            response = client.dashboard.GetStrategyDetails(request)
        except grpc.RpcError as e:
            logger.exception("Failed to get strategy details")
            raise GatewayConnectionError(f"Failed to get strategy details: {e}") from e

        return self._convert_details(response)

    def get_timeline(
        self,
        deployment_id: str,
        limit: int = 50,
        event_type_filter: str | None = None,
    ) -> list[TimelineEvent]:
        """Get timeline events for a strategy.

        Args:
            deployment_id: Deployment identifier
            limit: Maximum number of events to return
            event_type_filter: Filter by event type

        Returns:
            List of TimelineEvent objects
        """
        client = self._ensure_connected()

        request = gateway_pb2.GetTimelineRequest(
            deployment_id=deployment_id,
            limit=limit,
            event_type_filter=event_type_filter or "",
        )

        try:
            response = client.dashboard.GetTimeline(request)
        except grpc.RpcError as e:
            logger.exception("Failed to get timeline")
            raise GatewayConnectionError(f"Failed to get timeline: {e}") from e

        return [self._convert_timeline_event(e) for e in response.events]

    def get_strategy_config(self, deployment_id: str) -> dict[str, Any]:
        """Get strategy configuration.

        Args:
            deployment_id: Deployment identifier

        Returns:
            Configuration dictionary
        """
        client = self._ensure_connected()

        request = gateway_pb2.GetStrategyConfigRequest(deployment_id=deployment_id)

        try:
            response = client.dashboard.GetStrategyConfig(request)
        except grpc.RpcError as e:
            logger.exception("Failed to get strategy config")
            raise GatewayConnectionError(f"Failed to get strategy config: {e}") from e

        if response.config_json:
            try:
                return json.loads(response.config_json)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to decode strategy config JSON for {deployment_id}: {e}")
                return {}
        return {}

    def get_strategy_state(
        self,
        deployment_id: str,
        fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """Get current strategy state.

        Args:
            deployment_id: Deployment identifier
            fields: Optional list of specific fields to return

        Returns:
            State dictionary
        """
        client = self._ensure_connected()

        request = gateway_pb2.GetStrategyStateRequest(
            deployment_id=deployment_id,
            fields=fields or [],
        )

        try:
            response = client.dashboard.GetStrategyState(request)
        except grpc.RpcError as e:
            logger.exception("Failed to get strategy state")
            raise GatewayConnectionError(f"Failed to get strategy state: {e}") from e

        if response.state_json:
            try:
                return json.loads(response.state_json)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to decode strategy state JSON for {deployment_id}: {e}")
                return {}
        return {}

    def execute_action(
        self,
        deployment_id: str,
        action: str,
        reason: str,
        params: dict[str, str] | None = None,
    ) -> bool:
        """Execute operator action (pause, resume, etc.).

        Args:
            deployment_id: Deployment identifier
            action: Action to execute ("PAUSE", "RESUME", etc.)
            reason: Reason for the action (required for audit)
            params: Optional action-specific parameters

        Returns:
            True if successful, False otherwise
        """
        client = self._ensure_connected()

        request = gateway_pb2.ExecuteActionRequest(
            deployment_id=deployment_id,
            action=action,
            reason=reason,
            params=params or {},
        )

        try:
            response = client.dashboard.ExecuteAction(request)
            if not response.success:
                logger.warning(f"Action {action} failed: {response.error}")
            return response.success
        except Exception:
            logger.exception("Failed to execute action")
            return False

    def archive_strategy_instance(self, deployment_id: str, reason: str = "") -> bool:
        """Archive a strategy instance (hidden from dashboard, data retained).

        Args:
            deployment_id: Strategy instance ID to archive.
            reason: Reason for archiving (for audit).

        Returns:
            True if successful.
        """
        client = self._ensure_connected()

        request = gateway_pb2.ArchiveInstanceRequest(
            deployment_id=deployment_id,
            reason=reason,
        )

        try:
            response = client.dashboard.ArchiveStrategyInstance(request)
            if not response.success:
                logger.warning(f"Archive failed for {deployment_id}: {response.error}")
            return response.success
        except Exception:
            logger.exception("Failed to archive strategy instance")
            return False

    def purge_strategy_instance(self, deployment_id: str, reason: str) -> bool:
        """Purge a strategy instance and all its events (permanent delete).

        Args:
            deployment_id: Strategy instance ID to purge.
            reason: Reason for purging (required for audit trail).

        Returns:
            True if successful.

        Raises:
            ValueError: If reason is empty.
        """
        if not reason:
            raise ValueError("reason is required when purging a strategy instance")

        client = self._ensure_connected()

        request = gateway_pb2.PurgeInstanceRequest(
            deployment_id=deployment_id,
            reason=reason,
        )

        try:
            response = client.dashboard.PurgeStrategyInstance(request)
            if not response.success:
                logger.warning(f"Purge failed for {deployment_id}: {response.error}")
            return response.success
        except Exception:
            logger.exception("Failed to purge strategy instance")
            return False

    # =========================================================================
    # Conversion helpers
    # =========================================================================

    def get_transaction_ledger(
        self,
        deployment_id: str,
        since: datetime | None = None,
        intent_type: str | None = None,
        limit: int = 100,
    ) -> list[LedgerTradeRecord]:
        """Get trade records from the transaction ledger.

        Args:
            deployment_id: Strategy to query.
            since: Only entries after this timestamp.
            intent_type: Filter by intent type (e.g. "SWAP", "BORROW").
            limit: Maximum entries to return.

        Returns:
            List of LedgerTradeRecord dataclasses.
        """
        client = self._ensure_connected()

        since_ts = int(since.timestamp()) if since else 0
        request = gateway_pb2.GetTransactionLedgerRequest(
            deployment_id=deployment_id,
            since_timestamp=since_ts,
            intent_type_filter=intent_type or "",
            limit=limit,
        )
        response = client.dashboard.GetTransactionLedger(request)

        return [self._convert_ledger_trade_record(entry) for entry in response.entries]

    def get_pnl_summary(self, deployment_id: str) -> "PnLSummary":
        """5-second-eyeball card via gateway (VIB-3969)."""
        client = self._ensure_connected()
        request = gateway_pb2.GetPnLSummaryRequest(deployment_id=deployment_id)
        try:
            response = client.dashboard.GetPnLSummary(request)
        except grpc.RpcError as e:
            logger.exception("Failed to get PnL summary")
            raise GatewayConnectionError(f"Failed to get PnL summary: {e}") from e
        return _convert_pnl_summary(response)

    def get_cost_stack(self, deployment_id: str) -> "CostStackInfo":
        """Life-to-date cost / earn decomposition via gateway (VIB-3969)."""
        client = self._ensure_connected()
        request = gateway_pb2.GetCostStackRequest(deployment_id=deployment_id)
        try:
            response = client.dashboard.GetCostStack(request)
        except grpc.RpcError as e:
            logger.exception("Failed to get cost stack")
            raise GatewayConnectionError(f"Failed to get cost stack: {e}") from e
        return _convert_cost_stack(response)

    def get_audit_posture(self, deployment_id: str) -> "AuditPosture":
        """Reconciliation + audit-trail completeness + Accountant Test
        posture via gateway (VIB-3969). Server-computed only — never
        reconstruct G6 from sub-fields client-side."""
        client = self._ensure_connected()
        request = gateway_pb2.GetAuditPostureRequest(deployment_id=deployment_id)
        try:
            response = client.dashboard.GetAuditPosture(request)
        except grpc.RpcError as e:
            logger.exception("Failed to get audit posture")
            raise GatewayConnectionError(f"Failed to get audit posture: {e}") from e
        return _convert_audit_posture(response)

    def get_trade_tape(
        self,
        deployment_id: str,
        limit: int = 50,
        before: datetime | None = None,
    ) -> "TradeTapeResponse":
        """Get the joined trade-tape view (one row per intent)."""
        client = self._ensure_connected()
        request = gateway_pb2.GetTradeTapeRequest(
            deployment_id=deployment_id,
            limit=limit,
            before_timestamp=int(before.timestamp()) if before else 0,
        )
        response = client.dashboard.GetTradeTape(request)
        rows = [_convert_trade_tape_row(r) for r in response.rows]
        return TradeTapeResponse(rows=rows, has_more=response.has_more)

    def get_activity_feed(
        self,
        deployment_id: str,
        limit: int = 50,
        before: datetime | None = None,
        event_type: str | None = None,
        intent_type: str | None = None,
        before_id: str = "",
    ) -> ActivityFeedResponse:
        """Get the merged activity feed (timeline + ledger) for a strategy.

        Per PRD-TimelineEvents §6.1 / §9, this is the only correct way for the
        dashboard to read a strategy's history — the gateway owns the merge,
        the dedup, and the cursor. Don't recompose `get_timeline()` and
        `get_transaction_ledger()` client-side; they will drift.

        Args:
            deployment_id: Deployment identifier.
            limit: Page size (server caps at 200).
            before: Cursor — only items at or before this timestamp.
            event_type: Optional timeline-side event-type filter.
            intent_type: Optional ledger-side intent-type filter.
            before_id: Composite tie-breaker. Pass the previous response's
                ``next_before_id`` so multiple items at the boundary timestamp
                paginate deterministically (VIB-4042 / Gemini review).
        """
        client = self._ensure_connected()
        request = gateway_pb2.GetActivityFeedRequest(
            deployment_id=deployment_id,
            limit=limit,
            before_timestamp=int(before.timestamp()) if before else 0,
            before_id=before_id,
            event_type_filter=event_type or "",
            intent_type_filter=intent_type or "",
        )
        try:
            response = client.dashboard.GetActivityFeed(request)
        except grpc.RpcError as e:
            logger.exception("Failed to get activity feed")
            raise GatewayConnectionError(f"Failed to get activity feed: {e}") from e

        items: list[ActivityFeedItem] = []
        for proto in response.items:
            kind_value = proto.kind
            if kind_value == gateway_pb2.ActivityFeedItem.Kind.TIMELINE_EVENT:
                kind = "TIMELINE_EVENT"
                timeline_event = self._convert_timeline_event(proto.timeline_event)
                ledger_entry = None
            elif kind_value == gateway_pb2.ActivityFeedItem.Kind.LEDGER_ENTRY:
                kind = "LEDGER_ENTRY"
                timeline_event = None
                ledger_entry = self._convert_ledger_trade_record(proto.ledger_entry)
            else:
                # CodeRabbit on PR #2117: skip unknown kinds entirely instead of
                # appending a blank ``ActivityFeedItem``. A placeholder with no
                # payload still counts toward ``limit`` / pagination / renderer
                # loops — an unexpected enum value would silently turn a full
                # page into a partially blank one and hide real rows behind
                # ``has_more``. Log and continue so the page accounting stays
                # honest.
                logger.warning(
                    "GetActivityFeed returned unknown ActivityFeedItem.Kind=%r; "
                    "skipping. Client and gateway proto stubs may be out of sync.",
                    kind_value,
                )
                continue

            items.append(
                ActivityFeedItem(
                    kind=kind,
                    timestamp=datetime.fromtimestamp(proto.timestamp, tz=UTC) if proto.timestamp else None,
                    deployment_id=proto.deployment_id,
                    cycle_id=proto.cycle_id,
                    timeline_event=timeline_event,
                    ledger_entry=ledger_entry,
                )
            )

        next_before = (
            datetime.fromtimestamp(response.next_before_timestamp, tz=UTC) if response.next_before_timestamp else None
        )
        return ActivityFeedResponse(
            items=items,
            has_more=response.has_more,
            next_before=next_before,
            next_before_id=response.next_before_id,
            backfill_truncated=response.backfill_truncated,
        )

    def _convert_summary(self, proto: gateway_pb2.StrategySummary) -> StrategySummary:
        """Convert protobuf StrategySummary to dataclass."""
        return StrategySummary(
            deployment_id=proto.deployment_id,
            name=proto.name,
            status=proto.status,
            chain=proto.chain,
            protocol=proto.protocol,
            total_value_usd=Decimal(proto.total_value_usd) if proto.total_value_usd else Decimal("0"),
            pnl_24h_usd=Decimal(proto.pnl_24h_usd) if proto.pnl_24h_usd else Decimal("0"),
            last_action_at=datetime.fromtimestamp(proto.last_action_at, tz=UTC) if proto.last_action_at else None,
            attention_required=proto.attention_required,
            attention_reason=proto.attention_reason,
            is_multi_chain=proto.is_multi_chain,
            chains=list(proto.chains),
            execution_mode=proto.execution_mode or "live",
            paper_metrics_json=proto.paper_metrics_json or "",
        )

    def _convert_details(self, proto: gateway_pb2.StrategyDetails) -> StrategyDetails:
        """Convert protobuf StrategyDetails to dataclass."""
        summary = self._convert_summary(proto.summary)

        # Convert position
        position = PositionInfo()
        if proto.position:
            position.token_balances = [
                TokenBalance(
                    symbol=b.symbol,
                    balance=Decimal(b.balance) if b.balance else Decimal("0"),
                    value_usd=Decimal(b.value_usd) if b.value_usd else Decimal("0"),
                )
                for b in proto.position.token_balances
            ]
            position.lp_positions = [
                LPPosition(
                    pool=p.pool,
                    token0=p.token0,
                    token1=p.token1,
                    liquidity_usd=Decimal(p.liquidity_usd) if p.liquidity_usd else Decimal("0"),
                    range_lower=Decimal(p.range_lower) if p.range_lower else Decimal("0"),
                    range_upper=Decimal(p.range_upper) if p.range_upper else Decimal("0"),
                    current_price=Decimal(p.current_price) if p.current_price else Decimal("0"),
                    in_range=p.in_range,
                )
                for p in proto.position.lp_positions
            ]
            if proto.position.total_lp_value_usd:
                position.total_lp_value_usd = Decimal(proto.position.total_lp_value_usd)
            if proto.position.health_factor:
                position.health_factor = Decimal(proto.position.health_factor)
            if proto.position.leverage:
                position.leverage = Decimal(proto.position.leverage)

        # Convert timeline
        timeline = [self._convert_timeline_event(e) for e in proto.timeline]

        # Convert pnl_history - filter out entries without valid timestamps
        pnl_history = [
            {
                "timestamp": datetime.fromtimestamp(p.timestamp, tz=UTC),
                "value_usd": Decimal(p.value_usd) if p.value_usd else Decimal("0"),
                "pnl_usd": Decimal(p.pnl_usd) if p.pnl_usd else Decimal("0"),
            }
            for p in proto.pnl_history
            if p.timestamp  # Only include entries with valid timestamps
        ]

        return StrategyDetails(
            summary=summary,
            position=position,
            timeline=timeline,
            pnl_history=pnl_history,
        )

    def _convert_ledger_trade_record(self, entry: Any) -> LedgerTradeRecord:
        """Convert protobuf ``LedgerEntry`` to ``LedgerTradeRecord`` dataclass.

        CodeRabbit on PR #2117 round 5: this conversion was previously
        duplicated across ``get_transaction_ledger`` and the
        ``get_activity_feed`` ``LEDGER_ENTRY`` branch. A drift bug here
        (missed proto field, type mismatch, timestamp tz handling) would
        only surface in one of the two call sites — exactly the
        asymmetric-write pattern that has bitten this codebase repeatedly.
        Centralising the converter ensures the two surfaces evolve in
        lockstep and any new ``LedgerEntry`` field added to
        ``gateway.proto`` is mapped exactly once.
        """
        return LedgerTradeRecord(
            id=entry.id,
            cycle_id=entry.cycle_id,
            deployment_id=entry.deployment_id,
            timestamp=datetime.fromtimestamp(entry.timestamp, tz=UTC) if entry.timestamp else None,
            intent_type=entry.intent_type,
            token_in=entry.token_in,
            amount_in=entry.amount_in,
            token_out=entry.token_out,
            amount_out=entry.amount_out,
            effective_price=entry.effective_price,
            slippage_bps=entry.slippage_bps,
            gas_used=entry.gas_used,
            gas_usd=entry.gas_usd,
            tx_hash=entry.tx_hash,
            chain=entry.chain,
            protocol=entry.protocol,
            success=entry.success,
            error=entry.error,
        )

    def _convert_timeline_event(self, proto: gateway_pb2.TimelineEventInfo) -> TimelineEvent:
        """Convert protobuf TimelineEventInfo to dataclass."""
        details = {}
        if proto.details_json:
            try:
                details = json.loads(proto.details_json)
            except json.JSONDecodeError:
                pass

        return TimelineEvent(
            timestamp=datetime.fromtimestamp(proto.timestamp, tz=UTC) if proto.timestamp else None,
            event_type=proto.event_type,
            description=proto.description,
            tx_hash=proto.tx_hash if proto.tx_hash else None,
            chain=proto.chain if proto.chain else None,
            details=details,
            cycle_id=proto.cycle_id or "",
            phase=proto.phase or "",
            related_ledger_entry_id=proto.related_ledger_entry_id or "",
        )


# =============================================================================
# Singleton accessor
# =============================================================================

_dashboard_client: GatewayDashboardClient | None = None


def get_dashboard_client() -> GatewayDashboardClient:
    """Get the default dashboard client (singleton).

    Returns a shared GatewayDashboardClient instance. The client is not
    connected by default; call connect() before use.

    Returns:
        Shared GatewayDashboardClient instance.
    """
    global _dashboard_client
    if _dashboard_client is None:
        _dashboard_client = GatewayDashboardClient()
    return _dashboard_client


def reset_dashboard_client() -> None:
    """Reset the default dashboard client.

    Disconnects and clears the singleton client. Useful for testing.
    """
    global _dashboard_client
    if _dashboard_client is not None:
        _dashboard_client.disconnect()
        _dashboard_client = None
