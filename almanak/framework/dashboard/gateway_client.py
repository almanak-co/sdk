"""Gateway-backed client for dashboard data access.

This client replaces direct filesystem/database access in the dashboard.
All data is fetched from the gateway via gRPC.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import grpc

from almanak.framework.gateway_client import GatewayClient, get_gateway_client
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


@dataclass
class StrategySummary:
    """Summary of a strategy for dashboard display."""

    strategy_id: str
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


@dataclass
class StrategyDetails:
    """Detailed strategy information."""

    summary: StrategySummary
    position: PositionInfo
    timeline: list[TimelineEvent] = field(default_factory=list)
    pnl_history: list[dict] = field(default_factory=list)


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
        strategy_id: str,
        include_timeline: bool = True,
        include_pnl_history: bool = False,
        timeline_limit: int = 20,
    ) -> StrategyDetails:
        """Get detailed information about a strategy.

        Args:
            strategy_id: Strategy identifier
            include_timeline: Include recent timeline events
            include_pnl_history: Include PnL history for charts
            timeline_limit: Maximum number of timeline events

        Returns:
            StrategyDetails object
        """
        client = self._ensure_connected()

        request = gateway_pb2.GetStrategyDetailsRequest(
            strategy_id=strategy_id,
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
        strategy_id: str,
        limit: int = 50,
        event_type_filter: str | None = None,
    ) -> list[TimelineEvent]:
        """Get timeline events for a strategy.

        Args:
            strategy_id: Strategy identifier
            limit: Maximum number of events to return
            event_type_filter: Filter by event type

        Returns:
            List of TimelineEvent objects
        """
        client = self._ensure_connected()

        request = gateway_pb2.GetTimelineRequest(
            strategy_id=strategy_id,
            limit=limit,
            event_type_filter=event_type_filter or "",
        )

        try:
            response = client.dashboard.GetTimeline(request)
        except grpc.RpcError as e:
            logger.exception("Failed to get timeline")
            raise GatewayConnectionError(f"Failed to get timeline: {e}") from e

        return [self._convert_timeline_event(e) for e in response.events]

    def get_strategy_config(self, strategy_id: str) -> dict[str, Any]:
        """Get strategy configuration.

        Args:
            strategy_id: Strategy identifier

        Returns:
            Configuration dictionary
        """
        client = self._ensure_connected()

        request = gateway_pb2.GetStrategyConfigRequest(strategy_id=strategy_id)

        try:
            response = client.dashboard.GetStrategyConfig(request)
        except grpc.RpcError as e:
            logger.exception("Failed to get strategy config")
            raise GatewayConnectionError(f"Failed to get strategy config: {e}") from e

        if response.config_json:
            try:
                return json.loads(response.config_json)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to decode strategy config JSON for {strategy_id}: {e}")
                return {}
        return {}

    def get_strategy_state(
        self,
        strategy_id: str,
        fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """Get current strategy state.

        Args:
            strategy_id: Strategy identifier
            fields: Optional list of specific fields to return

        Returns:
            State dictionary
        """
        client = self._ensure_connected()

        request = gateway_pb2.GetStrategyStateRequest(
            strategy_id=strategy_id,
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
                logger.warning(f"Failed to decode strategy state JSON for {strategy_id}: {e}")
                return {}
        return {}

    def execute_action(
        self,
        strategy_id: str,
        action: str,
        reason: str,
        params: dict[str, str] | None = None,
    ) -> bool:
        """Execute operator action (pause, resume, etc.).

        Args:
            strategy_id: Strategy identifier
            action: Action to execute ("PAUSE", "RESUME", etc.)
            reason: Reason for the action (required for audit)
            params: Optional action-specific parameters

        Returns:
            True if successful, False otherwise
        """
        client = self._ensure_connected()

        request = gateway_pb2.ExecuteActionRequest(
            strategy_id=strategy_id,
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

    def archive_strategy_instance(self, strategy_id: str, reason: str = "") -> bool:
        """Archive a strategy instance (hidden from dashboard, data retained).

        Args:
            strategy_id: Strategy instance ID to archive.
            reason: Reason for archiving (for audit).

        Returns:
            True if successful.
        """
        client = self._ensure_connected()

        request = gateway_pb2.ArchiveInstanceRequest(
            strategy_id=strategy_id,
            reason=reason,
        )

        try:
            response = client.dashboard.ArchiveStrategyInstance(request)
            if not response.success:
                logger.warning(f"Archive failed for {strategy_id}: {response.error}")
            return response.success
        except Exception:
            logger.exception("Failed to archive strategy instance")
            return False

    def purge_strategy_instance(self, strategy_id: str, reason: str) -> bool:
        """Purge a strategy instance and all its events (permanent delete).

        Args:
            strategy_id: Strategy instance ID to purge.
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
            strategy_id=strategy_id,
            reason=reason,
        )

        try:
            response = client.dashboard.PurgeStrategyInstance(request)
            if not response.success:
                logger.warning(f"Purge failed for {strategy_id}: {response.error}")
            return response.success
        except Exception:
            logger.exception("Failed to purge strategy instance")
            return False

    # =========================================================================
    # Conversion helpers
    # =========================================================================

    def _convert_summary(self, proto: gateway_pb2.StrategySummary) -> StrategySummary:
        """Convert protobuf StrategySummary to dataclass."""
        return StrategySummary(
            strategy_id=proto.strategy_id,
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
