"""Data source module for the Almanak Operator Dashboard.

All data access goes through the gateway. No direct filesystem,
database, or timeline store access.

The gateway is REQUIRED for the dashboard to function. If the gateway
is not available, the dashboard will show an error and stop.
"""

import logging
from decimal import Decimal

from almanak.framework.dashboard.gateway_client import (
    GatewayConnectionError,
    StrategyDetails,
    StrategySummary,
    TimelineEvent,
    get_dashboard_client,
    reset_dashboard_client,
)
from almanak.framework.dashboard.models import (
    LPPosition,
    PositionSummary,
    Strategy,
    StrategyStatus,
    TimelineEventType,
    TokenBalance,
)
from almanak.framework.dashboard.models import (
    TimelineEvent as DashboardTimelineEvent,
)

logger = logging.getLogger(__name__)

# Re-export for backward compatibility
__all__ = [
    "GatewayConnectionError",
    "archive_strategy_instance",
    "execute_strategy_action",
    "get_all_strategies",
    "get_available_strategies",
    "get_dashboard_client",
    "purge_strategy_instance",
    "get_strategy_details",
    "get_timeline",
    "is_gateway_available",
    "reset_gateway_connection",
]


def _convert_status(status_str: str) -> StrategyStatus:
    """Convert status string to StrategyStatus enum."""
    status_map = {
        "RUNNING": StrategyStatus.RUNNING,
        "PAUSED": StrategyStatus.PAUSED,
        "ERROR": StrategyStatus.ERROR,
        "STUCK": StrategyStatus.STUCK,
        "INACTIVE": StrategyStatus.INACTIVE,
        "STALE": StrategyStatus.STALE,
        "ARCHIVED": StrategyStatus.ARCHIVED,
    }
    return status_map.get(status_str, StrategyStatus.PAUSED)


def _convert_event_type(event_type_str: str) -> TimelineEventType:
    """Convert event type string to TimelineEventType enum."""
    normalized = event_type_str.upper()
    try:
        return TimelineEventType(normalized)
    except ValueError:
        pass

    event_type_map = {
        "TRADE": TimelineEventType.TRADE,
        "SWAP": TimelineEventType.TRADE,
        "REBALANCE": TimelineEventType.REBALANCE,
        "DEPOSIT": TimelineEventType.DEPOSIT,
        "WITHDRAWAL": TimelineEventType.WITHDRAWAL,
        "LP_OPEN": TimelineEventType.LP_OPEN,
        "LP_CLOSE": TimelineEventType.LP_CLOSE,
        "ERROR": TimelineEventType.ERROR,
        "STATE_CHANGE": TimelineEventType.STATE_CHANGE,
        "EXECUTION": TimelineEventType.TRADE,
        "TRANSACTION_CONFIRMED": TimelineEventType.TRADE,
        "TRANSACTION_FAILED": TimelineEventType.ERROR,
        "TRANSACTION_REVERTED": TimelineEventType.ERROR,
        "STRATEGY_STARTED": TimelineEventType.STATE_CHANGE,
        "STRATEGY_PAUSED": TimelineEventType.STATE_CHANGE,
        "STRATEGY_RESUMED": TimelineEventType.STATE_CHANGE,
        "STRATEGY_STOPPED": TimelineEventType.STATE_CHANGE,
    }
    return event_type_map.get(normalized, TimelineEventType.TRADE)


def _convert_gateway_summary_to_model(summary: StrategySummary) -> Strategy:
    """Convert gateway StrategySummary to dashboard Strategy model.

    Args:
        summary: GatewayDashboardClient.StrategySummary dataclass

    Returns:
        Strategy model for dashboard display
    """
    return Strategy(
        id=summary.strategy_id,
        name=summary.name,
        status=_convert_status(summary.status),
        pnl_24h_usd=summary.pnl_24h_usd,
        total_value_usd=summary.total_value_usd,
        chain=summary.chain,
        protocol=summary.protocol,
        last_action_at=summary.last_action_at,
        attention_required=summary.attention_required,
        attention_reason=summary.attention_reason,
        position=PositionSummary(
            token_balances=[],
            lp_positions=[],
            total_lp_value_usd=Decimal("0"),
        ),
        timeline_events=[],
        pnl_history=[],
        is_multi_chain=summary.is_multi_chain,
        chains=summary.chains,
    )


def _convert_gateway_details_to_model(details: StrategyDetails) -> Strategy:
    """Convert gateway StrategyDetails to dashboard Strategy model.

    Args:
        details: GatewayDashboardClient.StrategyDetails dataclass

    Returns:
        Strategy model with full details for dashboard display
    """
    strategy = _convert_gateway_summary_to_model(details.summary)

    # Add position details
    if details.position:
        strategy.position = PositionSummary(
            token_balances=[
                TokenBalance(
                    symbol=b.symbol,
                    balance=b.balance,
                    value_usd=b.value_usd,
                )
                for b in details.position.token_balances
            ],
            lp_positions=[
                LPPosition(
                    pool=p.pool,
                    token0=p.token0,
                    token1=p.token1,
                    liquidity_usd=p.liquidity_usd,
                    range_lower=p.range_lower,
                    range_upper=p.range_upper,
                    current_price=p.current_price,
                    in_range=p.in_range,
                )
                for p in details.position.lp_positions
            ],
            total_lp_value_usd=details.position.total_lp_value_usd,
            health_factor=details.position.health_factor,
            leverage=details.position.leverage,
        )

    # Add timeline events
    strategy.timeline_events = [_convert_gateway_timeline_event(e) for e in details.timeline]

    return strategy


def _convert_gateway_timeline_event(event: TimelineEvent) -> DashboardTimelineEvent:
    """Convert gateway TimelineEvent to dashboard TimelineEvent model."""
    timestamp = event.timestamp
    if timestamp is None:
        from datetime import UTC, datetime

        timestamp = datetime.now(tz=UTC)
    return DashboardTimelineEvent(
        timestamp=timestamp,
        event_type=_convert_event_type(event.event_type),
        description=event.description,
        tx_hash=event.tx_hash,
        chain=event.chain,
        details=event.details,
    )


def get_all_strategies() -> list[Strategy]:
    """Get executed/running strategies from the instance registry.

    These are strategies that have been run and have real state data.
    Used by the Command Center (main page).

    Returns:
        List of Strategy objects

    Raises:
        GatewayConnectionError: If gateway is not available
    """
    client = get_dashboard_client()

    if not client.is_connected:
        client.connect()

    summaries = client.list_strategies(include_position=True)
    return [_convert_gateway_summary_to_model(s) for s in summaries]


def get_available_strategies() -> list[Strategy]:
    """Get available strategy templates from the filesystem.

    These are strategies with config.json files that haven't been
    executed yet. Used by the Strategy Library page.

    Returns:
        List of Strategy objects

    Raises:
        GatewayConnectionError: If gateway is not available
    """
    client = get_dashboard_client()

    if not client.is_connected:
        client.connect()

    summaries = client.list_available_strategies()
    return [_convert_gateway_summary_to_model(s) for s in summaries]


def get_strategy_details(strategy_id: str) -> Strategy | None:
    """Get detailed strategy information from the gateway.

    Args:
        strategy_id: Strategy identifier

    Returns:
        Strategy with full details or None if not found

    Raises:
        GatewayConnectionError: If gateway is not available
    """
    client = get_dashboard_client()

    if not client.is_connected:
        client.connect()

    try:
        details = client.get_strategy_details(
            strategy_id,
            include_timeline=True,
            include_pnl_history=True,
            timeline_limit=50,
        )
        return _convert_gateway_details_to_model(details)
    except GatewayConnectionError:
        raise
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to get strategy details for {strategy_id}: {e}")
        return None


def get_timeline(strategy_id: str, limit: int = 50) -> list[DashboardTimelineEvent]:
    """Get timeline events from the gateway.

    Args:
        strategy_id: Strategy identifier
        limit: Maximum number of events

    Returns:
        List of TimelineEvent objects

    Raises:
        GatewayConnectionError: If gateway is not available
    """
    client = get_dashboard_client()

    if not client.is_connected:
        client.connect()

    events = client.get_timeline(strategy_id, limit=limit)
    return [_convert_gateway_timeline_event(e) for e in events]


def execute_strategy_action(strategy_id: str, action: str, reason: str) -> bool:
    """Execute an operator action via gateway DashboardService."""
    client = get_dashboard_client()
    if not client.is_connected:
        client.connect()
    return client.execute_action(strategy_id, action=action, reason=reason)


def archive_strategy_instance(strategy_id: str, reason: str = "Archived from dashboard") -> bool:
    """Archive a strategy instance via gateway DashboardService."""
    client = get_dashboard_client()
    if not client.is_connected:
        client.connect()
    return client.archive_strategy_instance(strategy_id, reason=reason)


def purge_strategy_instance(strategy_id: str, reason: str) -> bool:
    """Purge a strategy instance via gateway DashboardService."""
    client = get_dashboard_client()
    if not client.is_connected:
        client.connect()
    return client.purge_strategy_instance(strategy_id, reason=reason)


def is_gateway_available() -> bool:
    """Check if gateway is available for dashboard data.

    Returns:
        True if gateway is connected and healthy
    """
    try:
        client = get_dashboard_client()
        # Always call connect() to verify health, even if already connected
        client.connect()
        return client.is_connected
    except Exception:  # noqa: BLE001
        return False


def reset_gateway_connection() -> None:
    """Reset the gateway connection state.

    Call this to force a reconnection attempt on the next data access.
    """
    reset_dashboard_client()
