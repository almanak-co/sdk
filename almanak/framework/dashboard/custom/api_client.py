"""API client for custom dashboards.

This client is passed to user-written dashboard code (ui.py files).
It provides a controlled interface to strategy data - all access goes
through the gateway.

SECURITY: This client is the ONLY way custom dashboards can access data.
Custom dashboards cannot import the gateway client directly because:
1. In production, the dashboard container has no direct gateway access
2. The api_client is injected by the framework with proper auth
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class DashboardAPIClient:
    """API client for custom dashboards.

    This is the interface provided to user-written dashboard code.
    All methods are read-only except for operator actions.

    Example usage in custom dashboard (ui.py):
        def render_custom_dashboard(
            strategy_id: str,
            strategy_config: dict,
            api_client: DashboardAPIClient,  # This client
            session_state: dict,
        ) -> None:
            # Get timeline events
            events = api_client.get_timeline(limit=10)

            # Get current state
            state = api_client.get_state()

            # Get price data
            eth_price = api_client.get_price("ETH", "USD")
    """

    def __init__(self, gateway_client: Any, strategy_id: str):
        """Initialize the API client.

        Args:
            gateway_client: The underlying GatewayDashboardClient
            strategy_id: The strategy this dashboard is for (for scoping)
        """
        self._client = gateway_client
        self._strategy_id = strategy_id

    @property
    def strategy_id(self) -> str:
        """Get the strategy ID this client is scoped to."""
        return self._strategy_id

    # =========================================================================
    # Strategy Data (scoped to current strategy)
    # =========================================================================

    def get_state(self, fields: list[str] | None = None) -> dict[str, Any]:
        """Get current strategy state.

        Args:
            fields: Optional list of specific fields to return.
                   If None, returns full state.

        Returns:
            Strategy state as dictionary.
        """
        try:
            return self._client.get_strategy_state(self._strategy_id, fields)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get strategy state: {e}")
            return {}

    def get_timeline(
        self,
        limit: int = 50,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get timeline events for this strategy.

        Args:
            limit: Maximum number of events to return
            event_type: Optional filter by event type

        Returns:
            List of timeline events as dictionaries.
        """
        try:
            events = self._client.get_timeline(
                self._strategy_id,
                limit=limit,
                event_type_filter=event_type,
            )
            return [self._event_to_dict(e) for e in events]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get timeline: {e}")
            return []

    def get_config(self) -> dict[str, Any]:
        """Get strategy configuration.

        Returns:
            Strategy configuration as dictionary.
        """
        try:
            return self._client.get_strategy_config(self._strategy_id)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get strategy config: {e}")
            return {}

    def get_position(self) -> dict[str, Any]:
        """Get current position summary.

        Returns:
            Position data including balances, LP positions, etc.
        """
        try:
            details = self._client.get_strategy_details(self._strategy_id)
            return self._position_to_dict(details.position)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get position: {e}")
            return {}

    def get_summary(self) -> dict[str, Any]:
        """Get strategy summary.

        Returns:
            Summary data including status, value, PnL, etc.
        """
        try:
            details = self._client.get_strategy_details(self._strategy_id)
            return self._summary_to_dict(details.summary)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get summary: {e}")
            return {}

    # =========================================================================
    # Market Data (via gateway)
    # =========================================================================

    def get_price(self, token: str, quote: str = "USD") -> float | None:
        """Get current token price.

        Args:
            token: Token symbol (e.g., "ETH", "BTC")
            quote: Quote currency (default "USD")

        Returns:
            Price as float, or None if unavailable.
        """
        try:
            # Access the underlying gateway client's market service
            from almanak.gateway.proto import gateway_pb2

            response = self._client._client.market.GetPrice(gateway_pb2.PriceRequest(token=token, quote=quote))
            return float(response.price) if response.price else None
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to get price for {token}/{quote}: {e}")
            return None

    def get_balance(self, token: str, chain: str | None = None) -> float | None:
        """Get token balance for strategy wallet.

        Args:
            token: Token symbol
            chain: Chain name (uses strategy's chain if not specified)

        Returns:
            Balance as float, or None if unavailable.
        """
        try:
            # Get wallet address from strategy config
            config = self.get_config()
            wallet = config.get("wallet_address", "")
            chain = chain or config.get("chain", "arbitrum")

            from almanak.gateway.proto import gateway_pb2

            response = self._client._client.market.GetBalance(
                gateway_pb2.BalanceRequest(
                    token=token,
                    chain=chain,
                    wallet_address=wallet,
                )
            )
            return float(response.balance) if response.balance else None
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to get balance for {token}: {e}")
            return None

    def get_indicator(
        self,
        indicator_type: str,
        token: str,
        quote: str = "USD",
        params: dict[str, str] | None = None,
    ) -> float | None:
        """Get technical indicator value.

        Args:
            indicator_type: Indicator type (e.g., "RSI", "SMA")
            token: Token symbol
            quote: Quote currency
            params: Indicator parameters (e.g., {"period": "14"})

        Returns:
            Indicator value as float, or None if unavailable.
        """
        try:
            from almanak.gateway.proto import gateway_pb2

            response = self._client._client.market.GetIndicator(
                gateway_pb2.IndicatorRequest(
                    indicator_type=indicator_type,
                    token=token,
                    quote=quote,
                    params=params or {},
                )
            )
            return float(response.value) if response.value else None
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to get indicator {indicator_type}: {e}")
            return None

    # =========================================================================
    # Operator Actions (with audit)
    # =========================================================================

    def pause_strategy(self, reason: str) -> bool:
        """Pause the strategy.

        Args:
            reason: Reason for pausing (required for audit)

        Returns:
            True if successful, False otherwise.
        """
        if not reason:
            logger.warning("Cannot pause strategy: reason is required")
            return False

        try:
            return self._client.execute_action(
                self._strategy_id,
                action="PAUSE",
                reason=reason,
            )
        except Exception:
            logger.exception("Failed to pause strategy")
            return False

    def resume_strategy(self, reason: str = "Resumed from dashboard") -> bool:
        """Resume the strategy.

        Args:
            reason: Reason for resuming (optional, defaults to generic message)

        Returns:
            True if successful, False otherwise.
        """
        try:
            return self._client.execute_action(
                self._strategy_id,
                action="RESUME",
                reason=reason,
            )
        except Exception:
            logger.exception("Failed to resume strategy")
            return False

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _event_to_dict(self, event: Any) -> dict[str, Any]:
        """Convert timeline event to dictionary."""
        return {
            "timestamp": event.timestamp.isoformat() if hasattr(event, "timestamp") and event.timestamp else None,
            "event_type": event.event_type if hasattr(event, "event_type") else str(type(event).__name__),
            "description": event.description if hasattr(event, "description") else "",
            "tx_hash": event.tx_hash if hasattr(event, "tx_hash") else None,
            "chain": event.chain if hasattr(event, "chain") else None,
            "details": event.details if hasattr(event, "details") and isinstance(event.details, dict) else {},
        }

    def _position_to_dict(self, position: Any) -> dict[str, Any]:
        """Convert position to dictionary."""
        if position is None:
            return {}

        result: dict[str, Any] = {
            "token_balances": [],
            "lp_positions": [],
            "total_lp_value_usd": "0",
            "health_factor": None,
            "leverage": None,
        }

        if hasattr(position, "token_balances") and position.token_balances:
            result["token_balances"] = [
                {
                    "symbol": b.symbol,
                    "balance": str(b.balance),
                    "value_usd": str(b.value_usd),
                }
                for b in position.token_balances
            ]

        if hasattr(position, "lp_positions") and position.lp_positions:
            result["lp_positions"] = [
                {
                    "pool": p.pool,
                    "token0": p.token0,
                    "token1": p.token1,
                    "liquidity_usd": str(p.liquidity_usd),
                    "in_range": p.in_range,
                }
                for p in position.lp_positions
            ]

        if hasattr(position, "total_lp_value_usd") and position.total_lp_value_usd:
            result["total_lp_value_usd"] = str(position.total_lp_value_usd)

        if hasattr(position, "health_factor") and position.health_factor is not None:
            result["health_factor"] = str(position.health_factor)

        if hasattr(position, "leverage") and position.leverage is not None:
            result["leverage"] = str(position.leverage)

        return result

    def _summary_to_dict(self, summary: Any) -> dict[str, Any]:
        """Convert summary to dictionary."""
        if summary is None:
            return {}

        return {
            "strategy_id": summary.strategy_id if hasattr(summary, "strategy_id") else self._strategy_id,
            "name": summary.name if hasattr(summary, "name") else "",
            "status": summary.status if hasattr(summary, "status") else "UNKNOWN",
            "chain": summary.chain if hasattr(summary, "chain") else "",
            "protocol": summary.protocol if hasattr(summary, "protocol") else "",
            "total_value_usd": str(summary.total_value_usd) if hasattr(summary, "total_value_usd") else "0",
            "pnl_24h_usd": str(summary.pnl_24h_usd) if hasattr(summary, "pnl_24h_usd") else "0",
            "attention_required": summary.attention_required if hasattr(summary, "attention_required") else False,
            "attention_reason": summary.attention_reason if hasattr(summary, "attention_reason") else "",
        }


def create_api_client(gateway_client: Any, strategy_id: str) -> DashboardAPIClient:
    """Create API client for custom dashboard.

    This is the factory function used by the renderer to create
    a gateway-backed API client for custom dashboards.

    Args:
        gateway_client: The GatewayDashboardClient instance
        strategy_id: Strategy this dashboard is for

    Returns:
        DashboardAPIClient for use in custom dashboard
    """
    return DashboardAPIClient(gateway_client, strategy_id)
