"""Gateway-backed PriceOracle implementation.

This module provides a PriceOracle that fetches prices through the gateway
sidecar instead of making direct external API calls. Used in strategy
containers that have no access to API keys.
"""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.data.interfaces import PriceOracle, PriceResult
from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


class GatewayPriceOracle(PriceOracle):
    """PriceOracle that fetches prices through the gateway.

    This implementation routes all price requests to the gateway sidecar,
    which has access to the actual price sources (CoinGecko, Chainlink, etc.).

    Example:
        from almanak.framework.gateway_client import GatewayClient
        from almanak.framework.data.price.gateway_oracle import GatewayPriceOracle

        with GatewayClient() as client:
            oracle = GatewayPriceOracle(client)
            result = await oracle.get_aggregated_price("ETH", "USD")
            print(f"ETH price: ${result.price}")
    """

    def __init__(self, client: GatewayClient, timeout: float = 30.0):
        """Initialize gateway-backed price oracle.

        Args:
            client: Connected GatewayClient instance
            timeout: RPC timeout in seconds
        """
        self._client = client
        self._timeout = timeout
        self._source_health: dict[str, dict] = {}

    async def get_aggregated_price(self, token: str, quote: str = "USD") -> PriceResult:
        """Get aggregated price from gateway.

        Args:
            token: Token symbol (e.g., "ETH", "WBTC")
            quote: Quote currency (default: "USD")

        Returns:
            PriceResult with price, source, timestamp, confidence

        Raises:
            DataSourceUnavailable: If gateway is unreachable
            AllDataSourcesFailed: If all price sources failed in gateway
        """
        from almanak.framework.data.interfaces import (
            AllDataSourcesFailed,
            DataSourceUnavailable,
        )

        try:
            request = gateway_pb2.PriceRequest(token=token, quote=quote)
            # Offload synchronous gRPC call to a thread to avoid blocking the event loop
            response = await asyncio.to_thread(self._client.market.GetPrice, request, timeout=self._timeout)

            if not response.price:
                raise AllDataSourcesFailed(errors={"gateway": "Empty price response from gateway"})

            source_details = None
            if response.sources_ok or response.sources_failed or response.outliers:
                source_details = {
                    "sources_ok": list(response.sources_ok),
                    "sources_failed": dict(response.sources_failed),
                    "outliers": list(response.outliers),
                }

            return PriceResult(
                price=Decimal(response.price),
                source=response.source or "gateway",
                timestamp=datetime.fromtimestamp(response.timestamp, tz=UTC),
                confidence=response.confidence,
                stale=response.stale,
                source_details=source_details,
            )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Gateway price request failed for {token}/{quote}: {error_msg}")

            if "UNAVAILABLE" in error_msg or "DEADLINE_EXCEEDED" in error_msg:
                raise DataSourceUnavailable(
                    source="gateway",
                    reason=error_msg,
                ) from e

            raise AllDataSourcesFailed(errors={"gateway": error_msg}) from e

    def get_source_health(self, source_name: str) -> dict | None:
        """Get health metrics for a specific source.

        Note: Health metrics are tracked in the gateway, not here.
        This method returns cached health data if available.

        Args:
            source_name: Name of the price source

        Returns:
            Health metrics dict or None if not available
        """
        return self._source_health.get(source_name)

    def get_all_source_health(self) -> dict[str, dict]:
        """Get health metrics for all sources.

        Returns:
            Dictionary of source name to health metrics
        """
        return self._source_health.copy()
