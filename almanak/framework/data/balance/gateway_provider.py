"""Gateway-backed BalanceProvider implementation.

This module provides a BalanceProvider that fetches balances through the gateway
sidecar instead of making direct RPC calls. Used in strategy containers that
have no access to RPC endpoints or private keys.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.data.interfaces import BalanceProvider, BalanceResult
from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


class GatewayBalanceProvider(BalanceProvider):
    """BalanceProvider that fetches balances through the gateway.

    This implementation routes all balance requests to the gateway sidecar,
    which has access to the RPC endpoints and can query on-chain balances.

    Example:
        from almanak.framework.gateway_client import GatewayClient
        from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider

        with GatewayClient() as client:
            provider = GatewayBalanceProvider(
                client=client,
                wallet_address="0x1234...",
                chain="arbitrum",
            )
            result = await provider.get_balance("WETH")
            print(f"WETH balance: {result.balance}")
    """

    def __init__(
        self,
        client: GatewayClient,
        wallet_address: str,
        chain: str = "arbitrum",
        timeout: float = 30.0,
    ):
        """Initialize gateway-backed balance provider.

        Args:
            client: Connected GatewayClient instance
            wallet_address: Wallet address to query balances for
            chain: Chain name (e.g., "arbitrum", "base")
            timeout: RPC timeout in seconds
        """
        self._client = client
        self._wallet_address = wallet_address
        self._chain = chain
        self._timeout = timeout
        self._cache: dict[str, BalanceResult] = {}

    @property
    def wallet_address(self) -> str:
        """Get the wallet address."""
        return self._wallet_address

    @property
    def chain(self) -> str:
        """Get the chain name."""
        return self._chain

    async def get_balance(self, token: str) -> BalanceResult:
        """Get token balance from gateway.

        Args:
            token: Token symbol (e.g., "WETH", "USDC")

        Returns:
            BalanceResult with balance in human-readable units

        Raises:
            DataSourceUnavailable: If gateway is unreachable
        """
        from almanak.framework.data.interfaces import DataSourceUnavailable

        try:
            request = gateway_pb2.BalanceRequest(
                token=token,
                chain=self._chain,
                wallet_address=self._wallet_address,
            )
            response = self._client.market.GetBalance(request, timeout=self._timeout)

            result = BalanceResult(
                balance=Decimal(response.balance) if response.balance else Decimal(0),
                token=token,
                address=response.address or self._wallet_address,
                decimals=response.decimals,
                raw_balance=int(response.raw_balance) if response.raw_balance else 0,
                timestamp=datetime.fromtimestamp(response.timestamp, tz=UTC)
                if response.timestamp
                else datetime.now(UTC),
                stale=response.stale,
            )

            # Cache the result
            self._cache[token] = result

            return result

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Gateway balance request failed for {token}: {error_msg}")

            if "UNAVAILABLE" in error_msg or "DEADLINE_EXCEEDED" in error_msg:
                raise DataSourceUnavailable(
                    source="gateway",
                    reason=error_msg,
                ) from e

            raise

    async def get_native_balance(self) -> BalanceResult:
        """Get native token balance (ETH, AVAX, etc.).

        Returns:
            BalanceResult with native token balance
        """
        # Map chain to native token
        native_tokens = {
            "ethereum": "ETH",
            "arbitrum": "ETH",
            "optimism": "ETH",
            "base": "ETH",
            "avalanche": "AVAX",
            "polygon": "MATIC",
        }

        native_token = native_tokens.get(self._chain.lower(), "ETH")
        return await self.get_balance(native_token)

    def invalidate_cache(self, token: str | None = None) -> None:
        """Invalidate cached balance data.

        Call this after executing transactions that change balances.

        Args:
            token: Specific token to invalidate, or None for all tokens
        """
        if token is None:
            self._cache.clear()
            logger.debug("Invalidated all cached balances")
        elif token in self._cache:
            del self._cache[token]
            logger.debug(f"Invalidated cached balance for {token}")
