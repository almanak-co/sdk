"""Gateway-backed BalanceProvider implementation.

This module provides a BalanceProvider that fetches balances through the gateway
sidecar instead of making direct RPC calls. Used in strategy containers that
have no access to RPC endpoints or private keys.

Includes retry with exponential backoff and cached fallback for resilience
against RPC rate limiting (VIB-1712).
"""

import asyncio
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.data.interfaces import BalanceProvider, BalanceResult
from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)

# Retry configuration
_MAX_RETRIES = 3
_BACKOFF_BASE_SECONDS = 0.5  # 0.5s, 1s, 2s

# Error substrings that indicate transient/retryable RPC failures
_RETRYABLE_ERROR_PATTERNS = (
    "UNAVAILABLE",
    "DEADLINE_EXCEEDED",
    "429",
    "rate limit",
    "Rate limit",
    "Too Many Requests",
    "Connection refused",
    "connection reset",
    "timed out",
)


def _is_retryable(error_msg: str) -> bool:
    """Check whether an error message indicates a transient RPC failure."""
    return any(pattern in error_msg for pattern in _RETRYABLE_ERROR_PATTERNS)


class GatewayBalanceProvider(BalanceProvider):
    """BalanceProvider that fetches balances through the gateway.

    This implementation routes all balance requests to the gateway sidecar,
    which has access to the RPC endpoints and can query on-chain balances.

    Resilience features (VIB-1712):
    - Retries up to 3 times with exponential backoff (0.5s, 1s, 2s) on
      transient RPC errors (429, timeouts, connection errors).
    - Caches successful reads with a configurable TTL (default 30s).
    - On total failure after retries, returns last-known cached balance
      with ``stale=True``. If no cached value exists, raises as before.

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
        cache_ttl: float = 30.0,
    ):
        """Initialize gateway-backed balance provider.

        Args:
            client: Connected GatewayClient instance
            wallet_address: Wallet address to query balances for
            chain: Chain name (e.g., "arbitrum", "base")
            timeout: RPC timeout in seconds
            cache_ttl: How long (seconds) cached balances remain valid before
                being considered stale. Default 30s.
        """
        self._client = client
        self._wallet_address = wallet_address
        self._chain = chain
        self._timeout = timeout
        self._cache_ttl = cache_ttl
        # Cache stores (BalanceResult, timestamp_of_cache_write)
        self._cache: dict[str, tuple[BalanceResult, float]] = {}

    @property
    def wallet_address(self) -> str:
        """Get the wallet address."""
        return self._wallet_address

    @property
    def chain(self) -> str:
        """Get the chain name."""
        return self._chain

    async def get_balance(self, token: str) -> BalanceResult:
        """Get token balance from gateway with retry and cached fallback.

        On transient RPC errors (rate-limit 429, timeouts, connection errors)
        the call is retried up to 3 times with exponential backoff.  On total
        failure the last cached balance is returned with ``stale=True``.

        Args:
            token: Token symbol (e.g., "WETH", "USDC")

        Returns:
            BalanceResult with balance in human-readable units.
            Check ``result.stale`` to know if the value is from cache.

        Raises:
            DataSourceUnavailable: If gateway is unreachable and no cache exists
        """
        from almanak.framework.data.interfaces import DataSourceUnavailable

        last_error: Exception | None = None

        for attempt in range(_MAX_RETRIES):
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

                # Cache the successful result
                self._cache[token] = (result, time.monotonic())

                return result

            except Exception as e:
                last_error = e
                error_msg = str(e)

                if not _is_retryable(error_msg):
                    # Non-retryable error -- break immediately
                    break

                if attempt < _MAX_RETRIES - 1:
                    backoff = _BACKOFF_BASE_SECONDS * (2**attempt)
                    logger.warning(
                        "Balance request for %s failed (attempt %d/%d), retrying in %.1fs: %s",
                        token,
                        attempt + 1,
                        _MAX_RETRIES,
                        backoff,
                        error_msg,
                    )
                    await asyncio.sleep(backoff)

        # All retries exhausted (or non-retryable error) -- try cached fallback
        if last_error is None:
            # Should not happen, but guard against it
            raise DataSourceUnavailable(source="gateway", reason="unknown error after retry loop")
        error_msg = str(last_error)

        cached_result = self._get_cached(token)
        if cached_result is not None:
            from dataclasses import replace

            logger.warning(
                "Returning cached (stale) balance for %s after %d failed attempts: %s",
                token,
                _MAX_RETRIES,
                error_msg,
            )
            return replace(cached_result, stale=True)

        # No cache -- propagate the error
        logger.error("Gateway balance request failed for %s with no cached fallback: %s", token, error_msg)

        if _is_retryable(error_msg):
            raise DataSourceUnavailable(
                source="gateway",
                reason=error_msg,
            ) from last_error

        raise last_error

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

    def _get_cached(self, token: str) -> BalanceResult | None:
        """Return a cached balance if it exists and is within TTL, else None."""
        cached = self._cache.get(token)
        if cached is None:
            return None
        result, cached_at = cached
        if (time.monotonic() - cached_at) > self._cache_ttl:
            return None
        return result

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
