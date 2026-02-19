"""TokenService implementation - unified token resolution and on-chain discovery.

This service provides token resolution and metadata discovery to strategy containers
via gRPC. It uses the TokenResolver for cached/static lookups and OnChainLookup
for discovering unknown tokens by querying their smart contracts directly.

Key Features:
    - ResolveToken: Resolve by symbol or address using cache/static registry
    - GetTokenMetadata: On-chain ERC20 metadata query for unknown tokens
    - GetTokenDecimals: Lightweight endpoint for decimals only
    - BatchResolveTokens: Resolve multiple tokens in a single call
    - Rate limiting: Prevents RPC abuse (max 10 on-chain lookups/second)
    - Timeout handling: Configurable timeout for on-chain queries
"""

import asyncio
import logging
import time
from typing import Any

import grpc

from almanak.framework.data.tokens import (
    InvalidTokenAddressError,
    ResolvedToken,
    TokenNotFoundError,
    TokenResolutionError,
    get_token_resolver,
)
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.services.onchain_lookup import OnChainLookup, TokenMetadata
from almanak.gateway.utils import get_rpc_url
from almanak.gateway.validation import ValidationError, validate_address, validate_batch_size, validate_chain

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Default timeout for on-chain lookups (seconds)
DEFAULT_ONCHAIN_TIMEOUT = 10.0

# Rate limiting: max on-chain lookups per second
DEFAULT_RATE_LIMIT = 10  # lookups per second


# =============================================================================
# Rate Limiter
# =============================================================================


class TokenRateLimiter:
    """Simple token bucket rate limiter for on-chain lookups.

    Prevents RPC abuse by limiting the number of on-chain lookups per second.
    Uses a sliding window approach for smooth rate limiting.
    """

    def __init__(self, max_rate: int = DEFAULT_RATE_LIMIT):
        """Initialize rate limiter.

        Args:
            max_rate: Maximum lookups per second
        """
        self._max_rate = max_rate
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Acquire permission for an on-chain lookup.

        Returns:
            True if permitted, False if rate limited
        """
        async with self._lock:
            now = time.monotonic()

            # Remove timestamps older than 1 second
            self._timestamps = [t for t in self._timestamps if now - t < 1.0]

            # Check if we're at the limit
            if len(self._timestamps) >= self._max_rate:
                return False

            # Record this lookup
            self._timestamps.append(now)
            return True

    async def wait_and_acquire(self, timeout: float = 1.0) -> bool:
        """Wait until we can acquire permission, with timeout.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if acquired, False if timed out
        """
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            if await self.acquire():
                return True
            await asyncio.sleep(0.1)
        return False


# =============================================================================
# TokenService Implementation
# =============================================================================


class TokenServiceServicer(gateway_pb2_grpc.TokenServiceServicer):
    """Implements TokenService gRPC interface.

    Provides token resolution and metadata discovery for strategy containers:
    - ResolveToken: Cached/static resolution by symbol or address
    - GetTokenMetadata: On-chain ERC20 contract queries
    - GetTokenDecimals: Lightweight decimals-only lookup
    - BatchResolveTokens: Batch resolution for multiple tokens
    """

    def __init__(
        self,
        settings: GatewaySettings,
        onchain_timeout: float = DEFAULT_ONCHAIN_TIMEOUT,
        rate_limit: int = DEFAULT_RATE_LIMIT,
    ):
        """Initialize TokenService.

        Args:
            settings: Gateway settings with network configuration
            onchain_timeout: Timeout for on-chain lookups in seconds
            rate_limit: Maximum on-chain lookups per second
        """
        self.settings = settings
        self._onchain_timeout = onchain_timeout
        self._rate_limiter = TokenRateLimiter(max_rate=rate_limit)

        # Lazy-initialized OnChainLookup instances per chain
        self._onchain_lookups: dict[str, OnChainLookup] = {}
        self._lookups_lock = asyncio.Lock()

        # Get the shared TokenResolver instance (no gateway client for circular ref)
        self._resolver = get_token_resolver()

        logger.info(
            "TokenService initialized",
            extra={
                "onchain_timeout": onchain_timeout,
                "rate_limit": rate_limit,
            },
        )

    async def _get_onchain_lookup(self, chain: str) -> OnChainLookup:
        """Get or create OnChainLookup for a chain.

        Args:
            chain: Chain name (e.g., "arbitrum", "base")

        Returns:
            OnChainLookup instance for the chain
        """
        async with self._lookups_lock:
            if chain not in self._onchain_lookups:
                network = self.settings.network
                rpc_url = get_rpc_url(chain, network=network)
                self._onchain_lookups[chain] = OnChainLookup(
                    rpc_url=rpc_url,
                    timeout=self._onchain_timeout,
                )
                logger.debug(f"Created OnChainLookup for {chain} (network={network})")

            return self._onchain_lookups[chain]

    def _resolved_to_response(
        self,
        resolved: ResolvedToken,
        success: bool = True,
        error: str = "",
    ) -> gateway_pb2.TokenMetadataResponse:
        """Convert ResolvedToken to gRPC response.

        Args:
            resolved: Resolved token data
            success: Whether resolution succeeded
            error: Error message if failed

        Returns:
            TokenMetadataResponse protobuf message
        """
        return gateway_pb2.TokenMetadataResponse(
            success=success,
            error=error,
            symbol=resolved.symbol,
            address=resolved.address,
            decimals=resolved.decimals,
            name=resolved.name or "",
            is_verified=resolved.is_verified,
            source=resolved.source,
        )

    def _metadata_to_response(
        self,
        metadata: TokenMetadata,
        success: bool = True,
        error: str = "",
    ) -> gateway_pb2.TokenMetadataResponse:
        """Convert TokenMetadata to gRPC response.

        Args:
            metadata: On-chain token metadata
            success: Whether lookup succeeded
            error: Error message if failed

        Returns:
            TokenMetadataResponse protobuf message
        """
        return gateway_pb2.TokenMetadataResponse(
            success=success,
            error=error,
            symbol=metadata.symbol,
            address=metadata.address,
            decimals=metadata.decimals,
            name=metadata.name or "",
            is_verified=False,  # On-chain lookups are not verified
            source="on_chain",
        )

    def _error_response(self, error: str) -> gateway_pb2.TokenMetadataResponse:
        """Create error response.

        Args:
            error: Error message

        Returns:
            TokenMetadataResponse with error
        """
        return gateway_pb2.TokenMetadataResponse(
            success=False,
            error=error,
            symbol="",
            address="",
            decimals=0,
            name="",
            is_verified=False,
            source="",
        )

    async def ResolveToken(
        self,
        request: gateway_pb2.ResolveTokenRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.TokenMetadataResponse:
        """Resolve a token by symbol or address.

        Checks: memory cache -> disk cache -> static registry.
        For addresses not in registry, use GetTokenMetadata for on-chain lookup.

        Args:
            request: ResolveTokenRequest with token and chain
            context: gRPC context

        Returns:
            TokenMetadataResponse with token metadata
        """
        token = request.token
        chain = request.chain

        # Validate chain
        try:
            chain = validate_chain(chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return self._error_response(str(e))

        if not token:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Token is required")
            return self._error_response("Token is required")

        try:
            resolved = self._resolver.resolve(token, chain)
            return self._resolved_to_response(resolved)

        except InvalidTokenAddressError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return self._error_response(str(e))

        except TokenNotFoundError as e:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(e))
            return self._error_response(str(e))

        except TokenResolutionError as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return self._error_response(str(e))

        except Exception as e:
            logger.error(f"ResolveToken failed for {token} on {chain}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return self._error_response(str(e))

    async def GetTokenMetadata(
        self,
        request: gateway_pb2.GetTokenMetadataRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.TokenMetadataResponse:
        """Get on-chain ERC20 metadata for a token address.

        Queries the token contract directly for decimals, symbol, name.
        Results are cached in the gateway-side TokenResolver.

        Args:
            request: GetTokenMetadataRequest with address and chain
            context: gRPC context

        Returns:
            TokenMetadataResponse with on-chain metadata
        """
        address = request.address
        chain = request.chain

        # Validate chain
        try:
            chain = validate_chain(chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return self._error_response(str(e))

        # Validate address
        try:
            address = validate_address(address, "address")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return self._error_response(str(e))

        # Check rate limit
        if not await self._rate_limiter.wait_and_acquire(timeout=2.0):
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            error_msg = "Rate limit exceeded for on-chain lookups"
            context.set_details(error_msg)
            logger.warning(f"Rate limited on-chain lookup for {address} on {chain}")
            return self._error_response(error_msg)

        try:
            # First try static resolution (fast path)
            try:
                resolved = self._resolver.resolve(address, chain)
                return self._resolved_to_response(resolved)
            except TokenNotFoundError:
                pass  # Fall through to on-chain lookup

            # On-chain lookup
            lookup = await self._get_onchain_lookup(chain)
            metadata = await asyncio.wait_for(
                lookup.lookup(chain, address),
                timeout=self._onchain_timeout,
            )

            if metadata is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                error_msg = f"Could not fetch metadata for {address} on {chain}"
                context.set_details(error_msg)
                return self._error_response(error_msg)

            # Cache the discovered token in resolver
            self._cache_discovered_token(metadata, chain)

            return self._metadata_to_response(metadata)

        except TimeoutError:
            context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
            error_msg = f"On-chain lookup timed out for {address} on {chain}"
            context.set_details(error_msg)
            logger.warning(error_msg)
            return self._error_response(error_msg)

        except Exception as e:
            logger.error(f"GetTokenMetadata failed for {address} on {chain}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return self._error_response(str(e))

    def _cache_discovered_token(self, metadata: TokenMetadata, chain: str) -> None:
        """Cache a discovered token in the resolver.

        Args:
            metadata: On-chain token metadata
            chain: Chain name
        """
        try:
            from datetime import datetime

            from almanak.core.enums import Chain
            from almanak.framework.data.tokens.models import CHAIN_ID_MAP, BridgeType

            # Find Chain enum
            chain_enum = None
            for c in Chain:
                if c.value.lower() == chain.lower():
                    chain_enum = c
                    break

            if chain_enum is None:
                logger.warning(f"Unknown chain {chain} - not caching discovered token")
                return

            # Create ResolvedToken for caching
            resolved = ResolvedToken(
                symbol=metadata.symbol,
                address=metadata.address,
                decimals=metadata.decimals,
                chain=chain_enum,
                chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
                name=metadata.name,
                coingecko_id=None,
                is_stablecoin=False,
                is_native=metadata.is_native,
                is_wrapped_native=False,
                canonical_symbol=metadata.symbol,
                bridge_type=BridgeType.NATIVE,
                source="on_chain",
                is_verified=False,
                resolved_at=datetime.now(),
            )

            # Register in resolver (which handles caching)
            self._resolver.register(resolved)
            logger.debug(f"Cached discovered token {metadata.symbol} at {metadata.address} on {chain}")

        except Exception as e:
            # Caching failure shouldn't break the response
            logger.warning(f"Failed to cache discovered token: {e}")

    async def GetTokenDecimals(
        self,
        request: gateway_pb2.GetTokenDecimalsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GetTokenDecimalsResponse:
        """Get token decimals (lightweight endpoint).

        This is a convenience method when only decimals are needed.
        Faster than full resolution as it doesn't need all metadata.

        Args:
            request: GetTokenDecimalsRequest with token and chain
            context: gRPC context

        Returns:
            GetTokenDecimalsResponse with decimals
        """
        token = request.token
        chain = request.chain

        # Validate chain
        try:
            chain = validate_chain(chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GetTokenDecimalsResponse(success=False, decimals=0, error=str(e))

        if not token:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Token is required")
            return gateway_pb2.GetTokenDecimalsResponse(success=False, decimals=0, error="Token is required")

        try:
            decimals = self._resolver.get_decimals(chain, token)
            return gateway_pb2.GetTokenDecimalsResponse(success=True, decimals=decimals, error="")

        except TokenNotFoundError as e:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(e))
            return gateway_pb2.GetTokenDecimalsResponse(success=False, decimals=0, error=str(e))

        except TokenResolutionError as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.GetTokenDecimalsResponse(success=False, decimals=0, error=str(e))

        except Exception as e:
            logger.error(f"GetTokenDecimals failed for {token} on {chain}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.GetTokenDecimalsResponse(success=False, decimals=0, error=str(e))

    async def BatchResolveTokens(
        self,
        request: gateway_pb2.BatchResolveTokensRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.BatchResolveTokensResponse:
        """Resolve multiple tokens in a single call.

        More efficient than individual ResolveToken calls for multiple tokens.
        Returns results for all tokens, with individual errors for failures.

        Args:
            request: BatchResolveTokensRequest with tokens and chain
            context: gRPC context

        Returns:
            BatchResolveTokensResponse with list of token metadata
        """
        tokens = list(request.tokens)
        chain = request.chain

        # Validate chain
        try:
            chain = validate_chain(chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BatchResolveTokensResponse(
                success=False,
                tokens=[],
                error=str(e),
            )

        if not tokens:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("At least one token is required")
            return gateway_pb2.BatchResolveTokensResponse(
                success=False,
                tokens=[],
                error="At least one token is required",
            )

        # Validate batch size
        try:
            validate_batch_size(tokens, "tokens")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BatchResolveTokensResponse(
                success=False,
                tokens=[],
                error=str(e),
            )

        results: list[gateway_pb2.TokenMetadataResponse] = []
        all_success = True

        for token in tokens:
            try:
                resolved = self._resolver.resolve(token, chain)
                results.append(self._resolved_to_response(resolved))

            except TokenResolutionError as e:
                all_success = False
                results.append(self._error_response(str(e)))

            except Exception as e:
                all_success = False
                logger.error(f"BatchResolveTokens failed for {token} on {chain}: {e}")
                results.append(self._error_response(str(e)))

        return gateway_pb2.BatchResolveTokensResponse(
            success=all_success,
            tokens=results,
            error="" if all_success else "Some tokens failed to resolve",
        )

    async def health_check(self) -> dict[str, Any]:
        """Check the health of the token service.

        Returns a health report with resolver stats, cache status, and gateway connectivity.
        This can be used by the gateway server to report token service health
        via the standard gRPC health check protocol.

        Returns:
            Dict with health status, resolver stats, cache stats, and gateway info
        """
        resolver_stats = self._resolver.stats()
        cache_stats = self._resolver.cache_stats()
        gateway_connected = self._resolver.is_gateway_connected()

        total_lookups = resolver_stats.get("cache_hits", 0) + resolver_stats.get("static_hits", 0)
        error_count = resolver_stats.get("errors", 0)

        # Determine health: degraded if gateway is expected but down, or high error rate
        healthy = True
        status = "serving"
        if total_lookups >= 100 and error_count / max(total_lookups, 1) > 0.1:
            healthy = False
            status = "degraded_high_error_rate"

        return {
            "healthy": healthy,
            "status": status,
            "resolver_stats": resolver_stats,
            "cache_stats": cache_stats,
            "gateway_connected": gateway_connected,
            "onchain_lookups_active": len(self._onchain_lookups),
        }

    async def close(self) -> None:
        """Close the service and release resources."""
        async with self._lookups_lock:
            for lookup in self._onchain_lookups.values():
                await lookup.close()
            self._onchain_lookups.clear()
        logger.info("TokenService closed")


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "TokenServiceServicer",
    "TokenRateLimiter",
    "DEFAULT_ONCHAIN_TIMEOUT",
    "DEFAULT_RATE_LIMIT",
]
