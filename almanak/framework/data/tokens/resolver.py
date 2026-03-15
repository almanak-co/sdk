"""Token resolver - unified API for all token lookups.

This module provides the TokenResolver class which is the main entry point
for all token resolution in the Almanak framework. It implements a multi-layer
resolution strategy with caching for optimal performance.

Resolution Order:
    1. Memory cache (fastest, <1ms)
    2. Disk cache (fast, <10ms)
    3. Static registry (fast, <5ms)
    4. Gateway on-chain lookup (slower, <500ms, requires gateway connection)

Key Components:
    - TokenResolver: Main resolver class (thread-safe singleton)
    - get_token_resolver(): Get the singleton instance

Performance Targets:
    - Cache hit: <1ms
    - Static registry: <5ms
    - Gateway on-chain: <500ms

Gateway Connection:
    The resolver can optionally connect to a gateway for on-chain token discovery.
    If the gateway is unavailable, static resolution still works (graceful fallback).

    # With gateway (enables on-chain discovery)
    import grpc
    channel = grpc.insecure_channel("localhost:50051")
    resolver = get_token_resolver(gateway_channel=channel)

    # Without gateway (static resolution only)
    resolver = get_token_resolver()

Example:
    from almanak.framework.data.tokens.resolver import get_token_resolver

    resolver = get_token_resolver()

    # Resolve by symbol
    usdc = resolver.resolve("USDC", "arbitrum")
    print(f"{usdc.symbol} has {usdc.decimals} decimals at {usdc.address}")

    # Resolve by address
    token = resolver.resolve("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")

    # Get decimals directly
    decimals = resolver.get_decimals("arbitrum", "USDC")

    # Resolve a trading pair
    usdc, weth = resolver.resolve_pair("USDC", "WETH", "arbitrum")
"""

import logging
import re
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from almanak.core.enums import Chain

from .cache import TokenCacheManager
from .defaults import DEFAULT_TOKENS, NATIVE_SENTINEL, SYMBOL_ALIASES, WRAPPED_NATIVE
from .exceptions import InvalidTokenAddressError, TokenNotFoundError, TokenResolutionError
from .models import CHAIN_ID_MAP, BridgeType, ResolvedToken, Token

if TYPE_CHECKING:
    import grpc

logger = logging.getLogger(__name__)


def _try_record_metric(func_name: str, *args: Any, **kwargs: Any) -> None:
    """Attempt to record a Prometheus metric, silently ignoring import failures.

    This allows the resolver to work without the gateway metrics module installed
    (e.g., in framework-only deployments or tests).
    """
    try:
        from almanak.gateway import metrics

        func = getattr(metrics, func_name, None)
        if func:
            func(*args, **kwargs)
    except ImportError:
        pass


# Address validation patterns
ADDRESS_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")
# Pattern to detect strings that look like addresses (start with 0x and are ~42 chars)
ADDRESS_LIKE_PATTERN = re.compile(r"^0x[a-zA-Z0-9]{38,42}$")
# Solana base58 address pattern (32-44 chars, base58 alphabet: no 0, O, I, l)
SOLANA_ADDRESS_PATTERN = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def _is_address(token: str, chain: str | None = None) -> bool:
    """Check if a token string is a valid address.

    If chain is provided, checks format for that chain's family.
    If chain is None, checks if it matches ANY known address format.
    """
    if chain and chain.lower() == "solana":
        return bool(SOLANA_ADDRESS_PATTERN.match(token))
    if ADDRESS_PATTERN.match(token):
        return True
    # When chain is unspecified, also accept Solana addresses
    if chain is None and SOLANA_ADDRESS_PATTERN.match(token):
        return True
    return False


def _looks_like_address(token: str) -> bool:
    """Check if a token string looks like it's trying to be an address.

    This catches cases like "0xGHIJ..." which are malformed addresses.
    """
    return bool(ADDRESS_LIKE_PATTERN.match(token))


def _validate_address(address: str, chain: str) -> None:
    """Validate an address format for the given chain.

    For EVM chains: must be 0x-prefixed, 42-char hex.
    For Solana: must be 32-44 char base58 (no 0, O, I, l).

    Args:
        address: The address to validate
        chain: Chain name for error context

    Raises:
        InvalidTokenAddressError: If address format is invalid
    """
    if chain.lower() == "solana":
        if not SOLANA_ADDRESS_PATTERN.match(address):
            raise InvalidTokenAddressError(
                token=address,
                chain=chain,
                reason="Solana address must be 32-44 base58 characters",
            )
        return

    # EVM validation
    if not address.startswith("0x"):
        raise InvalidTokenAddressError(
            token=address,
            chain=chain,
            reason="Address must start with '0x'",
        )
    if len(address) != 42:
        raise InvalidTokenAddressError(
            token=address,
            chain=chain,
            reason=f"Address must be 42 characters, got {len(address)}",
        )
    if not ADDRESS_PATTERN.match(address):
        raise InvalidTokenAddressError(
            token=address,
            chain=chain,
            reason="Address contains invalid hex characters",
        )


def _normalize_address_for_chain(address: str, chain: str) -> str:
    """Normalize an address for comparison/indexing.

    EVM addresses are case-insensitive -> lowercase.
    Solana base58 addresses are case-sensitive -> preserve case.
    """
    if chain.lower() == "solana":
        return address
    return address.lower()


def _normalize_chain(chain: str | Chain) -> tuple[str, Chain]:
    """Normalize chain input to both string and Chain enum.

    Uses the central resolve_chain_name() for alias resolution.

    Args:
        chain: Chain as string or Chain enum

    Returns:
        Tuple of (chain_name_lower, Chain enum)

    Raises:
        TokenResolutionError: If chain is not recognized
    """
    if isinstance(chain, Chain):
        return chain.value.lower(), chain

    try:
        from almanak.core.constants import resolve_chain_name

        chain_lower = resolve_chain_name(chain)
    except (ValueError, ImportError):
        chain_lower = chain.lower()

    # Try to find matching Chain enum
    for c in Chain:
        if c.value.lower() == chain_lower:
            return chain_lower, c

    raise TokenResolutionError(
        token="",
        chain=chain,
        reason=f"Unknown chain '{chain}'",
        suggestions=[f"Supported chains: {', '.join(c.value.lower() for c in Chain)}"],
    )


class TokenResolver:
    """Unified token resolver with multi-layer caching.

    This class provides the main API for token resolution in the Almanak framework.
    It implements a singleton pattern for thread-safe global access.

    Resolution Order:
        1. Memory cache - fastest, O(1)
        2. Disk cache - loads from JSON, promotes to memory
        3. Static registry - DEFAULT_TOKENS from defaults.py
        4. Gateway on-chain lookup - queries ERC20 contracts (if gateway_client provided)

    Thread Safety:
        Uses threading.RLock for all operations. Safe for concurrent access.

    Attributes:
        gateway_client: Optional gateway client for on-chain lookups

    Example:
        resolver = TokenResolver.get_instance()

        # Resolve by symbol
        token = resolver.resolve("USDC", "arbitrum")

        # Resolve by address
        token = resolver.resolve("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")

        # Register a custom token
        resolver.register(my_custom_token)
    """

    _instance: "TokenResolver | None" = None
    _instance_lock = threading.Lock()

    def __init__(
        self,
        gateway_client: Any | None = None,
        cache_file: str | None = None,
        gateway_channel: "grpc.Channel | None" = None,
    ) -> None:
        """Initialize the TokenResolver.

        NOTE: Prefer using get_instance() for singleton access.

        Args:
            gateway_client: DEPRECATED - Use gateway_channel instead.
                           Kept for backward compatibility.
            cache_file: Optional path to cache file. Defaults to ~/.almanak/token_cache.json
            gateway_channel: Optional gRPC channel to gateway for on-chain lookups.
                            If None, only static resolution is available.
                            On-chain discovery will gracefully fall back to static
                            resolution if the gateway becomes unavailable.
        """
        # Handle backward compatibility - gateway_client is deprecated
        self._gateway_client = gateway_client
        self._gateway_channel = gateway_channel
        self._gateway_stub: Any | None = None  # Lazy initialized TokenServiceStub
        self._gateway_available: bool | None = None  # None = unknown, True/False = cached state
        self._gateway_check_time: float = 0  # Last time we checked gateway availability
        self._cache = TokenCacheManager(cache_file=cache_file)
        self._lock = threading.RLock()

        # Build static registry index for fast lookups
        # Maps: chain_lower -> symbol_upper -> Token
        self._static_registry: dict[str, dict[str, Token]] = {}
        # Maps: chain_lower -> address_lower -> Token
        self._static_address_index: dict[str, dict[str, Token]] = {}

        self._build_static_indices()

        # Performance tracking
        self._stats = {
            "cache_hits": 0,
            "static_hits": 0,
            "gateway_lookups": 0,
            "gateway_errors": 0,
            "errors": 0,
        }

    def _build_static_indices(self) -> None:
        """Build indices for fast static registry lookups."""
        for token in DEFAULT_TOKENS:
            for chain_name in token.chains:
                chain_lower = chain_name.lower()

                # Index by symbol
                if chain_lower not in self._static_registry:
                    self._static_registry[chain_lower] = {}
                self._static_registry[chain_lower][token.symbol.upper()] = token

                # Index by address (case-insensitive for EVM, case-sensitive for Solana)
                address = token.get_address(chain_name)
                if address:
                    addr_key = _normalize_address_for_chain(address, chain_lower)
                    if chain_lower not in self._static_address_index:
                        self._static_address_index[chain_lower] = {}
                    self._static_address_index[chain_lower][addr_key] = token

    @classmethod
    def get_instance(
        cls,
        gateway_client: Any | None = None,
        cache_file: str | None = None,
        gateway_channel: "grpc.Channel | None" = None,
    ) -> "TokenResolver":
        """Get the singleton TokenResolver instance.

        This is the recommended way to get a TokenResolver. The first call
        creates the instance, subsequent calls return the same instance.

        Args:
            gateway_client: DEPRECATED - Use gateway_channel instead.
            cache_file: Optional path to cache file. Only used on first call.
            gateway_channel: Optional gRPC channel to gateway for on-chain lookups.
                            Only used on first call when creating instance.
                            Pass a grpc.Channel connected to the gateway server.

        Returns:
            The singleton TokenResolver instance

        Example:
            # Without gateway (static resolution only)
            resolver = TokenResolver.get_instance()
            token = resolver.resolve("USDC", "arbitrum")

            # With gateway (enables on-chain discovery)
            import grpc
            channel = grpc.insecure_channel("localhost:50051")
            resolver = TokenResolver.get_instance(gateway_channel=channel)
        """
        if cls._instance is None:
            with cls._instance_lock:
                # Double-check locking
                if cls._instance is None:
                    cls._instance = cls(
                        gateway_client=gateway_client,
                        cache_file=cache_file,
                        gateway_channel=gateway_channel,
                    )
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance. Primarily for testing."""
        with cls._instance_lock:
            cls._instance = None

    def resolve(
        self, token: str, chain: str | Chain, *, log_errors: bool = True, skip_gateway: bool = False
    ) -> ResolvedToken:
        """Resolve a token by symbol or address on a specific chain.

        This is the main resolution method. It checks:
        1. Memory cache
        2. Disk cache
        3. Static registry
        4. Gateway on-chain lookup (if token is an address and gateway available)

        Args:
            token: Token symbol (e.g., "USDC") or address (e.g., "0x...")
            chain: Chain name or Chain enum
            log_errors: If False, suppress warning logs on resolution failure (default True).
                Use False for best-effort lookups where failures are expected and handled.
            skip_gateway: If True, skip the slow gateway on-chain lookup and fail fast
                after cache + static registry. Use for cosmetic/best-effort lookups
                where a 30s gateway timeout is unacceptable.

        Returns:
            ResolvedToken with full metadata

        Raises:
            TokenNotFoundError: If token cannot be resolved
            InvalidTokenAddressError: If address format is invalid
            TokenResolutionError: For other resolution errors

        Example:
            # By symbol
            usdc = resolver.resolve("USDC", "arbitrum")

            # By address
            token = resolver.resolve("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")
        """
        start_time = time.perf_counter()
        chain_lower, chain_enum = _normalize_chain(chain)

        try:
            # Determine if input is address or symbol (pure functions, no lock needed)
            is_address = _is_address(token, chain_lower)

            if is_address:
                _validate_address(token, chain_lower)
            elif _looks_like_address(token):
                _validate_address(token, chain_lower)

            # Fast path: cache + static registry (under lock)
            with self._lock:
                if is_address:
                    result = self._try_fast_resolve_address(token, chain_lower, chain_enum)
                else:
                    result = self._resolve_by_symbol(token, chain_lower, chain_enum)
                    # Symbol resolution is fully handled (no gateway path for symbols)
                    # _resolve_by_symbol raises TokenNotFoundError if not found

            if result is not None:
                self._record_resolution_success(token, chain_lower, result, start_time)
                return result

            # Slow path: gateway on-chain lookup (NO lock held)
            # Only reached for address resolution when not in cache/static
            if not skip_gateway and (self._gateway_channel is not None or self._gateway_client is not None):
                resolved = self._resolve_via_gateway(token, chain_lower, chain_enum)
                if resolved:
                    # Write back to cache (under lock)
                    with self._lock:
                        self._cache.put(resolved)
                    self._record_resolution_success(token, chain_lower, resolved, start_time)
                    return resolved

            # Token not found - provide helpful error
            _try_record_metric("record_token_resolution_cache_miss", chain_lower)
            suggestions = [
                "Verify the contract address is correct",
                "Check if the address is deployed on this chain",
                "Use register() to add custom tokens",
            ]
            if self._gateway_channel is None and self._gateway_client is None:
                suggestions.append("Connect to gateway for on-chain token discovery")

            raise TokenNotFoundError(
                token=token,
                chain=chain_lower,
                reason=f"Address not found in registry for {chain_lower}",
                suggestions=suggestions,
            )

        except TokenResolutionError as e:
            with self._lock:
                self._stats["errors"] += 1
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            error_type = type(e).__name__
            latency_ms = round(elapsed_ms, 3)
            if log_errors:
                logger.warning(
                    "token_resolution_error token=%s chain=%s error_type=%s detail=%s latency_ms=%.3f",
                    token,
                    chain_lower,
                    error_type,
                    str(e),
                    latency_ms,
                    extra={
                        "token": token,
                        "chain": chain_lower,
                        "error_type": error_type,
                        "latency_ms": latency_ms,
                        "error_detail": str(e),
                    },
                )
            _try_record_metric("record_token_resolution_error", chain_lower, error_type)
            raise

    def _try_fast_resolve_address(self, address: str, chain_lower: str, chain_enum: Chain) -> ResolvedToken | None:
        """Try to resolve an address from cache or static registry (must be called under lock).

        Returns:
            ResolvedToken if found in cache or static, None if gateway lookup needed.
        """
        addr_key = _normalize_address_for_chain(address, chain_lower)

        # 1. Check cache (memory + disk)
        cached = self._cache.get(chain_lower, address=addr_key)
        if cached:
            self._stats["cache_hits"] += 1
            logger.debug(
                "token_cache_hit",
                extra={"token": address, "chain": chain_lower, "cache_type": "memory"},
            )
            _try_record_metric("record_token_resolution_cache_hit", chain_lower, "memory")
            return cached

        # 2. Check static registry address index
        chain_index = self._static_address_index.get(chain_lower, {})
        static_token = chain_index.get(addr_key)

        if static_token:
            self._stats["static_hits"] += 1
            resolved = self._token_to_resolved(static_token, chain_lower, chain_enum, source="static")
            self._cache.put(resolved)
            logger.debug(
                "token_cache_miss",
                extra={"token": address, "chain": chain_lower, "resolved_via": "static"},
            )
            _try_record_metric("record_token_resolution_cache_hit", chain_lower, "static")
            return resolved

        # Not found in fast path - needs gateway lookup
        return None

    def _record_resolution_success(
        self, token: str, chain_lower: str, result: ResolvedToken, start_time: float
    ) -> None:
        """Record metrics and logging for a successful resolution."""
        elapsed_s = time.perf_counter() - start_time
        elapsed_ms = elapsed_s * 1000
        logger.debug(
            "token_resolved",
            extra={
                "token": token,
                "chain": chain_lower,
                "resolution_source": result.source,
                "latency_ms": round(elapsed_ms, 3),
            },
        )
        _try_record_metric("record_token_resolution_latency", chain_lower, result.source, elapsed_s)

    def _resolve_by_symbol(self, symbol: str, chain_lower: str, chain_enum: Chain) -> ResolvedToken:
        """Resolve a token by symbol."""
        symbol_upper = symbol.upper()

        # 1. Check cache (memory + disk)
        cached = self._cache.get(chain_lower, symbol=symbol_upper)
        if cached:
            self._stats["cache_hits"] += 1
            logger.debug(
                "token_cache_hit",
                extra={"token": symbol, "chain": chain_lower, "cache_type": "memory"},
            )
            _try_record_metric("record_token_resolution_cache_hit", chain_lower, "memory")
            return cached

        # 2. Check static registry
        chain_registry = self._static_registry.get(chain_lower, {})
        static_token = chain_registry.get(symbol_upper)

        if static_token:
            self._stats["static_hits"] += 1
            resolved = self._token_to_resolved(static_token, chain_lower, chain_enum, source="static")
            # Cache for future lookups
            self._cache.put(resolved)
            logger.debug(
                "token_cache_miss",
                extra={"token": symbol, "chain": chain_lower, "resolved_via": "static"},
            )
            _try_record_metric("record_token_resolution_cache_hit", chain_lower, "static")
            return resolved

        # 3. Check symbol aliases (bridged tokens like USDC.e, USDbC, USDT.e, WETH.e)
        alias_address = SYMBOL_ALIASES.get((chain_lower, symbol_upper))
        if alias_address:
            logger.debug(
                "token_alias_resolved",
                extra={"token": symbol, "chain": chain_lower, "alias_address": alias_address},
            )
            # Resolve by the canonical address
            return self._resolve_by_address(alias_address, chain_lower, chain_enum)

        # Token not found - provide helpful error
        _try_record_metric("record_token_resolution_cache_miss", chain_lower)
        raise TokenNotFoundError(
            token=symbol,
            chain=chain_lower,
            reason=f"Symbol '{symbol}' not found in registry for {chain_lower}",
            suggestions=self._get_symbol_suggestions(symbol_upper, chain_lower),
        )

    def _resolve_by_address(self, address: str, chain_lower: str, chain_enum: Chain) -> ResolvedToken:
        """Resolve a token by address from cache or static registry (must be called under lock).

        This method does NOT call the gateway. It is used for alias resolution
        (from _resolve_by_symbol) where the address should be in the static registry.
        Gateway-based address resolution is handled in resolve() outside the lock.
        """
        result = self._try_fast_resolve_address(address, chain_lower, chain_enum)
        if result is not None:
            return result

        # Address not in cache or static -- this shouldn't happen for aliases
        raise TokenNotFoundError(
            token=address,
            chain=chain_lower,
            reason=f"Address not found in registry for {chain_lower}",
            suggestions=[
                "Verify the contract address is correct",
                "Check if the address is deployed on this chain",
                "Use register() to add custom tokens",
            ],
        )

    def _get_gateway_stub(self) -> Any:
        """Get or create the gateway TokenService stub.

        Returns:
            TokenServiceStub for gateway communication, or None if unavailable
        """
        if self._gateway_stub is not None:
            return self._gateway_stub

        if self._gateway_channel is None:
            return None

        try:
            # Lazy import to avoid circular dependencies
            from almanak.gateway.proto import gateway_pb2_grpc

            self._gateway_stub = gateway_pb2_grpc.TokenServiceStub(self._gateway_channel)
            return self._gateway_stub
        except Exception as e:
            logger.warning(f"Failed to create gateway stub: {e}")
            return None

    def _check_gateway_available(self) -> bool:
        """Check if gateway is available.

        Uses cached state with 30-second TTL to avoid excessive checks.

        Returns:
            True if gateway is available, False otherwise
        """
        # Cache gateway availability state for 30 seconds
        cache_ttl = 30.0
        now = time.time()

        if self._gateway_available is not None and (now - self._gateway_check_time) < cache_ttl:
            return self._gateway_available

        # Check if we have a gateway channel
        if self._gateway_channel is None:
            self._gateway_available = False
            self._gateway_check_time = now
            return False

        # Try to get the stub - this validates the channel
        stub = self._get_gateway_stub()
        if stub is None:
            self._gateway_available = False
            self._gateway_check_time = now
            return False

        # Assume available - actual availability will be determined on use
        self._gateway_available = True
        self._gateway_check_time = now
        return True

    def _resolve_via_gateway(self, address: str, chain_lower: str, chain_enum: Chain) -> ResolvedToken | None:
        """Attempt to resolve token via gateway on-chain lookup.

        Makes a gRPC call to the gateway's GetTokenMetadata RPC to query
        the token contract directly for metadata.

        Args:
            address: Token contract address
            chain_lower: Chain name (lowercase)
            chain_enum: Chain enum value

        Returns:
            ResolvedToken if successful, None otherwise

        Note:
            - Gracefully returns None if gateway is unavailable (no error raised)
            - Caches discovered tokens for future lookups
            - Logs warnings on gateway errors but doesn't fail
        """
        # Check if gateway is available
        if not self._check_gateway_available():
            logger.debug(f"Gateway not available for on-chain lookup of {address} on {chain_lower}")
            return None

        stub = self._get_gateway_stub()
        if stub is None:
            return None

        with self._lock:
            self._stats["gateway_lookups"] += 1
        gateway_start = time.perf_counter()

        try:
            # Import proto message type
            from almanak.gateway.proto import gateway_pb2

            # Create request
            request = gateway_pb2.GetTokenMetadataRequest(
                address=address,
                chain=chain_lower,
            )

            # Make the RPC call with timeout.
            # 30s allows the server-side OnChainLookup to complete on cold starts
            # (first call initializes Web3 connection and queries ERC20 contract).
            response = stub.GetTokenMetadata(request, timeout=30.0)

            # Check if successful
            if not response.success:
                with self._lock:
                    self._stats["gateway_errors"] += 1
                logger.info(
                    "token_onchain_lookup_failed",
                    extra={
                        "token": address,
                        "chain": chain_lower,
                        "error": response.error,
                        "latency_ms": round((time.perf_counter() - gateway_start) * 1000, 3),
                    },
                )
                _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, "not_found")
                return None

            # Convert response to ResolvedToken
            resolved = ResolvedToken(
                symbol=response.symbol,
                address=response.address or address,
                decimals=response.decimals,
                chain=chain_enum,
                chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
                name=response.name or None,
                coingecko_id=None,
                is_stablecoin=False,  # Can't determine from on-chain
                is_native=False,  # If it has an address, it's not native
                is_wrapped_native=False,  # Can't determine from on-chain
                canonical_symbol=response.symbol,
                bridge_type=BridgeType.NATIVE,
                source="on_chain",
                is_verified=False,  # On-chain lookups are not verified
                resolved_at=datetime.now(),
            )

            # Note: cache write is done by the caller (resolve()) under lock
            gateway_elapsed_ms = (time.perf_counter() - gateway_start) * 1000
            logger.info(
                "token_onchain_discovered",
                extra={
                    "token": address,
                    "chain": chain_lower,
                    "symbol": response.symbol,
                    "decimals": response.decimals,
                    "latency_ms": round(gateway_elapsed_ms, 3),
                },
            )
            _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, "success")

            return resolved

        except Exception as e:
            # Log the error but don't fail - graceful fallback
            error_str = str(e)
            with self._lock:
                self._stats["gateway_errors"] += 1
            gateway_elapsed_ms = (time.perf_counter() - gateway_start) * 1000

            # Check for common gRPC errors
            is_timeout = "DEADLINE_EXCEEDED" in error_str.upper()
            is_unavailable = "UNAVAILABLE" in error_str.upper()

            if is_unavailable:
                # Gateway is truly unreachable - cache as unavailable for TTL
                self._gateway_available = False
                self._gateway_check_time = time.time()
                status = "error"
                logger.warning(
                    "token_gateway_unavailable",
                    extra={
                        "token": address,
                        "chain": chain_lower,
                        "error": error_str,
                        "latency_ms": round(gateway_elapsed_ms, 3),
                    },
                )
            elif is_timeout:
                # Timeout - gateway is reachable but slow (e.g. cold on-chain lookup).
                # Do NOT cache as unavailable so the next attempt can retry.
                status = "timeout"
                logger.warning(
                    "token_gateway_timeout",
                    extra={
                        "token": address,
                        "chain": chain_lower,
                        "error": error_str,
                        "latency_ms": round(gateway_elapsed_ms, 3),
                    },
                )
            else:
                status = "error"
                logger.warning(
                    "token_onchain_lookup_error",
                    extra={
                        "token": address,
                        "chain": chain_lower,
                        "error": error_str,
                        "latency_ms": round(gateway_elapsed_ms, 3),
                    },
                )

            _try_record_metric("record_token_resolution_onchain_lookup", chain_lower, status)
            return None

    def is_gateway_connected(self) -> bool:
        """Check if gateway is connected and available for on-chain lookups.

        This method checks if a gateway channel is configured and appears to be
        connected. Note that the actual availability is verified lazily - the
        gateway might become unavailable between this check and actual use.

        Returns:
            True if gateway channel is configured and appears available,
            False otherwise.

        Example:
            resolver = get_token_resolver(gateway_channel=channel)
            if resolver.is_gateway_connected():
                print("Gateway available for on-chain token discovery")
            else:
                print("Static resolution only")
        """
        with self._lock:
            return self._check_gateway_available()

    def set_gateway_channel(self, channel: "grpc.Channel | None") -> None:
        """Set or update the gateway channel.

        This allows changing the gateway connection after initialization.
        Useful for reconnection scenarios or testing.

        Args:
            channel: gRPC channel to gateway, or None to disable gateway

        Example:
            import grpc
            resolver = get_token_resolver()

            # Connect to gateway later
            channel = grpc.insecure_channel("localhost:50051")
            resolver.set_gateway_channel(channel)

            # Disconnect from gateway
            resolver.set_gateway_channel(None)
        """
        with self._lock:
            self._gateway_channel = channel
            self._gateway_stub = None  # Reset stub to force re-creation
            self._gateway_available = None  # Reset availability state
            self._gateway_check_time = 0

            if channel is not None:
                logger.info("Gateway channel configured for on-chain token discovery")
            else:
                logger.info("Gateway channel disconnected - static resolution only")

    def _token_to_resolved(
        self,
        token: Token,
        chain_lower: str,
        chain_enum: Chain,
        source: str = "static",
    ) -> ResolvedToken:
        """Convert a Token to ResolvedToken for a specific chain."""
        address = token.get_address(chain_lower)
        if not address:
            raise TokenNotFoundError(
                token=token.symbol,
                chain=chain_lower,
                reason=f"Token '{token.symbol}' not available on {chain_lower}",
            )

        decimals = token.get_decimals(chain_lower)
        chain_config = token.get_chain_config(chain_lower)

        # Determine if native token
        addr_norm = _normalize_address_for_chain(address, chain_lower)
        is_native = addr_norm == _normalize_address_for_chain(NATIVE_SENTINEL, chain_lower)
        if chain_config:
            is_native = chain_config.is_native

        # Determine bridge type
        bridge_type = BridgeType.NATIVE
        if chain_config:
            bridge_type = chain_config.bridge_type

        # Check if wrapped native by comparing address to WRAPPED_NATIVE registry
        wrapped_addr = WRAPPED_NATIVE.get(chain_lower, "")
        is_wrapped_native = bool(wrapped_addr and addr_norm == _normalize_address_for_chain(wrapped_addr, chain_lower))

        return ResolvedToken(
            symbol=token.symbol,
            address=address,
            decimals=decimals,
            chain=chain_enum,
            chain_id=CHAIN_ID_MAP.get(chain_enum, 0),
            name=token.name,
            coingecko_id=token.coingecko_id,
            is_stablecoin=token.is_stablecoin,
            is_native=is_native,
            is_wrapped_native=is_wrapped_native,
            canonical_symbol=token.symbol,
            bridge_type=bridge_type,
            source=source,
            is_verified=True,
            resolved_at=datetime.now(),
        )

    def _get_symbol_suggestions(self, symbol: str, chain: str) -> list[str]:
        """Get suggestions for similar symbols."""
        suggestions = []

        # Look for similar symbols in registry
        chain_registry = self._static_registry.get(chain, {})
        all_symbols = list(chain_registry.keys())

        # Find symbols that start with same letters or contain similar parts
        for s in all_symbols:
            if s.startswith(symbol[:2]) or symbol[:3] in s:
                suggestions.append(f"Did you mean '{s}'?")

        # Limit suggestions
        return suggestions[:3]

    def resolve_pair(
        self,
        token_in: str,
        token_out: str,
        chain: str | Chain,
    ) -> tuple[ResolvedToken, ResolvedToken]:
        """Resolve a pair of tokens for a swap operation.

        Convenience method for resolving both tokens in a trading pair.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            chain: Chain name or Chain enum

        Returns:
            Tuple of (resolved_token_in, resolved_token_out)

        Raises:
            TokenNotFoundError: If either token cannot be resolved
            TokenResolutionError: For other resolution errors

        Example:
            usdc, weth = resolver.resolve_pair("USDC", "WETH", "arbitrum")
        """
        resolved_in = self.resolve(token_in, chain)
        resolved_out = self.resolve(token_out, chain)
        return resolved_in, resolved_out

    def get_decimals(self, chain: str | Chain, token: str) -> int:
        """Get the decimals for a token on a specific chain.

        Convenience method that extracts just the decimals from resolution.
        NEVER defaults to 18 - always raises TokenNotFoundError if unknown.

        Args:
            chain: Chain name or Chain enum
            token: Token symbol or address

        Returns:
            Number of decimal places

        Raises:
            TokenNotFoundError: If token cannot be resolved

        Example:
            decimals = resolver.get_decimals("arbitrum", "USDC")
            # Returns 6
        """
        resolved = self.resolve(token, chain)
        return resolved.decimals

    def get_address(self, chain: str | Chain, symbol: str) -> str:
        """Get the address for a token symbol on a specific chain.

        Convenience method that extracts just the address from resolution.

        Args:
            chain: Chain name or Chain enum
            symbol: Token symbol (e.g., "USDC")

        Returns:
            Contract address

        Raises:
            TokenNotFoundError: If token cannot be resolved

        Example:
            address = resolver.get_address("arbitrum", "USDC")
            # Returns "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        """
        resolved = self.resolve(symbol, chain)
        return resolved.address

    def resolve_for_swap(self, token: str, chain: str | Chain) -> ResolvedToken:
        """Resolve a token for swap operations, auto-wrapping native tokens.

        This method resolves a token and if it's a native token (ETH, MATIC, AVAX, BNB),
        automatically returns the wrapped version instead (WETH, WMATIC, WAVAX, WBNB).
        This is because most DEX protocols cannot swap native tokens directly.

        For non-native tokens, this behaves identically to resolve().

        Args:
            token: Token symbol (e.g., "ETH", "USDC") or address
            chain: Chain name or Chain enum

        Returns:
            ResolvedToken - wrapped version if native, original otherwise

        Raises:
            TokenNotFoundError: If token or wrapped version cannot be resolved
            InvalidTokenAddressError: If address format is invalid
            TokenResolutionError: For other resolution errors

        Example:
            # ETH on Arbitrum returns WETH
            token = resolver.resolve_for_swap("ETH", "arbitrum")
            assert token.symbol == "WETH"

            # USDC returns USDC (not native)
            token = resolver.resolve_for_swap("USDC", "arbitrum")
            assert token.symbol == "USDC"
        """
        resolved = self.resolve(token, chain)

        # If it's a native token, resolve to wrapped version
        if resolved.is_native:
            chain_lower, _ = _normalize_chain(chain)
            wrapped_address = WRAPPED_NATIVE.get(chain_lower)

            if wrapped_address:
                logger.debug(f"Auto-wrapping native token {resolved.symbol} -> wrapped version on {chain_lower}")
                # Resolve the wrapped token by address to get full metadata
                return self.resolve(wrapped_address, chain)
            else:
                # No wrapped native defined for this chain - log warning and return original
                logger.warning(f"No wrapped native token defined for {chain_lower}, returning native {resolved.symbol}")
                return resolved

        return resolved

    def resolve_for_protocol(
        self,
        token: str,
        chain: str | Chain,
        protocol: str,
    ) -> ResolvedToken:
        """Resolve a token with protocol-specific handling.

        This method provides a hook for future protocol-specific token resolution.
        Currently, it simply delegates to resolve_for_swap() for DEX protocols
        and to resolve() for other protocols.

        This allows for future expansion where specific protocols might have
        unique token requirements (e.g., protocol-specific wrapped tokens,
        canonical bridge tokens, etc.).

        Args:
            token: Token symbol or address
            chain: Chain name or Chain enum
            protocol: Protocol identifier (e.g., "uniswap_v3", "aave_v3")

        Returns:
            ResolvedToken with appropriate protocol handling

        Raises:
            TokenNotFoundError: If token cannot be resolved
            TokenResolutionError: For other resolution errors

        Example:
            # DEX protocols get auto-wrapped native tokens
            token = resolver.resolve_for_protocol("ETH", "arbitrum", "uniswap_v3")
            assert token.symbol == "WETH"

            # Lending protocols get the original token
            token = resolver.resolve_for_protocol("ETH", "ethereum", "aave_v3")
            assert token.symbol == "ETH"
        """
        # List of DEX protocols that need native token wrapping
        dex_protocols = {
            "uniswap_v3",
            "uniswap_v2",
            "sushiswap_v3",
            "sushiswap_v2",
            "pancakeswap_v3",
            "pancakeswap_v2",
            "aerodrome",
            "velodrome",
            "traderjoe_v2",
            "traderjoe_v1",
            "curve",
            "balancer",
            "camelot",
        }

        protocol_lower = protocol.lower()

        if protocol_lower in dex_protocols:
            # DEX protocols need wrapped native tokens
            return self.resolve_for_swap(token, chain)
        else:
            # Other protocols (lending, etc.) may accept native tokens
            return self.resolve(token, chain)

    def register(self, token: ResolvedToken) -> None:
        """Register a token explicitly at runtime.

        This allows adding custom tokens that aren't in the static registry.
        Registered tokens are stored in the cache.

        Args:
            token: ResolvedToken to register

        Example:
            custom_token = ResolvedToken(
                symbol="CUSTOM",
                address="0x...",
                decimals=18,
                chain=Chain.ARBITRUM,
                chain_id=42161,
                name="Custom Token",
            )
            resolver.register(custom_token)
        """
        with self._lock:
            self._cache.put(token)
            logger.debug(f"Registered token {token.symbol} on {token.chain.value}")

    def register_token(
        self,
        symbol: str,
        chain: str | Chain,
        address: str,
        decimals: int,
        *,
        name: str | None = None,
        coingecko_id: str | None = None,
        is_stablecoin: bool = False,
    ) -> ResolvedToken:
        """Register a custom token by its basic properties.

        Convenience wrapper around register() for strategy authors who need to
        register protocol-specific tokens (e.g., Pendle PT/YT, LP tokens) that
        aren't in the static registry.

        After registration, the token is resolvable via resolve(), get_address(),
        and get_decimals() within the same process.

        Note: This registers tokens in the local resolver only. Gateway-backed
        lookups (e.g., MarketSnapshot.balance() by symbol) require the gateway
        to also know the token. For balance queries on custom tokens, use the
        token address directly: market.balance("0x...").

        Args:
            symbol: Token symbol (e.g., "PT-wstETH-25JUN2026")
            chain: Chain name or Chain enum
            address: Token contract address
            decimals: Token decimal places
            name: Optional human-readable name
            coingecko_id: Optional CoinGecko ID for price fetching
            is_stablecoin: Whether this is a stablecoin (default False)

        Returns:
            The registered ResolvedToken (can be used immediately)

        Raises:
            InvalidTokenAddressError: If address format is invalid
            TokenResolutionError: If chain is not recognized

        Example:
            resolver = get_token_resolver()
            resolver.register_token(
                symbol="PT-wstETH-25JUN2026",
                chain="arbitrum",
                address="0x71fbf40651e9d4bc027876e5aa4a3806d8e0b243",
                decimals=18,
            )
            # Now works:
            token = resolver.resolve("PT-wstETH-25JUN2026", "arbitrum")
        """
        chain_lower, chain_enum = _normalize_chain(chain)
        _validate_address(address, chain_lower)

        from almanak.core.constants import get_chain_id

        try:
            chain_id = get_chain_id(chain_enum)
        except ValueError:
            chain_id = 0  # Fallback for chains without EIP-155 IDs (e.g., Solana)

        try:
            resolved = ResolvedToken(
                symbol=symbol,
                address=_normalize_address_for_chain(address, chain_lower),
                decimals=decimals,
                chain=chain_enum,
                chain_id=chain_id,
                name=name or symbol,
                coingecko_id=coingecko_id,
                is_stablecoin=is_stablecoin,
                is_native=False,
                is_wrapped_native=False,
                canonical_symbol=symbol.upper(),
                source="registered",
                is_verified=False,
            )
        except ValueError as e:
            raise TokenResolutionError(
                token=symbol,
                chain=chain_lower,
                reason=str(e),
            ) from e

        self.register(resolved)
        logger.info(f"Registered custom token {symbol} ({address}) on {chain_lower} with {decimals} decimals")
        return resolved

    def stats(self) -> dict[str, int]:
        """Get resolver performance statistics.

        Returns:
            Dict with cache_hits, static_hits, gateway_lookups, errors
        """
        with self._lock:
            return dict(self._stats)

    def cache_stats(self) -> dict[str, int]:
        """Get cache performance statistics.

        Returns:
            Dict with memory_hits, disk_hits, misses, evictions
        """
        return self._cache.stats()


def get_token_resolver(
    gateway_client: Any | None = None,
    cache_file: str | None = None,
    gateway_channel: "grpc.Channel | None" = None,
) -> TokenResolver:
    """Get the singleton TokenResolver instance.

    This is the recommended entry point for token resolution.

    Args:
        gateway_client: DEPRECATED - Use gateway_channel instead.
        cache_file: Optional path to cache file. Only used on first call.
        gateway_channel: Optional gRPC channel to gateway for on-chain lookups.
                        If None, only static resolution is available.
                        On-chain discovery gracefully falls back to static
                        resolution if the gateway becomes unavailable.

    Returns:
        The singleton TokenResolver instance

    Example:
        from almanak.framework.data.tokens import get_token_resolver

        # Static resolution only
        resolver = get_token_resolver()
        usdc = resolver.resolve("USDC", "arbitrum")

        # With gateway for on-chain discovery
        import grpc
        channel = grpc.insecure_channel("localhost:50051")
        resolver = get_token_resolver(gateway_channel=channel)
    """
    return TokenResolver.get_instance(
        gateway_client=gateway_client,
        cache_file=cache_file,
        gateway_channel=gateway_channel,
    )


__all__ = [
    "TokenResolver",
    "get_token_resolver",
]
