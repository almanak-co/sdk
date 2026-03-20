"""Web3 Balance Provider for on-chain balance queries.

This module provides a production-ready balance provider using Web3.py,
with proper caching, decimal handling, and error recovery.

Key Features:
    - Query ERC-20 balances via balanceOf call
    - Query native ETH balance via eth_getBalance
    - Handle token decimals correctly (6 for USDC, 18 for WETH, etc.)
    - Token metadata registry per chain
    - Cache balances with short TTL (5s) to reduce RPC load
    - Cache invalidation after transaction execution
    - RPC error handling with retry and clear error messages

Example:
    from almanak.framework.data.balance import Web3BalanceProvider

    provider = Web3BalanceProvider(
        rpc_url="https://arb1.arbitrum.io/rpc",
        wallet_address="0x1234...",
        chain="arbitrum",
    )

    # Query WETH balance
    result = await provider.get_balance("WETH")
    print(f"Balance: {result.balance} WETH")

    # Query native ETH
    result = await provider.get_native_balance()
    print(f"ETH Balance: {result.balance}")

    # Invalidate cache after transaction
    provider.invalidate_cache()
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.exceptions import ContractLogicError, Web3Exception

from almanak.framework.data.interfaces import (
    BalanceResult,
    DataSourceError,
    DataSourceUnavailable,
)

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Minimal ERC20 ABI for balanceOf and decimals
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
]

# Native token placeholder address (used by many protocols)
NATIVE_TOKEN_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"


# =============================================================================
# Token Metadata
# =============================================================================


@dataclass
class TokenMetadata:
    """Metadata for a token on a specific chain.

    Attributes:
        symbol: Token symbol (e.g., "WETH", "USDC")
        address: Token contract address
        decimals: Token decimal places (e.g., 6 for USDC, 18 for WETH)
        is_native: Whether this is the chain's native token
    """

    symbol: str
    address: str
    decimals: int
    is_native: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "symbol": self.symbol,
            "address": self.address,
            "decimals": self.decimals,
            "is_native": self.is_native,
        }


# Chain-specific native token symbols
NATIVE_TOKEN_SYMBOLS: dict[str, str] = {
    "ethereum": "ETH",
    "arbitrum": "ETH",
    "optimism": "ETH",
    "polygon": "MATIC",
    "base": "ETH",
    "avalanche": "AVAX",
    "plasma": "XPL",
    "mantle": "MNT",
}


# =============================================================================
# Cache Entry
# =============================================================================


@dataclass
class BalanceCacheEntry:
    """Cache entry for balance data.

    Attributes:
        result: The cached BalanceResult
        cached_at: When the entry was cached
        fetch_latency_ms: Time taken to fetch the data
    """

    result: BalanceResult
    cached_at: datetime
    fetch_latency_ms: float = 0.0


# =============================================================================
# Provider Health Metrics
# =============================================================================


@dataclass
class ProviderHealthMetrics:
    """Health metrics for the balance provider.

    Tracks RPC health for observability.
    """

    total_requests: int = 0
    successful_requests: int = 0
    cache_hits: int = 0
    timeouts: int = 0
    errors: int = 0
    total_latency_ms: float = 0.0
    last_error: str | None = None
    last_error_time: datetime | None = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_requests == 0:
            return 100.0
        return (self.successful_requests / self.total_requests) * 100

    @property
    def average_latency_ms(self) -> float:
        """Calculate average latency in milliseconds."""
        if self.successful_requests == 0:
            return 0.0
        return self.total_latency_ms / self.successful_requests

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/metrics."""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "cache_hits": self.cache_hits,
            "timeouts": self.timeouts,
            "errors": self.errors,
            "success_rate": round(self.success_rate, 2),
            "average_latency_ms": round(self.average_latency_ms, 2),
            "last_error": self.last_error,
            "last_error_time": (self.last_error_time.isoformat() if self.last_error_time else None),
        }


# =============================================================================
# Exceptions
# =============================================================================


class RPCError(DataSourceError):
    """Raised when an RPC call fails.

    Attributes:
        rpc_url: The RPC URL that was called
        method: The RPC method that failed
        original_error: The underlying exception
    """

    def __init__(
        self,
        message: str,
        rpc_url: str = "",
        method: str = "",
        original_error: Exception | None = None,
    ) -> None:
        self.rpc_url = rpc_url
        self.method = method
        self.original_error = original_error
        super().__init__(message)


class TokenNotFoundError(DataSourceError):
    """Raised when a token is not found in the registry.

    Attributes:
        token: The token symbol or address that wasn't found
        chain: The chain being queried
    """

    def __init__(self, token: str, chain: str) -> None:
        self.token = token
        self.chain = chain
        super().__init__(
            f"Token '{token}' not found in registry for chain '{chain}'. Use add_token() to register it or provide an address."
        )


# =============================================================================
# Web3 Balance Provider
# =============================================================================


class Web3BalanceProvider:
    """On-chain balance provider using Web3.py.

    Implements the BalanceProvider protocol with production-grade features:
    - Query ERC-20 balances via balanceOf call
    - Query native ETH/MATIC balance via eth_getBalance
    - Handle token decimals correctly
    - Maintain token metadata registry per chain
    - Cache balances with configurable TTL (default 5s)
    - Invalidate cache after transaction execution
    - Handle RPC errors with retry and clear error messages

    Example:
        provider = Web3BalanceProvider(
            rpc_url="https://arb1.arbitrum.io/rpc",
            wallet_address="0x1234...",
            chain="arbitrum",
        )

        # Query balance
        result = await provider.get_balance("WETH")
        print(f"WETH Balance: {result.balance}")

        # After executing a transaction, invalidate cache
        provider.invalidate_cache()

        # Or invalidate specific token
        provider.invalidate_cache("WETH")
    """

    def __init__(
        self,
        rpc_url: str,
        wallet_address: str,
        chain: str = "arbitrum",
        cache_ttl: int = 5,
        request_timeout: float = 10.0,
        max_retries: int = 3,
        retry_delay: float = 0.5,
        token_resolver: "TokenResolver | None" = None,
    ) -> None:
        """Initialize the Web3 balance provider.

        Args:
            rpc_url: RPC endpoint URL
            wallet_address: Wallet address to query balances for
            chain: Chain name (ethereum, arbitrum, optimism, polygon, base)
            cache_ttl: Cache time-to-live in seconds. Default 5.
            request_timeout: HTTP request timeout in seconds. Default 10.
            max_retries: Maximum number of retries on RPC failure. Default 3.
            retry_delay: Base delay between retries in seconds. Default 0.5.
            token_resolver: Optional TokenResolver instance. Defaults to get_token_resolver().
        """
        self._rpc_url = rpc_url
        self._wallet_address = AsyncWeb3.to_checksum_address(wallet_address)
        self._chain = chain.lower()
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._max_retries = max_retries
        self._retry_delay = retry_delay

        # Initialize Web3 with async HTTP provider
        self._w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))

        # Token resolver (unified token resolution)
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Native token symbol for this chain
        self._native_symbol = NATIVE_TOKEN_SYMBOLS.get(self._chain, "ETH")

        # Balance cache: token_symbol -> BalanceCacheEntry
        self._cache: dict[str, BalanceCacheEntry] = {}

        # Health metrics
        self._metrics = ProviderHealthMetrics()

        logger.info(
            "Initialized Web3BalanceProvider",
            extra={
                "rpc_url": self._mask_rpc_url(rpc_url),
                "wallet": wallet_address[:10] + "...",
                "chain": chain,
                "cache_ttl": cache_ttl,
            },
        )

    @staticmethod
    def _mask_rpc_url(url: str) -> str:
        """Mask sensitive parts of RPC URL for logging."""
        if "@" in url:
            # Contains credentials
            parts = url.split("@")
            return parts[0].split("//")[0] + "//***@" + parts[1]
        return url

    async def get_balance(self, token: str) -> BalanceResult:
        """Get the balance of a token for the configured wallet.

        This method handles:
        - Native token (ETH, MATIC) queries via eth_getBalance
        - ERC-20 token queries via balanceOf
        - Caching with TTL
        - Decimal conversion
        - RPC error handling with retries

        Args:
            token: Token symbol (e.g., "WETH", "USDC") or "ETH" for native.
                   Can also be a token contract address starting with "0x".

        Returns:
            BalanceResult with balance in human-readable units

        Raises:
            DataSourceUnavailable: If RPC is unavailable after retries
            TokenNotFoundError: If token symbol is not in registry
        """
        self._metrics.total_requests += 1
        token_key = token.upper()

        # Check cache first
        cached = self._get_cached(token_key)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            logger.debug(
                "Cache hit for %s balance",
                token_key,
                extra={"token": token_key, "source": "cache"},
            )
            return cached.result

        # Resolve token metadata
        token_meta = self._resolve_token(token)
        if token_meta is None:
            raise TokenNotFoundError(token, self._chain)

        # Query balance
        start_time = time.time()

        try:
            if token_meta.is_native:
                raw_balance = await self._get_native_balance_with_retry()
            else:
                raw_balance = await self._get_erc20_balance_with_retry(token_meta.address)

            latency_ms = (time.time() - start_time) * 1000

            # Convert to human-readable units
            balance = Decimal(raw_balance) / Decimal(10**token_meta.decimals)

            result = BalanceResult(
                balance=balance,
                token=token_meta.symbol,
                address=token_meta.address,
                decimals=token_meta.decimals,
                raw_balance=raw_balance,
                timestamp=datetime.now(UTC),
                stale=False,
            )

            # Update cache
            self._update_cache(token_key, result, latency_ms)

            # Update metrics
            self._metrics.successful_requests += 1
            self._metrics.total_latency_ms += latency_ms

            logger.debug(
                "Fetched balance for %s: %s (latency: %.2fms)",
                token_key,
                balance,
                latency_ms,
                extra={
                    "token": token_key,
                    "balance": str(balance),
                    "raw_balance": raw_balance,
                },
            )

            return result

        except Exception as e:
            self._metrics.errors += 1
            self._metrics.last_error = str(e)
            self._metrics.last_error_time = datetime.now(UTC)

            logger.error(
                "Failed to fetch balance for %s: %s",
                token_key,
                str(e),
                extra={"token": token_key, "error": str(e)},
            )

            # Try to return stale data if available
            stale = self._get_stale_cached(token_key)
            if stale is not None:
                logger.info(
                    "Returning stale balance for %s due to error",
                    token_key,
                )
                self._metrics.successful_requests += 1
                return BalanceResult(
                    balance=stale.result.balance,
                    token=stale.result.token,
                    address=stale.result.address,
                    decimals=stale.result.decimals,
                    raw_balance=stale.result.raw_balance,
                    timestamp=stale.result.timestamp,
                    stale=True,
                )

            raise DataSourceUnavailable(
                source="web3_balance_provider",
                reason=f"RPC error: {str(e)}",
            ) from e

    async def get_native_balance(self) -> BalanceResult:
        """Get the native token balance (ETH, MATIC, etc.).

        Convenience method for getting the chain's native token balance.

        Returns:
            BalanceResult for native token
        """
        return await self.get_balance(self._native_symbol)

    def invalidate_cache(self, token: str | None = None) -> None:
        """Invalidate cached balances.

        Should be called after transaction execution to ensure fresh data.

        Args:
            token: Specific token to invalidate, or None to clear all
        """
        if token is not None:
            token_key = token.upper()
            if token_key in self._cache:
                del self._cache[token_key]
                logger.debug("Invalidated cache for %s", token_key)
        else:
            self._cache.clear()
            logger.debug("Invalidated all balance cache")

    def add_token(
        self,
        symbol: str,
        address: str,
        decimals: int,
        is_native: bool = False,
    ) -> None:
        """Add a token to the resolver for this provider instance.

        Use this to add custom tokens that aren't in the default registry.
        Registers the token with the TokenResolver so it can be resolved.

        Args:
            symbol: Token symbol (e.g., "CUSTOM")
            address: Token contract address
            decimals: Token decimal places
            is_native: Whether this is the native token

        Example:
            provider.add_token(
                symbol="CUSTOM",
                address="0x1234...",
                decimals=18,
            )
            result = await provider.get_balance("CUSTOM")
        """
        from almanak.framework.data.tokens.models import CHAIN_ID_MAP, ResolvedToken

        checksum_address = AsyncWeb3.to_checksum_address(address)

        from almanak.core.enums import Chain

        # Normalize canonical chain aliases to Chain enum values
        _CHAIN_ALIASES: dict[str, str] = {
            "bnb": "BSC",
            "bsc": "BSC",
            "eth": "ETHEREUM",
            "avax": "AVALANCHE",
            "matic": "POLYGON",
            "op": "OPTIMISM",
            "arb": "ARBITRUM",
        }
        chain_str = _CHAIN_ALIASES.get(self._chain.lower(), self._chain.upper())

        try:
            matched_chain = Chain(chain_str)
        except ValueError:
            raise ValueError(
                f"Unknown chain '{self._chain}': cannot resolve to a valid Chain enum. "
                f"Known chains: {[c.value for c in Chain]}"
            ) from None

        chain_id = CHAIN_ID_MAP.get(matched_chain, 0)
        if chain_id == 0:
            raise ValueError(f"Chain '{matched_chain.value}' has no chain_id mapping in CHAIN_ID_MAP")

        resolved = ResolvedToken(
            symbol=symbol.upper(),
            address=checksum_address,
            decimals=decimals,
            chain=matched_chain,
            chain_id=chain_id,
            is_native=is_native,
            source="manual",
        )
        self._token_resolver.register(resolved)
        logger.info(
            "Added token to resolver: %s at %s (decimals=%d)",
            symbol,
            checksum_address,
            decimals,
        )

    def get_health_metrics(self) -> dict[str, Any]:
        """Get current health metrics for observability."""
        return self._metrics.to_dict()

    # =========================================================================
    # Private Methods
    # =========================================================================

    def _resolve_token(self, token: str) -> TokenMetadata | None:
        """Resolve a token symbol or address to TokenMetadata.

        Uses the unified TokenResolver as the single source of truth for
        token resolution across the codebase.

        Args:
            token: Token symbol (e.g., "WETH") or address (e.g., "0x...")

        Returns:
            TokenMetadata or None if not found
        """
        try:
            resolved = self._token_resolver.resolve(token, self._chain)
            return TokenMetadata(
                symbol=resolved.symbol,
                address=resolved.address,
                decimals=resolved.decimals,
                is_native=resolved.is_native,
            )
        except Exception:
            logger.debug(
                "TokenResolver failed for %s on %s",
                token,
                self._chain,
            )
            return None

    def _get_cached(self, token: str) -> BalanceCacheEntry | None:
        """Get cached entry if exists and not expired."""
        entry = self._cache.get(token)
        if entry is None:
            return None

        # Check if expired
        age_seconds = (datetime.now(UTC) - entry.cached_at).total_seconds()
        if age_seconds > self._cache_ttl:
            return None

        return entry

    def _get_stale_cached(self, token: str) -> BalanceCacheEntry | None:
        """Get cached entry even if expired (for fallback)."""
        return self._cache.get(token)

    def _update_cache(self, token: str, result: BalanceResult, latency_ms: float) -> None:
        """Update cache with fresh result."""
        self._cache[token] = BalanceCacheEntry(
            result=result,
            cached_at=datetime.now(UTC),
            fetch_latency_ms=latency_ms,
        )

    async def _get_native_balance_with_retry(self) -> int:
        """Get native token balance with retry logic.

        Returns:
            Raw balance in wei

        Raises:
            RPCError: If all retries fail
        """
        last_error: Exception | None = None

        for attempt in range(self._max_retries):
            try:
                balance = await asyncio.wait_for(
                    self._w3.eth.get_balance(self._wallet_address),
                    timeout=self._request_timeout,
                )
                return balance

            except TimeoutError as e:
                self._metrics.timeouts += 1
                last_error = e
                logger.warning(
                    "Timeout getting native balance (attempt %d/%d)",
                    attempt + 1,
                    self._max_retries,
                )

            except Web3Exception as e:
                last_error = e
                logger.warning(
                    "RPC error getting native balance: %s (attempt %d/%d)",
                    str(e),
                    attempt + 1,
                    self._max_retries,
                )

            except Exception as e:
                last_error = e
                logger.warning(
                    "Error getting native balance: %s (attempt %d/%d)",
                    str(e),
                    attempt + 1,
                    self._max_retries,
                )

            # Wait before retry (exponential backoff)
            if attempt < self._max_retries - 1:
                wait_time = self._retry_delay * (2**attempt)
                await asyncio.sleep(wait_time)

        raise RPCError(
            f"Failed to get native balance after {self._max_retries} attempts",
            rpc_url=self._mask_rpc_url(self._rpc_url),
            method="eth_getBalance",
            original_error=last_error,
        )

    async def _get_erc20_balance_with_retry(self, token_address: str) -> int:
        """Get ERC-20 token balance with retry logic.

        Args:
            token_address: Token contract address

        Returns:
            Raw balance in smallest units

        Raises:
            RPCError: If all retries fail
        """
        last_error: Exception | None = None

        # Create contract instance
        checksum_address = AsyncWeb3.to_checksum_address(token_address)
        contract = self._w3.eth.contract(address=checksum_address, abi=ERC20_ABI)

        for attempt in range(self._max_retries):
            try:
                balance = await asyncio.wait_for(
                    contract.functions.balanceOf(self._wallet_address).call(),
                    timeout=self._request_timeout,
                )
                return balance

            except TimeoutError as e:
                self._metrics.timeouts += 1
                last_error = e
                logger.warning(
                    "Timeout getting ERC20 balance for %s (attempt %d/%d)",
                    token_address,
                    attempt + 1,
                    self._max_retries,
                )

            except ContractLogicError as e:
                # Contract logic errors usually won't resolve with retry
                raise RPCError(
                    f"Contract error querying balanceOf: {str(e)}",
                    rpc_url=self._mask_rpc_url(self._rpc_url),
                    method="balanceOf",
                    original_error=e,
                ) from e

            except Web3Exception as e:
                last_error = e
                logger.warning(
                    "RPC error getting ERC20 balance for %s: %s (attempt %d/%d)",
                    token_address,
                    str(e),
                    attempt + 1,
                    self._max_retries,
                )

            except Exception as e:
                last_error = e
                logger.warning(
                    "Error getting ERC20 balance for %s: %s (attempt %d/%d)",
                    token_address,
                    str(e),
                    attempt + 1,
                    self._max_retries,
                )

            # Wait before retry (exponential backoff)
            if attempt < self._max_retries - 1:
                wait_time = self._retry_delay * (2**attempt)
                await asyncio.sleep(wait_time)

        raise RPCError(
            f"Failed to get ERC20 balance for {token_address} after {self._max_retries} attempts",
            rpc_url=self._mask_rpc_url(self._rpc_url),
            method="balanceOf",
            original_error=last_error,
        )

    async def close(self) -> None:
        """Close the Web3 provider connection."""
        # AsyncWeb3 doesn't require explicit closing in most cases
        # but we clear the cache on close
        self._cache.clear()
        logger.info("Closed Web3BalanceProvider")

    async def __aenter__(self) -> "Web3BalanceProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "Web3BalanceProvider",
    "TokenMetadata",
    "NATIVE_TOKEN_SYMBOLS",
    "NATIVE_TOKEN_ADDRESS",
    "RPCError",
    "TokenNotFoundError",
]
