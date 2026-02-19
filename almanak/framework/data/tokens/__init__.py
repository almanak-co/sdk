"""Token module for cross-chain token metadata and resolution.

This module provides the unified token resolution system for the Almanak framework.
All token lookups should go through the TokenResolver via `get_token_resolver()`.

Token Resolution System
-----------------------
The TokenResolver provides a multi-layer resolution strategy:
    1. Memory cache (fastest, <1ms)
    2. Disk cache (fast, <10ms)
    3. Static registry (fast, <5ms)
    4. Gateway on-chain lookup (slower, <500ms, requires gateway connection)

Key Components:
    - get_token_resolver: Primary entry point, returns singleton TokenResolver
    - TokenResolver: Main resolver class with resolve(), get_decimals(), get_address()
    - ResolvedToken: Immutable resolved token with full metadata
    - BridgeType: Enum for token bridge status (NATIVE, BRIDGED, CANONICAL)

Token Resolution Exceptions:
    - TokenResolutionError: Base exception for all token resolution errors
    - TokenNotFoundError: Token not found in any registry
    - InvalidTokenAddressError: Malformed token address

Legacy Components (Deprecated):
    - get_default_registry: Use get_token_resolver() instead
    - TokenRegistry: Use TokenResolver instead

Example - Modern API (Recommended):
    from almanak.framework.data.tokens import get_token_resolver

    # Get the singleton resolver
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

    # Resolve for swap (auto-wraps native tokens like ETH -> WETH)
    token = resolver.resolve_for_swap("ETH", "arbitrum")  # Returns WETH

Example - Legacy API (Deprecated):
    # This still works but triggers a deprecation warning
    from almanak.framework.data.tokens import get_default_registry

    registry = get_default_registry()  # DeprecationWarning emitted
    usdc = registry.get("USDC")
"""

import warnings

from .defaults import (
    AAVE,
    ARB,
    CRV,
    DAI,
    DEFAULT_TOKENS,
    ETH,
    GMX,
    LINK,
    MATIC,
    NATIVE_SENTINEL,
    OP,
    PENDLE,
    STABLECOINS,
    SYMBOL_ALIASES,
    UNI,
    USDC,
    USDT,
    WBTC,
    WETH,
    WRAPPED_NATIVE,
    get_coingecko_id,
    get_coingecko_ids,
)
from .defaults import (
    get_default_registry as _get_default_registry_impl,
)
from .exceptions import (
    AmbiguousTokenError,
    InvalidTokenAddressError,
    TokenNotFoundError,
    TokenResolutionError,
    TokenResolutionTimeoutError,
)
from .models import BridgeType, ChainToken, ChainTokenConfig, ResolvedToken, Token
from .registry import TokenRegistry
from .resolver import TokenResolver, get_token_resolver
from .utils import denormalize, denormalize_token, normalize, normalize_token


def get_default_registry() -> TokenRegistry:
    """Create a TokenRegistry pre-populated with default tokens.

    .. deprecated::
        Use `get_token_resolver()` instead. The TokenResolver provides
        better performance through caching and a unified API.
        See blueprints/17-token-resolution.md for migration guide.

    Returns:
        TokenRegistry with pre-registered tokens.

    Example:
        # Old way (deprecated)
        registry = get_default_registry()
        usdc = registry.get("USDC")

        # New way (recommended)
        resolver = get_token_resolver()
        usdc = resolver.resolve("USDC", "arbitrum")
    """
    warnings.warn(
        "get_default_registry() is deprecated. Use get_token_resolver() instead. "
        "See blueprints/17-token-resolution.md for migration guide.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _get_default_registry_impl()


__all__ = [
    # =========================================================================
    # Primary API (Recommended)
    # =========================================================================
    # Token Resolver - main entry point
    "get_token_resolver",
    "TokenResolver",
    # Resolved token model
    "ResolvedToken",
    "BridgeType",
    "ChainTokenConfig",
    # Token resolution exceptions
    "TokenResolutionError",
    "TokenNotFoundError",
    "InvalidTokenAddressError",
    "TokenResolutionTimeoutError",
    "AmbiguousTokenError",
    # =========================================================================
    # Constants
    # =========================================================================
    "NATIVE_SENTINEL",
    "WRAPPED_NATIVE",
    "STABLECOINS",
    "SYMBOL_ALIASES",
    "DEFAULT_TOKENS",
    # =========================================================================
    # Legacy API (Deprecated - will be removed in future release)
    # =========================================================================
    # Core classes (still useful for some operations)
    "Token",
    "ChainToken",
    "TokenRegistry",
    # Registry functions (deprecated - use get_token_resolver() instead)
    "get_default_registry",
    "get_coingecko_id",
    "get_coingecko_ids",
    # Utility functions
    "normalize",
    "denormalize",
    "normalize_token",
    "denormalize_token",
    # =========================================================================
    # Default Token Instances
    # =========================================================================
    "ETH",
    "WETH",
    "USDC",
    "USDT",
    "DAI",
    "WBTC",
    "ARB",
    "OP",
    "MATIC",
    "LINK",
    "UNI",
    "AAVE",
    "CRV",
    "GMX",
    "PENDLE",
]
