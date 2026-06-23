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

Example:
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
"""

from .caip import ParsedAsset, parse_caip19, token_ref_to_caip19
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
from .exceptions import (
    AmbiguousTokenError,
    InvalidTokenAddressError,
    TokenNotFoundError,
    TokenResolutionError,
    TokenResolutionTimeoutError,
)
from .identity import canonicalize_token_identity
from .models import (
    BridgeType,
    ChainToken,
    ChainTokenConfig,
    ResolvedToken,
    Token,
    TokenRef,
    normalize_token_address_for_chain,
)
from .resolver import TokenResolver, create_token_resolver, get_token_resolver
from .utils import denormalize, normalize

__all__ = [
    # =========================================================================
    # Primary API
    # =========================================================================
    # Token Resolver - main entry point
    "get_token_resolver",
    "create_token_resolver",
    "TokenResolver",
    # Read-side canonical identity helper (W1-4 / VIB-TA-8)
    "canonicalize_token_identity",
    # Resolved token model
    "ResolvedToken",
    "TokenRef",
    "BridgeType",
    "ChainTokenConfig",
    # CAIP-19 asset-id codec (VIB-5175)
    "ParsedAsset",
    "parse_caip19",
    "token_ref_to_caip19",
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
    # Token model and helpers
    # =========================================================================
    "Token",
    "ChainToken",
    "normalize_token_address_for_chain",
    "get_coingecko_id",
    "get_coingecko_ids",
    # Utility functions
    "normalize",
    "denormalize",
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
