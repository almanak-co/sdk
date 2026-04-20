"""Token registry for managing token metadata across chains.

This module provides a centralized registry for token information,
enabling consistent token lookups across the data module.

.. deprecated::
    TokenRegistry is deprecated. Use TokenResolver via `get_token_resolver()` instead.
    See almanak.framework.data.tokens.resolver module for details.

Key Components:
    - TokenRegistry: Central registry for token metadata (deprecated)
"""

import warnings

from .models import ChainToken, Token


class TokenRegistry:
    """Central registry for token metadata.

    .. deprecated::
        TokenRegistry is deprecated. Use `get_token_resolver()` from
        ``almanak.framework.data.tokens`` instead. The TokenResolver provides
        better performance through caching and a unified API.
        See almanak.framework.data.tokens.resolver module for details.

    Provides a unified interface for looking up token information
    by symbol and chain, supporting cross-chain token resolution.

    The registry maintains:
    - A mapping of symbols to Token objects
    - Quick lookups by symbol and optional chain
    - List operations for registered tokens

    Example (deprecated)::

        # Old way (deprecated)
        registry = TokenRegistry()
        token = registry.get("USDC")

        # New way (recommended)
        from almanak.framework.data.tokens import get_token_resolver
        resolver = get_token_resolver()
        usdc = resolver.resolve("USDC", "arbitrum")
    """

    _warned = False

    def __init__(self) -> None:
        """Initialize an empty token registry.

        .. deprecated::
            Use `get_token_resolver()` instead. The TokenResolver provides
            better performance through caching and a unified API.
            See almanak.framework.data.tokens.resolver module for details.
        """
        if not TokenRegistry._warned:
            warnings.warn(
                "TokenRegistry is deprecated. Use get_token_resolver() from "
                "almanak.framework.data.tokens instead. "
                "See almanak.framework.data.tokens.resolver module for details.",
                DeprecationWarning,
                stacklevel=2,
            )
            TokenRegistry._warned = True
        self._tokens: dict[str, Token] = {}

    def get(self, symbol: str, chain: str | None = None) -> Token | ChainToken | None:
        """Get a token by symbol, optionally for a specific chain.

        Args:
            symbol: Token symbol (e.g., "ETH", "USDC")
            chain: Optional chain name. If provided, returns ChainToken
                   with chain-specific details.

        Returns:
            - If chain is None: Token object or None if not found
            - If chain is provided: ChainToken object or None if not found/not on chain

        Example:
            token = registry.get("USDC")  # Returns Token
            chain_token = registry.get("USDC", chain="arbitrum")  # Returns ChainToken
        """
        symbol_upper = symbol.upper()
        token = self._tokens.get(symbol_upper)

        if token is None:
            return None

        if chain is None:
            return token

        # Chain specified - return ChainToken if available
        address = token.get_address(chain)
        if address is None:
            return None

        return ChainToken(
            token=token,
            chain=chain.lower(),
            address=address,
            decimals=token.decimals,
            bridge_canonical=True,
        )

    def register(self, token: Token) -> None:
        """Register a token in the registry.

        If a token with the same symbol already exists, it will be
        replaced with the new token.

        Args:
            token: Token object to register

        Example:
            registry.register(Token(
                symbol="WETH",
                name="Wrapped Ether",
                decimals=18,
                addresses={"ethereum": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"},
                coingecko_id="weth",
            ))
        """
        self._tokens[token.symbol.upper()] = token

    def list_tokens(self) -> list[Token]:
        """Get all registered tokens.

        Returns:
            List of all Token objects in the registry,
            sorted alphabetically by symbol.

        Example:
            for token in registry.list_tokens():
                print(f"{token.symbol}: {token.name}")
        """
        return sorted(self._tokens.values(), key=lambda t: t.symbol)

    def has(self, symbol: str) -> bool:
        """Check if a token is registered.

        Args:
            symbol: Token symbol to check

        Returns:
            True if token is registered
        """
        return symbol.upper() in self._tokens

    def __len__(self) -> int:
        """Return the number of registered tokens."""
        return len(self._tokens)

    def __contains__(self, symbol: str) -> bool:
        """Check if a token is registered (for 'in' operator)."""
        return self.has(symbol)


__all__ = [
    "TokenRegistry",
]
