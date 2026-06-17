"""Token registry for address-to-symbol resolution.

Provides a structured registry mapping token addresses to their metadata
(symbol, decimals) for use in receipt processing and portfolio tracking.

This module enables Paper Trader to display human-readable token symbols
instead of raw addresses in portfolio reports and trade records.

Internally delegates to TokenResolver for unified token resolution, with
TOKEN_REGISTRY exposed as a resolver-backed compatibility view.

Example:
    >>> from almanak.framework.backtesting.paper.token_registry import (
    ...     TOKEN_REGISTRY, get_token_info, CHAIN_ID_ETHEREUM
    ... )
    >>> info = get_token_info(CHAIN_ID_ETHEREUM, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
    >>> info.symbol
    'USDC'
    >>> info.decimals
    6

    # Async lookup with on-chain fallback
    >>> symbol = await get_token_symbol_with_fallback(
    ...     CHAIN_ID_ETHEREUM,
    ...     "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    ...     "https://eth.llamarpc.com"
    ... )
    >>> symbol
    'USDC'
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import chain_name_for_id

if TYPE_CHECKING:
    from web3 import AsyncWeb3

logger = logging.getLogger(__name__)


def _get_resolver():
    """Lazy import and return the TokenResolver singleton.

    Uses lazy import to avoid circular dependencies and import-time overhead.
    Returns None if TokenResolver is not available (should not happen in practice).
    """
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        return get_token_resolver()
    except Exception:
        logger.debug("TokenResolver not available")
        return None


# =============================================================================
# Chain IDs (EIP-155)
# =============================================================================
CHAIN_ID_ETHEREUM = 1
CHAIN_ID_ARBITRUM = 42161
CHAIN_ID_BASE = 8453
CHAIN_ID_OPTIMISM = 10
CHAIN_ID_POLYGON = 137
CHAIN_ID_AVALANCHE = 43114
CHAIN_ID_BSC = 56

# Native token sentinel addresses
NATIVE_ETH_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
NATIVE_AVAX_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
NATIVE_MATIC_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
NATIVE_BNB_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


@dataclass(frozen=True)
class TokenInfo:
    """Immutable token metadata.

    Attributes:
        symbol: Human-readable token symbol (e.g., 'USDC', 'WETH')
        decimals: Number of decimal places for token amounts
        address: Token contract address (lowercase, checksummed format not required)

    Example:
        >>> usdc = TokenInfo(symbol="USDC", decimals=6, address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        >>> usdc.symbol
        'USDC'
    """

    symbol: str
    decimals: int
    address: str


# =============================================================================
# Token Registry compatibility view
# =============================================================================


class _ResolverTokenRegistry(Mapping[int, Mapping[str, TokenInfo]]):
    """Read-only ``TOKEN_REGISTRY`` view backed by ``TokenResolver``.

    Older paper-trading callers import ``TOKEN_REGISTRY`` directly and expect a
    ``chain_id -> lowercase_address -> TokenInfo`` mapping. Keep that shape,
    but derive it from the canonical JSON-backed resolver instead of carrying a
    second address table in framework code.
    """

    def __init__(self) -> None:
        self._snapshot: dict[int, Mapping[str, TokenInfo]] | None = None

    def refresh(self) -> None:
        """Clear the cached snapshot so the next read reflects resolver state."""
        self._snapshot = None

    def _data(self) -> dict[int, Mapping[str, TokenInfo]]:
        if self._snapshot is None:
            self._snapshot = self._build_snapshot()
        return self._snapshot

    def _build_snapshot(self) -> dict[int, Mapping[str, TokenInfo]]:
        resolver = _get_resolver()
        if resolver is None:
            return {}

        snapshot: dict[int, Mapping[str, TokenInfo]] = {}
        for chain_name, resolved_by_address in resolver.known_static_tokens_by_chain().items():
            descriptor = ChainRegistry.try_resolve(chain_name)
            if descriptor is None or descriptor.chain_id == 0:
                continue

            chain_tokens = {
                resolved.address.lower(): TokenInfo(
                    symbol=resolved.symbol,
                    decimals=resolved.decimals,
                    address=resolved.address.lower(),
                )
                for resolved in resolved_by_address.values()
            }
            if chain_tokens:
                snapshot[descriptor.chain_id] = MappingProxyType(chain_tokens)

        return snapshot

    def __getitem__(self, chain_id: int) -> Mapping[str, TokenInfo]:
        return self._data()[chain_id]

    def __iter__(self) -> Iterator[int]:
        return iter(self._data())

    def __len__(self) -> int:
        return len(self._data())


TOKEN_REGISTRY: Mapping[int, Mapping[str, TokenInfo]] = _ResolverTokenRegistry()


def get_token_info(chain_id: int, address: str) -> TokenInfo | None:
    """Look up token info from the registry.

    Delegates to TokenResolver for unified resolution, then falls back to
    the resolver-backed ``TOKEN_REGISTRY`` compatibility view.

    Args:
        chain_id: EIP-155 chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum)
        address: Token contract address (case-insensitive)

    Returns:
        TokenInfo if found in registry, None otherwise

    Example:
        >>> info = get_token_info(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        >>> info.symbol if info else None
        'USDC'
    """
    normalized_address = address.lower()

    # Try TokenResolver first (chain_id -> name derived from the registry, VIB-4851 A2)
    chain_name = chain_name_for_id(chain_id)
    if chain_name:
        resolver = _get_resolver()
        if resolver:
            try:
                resolved = resolver.resolve(normalized_address, chain_name, skip_gateway=True)
                return TokenInfo(
                    symbol=resolved.symbol,
                    decimals=resolved.decimals,
                    address=resolved.address.lower(),
                )
            except Exception:
                pass  # Fall through to compatibility view

    # Fallback to resolver-backed TOKEN_REGISTRY compatibility view.
    chain_registry = TOKEN_REGISTRY.get(chain_id)
    if chain_registry is None:
        return None
    return chain_registry.get(normalized_address)


def get_token_symbol(chain_id: int, address: str) -> str | None:
    """Get token symbol from registry.

    Convenience function that returns just the symbol.

    Args:
        chain_id: EIP-155 chain ID
        address: Token contract address (case-insensitive)

    Returns:
        Token symbol if found, None otherwise

    Example:
        >>> get_token_symbol(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        'USDC'
    """
    info = get_token_info(chain_id, address)
    return info.symbol if info else None


def get_token_decimals(chain_id: int, address: str) -> int | None:
    """Get token decimals from registry.

    Convenience function that returns just the decimals.

    Args:
        chain_id: EIP-155 chain ID
        address: Token contract address (case-insensitive)

    Returns:
        Token decimals if found, None otherwise

    Example:
        >>> get_token_decimals(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        6
    """
    info = get_token_info(chain_id, address)
    return info.decimals if info else None


def is_token_known(chain_id: int, address: str) -> bool:
    """Check if a token is registered in the canonical registry.

    Useful for determining whether a token address can be resolved to
    a canonical symbol or if it requires on-chain lookup.

    Args:
        chain_id: EIP-155 chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum)
        address: Token contract address (case-insensitive)

    Returns:
        True if the token is in the registry, False otherwise

    Example:
        >>> is_token_known(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        True
        >>> is_token_known(1, "0x1234567890123456789012345678901234567890")
        False
    """
    return get_token_info(chain_id, address) is not None


def resolve_to_canonical_symbol(chain_id: int, address: str) -> str:
    """Resolve a token address to its canonical symbol deterministically.

    This function provides a deterministic mapping from token address to symbol,
    ensuring consistent token identification across the backtesting system.

    Unlike get_token_symbol_with_fallback, this function:
    - Does NOT make network calls
    - Returns a deterministic result (either known symbol or checksummed address)
    - Is suitable for use in hot paths where consistency is critical

    The resolution priority is:
    1. If token is in TOKEN_REGISTRY, return the registered symbol
    2. If not found, return the checksummed address as the symbol

    Args:
        chain_id: EIP-155 chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum)
        address: Token contract address (case-insensitive)

    Returns:
        Canonical symbol (e.g., "USDC") if known, or checksummed address if unknown

    Example:
        >>> resolve_to_canonical_symbol(1, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        'USDC'
        >>> resolve_to_canonical_symbol(1, "0x1234567890123456789012345678901234567890")
        '0x1234567890123456789012345678901234567890'
    """
    symbol = get_token_symbol(chain_id, address)
    if symbol is not None:
        return symbol
    # Return checksummed address as fallback symbol
    return _checksum_address(address)


def get_supported_chain_ids() -> list[int]:
    """Get all chain IDs supported by the token registry.

    Returns:
        List of supported EIP-155 chain IDs

    Example:
        >>> chain_ids = get_supported_chain_ids()
        >>> 1 in chain_ids  # Ethereum
        True
        >>> 42161 in chain_ids  # Arbitrum
        True
    """
    return list(TOKEN_REGISTRY.keys())


def get_all_tokens_for_chain(chain_id: int) -> list[TokenInfo]:
    """Get all registered tokens for a specific chain.

    Args:
        chain_id: EIP-155 chain ID

    Returns:
        List of TokenInfo objects for the chain, empty list if chain not supported

    Example:
        >>> tokens = get_all_tokens_for_chain(1)
        >>> len(tokens) > 0
        True
    """
    chain_registry = TOKEN_REGISTRY.get(chain_id)
    if chain_registry is None:
        return []
    return list(chain_registry.values())


def get_token_count() -> int:
    """Get total number of tokens across all chains in the registry.

    Returns:
        Total count of registered tokens

    Example:
        >>> get_token_count() > 50
        True
    """
    return sum(len(chain_tokens) for chain_tokens in TOKEN_REGISTRY.values())


# =============================================================================
# ERC-20 symbol() function selector
# =============================================================================
# Keccak256("symbol()")[:4] = 0x95d89b41
SYMBOL_SELECTOR = "0x95d89b41"


def _checksum_address(address: str) -> str:
    """Convert address to checksummed format (EIP-55).

    Uses Keccak-256 hash as per EIP-55 specification.

    Args:
        address: Ethereum address (any case)

    Returns:
        Checksummed address string

    Example:
        >>> _checksum_address("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
    """
    # Import eth_utils for correct keccak256-based checksum
    # This is the standard library used by web3.py
    try:
        from eth_utils import to_checksum_address

        return to_checksum_address(address)
    except ImportError:
        # Fallback: use web3 if eth_utils not directly available
        from web3 import Web3

        return Web3.to_checksum_address(address)


async def _query_symbol_onchain(web3: AsyncWeb3, address: str) -> str | None:
    """Query ERC-20 symbol() from the blockchain.

    Makes an eth_call to the token contract's symbol() function.

    Args:
        web3: AsyncWeb3 instance connected to the chain
        address: Token contract address (checksummed)

    Returns:
        Token symbol if call succeeds and returns valid data, None otherwise
    """
    try:
        result = await web3.eth.call({"to": address, "data": SYMBOL_SELECTOR})  # type: ignore[typeddict-item]

        if len(result) < 64:
            # Symbol is a string, so it should be ABI-encoded
            # Minimum: 32 bytes offset + 32 bytes length = 64 bytes
            return None

        # ABI-decode the string
        # Layout: offset (32 bytes) + length (32 bytes) + data (variable)
        offset = int.from_bytes(result[0:32], byteorder="big")
        length = int.from_bytes(result[offset : offset + 32], byteorder="big")
        symbol_bytes = result[offset + 32 : offset + 32 + length]

        # Decode as UTF-8, strip null bytes
        symbol = symbol_bytes.decode("utf-8").rstrip("\x00")

        if not symbol:
            return None

        return symbol

    except Exception as e:
        logger.debug(f"Failed to query symbol for {address}: {e}")
        return None


async def get_token_symbol_with_fallback(
    chain_id: int,
    address: str,
    rpc_url: str | None = None,
) -> str:
    """Get token symbol with registry lookup and on-chain fallback.

    Attempts to resolve token address to symbol using the following priority:
    1. TokenResolver lookup (unified resolution via cache/registry/gateway)
    2. TOKEN_REGISTRY compatibility view
    3. On-chain ERC-20 symbol() call (requires RPC, skipped if rpc_url is None)
    4. Checksummed address as fallback (always succeeds)

    Args:
        chain_id: EIP-155 chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum)
        address: Token contract address (case-insensitive)
        rpc_url: RPC endpoint URL for on-chain fallback queries (optional)

    Returns:
        Token symbol if found, or checksummed address if all lookups fail

    Example:
        >>> symbol = await get_token_symbol_with_fallback(
        ...     1,
        ...     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        ...     "https://eth.llamarpc.com"
        ... )
        >>> symbol
        'USDC'
    """
    # 1. Try registry lookup first (delegates to TokenResolver internally)
    symbol = get_token_symbol(chain_id, address)
    if symbol is not None:
        return symbol

    # 2. Try on-chain symbol() query (only if RPC URL provided)
    if rpc_url is not None:
        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3

            from almanak.gateway.utils.ssl_context import build_ssl_context

            web3 = AsyncWeb3(AsyncHTTPProvider(rpc_url, request_kwargs={"ssl": build_ssl_context()}))
            checksum_address = web3.to_checksum_address(address)

            symbol = await _query_symbol_onchain(web3, checksum_address)
            if symbol is not None:
                return symbol

        except Exception as e:
            logger.debug(f"On-chain symbol lookup failed for {address}: {e}")

    # 3. Fall back to checksummed address
    fallback_address = _checksum_address(address)
    logger.warning(
        f"Token symbol not found for {address} on chain {chain_id}, using address as fallback: {fallback_address}"
    )
    return fallback_address
