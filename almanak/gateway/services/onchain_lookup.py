"""On-chain ERC20 token metadata lookup for the Gateway.

This module provides on-chain token metadata fetching capabilities for the gateway,
allowing discovery of unknown tokens by querying their smart contracts directly.

Key Features:
    - Query ERC20 contracts for decimals(), symbol(), name()
    - Handle non-standard ERC20s (bytes32 symbol/name returns)
    - Handle native tokens with sentinel address
    - Timeout handling with configurable limits
    - Retry with exponential backoff
    - Return None for invalid/non-ERC20 addresses

IMPORTANT: This module is GATEWAY-SIDE ONLY. Framework code must NOT use Web3
directly - it should call the gateway's TokenService instead.

Example:
    from almanak.gateway.services.onchain_lookup import OnChainLookup
    from almanak.gateway.utils import get_rpc_url

    # Initialize with RPC URL
    rpc_url = get_rpc_url("arbitrum", network="mainnet")
    lookup = OnChainLookup(rpc_url)

    # Lookup token metadata
    metadata = await lookup.lookup("arbitrum", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
    if metadata:
        print(f"Token: {metadata.symbol} ({metadata.decimals} decimals)")
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.exceptions import ContractLogicError, Web3Exception

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Native token placeholder address (used by many protocols)
NATIVE_SENTINEL_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

# Chain-specific native token info
NATIVE_TOKEN_INFO: dict[str, dict[str, Any]] = {
    "ethereum": {"symbol": "ETH", "name": "Ethereum", "decimals": 18},
    "arbitrum": {"symbol": "ETH", "name": "Ethereum", "decimals": 18},
    "optimism": {"symbol": "ETH", "name": "Ethereum", "decimals": 18},
    "base": {"symbol": "ETH", "name": "Ethereum", "decimals": 18},
    "polygon": {"symbol": "MATIC", "name": "Polygon", "decimals": 18},
    "avalanche": {"symbol": "AVAX", "name": "Avalanche", "decimals": 18},
    "bsc": {"symbol": "BNB", "name": "BNB", "decimals": 18},
    "sonic": {"symbol": "S", "name": "Sonic", "decimals": 18},
    "plasma": {"symbol": "XPL", "name": "Plasma", "decimals": 18},
}

# Minimal ERC20 ABI for metadata queries
ERC20_METADATA_ABI = [
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
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function",
    },
]

# Alternative ABI for tokens that return bytes32 instead of string
ERC20_BYTES32_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "bytes32"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "bytes32"}],
        "type": "function",
    },
]


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class TokenMetadata:
    """Metadata for a token fetched from on-chain.

    Attributes:
        symbol: Token symbol (e.g., "WETH", "USDC")
        name: Full token name (e.g., "Wrapped Ether", "USD Coin")
        decimals: Token decimal places
        address: Token contract address
        is_native: Whether this is the chain's native token
    """

    symbol: str
    name: str | None
    decimals: int
    address: str
    is_native: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "symbol": self.symbol,
            "name": self.name,
            "decimals": self.decimals,
            "address": self.address,
            "is_native": self.is_native,
        }


# =============================================================================
# OnChainLookup
# =============================================================================


class OnChainLookup:
    """On-chain ERC20 token metadata lookup.

    This class queries ERC20 contracts directly to fetch token metadata
    (decimals, symbol, name). It handles:
    - Standard ERC20 contracts
    - Non-standard contracts (bytes32 symbol/name)
    - Native tokens with sentinel address
    - Timeout and retry logic

    Example:
        lookup = OnChainLookup(rpc_url)
        metadata = await lookup.lookup("arbitrum", "0x...")
        if metadata:
            print(f"{metadata.symbol}: {metadata.decimals} decimals")
    """

    def __init__(
        self,
        rpc_url: str,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ) -> None:
        """Initialize OnChainLookup.

        Args:
            rpc_url: RPC endpoint URL for blockchain queries
            timeout: Request timeout in seconds (default 10)
            max_retries: Maximum retry attempts (default 3)
            backoff_factor: Exponential backoff multiplier (default 2.0)
        """
        self._rpc_url = rpc_url
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor

        # Initialize Web3 with async HTTP provider
        self._w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))

        logger.info(
            "Initialized OnChainLookup",
            extra={
                "rpc_url": self._mask_rpc_url(rpc_url),
                "timeout": timeout,
                "max_retries": max_retries,
            },
        )

    @staticmethod
    def _mask_rpc_url(url: str) -> str:
        """Mask sensitive parts of RPC URL for logging."""
        if "@" in url:
            parts = url.split("@")
            return parts[0].split("//")[0] + "//***@" + parts[1]
        # Mask API keys in query params or path
        if "?" in url:
            return url.split("?")[0] + "?***"
        return url

    async def lookup(self, chain: str, address: str) -> TokenMetadata | None:
        """Lookup token metadata from on-chain.

        Queries the ERC20 contract at the given address for metadata.
        Returns None for invalid or non-ERC20 addresses instead of raising.

        Args:
            chain: Chain name (e.g., "arbitrum", "ethereum")
            address: Token contract address

        Returns:
            TokenMetadata if successful, None if lookup fails

        Note:
            This method handles native tokens (ETH, MATIC, etc.) with the
            sentinel address 0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE
        """
        # Handle native token sentinel address
        if address.lower() == NATIVE_SENTINEL_ADDRESS.lower():
            return self._get_native_token_metadata(chain, address)

        # Validate address format
        if not address.startswith("0x") or len(address) != 42:
            logger.warning("Invalid address format: %s", address)
            return None

        try:
            checksum_address = AsyncWeb3.to_checksum_address(address)
        except ValueError:
            logger.warning("Invalid checksum for address: %s", address)
            return None

        # Fetch metadata with retry
        decimals = await self._fetch_decimals_with_retry(checksum_address)
        if decimals is None:
            logger.debug("Could not fetch decimals for %s on %s", address, chain)
            return None

        symbol = await self._fetch_symbol_with_retry(checksum_address)
        name = await self._fetch_name_with_retry(checksum_address)

        # Symbol is required, name is optional
        if symbol is None:
            logger.debug("Could not fetch symbol for %s on %s", address, chain)
            return None

        return TokenMetadata(
            symbol=symbol,
            name=name,
            decimals=decimals,
            address=checksum_address,
            is_native=False,
        )

    def _get_native_token_metadata(self, chain: str, address: str) -> TokenMetadata | None:
        """Get metadata for native token on a chain.

        Args:
            chain: Chain name
            address: The sentinel address

        Returns:
            TokenMetadata for the native token, or None if chain unknown
        """
        chain_lower = chain.lower()
        if chain_lower not in NATIVE_TOKEN_INFO:
            logger.warning("Unknown chain for native token lookup: %s", chain)
            return None

        info = NATIVE_TOKEN_INFO[chain_lower]
        return TokenMetadata(
            symbol=info["symbol"],
            name=info["name"],
            decimals=info["decimals"],
            address=address,
            is_native=True,
        )

    async def _fetch_decimals_with_retry(self, address: str) -> int | None:
        """Fetch token decimals with retry logic.

        Args:
            address: Checksum token address

        Returns:
            Decimals value or None if all retries fail
        """
        contract = self._w3.eth.contract(address=self._w3.to_checksum_address(address), abi=ERC20_METADATA_ABI)
        last_error: Exception | None = None

        for attempt in range(self._max_retries):
            try:
                decimals = await asyncio.wait_for(
                    contract.functions.decimals().call(),
                    timeout=self._timeout,
                )
                return int(decimals)

            except TimeoutError as e:
                last_error = e
                logger.debug(
                    "Timeout fetching decimals for %s (attempt %d/%d)",
                    address,
                    attempt + 1,
                    self._max_retries,
                )

            except ContractLogicError as e:
                # Contract doesn't have decimals() - likely not ERC20
                logger.debug("Contract %s has no decimals() function: %s", address, e)
                return None

            except Web3Exception as e:
                last_error = e
                logger.debug(
                    "Web3 error fetching decimals for %s: %s (attempt %d/%d)",
                    address,
                    e,
                    attempt + 1,
                    self._max_retries,
                )

            except Exception as e:
                last_error = e
                logger.debug(
                    "Error fetching decimals for %s: %s (attempt %d/%d)",
                    address,
                    e,
                    attempt + 1,
                    self._max_retries,
                )

            # Exponential backoff before retry
            if attempt < self._max_retries - 1:
                wait_time = (self._backoff_factor**attempt) * 0.5
                await asyncio.sleep(wait_time)

        logger.warning(
            "Failed to fetch decimals for %s after %d attempts: %s",
            address,
            self._max_retries,
            last_error,
        )
        return None

    async def _fetch_symbol_with_retry(self, address: str) -> str | None:
        """Fetch token symbol with retry logic.

        Handles both string and bytes32 return types.

        Args:
            address: Checksum token address

        Returns:
            Symbol string or None if all retries fail
        """
        # Try standard string ABI first
        result = await self._fetch_string_field_with_retry(address, "symbol")
        if result is not None:
            return result

        # Try bytes32 ABI as fallback
        return await self._fetch_bytes32_field_with_retry(address, "symbol")

    async def _fetch_name_with_retry(self, address: str) -> str | None:
        """Fetch token name with retry logic.

        Handles both string and bytes32 return types.

        Args:
            address: Checksum token address

        Returns:
            Name string or None if lookup fails
        """
        # Try standard string ABI first
        result = await self._fetch_string_field_with_retry(address, "name")
        if result is not None:
            return result

        # Try bytes32 ABI as fallback
        return await self._fetch_bytes32_field_with_retry(address, "name")

    async def _fetch_string_field_with_retry(self, address: str, field: str) -> str | None:
        """Fetch a string field (symbol or name) with retry logic.

        Args:
            address: Checksum token address
            field: Field name ("symbol" or "name")

        Returns:
            String value or None if all retries fail
        """
        contract = self._w3.eth.contract(address=self._w3.to_checksum_address(address), abi=ERC20_METADATA_ABI)
        last_error: Exception | None = None

        for attempt in range(self._max_retries):
            try:
                if field == "symbol":
                    value = await asyncio.wait_for(
                        contract.functions.symbol().call(),
                        timeout=self._timeout,
                    )
                else:
                    value = await asyncio.wait_for(
                        contract.functions.name().call(),
                        timeout=self._timeout,
                    )
                return str(value) if value else None

            except TimeoutError as e:
                last_error = e
                logger.debug(
                    "Timeout fetching %s for %s (attempt %d/%d)",
                    field,
                    address,
                    attempt + 1,
                    self._max_retries,
                )

            except ContractLogicError:
                # Contract doesn't have this function - return None
                return None

            except Web3Exception as e:
                # May be bytes32 return type - handled by caller
                if "decode" in str(e).lower() or "type" in str(e).lower():
                    return None
                last_error = e
                logger.debug(
                    "Web3 error fetching %s for %s: %s (attempt %d/%d)",
                    field,
                    address,
                    e,
                    attempt + 1,
                    self._max_retries,
                )

            except Exception as e:
                # Decoding error likely means bytes32 return type
                error_str = str(e).lower()
                if "decode" in error_str or "string" in error_str or "bytes" in error_str:
                    return None
                last_error = e
                logger.debug(
                    "Error fetching %s for %s: %s (attempt %d/%d)",
                    field,
                    address,
                    e,
                    attempt + 1,
                    self._max_retries,
                )

            # Exponential backoff before retry
            if attempt < self._max_retries - 1:
                wait_time = (self._backoff_factor**attempt) * 0.5
                await asyncio.sleep(wait_time)

        logger.debug("Could not fetch %s (string) for %s: %s", field, address, last_error)
        return None

    async def _fetch_bytes32_field_with_retry(self, address: str, field: str) -> str | None:
        """Fetch a bytes32 field (symbol or name) with retry logic.

        Some tokens (like MKR) return bytes32 instead of string.

        Args:
            address: Checksum token address
            field: Field name ("symbol" or "name")

        Returns:
            Decoded string or None if all retries fail
        """
        contract = self._w3.eth.contract(address=self._w3.to_checksum_address(address), abi=ERC20_BYTES32_ABI)
        last_error: Exception | None = None

        for attempt in range(self._max_retries):
            try:
                if field == "symbol":
                    value = await asyncio.wait_for(
                        contract.functions.symbol().call(),
                        timeout=self._timeout,
                    )
                else:
                    value = await asyncio.wait_for(
                        contract.functions.name().call(),
                        timeout=self._timeout,
                    )
                # Decode bytes32 to string (strip null bytes)
                if isinstance(value, bytes):
                    return value.rstrip(b"\x00").decode("utf-8", errors="replace")
                return str(value) if value else None

            except TimeoutError as e:
                last_error = e
                logger.debug(
                    "Timeout fetching %s (bytes32) for %s (attempt %d/%d)",
                    field,
                    address,
                    attempt + 1,
                    self._max_retries,
                )

            except ContractLogicError:
                # Contract doesn't have this function
                return None

            except Web3Exception as e:
                last_error = e
                logger.debug(
                    "Web3 error fetching %s (bytes32) for %s: %s (attempt %d/%d)",
                    field,
                    address,
                    e,
                    attempt + 1,
                    self._max_retries,
                )

            except Exception as e:
                last_error = e
                logger.debug(
                    "Error fetching %s (bytes32) for %s: %s (attempt %d/%d)",
                    field,
                    address,
                    e,
                    attempt + 1,
                    self._max_retries,
                )

            # Exponential backoff before retry
            if attempt < self._max_retries - 1:
                wait_time = (self._backoff_factor**attempt) * 0.5
                await asyncio.sleep(wait_time)

        logger.debug("Could not fetch %s (bytes32) for %s: %s", field, address, last_error)
        return None

    async def close(self) -> None:
        """Close the lookup instance and release resources."""
        # AsyncWeb3 doesn't require explicit closing in most cases
        logger.info("Closed OnChainLookup")

    async def __aenter__(self) -> "OnChainLookup":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "OnChainLookup",
    "TokenMetadata",
    "NATIVE_SENTINEL_ADDRESS",
    "NATIVE_TOKEN_INFO",
]
