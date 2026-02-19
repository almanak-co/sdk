"""Token data models for the data module.

This module defines the core data structures for representing tokens
across different chains with their metadata and addresses.

Key Components:
    - Token: Core token metadata with addresses across chains
    - ChainToken: Token-on-chain representation with chain-specific details
    - ResolvedToken: Fully resolved token with all metadata (frozen for caching)
    - BridgeType: Enum for token bridge status (NATIVE, BRIDGED, CANONICAL)
    - ChainTokenConfig: Chain-specific token configuration overrides
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from almanak.core.enums import Chain


class BridgeType(Enum):
    """Token bridge status indicating origin of the token on a chain.

    Attributes:
        NATIVE: Token is native to this chain (e.g., ETH on Ethereum, USDC native on Arbitrum)
        BRIDGED: Token was bridged from another chain (e.g., USDC.e on Arbitrum)
        CANONICAL: Token is the canonical/official bridge representation for cross-chain transfers

    Example:
        # Native USDC on Arbitrum (issued by Circle directly)
        native_usdc = ResolvedToken(..., bridge_type=BridgeType.NATIVE)

        # Bridged USDC.e on Arbitrum (bridged from Ethereum)
        bridged_usdc = ResolvedToken(..., bridge_type=BridgeType.BRIDGED)
    """

    NATIVE = "NATIVE"
    BRIDGED = "BRIDGED"
    CANONICAL = "CANONICAL"


@dataclass
class ChainTokenConfig:
    """Chain-specific configuration overrides for a token.

    Used to specify different addresses, decimals, or bridge types
    for a token on specific chains.

    Attributes:
        address: Contract address on this chain
        decimals: Decimal places (may differ from default on some chains)
        is_native: Whether this is the native gas token on this chain
        bridge_type: Bridge status of the token on this chain

    Example:
        # USDC.e on Arbitrum has different address than native USDC
        usdc_e_config = ChainTokenConfig(
            address="0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            decimals=6,
            is_native=False,
            bridge_type=BridgeType.BRIDGED,
        )
    """

    address: str
    decimals: int
    is_native: bool = False
    bridge_type: BridgeType = BridgeType.NATIVE

    def __post_init__(self) -> None:
        """Validate chain token config data."""
        if not self.address:
            raise ValueError("Address cannot be empty")
        if self.decimals < 0 or self.decimals > 77:
            raise ValueError(f"Invalid decimals: {self.decimals}. Must be 0-77.")


@dataclass(frozen=True)
class ResolvedToken:
    """Fully resolved token with all metadata for a specific chain.

    This is a frozen (immutable) dataclass representing a token that has been
    fully resolved with all its metadata. It's designed for caching and
    thread-safe access.

    Attributes:
        symbol: Token symbol (e.g., "ETH", "USDC", "WBTC")
        address: Contract address on the resolved chain
        decimals: Token decimal places
        chain: Chain enum value where this token is resolved
        chain_id: Numeric chain ID for the resolved chain
        name: Human-readable token name (e.g., "Ethereum", "USD Coin")
        coingecko_id: CoinGecko API identifier for price fetching
        is_stablecoin: Whether this token is a stablecoin
        is_native: Whether this is the native gas token (ETH, MATIC, AVAX, etc.)
        is_wrapped_native: Whether this is wrapped native (WETH, WMATIC, WAVAX, etc.)
        canonical_symbol: Canonical symbol for cross-chain identification (e.g., "USDC" for both USDC and USDC.e)
        bridge_type: Bridge status of the token
        source: Where the token metadata came from ("static", "on_chain", "cache")
        is_verified: Whether the token metadata has been verified
        resolved_at: Timestamp when the token was resolved

    Example:
        resolved_usdc = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain=Chain.ARBITRUM,
            chain_id=42161,
            name="USD Coin",
            coingecko_id="usd-coin",
            is_stablecoin=True,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol="USDC",
            bridge_type=BridgeType.NATIVE,
            source="static",
            is_verified=True,
            resolved_at=datetime.now(),
        )
    """

    symbol: str
    address: str
    decimals: int
    chain: Chain
    chain_id: int
    name: str | None = None
    coingecko_id: str | None = None
    is_stablecoin: bool = False
    is_native: bool = False
    is_wrapped_native: bool = False
    canonical_symbol: str | None = None
    bridge_type: BridgeType = BridgeType.NATIVE
    source: str = "static"
    is_verified: bool = True
    resolved_at: datetime | None = None

    def __post_init__(self) -> None:
        """Validate resolved token data."""
        # Use object.__setattr__ since this is a frozen dataclass
        if not self.symbol:
            raise ValueError("Token symbol cannot be empty")
        if not self.address:
            raise ValueError("Token address cannot be empty")
        if self.decimals < 0 or self.decimals > 77:
            raise ValueError(f"Invalid decimals: {self.decimals}. Must be 0-77.")

        # Ensure chain and chain_id stay in sync
        expected_chain_id = CHAIN_ID_MAP.get(self.chain)
        if expected_chain_id is not None and self.chain_id != expected_chain_id:
            raise ValueError(f"Chain {self.chain.value} has chain_id {expected_chain_id}, but got {self.chain_id}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "symbol": self.symbol,
            "address": self.address,
            "decimals": self.decimals,
            "chain": self.chain.value,
            "chain_id": self.chain_id,
            "name": self.name,
            "coingecko_id": self.coingecko_id,
            "is_stablecoin": self.is_stablecoin,
            "is_native": self.is_native,
            "is_wrapped_native": self.is_wrapped_native,
            "canonical_symbol": self.canonical_symbol,
            "bridge_type": self.bridge_type.value,
            "source": self.source,
            "is_verified": self.is_verified,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ResolvedToken":
        """Create ResolvedToken from dictionary."""
        return cls(
            symbol=data["symbol"],
            address=data["address"],
            decimals=data["decimals"],
            chain=Chain(data["chain"]),
            chain_id=data["chain_id"],
            name=data.get("name"),
            coingecko_id=data.get("coingecko_id"),
            is_stablecoin=data.get("is_stablecoin", False),
            is_native=data.get("is_native", False),
            is_wrapped_native=data.get("is_wrapped_native", False),
            canonical_symbol=data.get("canonical_symbol"),
            bridge_type=BridgeType(data.get("bridge_type", "NATIVE")),
            source=data.get("source", "cache"),
            is_verified=data.get("is_verified", True),
            resolved_at=datetime.fromisoformat(data["resolved_at"]) if data.get("resolved_at") else None,
        )


# Chain ID mapping for validation - keeps chain and chain_id in sync
# Must stay in sync with Chain enum in almanak/core/enums.py
CHAIN_ID_MAP: dict[Chain, int] = {
    Chain.ETHEREUM: 1,
    Chain.ARBITRUM: 42161,
    Chain.OPTIMISM: 10,
    Chain.BASE: 8453,
    Chain.AVALANCHE: 43114,
    Chain.POLYGON: 137,
    Chain.BSC: 56,
    Chain.SONIC: 146,
    Chain.PLASMA: 9745,
    Chain.BLAST: 81457,
    Chain.MANTLE: 5000,
    Chain.BERACHAIN: 80094,
}


@dataclass
class Token:
    """Core token metadata with addresses across multiple chains.

    Represents a token's cross-chain identity, including its symbol,
    name, decimals, and contract addresses on various chains.

    Attributes:
        symbol: Token symbol (e.g., "ETH", "USDC", "WBTC")
        name: Human-readable token name (e.g., "Ethereum", "USD Coin")
        decimals: Default decimal places for the token (usually 18, but 6 for USDC/USDT, 8 for WBTC)
        addresses: Dictionary mapping chain names to contract addresses
                   (e.g., {"ethereum": "0x...", "arbitrum": "0x..."})
        coingecko_id: CoinGecko API identifier for price fetching
        is_stablecoin: Whether this token is a stablecoin (affects pricing logic)
        chain_overrides: Dictionary mapping chain names to ChainTokenConfig for
                        chain-specific overrides (e.g., different decimals or bridge type)

    Example:
        usdc = Token(
            symbol="USDC",
            name="USD Coin",
            decimals=6,
            addresses={
                "ethereum": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "arbitrum": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            },
            coingecko_id="usd-coin",
            is_stablecoin=True,
            chain_overrides={
                "arbitrum": ChainTokenConfig(
                    address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                    decimals=6,
                    is_native=False,
                    bridge_type=BridgeType.NATIVE,  # Native USDC on Arbitrum
                ),
            },
        )
    """

    symbol: str
    name: str
    decimals: int
    addresses: dict[str, str] = field(default_factory=dict)
    coingecko_id: str | None = None
    is_stablecoin: bool = False
    chain_overrides: dict[str, ChainTokenConfig] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate token data."""
        if not self.symbol:
            raise ValueError("Token symbol cannot be empty")
        if self.decimals < 0 or self.decimals > 77:
            raise ValueError(f"Invalid decimals: {self.decimals}. Must be 0-77.")
        # Normalize symbol to uppercase
        object.__setattr__(self, "symbol", self.symbol.upper())

    def get_address(self, chain: str) -> str | None:
        """Get the token address for a specific chain.

        Args:
            chain: Chain name (e.g., "ethereum", "arbitrum")

        Returns:
            Contract address or None if not available on this chain
        """
        chain_lower = chain.lower()
        # Check chain_overrides first
        if chain_lower in self.chain_overrides:
            return self.chain_overrides[chain_lower].address
        return self.addresses.get(chain_lower)

    def get_decimals(self, chain: str) -> int:
        """Get the token decimals for a specific chain.

        Args:
            chain: Chain name (e.g., "ethereum", "arbitrum")

        Returns:
            Decimal places for the token on this chain (uses chain override if available)
        """
        chain_lower = chain.lower()
        if chain_lower in self.chain_overrides:
            return self.chain_overrides[chain_lower].decimals
        return self.decimals

    def get_chain_config(self, chain: str) -> ChainTokenConfig | None:
        """Get the chain-specific configuration for a token.

        Args:
            chain: Chain name (e.g., "ethereum", "arbitrum")

        Returns:
            ChainTokenConfig if override exists, None otherwise
        """
        return self.chain_overrides.get(chain.lower())

    def has_address_on(self, chain: str) -> bool:
        """Check if token has an address on a specific chain.

        Args:
            chain: Chain name to check

        Returns:
            True if token is deployed on the chain
        """
        chain_lower = chain.lower()
        return chain_lower in self.addresses or chain_lower in self.chain_overrides

    @property
    def chains(self) -> list[str]:
        """Return list of chains this token is deployed on."""
        all_chains = set(self.addresses.keys())
        all_chains.update(self.chain_overrides.keys())
        return list(all_chains)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "symbol": self.symbol,
            "name": self.name,
            "decimals": self.decimals,
            "addresses": self.addresses,
            "coingecko_id": self.coingecko_id,
            "is_stablecoin": self.is_stablecoin,
            "chain_overrides": {
                chain: {
                    "address": config.address,
                    "decimals": config.decimals,
                    "is_native": config.is_native,
                    "bridge_type": config.bridge_type.value,
                }
                for chain, config in self.chain_overrides.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Token":
        """Create Token from dictionary."""
        chain_overrides = {}
        if "chain_overrides" in data:
            for chain, config_data in data["chain_overrides"].items():
                chain_overrides[chain] = ChainTokenConfig(
                    address=config_data["address"],
                    decimals=config_data["decimals"],
                    is_native=config_data.get("is_native", False),
                    bridge_type=BridgeType(config_data.get("bridge_type", "NATIVE")),
                )
        return cls(
            symbol=data["symbol"],
            name=data["name"],
            decimals=data["decimals"],
            addresses=data.get("addresses", {}),
            coingecko_id=data.get("coingecko_id"),
            is_stablecoin=data.get("is_stablecoin", False),
            chain_overrides=chain_overrides,
        )


@dataclass
class ChainToken:
    """Token representation on a specific chain.

    Represents a token's presence on a specific blockchain with
    chain-specific details like address and decimals override.

    Attributes:
        token: Reference to the base Token
        chain: Chain name (e.g., "ethereum", "arbitrum", "optimism")
        address: Contract address on this specific chain
        decimals: Decimal places (may differ from base token on some chains)
        bridge_canonical: Whether this is the canonical/official bridge representation

    Example:
        weth_token = Token(symbol="WETH", name="Wrapped Ether", decimals=18)
        weth_arbitrum = ChainToken(
            token=weth_token,
            chain="arbitrum",
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            decimals=18,
            bridge_canonical=True,
        )
    """

    token: Token
    chain: str
    address: str
    decimals: int
    bridge_canonical: bool = True

    def __post_init__(self) -> None:
        """Validate chain token data."""
        if not self.chain:
            raise ValueError("Chain cannot be empty")
        if not self.address:
            raise ValueError("Address cannot be empty")
        if self.decimals < 0 or self.decimals > 77:
            raise ValueError(f"Invalid decimals: {self.decimals}. Must be 0-77.")
        # Normalize chain to lowercase
        object.__setattr__(self, "chain", self.chain.lower())

    @property
    def symbol(self) -> str:
        """Return the token symbol."""
        return self.token.symbol

    @property
    def name(self) -> str:
        """Return the token name."""
        return self.token.name

    @property
    def coingecko_id(self) -> str | None:
        """Return the CoinGecko ID."""
        return self.token.coingecko_id

    @property
    def is_stablecoin(self) -> bool:
        """Return whether this is a stablecoin."""
        return self.token.is_stablecoin

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "token": self.token.to_dict(),
            "chain": self.chain,
            "address": self.address,
            "decimals": self.decimals,
            "bridge_canonical": self.bridge_canonical,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChainToken":
        """Create ChainToken from dictionary."""
        return cls(
            token=Token.from_dict(data["token"]),
            chain=data["chain"],
            address=data["address"],
            decimals=data["decimals"],
            bridge_canonical=data.get("bridge_canonical", True),
        )


__all__ = [
    "BridgeType",
    "ChainTokenConfig",
    "ResolvedToken",
    "CHAIN_ID_MAP",
    "Token",
    "ChainToken",
]
