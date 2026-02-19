"""Historical Data Provider Registry for PnL backtesting.

This module provides a registry for historical data providers, enabling:
- Dynamic provider discovery
- Priority-based provider selection
- Factory-style instantiation
- Runtime provider lookup by name

Example:
    from almanak.framework.backtesting.pnl.providers.registry import ProviderRegistry
    from almanak.framework.backtesting.pnl.providers import ChainlinkDataProvider

    # Register providers
    ProviderRegistry.register("chainlink", ChainlinkDataProvider, priority=10)

    # Discover available providers
    print(ProviderRegistry.list_all())  # ['chainlink']

    # Get provider class by name
    ChainlinkClass = ProviderRegistry.get("chainlink")
    provider = ChainlinkClass(chain="arbitrum")

    # Get providers by priority
    providers = ProviderRegistry.get_by_priority()
    # Returns list sorted by priority (lowest number first)
"""

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ProviderMetadata:
    """Metadata for a registered data provider.

    Attributes:
        name: Provider name (unique identifier)
        provider_class: The provider class
        priority: Priority for selection (lower = higher priority)
        description: Human-readable description
        supported_tokens: List of supported token symbols
        supported_chains: List of supported chain identifiers
        extra: Additional metadata
    """

    name: str
    provider_class: type
    priority: int = 100
    description: str = ""
    supported_tokens: list[str] = field(default_factory=list)
    supported_chains: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "provider_class": self.provider_class.__name__,
            "priority": self.priority,
            "description": self.description,
            "supported_tokens": self.supported_tokens,
            "supported_chains": self.supported_chains,
            "extra": self.extra,
        }


class ProviderRegistry:
    """Registry for historical data provider discovery and instantiation.

    This class provides a centralized registry for data provider classes,
    enabling dynamic lookup, priority-based selection, and factory-style creation.

    The registry uses class methods so it can be used without instantiation,
    acting as a singleton-like pattern for global provider registration.

    Priority System:
        - Lower priority numbers mean higher precedence
        - Default priority is 100
        - Recommended ranges:
            - 1-10: On-chain oracles (Chainlink, TWAP)
            - 11-50: Primary APIs (CoinGecko, DeFiLlama)
            - 51-100: Secondary/fallback sources

    Example:
        # Register a provider with priority
        ProviderRegistry.register(
            "chainlink",
            ChainlinkDataProvider,
            priority=10,
            metadata={
                "description": "Chainlink on-chain price feeds",
                "supported_chains": ["ethereum", "arbitrum", "base"],
            }
        )

        # Get providers sorted by priority
        providers = ProviderRegistry.get_by_priority()
        for meta in providers:
            print(f"{meta.name}: priority {meta.priority}")

        # Create a provider instance
        provider = ProviderRegistry.create("chainlink", chain="arbitrum")
    """

    _providers: dict[str, ProviderMetadata] = {}

    @classmethod
    def register(
        cls,
        name: str,
        provider_class: type,
        priority: int = 100,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register a data provider class.

        Args:
            name: Unique name for the provider (case-insensitive)
            provider_class: The provider class to register
            priority: Priority for selection (lower = higher priority, default 100)
            metadata: Optional metadata about the provider (description, supported_tokens, etc.)

        Example:
            ProviderRegistry.register("chainlink", ChainlinkDataProvider, priority=10, metadata={
                "description": "Chainlink on-chain price feeds",
                "supported_chains": ["ethereum", "arbitrum"],
            })
        """
        name_lower = name.lower()
        extra_metadata = metadata or {}

        if name_lower in cls._providers:
            logger.warning(
                "Overwriting existing provider registration: %s",
                name_lower,
            )

        # Extract known metadata fields
        description = extra_metadata.pop("description", "")
        supported_tokens = extra_metadata.pop("supported_tokens", [])
        supported_chains = extra_metadata.pop("supported_chains", [])

        provider_meta = ProviderMetadata(
            name=name_lower,
            provider_class=provider_class,
            priority=priority,
            description=description,
            supported_tokens=supported_tokens,
            supported_chains=supported_chains,
            extra=extra_metadata,
        )

        cls._providers[name_lower] = provider_meta

        logger.info(
            "Registered data provider: %s -> %s (priority: %d)",
            name_lower,
            provider_class.__name__,
            priority,
        )

    @classmethod
    def get(cls, name: str) -> type | None:
        """Get a provider class by name.

        Args:
            name: Provider name (case-insensitive)

        Returns:
            The provider class, or None if not found

        Example:
            ChainlinkClass = ProviderRegistry.get("chainlink")
            if ChainlinkClass:
                provider = ChainlinkClass(chain="arbitrum")
        """
        meta = cls._providers.get(name.lower())
        return meta.provider_class if meta else None

    @classmethod
    def get_metadata(cls, name: str) -> ProviderMetadata | None:
        """Get full metadata for a provider.

        Args:
            name: Provider name (case-insensitive)

        Returns:
            ProviderMetadata, or None if not found
        """
        return cls._providers.get(name.lower())

    @classmethod
    def has(cls, name: str) -> bool:
        """Check if a provider is registered.

        Args:
            name: Provider name (case-insensitive)

        Returns:
            True if registered, False otherwise
        """
        return name.lower() in cls._providers

    @classmethod
    def list_all(cls) -> list[str]:
        """List all registered provider names.

        Returns:
            Sorted list of registered provider names
        """
        return sorted(cls._providers.keys())

    @classmethod
    def get_by_priority(cls) -> list[ProviderMetadata]:
        """Get all providers sorted by priority (lowest first).

        Returns:
            List of ProviderMetadata sorted by priority

        Example:
            for meta in ProviderRegistry.get_by_priority():
                print(f"{meta.name}: priority {meta.priority}")
        """
        return sorted(cls._providers.values(), key=lambda m: m.priority)

    @classmethod
    def get_for_chain(cls, chain: str) -> list[ProviderMetadata]:
        """Get providers that support a specific chain, sorted by priority.

        Args:
            chain: Chain identifier (e.g., "ethereum", "arbitrum")

        Returns:
            List of ProviderMetadata for providers supporting the chain
        """
        chain_lower = chain.lower()
        matching = [
            meta
            for meta in cls._providers.values()
            if not meta.supported_chains or chain_lower in [c.lower() for c in meta.supported_chains]
        ]
        return sorted(matching, key=lambda m: m.priority)

    @classmethod
    def get_for_token(cls, token: str) -> list[ProviderMetadata]:
        """Get providers that support a specific token, sorted by priority.

        Args:
            token: Token symbol (e.g., "ETH", "WETH")

        Returns:
            List of ProviderMetadata for providers supporting the token
        """
        token_upper = token.upper()
        matching = [
            meta
            for meta in cls._providers.values()
            if not meta.supported_tokens or token_upper in [t.upper() for t in meta.supported_tokens]
        ]
        return sorted(matching, key=lambda m: m.priority)

    @classmethod
    def unregister(cls, name: str) -> bool:
        """Unregister a provider.

        Args:
            name: Provider name (case-insensitive)

        Returns:
            True if unregistered, False if not found
        """
        name_lower = name.lower()
        if name_lower in cls._providers:
            del cls._providers[name_lower]
            logger.info("Unregistered data provider: %s", name_lower)
            return True
        return False

    @classmethod
    def clear(cls) -> None:
        """Clear all registered providers.

        Primarily used for testing.
        """
        cls._providers.clear()
        logger.info("Cleared all provider registrations")

    @classmethod
    def create(
        cls,
        name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any | None:
        """Factory method to create a provider instance.

        Args:
            name: Provider name (case-insensitive)
            *args: Positional arguments for provider constructor
            **kwargs: Keyword arguments for provider constructor

        Returns:
            Provider instance, or None if not found

        Example:
            provider = ProviderRegistry.create("chainlink", chain="arbitrum")
            if provider:
                price = await provider.get_price("ETH", datetime.now())
        """
        provider_class = cls.get(name)
        if provider_class is None:
            logger.warning("Provider not found: %s", name)
            return None

        return provider_class(*args, **kwargs)

    @classmethod
    def get_best_provider(
        cls,
        token: str | None = None,
        chain: str | None = None,
    ) -> ProviderMetadata | None:
        """Get the highest priority provider matching criteria.

        Args:
            token: Optional token symbol to filter by
            chain: Optional chain identifier to filter by

        Returns:
            ProviderMetadata for the best matching provider, or None if none match

        Example:
            # Get best provider for ETH on Arbitrum
            meta = ProviderRegistry.get_best_provider(token="ETH", chain="arbitrum")
            if meta:
                provider = meta.provider_class(chain="arbitrum")
        """
        candidates = list(cls._providers.values())

        # Filter by chain if specified
        if chain:
            chain_lower = chain.lower()
            candidates = [
                m
                for m in candidates
                if not m.supported_chains or chain_lower in [c.lower() for c in m.supported_chains]
            ]

        # Filter by token if specified
        if token:
            token_upper = token.upper()
            candidates = [
                m
                for m in candidates
                if not m.supported_tokens or token_upper in [t.upper() for t in m.supported_tokens]
            ]

        if not candidates:
            return None

        # Return highest priority (lowest number)
        return min(candidates, key=lambda m: m.priority)

    @classmethod
    def to_dict(cls) -> dict[str, dict[str, Any]]:
        """Convert registry to dictionary for serialization.

        Returns:
            Dictionary mapping provider names to their metadata
        """
        return {name: meta.to_dict() for name, meta in cls._providers.items()}


# =============================================================================
# Auto-registration of built-in providers
# =============================================================================


def _register_builtin_providers() -> None:
    """Register built-in providers with the registry.

    This function is called on module import to register the default providers.
    """
    # Import here to avoid circular imports
    from .chainlink import CHAINLINK_PRICE_FEEDS, TOKEN_TO_PAIR, ChainlinkDataProvider
    from .coingecko import CoinGeckoDataProvider

    # Register Chainlink provider
    ProviderRegistry.register(
        "chainlink",
        ChainlinkDataProvider,
        priority=ChainlinkDataProvider.DEFAULT_PRIORITY,
        metadata={
            "description": "Chainlink on-chain price feeds with staleness checking",
            "supported_tokens": list(TOKEN_TO_PAIR.keys()),
            "supported_chains": list(CHAINLINK_PRICE_FEEDS.keys()),
        },
    )

    # Register CoinGecko provider
    # CoinGecko is a secondary source (API-based) so higher priority number
    ProviderRegistry.register(
        "coingecko",
        CoinGeckoDataProvider,
        priority=50,  # Lower priority than Chainlink
        metadata={
            "description": "CoinGecko API-based historical price data",
            "supported_chains": ["ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche"],
        },
    )

    logger.debug("Registered %d built-in providers", len(ProviderRegistry.list_all()))


# Auto-register on import
_register_builtin_providers()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "ProviderRegistry",
    "ProviderMetadata",
]
