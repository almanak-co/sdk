"""Multi-DEX volume provider aggregator.

This module provides an aggregator that routes volume queries to the correct
DEX-specific provider based on protocol or pool detection. It implements the
HistoricalVolumeProvider interface and provides a unified entry point for
fetching historical volume data across multiple DEX protocols.

Supported Protocols:
    - Uniswap V3 (Ethereum, Arbitrum, Base, Optimism, Polygon)
    - SushiSwap V3 (Ethereum)
    - PancakeSwap V3 (Ethereum, Arbitrum, BSC, Base)
    - Aerodrome (Base)
    - TraderJoe V2 (Avalanche)
    - Curve (Ethereum, Optimism)
    - Balancer (Ethereum, Arbitrum, Polygon)

Example:
    from almanak.framework.backtesting.pnl.providers.multi_dex_volume import (
        MultiDEXVolumeProvider,
    )
    from almanak.core.enums import Chain, Protocol
    from datetime import date

    # With explicit protocol
    provider = MultiDEXVolumeProvider()
    async with provider:
        volumes = await provider.get_volume(
            pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            chain=Chain.ARBITRUM,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            protocol=Protocol.UNISWAP_V3,
        )

    # With string protocol identifier
    volumes = await provider.get_volume(
        pool_address="0x...",
        chain=Chain.BASE,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        protocol="aerodrome",
    )
"""

import logging
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.core.enums import Chain, Protocol

from ..types import DataConfidence, DataSourceInfo, VolumeResult
from .base import HistoricalVolumeProvider
from .dex import (
    AERODROME_SUBGRAPH_IDS,
    BALANCER_SUBGRAPH_IDS,
    CURVE_SUBGRAPH_IDS,
    PANCAKESWAP_V3_SUBGRAPH_IDS,
    SUSHISWAP_V3_SUBGRAPH_IDS,
    TRADERJOE_V2_SUBGRAPH_IDS,
    UNISWAP_V3_SUBGRAPH_IDS,
    AerodromeVolumeProvider,
    BalancerVolumeProvider,
    CurveVolumeProvider,
    PancakeSwapV3VolumeProvider,
    SushiSwapV3VolumeProvider,
    TraderJoeV2VolumeProvider,
    UniswapV3VolumeProvider,
)
from .subgraph_client import SubgraphClient, SubgraphClientConfig

logger = logging.getLogger(__name__)


# =============================================================================
# Protocol to Provider Mapping
# =============================================================================

# Map Protocol enum values to provider info
PROTOCOL_PROVIDER_MAP: dict[Protocol, type[HistoricalVolumeProvider]] = {
    Protocol.UNISWAP_V3: UniswapV3VolumeProvider,
    Protocol.SUSHISWAP_V3: SushiSwapV3VolumeProvider,
    Protocol.PANCAKESWAP_V3: PancakeSwapV3VolumeProvider,
    Protocol.AERODROME: AerodromeVolumeProvider,
    Protocol.TRADERJOE_V2: TraderJoeV2VolumeProvider,
}

# Map string identifiers to Protocol enum (for protocols not in Protocol enum)
STRING_PROTOCOL_MAP: dict[str, type[HistoricalVolumeProvider]] = {
    # Protocol enum values (lowercase)
    "uniswap_v3": UniswapV3VolumeProvider,
    "sushiswap_v3": SushiSwapV3VolumeProvider,
    "pancakeswap_v3": PancakeSwapV3VolumeProvider,
    "aerodrome": AerodromeVolumeProvider,
    "traderjoe_v2": TraderJoeV2VolumeProvider,
    # Additional protocols not in Protocol enum
    "curve": CurveVolumeProvider,
    "balancer": BalancerVolumeProvider,
    # Common aliases
    "uni_v3": UniswapV3VolumeProvider,
    "sushi_v3": SushiSwapV3VolumeProvider,
    "pancake_v3": PancakeSwapV3VolumeProvider,
    "joe_v2": TraderJoeV2VolumeProvider,
    "bal": BalancerVolumeProvider,
    "crv": CurveVolumeProvider,
}

# Map protocols to their supported chains via subgraph IDs
PROTOCOL_CHAIN_SUPPORT: dict[str, dict[Chain, str]] = {
    "uniswap_v3": UNISWAP_V3_SUBGRAPH_IDS,
    "sushiswap_v3": SUSHISWAP_V3_SUBGRAPH_IDS,
    "pancakeswap_v3": PANCAKESWAP_V3_SUBGRAPH_IDS,
    "aerodrome": AERODROME_SUBGRAPH_IDS,
    "traderjoe_v2": TRADERJOE_V2_SUBGRAPH_IDS,
    "curve": CURVE_SUBGRAPH_IDS,
    "balancer": BALANCER_SUBGRAPH_IDS,
}

# Data source identifier for fallback results
FALLBACK_DATA_SOURCE = "multi_dex_fallback"


# =============================================================================
# MultiDEXVolumeProvider
# =============================================================================


class MultiDEXVolumeProvider(HistoricalVolumeProvider):
    """Aggregator that routes volume queries to DEX-specific providers.

    Routes volume queries to the correct DEX-specific provider based on the
    protocol parameter. Supports both Protocol enum and string identifiers
    for flexibility.

    When no protocol is specified, the provider will use chain-based heuristics
    to attempt protocol detection (e.g., Base chain pools default to Aerodrome).

    Attributes:
        providers: Dictionary mapping protocol identifiers to provider instances
        fallback_volume: Volume to return when no provider is available

    Example:
        provider = MultiDEXVolumeProvider()

        # With Protocol enum
        async with provider:
            volumes = await provider.get_volume(
                pool_address="0x...",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                protocol=Protocol.UNISWAP_V3,
            )

        # With string identifier
        volumes = await provider.get_volume(
            pool_address="0x...",
            chain=Chain.ETHEREUM,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            protocol="curve",
        )
    """

    def __init__(
        self,
        fallback_volume: Decimal = Decimal("0"),
        requests_per_minute: int = 100,
    ) -> None:
        """Initialize the Multi-DEX volume provider.

        Args:
            fallback_volume: Volume to return when no provider is available.
                            Default is 0, indicating no data.
            requests_per_minute: Rate limit for subgraph requests. Default 100.
        """
        self._fallback_volume = fallback_volume
        self._requests_per_minute = requests_per_minute

        # Create shared subgraph client for efficiency
        config = SubgraphClientConfig(requests_per_minute=requests_per_minute)
        self._shared_client = SubgraphClient(config=config)

        # Lazy-initialized provider instances
        self._providers: dict[str, HistoricalVolumeProvider] = {}

        logger.debug(
            "Initialized MultiDEXVolumeProvider: fallback_volume=%s, requests_per_minute=%s",
            fallback_volume,
            requests_per_minute,
        )

    async def close(self) -> None:
        """Close all provider instances and release resources."""
        # Close all initialized providers
        for protocol_id, provider in self._providers.items():
            try:
                if hasattr(provider, "close"):
                    await provider.close()
            except Exception as e:
                logger.warning("Error closing provider %s: %s", protocol_id, e)

        # Close shared client
        await self._shared_client.close()

        logger.debug("MultiDEXVolumeProvider closed")

    async def __aenter__(self) -> "MultiDEXVolumeProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close all providers."""
        await self.close()

    def _get_protocol_id(self, protocol: Protocol | str | None) -> str | None:
        """Normalize protocol to string identifier.

        Args:
            protocol: Protocol enum, string identifier, or None

        Returns:
            Lowercase string protocol identifier or None
        """
        if protocol is None:
            return None
        if isinstance(protocol, Protocol):
            return protocol.value.lower()
        return protocol.lower()

    def _get_provider(self, protocol_id: str) -> HistoricalVolumeProvider | None:
        """Get or create a provider instance for the given protocol.

        Args:
            protocol_id: Lowercase protocol identifier

        Returns:
            Provider instance or None if protocol not supported
        """
        # Return cached provider if available
        if protocol_id in self._providers:
            return self._providers[protocol_id]

        # Look up provider class
        provider_class: type[HistoricalVolumeProvider] | None = None

        # Try Protocol enum mapping first
        try:
            protocol_enum = Protocol(protocol_id.upper())
            provider_class = PROTOCOL_PROVIDER_MAP.get(protocol_enum)
        except ValueError:
            pass

        # Try string mapping if not found
        if provider_class is None:
            provider_class = STRING_PROTOCOL_MAP.get(protocol_id)

        if provider_class is None:
            logger.warning("No provider found for protocol: %s", protocol_id)
            return None

        # Create provider instance with shared client
        try:
            provider = provider_class(  # type: ignore[call-arg]
                client=self._shared_client,
                fallback_volume=self._fallback_volume,
            )
            self._providers[protocol_id] = provider
            logger.debug("Created provider for protocol: %s", protocol_id)
            return provider
        except Exception as e:
            logger.error("Failed to create provider for %s: %s", protocol_id, e)
            return None

    def _detect_protocol_from_chain(self, chain: Chain) -> str | None:
        """Attempt to detect protocol based on chain.

        Uses heuristics based on chain-specific DEXs. This is a fallback
        when no protocol is specified.

        Args:
            chain: The blockchain chain

        Returns:
            Best-guess protocol identifier or None
        """
        # Chain-specific DEX defaults
        chain_defaults: dict[Chain, str] = {
            Chain.BASE: "aerodrome",  # Aerodrome is native to Base
            Chain.AVALANCHE: "traderjoe_v2",  # TraderJoe is dominant on Avalanche
        }

        if chain in chain_defaults:
            return chain_defaults[chain]

        # Default to Uniswap V3 for other chains (most common)
        if chain in UNISWAP_V3_SUBGRAPH_IDS:
            return "uniswap_v3"

        return None

    def _create_fallback_result(self, d: date) -> VolumeResult:
        """Create a fallback VolumeResult with LOW confidence.

        Args:
            d: Date for the result

        Returns:
            VolumeResult with fallback volume and LOW confidence
        """
        return VolumeResult(
            value=self._fallback_volume,
            source_info=DataSourceInfo(
                source=FALLBACK_DATA_SOURCE,
                confidence=DataConfidence.LOW,
                timestamp=datetime.combine(d, datetime.min.time(), tzinfo=UTC),
            ),
        )

    def _generate_fallback_results(
        self,
        start_date: date,
        end_date: date,
    ) -> list[VolumeResult]:
        """Generate fallback results for a date range.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            List of VolumeResult with LOW confidence fallback values
        """
        results = []
        current = start_date
        while current <= end_date:
            results.append(self._create_fallback_result(current))
            current += timedelta(days=1)
        return results

    async def get_volume(
        self,
        pool_address: str,
        chain: Chain,
        start_date: date,
        end_date: date,
        protocol: Protocol | str | None = None,
    ) -> list[VolumeResult]:
        """Fetch historical volume data by routing to the correct provider.

        Routes the query to the appropriate DEX-specific provider based on
        the protocol parameter. If no protocol is specified, attempts to
        detect based on chain.

        Args:
            pool_address: The pool contract address (checksummed or lowercase).
            chain: The blockchain the pool is on.
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).
            protocol: Protocol enum, string identifier (e.g., "curve"), or None.
                     If None, attempts to detect based on chain.

        Returns:
            List of VolumeResult objects, one per day with available data.
            Returns HIGH confidence results from subgraph data when available.
            Returns LOW confidence fallback results when provider unavailable.

        Example:
            # With explicit protocol
            volumes = await provider.get_volume(
                pool_address="0x...",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                protocol=Protocol.UNISWAP_V3,
            )

            # With string protocol
            volumes = await provider.get_volume(
                pool_address="0x...",
                chain=Chain.ETHEREUM,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                protocol="balancer",
            )

            # Auto-detect protocol
            volumes = await provider.get_volume(
                pool_address="0x...",
                chain=Chain.BASE,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            )  # Will use Aerodrome for Base chain
        """
        # Normalize protocol identifier
        protocol_id = self._get_protocol_id(protocol)

        # If no protocol specified, try to detect from chain
        if protocol_id is None:
            protocol_id = self._detect_protocol_from_chain(chain)
            if protocol_id:
                logger.info(
                    "Auto-detected protocol %s for chain %s",
                    protocol_id,
                    chain.value,
                )

        # If still no protocol, return fallback
        if protocol_id is None:
            logger.warning(
                "Could not determine protocol for chain=%s, pool=%s..., returning fallback",
                chain.value,
                pool_address[:10],
            )
            return self._generate_fallback_results(start_date, end_date)

        # Get provider instance
        provider = self._get_provider(protocol_id)
        if provider is None:
            logger.warning(
                "No provider available for protocol=%s, chain=%s, returning fallback",
                protocol_id,
                chain.value,
            )
            return self._generate_fallback_results(start_date, end_date)

        # Check if chain is supported by this protocol
        chain_support = PROTOCOL_CHAIN_SUPPORT.get(protocol_id, {})
        if chain not in chain_support:
            logger.warning(
                "Chain %s not supported by protocol %s, returning fallback",
                chain.value,
                protocol_id,
            )
            return self._generate_fallback_results(start_date, end_date)

        # Route to the specific provider
        logger.info(
            "Routing volume query to %s: chain=%s, pool=%s...",
            protocol_id,
            chain.value,
            pool_address[:10],
        )

        try:
            results = await provider.get_volume(
                pool_address=pool_address,
                chain=chain,
                start_date=start_date,
                end_date=end_date,
            )
            return results

        except Exception as e:
            logger.error(
                "Error from %s provider: chain=%s, pool=%s...: %s",
                protocol_id,
                chain.value,
                pool_address[:10],
                str(e),
            )
            return self._generate_fallback_results(start_date, end_date)

    def get_supported_protocols(self) -> list[str]:
        """Get list of supported protocol identifiers.

        Returns:
            List of supported protocol string identifiers
        """
        return list(PROTOCOL_CHAIN_SUPPORT.keys())

    def get_supported_chains(self, protocol: Protocol | str) -> list[Chain]:
        """Get list of supported chains for a protocol.

        Args:
            protocol: Protocol enum or string identifier

        Returns:
            List of supported Chain enums
        """
        protocol_id = self._get_protocol_id(protocol)
        if protocol_id is None:
            return []
        chain_support = PROTOCOL_CHAIN_SUPPORT.get(protocol_id, {})
        return list(chain_support.keys())


__all__ = [
    "MultiDEXVolumeProvider",
    "PROTOCOL_PROVIDER_MAP",
    "STRING_PROTOCOL_MAP",
    "PROTOCOL_CHAIN_SUPPORT",
]
