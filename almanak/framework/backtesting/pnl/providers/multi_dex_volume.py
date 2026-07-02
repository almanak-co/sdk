"""Multi-DEX volume provider aggregator.

This module provides an aggregator that routes volume queries to the correct
DEX based on protocol or chain detection. It implements the
HistoricalVolumeProvider interface and provides a unified entry point for
fetching historical volume data across multiple DEX protocols.

**VIB-4851 Phase D**: routing is declaration-driven. Each DEX connector's
``dex_volume=DexVolumeDecl(...)`` manifest declaration owns the dispatch keys,
aliases, chain support, provenance string, and chain-detection defaults —
this aggregator names no DEX and holds no dispatch table (previously:
``PROTOCOL_PROVIDER_MAP`` keyed by the since-removed ``Protocol`` enum +
``STRING_PROTOCOL_MAP`` + ``PROTOCOL_CHAIN_SUPPORT``). Adding a DEX's volume
lane is one connector folder, no edit here.

**VIB-4859 / W7 (VIB-4870)**: the per-DEX fetch is a thin gRPC client of
``RateHistoryService.GetDexVolumeHistory``. The aggregator therefore holds no
subgraph HTTP client and opens no socket — all TheGraph egress lives
gateway-side. Routing-level mismatches (unknown protocol / unsupported chain /
undetectable protocol) still return LOW-confidence fallback rows: these are
*configuration* mismatches that never reach a data source, so the "no silent
zeros" rule (which governs empty/errored *subgraph* responses) does not apply
to them. A genuine "subgraph returned nothing / errored" surfaces as
:class:`DataSourceUnavailable` raised by the gateway-backed fetch and
propagates to the caller (no silent zero-fill).

Example:
    from almanak.framework.backtesting.pnl.providers.multi_dex_volume import (
        MultiDEXVolumeProvider,
    )
    from almanak.core.enums import Chain
    from datetime import date

    provider = MultiDEXVolumeProvider()
    async with provider:
        volumes = await provider.get_volume(
            pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            chain=Chain.ARBITRUM,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            protocol="uniswap_v3",
        )
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry
from almanak.core.enums import Chain

from ..types import DataConfidence, DataSourceInfo, VolumeResult
from .base import HistoricalVolumeProvider
from .dex import GatewayDexVolumeProvider

logger = logging.getLogger(__name__)


# Data source identifier for fallback results
FALLBACK_DATA_SOURCE = "multi_dex_fallback"


# =============================================================================
# MultiDEXVolumeProvider
# =============================================================================


class MultiDEXVolumeProvider(HistoricalVolumeProvider):
    """Aggregator that routes volume queries to declared DEX volume lanes.

    Routes volume queries to the gateway-backed volume lane the protocol's
    connector declares, keyed by string protocol identifiers.

    When no protocol is specified, the provider uses the connector-declared
    chain defaults (e.g. Base chain pools default to Aerodrome).

    Attributes:
        fallback_volume: Volume to return for routing-level mismatches

    Example:
        provider = MultiDEXVolumeProvider()
        async with provider:
            volumes = await provider.get_volume(
                pool_address="0x...",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                protocol="uniswap_v3",
            )
    """

    def __init__(
        self,
        fallback_volume: Decimal = Decimal("0"),
        requests_per_minute: int = 100,
    ) -> None:
        """Initialize the Multi-DEX volume provider.

        Args:
            fallback_volume: Volume returned for routing-level mismatches
                (unknown protocol / unsupported chain / undetectable
                protocol). Default is 0, indicating no data. NOTE: a genuine
                empty/errored subgraph no longer falls back here — the
                gateway-backed fetch raises :class:`DataSourceUnavailable`.
            requests_per_minute: Ignored (kept for back-compat). Rate
                limiting now lives on the gateway side.
        """
        self._fallback_volume = fallback_volume
        self._requests_per_minute = requests_per_minute

        # Lazy-initialized provider instances, keyed by canonical protocol.
        self._providers: dict[str, GatewayDexVolumeProvider] = {}

        logger.debug(
            "Initialized MultiDEXVolumeProvider (gateway-backed): fallback_volume=%s",
            fallback_volume,
        )

    async def close(self) -> None:
        """Close all provider instances and release resources."""
        for protocol_id, provider in self._providers.items():
            try:
                await provider.close()
            except Exception as e:
                logger.warning("Error closing provider %s: %s", protocol_id, e)

        logger.debug("MultiDEXVolumeProvider closed")

    async def __aenter__(self) -> MultiDEXVolumeProvider:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close all providers."""
        await self.close()

    def _get_protocol_id(self, protocol: str | None) -> str | None:
        """Normalize protocol to its canonical declared identifier.

        Args:
            protocol: String protocol identifier, or None

        Returns:
            Canonical declared protocol key, or None when unknown/None
        """
        if protocol is None:
            return None
        return DexVolumeRegistry.canonical(str(protocol))

    def _detect_protocol_from_chain(self, chain: Chain) -> str | None:
        """Attempt to detect protocol based on chain.

        Connector-declared defaults: ``chain_default`` declarations win
        (aerodrome on base, traderjoe_v2 on avalanche), then the
        ``generic_default`` DEX (uniswap_v3) for any chain it supports.

        Args:
            chain: The blockchain chain

        Returns:
            Best-guess protocol identifier or None
        """
        return DexVolumeRegistry.chain_default(chain.value)

    def _get_provider(self, protocol_id: str) -> GatewayDexVolumeProvider | None:
        """Get or create the gateway-backed provider for ``protocol_id``.

        Returns None for identifiers no connector declares (legacy contract;
        ``get_volume`` treats that as a routing mismatch).
        """
        provider = self._providers.get(protocol_id)
        if provider is not None:
            return provider
        if not DexVolumeRegistry.has(protocol_id):
            logger.warning("No provider found for protocol: %s", protocol_id)
            return None
        provider = GatewayDexVolumeProvider(
            protocol=protocol_id,
            fallback_volume=self._fallback_volume,
        )
        self._providers[protocol_id] = provider
        logger.debug("Created provider for protocol: %s", protocol_id)
        return provider

    def _create_fallback_result(self, d: date) -> VolumeResult:
        """Create a fallback VolumeResult with LOW confidence.

        Used only for routing-level mismatches (unknown protocol /
        unsupported chain / undetectable protocol) — NOT for empty/errored
        subgraph responses, which raise :class:`DataSourceUnavailable`.

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
        protocol: str | None = None,
    ) -> list[VolumeResult]:
        """Fetch historical volume data by routing to the declared DEX lane.

        Routes the query to the connector-declared (gateway-backed) volume
        lane based on the protocol parameter. If no protocol is specified,
        attempts to detect based on the connector-declared chain defaults.

        Args:
            pool_address: The pool contract address (checksummed or lowercase).
            chain: The blockchain the pool is on.
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).
            protocol: String protocol identifier (e.g., "curve"), or None.
                     If None, attempts to detect based on chain.

        Returns:
            List of HIGH-confidence VolumeResult objects from the gateway.
            Returns LOW-confidence fallback results ONLY for routing-level
            mismatches (no protocol / unknown protocol / unsupported chain).

        Raises:
            DataSourceUnavailable: when the gateway call fails or the
                subgraph returned no / errored data. The pre-W7 silent
                ``Decimal("0")`` LOW row for an empty/errored subgraph is
                intentionally removed (VIB-4859 decision 4).
        """
        # Normalize protocol identifier (canonical declared key or None)
        protocol_id = self._get_protocol_id(protocol)

        if protocol is not None and protocol_id is None:
            # An explicit-but-unknown protocol is a routing mismatch.
            logger.warning(
                "No provider found for protocol: %s",
                getattr(protocol, "value", protocol),
            )
            return self._generate_fallback_results(start_date, end_date)

        # If no protocol specified, try to detect from chain
        if protocol_id is None:
            protocol_id = self._detect_protocol_from_chain(chain)
            if protocol_id:
                logger.info(
                    "Auto-detected protocol %s for chain %s",
                    protocol_id,
                    chain.value,
                )

        # If still no protocol, return fallback (routing mismatch)
        if protocol_id is None:
            logger.warning(
                "Could not determine protocol for chain=%s, pool=%s..., returning fallback",
                chain.value,
                pool_address[:10],
            )
            return self._generate_fallback_results(start_date, end_date)

        # Check if chain is declared by this protocol (routing mismatch)
        entry = DexVolumeRegistry.entry_for(protocol_id)
        if entry is None or chain.value.lower() not in entry.chains:
            logger.warning(
                "Chain %s not supported by protocol %s, returning fallback",
                chain.value,
                protocol_id,
            )
            return self._generate_fallback_results(start_date, end_date)

        # Route to the declared (gateway-backed) lane. A gateway failure
        # or an empty/errored subgraph raises DataSourceUnavailable, which
        # propagates to the caller — no silent zero-fill.
        logger.info(
            "Routing volume query to %s: chain=%s, pool=%s...",
            protocol_id,
            chain.value,
            pool_address[:10],
        )
        provider = self._get_provider(protocol_id)
        if provider is None:  # pragma: no cover - canonical ids are declared
            return self._generate_fallback_results(start_date, end_date)
        return await provider.get_volume(
            pool_address=pool_address,
            chain=chain,
            start_date=start_date,
            end_date=end_date,
        )

    def get_supported_protocols(self) -> list[str]:
        """Get list of supported protocol identifiers.

        Returns:
            List of declared protocol string identifiers (sorted)
        """
        return list(DexVolumeRegistry.supported_protocols())

    def get_supported_chains(self, protocol: str) -> list[Chain]:
        """Get list of supported chains for a protocol.

        Args:
            protocol: String protocol identifier

        Returns:
            List of supported Chain enums (declaration order)
        """
        protocol_id = self._get_protocol_id(protocol)
        if protocol_id is None:
            return []
        entry = DexVolumeRegistry.entry_for(protocol_id)
        if entry is None:
            return []
        return [Chain(c.upper()) for c in entry.chains]


__all__ = [
    "MultiDEXVolumeProvider",
    "FALLBACK_DATA_SOURCE",
]
