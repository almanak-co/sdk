"""Abstract base classes for historical data providers.

This module defines the abstract interfaces that all historical data providers
must implement. These interfaces ensure consistent behavior across different
data sources (subgraphs, APIs, RPCs) and enable composable provider patterns.

Key Interfaces:
    - HistoricalVolumeProvider: For fetching historical pool volume data
    - HistoricalFundingProvider: For fetching historical perp funding rates
    - HistoricalAPYProvider: For fetching historical lending supply/borrow APYs
    - HistoricalLiquidityProvider: For fetching historical liquidity depth

All methods are async to support efficient I/O operations and concurrent requests.

Examples:
    Implementing a volume provider:

        from almanak.framework.backtesting.pnl.providers.base import (
            HistoricalVolumeProvider,
        )
        from almanak.framework.backtesting.pnl.types import VolumeResult
        from almanak.core.enums import Chain
        from datetime import date

        class MyVolumeProvider(HistoricalVolumeProvider):
            async def get_volume(
                self,
                pool_address: str,
                chain: Chain,
                start_date: date,
                end_date: date,
            ) -> list[VolumeResult]:
                # Fetch volume data from your source
                ...

    Using a provider:

        provider = MyVolumeProvider()
        volumes = await provider.get_volume(
            pool_address="0x...",
            chain=Chain.ARBITRUM,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )
        for vol in volumes:
            print(f"{vol.source_info.timestamp}: {vol.value} ({vol.source_info.confidence})")
"""

from abc import ABC, abstractmethod
from datetime import date, datetime

from almanak.core.enums import Chain

from ..types import APYResult, FundingResult, LiquidityResult, VolumeResult


class HistoricalVolumeProvider(ABC):
    """Abstract base class for historical volume data providers.

    Implementations fetch historical trading volume data from various sources
    such as subgraphs (Uniswap V3, SushiSwap, PancakeSwap, etc.) or APIs.

    Volume data is used for:
        - LP fee calculations in backtesting
        - Pool selection based on historical activity
        - Volume-weighted average price calculations

    All implementations must handle:
        - Rate limiting and backoff
        - Caching for efficiency
        - Graceful degradation when data is unavailable
    """

    @abstractmethod
    async def get_volume(
        self,
        pool_address: str,
        chain: Chain,
        start_date: date,
        end_date: date,
    ) -> list[VolumeResult]:
        """Fetch historical volume data for a pool.

        Args:
            pool_address: The pool contract address (checksummed or lowercase).
            chain: The blockchain the pool is on.
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).

        Returns:
            List of VolumeResult objects, one per day with available data.
            May return fewer results than days if data is not available for
            all days in the range. Each result includes source info with
            confidence level.

        Raises:
            May raise provider-specific exceptions (e.g., SubgraphRateLimitError).
            Implementations should document their specific exceptions.

        Example:
            volumes = await provider.get_volume(
                pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            )
        """
        ...


class HistoricalFundingProvider(ABC):
    """Abstract base class for historical funding rate providers.

    Implementations fetch historical funding rate data from perpetual futures
    protocols such as GMX V2 and Hyperliquid.

    Funding rate data is used for:
        - Perp position P&L calculations in backtesting
        - Funding cost/income projections
        - Protocol comparison analysis

    All implementations must handle:
        - Rate limiting and backoff
        - Caching for efficiency
        - Graceful degradation when data is unavailable
    """

    @abstractmethod
    async def get_funding_rates(
        self,
        market: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingResult]:
        """Fetch historical funding rates for a market.

        Args:
            market: The market identifier (e.g., "ETH-USD", "BTC-USD").
                Format may vary by protocol.
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).

        Returns:
            List of FundingResult objects, typically one per funding period
            (hourly for most protocols). Each result includes source info
            with confidence level.

        Raises:
            May raise provider-specific exceptions (e.g., FundingRateRateLimitError).
            Implementations should document their specific exceptions.

        Example:
            rates = await provider.get_funding_rates(
                market="ETH-USD",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )
        """
        ...


class HistoricalAPYProvider(ABC):
    """Abstract base class for historical APY providers.

    Implementations fetch historical supply and borrow APY data from lending
    protocols such as Aave V3, Compound V3, Morpho Blue, and Spark.

    APY data is used for:
        - Interest accrual calculations in backtesting
        - Yield strategy optimization
        - Protocol comparison analysis

    All implementations must handle:
        - Rate limiting and backoff
        - Caching for efficiency
        - Graceful degradation when data is unavailable
    """

    @abstractmethod
    async def get_apy(
        self,
        protocol: str,
        market: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[APYResult]:
        """Fetch historical APY data for a lending market.

        Args:
            protocol: The protocol identifier (e.g., "aave_v3", "compound_v3").
            market: The market/asset identifier (e.g., "USDC", "WETH").
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).

        Returns:
            List of APYResult objects containing both supply and borrow APYs.
            Frequency depends on the protocol's data availability (daily or
            more granular). Each result includes source info with confidence
            level.

        Raises:
            May raise provider-specific exceptions (e.g., LendingAPYRateLimitError).
            Implementations should document their specific exceptions.

        Example:
            apys = await provider.get_apy(
                protocol="aave_v3",
                market="USDC",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 31, tzinfo=UTC),
            )
        """
        ...


class HistoricalLiquidityProvider(ABC):
    """Abstract base class for historical liquidity depth providers.

    Implementations fetch historical liquidity depth data from DEX subgraphs
    for accurate slippage modeling in backtesting.

    Liquidity data is used for:
        - Slippage calculations for LP entry/exit
        - Swap impact estimation
        - Pool quality assessment

    All implementations must handle:
        - Rate limiting and backoff
        - Caching for efficiency
        - Graceful degradation when data is unavailable
    """

    @abstractmethod
    async def get_liquidity_depth(
        self,
        pool_address: str,
        chain: Chain,
        timestamp: datetime,
    ) -> LiquidityResult:
        """Fetch historical liquidity depth for a pool at a specific timestamp.

        Args:
            pool_address: The pool contract address (checksummed or lowercase).
            chain: The blockchain the pool is on.
            timestamp: The point in time to get liquidity depth for.

        Returns:
            LiquidityResult containing the liquidity depth in USD and source
            info with confidence level.

        Raises:
            May raise provider-specific exceptions (e.g., SubgraphRateLimitError).
            Implementations should document their specific exceptions.

        Note:
            For concentrated liquidity pools (V3-style), depth represents
            liquidity within the active tick range. For constant product pools
            (V2-style), depth represents total pool TVL.

        Example:
            liquidity = await provider.get_liquidity_depth(
                pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                chain=Chain.ARBITRUM,
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )
        """
        ...


__all__ = [
    "HistoricalVolumeProvider",
    "HistoricalFundingProvider",
    "HistoricalAPYProvider",
    "HistoricalLiquidityProvider",
]
