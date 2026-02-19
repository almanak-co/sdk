"""Data types for backtesting data providers.

This module defines types for tracking data confidence levels and sources
for transparency in backtest results. These types are used by historical
data providers to communicate the quality and provenance of fetched data.

Key Components:
    - DataConfidence: Enum for data quality/confidence levels
    - DataSourceInfo: Metadata about where data came from
    - VolumeResult: Historical volume data with source tracking
    - FundingResult: Historical funding rate data with source tracking
    - APYResult: Historical APY data with source tracking
    - LiquidityResult: Historical liquidity depth with source tracking

Examples:
    Creating a volume result from subgraph data:

        from datetime import datetime, UTC
        from decimal import Decimal
        from almanak.framework.backtesting.pnl.types import (
            DataConfidence, DataSourceInfo, VolumeResult
        )

        source_info = DataSourceInfo(
            source="uniswap_v3_subgraph",
            confidence=DataConfidence.HIGH,
            timestamp=datetime.now(UTC),
        )
        volume = VolumeResult(
            value=Decimal("1500000"),
            source_info=source_info,
        )

    Creating a fallback result with low confidence:

        source_info = DataSourceInfo(
            source="fallback_multiplier",
            confidence=DataConfidence.LOW,
            timestamp=datetime.now(UTC),
        )
        volume = VolumeResult(
            value=Decimal("100000"),
            source_info=source_info,
        )
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum


class DataConfidence(Enum):
    """Confidence level for historical data.

    Indicates the reliability and accuracy of data fetched from
    historical data providers. Used to track data quality in
    backtest results.

    Attributes:
        HIGH: Data from primary source (e.g., subgraph, direct API).
            Considered highly accurate and reliable.
        MEDIUM: Data from secondary source or with some uncertainty.
            May involve interpolation or less reliable sources.
        LOW: Data from fallback values or estimates.
            Should be treated with caution in backtest analysis.
    """

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class DataSourceInfo:
    """Metadata about data source and confidence.

    Tracks where data came from and how confident we are in its
    accuracy. Attached to result objects for transparency in
    backtest results.

    Attributes:
        source: Identifier for the data source (e.g., "uniswap_v3_subgraph",
            "coingecko_api", "fallback_multiplier").
        confidence: Confidence level of the data.
        timestamp: When the data was fetched or the timestamp it represents.

    Example:
        source_info = DataSourceInfo(
            source="aave_v3_subgraph",
            confidence=DataConfidence.HIGH,
            timestamp=datetime.now(UTC),
        )
    """

    source: str
    confidence: DataConfidence
    timestamp: datetime


@dataclass
class VolumeResult:
    """Historical volume data with source tracking.

    Contains pool volume data (typically daily) along with metadata
    about where the data came from and its confidence level.

    Attributes:
        value: Trading volume in USD.
        source_info: Metadata about data source and confidence.

    Example:
        result = VolumeResult(
            value=Decimal("1500000"),
            source_info=DataSourceInfo(
                source="uniswap_v3_subgraph",
                confidence=DataConfidence.HIGH,
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            ),
        )
    """

    value: Decimal
    source_info: DataSourceInfo


@dataclass
class FundingResult:
    """Historical funding rate data with source tracking.

    Contains perpetual protocol funding rate data along with metadata
    about where the data came from and its confidence level.

    Attributes:
        rate: Funding rate (typically hourly). Positive means longs pay shorts,
            negative means shorts pay longs. E.g., 0.0001 = 0.01%/hr.
        source_info: Metadata about data source and confidence.

    Example:
        result = FundingResult(
            rate=Decimal("0.0001"),
            source_info=DataSourceInfo(
                source="gmx_api",
                confidence=DataConfidence.HIGH,
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            ),
        )
    """

    rate: Decimal
    source_info: DataSourceInfo


@dataclass
class APYResult:
    """Historical APY data with source tracking.

    Contains lending protocol APY data (both supply and borrow rates)
    along with metadata about where the data came from and its confidence level.

    Attributes:
        supply_apy: Annual supply/deposit APY as a decimal (e.g., 0.03 = 3%).
        borrow_apy: Annual borrow APY as a decimal (e.g., 0.05 = 5%).
        source_info: Metadata about data source and confidence.

    Example:
        result = APYResult(
            supply_apy=Decimal("0.03"),
            borrow_apy=Decimal("0.05"),
            source_info=DataSourceInfo(
                source="aave_v3_subgraph",
                confidence=DataConfidence.HIGH,
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            ),
        )
    """

    supply_apy: Decimal
    borrow_apy: Decimal
    source_info: DataSourceInfo


@dataclass
class LiquidityResult:
    """Historical liquidity depth data with source tracking.

    Contains pool liquidity depth data for slippage modeling along with
    metadata about where the data came from and its confidence level.

    Attributes:
        depth: Liquidity depth in USD. For concentrated liquidity pools (V3),
            this represents the available liquidity within the current tick range.
            For constant product pools (V2), this represents total pool TVL.
        source_info: Metadata about data source and confidence.

    Example:
        result = LiquidityResult(
            depth=Decimal("50000000"),
            source_info=DataSourceInfo(
                source="uniswap_v3_subgraph",
                confidence=DataConfidence.HIGH,
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            ),
        )
    """

    depth: Decimal
    source_info: DataSourceInfo


__all__ = [
    "DataConfidence",
    "DataSourceInfo",
    "VolumeResult",
    "FundingResult",
    "APYResult",
    "LiquidityResult",
]
