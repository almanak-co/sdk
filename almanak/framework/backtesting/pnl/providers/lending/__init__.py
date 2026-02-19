"""Lending protocol historical data providers.

This module provides historical data providers for lending protocols
including supply and borrow APY data for accurate interest calculations in backtesting.

Available Providers:
    - AaveV3APYProvider: Aave V3 historical APY provider (multi-chain)
    - CompoundV3APYProvider: Compound V3 historical APY provider (multi-chain)
    - MorphoBlueAPYProvider: Morpho Blue historical APY provider (Ethereum, Base)
    - SparkAPYProvider: Spark historical APY provider (Ethereum)

Example:
    from almanak.framework.backtesting.pnl.providers.lending import (
        AaveV3APYProvider,
        CompoundV3APYProvider,
        MorphoBlueAPYProvider,
        SparkAPYProvider,
    )
    from almanak.core.enums import Chain
    from datetime import datetime, UTC

    # Aave V3 historical APY
    async with AaveV3APYProvider() as provider:
        apys = await provider.get_apy(
            protocol="aave_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 31, tzinfo=UTC),
        )

    # Compound V3 historical APY
    async with CompoundV3APYProvider() as provider:
        apys = await provider.get_apy(
            protocol="compound_v3",
            market="USDC",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 31, tzinfo=UTC),
        )

    # Morpho Blue historical APY
    async with MorphoBlueAPYProvider() as provider:
        apys = await provider.get_apy(
            protocol="morpho_blue",
            market="0x...",  # market unique key
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 31, tzinfo=UTC),
        )

    # Spark historical APY
    async with SparkAPYProvider() as provider:
        apys = await provider.get_apy(
            protocol="spark",
            market="DAI",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 31, tzinfo=UTC),
        )
"""

from .aave_v3_apy import (
    AAVE_V3_SUBGRAPH_IDS,
    AaveV3APYProvider,
)
from .aave_v3_apy import (
    DATA_SOURCE as AAVE_V3_DATA_SOURCE,
)
from .aave_v3_apy import (
    SUPPORTED_CHAINS as AAVE_V3_SUPPORTED_CHAINS,
)
from .compound_v3_apy import (
    COMPOUND_V3_SUBGRAPH_IDS,
    CompoundV3APYProvider,
)
from .compound_v3_apy import (
    DATA_SOURCE as COMPOUND_V3_DATA_SOURCE,
)
from .compound_v3_apy import (
    SUPPORTED_CHAINS as COMPOUND_V3_SUPPORTED_CHAINS,
)
from .morpho_apy import (
    DATA_SOURCE as MORPHO_BLUE_DATA_SOURCE,
)
from .morpho_apy import (
    MORPHO_BLUE_SUBGRAPH_IDS,
    MorphoBlueAPYProvider,
)
from .morpho_apy import (
    SUPPORTED_CHAINS as MORPHO_BLUE_SUPPORTED_CHAINS,
)
from .spark_apy import (
    DATA_SOURCE as SPARK_DATA_SOURCE,
)
from .spark_apy import (
    SPARK_SUBGRAPH_IDS,
    SparkAPYProvider,
)
from .spark_apy import (
    SUPPORTED_CHAINS as SPARK_SUPPORTED_CHAINS,
)

__all__ = [
    # Aave V3 Provider
    "AaveV3APYProvider",
    "AAVE_V3_SUBGRAPH_IDS",
    "AAVE_V3_SUPPORTED_CHAINS",
    "AAVE_V3_DATA_SOURCE",
    # Compound V3 Provider
    "CompoundV3APYProvider",
    "COMPOUND_V3_SUBGRAPH_IDS",
    "COMPOUND_V3_SUPPORTED_CHAINS",
    "COMPOUND_V3_DATA_SOURCE",
    # Morpho Blue Provider
    "MorphoBlueAPYProvider",
    "MORPHO_BLUE_SUBGRAPH_IDS",
    "MORPHO_BLUE_SUPPORTED_CHAINS",
    "MORPHO_BLUE_DATA_SOURCE",
    # Spark Provider
    "SparkAPYProvider",
    "SPARK_SUBGRAPH_IDS",
    "SPARK_SUPPORTED_CHAINS",
    "SPARK_DATA_SOURCE",
]
