"""DeFi Data Module.

This module provides data structures and interfaces for DeFi-specific data,
including gas prices, DEX pool reserves, and lending protocol data.

Key Components:
    - GasPrice: Dataclass for gas price data with L1/L2 support
    - GasOracle: Protocol for gas price providers
    - Web3GasOracle: Implementation that fetches gas prices from RPC
    - PoolReserves: Dataclass for DEX pool reserve data
    - DexType: Literal type for supported DEX protocols
"""

from almanak.framework.data.defi.gas import (
    L2_CHAINS,
    L2_GAS_ORACLE_ADDRESSES,
    STANDARD_GAS_UNITS,
    GasOracle,
    GasPrice,
    Web3GasOracle,
)
from almanak.framework.data.defi.pools import (
    UNISWAP_V3_POOL_ABI,
    VALID_DEX_TYPES,
    DexType,
    PoolReserves,
    UniswapV3PoolReader,
)

__all__ = [
    # Gas
    "GasPrice",
    "GasOracle",
    "Web3GasOracle",
    "L2_GAS_ORACLE_ADDRESSES",
    "L2_CHAINS",
    "STANDARD_GAS_UNITS",
    # Pools
    "DexType",
    "PoolReserves",
    "VALID_DEX_TYPES",
    "UNISWAP_V3_POOL_ABI",
    "UniswapV3PoolReader",
]
