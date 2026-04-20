"""GMX v2 Connector.

This module provides an adapter for interacting with GMX v2 perpetuals protocol,
supporting position management, order execution, and event parsing.

GMX v2 is a decentralized perpetual exchange supporting:
- Long and short positions with leverage
- Multiple collateral types
- Limit and market orders
- Position sizing and management

Supported chains:
- Arbitrum
- Avalanche

Example:
    from almanak.framework.connectors.gmx_v2 import GMXv2Adapter, GMXv2Config

    config = GMXv2Config(
        chain="arbitrum",
        wallet_address="0x...",
    )
    adapter = GMXv2Adapter(config)

    # Open a position
    result = adapter.open_position(
        market="ETH/USD",
        collateral_token="USDC",
        collateral_amount=Decimal("1000"),
        size_delta_usd=Decimal("5000"),
        is_long=True,
    )
"""

from almanak.core.contracts import GMX_V2_TOKENS

from .adapter import (
    DEFAULT_EXECUTION_FEE,
    GMX_V2_ADDRESSES,
    GMX_V2_GAS_ESTIMATES,
    GMX_V2_MARKETS,
    GMXv2Adapter,
    GMXv2Config,
    GMXv2Order,
    GMXv2OrderType,
    GMXv2Position,
    GMXv2PositionSide,
)
from .receipt_parser import (
    GMXv2Event,
    GMXv2EventType,
    GMXv2ReceiptParser,
)
from .sdk import (
    GMX_V2_SDK_ADDRESSES,
    GMXV2SDK,
    DecreasePositionSwapType,
    GMXV2OrderParams,
    GMXV2TransactionData,
    OrderType,
    get_gmx_v2_sdk,
)

__all__ = [
    # Adapter
    "GMXv2Adapter",
    "GMXv2Config",
    "GMXv2Position",
    "GMXv2Order",
    "GMXv2OrderType",
    "GMXv2PositionSide",
    # Receipt Parser
    "GMXv2ReceiptParser",
    "GMXv2Event",
    "GMXv2EventType",
    # Constants
    "GMX_V2_ADDRESSES",
    "GMX_V2_MARKETS",
    "GMX_V2_TOKENS",
    "GMX_V2_GAS_ESTIMATES",
    "DEFAULT_EXECUTION_FEE",
    # SDK
    "GMXV2SDK",
    "GMXV2OrderParams",
    "GMXV2TransactionData",
    "OrderType",
    "DecreasePositionSwapType",
    "GMX_V2_SDK_ADDRESSES",
    "get_gmx_v2_sdk",
]
