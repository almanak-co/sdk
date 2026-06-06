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
    from almanak.connectors.gmx_v2 import GMXv2Adapter, GMXv2Config

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

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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
    from .market_rules import (
        get_allowed_collaterals,
        is_market_registered,
        registered_markets,
        validate_collateral,
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
    "DEFAULT_EXECUTION_FEE",
    "DecreasePositionSwapType",
    "GMXV2OrderParams",
    "GMXV2SDK",
    "GMXV2TransactionData",
    "GMX_V2_ADDRESSES",
    "GMX_V2_GAS_ESTIMATES",
    "GMX_V2_MARKETS",
    "GMX_V2_SDK_ADDRESSES",
    "GMXv2Adapter",
    "GMXv2Config",
    "GMXv2Event",
    "GMXv2EventType",
    "GMXv2Order",
    "GMXv2OrderType",
    "GMXv2Position",
    "GMXv2PositionSide",
    "GMXv2ReceiptParser",
    "OrderType",
    "get_allowed_collaterals",
    "get_gmx_v2_sdk",
    "is_market_registered",
    "registered_markets",
    "validate_collateral",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DEFAULT_EXECUTION_FEE": (".adapter", "DEFAULT_EXECUTION_FEE"),
    "DecreasePositionSwapType": (".sdk", "DecreasePositionSwapType"),
    "GMXV2OrderParams": (".sdk", "GMXV2OrderParams"),
    "GMXV2SDK": (".sdk", "GMXV2SDK"),
    "GMXV2TransactionData": (".sdk", "GMXV2TransactionData"),
    "GMX_V2_ADDRESSES": (".adapter", "GMX_V2_ADDRESSES"),
    "GMX_V2_GAS_ESTIMATES": (".adapter", "GMX_V2_GAS_ESTIMATES"),
    "GMX_V2_MARKETS": (".adapter", "GMX_V2_MARKETS"),
    "GMX_V2_SDK_ADDRESSES": (".sdk", "GMX_V2_SDK_ADDRESSES"),
    "GMXv2Adapter": (".adapter", "GMXv2Adapter"),
    "GMXv2Config": (".adapter", "GMXv2Config"),
    "GMXv2Event": (".receipt_parser", "GMXv2Event"),
    "GMXv2EventType": (".receipt_parser", "GMXv2EventType"),
    "GMXv2Order": (".adapter", "GMXv2Order"),
    "GMXv2OrderType": (".adapter", "GMXv2OrderType"),
    "GMXv2Position": (".adapter", "GMXv2Position"),
    "GMXv2PositionSide": (".adapter", "GMXv2PositionSide"),
    "GMXv2ReceiptParser": (".receipt_parser", "GMXv2ReceiptParser"),
    "OrderType": (".sdk", "OrderType"),
    "get_allowed_collaterals": (".market_rules", "get_allowed_collaterals"),
    "get_gmx_v2_sdk": (".sdk", "get_gmx_v2_sdk"),
    "is_market_registered": (".market_rules", "is_market_registered"),
    "registered_markets": (".market_rules", "registered_markets"),
    "validate_collateral": (".market_rules", "validate_collateral"),
}

_registered = False


def _register_once() -> None:
    """Compatibility no-op; strategy registration lives in connector.py."""
    global _registered
    if _registered:
        return
    _registered = True


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
