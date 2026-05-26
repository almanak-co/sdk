"""Compound V3 (Comet) Connector.

This module provides adapters and utilities for interacting with Compound V3,
a lending protocol with single borrowable assets and multiple collateral options.

Compound V3 Features:
- Single borrowable asset (base) per market (USDC, WETH, USDT)
- Multiple collateral assets per market
- No cTokens for collateral (only base asset is tokenized)
- Simplified interest rate model
- Efficient liquidation mechanism

Supported Chains:
- Ethereum
- Arbitrum
- Base
- Optimism
- Polygon

Example:
    from almanak.connectors.compound_v3 import (
        CompoundV3Adapter,
        CompoundV3Config,
        CompoundV3ReceiptParser,
    )

    # Initialize adapter
    config = CompoundV3Config(
        chain="ethereum",
        wallet_address="0x...",
        market="usdc",
    )
    adapter = CompoundV3Adapter(config)

    # Get market info
    market_info = adapter.get_market_info()
    print(f"Market: {market_info.name}")

    # Build a supply transaction
    result = adapter.supply(
        amount=Decimal("1000"),
    )

    # Parse transaction receipts
    parser = CompoundV3ReceiptParser()
    events = parser.parse_receipt(receipt)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        COMPOUND_V3_ABSORB_SELECTOR,
        COMPOUND_V3_BUY_COLLATERAL_SELECTOR,
        COMPOUND_V3_COMET_ADDRESSES,
        COMPOUND_V3_MARKETS,
        COMPOUND_V3_SUPPLY_FROM_SELECTOR,
        COMPOUND_V3_SUPPLY_SELECTOR,
        COMPOUND_V3_SUPPLY_TO_SELECTOR,
        COMPOUND_V3_WITHDRAW_FROM_SELECTOR,
        COMPOUND_V3_WITHDRAW_SELECTOR,
        COMPOUND_V3_WITHDRAW_TO_SELECTOR,
        DEFAULT_GAS_ESTIMATES,
        ERC20_APPROVE_SELECTOR,
        MAX_UINT256,
        CompoundV3Adapter,
        CompoundV3Config,
        CompoundV3HealthFactor,
        CompoundV3MarketInfo,
        CompoundV3Position,
        TransactionResult,
    )
    from .receipt_parser import (
        EVENT_NAME_TO_TYPE,
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        CompoundV3Event,
        CompoundV3EventType,
        CompoundV3ReceiptParser,
        ParseResult,
    )

__all__ = [
    "COMPOUND_V3_ABSORB_SELECTOR",
    "COMPOUND_V3_BUY_COLLATERAL_SELECTOR",
    "COMPOUND_V3_COMET_ADDRESSES",
    "COMPOUND_V3_MARKETS",
    "COMPOUND_V3_SUPPLY_FROM_SELECTOR",
    "COMPOUND_V3_SUPPLY_SELECTOR",
    "COMPOUND_V3_SUPPLY_TO_SELECTOR",
    "COMPOUND_V3_WITHDRAW_FROM_SELECTOR",
    "COMPOUND_V3_WITHDRAW_SELECTOR",
    "COMPOUND_V3_WITHDRAW_TO_SELECTOR",
    "CompoundV3Adapter",
    "CompoundV3Config",
    "CompoundV3Event",
    "CompoundV3EventType",
    "CompoundV3HealthFactor",
    "CompoundV3MarketInfo",
    "CompoundV3Position",
    "CompoundV3ReceiptParser",
    "DEFAULT_GAS_ESTIMATES",
    "ERC20_APPROVE_SELECTOR",
    "EVENT_NAME_TO_TYPE",
    "EVENT_TOPICS",
    "MAX_UINT256",
    "ParseResult",
    "TOPIC_TO_EVENT",
    "TransactionResult",
]

_LAZY: dict[str, tuple[str, str]] = {
    "COMPOUND_V3_ABSORB_SELECTOR": (".adapter", "COMPOUND_V3_ABSORB_SELECTOR"),
    "COMPOUND_V3_BUY_COLLATERAL_SELECTOR": (".adapter", "COMPOUND_V3_BUY_COLLATERAL_SELECTOR"),
    "COMPOUND_V3_COMET_ADDRESSES": (".adapter", "COMPOUND_V3_COMET_ADDRESSES"),
    "COMPOUND_V3_MARKETS": (".adapter", "COMPOUND_V3_MARKETS"),
    "COMPOUND_V3_SUPPLY_FROM_SELECTOR": (".adapter", "COMPOUND_V3_SUPPLY_FROM_SELECTOR"),
    "COMPOUND_V3_SUPPLY_SELECTOR": (".adapter", "COMPOUND_V3_SUPPLY_SELECTOR"),
    "COMPOUND_V3_SUPPLY_TO_SELECTOR": (".adapter", "COMPOUND_V3_SUPPLY_TO_SELECTOR"),
    "COMPOUND_V3_WITHDRAW_FROM_SELECTOR": (".adapter", "COMPOUND_V3_WITHDRAW_FROM_SELECTOR"),
    "COMPOUND_V3_WITHDRAW_SELECTOR": (".adapter", "COMPOUND_V3_WITHDRAW_SELECTOR"),
    "COMPOUND_V3_WITHDRAW_TO_SELECTOR": (".adapter", "COMPOUND_V3_WITHDRAW_TO_SELECTOR"),
    "CompoundV3Adapter": (".adapter", "CompoundV3Adapter"),
    "CompoundV3Config": (".adapter", "CompoundV3Config"),
    "CompoundV3Event": (".receipt_parser", "CompoundV3Event"),
    "CompoundV3EventType": (".receipt_parser", "CompoundV3EventType"),
    "CompoundV3HealthFactor": (".adapter", "CompoundV3HealthFactor"),
    "CompoundV3MarketInfo": (".adapter", "CompoundV3MarketInfo"),
    "CompoundV3Position": (".adapter", "CompoundV3Position"),
    "CompoundV3ReceiptParser": (".receipt_parser", "CompoundV3ReceiptParser"),
    "DEFAULT_GAS_ESTIMATES": (".adapter", "DEFAULT_GAS_ESTIMATES"),
    "ERC20_APPROVE_SELECTOR": (".adapter", "ERC20_APPROVE_SELECTOR"),
    "EVENT_NAME_TO_TYPE": (".receipt_parser", "EVENT_NAME_TO_TYPE"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "MAX_UINT256": (".adapter", "MAX_UINT256"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionResult": (".adapter", "TransactionResult"),
}

_registered = False


def _register_once() -> None:
    """Fire ``register_connector`` once on first strategy-side access.

    Deferred so importing the connector's gateway-side surface during
    gateway boot does not pull ``framework.intents.vocabulary`` into the
    partially-initialised config-init chain (VIB-4835).
    """
    global _registered
    if _registered:
        return
    _registered = True
    try:
        from almanak.connectors._strategy_base.registry import register_connector
        from almanak.framework.intents.vocabulary import IntentType

        register_connector(
            name="compound_v3",
            intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW),
            chains=("ethereum", "arbitrum", "base", "optimism", "polygon"),
        )
    except Exception:
        _registered = False
        raise


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
