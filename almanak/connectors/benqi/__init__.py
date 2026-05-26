"""BENQI Lending Connector (Compound V2 fork on Avalanche).

This module provides adapters and utilities for interacting with BENQI,
a leading lending/borrowing protocol on Avalanche using the qiToken architecture.

BENQI Features:
- Compound V2-style qiToken model
- Supply to earn yield (mint qiTokens)
- Borrow against collateral (enterMarkets + borrow)
- Multiple asset markets: AVAX, USDC, USDT, WETH.e, BTC.b, sAVAX

Supported Chains:
- Avalanche

Example:
    from almanak.connectors.benqi import (
        BenqiAdapter,
        BenqiConfig,
        BenqiReceiptParser,
    )

    config = BenqiConfig(
        chain="avalanche",
        wallet_address="0x...",
    )
    adapter = BenqiAdapter(config)

    # Supply USDC
    result = adapter.supply(asset="USDC", amount=Decimal("1000"))

    # Parse receipts
    parser = BenqiReceiptParser(underlying_decimals=6)
    events = parser.parse_receipt(receipt)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        BENQI_BORROW_SELECTOR,
        BENQI_COMPTROLLER_ADDRESS,
        BENQI_ENTER_MARKETS_SELECTOR,
        BENQI_EXIT_MARKET_SELECTOR,
        BENQI_MINT_NATIVE_SELECTOR,
        BENQI_MINT_SELECTOR,
        BENQI_QI_TOKENS,
        BENQI_REDEEM_SELECTOR,
        BENQI_REDEEM_UNDERLYING_SELECTOR,
        BENQI_REPAY_BORROW_NATIVE_SELECTOR,
        BENQI_REPAY_BORROW_SELECTOR,
        DEFAULT_GAS_ESTIMATES,
        ERC20_APPROVE_SELECTOR,
        MAX_UINT256,
        BenqiAdapter,
        BenqiConfig,
        BenqiMarketInfo,
        BenqiPosition,
        TransactionResult,
    )
    from .receipt_parser import (
        EVENT_NAME_TO_TYPE,
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        BenqiEvent,
        BenqiEventType,
        BenqiReceiptParser,
        ParseResult,
    )

__all__ = [
    "BENQI_BORROW_SELECTOR",
    "BENQI_COMPTROLLER_ADDRESS",
    "BENQI_ENTER_MARKETS_SELECTOR",
    "BENQI_EXIT_MARKET_SELECTOR",
    "BENQI_MINT_NATIVE_SELECTOR",
    "BENQI_MINT_SELECTOR",
    "BENQI_QI_TOKENS",
    "BENQI_REDEEM_SELECTOR",
    "BENQI_REDEEM_UNDERLYING_SELECTOR",
    "BENQI_REPAY_BORROW_NATIVE_SELECTOR",
    "BENQI_REPAY_BORROW_SELECTOR",
    "BenqiAdapter",
    "BenqiConfig",
    "BenqiEvent",
    "BenqiEventType",
    "BenqiMarketInfo",
    "BenqiPosition",
    "BenqiReceiptParser",
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
    "BENQI_BORROW_SELECTOR": (".adapter", "BENQI_BORROW_SELECTOR"),
    "BENQI_COMPTROLLER_ADDRESS": (".adapter", "BENQI_COMPTROLLER_ADDRESS"),
    "BENQI_ENTER_MARKETS_SELECTOR": (".adapter", "BENQI_ENTER_MARKETS_SELECTOR"),
    "BENQI_EXIT_MARKET_SELECTOR": (".adapter", "BENQI_EXIT_MARKET_SELECTOR"),
    "BENQI_MINT_NATIVE_SELECTOR": (".adapter", "BENQI_MINT_NATIVE_SELECTOR"),
    "BENQI_MINT_SELECTOR": (".adapter", "BENQI_MINT_SELECTOR"),
    "BENQI_QI_TOKENS": (".adapter", "BENQI_QI_TOKENS"),
    "BENQI_REDEEM_SELECTOR": (".adapter", "BENQI_REDEEM_SELECTOR"),
    "BENQI_REDEEM_UNDERLYING_SELECTOR": (".adapter", "BENQI_REDEEM_UNDERLYING_SELECTOR"),
    "BENQI_REPAY_BORROW_NATIVE_SELECTOR": (".adapter", "BENQI_REPAY_BORROW_NATIVE_SELECTOR"),
    "BENQI_REPAY_BORROW_SELECTOR": (".adapter", "BENQI_REPAY_BORROW_SELECTOR"),
    "BenqiAdapter": (".adapter", "BenqiAdapter"),
    "BenqiConfig": (".adapter", "BenqiConfig"),
    "BenqiEvent": (".receipt_parser", "BenqiEvent"),
    "BenqiEventType": (".receipt_parser", "BenqiEventType"),
    "BenqiMarketInfo": (".adapter", "BenqiMarketInfo"),
    "BenqiPosition": (".adapter", "BenqiPosition"),
    "BenqiReceiptParser": (".receipt_parser", "BenqiReceiptParser"),
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
    from almanak.connectors._strategy_base.registry import register_connector
    from almanak.framework.intents.vocabulary import IntentType

    register_connector(
        name="benqi",
        intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW),
        chains=("avalanche",),
    )
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
