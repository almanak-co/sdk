"""Ethena Connector.

This module provides an adapter for interacting with Ethena synthetic dollar protocol.

Ethena is a synthetic dollar protocol supporting:
- Stake USDe to receive sUSDe (yield-bearing)
- Unstake sUSDe to receive USDe (with cooldown period)

Supported chains:
- Ethereum (full staking + unstaking)

sUSDe is an ERC4626 vault token that accrues yield from delta-neutral strategies.

Example:
    from almanak.connectors.ethena import EthenaAdapter, EthenaConfig

    config = EthenaConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = EthenaAdapter(config)

    # Stake USDe to receive sUSDe
    result = adapter.stake_usde(amount=Decimal("1000.0"))
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        DEFAULT_GAS_ESTIMATES,
        ETHENA_ADDRESSES,
        ETHENA_COOLDOWN_ASSETS_SELECTOR,
        ETHENA_COOLDOWN_SHARES_SELECTOR,
        ETHENA_DEPOSIT_SELECTOR,
        ETHENA_UNSTAKE_SELECTOR,
        EthenaAdapter,
        EthenaConfig,
        TransactionResult,
    )
    from .receipt_parser import (
        ETHENA_EVENT_SIGNATURES,
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        EthenaEventType,
        EthenaReceiptParser,
        ParseResult,
        StakeEventData,
        UnstakeEventData,
        WithdrawEventData,
    )

__all__ = [
    "DEFAULT_GAS_ESTIMATES",
    "ETHENA_ADDRESSES",
    "ETHENA_COOLDOWN_ASSETS_SELECTOR",
    "ETHENA_COOLDOWN_SHARES_SELECTOR",
    "ETHENA_DEPOSIT_SELECTOR",
    "ETHENA_EVENT_SIGNATURES",
    "ETHENA_UNSTAKE_SELECTOR",
    "EVENT_TOPICS",
    "EthenaAdapter",
    "EthenaConfig",
    "EthenaEventType",
    "EthenaReceiptParser",
    "ParseResult",
    "StakeEventData",
    "TOPIC_TO_EVENT",
    "TransactionResult",
    "UnstakeEventData",
    "WithdrawEventData",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DEFAULT_GAS_ESTIMATES": (".adapter", "DEFAULT_GAS_ESTIMATES"),
    "ETHENA_ADDRESSES": (".adapter", "ETHENA_ADDRESSES"),
    "ETHENA_COOLDOWN_ASSETS_SELECTOR": (".adapter", "ETHENA_COOLDOWN_ASSETS_SELECTOR"),
    "ETHENA_COOLDOWN_SHARES_SELECTOR": (".adapter", "ETHENA_COOLDOWN_SHARES_SELECTOR"),
    "ETHENA_DEPOSIT_SELECTOR": (".adapter", "ETHENA_DEPOSIT_SELECTOR"),
    "ETHENA_EVENT_SIGNATURES": (".receipt_parser", "ETHENA_EVENT_SIGNATURES"),
    "ETHENA_UNSTAKE_SELECTOR": (".adapter", "ETHENA_UNSTAKE_SELECTOR"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "EthenaAdapter": (".adapter", "EthenaAdapter"),
    "EthenaConfig": (".adapter", "EthenaConfig"),
    "EthenaEventType": (".receipt_parser", "EthenaEventType"),
    "EthenaReceiptParser": (".receipt_parser", "EthenaReceiptParser"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "StakeEventData": (".receipt_parser", "StakeEventData"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionResult": (".adapter", "TransactionResult"),
    "UnstakeEventData": (".receipt_parser", "UnstakeEventData"),
    "WithdrawEventData": (".receipt_parser", "WithdrawEventData"),
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

    register_connector(name="ethena", intents=(IntentType.STAKE, IntentType.UNSTAKE), chains=("ethereum",))
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
