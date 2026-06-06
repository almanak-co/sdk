"""Lido Connector.

This module provides an adapter for interacting with Lido liquid staking protocol.

Lido is a decentralized liquid staking protocol supporting:
- Stake ETH to receive stETH
- Wrap stETH to wstETH (non-rebasing)
- Unwrap wstETH to stETH

Supported chains:
- Ethereum (full staking + wrap/unwrap)
- Arbitrum, Optimism, Polygon (wstETH only)

Example:
    from almanak.connectors.lido import LidoAdapter, LidoConfig

    config = LidoConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = LidoAdapter(config)

    # Stake ETH to receive stETH
    result = adapter.stake(amount=Decimal("1.0"))

    # Wrap stETH to wstETH
    result = adapter.wrap(amount=Decimal("1.0"))
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        DEFAULT_GAS_ESTIMATES,
        LIDO_ADDRESSES,
        LIDO_STAKE_SELECTOR,
        LIDO_UNWRAP_SELECTOR,
        LIDO_WRAP_SELECTOR,
        LidoAdapter,
        LidoConfig,
        TransactionResult,
    )
    from .receipt_parser import (
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        LidoEventType,
        LidoReceiptParser,
        ParseResult,
        StakeEventData,
        UnwrapEventData,
        WithdrawalClaimedEventData,
        WithdrawalRequestedEventData,
        WrapEventData,
    )

__all__ = [
    "DEFAULT_GAS_ESTIMATES",
    "EVENT_TOPICS",
    "LIDO_ADDRESSES",
    "LIDO_STAKE_SELECTOR",
    "LIDO_UNWRAP_SELECTOR",
    "LIDO_WRAP_SELECTOR",
    "LidoAdapter",
    "LidoConfig",
    "LidoEventType",
    "LidoReceiptParser",
    "ParseResult",
    "StakeEventData",
    "TOPIC_TO_EVENT",
    "TransactionResult",
    "UnwrapEventData",
    "WithdrawalClaimedEventData",
    "WithdrawalRequestedEventData",
    "WrapEventData",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DEFAULT_GAS_ESTIMATES": (".adapter", "DEFAULT_GAS_ESTIMATES"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "LIDO_ADDRESSES": (".adapter", "LIDO_ADDRESSES"),
    "LIDO_STAKE_SELECTOR": (".adapter", "LIDO_STAKE_SELECTOR"),
    "LIDO_UNWRAP_SELECTOR": (".adapter", "LIDO_UNWRAP_SELECTOR"),
    "LIDO_WRAP_SELECTOR": (".adapter", "LIDO_WRAP_SELECTOR"),
    "LidoAdapter": (".adapter", "LidoAdapter"),
    "LidoConfig": (".adapter", "LidoConfig"),
    "LidoEventType": (".receipt_parser", "LidoEventType"),
    "LidoReceiptParser": (".receipt_parser", "LidoReceiptParser"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "StakeEventData": (".receipt_parser", "StakeEventData"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionResult": (".adapter", "TransactionResult"),
    "UnwrapEventData": (".receipt_parser", "UnwrapEventData"),
    "WithdrawalClaimedEventData": (".receipt_parser", "WithdrawalClaimedEventData"),
    "WithdrawalRequestedEventData": (".receipt_parser", "WithdrawalRequestedEventData"),
    "WrapEventData": (".receipt_parser", "WrapEventData"),
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
