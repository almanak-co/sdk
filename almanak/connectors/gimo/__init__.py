"""Gimo Finance Connector — liquid staking on 0G Chain.

Gimo Finance is a liquid staking protocol on 0G Chain (AI L1) built on StaFi's
EVM LSD Stack. Users stake A0GI (0G's native token) and receive st0G, a
yield-bearing liquid staking derivative.

Architecture (StaFi EVM LSD Stack):
    - LsdToken (st0G): ERC-20 liquid staking derivative
    - StakeManager: Manages staking lifecycle and exchange rate
    - StakePool: Holds staked A0GI and distributes to validators

Available Operations:
    - stake(): Deposit A0GI -> receive st0G
    - unstake(): Request st0G -> A0GI withdrawal (22-day unbonding)
    - withdraw(): Claim A0GI after unbonding period

Reference:
    - StaFi EVM LSD Architecture: https://docs.stafi.io/lsaas/architecture_evm_lsd/
    - Gimo Finance Docs: https://docs.gimofinance.xyz/docs/
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        DEFAULT_GAS_ESTIMATES,
        GIMO_ADDRESSES,
        GIMO_STAKE_SELECTOR,
        GIMO_UNSTAKE_SELECTOR,
        GimoAdapter,
        GimoConfig,
        TransactionResult,
    )
    from .receipt_parser import (
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        GimoEventType,
        GimoReceiptParser,
        ParseResult,
        StakeEventData,
        UnstakeEventData,
    )

__all__ = [
    "DEFAULT_GAS_ESTIMATES",
    "EVENT_TOPICS",
    "GIMO_ADDRESSES",
    "GIMO_STAKE_SELECTOR",
    "GIMO_UNSTAKE_SELECTOR",
    "GimoAdapter",
    "GimoConfig",
    "GimoEventType",
    "GimoReceiptParser",
    "ParseResult",
    "StakeEventData",
    "TOPIC_TO_EVENT",
    "TransactionResult",
    "UnstakeEventData",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DEFAULT_GAS_ESTIMATES": (".adapter", "DEFAULT_GAS_ESTIMATES"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "GIMO_ADDRESSES": (".adapter", "GIMO_ADDRESSES"),
    "GIMO_STAKE_SELECTOR": (".adapter", "GIMO_STAKE_SELECTOR"),
    "GIMO_UNSTAKE_SELECTOR": (".adapter", "GIMO_UNSTAKE_SELECTOR"),
    "GimoAdapter": (".adapter", "GimoAdapter"),
    "GimoConfig": (".adapter", "GimoConfig"),
    "GimoEventType": (".receipt_parser", "GimoEventType"),
    "GimoReceiptParser": (".receipt_parser", "GimoReceiptParser"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "StakeEventData": (".receipt_parser", "StakeEventData"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionResult": (".adapter", "TransactionResult"),
    "UnstakeEventData": (".receipt_parser", "UnstakeEventData"),
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
