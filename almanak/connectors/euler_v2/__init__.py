"""Euler V2 lending protocol connector for Avalanche.

Euler V2 uses ERC-4626 vaults with the Ethereum Vault Connector (EVC)
for cross-vault collateral/borrow relationships.

Supported operations: SUPPLY, WITHDRAW, BORROW, REPAY
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        EULER_V2_VAULTS,
        EVAULT_FACTORY_ADDRESS,
        EVC_ADDRESS,
        MAX_UINT256,
        VAULT_LENS_ADDRESS,
        EulerV2Adapter,
        EulerV2Config,
        EulerV2VaultInfo,
        TransactionResult,
    )
    from .receipt_parser import (
        BORROW_TOPIC,
        DEPOSIT_TOPIC,
        REPAY_TOPIC,
        WITHDRAW_TOPIC,
        EulerV2ParseResult,
        EulerV2ReceiptParser,
    )

__all__ = [
    "BORROW_TOPIC",
    "DEPOSIT_TOPIC",
    "EULER_V2_VAULTS",
    "EVAULT_FACTORY_ADDRESS",
    "EVC_ADDRESS",
    "EulerV2Adapter",
    "EulerV2Config",
    "EulerV2ParseResult",
    "EulerV2ReceiptParser",
    "EulerV2VaultInfo",
    "MAX_UINT256",
    "REPAY_TOPIC",
    "TransactionResult",
    "VAULT_LENS_ADDRESS",
    "WITHDRAW_TOPIC",
]

_LAZY: dict[str, tuple[str, str]] = {
    "BORROW_TOPIC": (".receipt_parser", "BORROW_TOPIC"),
    "DEPOSIT_TOPIC": (".receipt_parser", "DEPOSIT_TOPIC"),
    "EULER_V2_VAULTS": (".adapter", "EULER_V2_VAULTS"),
    "EVAULT_FACTORY_ADDRESS": (".adapter", "EVAULT_FACTORY_ADDRESS"),
    "EVC_ADDRESS": (".adapter", "EVC_ADDRESS"),
    "EulerV2Adapter": (".adapter", "EulerV2Adapter"),
    "EulerV2Config": (".adapter", "EulerV2Config"),
    "EulerV2ParseResult": (".receipt_parser", "EulerV2ParseResult"),
    "EulerV2ReceiptParser": (".receipt_parser", "EulerV2ReceiptParser"),
    "EulerV2VaultInfo": (".adapter", "EulerV2VaultInfo"),
    "MAX_UINT256": (".adapter", "MAX_UINT256"),
    "REPAY_TOPIC": (".receipt_parser", "REPAY_TOPIC"),
    "TransactionResult": (".adapter", "TransactionResult"),
    "VAULT_LENS_ADDRESS": (".adapter", "VAULT_LENS_ADDRESS"),
    "WITHDRAW_TOPIC": (".receipt_parser", "WITHDRAW_TOPIC"),
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
            name="euler_v2",
            intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW),
            chains=("ethereum", "avalanche"),
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
