"""Balancer Flash Loan Connector.

This module provides an adapter for executing flash loans via Balancer's Vault contract.
Balancer flash loans have zero fees (no premium), making them ideal for arbitrage.

Balancer Vault features:
- Zero-fee flash loans (no premium)
- Single or multi-token flash loans
- All supported tokens available from liquidity pools
- Simple interface: flashLoan(recipient, tokens, amounts, userData)

Supported chains:
- Ethereum
- Arbitrum
- Optimism
- Polygon
- Base
- Avalanche

Example:
    from almanak.connectors.balancer_v2 import BalancerFlashLoanAdapter, BalancerFlashLoanConfig

    config = BalancerFlashLoanConfig(
        chain="arbitrum",
        wallet_address="0x...",
    )
    adapter = BalancerFlashLoanAdapter(config)

    # Execute flash loan
    result = adapter.flash_loan(
        recipient="0x...",
        tokens=["USDC"],
        amounts=[Decimal("100000")],
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        BALANCER_FLASH_LOAN_SELECTOR,
        BALANCER_VAULT_ADDRESSES,
        DEFAULT_GAS_ESTIMATES,
        BalancerFlashLoanAdapter,
        BalancerFlashLoanConfig,
        BalancerFlashLoanParams,
        TransactionResult,
    )

__all__ = [
    "BALANCER_FLASH_LOAN_SELECTOR",
    "BALANCER_VAULT_ADDRESSES",
    "BalancerFlashLoanAdapter",
    "BalancerFlashLoanConfig",
    "BalancerFlashLoanParams",
    "DEFAULT_GAS_ESTIMATES",
    "TransactionResult",
]

_LAZY: dict[str, tuple[str, str]] = {
    "BALANCER_FLASH_LOAN_SELECTOR": (".adapter", "BALANCER_FLASH_LOAN_SELECTOR"),
    "BALANCER_VAULT_ADDRESSES": (".adapter", "BALANCER_VAULT_ADDRESSES"),
    "BalancerFlashLoanAdapter": (".adapter", "BalancerFlashLoanAdapter"),
    "BalancerFlashLoanConfig": (".adapter", "BalancerFlashLoanConfig"),
    "BalancerFlashLoanParams": (".adapter", "BalancerFlashLoanParams"),
    "DEFAULT_GAS_ESTIMATES": (".adapter", "DEFAULT_GAS_ESTIMATES"),
    "TransactionResult": (".adapter", "TransactionResult"),
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
