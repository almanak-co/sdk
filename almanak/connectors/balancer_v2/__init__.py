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
        from almanak.connectors._strategy_base.registry import MatrixEntry, register_connector
        from almanak.framework.intents.vocabulary import IntentType

        from .adapter import BALANCER_VAULT_ADDRESSES

        register_connector(
            # Renamed during VIB-4835 Phase 2 — see the rename commit message.
            # Strategies that referenced ``balancer`` need to update; coverage
            # gate entries under ``scripts/ci/intent-coverage-excused.yml``
            # follow the new name too.
            name="balancer_v2",
            intents=(IntentType.FLASH_LOAN,),
            chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche"),
            # Matrix output is owned by the connector (VIB-4856 / W4).
            # The Vault is the canonical flash-loan venue; the matrix has
            # historically rendered the row under the bare ``"balancer"``
            # name (not ``"balancer_v2"``) because that's the only Balancer
            # surface advertised. Chains come from ``BALANCER_VAULT_ADDRESSES``,
            # the connector-owned source of truth.
            matrix_entries=(
                MatrixEntry(
                    matrix_name="balancer",
                    category="flash_loan",
                    chains=frozenset(BALANCER_VAULT_ADDRESSES.keys()),
                ),
            ),
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
