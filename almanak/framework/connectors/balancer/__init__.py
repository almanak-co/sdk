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

Example:
    from almanak.framework.connectors.balancer import BalancerFlashLoanAdapter, BalancerFlashLoanConfig

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

from .adapter import (
    BALANCER_FLASH_LOAN_SELECTOR,
    # Constants
    BALANCER_VAULT_ADDRESSES,
    DEFAULT_GAS_ESTIMATES,
    # Adapter
    BalancerFlashLoanAdapter,
    BalancerFlashLoanConfig,
    # Data classes
    BalancerFlashLoanParams,
    TransactionResult,
)

__all__ = [
    # Adapter
    "BalancerFlashLoanAdapter",
    "BalancerFlashLoanConfig",
    # Data classes
    "BalancerFlashLoanParams",
    "TransactionResult",
    # Constants
    "BALANCER_VAULT_ADDRESSES",
    "DEFAULT_GAS_ESTIMATES",
    "BALANCER_FLASH_LOAN_SELECTOR",
]
