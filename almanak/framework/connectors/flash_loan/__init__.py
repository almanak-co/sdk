"""Flash Loan Module.

This module provides flash loan adapters and the FlashLoanSelector for
automatic provider selection based on liquidity, fees, and token availability.

Supported providers:
- Aave V3: Most widely supported, 0.09% fee
- Balancer: Zero fees, limited token availability

Example:
    from almanak.framework.connectors.flash_loan import FlashLoanSelector

    selector = FlashLoanSelector(chain="arbitrum")

    # Select best provider for USDC flash loan
    result = selector.select_provider(
        token="USDC",
        amount=Decimal("1000000"),
    )

    print(f"Selected: {result.provider}, fee: {result.fee_amount}")
"""

from .selector import (
    DEFAULT_PROVIDER_RELIABILITY,
    FlashLoanProviderInfo,
    FlashLoanSelectionResult,
    FlashLoanSelector,
    FlashLoanSelectorError,
    NoProviderAvailableError,
    SelectionPriority,
)

__all__ = [
    # Main class
    "FlashLoanSelector",
    # Data classes
    "FlashLoanProviderInfo",
    "FlashLoanSelectionResult",
    # Enums
    "SelectionPriority",
    # Exceptions
    "FlashLoanSelectorError",
    "NoProviderAvailableError",
    # Constants
    "DEFAULT_PROVIDER_RELIABILITY",
]
