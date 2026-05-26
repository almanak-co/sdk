"""Flash Loan Module.

This module provides flash loan adapters and the FlashLoanSelector for
automatic provider selection based on liquidity, fees, and token availability.

Supported providers:
- Aave V3: Most widely supported, 0.09% fee
- Balancer: Zero fees, limited token availability

Example:
    from almanak.connectors.flash_loan import FlashLoanSelector

    selector = FlashLoanSelector(chain="arbitrum")

    # Select best provider for USDC flash loan
    result = selector.select_provider(
        token="USDC",
        amount=Decimal("1000000"),
    )

    print(f"Selected: {result.provider}, fee: {result.fee_amount}")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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
    "DEFAULT_PROVIDER_RELIABILITY",
    "FlashLoanProviderInfo",
    "FlashLoanSelectionResult",
    "FlashLoanSelector",
    "FlashLoanSelectorError",
    "NoProviderAvailableError",
    "SelectionPriority",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DEFAULT_PROVIDER_RELIABILITY": (".selector", "DEFAULT_PROVIDER_RELIABILITY"),
    "FlashLoanProviderInfo": (".selector", "FlashLoanProviderInfo"),
    "FlashLoanSelectionResult": (".selector", "FlashLoanSelectionResult"),
    "FlashLoanSelector": (".selector", "FlashLoanSelector"),
    "FlashLoanSelectorError": (".selector", "FlashLoanSelectorError"),
    "NoProviderAvailableError": (".selector", "NoProviderAvailableError"),
    "SelectionPriority": (".selector", "SelectionPriority"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    return value
