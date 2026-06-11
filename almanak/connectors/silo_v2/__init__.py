"""Silo V2 connector for Almanak SDK.

Silo V2 is an isolated lending protocol on Avalanche where each market consists
of exactly two assets paired together in separate ERC-4626 vaults.

Key concepts:
- Each market is a pair of two Silo vaults (silo0 + silo1)
- Depositing into one silo enables borrowing from the paired silo
- No shared pool — bad debt is isolated per market
- CollateralType: 0=Protected (non-borrowable), 1=Collateral (borrowable)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        MAX_UINT256,
        SILO_V2_FUNCTION_SELECTORS,
        SILO_V2_MARKETS,
        SiloV2Adapter,
        SiloV2Config,
        SiloV2MarketInfo,
        SiloV2Position,
        TransactionResult,
    )
    from .receipt_parser import SiloV2ReceiptParser

__all__ = [
    "MAX_UINT256",
    "SILO_V2_FUNCTION_SELECTORS",
    "SILO_V2_MARKETS",
    "SiloV2Adapter",
    "SiloV2Config",
    "SiloV2MarketInfo",
    "SiloV2Position",
    "SiloV2ReceiptParser",
    "TransactionResult",
]

_LAZY: dict[str, tuple[str, str]] = {
    "MAX_UINT256": (".adapter", "MAX_UINT256"),
    "SILO_V2_FUNCTION_SELECTORS": (".adapter", "SILO_V2_FUNCTION_SELECTORS"),
    "SILO_V2_MARKETS": (".adapter", "SILO_V2_MARKETS"),
    "SiloV2Adapter": (".adapter", "SiloV2Adapter"),
    "SiloV2Config": (".adapter", "SiloV2Config"),
    "SiloV2MarketInfo": (".adapter", "SiloV2MarketInfo"),
    "SiloV2Position": (".adapter", "SiloV2Position"),
    "SiloV2ReceiptParser": (".receipt_parser", "SiloV2ReceiptParser"),
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
