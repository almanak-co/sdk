"""Silo V2 connector for Almanak SDK.

Silo V2 is an isolated lending protocol on Avalanche where each market consists
of exactly two assets paired together in separate ERC-4626 vaults.

Key concepts:
- Each market is a pair of two Silo vaults (silo0 + silo1)
- Depositing into one silo enables borrowing from the paired silo
- No shared pool — bad debt is isolated per market
- CollateralType: 0=Collateral (borrowable), 1=Protected (non-borrowable)
"""

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
