"""Aave V3 Lending Lifecycle on Sonic.

Demonstrates the full Aave V3 lending lifecycle on Sonic chain:
supply collateral -> borrow -> repay -> withdraw.
"""

from .strategy import AaveV3LendingSonicStrategy

__all__ = ["AaveV3LendingSonicStrategy"]
