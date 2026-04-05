"""Spark Protocol Full Lending Lifecycle on Ethereum.

Demonstrates the full Spark lending lifecycle on Ethereum:
supply wstETH collateral -> borrow DAI -> repay DAI -> withdraw wstETH.
"""

from .strategy import SparkLendingLifecycleStrategy

__all__ = ["SparkLendingLifecycleStrategy"]
