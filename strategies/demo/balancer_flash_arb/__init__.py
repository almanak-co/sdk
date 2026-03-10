"""Balancer Flash Loan Arbitrage Demo Strategy.

Exercises the Balancer flash loan connector on Arbitrum with Enso swap callbacks.
First kitchenloop test of the Balancer connector (flash loan path).
"""

from .strategy import BalancerFlashArbStrategy

__all__ = ["BalancerFlashArbStrategy"]
