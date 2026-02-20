"""LiFi Cross-Chain Yield Rotator Strategy.

Rotates USDC between Aave V3 deployments across chains for highest supply APY,
using LiFi for cross-chain bridging.

Usage:
    almanak strat run -d strategies/incubating/lifi_yield_rotator --network anvil --once
"""

from .strategy import LiFiYieldRotatorStrategy

__all__ = ["LiFiYieldRotatorStrategy"]
