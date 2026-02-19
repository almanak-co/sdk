"""Yield comparison across DeFi protocols.

Provides cross-protocol yield scanning via DeFi Llama yields API (primary)
with on-chain lending rate reads as fallback.
"""

from __future__ import annotations

from .aggregator import YieldAggregator, YieldOpportunity

__all__ = [
    "YieldAggregator",
    "YieldOpportunity",
]
