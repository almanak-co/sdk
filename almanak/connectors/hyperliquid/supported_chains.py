"""Hyperliquid strategy-side chain coverage.

Declares the chains on which the Hyperliquid connector is reachable.
Hyperliquid runs on its own L1 but is accessed via Arbitrum, so ``arbitrum``
is the strategy-facing venue. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Hyperliquid is on its own L1 but accessed via Arbitrum.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "hyperliquid": frozenset({"arbitrum"}),
}
