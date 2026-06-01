"""PancakeSwap V3 strategy-side chain coverage.

Declares the chains on which the PancakeSwap V3 DEX connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# PancakeSwap V3 DEX.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "pancakeswap_v3": frozenset({"bsc", "ethereum", "arbitrum"}),
}
