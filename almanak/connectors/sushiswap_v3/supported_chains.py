"""SushiSwap V3 strategy-side chain coverage.

Declares the chains on which the SushiSwap V3 DEX connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# SushiSwap V3 DEX. ``avalanche`` is excluded: zero usable liquidity (VIB-2069).
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "sushiswap_v3": frozenset(
        {
            "ethereum",
            "arbitrum",
            "optimism",
            "polygon",
            "base",
            "bsc",
        }
    ),
}
