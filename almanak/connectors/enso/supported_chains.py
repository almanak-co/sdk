"""Enso strategy-side chain coverage.

Declares the chains on which the Enso aggregator connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Aggregator. ``mantle`` is excluded: the Enso client CHAIN_MAPPING does not
# support it.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "enso": frozenset(
        {
            "ethereum",
            "arbitrum",
            "optimism",
            "polygon",
            "base",
            "avalanche",
            "bsc",
            "linea",
            "plasma",
            "blast",
            "berachain",
            "sonic",
        }
    ),
}
