"""Enso strategy-side chain coverage.

Declares the chains on which the Enso aggregator connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Aggregator. ``mantle`` is excluded: the Enso client CHAIN_MAPPING does not
# support it.
#
# VIB-6231: ``blast`` and ``plasma`` removed. Neither is in the manifest's
# ``strategy_chains``, so ``almanak info matrix`` never published them, but this
# declaration made config validation ACCEPT them -- and a swap on either fails
# at compile with "Unknown token: USDC" because no tokens are registered for
# those chains. Accept-then-reject, same shape as the ``lido`` L2 rows.
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
            "berachain",
            "sonic",
        }
    ),
}
