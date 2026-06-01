"""Lido strategy-side chain coverage.

Declares the chains on which the Lido liquid-staking connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Lido liquid staking.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "lido": frozenset({"ethereum", "arbitrum", "optimism", "polygon"}),
}
