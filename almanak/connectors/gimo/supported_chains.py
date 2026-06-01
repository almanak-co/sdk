"""Gimo strategy-side chain coverage.

Declares the chains on which the Gimo connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Gimo Finance liquid staking on 0G Chain.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "gimo": frozenset({"zerog"}),
}
