"""BENQI strategy-side chain coverage.

Declares the chains on which the BENQI connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# BENQI (Compound V2 fork) on Avalanche.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "benqi": frozenset({"avalanche"}),
}
