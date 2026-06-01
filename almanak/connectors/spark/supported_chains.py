"""Spark strategy-side chain coverage.

Declares the chains on which the Spark connector is alive. Spark is an Aave V3
fork on Ethereum with its own connector folder. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Spark is an Aave V3 fork on Ethereum.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "spark": frozenset({"ethereum"}),
}
