"""Radiant V2 strategy-side chain coverage.

Declares the chains on which the Radiant V2 connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Radiant V2 (Aave V2 fork) — Arbitrum pool frozen post-hack, so only the
# Ethereum deployment is exposed.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "radiant_v2": frozenset({"ethereum"}),
}
