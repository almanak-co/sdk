"""Euler V2 strategy-side chain coverage.

Declares the chains on which the Euler V2 connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Euler V2 (ERC-4626 vaults + EVC).
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "euler_v2": frozenset({"avalanche", "ethereum"}),
}
