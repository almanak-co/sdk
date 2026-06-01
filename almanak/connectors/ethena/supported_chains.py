"""Ethena strategy-side chain coverage.

Declares the chains on which the Ethena connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Ethena synthetic dollar (USDe/sUSDe).
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "ethena": frozenset({"ethereum"}),
}
