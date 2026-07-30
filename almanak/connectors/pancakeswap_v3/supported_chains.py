"""PancakeSwap V3 strategy-side chain coverage.

Declares the chains on which the PancakeSwap V3 DEX connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# PancakeSwap V3 DEX.
#
# VIB-6231: ``linea`` added. The manifest already declared it, so
# ``almanak info matrix`` published pancakeswap_v3 on linea and a linea swap
# compiles successfully -- but this declaration omitted it, so
# ``MultiChainRuntimeConfig._validate_protocols`` rejected the pair and the
# strategy could not even load. Published-but-unloadable is the worse direction
# of the two: the user follows the catalogue and gets a config error.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "pancakeswap_v3": frozenset({"bsc", "ethereum", "arbitrum", "base", "linea"}),
}
