"""Hyperliquid strategy-side chain coverage.

Declares the chains on which the Hyperliquid connector is reachable.
Hyperliquid perps execute on **HyperEVM** (chain id 999) via the CoreWriter
system contract, so ``hyperevm`` is the strategy-facing venue. (The abandoned
V1 approach targeted the native L1 order API via an Arbitrum deposit bridge —
that path is not used here.) See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Hyperliquid perps are reached on HyperEVM (999) via CoreWriter.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "hyperliquid": frozenset({"hyperevm"}),
}
