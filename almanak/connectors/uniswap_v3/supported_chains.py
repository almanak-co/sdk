"""Uniswap V3 strategy-side chain coverage.

Declares the chains on which the Uniswap V3 connector is alive. The connector
also owns the ``agni_finance`` identifier: Agni Finance is a Uniswap V3 fork
(primary DEX on Mantle) with **no own connector folder** — it shares this
connector's compiler/adapter — so its chain coverage is declared here rather
than spawning a parallel folder. Keeping it in the same module preserves the
"add a connector = one folder" invariant: the only place a Uniswap-V3-family
chain set lives is this file.

See ``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# protocol identifier → chains the connector runs on. ``agni_finance`` is a
# Uniswap V3 fork backed by this same connector (no own folder), so it is
# declared alongside the canonical ``uniswap_v3`` key.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "uniswap_v3": frozenset(
        {
            "ethereum",
            "arbitrum",
            "optimism",
            "polygon",
            "base",
            "avalanche",
            "bsc",
            "linea",
            "blast",
            "monad",
            "xlayer",
            "zerog",  # JAINE DEX (Uniswap V3 fork on 0G Chain)
        }
    ),
    # Agni Finance (Uniswap V3 fork, primary DEX on Mantle).
    "agni_finance": frozenset({"mantle"}),
}
