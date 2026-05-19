"""Fluid DEX Connector — Phase 1 (Arbitrum swap surface + LP scaffolding).

Provides the Fluid DEX T1 integration surface on Arbitrum. Swaps currently
fail fast because all known T1 pools reject swaps at tested amounts; LP open
also fails fast while Liquidity-layer routing remains unsupported. LP close
uses the adapter encumbrance guard before building remove-liquidity calldata.

Scope (phase 1):
- Arbitrum only
- Swaps via swapIn() (compile path currently disabled)
- LP deposit deferred (Liquidity-layer routing causes reverts)
- LP close compile support for unencumbered positions

Key contracts (Arbitrum):
- DexFactory: 0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085
- DexResolver: 0x11D80CfF056Cef4F9E6d23da8672fE9873e5cC07

Example:
    from almanak.framework.connectors.fluid import FluidAdapter, FluidConfig

    config = FluidConfig(
        chain="arbitrum",
        wallet_address="0x...",
        rpc_url="https://arb-mainnet.g.alchemy.com/v2/...",
    )
    adapter = FluidAdapter(config)
"""

from almanak.framework.connectors.fluid.adapter import (
    FluidAdapter,
    FluidConfig,
    FluidPositionDetails,
)
from almanak.framework.connectors.fluid.compiler import FluidCompiler
from almanak.framework.connectors.fluid.receipt_parser import FluidReceiptParser
from almanak.framework.connectors.fluid.sdk import FluidSDK

__all__ = [
    "FluidAdapter",
    "FluidConfig",
    "FluidCompiler",
    "FluidPositionDetails",
    "FluidReceiptParser",
    "FluidSDK",
]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="fluid",
    intents=(
        IntentType.SWAP,
        IntentType.LP_OPEN,
        IntentType.LP_CLOSE,
    ),
    chains=("arbitrum",),
)
