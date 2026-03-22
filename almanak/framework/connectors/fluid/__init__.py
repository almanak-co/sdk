"""Fluid DEX Connector — Phase 1 (Arbitrum swaps + LP scaffolding).

Provides swap support for Fluid DEX T1 pools on Arbitrum via swapIn().
LP open/close intent routing is wired but LP deposit reverts on-chain (phase 2).

Scope (phase 1):
- Arbitrum only
- Swaps via swapIn() (fully functional)
- LP deposit deferred (Liquidity-layer routing causes reverts)

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
from almanak.framework.connectors.fluid.receipt_parser import FluidReceiptParser
from almanak.framework.connectors.fluid.sdk import FluidSDK

__all__ = [
    "FluidAdapter",
    "FluidConfig",
    "FluidPositionDetails",
    "FluidReceiptParser",
    "FluidSDK",
]
