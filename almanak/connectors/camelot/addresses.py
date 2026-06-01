"""Camelot contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the Camelot entries previously held in
``almanak.framework.intents.compiler_constants`` (VIB-4872 / epic VIB-4851).

Camelot is an Algebra V1.9 DEX on Arbitrum. The Algebra router is the
only swap-routing surface (no protocol-fee tiers — Algebra pools quote
their own dynamic fees), and the NonfungiblePositionManager handles
concentrated-liquidity LP positions (VIB-1636).

The contract-kind vocabulary is connector-private — callers outside
this folder should consume the registry, not guess key names.
"""

from __future__ import annotations

CAMELOT: dict[str, dict[str, str]] = {
    "arbitrum": {
        # Algebra V3 SwapRouter (VIB-1636). Camelot V3's exactInputSingle
        # uses the Algebra ABI; no fee-tier parameter, no struct.
        "swap_router": "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18",
        # Camelot V3 (Algebra V1.9) NonfungiblePositionManager — verified on
        # the Camelot docs (camelot.exchange/contracts).
        "position_manager": "0x00c7f3082833e796A5b3e4Bd59f6642FF44DCD15",
        # Camelot V3 (Algebra V1.9) Quoter — VIB-3750.
        # Algebra-style ABI:
        #   quoteExactInputSingle(address tokenIn, address tokenOut,
        #                         uint256 amountIn, uint160 limitSqrtPrice)
        #     -> (uint256 amountOut, uint16 fee)
        # Source: https://docs.camelot.exchange/contracts/amm-v3/deployed-contracts
        "quoter": "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E",
    },
}


__all__ = ["CAMELOT"]
