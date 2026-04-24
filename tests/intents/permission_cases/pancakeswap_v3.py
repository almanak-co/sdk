"""On-chain permission-authorisation test cases for the PancakeSwap V3 connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

# BSC USDC / WBNB addresses — must match CHAIN_TOKENS['bsc'] in
# ``almanak/framework/intents/compiler_constants.py`` so the compiled
# LP_OPEN intent targets the same pool the manifest authorised.
_BSC_USDC = "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"
_BSC_WBNB = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"

# PancakeSwap V3 supports (100, 500, 2500, 10000); the connector's
# permission_hints.py declares no ``synthetic_fee_tier`` override, so
# the manifest generator picks DEFAULT_SWAP_FEE_TIER['pancakeswap_v3']
# == 2500. The LP_OPEN case below mirrors that fee tier so the
# NonfungiblePositionManager.mint selector + pool authorised by the
# manifest exactly matches the tx built by the compiler.
_PANCAKE_V3_BSC_FEE_TIER = 2500

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        # PancakeSwap is BNB-native. "bsc" is the canonical key used by
        # PROTOCOL_ROUTERS / CHAIN_TOKENS; "bnb" is a CHAIN_CONFIGS alias.
        chain="bsc",
        protocol="pancakeswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WBNB", "amount": "100"},
    ),
    PermissionTestCase(
        # LP_OPEN on the same chain as the SWAP case (bsc) — avoids
        # cross-chain drift within one protocol's case file. USDC/WBNB
        # is the deepest-liquidity pancakeswap_v3 pair on BSC. The
        # ``token0``/``token1`` symbols are stripped by the harness
        # and used only to pre-fund the Safe; ``pool`` carries the
        # addresses the compiler actually encodes into the mint call.
        chain="bsc",
        protocol="pancakeswap_v3",
        intent_type="LP_OPEN",
        config={
            "token0": "USDC",
            "token1": "WBNB",
            "pool": f"{_BSC_USDC}/{_BSC_WBNB}/{_PANCAKE_V3_BSC_FEE_TIER}",
            "amount0": "100",
            "amount1": "0.05",
            # USDC/WBNB — WBNB is typically ~500-700 USDC; pick a wide
            # range that brackets common spot so the mint is not
            # entirely out-of-range (would LP one-sided).
            "range_lower": "300",
            "range_upper": "1500",
        },
    ),
    PermissionTestCase(
        # LP_CLOSE via the harness's open-then-close seed. Mirrors
        # uniswap_v3 / sushiswap_v3: mint an NFT position via
        # Safe.execTransaction, parse the tokenId from the ERC-721
        # Transfer event, then compile CLOSE against that tokenId.
        chain="bsc",
        protocol="pancakeswap_v3",
        intent_type="LP_CLOSE",
        config={
            "token0": "USDC",
            "token1": "WBNB",
            "pool": f"{_BSC_USDC}/{_BSC_WBNB}/{_PANCAKE_V3_BSC_FEE_TIER}",
            "amount0": "100",
            "amount1": "0.05",
            "range_lower": "300",
            "range_upper": "1500",
            # Harness-overridden at seeding time with the minted tokenId.
            "position_id": "0",
        },
    ),
]
