"""On-chain permission-authorisation test cases for the SushiSwap V3 connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

# BSC USDT / WBNB addresses — must match CHAIN_TOKENS['bsc'] in
# ``almanak/framework/intents/compiler_constants.py`` so the compiled
# LP_OPEN intent targets the same pool the manifest authorised. SushiSwap
# V3 on bsc has no liquid USDC/* pool; USDT/WBNB is the canonical pair
# (mirrors the bnb intent test in tests/intents/bnb/test_sushiswap_v3_lp.py
# and the synthetic_lp_pair override on the connector's permission_hints).
_BSC_USDT = "0x55d398326f99059fF775485246999027B3197955"
_BSC_WBNB = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="arbitrum",
        protocol="sushiswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WETH", "amount": "100"},
    ),
    PermissionTestCase(
        # Same chain as the SWAP case above — sushiswap_v3 LP position manager
        # is registered for arbitrum in LP_POSITION_MANAGERS. Fee tier 3000
        # matches DEFAULT_SWAP_FEE_TIER["sushiswap_v3"]; permission_hints.py
        # declares no synthetic_fee_tier override, so the manifest generator
        # lands on the same 3000 tier the mint call uses.
        #
        # token0/token1 are harness funding hints (stripped before unpacking
        # into LPOpenIntent). amount0/amount1/range_* pin the intent payload
        # itself. Values mirror
        # almanak.framework.permissions.synthetic_intents._build_lp_open_intents
        # so the manifest-under-test and the executed intent reference the
        # same pool + range semantics.
        chain="arbitrum",
        protocol="sushiswap_v3",
        intent_type="LP_OPEN",
        config={
            "token0": "USDC",
            "token1": "WETH",
            "pool": "USDC/WETH/3000",
            "amount0": "100",
            "amount1": "0.05",
            "range_lower": "1500",
            "range_upper": "4000",
        },
    ),
    PermissionTestCase(
        # LP_CLOSE via the harness's open-then-close seed. Mirrors
        # uniswap_v3 / pancakeswap_v3: mint an NFT position via
        # Safe.execTransaction, parse the tokenId from the ERC-721
        # Transfer event, then compile CLOSE against that tokenId.
        chain="arbitrum",
        protocol="sushiswap_v3",
        intent_type="LP_CLOSE",
        config={
            "token0": "USDC",
            "token1": "WETH",
            "pool": "USDC/WETH/3000",
            "amount0": "100",
            "amount1": "0.05",
            "range_lower": "1500",
            "range_upper": "4000",
            # Harness-overridden at seeding time with the minted tokenId.
            "position_id": "0",
        },
    ),
    PermissionTestCase(
        # bsc / sushiswap_v3 LP_OPEN — added for #1902. The synthetic
        # discovery's chain-default pair on bsc resolves to (USDC, ETH-bsc)
        # which has no liquid sushiswap_v3 pool. The connector's
        # permission_hints.synthetic_lp_pair pins (USDT, WBNB); this case
        # mirrors the same pair so the on-chain harness validates the
        # manifest authorises the actual liquid pool.
        chain="bsc",
        protocol="sushiswap_v3",
        intent_type="LP_OPEN",
        config={
            "token0": "USDT",
            "token1": "WBNB",
            "pool": f"{_BSC_USDT}/{_BSC_WBNB}/3000",
            "amount0": "500",
            "amount1": "1.0",
            # WBNB-per-USDT range: token0=USDT (lower address) → range
            # is denominated in token1/token0 (WBNB per USDT). Mirrors
            # tests/intents/bnb/test_sushiswap_v3_lp.py.
            "range_lower": "0.0005",
            "range_upper": "0.05",
        },
    ),
    PermissionTestCase(
        # bsc / sushiswap_v3 LP_CLOSE — companion to the LP_OPEN case
        # above. Same fix #1902.
        chain="bsc",
        protocol="sushiswap_v3",
        intent_type="LP_CLOSE",
        config={
            "token0": "USDT",
            "token1": "WBNB",
            "pool": f"{_BSC_USDT}/{_BSC_WBNB}/3000",
            "amount0": "500",
            "amount1": "1.0",
            "range_lower": "0.0005",
            "range_upper": "0.05",
            # Harness-overridden at seeding time with the minted tokenId.
            "position_id": "0",
        },
    ),
]
