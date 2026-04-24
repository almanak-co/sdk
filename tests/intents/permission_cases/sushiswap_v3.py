"""On-chain permission-authorisation test cases for the SushiSwap V3 connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

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
]

# LP_CLOSE coverage requires setting up a real on-chain position first
# (the harness's _run_lp_close_positive cannot mint from empty state).
# Follow-up once the harness gains a "open-then-close" helper.
DEFERRED_INTENT_TYPES: list[str] = ["LP_CLOSE"]
