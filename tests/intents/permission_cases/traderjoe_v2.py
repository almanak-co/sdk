"""On-chain permission-authorisation test cases for the TraderJoe V2 connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="avalanche",
        protocol="traderjoe_v2",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WAVAX", "amount": "100"},
    ),
    PermissionTestCase(
        # TraderJoe V2 Liquidity Book uses discrete bins, not ticks. The
        # pool string is ``{token0}/{token1}`` WITHOUT a fee tier because
        # fees in LB are per-bin (determined by the pair's bin_step), not a
        # pool-level constant. This mirrors the synthetic builder in
        # ``almanak.framework.permissions.synthetic_intents._build_lp_open_intents``
        # whose SWAP_FEE_TIERS/synthetic_fee_tier branch is false for tjv2.
        #
        # ``range_lower`` / ``range_upper`` are price-ish (USDC per WAVAX);
        # the TraderJoe V2 adapter handles the semantic conversion to bin
        # IDs around the pool's active bin.
        chain="avalanche",
        protocol="traderjoe_v2",
        intent_type="LP_OPEN",
        config={
            "token0": "USDC",
            "token1": "WAVAX",
            "pool": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E/0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "amount0": "100",
            "amount1": "2",
            "range_lower": "20",
            "range_upper": "60",
        },
    ),
]

# LP_CLOSE + LP_COLLECT_FEES need a pre-existing on-chain position;
# the harness's _run_lp_close_positive cannot mint from empty state.
# Follow-up once the harness gains an "open-then-close/collect" helper.
# (traderjoe_v2 declares supports_standalone_fee_collection=True, so the
# matrix includes LP_COLLECT_FEES in addition to open/close.)
DEFERRED_INTENT_TYPES: list[str] = ["LP_CLOSE", "LP_COLLECT_FEES"]
