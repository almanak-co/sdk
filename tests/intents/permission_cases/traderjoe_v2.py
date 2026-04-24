"""On-chain permission-authorisation test cases for the TraderJoe V2 connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

_TJV2_USDC_WAVAX_POOL = "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E/0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"

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
            "pool": _TJV2_USDC_WAVAX_POOL,
            "amount0": "100",
            "amount1": "2",
            "range_lower": "20",
            "range_upper": "60",
        },
    ),
    PermissionTestCase(
        # LP_CLOSE for TraderJoe V2 LB (fungible LBT positions, bin-based).
        # The harness's open-then-close seed mints the LB position via
        # Safe.execTransaction, extracts the bin IDs from the
        # ``DepositedToBins`` event, and merges them into the CLOSE case's
        # ``protocol_params["bin_ids"]`` before compilation. The compiler
        # keys on ``intent.pool`` for the pair + ``protocol_params["bin_ids"]``
        # for the position; ``position_id`` is a required str on
        # LPCloseIntent but is unused by the TJv2 LP_CLOSE compile path.
        chain="avalanche",
        protocol="traderjoe_v2",
        intent_type="LP_CLOSE",
        config={
            "token0": "USDC",
            "token1": "WAVAX",
            "pool": _TJV2_USDC_WAVAX_POOL,
            "amount0": "100",
            "amount1": "2",
            "range_lower": "20",
            "range_upper": "60",
            "position_id": "tjv2-lb-position",  # harness-overridden at seeding
        },
    ),
]

# LP_COLLECT_FEES is still deferred: the on-chain permission harness only
# dispatches LP_OPEN / LP_CLOSE today (see ``_build_intent`` in
# ``tests/intents/_permission_onchain_harness.py``). Activating
# LP_COLLECT_FEES requires adding CollectFeesIntent construction, a
# ``_run_lp_collect_fees_positive`` executor, and a balance-direction
# assertion distinct from LP_CLOSE (fees trickle vs. full principal return).
# That wiring is in scope for a follow-up ticket — do not defer LP_CLOSE
# with this file; LP_CLOSE uses the general open-then-close harness seed
# and is activated above.
DEFERRED_INTENT_TYPES: list[str] = ["LP_COLLECT_FEES"]
