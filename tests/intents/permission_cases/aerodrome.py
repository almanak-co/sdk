"""On-chain permission-authorisation test cases for the Aerodrome connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

# Aerodrome on Base runs the classic Solidly (fungible LP) router, not
# Slipstream. ``permission_hints.synthetic_fee_tier`` is unset for this
# connector, so the synthetic generator emits ``"{token0}/{token1}"`` without
# a fee tier (see ``_build_lp_open_intents`` in
# ``almanak/framework/permissions/synthetic_intents.py``). The aerodrome
# compiler's LP_OPEN parser defaults ``stable=False`` (volatile) when the
# third pool segment is omitted, matching what the generator produces.
CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="base",
        protocol="aerodrome",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WETH", "amount": "100"},
    ),
    PermissionTestCase(
        chain="base",
        protocol="aerodrome",
        intent_type="LP_OPEN",
        config={
            "token0": "USDC",
            "token1": "WETH",
            # Classic Solidly pool string — no fee tier segment. Mirrors
            # ``_build_lp_open_intents`` for protocols without synthetic_fee_tier.
            "pool": "USDC/WETH",
            "amount0": "100",
            "amount1": "0.05",
            "range_lower": "1500",
            "range_upper": "4000",
        },
    ),
    PermissionTestCase(
        # LP_CLOSE for classic Aerodrome (Solidly fork, fungible LP tokens).
        # The harness's open-then-close seed mints the LP position via
        # Safe.execTransaction, extracts the pool address from the Mint
        # event (pool address IS the LP token for Solidly), then overwrites
        # ``position_id`` below with the parsed pool before compiling CLOSE.
        chain="base",
        protocol="aerodrome",
        intent_type="LP_CLOSE",
        config={
            "token0": "USDC",
            "token1": "WETH",
            "pool": "USDC/WETH",
            "amount0": "100",
            "amount1": "0.05",
            "range_lower": "1500",
            "range_upper": "4000",
            # Harness-overridden at seeding time with the parsed pool address.
            "position_id": "USDC/WETH",
        },
    ),
]
