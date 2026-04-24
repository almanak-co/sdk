"""On-chain permission-authorisation test cases for the Uniswap V3 connector.

Each case here is consumed by the parametrized harness in
``tests/intents/_permission_onchain_harness.py`` and gated by
``tests/unit/permissions/test_onchain_case_coverage.py``. See
``docs/internal/zodiac-permission-onchain-coverage-plan.md`` for the design.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

# Arbitrum token addresses (mirror tests/intents/conftest.py::CHAIN_CONFIGS).
# Inlined here so the pool string doesn't require importing the fixture module
# at case-collection time.
_ARBITRUM_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
_ARBITRUM_WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="arbitrum",
        protocol="uniswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WETH", "amount": "100"},
    ),
    PermissionTestCase(
        chain="arbitrum",
        protocol="uniswap_v3",
        intent_type="LP_OPEN",
        config={
            # token0 / token1 are funding-only hints for the harness and are
            # stripped before unpacking into LPOpenIntent (see
            # ``_build_lp_open_intent`` / ``_LP_FUNDING_KEYS``).
            "token0": "USDC",
            "token1": "WETH",
            # Pool string encodes the token addresses + fee tier. 3000 is the
            # canonical uniswap_v3 default (``DEFAULT_SWAP_FEE_TIER``) and
            # matches the synthetic-intent shape built by
            # ``almanak.framework.permissions.synthetic_intents._build_lp_open_intents``.
            "pool": f"{_ARBITRUM_USDC}/{_ARBITRUM_WETH}/3000",
            "amount0": "100",
            "amount1": "0.05",
            "range_lower": "1500",
            "range_upper": "4000",
        },
    ),
    PermissionTestCase(
        # LP_CLOSE executes via the harness's open-then-close seed — it mints
        # a real on-chain position first (via Safe.execTransaction, no
        # Zodiac), extracts the NFT tokenId from the mint receipt, then
        # compiles the CLOSE intent against that position. The ``token0`` /
        # ``token1`` / ``pool`` / range/amount fields are reused by the
        # seed step; ``position_id`` below is a placeholder that the harness
        # overwrites with the real tokenId before compilation.
        chain="arbitrum",
        protocol="uniswap_v3",
        intent_type="LP_CLOSE",
        config={
            "token0": "USDC",
            "token1": "WETH",
            "pool": f"{_ARBITRUM_USDC}/{_ARBITRUM_WETH}/3000",
            "amount0": "100",
            "amount1": "0.05",
            "range_lower": "1500",
            "range_upper": "4000",
            # Harness-overridden at seeding time — set to a sentinel so
            # LPCloseIntent construction doesn't trip on the required field.
            "position_id": "0",
        },
    ),
]
