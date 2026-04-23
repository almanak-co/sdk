"""On-chain permission-authorisation test cases for the Uniswap V3 connector.

Each case here is consumed by the parametrized harness in
``tests/intents/_permission_onchain_harness.py`` and gated by
``tests/unit/permissions/test_onchain_case_coverage.py``. See
``docs/internal/zodiac-permission-onchain-coverage-plan.md`` for the design.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="arbitrum",
        protocol="uniswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WETH", "amount": "100"},
    ),
]

# LP_OPEN / LP_CLOSE coverage lands in Phase D of
# docs/internal/zodiac-permission-onchain-coverage-plan.md.
DEFERRED_INTENT_TYPES: list[str] = ["LP_OPEN", "LP_CLOSE"]
