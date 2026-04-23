"""On-chain permission-authorisation test cases for the Aerodrome connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="base",
        protocol="aerodrome",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WETH", "amount": "100"},
    ),
]

# LP_OPEN / LP_CLOSE coverage lands in Phase D of
# docs/internal/zodiac-permission-onchain-coverage-plan.md.
DEFERRED_INTENT_TYPES: list[str] = ["LP_OPEN", "LP_CLOSE"]
