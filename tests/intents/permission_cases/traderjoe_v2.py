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
]

# LP_OPEN / LP_CLOSE / LP_COLLECT_FEES coverage lands in Phase D of
# docs/internal/zodiac-permission-onchain-coverage-plan.md.
# (traderjoe_v2 declares supports_standalone_fee_collection=True, so the
# matrix includes LP_COLLECT_FEES in addition to open/close.)
DEFERRED_INTENT_TYPES: list[str] = ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"]
