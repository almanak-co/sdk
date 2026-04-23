"""On-chain permission-authorisation test cases for the PancakeSwap V3 connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        # PancakeSwap is BNB-native. "bsc" is the canonical key used by
        # PROTOCOL_ROUTERS / CHAIN_TOKENS; "bnb" is a CHAIN_CONFIGS alias.
        chain="bsc",
        protocol="pancakeswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WBNB", "amount": "100"},
    ),
]

# LP_OPEN / LP_CLOSE coverage lands in Phase D of
# docs/internal/zodiac-permission-onchain-coverage-plan.md.
DEFERRED_INTENT_TYPES: list[str] = ["LP_OPEN", "LP_CLOSE"]
