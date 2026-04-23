"""On-chain permission-authorisation test cases for the Curve connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.

Curve's pools are pair-specific (stableswap / tricrypto), so the default
USDC/WETH synthetic pair does not map to any pool. The connector's
``permission_hints.synthetic_swap_pair`` override pins ethereum to the
3pool USDC/USDT leg; this case mirrors that pair so the generated
manifest matches the compiled intent.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="ethereum",
        protocol="curve",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "USDT", "amount": "100"},
    ),
]
