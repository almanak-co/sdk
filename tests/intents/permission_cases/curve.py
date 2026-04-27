"""On-chain permission-authorisation test cases for the Curve connector.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.

Curve pools are pair-specific (StableSwap, CryptoSwap, Tricrypto). Synthetic
discovery iterates ``CURVE_POOLS[chain]`` so the manifest authorises every
registered pool (#1903) — these on-chain cases pin the load-bearing
combinations:

- ethereum / 3pool (StableSwap, USDC <-> USDT)
- ethereum / tricrypto2 (Tricrypto, USDT <-> WETH) — regression for #1903

Coverage for the remaining chains (arbitrum 2pool + tricrypto, base
weth_cbeth + 4pool, optimism 3pool + crvusd_usdc, polygon am3pool) is
deferred to a follow-up that wires fork blocks + funded wallets per pool.
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
    # #1903 regression: tricrypto2 USDT/WETH must be on the manifest. Without
    # the multi-pool synthetic discovery, this case reverts with
    # AuthorizationFailed under execTransactionWithRole.
    PermissionTestCase(
        chain="ethereum",
        protocol="curve",
        intent_type="SWAP",
        config={"from_token": "USDT", "to_token": "WETH", "amount": "100"},
    ),
]
