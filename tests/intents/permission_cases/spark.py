"""On-chain permission-authorisation test cases for the Spark connector.

Spark is an Aave V3 fork deployed on Ethereum; the intent shape mirrors
``aave_v3`` with the Spark pool as the target. Coverage below exercises the
full LEND family (SUPPLY / WITHDRAW / BORROW / REPAY) on ethereum/USDC/WETH.

Each case here is consumed by the parametrized harness in
``tests/intents/_permission_onchain_harness.py`` and gated by
``tests/unit/permissions/test_onchain_case_coverage.py``. See
``docs/internal/zodiac-permission-onchain-coverage-plan.md`` for the design.

``market_id`` is intentionally omitted: ``almanak.framework.connectors.spark.
permission_hints.PERMISSION_HINTS.synthetic_market_id`` is ``None`` (Spark uses
Aave's unified pool, not an isolated-market registry), so no synthetic market
id needs to flow through to the intent constructor.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="ethereum",
        protocol="spark",
        intent_type="SUPPLY",
        config={"token": "USDC", "amount": "100"},
    ),
    PermissionTestCase(
        chain="ethereum",
        protocol="spark",
        intent_type="WITHDRAW",
        config={"token": "USDC", "amount": "50"},
    ),
    PermissionTestCase(
        chain="ethereum",
        protocol="spark",
        intent_type="BORROW",
        # 1 WETH collateral (~$2.5-3k) against 500 USDC borrow => well under
        # the 30% LTV ceiling called for in the Phase C plan, with headroom
        # for price drift on the Anvil fork.
        config={
            "collateral_token": "WETH",
            "collateral_amount": "1",
            "borrow_token": "USDC",
            "borrow_amount": "500",
        },
    ),
    PermissionTestCase(
        chain="ethereum",
        protocol="spark",
        intent_type="REPAY",
        config={"token": "USDC", "amount": "50"},
    ),
]
