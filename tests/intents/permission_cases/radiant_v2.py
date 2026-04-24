"""On-chain permission-authorisation test cases for the Radiant V2 connector.

Radiant V2 is an Aave V2 fork deployed on Arbitrum; the synthetic-intent
compiler routes it through the shared lending-pool path (see
``almanak.framework.permissions.synthetic_intents._LENDING_PROTOCOLS``). The
Radiant pool address on Arbitrum is wired into
``LENDING_POOL_ADDRESSES`` in
``almanak/framework/intents/compiler_constants.py``.

``market_id`` is intentionally omitted — Radiant uses a unified pool, and
``permission_hints.PERMISSION_HINTS`` declares no ``synthetic_market_id`` for
this connector.

Each case here is consumed by the parametrized harness in
``tests/intents/_permission_onchain_harness.py`` and gated by
``tests/unit/permissions/test_onchain_case_coverage.py``. See
``docs/internal/zodiac-permission-onchain-coverage-plan.md`` for the design.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

# BORROW sizing targets ~20% LTV (1 WETH collateral at ~$2.5k, 500 USDC
# borrow), well inside the ``.claude/rules/intent-tests.md`` ≤30% ceiling.
#
# WITHDRAW / BORROW / REPAY need a prior SUPPLY (and BORROW needs prior
# collateral + debt position) on-chain for this Safe. The cold-Safe harness
# cannot seed that state yet (plan doc P1 — "harness-seeding of prior state"),
# so defer these at runtime. Declaration-level coverage gate still runs against
# them so a connector change that drops selector support still fails PR-time.
DEFERRED_INTENT_TYPES: list[str] = ["WITHDRAW", "BORROW", "REPAY"]

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="arbitrum",
        protocol="radiant_v2",
        intent_type="SUPPLY",
        config={"token": "USDC", "amount": "100"},
    ),
    PermissionTestCase(
        chain="arbitrum",
        protocol="radiant_v2",
        intent_type="WITHDRAW",
        config={"token": "USDC", "amount": "50"},
    ),
    PermissionTestCase(
        chain="arbitrum",
        protocol="radiant_v2",
        intent_type="BORROW",
        config={
            "collateral_token": "WETH",
            "collateral_amount": "1",
            "borrow_token": "USDC",
            "borrow_amount": "500",
        },
    ),
    PermissionTestCase(
        chain="arbitrum",
        protocol="radiant_v2",
        intent_type="REPAY",
        config={"token": "USDC", "amount": "50"},
    ),
]
