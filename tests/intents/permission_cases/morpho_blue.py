"""On-chain permission-authorisation test cases for the Morpho Blue connector.

Each case here is consumed by the parametrized harness in
``tests/intents/_permission_onchain_harness.py`` and gated by
``tests/unit/permissions/test_onchain_case_coverage.py``. See
``docs/internal/zodiac-permission-onchain-coverage-plan.md`` for the design.

Morpho Blue is market-ID driven — every isolated (loan, collateral, oracle,
IRM, LLTV) tuple hashes to a ``bytes32`` market identifier that the SUPPLY /
WITHDRAW / BORROW / REPAY selectors key on. The synthetic value below mirrors
``_SYNTHETIC_MARKET_ID`` in
``almanak/framework/connectors/morpho_blue/permission_hints.py`` so the
manifest the generator emits for these cases matches the selectors the
compiled intent actually calls.

BORROW stays at ~28% LTV (1 WETH collateral → 500 USDC borrow) per the
LTV cap in ``.claude/rules/intent-tests.md``.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

# Well-known WETH/USDC Morpho Blue market on Ethereum. Sourced from
# ``almanak/framework/connectors/morpho_blue/permission_hints.py::_SYNTHETIC_MARKET_ID``
# — the same value the manifest generator injects into synthetic intents
# for this protocol via ``PermissionHints.synthetic_market_id``.
_MORPHO_BLUE_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"

# WITHDRAW / BORROW / REPAY require the Safe to have an existing Morpho Blue
# market position for this market id, which the cold-Safe harness doesn't seed
# (plan doc P1 — "harness-seeding of prior state"). Deferred at runtime until
# that lands; the declaration-level coverage gate still runs against them so a
# future connector change that drops selector support still fails PR-time.
DEFERRED_INTENT_TYPES: list[str] = ["WITHDRAW", "BORROW", "REPAY"]

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="ethereum",
        protocol="morpho_blue",
        intent_type="SUPPLY",
        config={
            # USDC is the loan token on this WETH/USDC market. SupplyIntent
            # defaults ``use_as_collateral=True`` which routes Morpho Blue
            # to ``supply_collateral`` — only valid for the collateral asset
            # (WETH). Pin the loan-token supply path explicitly so the
            # compiled intent hits the ``supply`` selector that matches the
            # USDC funded/asserted in the harness.
            "token": "USDC",
            "amount": "100",
            "market_id": _MORPHO_BLUE_MARKET_ID,
            "use_as_collateral": False,
        },
    ),
    PermissionTestCase(
        chain="ethereum",
        protocol="morpho_blue",
        intent_type="WITHDRAW",
        config={
            # Mirror the SUPPLY case: WithdrawIntent defaults
            # ``is_collateral=True`` which compiles to ``withdraw_collateral``.
            # For a USDC (loan-token) withdraw on WETH/USDC we need the
            # ``withdraw`` selector — pin the flag so the authorised selector
            # matches the intent actually executed.
            "token": "USDC",
            "amount": "50",
            "market_id": _MORPHO_BLUE_MARKET_ID,
            "is_collateral": False,
        },
    ),
    PermissionTestCase(
        chain="ethereum",
        protocol="morpho_blue",
        intent_type="BORROW",
        config={
            # 1 WETH collateral (~$1800) -> 500 USDC borrow ~= 28% LTV.
            # Safe headroom across live-oracle price swings per intent-tests.md.
            "collateral_token": "WETH",
            "collateral_amount": "1",
            "borrow_token": "USDC",
            "borrow_amount": "500",
            "market_id": _MORPHO_BLUE_MARKET_ID,
        },
    ),
    PermissionTestCase(
        chain="ethereum",
        protocol="morpho_blue",
        intent_type="REPAY",
        config={
            "token": "USDC",
            "amount": "50",
            "market_id": _MORPHO_BLUE_MARKET_ID,
        },
    ),
]
