"""On-chain permission-authorisation test cases for Aave V3.

See docs/internal/zodiac-permission-onchain-coverage-plan.md.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="arbitrum",
        protocol="aave_v3",
        intent_type="SUPPLY",
        config={"token": "USDC", "amount": "100"},
    ),
    PermissionTestCase(
        chain="arbitrum",
        protocol="aave_v3",
        intent_type="WITHDRAW",
        config={"token": "USDC", "amount": "50"},
    ),
    PermissionTestCase(
        chain="arbitrum",
        protocol="aave_v3",
        intent_type="BORROW",
        # ~28% LTV against $1800 ETH ≈ $500 USDC borrow. Stay at/below 30%
        # per .claude/rules/intent-tests.md — price volatility headroom.
        config={
            "collateral_token": "WETH",
            "collateral_amount": "1",
            "borrow_token": "USDC",
            "borrow_amount": "500",
        },
    ),
    PermissionTestCase(
        chain="arbitrum",
        protocol="aave_v3",
        intent_type="REPAY",
        config={"token": "USDC", "amount": "50"},
    ),
]
