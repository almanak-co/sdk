"""Nightly on-chain permission authorisation coverage — ethereum.

Auto-discovers active cases from ``tests/intents/permission_cases/`` and runs
each against the deployed Zodiac Roles Modifier on an Anvil fork. Positive
path asserts the generator's manifest authorises the compiled intent;
negative path asserts that stripping a load-bearing target makes the same
intent revert.

Not a PR-time gate — only the nightly workflow runs this file. See
``.github/workflows/permission-onchain-nightly.yml`` for scheduling and
``docs/internal/permission-onchain-failure-triage.md`` for triage.

Parent plan: ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.

``.claude/rules/intent-tests.md`` — these are authorisation-only tests (not
4-layer intent-semantics tests), hence the ``# noqa: layers`` annotations.
"""

from __future__ import annotations

import pytest
from web3 import Web3

from tests.intents._permission_onchain_harness import (
    discover_cases,
    discover_negative_cases,
    run_negative_authorisation_case,
    run_positive_authorisation_case,
)

CHAIN_NAME = "ethereum"

_POSITIVE_CASES = discover_cases(CHAIN_NAME)
_NEGATIVE_CASES = discover_negative_cases(CHAIN_NAME)


@pytest.mark.ethereum
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    _POSITIVE_CASES,
    ids=lambda c: f"{c.protocol}-{c.intent_type}",
)
async def test_manifest_authorises_intent(  # noqa: layers
    case,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Positive authz path — plan doc: zodiac-permission-onchain-coverage-plan.md."""
    run_positive_authorisation_case(
        case,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
    )


@pytest.mark.ethereum
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    _NEGATIVE_CASES,
    ids=lambda c: f"{c.protocol}-{c.intent_type}",
)
async def test_revoking_load_bearing_target_denies_intent(  # noqa: layers
    case,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Negative authz path — plan doc: zodiac-permission-onchain-coverage-plan.md."""
    run_negative_authorisation_case(
        case,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
    )
