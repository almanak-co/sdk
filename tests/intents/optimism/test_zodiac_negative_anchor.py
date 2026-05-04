"""Optimism Zodiac negative-authorisation anchor.

Per-chain "negative anchor" companion to the regular intent tests under
``tests/intents/optimism/``. The default-on Zodiac model wired up in
PR #2026 means every regular intent test proves the manifest is
*sufficient* (intent succeeds when the manifest is correct). That answers
half of the manifest's correctness contract — the other half is whether
the manifest is *load-bearing*: revoking a target the intent actually
needs must make the same intent revert with the authorisation selector.

Pre-Phase-G.4 (PR #2029) negative-path coverage came from per-pair files
under ``tests/intents/permission_cases/`` driven by a parallel runtime
in each chain's ``test_permission_onchain.py``. G.4 retired that runtime
in favour of the default-on path, but the sole surviving negative-path
runner only covers ``arbitrum/uniswap_v3/SWAP``. That left a P1 gap on
every other chain, including optimism.

This anchor closes the gap on optimism by parametrizing the canonical
``run_negative_authorisation_case`` helper across one pair per intent-type
family on this chain.

The file is ``no_zodiac``-marked because it deploys its own Safe + Roles
inside ``run_negative_authorisation_case``; if the conftest fixture also
deployed one, the two would conflict on Safe nonces and role-key
collisions.

Tracking issue: #2030. References: PR #2026 (default-on Zodiac), PR #2029
(Phase G.4 — parallel runtime retirement).
"""

from __future__ import annotations

import pytest
from web3 import Web3

from tests.intents._permission_onchain_harness import (
    PermissionTestCase,
    run_negative_authorisation_case,
)

pytestmark = pytest.mark.no_zodiac(
    reason="Negative anchor — deploys its own Safe+Roles inside the harness; "
    "the conftest Zodiac fixture would conflict with that.",
)

CHAIN_NAME = "optimism"


# Canonical negative-anchor case set: one pair per intent-type family the
# manifest generator covers on this chain.
_CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain=CHAIN_NAME,
        protocol="uniswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WETH", "amount": "100"},
    ),
    PermissionTestCase(
        chain=CHAIN_NAME,
        protocol="aave_v3",
        intent_type="SUPPLY",
        config={"token": "USDC", "amount": "100"},
    ),
    PermissionTestCase(
        chain=CHAIN_NAME,
        protocol="uniswap_v3",
        intent_type="LP_OPEN",
        config={
            "token0": "WETH",
            "token1": "USDC",
            "pool": "WETH/USDC/3000",
            "amount0": "0.2",
            "amount1": "500",
            "range_lower": "200",
            "range_upper": "20000",
        },
    ),
]


def _case_id(case: PermissionTestCase) -> str:
    """Render a stable, readable parametrize id."""
    return f"{case.protocol}-{case.intent_type}"


@pytest.mark.optimism
@pytest.mark.parametrize("case", _CASES, ids=_case_id)
def test_negative_authorisation_blocks_revoked_target(
    case: PermissionTestCase,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Revoking a load-bearing manifest target makes the same intent revert.

    # noqa: layers — authorisation, not intent semantics. Semantic
    # correctness is covered by the regular intent tests on this chain;
    # this file proves the manifest is load-bearing. See plan doc
    # ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.
    """
    run_negative_authorisation_case(
        case,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
    )
