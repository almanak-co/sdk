"""BNB Chain (BSC) Zodiac negative-authorisation anchor.

Per-chain "negative anchor" companion to the regular intent tests under
``tests/intents/bnb/``. The default-on Zodiac model wired up in PR #2026
means every regular intent test proves the manifest is *sufficient*
(intent succeeds when the manifest is correct). That answers half of the
manifest's correctness contract — the other half is whether the manifest
is *load-bearing*: revoking a target the intent actually needs must make
the same intent revert with the authorisation selector.

Pre-Phase-G.4 (PR #2029) negative-path coverage came from per-pair files
under ``tests/intents/permission_cases/`` driven by a parallel runtime
in each chain's ``test_permission_onchain.py``. G.4 retired that runtime
in favour of the default-on path, but the sole surviving negative-path
runner only covers ``arbitrum/uniswap_v3/SWAP``. That left a P1 gap on
every other chain, including bsc — an over-permissive manifest regression
on, say, pancakeswap_v3 SWAP or aave_v3 SUPPLY would slip past CI because
no test ever revokes a load-bearing target.

This anchor closes the gap on bsc by parametrizing the canonical
``run_negative_authorisation_case`` helper across one pair per intent-type
family on this chain. Cases use ``chain="bsc"`` (the canonical key the
framework's protocol/router maps key on) even though the test directory
is named ``bnb`` — the bnb conftest's ``CHAIN_NAME`` resolves to ``bsc``
for the same reason.

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

# Canonical chain key used by the framework's protocol/router maps. The
# pytest test-directory is named "bnb" (CLI-friendly alias) but everything
# downstream — PROTOCOL_ROUTERS, CHAIN_TOKENS, LENDING_POOL_ADDRESSES,
# LP_POSITION_MANAGERS — keys on "bsc".
CHAIN_NAME = "bsc"

# PancakeSwap V3 supports (100, 500, 2500, 10000); the connector's
# permission_hints declares no ``synthetic_fee_tier`` override, so the
# manifest generator picks DEFAULT_SWAP_FEE_TIER['pancakeswap_v3'] == 2500.
# Mirror that fee tier so the LP_OPEN authorised target and the executed
# mint call land on the same pool.
_PANCAKE_V3_BSC_FEE_TIER = 2500


# Canonical negative-anchor case set: one pair per intent-type family the
# manifest generator covers on this chain. Pancakeswap is BNB-native so
# it carries both SWAP and LP-OPEN cases here; aave_v3 covers the LEND
# family.
_CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain=CHAIN_NAME,
        protocol="pancakeswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WBNB", "amount": "100"},
    ),
    PermissionTestCase(
        chain=CHAIN_NAME,
        protocol="aave_v3",
        intent_type="SUPPLY",
        config={"token": "USDC", "amount": "100"},
    ),
    PermissionTestCase(
        chain=CHAIN_NAME,
        protocol="pancakeswap_v3",
        intent_type="LP_OPEN",
        config={
            "token0": "USDC",
            "token1": "WBNB",
            "pool": f"USDC/WBNB/{_PANCAKE_V3_BSC_FEE_TIER}",
            "amount0": "100",
            "amount1": "0.05",
            # USDC/WBNB — WBNB is typically ~500-700 USDC; pick a wide
            # range that brackets common spot so the mint is not entirely
            # one-sided.
            "range_lower": "300",
            "range_upper": "1500",
        },
    ),
]


def _case_id(case: PermissionTestCase) -> str:
    """Render a stable, readable parametrize id."""
    return f"{case.protocol}-{case.intent_type}"


@pytest.mark.bsc
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
