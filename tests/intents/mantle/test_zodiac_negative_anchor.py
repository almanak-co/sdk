"""Mantle Zodiac negative-authorisation anchor.

Per-chain "negative anchor" companion to the regular intent tests under
``tests/intents/mantle/``. The default-on Zodiac model wired up in
PR #2026 means every regular intent test proves the manifest is
*sufficient* (intent succeeds when the manifest is correct). That answers
half of the manifest's correctness contract — the other half is whether
the manifest is *load-bearing*: revoking a target the intent actually
needs must make the same intent revert with the authorisation selector.

Pre-Phase-G.4 (PR #2029) negative-path coverage came from per-pair files
under ``tests/intents/permission_cases/`` driven by a parallel runtime
in each chain's ``test_permission_onchain.py``. G.4 retired that runtime
in favour of the default-on path. PR #2037 (issue #2030) closed the
load-bearing gap on 7 chains (arbitrum, avalanche, base, bnb, ethereum,
optimism, polygon) but missed mantle, which has matrix-covered protocols
(``uniswap_v3`` SWAP + ``aave_v3`` SUPPLY) and the Zodiac fixture wired
up in its conftest, so the asymmetric coverage on this chain is the same
P1 gap the issue calls out: the positive default-on path proves the
manifest suffices on mantle, but no test on this chain proves it is
load-bearing.

This anchor closes that gap. Note the SWAP pair (USDT → WETH) and the
SUPPLY token (WETH) deliberately mirror the existing mantle intent tests
under ``test_uniswap_swap.py`` / ``test_aave_v3_lending.py`` rather than
the default USDC pattern used on other chains — bridged USDC liquidity
on mantle Uniswap V3 (Agni) pools is thinner than the USDT/WETH pair the
existing positive tests already exercise, and Aave V3 mantle's reserve
config makes WETH the established working collateral. The harness's
``_auto_derive_load_bearing_selector`` picks the function-scoped,
non-approve target deterministically from the generated manifest, so the
anchor stays minimal — no hardcoded selector per pair.

mantle's matrix-covered LP test surface is empty: ``test_agni_lp.py``
uses ``protocol="agni"`` (off-matrix), and there is no
``test_uniswap_v3_lp.py`` on this chain. Adding an LP_OPEN case here
would be testing infrastructure the regular intent suite hasn't
validated on mantle, so the anchor stops at SWAP + SUPPLY.

The file is ``no_zodiac``-marked because it deploys its own Safe + Roles
inside ``run_negative_authorisation_case``; if the conftest fixture also
deployed one, the two would conflict on Safe nonces and role-key
collisions.

Tracking issue: #2030. References: PR #2026 (default-on Zodiac), PR #2029
(Phase G.4 — parallel runtime retirement), PR #2037 (initial 7-chain
anchor rollout).
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

CHAIN_NAME = "mantle"


# Canonical negative-anchor case set: one pair per intent-type family the
# manifest generator covers on this chain. Token choices mirror the
# existing positive intent tests on mantle (USDT/WETH on Uniswap V3,
# WETH on Aave V3) so the negative path exercises the same proven
# liquidity / reserve config the positive surface validates.
_CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain=CHAIN_NAME,
        protocol="uniswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDT", "to_token": "WETH", "amount": "100"},
    ),
    PermissionTestCase(
        chain=CHAIN_NAME,
        protocol="aave_v3",
        intent_type="SUPPLY",
        config={"token": "WETH", "amount": "0.1"},
    ),
]


def _case_id(case: PermissionTestCase) -> str:
    """Render a stable, readable parametrize id."""
    return f"{case.protocol}-{case.intent_type}"


@pytest.mark.mantle
@pytest.mark.parametrize("case", _CASES, ids=_case_id)
def test_negative_authorisation_blocks_revoked_target(  # noqa: layers
    case: PermissionTestCase,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Revoking a load-bearing manifest target makes the same intent revert.

    Authorisation, not intent semantics — semantic correctness is covered
    by the regular intent tests on this chain; this file proves the
    manifest is load-bearing. See plan doc
    ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.
    """
    run_negative_authorisation_case(
        case,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
    )
