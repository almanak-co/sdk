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

This anchor closes that gap. The SUPPLY token is USDC — the existing
``test_aave_v3_lending.py`` happy-path tests use USDC after #2102 surfaced
that the WETH reserve is frozen on Mantle (LTV=0, isFrozen=true). The
SWAP pair (USDT → WETH) mirrors ``test_uniswap_swap.py``. The harness's
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

from almanak.framework.intents.vocabulary import IntentType
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
# WETH on Aave V3).
#
# SUPPLY uses USDC: WETH is frozen on Mantle Aave V3 (#2102), so supplying
# WETH reverts at the Aave layer for reasons unrelated to authorisation.
# USDC is active and non-frozen, which gives the harness a clean
# authz-revoke signal to assert against.
_CASES: list = [
    pytest.param(
        PermissionTestCase(
            chain=CHAIN_NAME,
            protocol="uniswap_v3",
            intent_type="SWAP",
            config={"from_token": "USDT", "to_token": "WETH", "amount": "100"},
        ),
        id="uniswap_v3-SWAP",
    ),
    pytest.param(
        PermissionTestCase(
            chain=CHAIN_NAME,
            protocol="aave_v3",
            intent_type="SUPPLY",
            config={"token": "USDC", "amount": "100"},
        ),
        id="aave_v3-SUPPLY",
    ),
]


@pytest.mark.intent(IntentType.SWAP, IntentType.SUPPLY)
@pytest.mark.mantle
@pytest.mark.parametrize("case", _CASES)
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
