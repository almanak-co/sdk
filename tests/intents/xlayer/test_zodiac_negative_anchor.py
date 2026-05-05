"""X-Layer Zodiac negative-authorisation anchor.

Per-chain "negative anchor" companion to the regular intent tests under
``tests/intents/xlayer/``. The default-on Zodiac model wired up in
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
every other chain, including X-Layer.

Coverage on this chain follows what regular intent tests exercise:
``uniswap_v3`` SWAP and ``aave_v3`` SUPPLY. LP_OPEN is intentionally
omitted until ``tests/intents/xlayer/test_uniswap_v3_lp.py`` lands —
adding a negative anchor for an intent type that has no positive
counterpart on the chain would diverge from the per-chain template.

The file is ``no_zodiac``-marked because it deploys its own Safe + Roles
inside ``run_negative_authorisation_case``; if the conftest fixture also
deployed one, the two would conflict on Safe nonces and role-key
collisions.

Tracking issue: #2094. References: PR #2026 (default-on Zodiac), PR #2029
(Phase G.4 — parallel runtime retirement), PR #2092 (mantle anchor — same
template).
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

CHAIN_NAME = "xlayer"


# Canonical negative-anchor case set: one pair per intent-type family the
# manifest generator covers on this chain.
#
# uniswap_v3 SWAP carries an xfail marker referencing #2106. xlayer's
# Uniswap V3 deployment (governance-deployed at non-canonical addresses)
# has near-zero usable liquidity on every pool involving tokens currently
# in the xlayer test conftest: USDC/WETH has no pool at any fee tier;
# USDC/USDT0 (fee=500), USDC/WOKB and USDT0/WETH (fee=100) all return
# 91–100% price impact for 1–100 USDC swap-in via the QuoterV2. The case
# is kept here (not deleted) so the negative-anchor suite still documents
# the manifest-regression surface; ``strict=True`` will surface the
# resolution as an ``XPASS`` failure once liquidity returns or the test
# conftest gains a swap pair with depth.
_CASES: list = [
    pytest.param(
        PermissionTestCase(
            chain=CHAIN_NAME,
            protocol="uniswap_v3",
            intent_type="SWAP",
            config={"from_token": "USDT0", "to_token": "WETH", "amount": "100"},
        ),
        marks=pytest.mark.xfail(
            reason="#2106: xlayer Uniswap V3 has near-zero usable liquidity on every "
            "pool involving conftest tokens (as of 2026-05-05). Compile-time "
            "price-impact guard rejects the intent before authorisation can be "
            "asserted. Strict so re-enabled liquidity surfaces as XPASS.",
            strict=True,
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


# Layer-validator escape hatch: this file asserts authorisation, not intent
# semantics. Semantic correctness is covered by the regular intent tests on
# this chain; this file proves the manifest is load-bearing. See
# ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.
@pytest.mark.xlayer
@pytest.mark.parametrize("case", _CASES)  # noqa: layers
def test_negative_authorisation_blocks_revoked_target(
    case: PermissionTestCase,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Revoking a load-bearing manifest target makes the same intent revert."""
    run_negative_authorisation_case(
        case,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
    )
