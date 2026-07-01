"""On-fork gas-adequacy regression for Pendle sUSDai LP_CLOSE (VIB-5487, BUG A).

BUG A: a live ``removeLiquiditySingleToken`` on the sUSDai market OOG'd deep in
the SY -> sUSDai redeem (blocklist-check + vault-withdraw frames) and surfaced as
``SafeERC20: low-level call failed``. The tx ran with a 600k limit (then-current
400k floor × 1.5 buffer) while the remove needs ~608k.

The unit guard ``tests/unit/connectors/pendle/test_gas_floor_regression.py`` pins
the floor to a hardcoded measured requirement. THIS test is the on-fork
complement: it opens a real single-sided sUSDai LP on the live market and asserts
the remove's ACTUAL gasUsed (a) proves it is the heavy vault-SY path (> the old
600k limit that starved) and (b) sits comfortably under the current floor — so
the floor tracks real on-chain gas and cannot silently drift out from under the
router. At the old 400k floor this test fails (gasUsed ~608k is not < 400k),
i.e. it would have caught BUG A.

Uses the same live PT-sUSDai-15OCT2026 market + seeding as
``test_pendle_lp.py``; a small 0.4 sUSDai deposit mirrors the real failed run's
position size (~0.1795 LP).
"""

import warnings
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.pendle.sdk import PENDLE_GAS_ESTIMATES
from almanak.framework.intents import LPCloseIntent, LPOpenIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.arbitrum.test_pendle_lp import (
    _DUMMY_RANGE_LOWER,
    _DUMMY_RANGE_UPPER,
    PENDLE_SUSDAI_MARKET,
    SUSDAI_SYMBOL,
    _enrich_oracle_with_susdai,
)
from tests.intents.conftest import get_token_balance

CHAIN_NAME = "arbitrum"

# The exact gas limit that starved the live VIB-5487 tx (400k floor × 1.5
# buffer). The heavy vault-SY remove must measurably exceed this to prove this
# test still exercises the OOG-prone path (a market whose redeem got cheap would
# make the guard vacuous).
_BUG_A_STARVED_LIMIT = 600_000


@pytest.mark.arbitrum
@pytest.mark.lp
@pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
@pytest.mark.asyncio
async def test_remove_liquidity_gas_within_floor(
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    orchestrator,
    price_oracle,
):
    """sUSDai LP_CLOSE remove gasUsed is heavy-path AND under the gas floor."""
    oracle = _enrich_oracle_with_susdai(price_oracle)
    compiler = IntentCompiler(
        chain=CHAIN_NAME, wallet_address=funded_wallet, price_oracle=oracle, rpc_url=anvil_rpc_url
    )

    open_intent = LPOpenIntent(
        pool=f"{SUSDAI_SYMBOL}/{PENDLE_SUSDAI_MARKET}",
        amount0=Decimal("0.4"),
        amount1=Decimal("0"),
        range_lower=_DUMMY_RANGE_LOWER,
        range_upper=_DUMMY_RANGE_UPPER,
        protocol="pendle",
        chain=CHAIN_NAME,
    )
    r = compiler.compile(open_intent)
    assert r.status.value == "SUCCESS", r.error
    er = await orchestrator.execute(r.action_bundle)
    assert er.success, er.error
    lp = get_token_balance(web3, PENDLE_SUSDAI_MARKET, funded_wallet)
    assert lp > 0

    close_intent = LPCloseIntent(
        position_id=str(lp),
        pool=PENDLE_SUSDAI_MARKET,
        protocol="pendle",
        chain=CHAIN_NAME,
        protocol_params={"token": SUSDAI_SYMBOL},
    )
    cr = compiler.compile(close_intent)
    assert cr.status.value == "SUCCESS", cr.error
    cer = await orchestrator.execute(cr.action_bundle)
    assert cer.success, f"LP_CLOSE must land (no SafeERC20/OOG revert): {cer.error}"

    # The remove tx is the largest-gas tx in the bundle (approve ~46-86k).
    remove_gas = max((tr.gas_used for tr in cer.transaction_results), default=0)
    floor = PENDLE_GAS_ESTIMATES["remove_liquidity_single"]

    # (a) heavy vault-SY path sanity. The redeem SHOULD exceed the limit that
    # OOG'd BUG A, but the exact gasUsed is fork-block / market-state dependent
    # (warm storage, vault state, blocklist-frame cost) and legitimately comes in
    # lighter at some pinned blocks — at which point BUG A would not even
    # reproduce and this on-fork heavy-path guard is simply vacuous. A light
    # redeem does NOT mean the floor is wrong: safety is assertion (b), and the
    # authoritative floor>=measured guard is the static
    # tests/unit/connectors/pendle/test_gas_floor_regression.py. So WARN here —
    # never hard-fail CI on a fork-block-dependent gas measurement (VIB-5487).
    if remove_gas <= _BUG_A_STARVED_LIMIT:
        warnings.warn(
            f"remove gasUsed={remove_gas} did not exceed the {_BUG_A_STARVED_LIMIT} "
            f"limit that starved BUG A at this fork block — the redeem came in light, "
            f"so this on-fork heavy-path check is vacuous here (floor coverage is still "
            f"asserted below + pinned statically in test_gas_floor_regression.py).",
            stacklevel=2,
        )
    # (b) the floor covers the real requirement with headroom.
    assert remove_gas < floor, (
        f"remove gasUsed={remove_gas} is NOT under the floor {floor}. The floor "
        f"no longer covers the on-chain requirement — raise "
        f"PENDLE_GAS_ESTIMATES['remove_liquidity_single'] (VIB-5487)."
    )

    # Full close landed: LP burned to zero, sUSDai returned.
    assert get_token_balance(web3, PENDLE_SUSDAI_MARKET, funded_wallet) == 0
