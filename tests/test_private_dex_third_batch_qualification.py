"""Third-batch DEX production admission and its exact production rehearsal selection."""

from datetime import UTC, datetime

import pytest

from almanak.caip import ChainId
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.support import OP_FAMILY_CHAINS, EconomicAction, RunMode
from tests.strategy_fork.rehearsal import production_references

ETHEREUM = "eip155:1"
ARBITRUM = "eip155:42161"
BASE = "eip155:8453"
OPTIMISM = "eip155:10"
# Retained backtest boundaries are unchanged by production admission.
CELLS = {
    ("uniswap_v3", ETHEREUM): datetime(2026, 9, 6, 23, 57, 23, tzinfo=UTC),
    ("uniswap_v3", ARBITRUM): datetime(2026, 9, 8, 8, 17, 7, tzinfo=UTC),
    ("uniswap_v3", BASE): datetime(2026, 9, 6, 23, 57, 29, tzinfo=UTC),
    ("uniswap_v3", OPTIMISM): datetime(2026, 9, 8, 4, 34, 47, tzinfo=UTC),
    ("uniswap_v4", ARBITRUM): datetime(2026, 8, 20, tzinfo=UTC),
    ("uniswap_v4", BASE): datetime(2026, 8, 20, tzinfo=UTC),
    ("uniswap_v4", OPTIMISM): datetime(2026, 8, 20, tzinfo=UTC),
    ("pancakeswap_v3", ETHEREUM): datetime(2026, 9, 7, 9, 40, tzinfo=UTC),
    ("pancakeswap_v3", ARBITRUM): datetime(2026, 9, 7, 9, 40, tzinfo=UTC),
    ("pancakeswap_v3", BASE): datetime(2026, 9, 7, 9, 20, tzinfo=UTC),
    ("aerodrome", BASE): datetime(2026, 9, 7, 11, 0, tzinfo=UTC),
    ("aerodrome_slipstream", BASE): datetime(2026, 9, 7, 10, 0, tzinfo=UTC),
}
PRODUCTION_REFERENCES = {
    *(
        f"aerodrome_base_{pool}_{kind}"
        for pool in ("stable", "volatile")
        for kind in ("lp", "trading")
    ),
    "aerodrome_slipstream_base_lp",
    "aerodrome_slipstream_base_lp_band",
    "aerodrome_slipstream_base_trading",
    *(
        f"pancakeswap_v3_{chain}_{kind}"
        for chain in ("ethereum", "arbitrum", "base")
        for kind in ("lp", "trading")
    ),
    *(
        f"uniswap_v3_{chain}_{kind}"
        for chain in ("ethereum", "arbitrum", "base", "optimism")
        for kind in ("lp", "trading")
    ),
    *(
        f"uniswap_v4_{chain}_{kind}"
        for chain in ("arbitrum", "base", "optimism")
        for kind in ("lp", "trading")
    ),
    # Aave V3 already declares Ethereum production support.
    "ethereum_aave_pancake_borrow_and_trade",
}
# Aave V3 compositions of the third-batch cells, admitted with Aave V3 production on
# Arbitrum, Base and Optimism. The two-chain Ethereum/Base composition was admitted
# after #73 cut its multi-chain checkpoint cost.
ADMITTED_AAVE_COMPOSED = {
    "arbitrum_aave_borrow_and_trade",
    "arbitrum_aave_borrow_and_trade_v4",
    "arbitrum_aave_pancake_borrow_and_trade",
    "base_aave_aerodrome_stable_borrow_and_trade",
    "base_aave_aerodrome_volatile_borrow_and_trade",
    "base_aave_pancake_borrow_and_trade",
    "base_aave_slipstream_borrow_and_trade",
    "base_borrow_and_trade",
    "base_borrow_and_trade_v4",
    "ethereum_base_borrow_and_trade",
    "optimism_aave_borrow_and_trade",
    "optimism_aave_borrow_and_trade_v4",
}
# The fourth batch admits the Compound and Morpho compositions on Arbitrum and Base.
ADMITTED_LENDING_COMPOSED = {
    "arbitrum_compound_borrow_and_trade",
    "arbitrum_morpho_borrow_and_trade",
    "base_compound_borrow_and_trade",
    "base_morpho_borrow_and_trade",
}
THIRD_BATCH_PREFIXES = (
    "aerodrome_",
    "pancakeswap_v3_ethereum",
    "pancakeswap_v3_arbitrum",
    "pancakeswap_v3_base",
    *(f"uniswap_v3_{chain}" for chain in ("ethereum", "arbitrum", "base", "optimism")),
    *(f"uniswap_v4_{chain}" for chain in ("arbitrum", "base", "optimism")),
    "ethereum_aave_pancake",
)


@pytest.mark.parametrize(("connector", "chain"), sorted(CELLS))
def test_third_batch_cell_admits_production_with_unchanged_scope(connector, chain):
    support = connector_from_wire({"connector": connector}).support_for(ChainId(chain))
    assert support.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    assert support.account_profiles == ("eoa", "safe_roles")
    for action in support.actions:
        support.require(RunMode.PRODUCTION, action, "eoa")
        support.require(RunMode.PRODUCTION, action, "safe_roles")
    assert not any("production unavailable" in item for item in support.restrictions)
    assert not any("not promoted" in item for item in support.restrictions)
    assert support.historical
    assert {h.backtest_start_at for h in support.historical} == {CELLS[connector, chain]}
    assert ("pinned Jovian L1 and operator fee accounting" in support.restrictions) == (
        support.chain in OP_FAMILY_CHAINS
    )
    assert all(
        EconomicAction.REVOKE not in h.actions
        for h in support.historical
        if connector != "uniswap_v4"
    )


def test_third_batch_production_rehearsal_selects_exactly_the_eligible_references():
    admitted = set(production_references())
    selected = {name for name in admitted if name.startswith(THIRD_BATCH_PREFIXES)}
    assert selected == PRODUCTION_REFERENCES
    assert len(PRODUCTION_REFERENCES) == 28
    assert ADMITTED_AAVE_COMPOSED | ADMITTED_LENDING_COMPOSED <= admitted
