"""Uniswap V4 and Curve entries repeat by slice while funding remains (blueprint 27 section 7).

Each entry swaps `capital * entry_swap_bps / 10000`, capped by the funding inventory the
allocation holds, and is offered before and after other entries and exits. A reference
that should enter once checks `entry_complete` itself.
"""

import json
from types import SimpleNamespace

import pytest

from almanak.connectors.curve import authoring as curve_authoring
from almanak.connectors.uniswap_v4.observations import initial_owned, quotes_for
from almanak.connectors.uniswap_v4.state import LiquidityPosition
from almanak.models import AssetAmount
from tests.test_curve_economic_model import component, execute
from tests.test_curve_settlement import target as target
from tests.uniswap_support import ROOT, live_state

TRADING = live_state("trading")
LP = live_state("lp")


def with_slice(config, bps):
    raw = config.model_dump(mode="json")
    raw["allocations"][0]["entry_swap_bps"] = bps
    return type(config).model_validate_json(json.dumps(raw))


def unit_quote(amount):
    """A one-for-one quote: these cases size entries and never price them."""
    return SimpleNamespace(
        amount=amount, output=AssetAmount(asset=amount.asset, amount=max(amount.amount, 1))
    )


def holding(config, funding, counter=0, **extra):
    owned = initial_owned(config, config.allocations[0].name)
    values = {owned.inventory0.asset: None, owned.inventory1.asset: None}
    capital = config.allocations[0].capital.asset
    for asset in values:
        values[asset] = funding if asset == capital else counter
    return owned.model_copy(
        update={
            "inventory0": owned.inventory0.model_copy(
                update={"amount": values[owned.inventory0.asset]}
            ),
            "inventory1": owned.inventory1.model_copy(
                update={"amount": values[owned.inventory1.asset]}
            ),
            **extra,
        }
    )


def entry(config, owned, pool):
    quote = quotes_for(config, owned, pool, unit_quote).get("entry_quote")
    return None if quote is None else quote.amount.amount


def test_v4_entry_is_one_slice_of_capital_capped_by_funding_inventory():
    config = with_slice(TRADING.config, 2500)
    pool = TRADING.baseline[0].pool
    capital = config.allocations[0].capital.amount
    assert config.allocations[0].model_dump(mode="json")["entry_swap_bps"] == 2500
    assert entry(config, holding(config, capital), pool) == capital // 4
    # Entries repeat after the first exposure while funding inventory remains.
    after = holding(config, capital - capital // 4, entry_complete=True)
    assert entry(config, after, pool) == capital // 4
    assert entry(config, holding(config, 7, entry_complete=True), pool) == 7
    assert entry(config, holding(config, 0, entry_complete=True), pool) is None
    # Funding beyond capital, for example after a profitable exit, still enters by slice.
    assert entry(config, holding(config, 3 * capital, entry_complete=True), pool) == capital // 4


def test_v4_default_entry_swaps_the_whole_capital_and_repeats_after_an_exit():
    config = TRADING.config
    pool = TRADING.baseline[0].pool
    capital = config.allocations[0].capital.amount
    assert config.allocations[0].entry_swap_bps == 10000
    assert entry(config, holding(config, capital), pool) == capital
    returned = holding(config, capital // 2, counter=0, entry_complete=True)
    assert entry(config, returned, pool) == capital // 2


def test_v4_liquidity_entry_is_offered_only_while_no_position_is_held():
    config = LP.config
    minted = next(o for o in LP.outcomes if o.operation.kind == "mint")
    pool = minted.before.pool
    capital = config.allocations[0].capital.amount
    assert entry(config, holding(config, capital, entry_complete=True), pool) is not None
    target = config.target(config.allocations[0].name)
    held = LiquidityPosition(
        nft=minted.after.owned.position.nft, liquidity=1, fee_growth=None, range=None
    )
    assert target.liquidity_enabled
    owned = holding(config, capital, position=held, entry_complete=True)
    assert entry(config, owned, pool) is None


def test_curve_entries_repeat_by_slice_until_funding_runs_out(target):
    target = target.model_copy(update={"single_coin_exit": None, "liquidity_enabled": False})
    model, state = component(target)
    allocation = model.config.allocations[0]
    capital = next(v.amount for v in allocation.capital.root if v.asset == target.funding_asset)
    assert allocation.entry_swap_bps == 5000
    swaps = 0
    observed = model.observe(state)[0]
    while observed.entry_quote is not None:
        assert observed.entry_quote.amount_in.amount == capital // 2
        assert observed.owned.entry_complete == (swaps > 0)
        intent = curve_authoring.swap(allocation, observed, observed.entry_quote)
        state, result = execute(model, state, intent)
        assert result.result.success
        swaps += 1
        observed = model.observe(state)[0]
    # Two slices spend the whole capital; no budget refused either, only inventory ends it.
    assert swaps == 2
    assert observed.owned.amount(target.funding_asset) == 0


def reference(path):
    namespace = {}
    exec(compile(path.read_text(), str(path), "exec"), namespace)
    return SimpleNamespace(**namespace)


@pytest.mark.parametrize(
    "name", ["uniswap_v4_trading", "uniswap_v4_avalanche_trading", "curve_ethereum_stable_trading"]
)
def test_enter_once_references_ignore_a_repeated_entry_quote(name):
    module = reference(ROOT / f"strategies/{name}/strategy.py")
    calls = []
    actions = SimpleNamespace(
        swap=lambda allocation: calls.append(("swap", allocation)) or "swap",
        exit=lambda allocation: calls.append(("exit", allocation)) or "exit",
    )
    context = SimpleNamespace(memory=module.State(), teardown=False, actions=actions)
    allocation = SimpleNamespace(name="trade", hold_seconds=60)
    quote = object()

    def observed(entered):
        return SimpleNamespace(
            owned=SimpleNamespace(entry_complete=entered),
            entry_quote=quote,
            effective_at=TRADING.baseline[0].effective_at,
        )

    assert module.decide_allocation(context, allocation, observed(False)) == "swap"
    assert module.decide_allocation(context, allocation, observed(True)) is None
    assert calls == [("swap", "trade")]
