"""Author fields are optional where they have no effect, and kept where they do.

`collect_seconds` is read only by strategy code; `entry_swap_bps: 0` offers no entry
quote; a minimum health factor bounds nothing where no allocation can borrow.
"""

from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak.connectors.funding import entry_slices
from almanak.connectors.lending_health import NO_DEBT_FLOOR, health_floor
from almanak.connectors.v3_pool_observations import entry_amount
from tests.test_dx_schemas import validator
from tests.test_optional_hold_seconds import Actions, reference, resolve, strategy_module

LIQUIDITY = [
    "uniswap_v4_lp",
    "uniswap_v3_base_lp",
    "pancakeswap_v3_base_lp",
    "aerodrome_slipstream_base_lp",
    "aerodrome_base_stable_lp",
    "curve_ethereum_stable_lp",
]
TRADING = [
    "uniswap_v4_trading",
    "uniswap_v3_base_trading",
    "pancakeswap_v3_base_trading",
    "aerodrome_slipstream_base_trading",
    "aerodrome_base_stable_trading",
    "curve_ethereum_stable_trading",
]
LENDING = [
    "aave_v3_base_lending",
    "sparklend_ethereum_lending",
    "morpho_blue_polygon_lending",
    "compound_v3_base_lending",
]


def allocations(raw):
    return raw["strategy"]["allocations"]


def schema_valid(raw):
    return validator("strategy.schema.json").is_valid(raw)


def test_a_v4_liquidity_allocation_without_collection_timing_resolves():
    raw = reference("uniswap_v4_lp")
    for allocation in allocations(raw).values():
        allocation.pop("collect_seconds")
    assert schema_valid(raw)
    for resolved in resolve(raw, "uniswap_v4_lp").values():
        assert resolved["collect_seconds"] is None


def test_stated_collection_timing_resolves_as_before():
    for resolved in resolve(reference("uniswap_v4_lp"), "uniswap_v4_lp").values():
        assert resolved["collect_seconds"] == 21600


def test_a_v4_trading_allocation_still_refuses_collection_timing():
    raw = reference("uniswap_v4_trading")
    for allocation in allocations(raw).values():
        allocation["collect_seconds"] = "6h"
    assert not schema_valid(raw)
    with pytest.raises(ValueError, match="only a liquidity allocation"):
        resolve(raw, "uniswap_v4_trading")


def with_entry(name, bps):
    raw = reference(name)
    for allocation in allocations(raw).values():
        allocation["entry_swap_bps"] = bps
    return raw


@pytest.mark.parametrize("name", LIQUIDITY)
def test_a_liquidity_allocation_may_enter_without_a_swap(name):
    raw = with_entry(name, 0)
    assert schema_valid(raw)
    for resolved in resolve(raw, name).values():
        assert resolved["entry_swap_bps"] == 0


@pytest.mark.parametrize("name", TRADING)
def test_a_trading_allocation_still_requires_a_positive_entry_swap(name):
    raw = with_entry(name, 0)
    assert not schema_valid(raw)
    with pytest.raises(ValueError, match="positive entry_swap_bps"):
        resolve(raw, name)


@pytest.mark.parametrize("name", LIQUIDITY + TRADING)
def test_a_stated_entry_swap_resolves_as_before(name):
    stated = {
        key: allocation.get("entry_swap_bps", 10000)
        for key, allocation in allocations(reference(name)).items()
    }
    resolved = resolve(reference(name), name)
    assert {key: value.get("entry_swap_bps", 10000) for key, value in resolved.items()} == stated


def test_a_zero_entry_share_offers_no_entry_quote():
    funding = SimpleNamespace(asset="usdc", amount=10**9)
    owned = SimpleNamespace(
        target=SimpleNamespace(funding_currency="usdc"), inventory0=funding, inventory1=funding
    )
    allocation = SimpleNamespace(capital=SimpleNamespace(amount=10**9), entry_swap_bps=0)
    assert entry_amount(allocation, owned) is None
    assert entry_slices(10**9, 0) == 0


def debt_free(name):
    raw = reference(name)
    for allocation in allocations(raw).values():
        allocation.pop("borrow", None)
        allocation.pop("minimum_health_factor")
    return raw


@pytest.mark.parametrize("name", LENDING)
def test_allocations_that_cannot_borrow_may_omit_the_health_factor(name):
    raw = debt_free(name)
    assert schema_valid(raw)
    for resolved in resolve(raw, name).values():
        assert "minimum_health_factor" not in resolved


@pytest.mark.parametrize("name", LENDING)
def test_a_borrowing_allocation_still_requires_the_health_factor(name):
    raw = reference(name)
    borrowing = [a for a in allocations(raw).values() if a.get("borrow")]
    assert borrowing
    for allocation in borrowing:
        allocation.pop("minimum_health_factor")
    assert not schema_valid(raw)
    with pytest.raises(ValueError, match="requires minimum_health_factor"):
        resolve(raw, name)


@pytest.mark.parametrize("name", LENDING)
def test_a_stated_health_factor_resolves_as_before(name):
    raw = reference(name)
    for resolved in resolve(deepcopy(raw), name).values():
        assert resolved["minimum_health_factor"] == 2 * 10**18


def test_an_omitted_floor_tolerates_no_debt():
    assert health_floor(2 * 10**18) == 2 * 10**18
    assert health_floor(None) == NO_DEBT_FLOOR == 2**256 - 1


V4_FIXED_RANGE_LP = [
    "uniswap_v4_lp",
    "uniswap_v4_arbitrum_lp",
    "uniswap_v4_avalanche_lp",
    "uniswap_v4_base_lp",
    "uniswap_v4_bnb_lp",
    "uniswap_v4_optimism_lp",
    "uniswap_v4_polygon_lp",
    "uniswap_v4_robinhood_lp",
]
MINTED = 1_000


def held_decision(name, collect_seconds, hold_seconds=None, elapsed=10**7, teardown=False):
    strategy = strategy_module(name)
    observed = SimpleNamespace(
        allocation="lp",
        effective_at=datetime.fromtimestamp(MINTED + elapsed, UTC),
        owned=SimpleNamespace(position=SimpleNamespace(liquidity=1), entry_complete=True),
    )
    allocation = SimpleNamespace(
        name="lp", collect_seconds=collect_seconds, hold_seconds=hold_seconds
    )
    context = SimpleNamespace(
        memory=strategy.State(minted_at={"lp": MINTED}),
        teardown=teardown,
        actions=Actions(),
    )
    return strategy.decide_allocation(context, allocation, observed)


@pytest.mark.parametrize("name", V4_FIXED_RANGE_LP)
def test_a_held_v4_position_without_collection_timing_holds(name):
    assert held_decision(name, None) is None


@pytest.mark.parametrize("name", V4_FIXED_RANGE_LP)
def test_a_held_v4_position_without_collection_timing_still_exits_on_time(name):
    assert held_decision(name, None, hold_seconds=60) == ("close_liquidity", "lp")
    assert held_decision(name, None, teardown=True) == ("close_liquidity", "lp")


@pytest.mark.parametrize("name", V4_FIXED_RANGE_LP)
def test_a_stated_collection_timing_still_collects_once_due(name):
    assert held_decision(name, 60) == ("collect", "lp")
    assert held_decision(name, 60, elapsed=59) is None
