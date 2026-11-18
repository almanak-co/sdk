"""Economic band lifecycles move ranges, credit bracketed growth and recentre freely.

The pool state is the captured Uniswap V3 sample; fee growth values are explicit
synthetic brackets supplied per range, not a measured temporal path.
"""

import json
from types import SimpleNamespace

import pytest

from almanak.configuration.strategy_parameters import StrategyParameters
from almanak.connectors.band_fee_growth import Bounds
from almanak.connectors.concentrated_policy import Coverage
from almanak.connectors.dex_economic_parameters import ParametersBinding
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v3 import authoring
from almanak.connectors.uniswap_v3.economic_collection import contract_bindings, resolve_boundaries
from almanak.connectors.uniswap_v3.economic_parameters import _binding as uniswap_binding
from almanak.connectors.uniswap_v3.economic_simulation import quote_at, simulate
from almanak.connectors.uniswap_v3.economic_state import EconomicPosition
from almanak.connectors.uniswap_v3.history import BoundaryEvidence
from almanak.connectors.uniswap_v3.models import Parameters, Target
from almanak.connectors.uniswap_v3.observations import initial_owned
from almanak.connectors.uniswap_v3.validation import validate_inventory
from almanak.connectors.v3_economic_prepare import validate_tick_coverage
from almanak.connectors.v3_economic_validation import swap_limits
from almanak.connectors.v3_fee_growth import accrued
from almanak.connectors.v3_pool_domain import TickBand
from almanak.connectors.v3_pool_positions import SimulatedPosition
from almanak.connectors.v3_pool_receipts import POOL_BURN, POOL_MINT
from almanak.dex_intents import (
    CollectLiquidityFees,
    IncreaseLiquidity,
    OpenLiquidity,
    SwapExactInput,
    TickBounds,
)
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.test_banded_liquidity import BAND, CENTRED, SHIFTED, bounds
from tests.test_uniswap_v3_economic_simulation import setup
from tests.test_uniswap_v3_history import CAPTURE, target_events
from tests.uniswap_v3_support import parameters
from tests.v3_economic_support import observed

BAND_MODEL = "bracketed_historical_fee_growth"
# Synthetic growth per unit liquidity small enough to keep fees within the inventory.
STEP = 2**100


def band_config():
    raw = parameters(0, lp=True).model_dump(mode="json")
    raw["allocations"][0].update(range=None, band=BAND)
    return Parameters.model_validate_json(json.dumps(raw))


class Growth:
    """Explicit synthetic cumulative brackets per concrete range."""

    def __init__(self):
        self.values = {}
        self.requested = []

    def set(self, selected, lower, upper):
        self.values[(selected.lower, selected.upper)] = Bounds(lower=lower, upper=upper)

    def __call__(self, record, target, selected):
        self.requested.append((selected.lower, selected.upper))
        return self.values[(selected.lower, selected.upper)]


def start(config=None):
    config = config or band_config()
    _, record, _ = setup(0)
    target = config.target("position")
    payload = EconomicPosition(target=target, owned=initial_owned(config, "position"))
    allocation = config.allocations[0]
    half = allocation.capital.model_copy(update={"amount": allocation.capital.amount // 2})
    intent = authoring.swap(
        allocation, observed(record, payload), quote_at(record, target, half, record.available_at)
    )
    payload = simulate(config, intent, payload, record, record.available_at, "entry")[0]
    return config, record, payload


def run(config, record, payload, intent, identity, growth):
    return simulate(config, intent, payload, record, record.available_at, identity, growth)


def test_band_positions_retain_their_range_and_credit_only_the_lower_bracket():
    config, record, payload = start()
    allocation, growth = config.allocations[0], Growth()
    intent = authoring.open_liquidity(allocation, observed(record, payload), fraction_bps=5000)
    assert intent.range == bounds(CENTRED)
    with pytest.raises(ValueError, match="prepared interval evidence"):
        run(config, record, payload, intent, "mint", None)
    growth.set(CENTRED, (10, 20), (30, 40))
    payload, _, _, liquidity, kind, _ = run(config, record, payload, intent, "mint", growth)
    position = payload.owned.position
    assert (kind, position.range, position.fee_growth) == ("mint", CENTRED, (10, 20))
    assert (position.bound_growth, position.bound_owed) == ((30, 40), (0, 0))
    # The upper bracket admits three times the credited lower growth.
    growth.set(CENTRED, (10 + STEP, 20 + STEP), (30 + 3 * STEP, 40 + 3 * STEP))
    collect = CollectLiquidityFees[Target](target=payload.target, deadline=10**12)
    payload, deltas, fees, _, _, _ = run(config, record, payload, collect, "collect", growth)
    earned = accrued(liquidity, 0, STEP)
    extra = accrued(liquidity, 0, 3 * STEP) - earned
    assert deltas == fees == (earned, earned) and extra > earned > 0
    assert (payload.unattributed0, payload.unattributed1) == (extra, extra)
    assert payload.owned.position.bound_owed == (0, 0)
    growth.set(CENTRED, (10 + 2 * STEP, 20 + STEP), (30 + 4 * STEP, 40 + 3 * STEP))
    increase = authoring.increase_liquidity(
        allocation, observed(record, payload), fraction_bps=5000
    )
    payload = run(config, record, payload, increase, "increase", growth)[0]
    retained = payload.owned.position
    assert (retained.owed0, retained.owed1) == (earned, 0)
    assert retained.bound_owed == (accrued(liquidity, 0, STEP), 0)
    close = authoring.close_liquidity(allocation, observed(record, payload))
    payload, _, fees, _, kind, _ = run(config, record, payload, close, "close", growth)
    assert (kind, payload.owned.position, fees) == ("close", None, (earned, 0))
    assert (payload.fees0, payload.unattributed0) == (2 * earned, extra)
    assert set(growth.requested) == {(CENTRED.lower, CENTRED.upper)}
    assert EconomicPosition.model_validate_json(payload.model_dump_json()) == payload


def test_a_reopened_band_position_moves_to_its_new_concrete_range():
    config, record, payload = start()
    allocation, growth = config.allocations[0], Growth()
    growth.set(CENTRED, (0, 0), (0, 0))
    growth.set(SHIFTED, (5, 5), (9, 9))
    first = authoring.open_liquidity(allocation, observed(record, payload), fraction_bps=5000)
    payload = run(config, record, payload, first, "first", growth)[0]
    close = authoring.close_liquidity(allocation, observed(record, payload))
    payload = run(config, record, payload, close, "close", growth)[0]
    reopen = OpenLiquidity[Target](
        **authoring.addition(allocation, observed(record, payload), 5000), range=bounds(SHIFTED)
    )
    payload = run(config, record, payload, reopen, "second", growth)[0]
    assert (payload.owned.position.identity, payload.owned.position.range) == ("second", SHIFTED)
    close = authoring.close_liquidity(allocation, observed(record, payload))
    payload = run(config, record, payload, close, "close-again", growth)[0]
    wider = TickBounds(lower=SHIFTED.lower, upper=SHIFTED.upper + 10)
    outside = OpenLiquidity[Target](
        **authoring.addition(allocation, observed(record, payload), 5000), range=wider
    )
    with pytest.raises(ValueError, match="band"):
        run(config, record, payload, outside, "outside", growth)


def deposit(target, record, amount0, kind=OpenLiquidity):
    extra = {"range": bounds(CENTRED)} if kind is OpenLiquidity else {}
    return kind[Target](
        target=target,
        maximum0=AssetAmount(asset=target.assets[0], amount=amount0),
        maximum1=AssetAmount(asset=target.assets[1], amount=10**15),
        deadline=record.available_at + 300,
        **extra,
    )


def test_recentring_repeats_beyond_any_former_quota_while_inventory_lasts():
    # Scope-only authority (blueprint 27): five 40 USDC deposits exceed the 120 USDC
    # deposit quota a budgeted allocation once held, and live and economic admission
    # both bound each deposit by the inventory held now.
    config, record, payload = start()
    target, allocation, growth = config.target("position"), config.allocations[0], Growth()
    growth.set(CENTRED, (0, 0), (0, 0))
    for cycle in range(5):
        opening = deposit(target, record, 40 * 10**6)
        validate_inventory(opening, SimpleNamespace(owned=payload.owned))
        opened = run(config, record, payload, opening, f"open-{cycle}", growth)[0]
        close = authoring.close_liquidity(allocation, observed(record, opened))
        payload = run(config, record, opened, close, f"close-{cycle}", growth)[0]
    assert payload.deposit0 == 200 * 10**6 and payload.owned.position is None
    beyond = deposit(target, record, payload.owned.inventory0.amount + 1)
    with pytest.raises(DataError, match="inventory"):
        validate_inventory(beyond, SimpleNamespace(owned=payload.owned))
    with pytest.raises(ValueError, match="inventory"):
        run(config, record, payload, beyond, "beyond", growth)


def test_band_increases_and_reentries_need_no_deposit_quota():
    config, record, payload = start()
    target, allocation, growth = config.target("position"), config.allocations[0], Growth()
    growth.set(CENTRED, (0, 0), (0, 0))
    payload = run(config, record, payload, deposit(target, record, 40 * 10**6), "first", growth)[0]
    close = authoring.close_liquidity(allocation, observed(record, payload))
    payload = run(config, record, payload, close, "close", growth)[0]
    payload = run(config, record, payload, deposit(target, record, 40 * 10**6), "second", growth)[0]
    increase = deposit(target, record, 5 * 10**6, IncreaseLiquidity)
    payload = run(config, record, payload, increase, "increase", growth)[0]
    payload = run(config, record, payload, increase, "again", growth)[0]
    assert payload.deposit0 == 90 * 10**6
    close = authoring.close_liquidity(allocation, observed(record, payload))
    payload = run(config, record, payload, close, "exit", growth)[0]
    assert payload.owned.position is None
    payload = run(config, record, payload, deposit(target, record, 1), "third", growth)[0]
    assert payload.owned.position is not None


def test_band_liquidity_model_is_explicit_and_refused_where_unsupported():
    author = SimpleNamespace(
        allocations=band_config().allocations,
        asset_metadata={},
        parameters=StrategyParameters(),
    )
    interval = SimpleNamespace(effective=parameters(0).decision_trigger)
    binding = band_config().accounts.bindings[0]
    funds = (SimpleNamespace(name=binding.name, profile=binding.profile),)
    accepted = uniswap_binding.parameters(
        author, SimpleNamespace(liquidity_model=BAND_MODEL, funds=funds), interval
    )
    assert accepted.target("position").band is not None
    with pytest.raises(ValueError, match=BAND_MODEL):
        uniswap_binding.parameters(
            author, SimpleNamespace(liquidity_model="fixed_historical_fee_growth"), interval
        )
    fixed = SimpleNamespace(
        allocations=parameters(0, lp=True).allocations,
        asset_metadata={},
        parameters=StrategyParameters(),
    )
    with pytest.raises(ValueError, match="requires a banded liquidity allocation"):
        uniswap_binding.parameters(fixed, SimpleNamespace(liquidity_model=BAND_MODEL), interval)
    unsupported = ParametersBinding(
        connector=ConnectorId("curve"),
        parameters_type=Parameters,
        position_type=None,
        state_type=None,
        initial_owned=None,
        position_record_type=None,
        portfolio_type=None,
        effects_type=None,
        change_type=None,
    )
    with pytest.raises(ValueError, match="not supported in economic backtests"):
        unsupported.parameters(author, SimpleNamespace(liquidity_model=BAND_MODEL), interval)


def test_simulated_band_ownership_requires_its_range_and_both_brackets():
    config, record, payload = start()
    target = config.target("position")
    base = dict(identity="x", liquidity=1, owed1=0, fee_growth=(0, 0))
    with pytest.raises(ValueError, match="range and both bounds"):
        SimulatedPosition(**base, owed0=0, bound_growth=(0, 0))
    with pytest.raises(ValueError, match="upper-bound fees"):
        SimulatedPosition(**base, range=CENTRED, bound_growth=(0, 0), bound_owed=(0, 0), owed0=1)
    missing = payload.owned.model_copy(update={"position": SimulatedPosition(**base, owed0=0)})
    with pytest.raises(ValueError, match="range and growth bounds"):
        EconomicPosition(target=target, owned=missing)
    fixed = parameters(0, lp=True).target("position")
    with pytest.raises(ValueError, match="no unattributed bound"):
        EconomicPosition(
            target=fixed, owned=initial_owned(parameters(0, lp=True), "position"), unattributed0=1
        )


def test_economic_rebalance_swaps_are_admitted_against_their_retained_quote():
    config, record, payload = start()
    target, allocation = config.target("position"), config.allocations[0]
    amount = payload.owned.inventory0.model_copy(update={"amount": 10**6})
    quote = quote_at(record, target, amount, record.available_at)
    intent = authoring.swap(allocation, observed(record, payload), quote)
    decision = SimpleNamespace(entry_quote=None, exit_quote=None, rebalance_quote=quote)
    swap_limits(intent, decision, allocation)
    with pytest.raises(ValueError, match="retained exact-size modeled quote"):
        swap_limits(
            intent,
            SimpleNamespace(entry_quote=None, exit_quote=None, rebalance_quote=None),
            allocation,
        )
    assert isinstance(intent, SwapExactInput)


def test_band_preparation_samples_the_whole_envelope():
    target = band_config().target("position")
    band = target.band

    def policy(lower, upper):
        coverage = Coverage(allocation="position", lower=lower, upper=upper)
        return SimpleNamespace(for_target=lambda _: coverage, max_bitmap_words=100)

    validate_tick_coverage(target, policy(band.lower, band.upper))
    with pytest.raises(ValueError, match="inside explicit prepared tick coverage"):
        validate_tick_coverage(target, policy(band.lower + 10, band.upper))


def test_band_preparation_authorizes_and_bounds_pool_liquidity_logs():
    config = band_config()
    target = config.target("position")
    chain = target.fund.chain
    settings = SimpleNamespace(
        history=(SimpleNamespace(chain=chain),),
        schedule=SimpleNamespace(start=1, deadline=2),
        data=SimpleNamespace(max_liquidity_log_requests=1),
    )
    first, last = SimpleNamespace(number=1), SimpleNamespace(number=10**7)
    (binding,) = contract_bindings(settings, config, {chain: (first, last)})
    assert [(scope.address, scope.topics) for scope in binding.logs] == [
        (target.pool.pool, (POOL_MINT, POOL_BURN))
    ]
    history = SimpleNamespace(floor=lambda at: first if at == 1 else last)
    with pytest.raises(ValueError, match="needs 5000 liquidity log requests"):
        resolve_boundaries({chain: history}, config, settings)


def test_band_continuity_evidence_retains_pool_events_without_boundary_measurements():
    target, events = target_events(CAPTURE[0])
    width = target.range.upper - target.range.lower
    band = {"lower": target.range.lower - 2 * width, "upper": target.range.upper + 2 * width}
    banded = target.model_copy(update={"range": None, "band": TickBand(**band, width=width)})
    first = events[0].block.model_copy(update={"number": events[0].block.number - 1})
    evidence = dict(target=banded, first=first, last=events[-1].block, contracts=(), inputs=())
    retained = BoundaryEvidence(**evidence, initial_gross=None, final_gross=None, events=events)
    assert retained.events == events
    with pytest.raises(ValueError, match="only a fixed range can claim boundary measurements"):
        BoundaryEvidence(**evidence, initial_gross=(1, 1), final_gross=(1, 1), events=events)
    forged = events[0].model_copy(update={"address": target.contracts.position_manager})
    with pytest.raises(ValueError, match="configured V3 pool"):
        BoundaryEvidence(**evidence, initial_gross=None, final_gross=None, events=(forged,))


def unbounded_config():
    raw = parameters(0, lp=True).model_dump(mode="json")
    raw["allocations"][0].update(range=None, band={"width": BAND["width"]})
    return Parameters.model_validate_json(json.dumps(raw))


def test_unbounded_band_preparation_needs_one_width_of_explicit_coverage():
    target = unbounded_config().target("position")

    def policy(lower, upper):
        coverage = Coverage(allocation="position", lower=lower, upper=upper)
        return SimpleNamespace(for_target=lambda _: coverage, max_bitmap_words=10**4)

    validate_tick_coverage(target, policy(-BAND["width"], 0))
    with pytest.raises(ValueError, match="narrower than one liquidity band width"):
        validate_tick_coverage(target, policy(-BAND["width"] + 10, 0))


def test_unbounded_band_opens_the_centred_range_in_the_economic_simulation():
    config, record, payload = start(unbounded_config())
    allocation = config.allocations[0]
    intent = authoring.open_liquidity(allocation, observed(record, payload))
    expected = TickBand(width=BAND["width"]).centered(
        record.payload.pool.tick, payload.target.pool.tick_spacing
    )
    assert intent.range == bounds(expected)
    growth = Growth()
    growth.set(expected, (0, 0), (0, 0))
    after = run(config, record, payload, intent, "unbounded-mint", growth)[0]
    assert after.owned.position.range == expected
