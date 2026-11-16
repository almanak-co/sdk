"""Banded liquidity recentres inside an owner-approved envelope; fixed ranges stay exact."""

import json
from datetime import UTC, datetime
from fractions import Fraction
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode

from almanak.caip import AssetId
from almanak.connectors.concentrated_math import amounts, sqrt_at_tick
from almanak.connectors.uniswap_v3 import abi, liquidity_permissions, planning, receipts
from almanak.connectors.uniswap_v3.api import Actions
from almanak.connectors.uniswap_v3.authority import admit, declared
from almanak.connectors.uniswap_v3.domain import TickRange
from almanak.connectors.uniswap_v3.models import Allocation, Parameters, Target
from almanak.connectors.uniswap_v3.observations import initial_owned, quotes_for
from almanak.connectors.uniswap_v3.positions import Allowance, Ownership, Position, Quote
from almanak.connectors.uniswap_v3.reader import nft_position
from almanak.connectors.v3_pool_author_inputs import (
    TICK_BASE,
    BandInput,
    validate_shape_inputs,
    width_from_bps,
)
from almanak.connectors.v3_pool_domain import TickBand
from almanak.dex_intents import OpenLiquidity, TickBounds
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_conditions import signed_at_least, signed_at_most
from almanak.models import AssetAmount, Block
from tests.uniswap_v3_support import captured_rows, parameters

ROW = captured_rows()[0]
TICK = ROW["observed"]["tick"]
SPACING = ROW["source"]["tick_spacing"]
ALIGNED = TICK // SPACING * SPACING
BAND = {"lower": ALIGNED - 3000, "upper": ALIGNED + 3000, "width": 1000}
CENTRED = TickRange(lower=ALIGNED - 500, upper=ALIGNED + 500)
SHIFTED = TickRange(lower=CENTRED.lower + SPACING, upper=CENTRED.upper + SPACING)


def band_parameters(**band):
    config = parameters(0, lp=True)
    raw = config.model_dump(mode="json")
    raw["allocations"][0].update(range=None, band=band or BAND)
    return Parameters.model_validate_json(json.dumps(raw))


def bounds(selected):
    return TickBounds(lower=selected.lower, upper=selected.upper)


def block():
    return Block.model_validate_json(json.dumps(ROW["observed"]["block"]))


def pool():
    return SimpleNamespace(sqrt_price_x96=ROW["observed"]["sqrt_price_x96"], tick=TICK)


def owned_for(config, amount0, amount1, **extra):
    target = config.target("position")
    return Ownership(
        target=target,
        inventory0=AssetAmount(asset=target.assets[0], amount=amount0),
        inventory1=AssetAmount(asset=target.assets[1], amount=amount1),
        **extra,
    )


def nft(target, token=77):
    return AssetId(
        f"{target.fund.chain.chain_id}/erc721:{target.contracts.position_manager}/{token}"
    )


def test_band_centres_on_the_aligned_tick_and_clamps_inside_the_envelope():
    band = TickBand(**BAND)
    assert band.centered(TICK, SPACING) == CENTRED
    low = band.centered(band.lower - 5000, SPACING)
    high = band.centered(band.upper + 5000, SPACING)
    assert (low.lower, low.upper) == (band.lower, band.lower + band.width)
    assert (high.lower, high.upper) == (band.upper - band.width, band.upper)
    assert band.validate_range(bounds(CENTRED), SPACING) == CENTRED
    for lower, upper in (
        (CENTRED.lower + 1, CENTRED.upper + 1),
        (band.lower - SPACING, band.lower - SPACING + band.width),
        (CENTRED.lower, CENTRED.upper + SPACING),
    ):
        with pytest.raises(ValueError):
            band.validate_range(TickBounds(lower=lower, upper=upper), SPACING)
    with pytest.raises(ValueError):
        TickBand(lower=0, upper=100, width=200)
    with pytest.raises(ValueError):
        TickBand(lower=0, upper=100, width=25).validate_pool(SimpleNamespace(tick_spacing=10))


@pytest.mark.parametrize(("bps", "spacing", "width"), [(500, 10, 980), (500, 60, 1080), (1, 1, 2)])
def test_width_in_basis_points_covers_the_distance_on_both_sides(bps, spacing, width):
    assert width_from_bps(bps, spacing) == width
    half = width // 2
    ratio = 1 + Fraction(bps, 10000)
    assert TICK_BASE**half >= ratio
    assert half == spacing or TICK_BASE ** (half - spacing) < ratio
    assert BandInput(lower=-1000, upper=1000, width_bps=bps).resolve(spacing)["width"] == width


def test_band_input_declares_exactly_one_width_and_one_shape():
    with pytest.raises(ValueError):
        BandInput(lower=0, upper=100, width=10, width_bps=50)
    with pytest.raises(ValueError):
        BandInput(lower=0, upper=100)
    with pytest.raises(ValueError):
        validate_shape_inputs(object(), object())
    validate_shape_inputs(None, object())


def test_allocation_declares_a_range_or_a_band_aligned_to_the_pool():
    config = band_parameters()
    allocation = config.allocations[0]
    target = config.target("position")
    assert (allocation.range, allocation.liquidity_enabled) == (None, True)
    assert (target.range, target.band, target.liquidity_enabled) == (None, TickBand(**BAND), True)
    raw = allocation.model_dump(mode="json")
    with pytest.raises(ValueError):
        Allocation.model_validate_json(json.dumps({**raw, "range": {"lower": -100, "upper": 100}}))
    with pytest.raises(ValueError):
        Allocation.model_validate_json(json.dumps({**raw, "band": {**BAND, "width": 995}}))
    assert "band" not in parameters(0, lp=True).allocations[0].model_dump(mode="json")


def test_signed_tick_conditions_admit_the_band_edges_and_refuse_their_neighbours():
    lower, upper = signed_at_least(BAND["lower"]), signed_at_most(BAND["upper"])
    assert (lower.parameter, lower.operator, upper.parameter, upper.operator) == (1, 19, 1, 20)
    assert decode(("int256",), bytes.fromhex(lower.value[2:])) == (BAND["lower"] - 1,)
    assert decode(("int256",), bytes.fromhex(upper.value[2:])) == (BAND["upper"] + 1,)


def mint_grant(config):
    capability = declared(config).requirements[0]
    profile = config.accounts.bindings[0].profile
    grants = liquidity_permissions.derive(profile, capability)
    (grant,) = [g for g in grants if g.signature.startswith("mint(")]
    return grant


def test_mint_grants_leave_ticks_to_the_gateway_for_bands_and_fixed_ranges():
    # Blueprint 27 section 4.3: the range or band is an economic choice the gateway enforces.
    domain = (signed_at_least(-887272), signed_at_most(887272))
    banded, fixed = mint_grant(band_parameters()), mint_grant(parameters(0, lp=True))
    for grant in (banded, fixed):
        assert grant.condition.children[0].children[3:5] == domain
        assert "the gateway enforces the configured range or band" in grant.explanation[0]
    # Neither the band envelope nor the fixed range reaches the grant: a new band needs no
    # new permissions.
    assert banded == fixed
    wider = band_parameters(lower=ALIGNED - 6000, upper=ALIGNED + 6000, width=2000)
    assert mint_grant(wider) == banded


def test_gateway_refuses_a_mint_outside_the_fixed_range_or_band():
    from almanak.connectors.uniswap_v3.operations import Mint
    from almanak.connectors.v3_pool_planning import concrete_range

    fixed = parameters(0, lp=True)
    target = fixed.target("position")
    selected = fixed.allocations[0].range
    other = TickRange(lower=selected.lower + SPACING, upper=selected.upper + SPACING)
    with pytest.raises(ValueError, match="fixed-range liquidity cannot select another"):
        concrete_range(target, requested=bounds(other))
    zero = tuple(AssetAmount(asset=asset, amount=0) for asset in target.assets)
    one = tuple(AssetAmount(asset=asset, amount=1) for asset in target.assets)
    with pytest.raises(ValueError, match="configured fixed range"):
        Mint(
            target=target,
            maximum0=one[0],
            maximum1=one[1],
            minimum0=zero[0],
            minimum1=zero[1],
            minimum_liquidity=1,
            deadline=1,
            range=other,
        )
    config = band_parameters()
    owned = owned_for(config, 10**8, 10**17)
    outside = TickRange(lower=BAND["upper"], upper=BAND["upper"] + BAND["width"])
    with pytest.raises(ValueError):
        admit(declared(config), open_intent(config, owned, outside))


def open_intent(config, owned, selected):
    return OpenLiquidity[Target](
        target=owned.target,
        maximum0=owned.inventory0,
        maximum1=owned.inventory1,
        deadline=block().timestamp + 300,
        **({} if selected is None else {"range": bounds(selected)}),
    )


def before_for(config, owned):
    target = owned.target
    return SimpleNamespace(
        target=target,
        owned=owned,
        pool=pool(),
        block=block(),
        allowances=[
            Allowance(
                asset=inventory.asset,
                spender=target.contracts.position_manager,
                amount=inventory.amount,
            )
            for inventory in (owned.inventory0, owned.inventory1)
        ],
    )


def planned(config, owned, selected):
    state = SimpleNamespace(capabilities=declared(config))
    return planning.operation(
        config, state, open_intent(config, owned, selected), before_for(config, owned)
    )


def test_planner_mints_the_selected_band_range_and_encodes_it():
    config = band_parameters()
    owned = owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    operation = planned(config, owned, CENTRED)
    assert (operation.kind, operation.range) == ("mint", CENTRED)
    params = decode((abi.MINT_PARAMS,), abi.mint(operation)[4:])[0]
    assert params[3:5] == (CENTRED.lower, CENTRED.upper)
    assert "range" in operation.model_dump(mode="json")
    price = pool().sqrt_price_x96
    assert amounts(
        operation.minimum_liquidity, price, sqrt_at_tick(CENTRED.lower), sqrt_at_tick(CENTRED.upper)
    ) <= (owned.inventory0.amount, owned.inventory1.amount)


def test_planner_refuses_ranges_outside_the_band_or_on_fixed_targets():
    config = band_parameters()
    owned = owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    outside = TickRange(lower=BAND["upper"] - 500, upper=BAND["upper"] + 500)
    with pytest.raises(ValueError, match="band"):
        planned(config, owned, outside)
    with pytest.raises(ValueError, match="band"):
        planned(config, owned, None)
    fixed = parameters(0, lp=True)
    owned = owned_for(fixed, 50 * 10**6, 2 * 10**16, entry_complete=True)
    selected = fixed.allocations[0].range
    with pytest.raises(ValueError, match="fixed-range"):
        planned(fixed, owned, TickRange(lower=selected.lower + SPACING, upper=selected.upper))
    operation = planned(fixed, owned, None)
    assert (operation.kind, operation.range) == ("mint", None)
    assert "range" not in operation.model_dump(mode="json")
    with pytest.raises(ValueError, match="fixed-range"):
        admit(declared(fixed), open_intent(fixed, owned, selected))


def test_close_and_collect_carry_the_retained_band_range():
    config = band_parameters()
    target = config.target("position")
    position = Position(
        nft=nft(target), liquidity=10**12, owed0=0, owed1=0, fee_growth=(0, 0), range=CENTRED
    )
    owned = owned_for(config, 0, 0, position=position, entry_complete=True)
    before = before_for(config, owned)
    from almanak.dex_intents import CloseLiquidity, CollectLiquidityFees

    state = SimpleNamespace(capabilities=declared(config))
    principal = amounts(
        position.liquidity,
        pool().sqrt_price_x96,
        sqrt_at_tick(CENTRED.lower),
        sqrt_at_tick(CENTRED.upper),
    )
    close = planning.operation(
        config,
        state,
        CloseLiquidity[Target](
            target=target,
            minimum0=AssetAmount(asset=target.assets[0], amount=principal[0] * 99 // 100),
            minimum1=AssetAmount(asset=target.assets[1], amount=principal[1] * 99 // 100),
            deadline=block().timestamp + 300,
        ),
        before,
    )
    collect = planning.operation(
        config,
        state,
        CollectLiquidityFees[Target](target=target, deadline=block().timestamp + 300),
        before,
    )
    assert (close.kind, close.range, collect.kind, collect.range) == (
        "close",
        CENTRED,
        "collect",
        CENTRED,
    )


def topic(type_, value):
    return "0x" + encode((type_,), (value,)).hex()


def test_pool_events_must_name_the_operation_range():
    config = band_parameters()
    owned = owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    operation = planned(config, owned, CENTRED)
    manager = operation.target.contracts.position_manager
    event = {
        "topics": [
            receipts.POOL_MINT,
            topic("address", manager),
            topic("int24", CENTRED.lower),
            topic("int24", CENTRED.upper),
        ]
    }
    receipts.pool_range(event, operation)
    other = {**event, "topics": [*event["topics"][:3], topic("int24", CENTRED.upper + SPACING)]}
    with pytest.raises(DataError):
        receipts.pool_range(other, operation)


class PositionReader:
    def __init__(self, target, ticks, owner=None):
        self.target = target
        self.ticks = ticks
        self.owner = owner or target.fund.address

    def call(self, address, signature, block, *, inputs, args, outputs=None):
        if signature == "ownerOf(uint256)":
            return (self.owner,)
        pool_ = self.target.pool
        return (
            0,
            "0x" + "00" * 20,
            pool_.currency0.reference,
            pool_.currency1.reference,
            pool_.fee,
            *self.ticks,
            10**12,
            1,
            2,
            3,
            4,
        )


def test_reader_retains_a_band_range_and_pins_a_fixed_one():
    config = band_parameters()
    target = config.target("position")
    reader = PositionReader(target, (CENTRED.lower, CENTRED.upper))
    position = nft_position(reader, target, nft(target), block())
    assert (position.range, position.liquidity) == (CENTRED, 10**12)
    assert nft_position(reader, target, nft(target), block(), expected=CENTRED) == position
    with pytest.raises(DataError):
        nft_position(
            reader,
            target,
            nft(target),
            block(),
            expected=TickRange(lower=CENTRED.lower - SPACING, upper=CENTRED.upper - SPACING),
        )
    with pytest.raises(DataError):
        nft_position(
            PositionReader(target, (BAND["upper"], BAND["upper"] + 1000)),
            target,
            nft(target),
            block(),
        )
    fixed = parameters(0, lp=True)
    target = fixed.target("position")
    selected = target.range
    position = nft_position(
        PositionReader(target, (selected.lower, selected.upper)), target, nft(target), block()
    )
    assert position.range is None
    with pytest.raises(DataError):
        nft_position(
            PositionReader(target, (selected.lower, selected.upper + SPACING)),
            target,
            nft(target),
            block(),
        )


def test_ownership_requires_the_retained_range_only_for_bands():
    config = band_parameters()
    target = config.target("position")
    values = dict(nft=nft(target), liquidity=10**12, owed0=0, owed1=0, fee_growth=(0, 0))
    owned_for(config, 0, 0, position=Position(**values, range=CENTRED))
    with pytest.raises(ValueError, match="retained position range"):
        owned_for(config, 0, 0, position=Position(**values))
    fixed = parameters(0, lp=True)
    target = fixed.target("position")
    values["nft"] = nft(target)
    owned_for(fixed, 0, 0, position=Position(**values))
    with pytest.raises(ValueError, match="fixed range"):
        owned_for(fixed, 0, 0, position=Position(**values, range=SHIFTED))


def spot():
    price = pool().sqrt_price_x96
    return Fraction(price * price, 2**192)


def requested(amount):
    """A quote stand-in that records its requested input and has some output."""
    return SimpleNamespace(amount_in=amount, amount_out=amount)


def test_rebalance_quote_targets_the_centred_position_ratio():
    config = band_parameters()
    owned = initial_owned(config, "position")
    result = quotes_for(config, owned, requested, pool())
    rebalance = result["rebalance_quote"].amount_in
    assert rebalance.asset == owned.target.assets[0]
    assert 0 < rebalance.amount < owned.inventory0.amount
    unit0, unit1 = amounts(
        2**96, pool().sqrt_price_x96, sqrt_at_tick(CENTRED.lower), sqrt_at_tick(CENTRED.upper)
    )
    held0 = owned.inventory0.amount - rebalance.amount
    held1 = rebalance.amount * spot()
    assert abs(Fraction(held0) / held1 - Fraction(unit0, unit1)) < Fraction(1, 10**6)
    assert "rebalance_quote" not in quotes_for(
        parameters(0, lp=True),
        initial_owned(parameters(0, lp=True), "position"),
        requested,
        pool(),
    )
    target = owned.target
    position = Position(
        nft=nft(target), liquidity=1, owed0=0, owed1=0, fee_growth=(0, 0), range=CENTRED
    )
    holding = owned_for(config, 0, 10**16, position=position, entry_complete=True)
    assert quotes_for(config, holding, requested, pool()) == {}


def test_rebalance_quote_swaps_the_surplus_side():
    config = band_parameters()
    owned = owned_for(config, 0, 10**16)
    result = quotes_for(config, owned, requested, pool())
    assert result["rebalance_quote"].amount_in.asset == owned.target.assets[1]
    assert 0 < result["rebalance_quote"].amount_in.amount < owned.inventory1.amount
    assert "entry_quote" not in result


def test_actions_swap_follows_the_rebalance_quote():
    config = band_parameters()
    target = config.target("position")
    quote = Quote(
        source=target,
        block=block(),
        amount_in=AssetAmount(asset=target.assets[0], amount=10 * 10**6),
        amount_out=AssetAmount(asset=target.assets[1], amount=3 * 10**15),
        sqrt_price_after=pool().sqrt_price_x96,
        initialized_ticks_crossed=0,
        calculation="contract_quoter",
    )
    observed = SimpleNamespace(
        target=target,
        block=block(),
        effective_at=datetime.fromtimestamp(block().timestamp, UTC),
        entry_quote=None,
        exit_quote=None,
        rebalance_quote=quote,
    )
    context = SimpleNamespace(require_observation=lambda name: observed, config=config)
    intent = Actions(context).swap("position", rebalance=True)
    assert (intent.kind, intent.amount) == ("swap_exact_input", quote.amount_in)
    with pytest.raises(ValueError):
        Actions(context).swap("position")
    with pytest.raises(ValueError):
        Actions(context).swap("position", exit=True, rebalance=True)


def test_open_liquidity_action_selects_the_centred_range():
    config = band_parameters()
    owned = owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    observed = SimpleNamespace(
        target=owned.target,
        owned=owned,
        pool=pool(),
        block=block(),
        effective_at=datetime.fromtimestamp(block().timestamp, UTC),
    )
    context = SimpleNamespace(require_observation=lambda name: observed, config=config)
    intent = Actions(context).open_liquidity("position")
    assert intent.range == bounds(CENTRED)
    fixed = parameters(0, lp=True)
    owned = owned_for(fixed, 50 * 10**6, 2 * 10**16, entry_complete=True)
    observed.target, observed.owned = owned.target, owned
    context = SimpleNamespace(require_observation=lambda name: observed, config=fixed)
    intent = Actions(context).open_liquidity("position")
    assert intent.range is None
    assert "range" not in intent.model_dump(mode="json")


def outcome(kind, delta0=0, delta1=0, selected=None):
    return SimpleNamespace(
        operation=SimpleNamespace(kind=kind, range=selected), delta0=delta0, delta1=delta1
    )


def test_fork_assertions_accept_a_recentring_round_trip_inside_the_band():
    from tests.strategy_fork import v3_pool_band_assertions as checks

    target = band_parameters().target("position")
    effects = [
        outcome("approve"),
        outcome("swap", delta0=-10, delta1=3),
        outcome("mint", selected=CENTRED),
        outcome("close", selected=CENTRED),
        outcome("swap", delta0=-2, delta1=1),
        outcome("mint", selected=SHIFTED),
        outcome("close", selected=SHIFTED),
        outcome("swap", delta0=4, delta1=-1),
    ]
    checks.assert_band_operations(target, effects)
    outside = TickRange(lower=BAND["upper"] - 500, upper=BAND["upper"] + 500)
    with pytest.raises(ValueError):
        checks.assert_band_operations(
            target,
            [*effects[:2], outcome("mint", selected=outside), outcome("close", selected=outside)],
        )
    with pytest.raises(AssertionError):
        checks.assert_band_operations(target, effects[:3])
    with pytest.raises(AssertionError):
        checks.assert_band_operations(target, [*effects[:-1], outcome("swap", delta0=-1)])
    position = Position(
        nft=nft(target), liquidity=1, owed0=0, owed1=0, fee_growth=(0, 0), range=CENTRED
    )
    holding = owned_for(band_parameters(), 0, 1, position=position, entry_complete=True)
    assert checks.holding_band(holding)
    assert not checks.holding_band(owned_for(band_parameters(), 1, 0, entry_complete=True))


def test_v4_fork_assertions_accept_mint_and_burn_cycles_inside_the_band():
    from tests.strategy_fork import uniswap_assertions as checks

    target = band_parameters().target("position")
    cycle = [outcome("mint", selected=CENTRED), outcome("burn", selected=CENTRED)]
    checks.assert_band_liquidity(target, [outcome("swap"), *cycle, *cycle, outcome("swap")])
    checks.assert_fixed_liquidity(target, [outcome("mint"), outcome("collect"), outcome("burn")])
    with pytest.raises(AssertionError):
        checks.assert_band_liquidity(target, [*cycle, outcome("collect")])
    with pytest.raises(AssertionError):
        checks.assert_band_liquidity(target, cycle[:1])
    outside = TickRange(lower=BAND["upper"] - 500, upper=BAND["upper"] + 500)
    with pytest.raises(ValueError):
        checks.assert_band_liquidity(target, [outcome("mint", selected=outside)] + cycle[1:])
