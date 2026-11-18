"""Uniswap V4 banded liquidity recentres inside an envelope the gateway enforces."""

import json
from types import SimpleNamespace

import pytest
from eth_abi import decode, encode

from almanak.caip import AssetId
from almanak.connectors.uniswap_v4 import (
    abi,
    backtest,
    history,
    permissions,
    planning,
    receipts,
    reconciliation,
)
from almanak.connectors.uniswap_v4.api import Actions
from almanak.connectors.uniswap_v4.author_inputs import AllocationInput
from almanak.connectors.uniswap_v4.authoring import close_liquidity
from almanak.connectors.uniswap_v4.authority import admit_intent, declared
from almanak.connectors.uniswap_v4.math import amounts, sqrt_at_tick
from almanak.connectors.uniswap_v4.models import Allocation, Range, Target
from almanak.connectors.uniswap_v4.observations import initial_owned, quotes_for
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.connectors.uniswap_v4.reader import Quote, position, sqrt_bounds
from almanak.connectors.uniswap_v4.state import Allowance, LiquidityPosition, Mint, Ownership
from almanak.connectors.v3_pool_author_inputs import width_from_bps
from almanak.connectors.v3_pool_domain import TickBand
from almanak.dex_intents import OpenLiquidity, TickBounds
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_conditions import equal, signed_at_least, signed_at_most
from almanak.models import AssetAmount
from tests.scope_trees import assert_same_scope
from tests.uniswap_support import live_state

STATE = live_state("lp")
MINTED = next(o for o in STATE.outcomes if o.operation.kind == "mint")
POOL = MINTED.before.pool
SPACING = POOL.source.tick_spacing
ALIGNED = POOL.tick // SPACING * SPACING
BAND = {"lower": ALIGNED - 3000, "upper": ALIGNED + 3000, "width": 1000}
CENTRED = Range(lower=ALIGNED - 500, upper=ALIGNED + 500)
SHIFTED = Range(lower=CENTRED.lower + SPACING, upper=CENTRED.upper + SPACING)
NAME = STATE.config.allocations[0].name


def band_config(**band):
    raw = STATE.config.model_dump(mode="json")
    raw["allocations"][0].update(range=None, band=band or BAND)
    return type(STATE.config).model_validate_json(json.dumps(raw))


def owned_for(config, amount0, amount1, **extra):
    target = config.target(NAME)
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


def bounds(selected):
    return TickBounds(lower=selected.lower, upper=selected.upper)


def test_fixed_range_records_and_grants_are_unchanged():
    allocation = STATE.config.allocations[0]
    target = STATE.config.target(NAME)
    assert (allocation.liquidity_enabled, target.liquidity_enabled, target.band) == (
        True,
        True,
        None,
    )
    assert "band" not in allocation.model_dump(mode="json")
    assert "band" not in target.model_dump(mode="json")
    assert "range" not in MINTED.operation.model_dump(mode="json")
    assert "range" not in MINTED.after.owned.position.model_dump(mode="json")
    assert "rebalance_quote" not in MINTED.before.model_dump(mode="json")
    partition = STATE.permissions.partitions[0]
    # Scope-only grants (blueprint 27) keep every installed scope word by value; only the
    # author deposit bounds and the two pinned mint tick words differ.
    grants = permissions.derive(partition.profile, STATE.capabilities)
    assert_same_scope(grants, partition.grants)


def test_allocation_declares_a_range_or_a_band_aligned_to_the_pool():
    config = band_config()
    allocation = config.allocations[0]
    target = config.target(NAME)
    assert (allocation.range, allocation.liquidity_enabled) == (None, True)
    assert (target.range, target.band, target.liquidity_enabled) == (None, TickBand(**BAND), True)
    raw = allocation.model_dump(mode="json")
    with pytest.raises(ValueError, match="either a fixed range or a band"):
        Allocation.model_validate_json(json.dumps({**raw, "range": {"lower": -100, "upper": 100}}))
    with pytest.raises(ValueError, match="tick spacing"):
        Allocation.model_validate_json(json.dumps({**raw, "band": {**BAND, "width": 995}}))
    uncollected = Allocation.model_validate_json(json.dumps({**raw, "collect_seconds": None}))
    assert uncollected.collect_seconds is None


def test_author_input_resolves_a_band_and_refuses_mixed_shapes():
    pool = POOL.source
    market = SimpleNamespace(
        selection=SimpleNamespace(chain=pool.chain.chain_id),
        assets=(POOL.currency0, POOL.currency1),
        pool=pool,
        contracts=STATE.config.allocations[0].contracts,
    )
    raw = {
        "market": "pool",
        "account": "fund",
        "kind": "liquidity",
        "band": {"lower": BAND["lower"], "upper": BAND["upper"], "width_bps": 500},
        "capital": {"asset": str(pool.currency0), "quantity": "100"},
        "hold_seconds": "43200s",
        "collect_seconds": "21600s",
        "slippage_bps": 100,
        "deadline_seconds": "300s",
        "minimum_native_balance": "0.001",
    }
    allocation = author_input(raw).resolve("lp", market)
    assert allocation.band == TickBand(
        lower=BAND["lower"], upper=BAND["upper"], width=width_from_bps(500, SPACING)
    )
    assert allocation.range is None and allocation.liquidity_enabled
    assert allocation.entry_swap_bps == 10000
    for retired in ("maximum0", "maximum1"):
        with pytest.raises(ValueError, match="Extra inputs"):
            author_input({**raw, retired: {"asset": str(pool.currency1), "quantity": "0.1"}})
    with pytest.raises(ValueError, match="not both"):
        author_input({**raw, "range": {"lower": 0, "upper": 10}})
    with pytest.raises(ValueError, match="only a liquidity allocation"):
        author_input({**raw, "kind": "trading", "collect_seconds": None})
    with pytest.raises(ValueError, match="only a liquidity allocation"):
        author_input({**raw, "band": None})


def author_input(raw):
    return AllocationInput.model_validate_json(json.dumps(raw))


def mint_words(config):
    profile = STATE.permissions.partitions[0].profile
    grants = permissions.derive(profile, declared(config))
    target = config.target(NAME)
    grant = next(
        g
        for g in grants
        if g.target == target.contracts.position_manager and g.signature == abi.MODIFY
    )
    branch = next(
        b for b in grant.condition.children if any(c.operator == 19 for c in b.children[0].children)
    )
    return grants, grant, branch.children[0].children


def test_mint_grant_leaves_the_range_or_band_to_the_gateway():
    grants, grant, words = mint_words(band_config())
    lower = [i for i, c in enumerate(words) if c.operator == 19]
    upper = [i for i, c in enumerate(words) if c.operator == 20]
    assert len(lower) == len(upper) == 1 and lower[0] + 1 == upper[0]
    # The tick words admit the protocol's whole tick domain, never the author's band.
    assert words[lower[0]] == signed_at_least(-887272)
    assert words[upper[0]] == signed_at_most(887272)
    assert decode(("int256",), bytes.fromhex(words[lower[0]].value[2:])) == (-887273,)
    assert "gateway checks" in grant.explanation[0] and "band" in grant.explanation[0]
    # A fixed range derives the same grants: changing the range or band changes no
    # permission, and the fixed ticks are never pinned on-chain.
    fixed_grants, _, fixed_words = mint_words(STATE.config)
    assert fixed_grants == grants and fixed_words == words
    selected = STATE.config.allocations[0].range
    pinned = {equal("uint256", tick % 2**256) for tick in (selected.lower, selected.upper)} | {
        equal("uint256", tick % 2**256) for tick in (BAND["lower"], BAND["upper"])
    }
    assert pinned.isdisjoint(words)


def before_for(owned):
    target = owned.target
    allowances = []
    for asset in target.assets:
        allowances.append(Allowance(asset=asset, spender=target.contracts.permit2, amount=2**120))
        for spender in (target.contracts.router, target.contracts.position_manager):
            allowances.append(
                Allowance(
                    asset=asset,
                    spender=spender,
                    amount=2**120,
                    expiration=POOL.block.timestamp + 10**6,
                    nonce=0,
                )
            )
    return SimpleNamespace(
        target=target, owned=owned, pool=POOL, block=POOL.block, allowances=tuple(allowances)
    )


def open_intent(owned, selected):
    return OpenLiquidity[Target](
        target=owned.target,
        maximum0=owned.inventory0,
        maximum1=owned.inventory1,
        deadline=POOL.block.timestamp + 300,
        **({} if selected is None else {"range": bounds(selected)}),
    )


def test_planner_mints_the_selected_band_range_and_encodes_it():
    config = band_config()
    owned = owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    operation = planning.operation(config, None, open_intent(owned, CENTRED), before_for(owned))
    assert (operation.kind, operation.range) == ("mint", CENTRED)
    assert operation.model_dump(mode="json")["range"] == {
        "lower": CENTRED.lower,
        "upper": CENTRED.upper,
    }
    call = planning.call(config, operation)
    unlock, _ = decode(("bytes", "uint256"), bytes.fromhex(call.data[10:]))
    _, params = decode(("bytes", "bytes[]"), unlock)
    values = decode(
        (abi.POOL_KEY, "int24", "int24", "uint256", "uint128", "uint128", "address", "bytes"),
        params[0],
    )
    assert values[1:4] == (CENTRED.lower, CENTRED.upper, operation.liquidity)
    lower, upper = sqrt_bounds(operation.target, requested=operation.range)
    assert amounts(operation.liquidity, POOL.sqrt_price_x96, lower, upper, up=True) <= (
        owned.inventory0.amount,
        owned.inventory1.amount,
    )
    admit_intent(declared(config), open_intent(owned, CENTRED))


def test_planner_and_authority_refuse_ranges_outside_the_band_or_on_fixed_targets():
    config = band_config()
    owned = owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    outside = Range(lower=BAND["upper"] - 500, upper=BAND["upper"] + 500)
    for selected in (outside, SHIFTED.model_copy(update={"upper": SHIFTED.upper + SPACING}), None):
        with pytest.raises(ValueError, match="band"):
            planning.operation(config, None, open_intent(owned, selected), before_for(owned))
        with pytest.raises(ValueError, match="band"):
            admit_intent(declared(config), open_intent(owned, selected))
    fixed = STATE.config
    owned = owned_for(fixed, 50 * 10**6, 2 * 10**16, entry_complete=True)
    selected = fixed.allocations[0].range
    with pytest.raises(ValueError, match="fixed-range"):
        planning.operation(fixed, None, open_intent(owned, SHIFTED), before_for(owned))
    with pytest.raises(ValueError, match="fixed-range"):
        admit_intent(declared(fixed), open_intent(owned, SHIFTED))
    operation = planning.operation(fixed, None, open_intent(owned, None), before_for(owned))
    assert (operation.kind, operation.range) == ("mint", None)
    assert "range" not in operation.model_dump(mode="json")
    assert planning.operation(fixed, None, open_intent(owned, selected), before_for(owned)) == (
        operation
    )


def test_exit_operations_and_close_minima_use_the_retained_position_range():
    config = band_config()
    target = config.target(NAME)
    held = LiquidityPosition(nft=nft(target), liquidity=10**12, fee_growth=(0, 0), range=CENTRED)
    owned = owned_for(config, 0, 0, position=held, entry_complete=True)
    before = before_for(owned)
    observed = SimpleNamespace(
        target=target,
        owned=owned,
        pool=POOL,
        block=POOL.block,
        effective_at=MINTED.before.effective_at,
    )
    allocation = config.allocation(NAME)
    close = close_liquidity(allocation, observed)
    principal = amounts(
        held.liquidity,
        POOL.sqrt_price_x96,
        sqrt_at_tick(CENTRED.lower),
        sqrt_at_tick(CENTRED.upper),
    )
    assert (close.minimum0.amount, close.minimum1.amount) == tuple(
        value * (10000 - allocation.slippage_bps) // 10000 for value in principal
    )
    burn = planning.operation(config, None, close, before)
    collect_intent = Actions(context_for(config, observed)).collect(NAME)
    collect = planning.operation(config, None, collect_intent, before)
    assert (burn.kind, burn.range, collect.kind, collect.range) == (
        "burn",
        CENTRED,
        "collect",
        CENTRED,
    )
    budget = dict(maximum0=owned.inventory0, maximum1=owned.inventory1, deadline=1)
    with pytest.raises(ValueError, match="retained position range"):
        Mint(target=target, liquidity=1, **budget)


def context_for(config, observed):
    return SimpleNamespace(require_observation=lambda name: observed, config=config)


def topic(type_, value):
    return "0x" + encode((type_,), (value,)).hex()


def modify_receipt(operation, lower, upper, token):
    target = operation.target
    return {
        "logs": [
            {
                "address": target.pool.pool_manager,
                "topics": [
                    receipts.MODIFY,
                    pool_id(target.pool),
                    topic("address", target.contracts.position_manager),
                ],
                "data": "0x"
                + encode(
                    ("int24", "int24", "int256", "bytes32"),
                    (lower, upper, operation.liquidity, token.to_bytes(32, "big")),
                ).hex(),
            }
        ]
    }


def test_pool_events_must_name_the_operation_range():
    config = band_config()
    owned = owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    operation = planning.operation(config, None, open_intent(owned, CENTRED), before_for(owned))
    token = nft(operation.target)
    receipts.liquidity_movement(
        modify_receipt(operation, CENTRED.lower, CENTRED.upper, 77), operation, (0, 0), token
    )
    with pytest.raises(DataError, match="range or amount"):
        receipts.liquidity_movement(
            modify_receipt(operation, SHIFTED.lower, SHIFTED.upper, 77), operation, (0, 0), token
        )
    fixed = MINTED.operation
    selected = fixed.target.range
    token = nft(fixed.target, 5)
    receipts.liquidity_movement(
        modify_receipt(fixed, selected.lower, selected.upper, 5), fixed, (0, 0), token
    )
    with pytest.raises(DataError, match="range or amount"):
        receipts.liquidity_movement(
            modify_receipt(fixed, CENTRED.lower, CENTRED.upper, 5), fixed, (0, 0), token
        )


class PositionReader:
    def __init__(self, target, ticks, owner=None):
        self.target = target
        self.ticks = ticks
        self.owner = owner or target.fund.address

    def call(self, address, signature, block, *, inputs, args, outputs=None):
        if signature == "ownerOf(uint256)":
            return (self.owner,)
        if signature == "getPositionLiquidity(uint256)":
            return (10**12,)
        lower, upper = (tick & (2**24 - 1) for tick in self.ticks)
        return abi.pool_key(self.target.pool), (upper << 32) | (lower << 8)


def test_reader_retains_a_band_range_and_pins_a_fixed_one():
    config = band_config()
    target = config.target(NAME)
    reader = PositionReader(target, (CENTRED.lower, CENTRED.upper))
    assert position(reader, target, 77, POOL.block) == (10**12, CENTRED)
    assert position(reader, target, 77, POOL.block, expected=CENTRED) == (10**12, CENTRED)
    with pytest.raises(DataError, match="retained position range"):
        position(reader, target, 77, POOL.block, expected=SHIFTED)
    with pytest.raises(DataError, match="outside the band"):
        position(
            PositionReader(target, (BAND["upper"], BAND["upper"] + 1000)), target, 77, POOL.block
        )
    foreign = PositionReader(target, (CENTRED.lower, CENTRED.upper), owner="0x" + "12" * 20)
    with pytest.raises(DataError, match="owner, pool or range"):
        position(foreign, target, 77, POOL.block)
    fixed = STATE.config.target(NAME)
    selected = fixed.range
    reader = PositionReader(fixed, (selected.lower, selected.upper))
    assert position(reader, fixed, 77, POOL.block) == (10**12, None)
    reader = PositionReader(fixed, (selected.lower, selected.upper + SPACING))
    with pytest.raises(DataError, match="fixed target range"):
        position(reader, fixed, 77, POOL.block)


def test_ownership_requires_the_retained_range_only_for_bands():
    config = band_config()
    target = config.target(NAME)
    values = dict(nft=nft(target), liquidity=10**12, fee_growth=None)
    owned_for(config, 0, 0, position=LiquidityPosition(**values, range=CENTRED))
    with pytest.raises(ValueError, match="retained position range"):
        owned_for(config, 0, 0, position=LiquidityPosition(**values))
    outside = Range(lower=BAND["upper"] - 500, upper=BAND["upper"] + 500)
    with pytest.raises(ValueError, match="band"):
        owned_for(config, 0, 0, position=LiquidityPosition(**values, range=outside))
    fixed = STATE.config
    values["nft"] = nft(fixed.target(NAME))
    owned_for(fixed, 0, 0, position=LiquidityPosition(**values))
    with pytest.raises(ValueError, match="fixed range"):
        owned_for(fixed, 0, 0, position=LiquidityPosition(**values, range=SHIFTED))


def spot_quote(amount):
    price = POOL.sqrt_price_x96
    ratio = price * price
    if amount.asset == POOL.source.currency0:
        output = AssetAmount(asset=POOL.source.currency1, amount=amount.amount * ratio // 2**192)
    else:
        output = AssetAmount(asset=POOL.source.currency0, amount=amount.amount * 2**192 // ratio)
    return SimpleNamespace(amount=amount, output=output)


def test_rebalance_quote_only_for_band_targets_without_a_position():
    config = band_config()
    owned = initial_owned(config, NAME)
    result = quotes_for(config, owned, POOL, spot_quote)
    rebalance = result["rebalance_quote"].amount
    assert rebalance.asset == owned.target.assets[0]
    assert 0 < rebalance.amount < owned.inventory0.amount
    assert "entry_quote" in result
    unit0, unit1 = amounts(
        2**96, POOL.sqrt_price_x96, sqrt_at_tick(CENTRED.lower), sqrt_at_tick(CENTRED.upper)
    )
    remaining0 = owned.inventory0.amount - rebalance.amount
    received1 = spot_quote(rebalance).output.amount
    assert abs(remaining0 * unit1 - received1 * unit0) <= unit0 + unit1
    surplus1 = quotes_for(
        config, owned_for(config, 0, 10**16, entry_complete=True), POOL, spot_quote
    )
    assert surplus1["rebalance_quote"].amount.asset == owned.target.assets[1]
    fixed = STATE.config
    assert "rebalance_quote" not in quotes_for(fixed, initial_owned(fixed, NAME), POOL, spot_quote)
    target = config.target(NAME)
    held = LiquidityPosition(nft=nft(target), liquidity=1, fee_growth=None, range=CENTRED)
    holding = owned_for(config, 0, 0, position=held, entry_complete=True)
    assert quotes_for(config, holding, POOL, spot_quote) == {}


def test_actions_swap_follows_the_rebalance_quote_and_open_selects_the_centred_range():
    config = band_config()
    owned = owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    target = owned.target
    quote = Quote(
        block=POOL.block,
        amount=AssetAmount(asset=target.assets[0], amount=10 * 10**6),
        output=AssetAmount(asset=target.assets[1], amount=3 * 10**15),
        quoter_gas=1,
    )
    observed = SimpleNamespace(
        target=target,
        owned=owned,
        pool=POOL,
        block=POOL.block,
        effective_at=MINTED.before.effective_at,
        entry_quote=None,
        exit_quote=None,
        rebalance_quote=quote,
    )
    actions = Actions(context_for(config, observed))
    intent = actions.swap(NAME, rebalance=True)
    assert (intent.kind, intent.amount) == ("swap_exact_input", quote.amount)
    with pytest.raises(ValueError):
        actions.swap(NAME)
    with pytest.raises(ValueError):
        actions.swap(NAME, exit=True, rebalance=True)
    assert actions.open_liquidity(NAME).range == bounds(CENTRED)
    fixed = STATE.config
    owned = owned_for(fixed, 50 * 10**6, 2 * 10**16, entry_complete=True)
    observed.target, observed.owned = owned.target, owned
    intent = Actions(context_for(fixed, observed)).open_liquidity(NAME)
    assert intent.range is None and "range" not in intent.model_dump(mode="json")


def test_reconciliation_retains_the_mint_range_on_the_position():
    config = band_config()
    owned = owned_for(config, 50 * 10**6, 2 * 10**16, entry_complete=True)
    operation = planning.operation(config, None, open_intent(owned, CENTRED), before_for(owned))
    pending = SimpleNamespace(before=SimpleNamespace(owned=owned), operation=operation)
    after = reconciliation.owned_after(pending, (-1, -1), nft(owned.target), (1, 2), True)
    assert (after.position.range, after.position.nft, after.entry_complete) == (
        CENTRED,
        nft(owned.target),
        True,
    )
    assert after.position.model_dump(mode="json")["range"] == CENTRED.model_dump(mode="json")


def test_detailed_historical_backtests_refuse_band_allocations():
    config = band_config()
    author = SimpleNamespace(allocations=config.allocations, asset_metadata={})
    settings = SimpleNamespace(liquidity_model="bracketed_historical_fee_growth", funds=())
    with pytest.raises(ValueError, match="historical_path"):
        backtest.parameters(author, settings, SimpleNamespace(effective=None))
    with pytest.raises(ValueError, match="historical_path"):
        history.refuse_band(config.target(NAME))
    # Economic band evidence retains pool events without fixed boundary measurements.
    history.BoundaryEvidence(
        target=config.target(NAME),
        first=POOL.block,
        last=POOL.block,
        initial_gross=None,
        final_gross=None,
        events=(),
        contracts=(),
        inputs=(),
    )
    history.refuse_band(STATE.config.target(NAME))
