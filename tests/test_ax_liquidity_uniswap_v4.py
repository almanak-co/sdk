"""`almanak ax` liquidity commands for Uniswap V4: one position in one pool named by its key.

Component evidence without signing or chain access: the pool is resolved from its
retained resolution reads, and observations are built over it. The fork rehearsal of
the whole commands is tests/fork/test_ax_liquidity_uniswap_v4.py.
"""

import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from almanak import ax, ax_actions
from almanak.caip import AssetId, AssetType, ChainId
from almanak.chains import evm_network
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.direct import ALLOCATION, ActionRequest
from almanak.connectors.support import RunMode, uniswap_candidate_entry, uniswap_support
from almanak.connectors.uniswap_v4 import (
    abi,
    liquidity_direct,
    observations,
    position_request,
    reconciliation,
)
from almanak.connectors.uniswap_v4 import reader as reads
from almanak.connectors.uniswap_v4.domain import PoolObservation
from almanak.connectors.uniswap_v4.math import amounts as principal_of
from almanak.connectors.uniswap_v4.math import liquidity_for, sqrt_at_tick
from almanak.connectors.uniswap_v4.models import Allocation, Target
from almanak.connectors.uniswap_v4.position_request import PositionRequest
from almanak.connectors.uniswap_v4.state import Burn, Collect, Mint, Snapshot
from almanak.dex_intents import SwapExactInput
from almanak.gateway.ethereum import DataError, Reverted
from almanak.models import AssetAmount
from almanak.strategy import StrategyDefinition
from tests import test_ax_swap as family
from tests import test_ax_swap_uniswap_v4 as swaps

CONNECTOR = "uniswap_v4"
USDC, WETH, NATIVE = swaps.USDC, swaps.WETH, swaps.NATIVE
WALLET = swaps.WALLET
KINDS = ("swap_exact_input", "open_liquidity", "collect_liquidity_fees", "close_liquidity")
COMMANDS = ("open-liquidity", "collect-liquidity-fees", "close-liquidity")
# The range the tests open: around the observed tick 0, aligned to the pool's spacing 10.
LOWER, UPPER = -600, 600
AMOUNT0, AMOUNT1 = 5000, 7000
TOKEN = 4242
UNLIMITED_TOKEN, UNLIMITED_PERMIT, FAR = 2**256 - 1, 2**160 - 1, 2**48 - 1
GROWTH = (11, 13)


def stated(action, **changes):
    values = {"slippage_bps": 50, "deadline_seconds": 300}
    if action == "open-liquidity":
        values.update(tick_lower=LOWER, tick_upper=UPPER, amount0=AMOUNT0, amount1=AMOUNT1)
    if action == "collect-liquidity-fees":
        values = {}
    if action != "open-liquidity":
        values["token_id"] = TOKEN
    return ActionRequest(action, **{**values, **changes})


def nft(market, token=TOKEN):
    manager = market.contracts.position_manager
    return AssetId(f"{market.pool.chain.chain_id}/erc721:{manager}/{token}")


def packed(lower, upper):
    """A PositionInfo word holding the two ticks as the PositionManager packs them."""
    return ((upper & 0xFFFFFF) << 32) | ((lower & 0xFFFFFF) << 8)


class Manager:
    """A reader that answers the PositionManager's reads of one NFT."""

    def __init__(self, market, *, owner=WALLET.address, key=None, liquidity=900, ticks=None):
        self.owner, self.liquidity = owner, liquidity
        self.key = abi.pool_key(market.pool) if key is None else key
        self.ticks = ticks or (LOWER, UPPER)

    def call(self, target, signature, block, *, inputs=(), args=(), outputs=()):
        if signature == "ownerOf(uint256)":
            if self.owner is None:
                raise Reverted("revert", "NOT_MINTED")
            return (self.owner,)
        if signature == "getPoolAndPositionInfo(uint256)":
            return self.key, packed(*self.ticks)
        return (self.liquidity,)


def positions(market, action, reader=None, **changes):
    request = stated(action, **changes)
    reader = reader or Manager(market)
    return direct.binding(CONNECTOR).positions(reader, market.block, market, WALLET, request)


def config_for(market, action, reader=None, **changes):
    return swaps.configuration(positions(market, action, reader, **changes), market)


def allowances(target, tokens=(0, 0), permits=((0, 0), (0, 0))):
    """Every allowance an observation reads; `tokens` are the ERC-20 approvals to Permit2
    and `permits` the amount and expiration of each Permit2 allowance to the manager."""
    contracts, rows = target.contracts, []
    for asset, token, permit in zip(target.assets, tokens, permits, strict=True):
        rows.append({"asset": asset, "spender": contracts.permit2, "amount": token})
        for spender in (contracts.router, contracts.position_manager):
            amount, expiration = permit if spender == contracts.position_manager else (0, 0)
            row = {"asset": asset, "spender": spender, "amount": amount}
            rows.append({**row, "expiration": expiration, "nonce": 0})
    return tuple(rows)


def observed(config, owned, market, *, wallets=(9000, 9000), **stated_fields):
    """An on-chain observation of the deployment's one position at the resolved block."""
    target = config.target(ALLOCATION)
    pool = PoolObservation(
        source=market.pool,
        block=market.block,
        currency0=market.asset0,
        currency1=market.asset1,
        sqrt_price_x96=2**96,
        tick=0,
        liquidity=10**18,
        protocol_fee=0,
        lp_fee=market.pool.fee,
    )
    granted = allowances(
        target, stated_fields.pop("tokens", (0, 0)), stated_fields.pop("permits", ((0, 0), (0, 0)))
    )
    native = AssetAmount(asset=evm_network(market.pool.chain.chain_id).native_asset, amount=10**18)
    return Snapshot(
        target=target,
        pool=pool,
        owned=owned,
        wallet0=AssetAmount(asset=target.assets[0], amount=wallets[0]),
        wallet1=AssetAmount(asset=target.assets[1], amount=wallets[1]),
        allowances=granted,
        native_balance=native,
        payer=target.fund,
        **stated_fields,
    )


def opening():
    """An open deployment with the observation its request is admitted with."""
    market = swaps.resolved()
    config = config_for(market, "open-liquidity")
    state = swaps.created(config)
    return market, config, state, observed(config, state.owned[0], market)


def adopting(action="collect-liquidity-fees", *, liquidity=900, growth=GROWTH):
    """A deployment that has adopted the named NFT, with its admitting observation."""
    market = swaps.resolved()
    config = config_for(market, action)
    state = swaps.created(config)
    found = connector_from_wire({"connector": CONNECTOR})
    before = observed(
        config, state.owned[0], market, position_liquidity=liquidity, position_growth=growth
    )
    owned = found.lifecycle.adopt(state.owned, (before,))
    state = state.model_copy(update={"owned": owned, "initialized": True, "baseline": (before,)})
    seen = observed(
        config,
        owned[0],
        market,
        position_liquidity=liquidity,
        position_growth=growth,
        fee_growth=growth,
    )
    return market, config, state, seen


def validating(config, state, intent, seen):
    """The connector's own validation of one operation under one retained decision."""
    execution = connector_from_wire({"connector": CONNECTOR}).execution
    deciding = SimpleNamespace(
        config=config,
        owned=state.owned,
        capabilities=state.capabilities,
        decision=SimpleNamespace(intent=intent, observations=(seen,)),
        outcomes=(),
        mode="running",
        operator=state.operator,
    )

    def step(before, operation=None):
        planned = operation or execution.operation(
            config, deciding, deciding.decision.intent, before
        )
        execution.validate_operation(None, deciding, planned, before)
        return planned

    return deciding, step


def test_the_connector_serves_open_collect_and_close_and_no_partial_change():
    found = connector_from_wire({"connector": CONNECTOR})
    assert direct.binding(CONNECTOR).actions == ("swap", *COMMANDS)
    production = [cell for cell in found.chain_support if RunMode.PRODUCTION in cell.modes]
    assert len(production) == 8
    for cell in production:
        assert tuple(str(action) for action in cell.direct_actions) == KINDS
        for command in COMMANDS:
            direct.require_direct(CONNECTOR, cell.chain, ActionRequest(command))
    for unserved in ("increase-liquidity", "decrease-liquidity", "revoke-exchange"):
        assert CONNECTOR not in direct.connectors_for(unserved)
        with pytest.raises(ValueError, match=f"uniswap_v4 serves no direct {unserved}"):
            direct.require_direct(CONNECTOR, production[0].chain, ActionRequest(unserved))
    template = next(cell for cell in uniswap_support() if cell.direct_actions)
    start = template.historical[0].backtest_start_at
    assert uniswap_candidate_entry(template, "eip155:59144", start).direct_actions == ()


def test_an_open_states_a_range_and_two_amounts_as_the_whole_inventory():
    market = swaps.resolved()
    config = config_for(market, "open-liquidity")
    (allocation,) = config.allocations
    assert (allocation.range.lower, allocation.range.upper) == (LOWER, UPPER)
    assert (str(allocation.capital.asset), allocation.capital.amount) == (USDC, AMOUNT0)
    assert (allocation.adopt.counter, allocation.adopt.nft) == (AMOUNT1, None)
    # Nothing is swapped: the entry share is stated as zero and there is no band.
    assert (allocation.entry_swap_bps, allocation.band) == (0, None)
    assert (allocation.slippage_bps, allocation.deadline_seconds) == (50, 300)
    owned = swaps.created(config).owned[0]
    assert (owned.inventory0.amount, owned.inventory1.amount) == (AMOUNT0, AMOUNT1)
    assert (owned.position, owned.adopts, config.target(ALLOCATION).liquidity_enabled) == (
        None,
        None,
        True,
    )


def test_an_open_refuses_what_it_cannot_mint_into_or_from():
    market = swaps.resolved()
    with pytest.raises(ValueError, match="multiples of the pool's tick spacing, 10"):
        positions(market, "open-liquidity", tick_lower=-605)
    with pytest.raises(ValueError, match="--tick-lower must be below --tick-upper"):
        positions(market, "open-liquidity", tick_lower=UPPER, tick_upper=LOWER)
    with pytest.raises(ValueError, match="requires --amount0 or --amount1 above zero"):
        positions(market, "open-liquidity", amount0=0, amount1=0)
    with pytest.raises(ValueError, match="open-liquidity requires --slippage-bps"):
        positions(market, "open-liquidity", slippage_bps=None)
    with pytest.raises(ValueError, match="close-liquidity requires --slippage-bps"):
        positions(market, "close-liquidity", slippage_bps=None)
    with pytest.raises(ValueError, match="collect-liquidity-fees requires --token-id"):
        positions(market, "collect-liquidity-fees", token_id=None)


@pytest.mark.parametrize("action", ["collect-liquidity-fees", "close-liquidity"])
def test_a_named_nft_is_read_and_becomes_the_allocations_range_and_adoption(action):
    market = swaps.resolved()
    config = config_for(market, action, Manager(market, ticks=(-120, 240)))
    (allocation,) = config.allocations
    assert (allocation.range.lower, allocation.range.upper) == (-120, 240)
    assert (allocation.capital.amount, allocation.adopt.counter) == (0, 0)
    assert allocation.adopt.nft == nft(market) and allocation.entry_swap_bps == 0
    owned = swaps.created(config).owned[0]
    assert (owned.adopts, owned.position) == (nft(market), None)
    assert (owned.inventory0.amount, owned.inventory1.amount) == (0, 0)


@pytest.mark.parametrize("action", ["collect-liquidity-fees", "close-liquidity"])
def test_a_wrong_token_id_is_refused_by_name(action):
    market = swaps.resolved()
    other = "0x" + "77" * 20
    with pytest.raises(ValueError, match=f"NFT {TOKEN} belongs to {other}, not to the wallet"):
        positions(market, action, Manager(market, owner=other))
    foreign = (*abi.pool_key(market.pool)[:2], 3000, 60, abi.pool_key(market.pool)[4])
    with pytest.raises(ValueError, match=f"NFT {TOKEN} is a position of another pool key"):
        positions(market, action, Manager(market, key=foreign))
    with pytest.raises(ValueError, match=f"NFT {TOKEN} holds no liquidity"):
        positions(market, action, Manager(market, liquidity=0))
    with pytest.raises(ValueError, match=f"holds no NFT {TOKEN}"):
        positions(market, action, Manager(market, owner=None))


def test_an_adopting_allocation_trades_nothing_and_only_an_operator_states_one():
    market = swaps.resolved()
    wire = config_for(market, "open-liquidity").allocations[0].model_dump(mode="json")

    def allocation(**changes):
        return Allocation.model_validate_json(json.dumps({**wire, **changes}))

    assert allocation().adopt.counter == AMOUNT1
    # The default entry share offers the whole capital as a swap, so it must be zero.
    for refused in ({"entry_swap_bps": 10000}, {"range": None}, {"band": {"width": 100}}):
        with pytest.raises(ValidationError, match="fixed range, no entry swap and no band"):
            allocation(**refused)
    elsewhere = f"eip155:1/erc721:0x{'55' * 20}/7"
    with pytest.raises(ValidationError, match="one of the allocation's PositionManager"):
        allocation(adopt={"counter": 0, "nft": elsewhere})
    # Only an adopting allocation may state no capital.
    empty = {"asset": USDC, "amount": 0}
    assert allocation(capital=empty).capital.amount == 0
    with pytest.raises(ValidationError, match="capital must fit positive uint128"):
        allocation(capital=empty, adopt=None, entry_swap_bps=0)
    # A strategy allocation keeps its wire form, and a strategy deployment refuses one.
    plain = swaps.strategy_checkpoint()
    assert "adopt" not in plain.config.allocations[0].model_dump()
    assert "adopts" not in plain.owned[0].model_dump()
    strategy = StrategyDefinition(filename="strategy.py", source="def decide(context):\n    pass\n")
    with pytest.raises(ValueError, match="only an operator deployment adopts"):
        swaps.created(config_for(market, "open-liquidity"), strategy)


def test_an_adopting_deployment_quotes_no_swap_and_its_validation_admits_none():
    market, config, state, seen = opening()

    # Two-sided inventory and no position would otherwise offer an exit quote. No
    # reader is given: a quote would have to read.
    assert observations.decision_quotes(None, config, state.owned[0], seen.pool) == {}
    assert (seen.entry_quote, seen.exit_quote, seen.rebalance_quote) == (None, None, None)
    swap = SwapExactInput[Target](
        target=config.target(ALLOCATION),
        amount=AssetAmount(asset=AssetType(USDC), amount=AMOUNT0),
        minimum_output=AssetAmount(asset=AssetType(WETH), amount=1),
        deadline=market.block.timestamp + 300,
    )
    _, step = validating(config, state, swap, seen)
    ready = observed(config, state.owned[0], market, tokens=(AMOUNT0, 0))
    # The capability admits a swap kind, but with no quote the gateway has none to admit.
    with pytest.raises(ValueError, match="swap requires its exact-size decision quote"):
        step(ready)


def test_a_swap_deployment_admits_no_mint():
    market, config, state, seen = swaps.admitted()
    opened = config_for(market, "open-liquidity")
    mint = liquidity_direct.intent(
        opened,
        (observed(opened, swaps.created(opened).owned[0], market),),
        stated("open-liquidity"),
    )
    with pytest.raises(ValueError, match="intent has no retained Uniswap capability"):
        connector_from_wire({"connector": CONNECTOR}).codec.capability.admit(
            state.capabilities, mint
        )


def test_adoption_takes_the_position_the_observation_read_from_the_chain():
    market, config, state, seen = adopting()
    held = state.owned[0]
    assert (held.position.nft, held.position.liquidity) == (nft(market), 900)
    # The fee checkpoint is the PoolManager's own, so uncollected fees are measured.
    assert (held.position.fee_growth, held.entry_complete, held.adopts) == (GROWTH, True, None)
    found = connector_from_wire({"connector": CONNECTOR})
    fresh = swaps.created(config).owned
    unmeasured = observed(config, fresh[0], market, position_liquidity=900, fee_error="unavailable")
    assert found.lifecycle.adopt(fresh, (unmeasured,))[0].position.fee_growth is None
    absent = observed(config, fresh[0], market)
    with pytest.raises(DataError, match="does not hold the NFT to adopt"):
        found.lifecycle.adopt(fresh, (absent,))


def test_adoption_attributes_stated_inventory_only_when_the_wallet_covers_it():
    market, config, state, seen = opening()
    found = connector_from_wire({"connector": CONNECTOR})
    assert found.lifecycle.adopt(state.owned, (seen,)) == state.owned
    short = observed(config, state.owned[0], market, wallets=(AMOUNT0, AMOUNT1 - 1))
    with pytest.raises(DataError, match=f"the wallet holds {AMOUNT1 - 1} base units") as refusal:
        found.lifecycle.adopt(state.owned, (short,))
    assert refusal.value.kind == "inventory" and WETH in str(refusal.value)


def test_the_first_observation_reads_the_nft_to_adopt_and_verifies_it(monkeypatch):
    market = swaps.resolved()
    config = config_for(market, "collect-liquidity-fees")
    owned = swaps.created(config).owned[0]
    monkeypatch.setattr(observations.reads, "fee_growth", lambda *a: (1, 2))
    monkeypatch.setattr(observations.reads, "position_growth", lambda *a: (900, GROWTH))
    evidence = observations.position_evidence(Manager(market), owned, market.block)
    assert evidence == {"position_liquidity": 900, "fee_growth": (1, 2), "position_growth": GROWTH}
    # The connector's own read refuses an NFT of another owner, pool key or range.
    target = config.target(ALLOCATION)
    foreign = (*abi.pool_key(market.pool)[:2], 3000, 60, abi.pool_key(market.pool)[4])
    for wrong in ({"owner": "0x" + "77" * 20}, {"key": foreign}, {"ticks": (-120, 240)}):
        with pytest.raises(DataError) as refusal:
            reads.position(Manager(market, **wrong), target, TOKEN, market.block)
        assert refusal.value.kind == "ownership"
    assert (
        observations.position_evidence(Manager(market), opening()[2].owned[0], market.block) == {}
    )


def test_the_open_intent_offers_exactly_the_stated_amounts_into_the_stated_range():
    market, config, state, seen = opening()
    intent = liquidity_direct.intent(config, (seen,), stated("open-liquidity"))
    assert (intent.kind, intent.range) == ("open_liquidity", None)
    assert (intent.maximum0.amount, intent.maximum1.amount) == (AMOUNT0, AMOUNT1)
    assert intent.deadline == market.block.timestamp + 300
    with pytest.raises(ValueError, match="does not own the position the command names"):
        liquidity_direct.intent(config, (seen,), stated("close-liquidity"))
    # A range beside the price takes one token only; stating only the other adds nothing.
    beside = config_for(market, "open-liquidity", tick_lower=600, tick_upper=1200, amount0=0)
    owned = swaps.created(beside).owned[0]
    with pytest.raises(ValueError, match="at the pool's tick 0 the range 600 to 1200 takes only"):
        liquidity_direct.intent(
            beside, (observed(beside, owned, market),), stated("open-liquidity")
        )


def test_an_open_is_two_approvals_per_token_then_the_mint():
    market, config, state, seen = opening()
    intent = liquidity_direct.intent(config, (seen,), stated("open-liquidity"))
    _, step = validating(config, state, intent, seen)
    contracts, owned, deadline = (
        config.target(ALLOCATION).contracts,
        state.owned[0],
        intent.deadline,
    )
    first = step(seen)
    assert (first.kind, first.spender, first.amount) == (
        "approve_erc20",
        contracts.permit2,
        AMOUNT0,
    )
    second = step(observed(config, owned, market, tokens=(AMOUNT0, 0)))
    assert (second.kind, second.spender) == ("approve_permit2", contracts.position_manager)
    assert (second.amount, second.expiration) == (AMOUNT0, deadline)
    half = ((AMOUNT0, deadline), (0, 0))
    third = step(observed(config, owned, market, tokens=(AMOUNT0, 0), permits=half))
    assert (third.kind, str(third.asset), third.amount) == ("approve_erc20", WETH, AMOUNT1)
    both = ((AMOUNT0, deadline), (AMOUNT1, deadline))
    mint = step(observed(config, owned, market, tokens=(AMOUNT0, AMOUNT1), permits=both))
    assert (mint.kind, mint.maximum0.amount, mint.maximum1.amount) == ("mint", AMOUNT0, AMOUNT1)
    expected = liquidity_for(AMOUNT0, AMOUNT1, 2**96, sqrt_at_tick(LOWER), sqrt_at_tick(UPPER))
    assert mint.liquidity == expected > 0


def test_standing_unlimited_approvals_serve_the_mint_and_a_lapsing_one_is_renewed():
    market, config, state, seen = opening()
    intent = liquidity_direct.intent(config, (seen,), stated("open-liquidity"))
    _, step = validating(config, state, intent, seen)
    owned, unlimited = state.owned[0], (UNLIMITED_TOKEN, UNLIMITED_TOKEN)
    whole = ((UNLIMITED_PERMIT, FAR), (UNLIMITED_PERMIT, FAR))
    assert step(observed(config, owned, market, tokens=unlimited, permits=whole)).kind == "mint"
    lapsing = ((UNLIMITED_PERMIT, intent.deadline - 1), (UNLIMITED_PERMIT, FAR))
    renewed = step(observed(config, owned, market, tokens=unlimited, permits=lapsing))
    assert (renewed.kind, renewed.amount, renewed.expiration) == (
        "approve_permit2",
        AMOUNT0,
        intent.deadline,
    )


def test_a_mint_beyond_the_stated_amounts_or_range_is_refused():
    market, config, state, seen = opening()
    intent = liquidity_direct.intent(config, (seen,), stated("open-liquidity"))
    deciding, step = validating(config, state, intent, seen)
    whole = ((UNLIMITED_PERMIT, FAR), (UNLIMITED_PERMIT, FAR))
    ready = observed(
        config, state.owned[0], market, tokens=(UNLIMITED_TOKEN, UNLIMITED_TOKEN), permits=whole
    )
    mint = step(ready)
    # One base unit above a stated amount is more than the attributed inventory.
    larger = intent.model_copy(
        update={"maximum1": AssetAmount(asset=AssetType(WETH), amount=AMOUNT1 + 1)}
    )
    deciding.decision = SimpleNamespace(intent=larger, observations=(seen,))
    with pytest.raises(DataError, match="mint exceeds the attributed inventory"):
        step(ready)
    # Another range than the stated one is not the allocation's.
    deciding.decision = SimpleNamespace(intent=intent, observations=(seen,))
    bound = config.target(ALLOCATION)
    shifted = bound.model_copy(update={"range": bound.range.model_copy(update={"upper": 1200})})
    with pytest.raises(ValueError, match="operation and observation require the bound target"):
        step(ready, mint.model_copy(update={"target": shifted}))
    with pytest.raises(ValueError, match="operation differs from its retained economic intent"):
        step(ready, mint.model_copy(update={"liquidity": mint.liquidity + 1}))
    # Another pool key is another target as well.
    other = bound.model_copy(update={"pool": bound.pool.model_copy(update={"fee": 3000})})
    with pytest.raises(ValueError, match="operation and observation require the bound target"):
        step(ready, mint.model_copy(update={"target": other}))


@pytest.mark.parametrize("action", ["collect-liquidity-fees", "close-liquidity"])
def test_a_position_command_acts_on_the_adopted_nft_and_no_other(action):
    market, config, state, seen = adopting(action)
    intent = liquidity_direct.intent(config, (seen,), stated(action))
    _, step = validating(config, state, intent, seen)
    planned = step(seen)
    assert (planned.nft, planned.deadline) == (nft(market), market.block.timestamp + 300)
    assert isinstance(planned, Collect if action == "collect-liquidity-fees" else Burn)
    with pytest.raises(ValueError, match="operation differs from its retained economic intent"):
        step(seen, planned.model_copy(update={"nft": nft(market, TOKEN + 1)}))
    with pytest.raises(ValueError, match="does not own the position the command names"):
        liquidity_direct.intent(config, (seen,), stated(action, token_id=TOKEN + 1))


def test_a_close_states_its_minimum_outputs_from_the_principal_less_the_slippage():
    market, config, state, seen = adopting("close-liquidity")
    intent = liquidity_direct.intent(config, (seen,), stated("close-liquidity"))
    principal = principal_of(900, 2**96, sqrt_at_tick(LOWER), sqrt_at_tick(UPPER))
    assert (intent.minimum0.amount, intent.minimum1.amount) == tuple(
        value * 9950 // 10000 for value in principal
    )
    deciding, step = validating(config, state, intent, seen)
    burn = step(seen)
    assert (burn.kind, burn.liquidity) == ("burn", 900)
    looser = intent.model_copy(update={"minimum0": AssetAmount(asset=AssetType(USDC), amount=0)})
    deciding.decision = SimpleNamespace(intent=looser, observations=(seen,))
    with pytest.raises(ValueError, match="close minima widen the configured slippage limit"):
        step(seen)


def mined_mint(before, after, *, used=(AMOUNT0 - 7, AMOUNT1 - 9)):
    """The connector's own allowance reconciliation over a mined mint that used `used`.

    `before` and `after` are, per token, the ERC-20 approval to Permit2 and the Permit2
    allowance to the position manager.
    """
    market, config, state, _ = opening()
    target = config.target(ALLOCATION)

    def seen(pairs):
        tokens = tuple(pair[0] for pair in pairs)
        permits = tuple((pair[1], FAR) for pair in pairs)
        return observed(config, state.owned[0], market, tokens=tokens, permits=permits)

    operation = Mint(
        target=target,
        liquidity=1,
        maximum0=AssetAmount(asset=AssetType(USDC), amount=AMOUNT0),
        maximum1=AssetAmount(asset=AssetType(WETH), amount=AMOUNT1),
        deadline=market.block.timestamp + 300,
    )
    pending = SimpleNamespace(before=seen(before), operation=operation)
    deltas = (-used[0], -used[1])
    return reconciliation.allowances_reconcile(pending, seen(after), deltas, True)


def test_a_mint_lowers_finite_allowances_by_what_it_took_and_leaves_unlimited_ones():
    exact = ((AMOUNT0, AMOUNT0), (AMOUNT1, AMOUNT1))
    assert mined_mint(exact, ((7, 7), (9, 9)))
    # What the mint did not take stays approved; an allowance that did not fall is refused.
    assert not mined_mint(exact, exact)
    assert not mined_mint(exact, ((7, 7), (9, AMOUNT1)))
    whole = ((UNLIMITED_TOKEN, UNLIMITED_PERMIT), (UNLIMITED_TOKEN, UNLIMITED_PERMIT))
    assert mined_mint(whole, whole)
    lowered = (
        (UNLIMITED_TOKEN - AMOUNT0 + 7, UNLIMITED_PERMIT),
        (UNLIMITED_TOKEN, UNLIMITED_PERMIT),
    )
    assert mined_mint(whole, lowered)
    assert not mined_mint(whole, ((UNLIMITED_TOKEN, UNLIMITED_PERMIT - 1), whole[1]))


def test_the_result_reports_the_position_what_moved_and_the_allowances():
    market, config, state, seen = adopting()
    report = direct.binding(CONNECTOR).position
    held = report(state)
    assert held["position"] == {
        "nft": str(nft(market)),
        "token_id": str(TOKEN),
        "range": {"lower": LOWER, "upper": UPPER},
        "liquidity": "900",
    }
    zero = [{"asset": USDC, "amount": "0"}, {"asset": WETH, "amount": "0"}]
    assert (held["moved"], held["inventory"], held["allowances"]) == (zero, zero, [])
    # Mined steps add up; a reverted one moved nothing.
    left = observed(config, state.owned[0], market, tokens=(7, 0), permits=((7, 1234), (0, 0)))
    steps = (
        SimpleNamespace(success=True, delta0=-40, delta1=-60, after=seen),
        SimpleNamespace(success=False, delta0=-1, delta1=-1, after=seen),
        SimpleNamespace(success=True, delta0=5, delta1=0, after=left),
    )
    after = report(SimpleNamespace(owned=state.owned, baseline=(), outcomes=steps))
    assert [item["amount"] for item in after["moved"]] == ["-35", "-60"]
    contracts = config.target(ALLOCATION).contracts
    assert after["allowances"] == [
        {"asset": USDC, "spender": contracts.permit2, "amount": "7"},
        {"asset": USDC, "spender": contracts.position_manager, "amount": "7", "expiration": 1234},
    ]
    closed = SimpleNamespace(owned=opening()[2].owned, baseline=(), outcomes=())
    assert report(closed)["position"] is None


def test_a_native_pool_position_is_stated_in_the_tokens_the_wallet_holds():
    market = swaps.native_market()
    request = stated("open-liquidity", tick_lower=-600, tick_upper=600)
    document = direct.binding(CONNECTOR).positions(None, market.block, market, WALLET, request)
    config = swaps.configuration(document, market)
    target = config.target(ALLOCATION)
    # The key keeps the native coin; amount0 is of the wrapped token held in its place.
    assert (str(target.pool.currency0), target.native_index) == (NATIVE, 0)
    assert [str(asset) for asset in target.assets] == [WETH, USDC]
    assert str(config.allocations[0].capital.asset) == WETH
    owned = swaps.created(config).owned[0]
    assert (owned.inventory0.amount, owned.inventory1.amount) == (AMOUNT0, AMOUNT1)


class Holding(family.Observed):
    """A gateway double that reads the chain as the ownership it is asked about names it:
    it answers each read with an observation of that ownership."""

    def __init__(self, build, store):
        self.build, self.store = build, store

    def observe(self):
        return (self.build(self.store.load().owned[0]),)

    def verify_snapshot(self, observation):
        return self.build(observation.owned)

    def snapshot(self, allocation, block, observation):
        return self.build(observation.owned)


def desk_over(tmp_path, config, build):
    desk = family.SwapDesk(tmp_path, config, build)
    desk.gateway = desk.service.backend = Holding(build, desk.store)
    return desk


def test_the_gateway_admits_an_open_and_a_collection_of_an_adopted_position(tmp_path):
    market, config, _, _ = opening()
    desk = desk_over(tmp_path / "open", config, lambda owned: observed(config, owned, market))
    record = desk.runtime().submit(
        lambda state, seen: liquidity_direct.intent(state.config, seen, stated("open-liquidity"))
    )
    assert (record.intent.kind, desk.store.load().decision) == ("open_liquidity", record)

    adopted_config = config_for(market, "collect-liquidity-fees")

    def holding(owned):
        return observed(
            adopted_config, owned, market, position_liquidity=900, position_growth=GROWTH
        )

    desk = desk_over(tmp_path / "collect", adopted_config, holding)
    record = desk.runtime().submit(
        lambda state, seen: liquidity_direct.intent(
            state.config, seen, stated("collect-liquidity-fees")
        )
    )
    held = desk.store.load()
    assert (record.intent.kind, held.owned[0].position.nft) == (
        "collect_liquidity_fees",
        nft(market),
    )
    assert held.owned[0].position.fee_growth == GROWTH and held.owned[0].adopts is None


KEY = {
    "--connector": CONNECTOR,
    "--chain": "eip155:1",
    "--currency0": USDC,
    "--currency1": WETH,
    "--fee": "500",
    "--tick-spacing": "10",
}
ACTING = {
    "--wallet": "wallet",
    "--env-file": ".env",
    "--max-gas-price-wei": "7",
    "--max-transaction-fee": "9",
}
OPEN = {
    "--tick-lower": str(LOWER),
    "--tick-upper": str(UPPER),
    "--amount0": str(AMOUNT0),
    "--amount1": str(AMOUNT1),
    "--slippage-bps": "50",
}


def parse(command, flags, *extra, without=()):
    words = [part for name, value in flags.items() if name not in without for part in (name, value)]
    return parse_validated(build_parser(), ["ax", command, *words, *extra])


def test_the_commands_name_the_pool_by_its_key_and_the_position_by_its_token_id(capsys):
    args = parse("open-liquidity", {**KEY, **OPEN, **ACTING})
    assert ax_actions.market_request(args) == swaps.selection()
    assert ax_actions.action_request(args) == stated("open-liquidity")
    for command in ("collect-liquidity-fees", "close-liquidity"):
        bound = {"--slippage-bps": "50"} if command == "close-liquidity" else {}
        args = parse(command, {**KEY, **bound, **ACTING, "--token-id": str(TOKEN)})
        assert ax_actions.action_request(args) == stated(command)
    with pytest.raises(SystemExit):
        parse("open-liquidity", {**KEY, **OPEN, **ACTING}, "--pool", "0x" + "ab" * 20)
    assert "uniswap_v4 names its pool with --currency0" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        parse("open-liquidity", {**KEY, **OPEN, **ACTING}, without=("--fee",))
    assert "missing --fee" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        parse("open-liquidity", {**KEY, **OPEN, **ACTING, "--connector": "uniswap_v3"})
    assert (
        "uniswap_v3 names its pool with --pool; it takes no --currency0" in capsys.readouterr().err
    )
    for unserved in ("increase-liquidity", "decrease-liquidity", "revoke-exchange"):
        with pytest.raises(SystemExit):
            parse(unserved, {**KEY, **ACTING, "--token-id": str(TOKEN)})
        assert "invalid choice: 'uniswap_v4'" in capsys.readouterr().err


READ = {**KEY, "--token-id": str(TOKEN), "--env-file": ".env"}


def test_the_position_read_names_a_uniswap_v4_position_by_key_and_token_id(capsys, monkeypatch):
    for name in ("ALMANAK_AX_GATEWAY_URL", "ALMANAK_AX_GATEWAY_AUTH_FILE"):
        monkeypatch.delenv(name, raising=False)
    args = parse("liquidity-position", READ)
    assert (args.product, args.block) == ("ax-liquidity-position", "finalized")
    request = ax.parse_request(args)
    assert isinstance(request, PositionRequest) and request.token_id == TOKEN
    assert request.selection() == swaps.selection()
    hooked = ax.parse_request(parse("liquidity-position", READ, "--hooks", swaps.UNREVIEWED_HOOK))
    assert hooked.hooks == swaps.UNREVIEWED_HOOK
    with pytest.raises(SystemExit):
        parse("liquidity-position", READ, "--pool", "0x" + "ab" * 20)
    assert "uniswap_v4 takes no --pool for this request" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        parse("liquidity-position", READ, without=("--tick-spacing",))
    assert "must be given together" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        parse("liquidity-position", {**READ, "--connector": "uniswap_v3"})
    refused = "uniswap_v3 takes no --currency0, --currency1, --fee, --tick-spacing for this"
    assert refused in capsys.readouterr().err


def test_the_position_read_reports_principal_and_the_fees_a_collection_would_pay(monkeypatch):
    market = swaps.resolved()
    liquidity, current = 10**12, (2**128 * 3, 2**128 * 5)
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v4.resolution.resolve_market", lambda *a: market
    )
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v4.pool.read_pool",
        lambda reader, pool, block: (
            observed(config_for(market, "open-liquidity"), opening()[2].owned[0], market).pool
        ),
    )
    monkeypatch.setattr(reads, "fee_growth", lambda *a: current)
    monkeypatch.setattr(reads, "position_growth", lambda *a: (liquidity, (2**128, 2**128)))
    request = PositionRequest.model_validate_json(
        json.dumps({"chain": "eip155:1", **swaps.KEY, "token_id": TOKEN})
    )
    reader = Manager(market, liquidity=liquidity, owner="0x" + "77" * 20)
    reader.chain = market.block.chain
    report = position_request.read_liquidity_position(reader, request, market.block)
    assert (report["product"], report["owner"]) == ("liquidity_position", "0x" + "77" * 20)
    assert report["position"] == {
        "nft": str(nft(market)),
        "token_id": str(TOKEN),
        "range": {"lower": LOWER, "upper": UPPER},
        "liquidity": str(liquidity),
    }
    principal = principal_of(liquidity, 2**96, sqrt_at_tick(LOWER), sqrt_at_tick(UPPER))
    assert [int(item["amount"]) for item in report["principal"]] == list(principal)
    # Growth of 2 and 4 per unit of liquidity since the position's checkpoint.
    assert [int(item["amount"]) for item in report["owed"]] == [2 * liquidity, 4 * liquidity]
    assert (report["pool"]["in_range"], report["support"]) == (True, "verified")
    with pytest.raises(ValueError, match="request chain differs from the reader chain"):
        reader.chain = SimpleNamespace(chain_id=ChainId("eip155:10"))
        position_request.read_liquidity_position(reader, request, market.block)
