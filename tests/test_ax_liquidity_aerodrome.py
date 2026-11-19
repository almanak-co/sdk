"""The direct liquidity commands of `almanak ax` in one Aerodrome stable or volatile pool.

Component evidence without signing or chain access: each pool is resolved from the
retained resolution reads, observations are built over that pool, and every refusal is
the connector's own planning, validation or reconciliation. The fork rehearsal of the
whole commands is tests/fork/test_ax_liquidity_aerodrome.py.
"""

import json
from types import SimpleNamespace

import pytest

from almanak import ax, ax_actions
from almanak.chains import evm_network
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct
from almanak.connectors.aerodrome import (
    liquidity_direct,
    observations,
    position_request,
    reconciliation,
)
from almanak.connectors.aerodrome import operations as ops
from almanak.connectors.aerodrome.domain import PoolObservation
from almanak.connectors.aerodrome.observations import Snapshot
from almanak.connectors.aerodrome.ownership import validate_owned
from almanak.connectors.aerodrome.position_request import (
    PositionRequest,
    read_liquidity_position,
)
from almanak.connectors.aerodrome.positions import Position, Quote
from almanak.connectors.aerodrome.receipts import Effects
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.direct import ALLOCATION, ActionRequest
from almanak.dex_intents import SwapExactInput
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import RetainedResolutionReader
from almanak.models import ZERO_ADDRESS, AssetAmount
from tests.test_ax_swap_aerodrome import (
    BASE,
    CONNECTOR,
    LIMITS,
    POOLS,
    STRATEGY,
    WALLET,
    allocation_document,
    configuration,
    created,
    resolved,
    retained,
    selection,
)

COMMANDS = liquidity_direct.ACTIONS
PRICED = ("open-liquidity", "increase-liquidity", "decrease-liquidity", "close-liquidity")
ADDITIONS = ("open-liquidity", "increase-liquidity")
# The pool every observation here states: equal reserves, so a deposit takes the two
# currencies one for one, and a share is worth RESERVE / SUPPLY of each.
RESERVE, SUPPLY = 10**12, 10**18
# Fee indices the pool stores per share, at 18 decimals, and where the wallet's own
# checkpoint stands: one index has moved since, the other has not.
POOL_INDICES = (7 * 10**9, 9 * 10**9)
HELD_INDICES = (5 * 10**9, 9 * 10**9)
SHARES = 10**9
# What the pool stored for the wallet at its last change.
RECORDED = (11, 13)

pytestmark = pytest.mark.parametrize("variant", tuple(POOLS))
LIFECYCLE = connector_from_wire({"connector": CONNECTOR}).lifecycle
EXECUTION = connector_from_wire({"connector": CONNECTOR}).execution


def stated(action, **more):
    priced = action in PRICED
    return ActionRequest(
        action,
        slippage_bps=50 if priced else None,
        deadline_seconds=300 if priced else None,
        **more,
    )


def depositing(action="open-liquidity", amount0=4000, amount1=3000):
    return stated(action, amount0=amount0, amount1=amount1)


def deployed(market, request):
    positions = direct.binding(CONNECTOR).positions(None, None, market, WALLET, request)
    return configuration(positions, market)


def holding(target, shares=SHARES, recorded=RECORDED, indices=HELD_INDICES):
    """The wallet's position as the pool stores it."""
    return Position(
        shares=AssetAmount(asset=target.pool.shares_asset, amount=shares),
        fees0=AssetAmount(asset=target.assets[0], amount=recorded[0]),
        fees1=AssetAmount(asset=target.assets[1], amount=recorded[1]),
        index0=indices[0],
        index1=indices[1],
    )


def seen(
    config, owned, market, *, wallets=(9000, 9000), allowed=(0, 0, 0), position=None, deposited=0
):
    """An observation of the deployment's one position at the resolved block.

    `allowed` is what the wallet lets the router spend of currency0, currency1 and the
    pool's shares. `position` is what the pool stores for the wallet.
    """
    target = config.target(ALLOCATION)
    pool = PoolObservation(
        source=market.pool,
        block=market.block,
        currency0=market.asset0,
        currency1=market.asset1,
        shares=market.share,
        reserve0=RESERVE,
        reserve1=RESERVE,
        balance0=RESERVE,
        balance1=RESERVE,
        deposited_shares=deposited,
        total_supply=SUPPLY,
        fee_bps=5,
        index0=POOL_INDICES[0],
        index1=POOL_INDICES[1],
        pool_fees="0x" + "a1" * 20,
        paused=False,
    )
    spent = (*target.assets, target.pool.shares_asset)
    return Snapshot(
        target=target,
        pool=pool,
        owned=owned,
        wallet0=AssetAmount(asset=target.assets[0], amount=wallets[0]),
        wallet1=AssetAmount(asset=target.assets[1], amount=wallets[1]),
        allowances=tuple(
            {"asset": asset, "spender": target.contracts.router, "amount": amount}
            for asset, amount in zip(spent, allowed, strict=True)
        ),
        native_balance=AssetAmount(asset=evm_network(BASE).native_asset, amount=10**18),
        payer=target.fund,
        position=position,
    )


def started(variant, request, *, holds=False, **observed):
    """A deployment after its start: the configuration, its state and what it adopted.

    With `holds` the wallet already holds shares and stored fee claims of the pool.
    """
    market = resolved(variant)
    config = deployed(market, request)
    state = created(config)
    position = holding(config.target(ALLOCATION)) if holds else None
    first = seen(config, state.owned[0], market, position=position, **observed)
    owned = LIFECYCLE.adopt(state.owned, (first,))
    adopted = SimpleNamespace(owned=owned, capabilities=state.capabilities)
    return market, config, adopted


def admitted(variant, request, *, holds=False, **observed):
    """The intent the command builds, and the gateway's own check of each of its steps."""
    market, config, state = started(variant, request, holds=holds, **observed)
    position = holding(config.target(ALLOCATION)) if holds else None
    before = seen(config, state.owned[0], market, position=position, **observed)
    intent = direct.binding(CONNECTOR).intent(config, (before,), request)
    return market, config, state, before, intent


def checked(config, state, intent, basis):
    """Plan one step of an intent and validate it as the gateway validates it."""
    deciding = SimpleNamespace(
        config=config,
        owned=state.owned,
        capabilities=state.capabilities,
        decision=SimpleNamespace(intent=intent, observations=(basis,)),
        outcomes=(),
        mode="running",
    )

    def step(before):
        operation = EXECUTION.operation(config, deciding, intent, before)
        EXECUTION.validate_operation(None, deciding, operation, before)
        return operation

    return step


def test_the_connector_serves_the_liquidity_commands_on_its_production_cell(variant):
    found = direct.binding(CONNECTOR)
    assert found.actions == ("swap", *COMMANDS)
    # A deposit states both amounts and a decrease its shares; nothing else names a
    # position, through the one declaration every connector's flags go through.
    assert [found.liquidity_flags(command) for command in COMMANDS] == [
        (("amount0", "amount1"), ()),
        (("amount0", "amount1"), ()),
        (("liquidity",), ()),
        ((), ()),
        ((), ()),
        ((), ()),
    ]
    (cell,) = connector_from_wire({"connector": CONNECTOR}).chain_support
    assert cell.chain == BASE and resolved(variant).pool.variant in cell.variants
    kinds = {str(action) for action in cell.direct_actions}
    for command in COMMANDS:
        assert CONNECTOR in direct.connectors_for(command)
        assert set(direct.COMMAND_INTENTS[command]) <= kinds
        direct.require_direct(CONNECTOR, BASE, ActionRequest(command))
    # The cell declares the swap as well, for `ax swap`; an exit stays a strategy action.
    assert kinds == {kind for name in found.actions for kind in direct.COMMAND_INTENTS[name]}
    assert "exit_exchange" not in kinds


def test_a_deposit_states_both_amounts_as_its_whole_inventory_and_swaps_nothing(variant):
    market = resolved(variant)
    config = deployed(market, depositing(amount0=77, amount1=55))
    (allocation,) = config.allocations
    assert (allocation.name, allocation.account) == (ALLOCATION, WALLET.account)
    assert (allocation.pool, allocation.contracts) == (market.pool, market.contracts)
    assert allocation.capital == AssetAmount(asset=market.pool.currency0, amount=77)
    # Liquidity authority, an adoption and no entry: nothing is offered to a swap.
    assert (allocation.liquidity_enabled, allocation.adopt.counter) == (True, 55)
    assert (allocation.entry_swap_bps, allocation.hold_seconds) == (0, None)
    assert (allocation.slippage_bps, allocation.deadline_seconds) == (50, 300)
    owned = created(config).owned[0]
    held = {str(v.asset): v.amount for v in (owned.inventory0, owned.inventory1)}
    assert held == {str(market.pool.currency0): 77, str(market.pool.currency1): 55}
    assert (owned.position, owned.target.liquidity_enabled) == (None, True)
    assert direct.binding_for(config) is direct.binding(CONNECTOR)


@pytest.mark.parametrize(
    "command", ["decrease-liquidity", "collect-liquidity-fees", "close-liquidity"]
)
def test_a_command_on_held_shares_brings_no_inventory(variant, command):
    more = {"liquidity": 5} if command == "decrease-liquidity" else {}
    (allocation,) = deployed(resolved(variant), stated(command, **more)).allocations
    assert (allocation.capital.amount, allocation.adopt.counter) == (0, 0)
    assert (allocation.liquidity_enabled, allocation.entry_swap_bps) == (True, 0)
    priced = command in PRICED
    # A fee claim has no price bound; the gateway still plans and signs it only so long.
    assert allocation.slippage_bps == (50 if priced else 0)
    assert allocation.deadline_seconds == (300 if priced else liquidity_direct.ADMISSION_SECONDS)


def test_a_command_refuses_what_cannot_name_a_balance_of_shares_or_is_left_out(variant):
    market = resolved(variant)
    build = direct.binding(CONNECTOR).positions

    def refused(match, request):
        with pytest.raises(ValueError, match=match):
            build(None, None, market, WALLET, request)

    # No tick range and no NFT can name the wallet's shares.
    ranged = stated("open-liquidity", amount0=5, amount1=5, tick_lower=-60, tick_upper=60)
    refused("open-liquidity takes no --tick-lower, --tick-upper", ranged)
    refused("takes no --tick-upper", stated("open-liquidity", amount0=5, tick_upper=60))
    for command in ("increase-liquidity", "decrease-liquidity", "close-liquidity"):
        refused(f"{command} takes no --token-id", stated(command, token_id=7, liquidity=5))
    refused("the pool's own share token", stated("collect-liquidity-fees", token_id=7))
    # Nor is a deposit stated coin by coin, or a burn given a payout choice.
    per_coin = ((str(market.pool.currency0), 5),)
    coined = stated("open-liquidity", amount0=5, amount1=5, amounts=per_coin)
    refused("open-liquidity takes no --amount", coined)
    refused("close-liquidity takes no --receive", stated("close-liquidity", receive="balanced"))
    # A deposit takes both currencies; one amount alone is refused.
    for amounts in ({"amount0": 5}, {"amount1": 5}, {"amount0": 5, "amount1": 0}, {}):
        for command in ADDITIONS:
            refused(
                f"{command} requires --amount0 and --amount1 above zero", stated(command, **amounts)
            )
    refused(
        "open-liquidity requires --slippage-bps, --deadline-seconds",
        ActionRequest("open-liquidity", amount0=5, amount1=5),
    )
    refused(
        "decrease-liquidity requires --liquidity",
        stated("decrease-liquidity"),
    )
    refused(
        "close-liquidity requires --slippage-bps",
        ActionRequest("close-liquidity", deadline_seconds=300),
    )


def test_an_adopting_allocation_is_an_operators_has_liquidity_authority_and_no_entry(variant):
    market = resolved(variant)
    adopting = {"liquidity_enabled": True, "entry_swap_bps": 0, "adopt": {"counter": 3}}

    def refused(match, **changes):
        with pytest.raises(ValueError, match=match):
            configuration(allocation_document(market, **{**adopting, **changes}), market)

    refused("has liquidity authority and states no entry swap", liquidity_enabled=False)
    refused("has liquidity authority and states no entry swap", entry_swap_bps=1)
    # Only an adopting allocation may bring no capital.
    config = configuration(
        allocation_document(
            market, **adopting, capital={"asset": str(market.pool.currency0), "amount": 0}
        ),
        market,
    )
    assert config.allocations[0].capital.amount == 0
    with pytest.raises(ValueError, match="capital requires a positive supported int128"):
        configuration(
            allocation_document(market, capital={"asset": str(market.pool.currency0), "amount": 0}),
            market,
        )
    # A strategy deployment refuses an adoption, and its allocation states none.
    with pytest.raises(ValueError, match="only an operator deployment adopts"):
        created(config, STRATEGY)
    plain = configuration(allocation_document(market, liquidity_enabled=True), market)
    assert "adopt" not in json.loads(plain.model_dump_json())["allocations"][0]
    assert created(plain, STRATEGY).operator is False


def test_one_fund_has_one_allocation_so_one_command_has_one_mode(variant):
    market = resolved(variant)
    trading = allocation_document(market)["allocations"][0]
    liquid = {
        **trading,
        "name": "shares",
        "liquidity_enabled": True,
        "entry_swap_bps": 0,
        "adopt": {"counter": 0},
    }
    # A trading allocation never owns the wallet's shares and a liquidity allocation
    # owns all of them. One wallet cannot be given both in one deployment.
    with pytest.raises(ValueError, match="requires one allocation per fund"):
        configuration({"allocations": [trading, liquid]}, market)


def test_a_wallet_without_shares_starts_with_its_stated_inventory_and_no_position(variant):
    market, config, state = started(variant, depositing())
    (owned,) = state.owned
    assert (owned.position, owned.entry_complete) == (None, False)
    assert (owned.inventory0.amount, owned.inventory1.amount) == (4000, 3000)
    initial = created(config).owned
    for short in ((3999, 9000), (9000, 2999)):
        thin = seen(config, initial[0], market, wallets=short)
        with pytest.raises(DataError, match="the wallet holds") as refusal:
            LIFECYCLE.adopt(initial, (thin,))
        assert refusal.value.kind == "inventory"


def test_the_whole_observed_position_is_adopted_exactly_as_the_pool_stores_it(variant):
    market, config, state = started(variant, stated("close-liquidity"), holds=True)
    target = config.target(ALLOCATION)
    (owned,) = state.owned
    # Every share, with the stored claims and the wallet's own index checkpoints.
    assert owned.position == holding(target) and owned.entry_complete
    assert (owned.inventory0.amount, owned.inventory1.amount) == (0, 0)
    # The gateway derives the same ownership from the baseline it verified.
    initial = created(config).owned
    baseline = seen(config, initial[0], market, position=holding(target))
    assert LIFECYCLE.adopt(initial, (baseline,)) == state.owned
    # Later observations must find exactly the adopted position: a share more or less,
    # or another checkpoint, is not this deployment's.
    validate_owned(owned, seen(config, owned, market, position=holding(target)))
    for changed in (holding(target, shares=SHARES + 1), holding(target, recorded=(12, 13)), None):
        with pytest.raises(DataError, match="observed pool shares or fee claims differ"):
            validate_owned(owned, seen(config, owned, market, position=changed))


def test_adoption_takes_fee_claims_that_outlived_the_shares(variant):
    market = resolved(variant)
    config = deployed(market, stated("collect-liquidity-fees"))
    initial = created(config).owned
    claims = holding(config.target(ALLOCATION), shares=0)
    (owned,) = LIFECYCLE.adopt(initial, (seen(config, initial[0], market, position=claims),))
    assert owned.position == claims and not claims.empty


def test_an_adopting_deployment_observes_no_quote_and_the_gateway_admits_no_swap(variant):
    market, config, state = started(variant, depositing())
    (owned,) = state.owned

    def unreachable(amount):
        raise AssertionError("an adopting allocation asked for a quote")

    # Inventory on both sides and no position: a trading allocation would be quoted.
    assert observations.quotes_for(config, owned, unreachable) == {}
    assert observations.snapshot_quotes(config, owned, unreachable, True, False) == {}
    before = seen(config, owned, market)
    assert (before.entry_quote, before.exit_quote, before.exit_residual) == (None, None, None)
    # The cell declares the swap kind for `ax swap`, so only the missing quote stops it.
    target = config.target(ALLOCATION)
    connector_from_wire({"connector": CONNECTOR}).chain_support[0].require_direct(
        "swap_exact_input"
    )
    swap = SwapExactInput[type(target)](
        target=target,
        amount=AssetAmount(asset=target.assets[0], amount=4000),
        minimum_output=AssetAmount(asset=target.assets[1], amount=1),
        deadline=market.block.timestamp + 300,
    )
    step = checked(config, state, swap, before)
    with pytest.raises(ValueError, match="swap requires its exact-size retained router quote"):
        step(seen(config, owned, market, allowed=(4000, 0, 0)))
    # A quote written into the decision's observation is not one a verifying read of
    # an adopting allocation reproduces, which the gateway compares it with.
    forged = Quote(
        source=target,
        block=market.block,
        amount_in=swap.amount,
        amount_out=AssetAmount(asset=target.assets[1], amount=3990),
        calculation="contract_router",
    )
    written = before.model_copy(update={"entry_quote": forged})
    assert written != before and observations.quotes_for(config, owned, unreachable) == {}


def test_an_open_offers_the_stated_amounts_and_the_gateway_admits_only_that_deposit(variant):
    market, config, state, before, intent = admitted(variant, depositing())
    target = config.target(ALLOCATION)
    router = target.contracts.router
    assert (intent.kind, intent.target) == ("open_liquidity", target)
    assert (intent.maximum0.amount, intent.maximum1.amount) == (4000, 3000)
    assert intent.deadline == market.block.timestamp + 300
    assert LIFECYCLE.increases_exposure(intent)
    (owned,) = state.owned
    step = checked(config, state, intent, before)

    first = step(before)
    assert (first.kind, first.asset, first.spender, first.amount) == (
        "approve",
        target.assets[0],
        router,
        4000,
    )
    second = step(seen(config, owned, market, allowed=(4000, 0, 0)))
    assert (second.kind, second.asset, second.amount) == ("approve", target.assets[1], 3000)
    # A standing allowance of another size is reset before the exact one is granted.
    assert step(seen(config, owned, market, allowed=(4000, 7, 0))).amount == 0
    mint = step(seen(config, owned, market, allowed=(4000, 3000, 0)))
    assert (mint.kind, mint.maximum0, mint.maximum1) == ("mint", intent.maximum0, intent.maximum1)
    # The pool takes the two currencies one for one here, so 3000 of each: the minimums
    # are that less 50 basis points, and so are the shares.
    assert (mint.minimum0.amount, mint.minimum1.amount) == (2985, 2985)
    assert mint.minimum_shares.amount == 3000 * SUPPLY // RESERVE * 9950 // 10000
    assert mint.deadline == intent.deadline


def test_a_deposit_larger_than_stated_or_into_another_pool_is_refused(variant):
    market, config, state, before, intent = admitted(variant, depositing())
    (owned,) = state.owned
    approved = seen(config, owned, market, allowed=(4001, 3000, 0))
    larger = intent.model_copy(
        update={"maximum0": AssetAmount(asset=intent.maximum0.asset, amount=4001)}
    )
    with pytest.raises(DataError, match="exceeds attributed input inventory") as refusal:
        checked(config, state, larger, before)(approved)
    assert refusal.value.kind == "inventory"
    other = next(name for name in POOLS if name != variant)
    foreign = deployed(resolved(other), depositing()).target(ALLOCATION)
    elsewhere = intent.model_copy(update={"target": foreign})
    with pytest.raises(ValueError, match="must retain its configured observation target"):
        checked(config, state, elsewhere, before)(before)
    # Shares another transfer left in the pool's own balance would be swept by a burn,
    # so no deposit is planned while they are there.
    stray = seen(config, owned, market, allowed=(4000, 3000, 0), deposited=1)
    with pytest.raises(DataError, match="pool already holds LP shares from another transfer"):
        checked(config, state, intent, before)(stray)


def test_an_open_is_refused_for_a_wallet_that_holds_shares_and_names_the_increase(variant):
    market, config, state = started(variant, depositing(), holds=True)
    (owned,) = state.owned
    before = seen(config, owned, market, position=owned.position)
    with pytest.raises(ValueError, match="already holds shares or fee claims.*increase-liquidity"):
        direct.binding(CONNECTOR).intent(config, (before,), depositing())
    # The connector's own builder and planning refuse it as well.
    actions = connector_from_wire({"connector": CONNECTOR}).codec.actions
    allocation = config.allocation(ALLOCATION)
    with pytest.raises(ValueError, match="liquidity opening cannot adopt existing shares"):
        actions.authoring.open_liquidity(allocation, before)
    without = seen(config, owned.model_copy(update={"position": None}), market)
    opening = actions.authoring.open_liquidity(allocation, without)
    with pytest.raises(ValueError, match="must preserve existing shares and fee ownership"):
        checked(config, state, opening, before)(
            seen(config, owned, market, allowed=(4000, 3000, 0), position=owned.position)
        )


def test_an_increase_adds_the_stated_amounts_to_the_adopted_shares(variant):
    request = depositing("increase-liquidity")
    market, config, state, before, intent = admitted(variant, request, holds=True)
    (owned,) = state.owned
    assert (intent.kind, intent.maximum0.amount, intent.maximum1.amount) == (
        "increase_liquidity",
        4000,
        3000,
    )
    step = checked(config, state, intent, before)
    held = {"position": owned.position}
    kinds = [
        step(seen(config, owned, market, allowed=allowed, **held)).kind
        for allowed in ((0, 0, 0), (4000, 0, 0), (4000, 3000, 0))
    ]
    assert kinds == ["approve", "approve", "increase"]
    # A wallet without shares is told to make its first deposit.
    _, empty, bare = started(variant, request)
    with pytest.raises(ValueError, match="holds no shares of this pool; open-liquidity"):
        direct.binding(CONNECTOR).intent(empty, (seen(empty, bare.owned[0], market),), request)


def test_a_decrease_burns_the_stated_shares_within_its_minimums(variant):
    request = stated("decrease-liquidity", liquidity=400_000_000)
    market, config, state, before, intent = admitted(variant, request, holds=True)
    target = config.target(ALLOCATION)
    (owned,) = state.owned
    # 400 of each currency at the observed balances, less 50 basis points.
    assert (intent.kind, intent.liquidity) == ("decrease_liquidity", 400_000_000)
    assert (intent.minimum0.amount, intent.minimum1.amount) == (398, 398)
    assert not LIFECYCLE.increases_exposure(intent)
    step = checked(config, state, intent, before)
    approval = step(before)
    assert (approval.kind, approval.asset, approval.amount) == (
        "approve",
        target.pool.shares_asset,
        400_000_000,
    )
    burn = step(seen(config, owned, market, allowed=(0, 0, 400_000_000), position=owned.position))
    assert (burn.kind, burn.shares.amount) == ("decrease", 400_000_000)
    assert (burn.minimum0, burn.minimum1, burn.deadline) == (
        intent.minimum0,
        intent.minimum1,
        intent.deadline,
    )


def test_a_burn_of_more_than_is_held_or_below_its_minimums_is_refused(variant):
    request = stated("decrease-liquidity", liquidity=400_000_000)
    market, config, state, before, intent = admitted(variant, request, holds=True)
    (owned,) = state.owned
    build = direct.binding(CONNECTOR).intent
    for too_many in (SHARES, SHARES + 1):
        with pytest.raises(ValueError, match=f"must be below the {SHARES} shares.*close-liquidity"):
            build(config, (before,), stated("decrease-liquidity", liquidity=too_many))
    # The same through the connector's own planning, for an intent built elsewhere.
    for too_many in (SHARES, SHARES + 1):
        whole = intent.model_copy(update={"liquidity": too_many})
        with pytest.raises(ValueError, match="partial decrease must retain positive owned shares"):
            checked(config, state, whole, before)(before)
    # An exit that would accept less than the stated slippage allows is refused.
    approved = seen(config, owned, market, allowed=(0, 0, 400_000_000), position=owned.position)
    lower = intent.model_copy(
        update={"minimum0": AssetAmount(asset=intent.minimum0.asset, amount=397)}
    )
    with pytest.raises(ValueError, match="removal minima widen the configured slippage limit"):
        checked(config, state, lower, before)(approved)
    stray = seen(config, owned, market, position=owned.position, deposited=1)
    with pytest.raises(DataError, match="pool already holds LP shares from another transfer"):
        checked(config, state, intent, before)(stray)
    # A wallet without shares has nothing to burn.
    _, empty, bare = started(variant, request)
    with pytest.raises(ValueError, match="holds no shares of this pool, so decrease-liquidity"):
        build(empty, (seen(empty, bare.owned[0], market),), request)


def test_a_close_burns_every_share_and_then_claims_what_is_left(variant):
    request = stated("close-liquidity")
    market, config, state, before, intent = admitted(variant, request, holds=True)
    target = config.target(ALLOCATION)
    (owned,) = state.owned
    # All 10**9 shares are 1000 of each currency, less 50 basis points.
    assert (intent.kind, intent.minimum0.amount, intent.minimum1.amount) == (
        "close_liquidity",
        995,
        995,
    )
    step = checked(config, state, intent, before)
    approval = step(before)
    assert (approval.asset, approval.amount) == (target.pool.shares_asset, SHARES)
    burn = step(seen(config, owned, market, allowed=(0, 0, SHARES), position=owned.position))
    assert (burn.kind, burn.shares.amount) == ("close", SHARES)
    # Once the shares are gone the same request claims the fees the burn did not pay.
    burned = owned.model_copy(update={"position": holding(target, shares=0)})
    emptied = SimpleNamespace(owned=(burned,), capabilities=state.capabilities)
    claim = checked(config, emptied, intent, before)(
        seen(config, burned, market, position=burned.position)
    )
    assert claim.kind == "collect"
    # A wallet that holds only fee claims is sent to the claim.
    with pytest.raises(ValueError, match="so close-liquidity has nothing to burn; collect"):
        direct.binding(CONNECTOR).intent(
            config, (seen(config, burned, market, position=burned.position),), request
        )


def test_a_claim_is_built_only_when_the_pool_would_pay_something(variant):
    request = stated("collect-liquidity-fees")
    market, config, state, before, intent = admitted(variant, request, holds=True)
    (owned,) = state.owned
    assert (intent.kind, intent.target) == ("collect_liquidity_fees", config.target(ALLOCATION))
    assert intent.deadline == market.block.timestamp + liquidity_direct.ADMISSION_SECONDS
    claim = checked(config, state, intent, before)(before)
    assert isinstance(claim, ops.Collect)
    # Nothing recorded and no index moved since the wallet's checkpoint: nothing to claim.
    build = direct.binding(CONNECTOR).intent
    target = config.target(ALLOCATION)
    for nothing in (holding(target, recorded=(0, 0), indices=POOL_INDICES), None):
        idle = owned.model_copy(update={"position": nothing})
        with pytest.raises(ValueError, match="no fees to claim from this pool"):
            build(config, (seen(config, idle, market, position=nothing),), request)
    # An index that moved is enough, with nothing recorded yet.
    accruing = owned.model_copy(update={"position": holding(target, recorded=(0, 0))})
    assert (
        build(config, (seen(config, accruing, market, position=accruing.position),), request).kind
        == "collect_liquidity_fees"
    )


def test_steps_on_adopted_shares_reconcile_against_the_adopted_checkpoints(variant):
    market, config, state = started(variant, stated("close-liquidity"), holds=True)
    target = config.target(ALLOCATION)
    (owned,) = state.owned
    before = seen(config, owned, market, position=owned.position)
    # What the pool pays at its indices: the recorded claim plus the shares' part of
    # the index that moved, 10**9 shares times 2 * 10**9 over 10**18.
    earned = (RECORDED[0] + 2, RECORDED[1])
    claimed = holding(target, recorded=(0, 0), indices=POOL_INDICES)
    paid = Effects(delta0=earned[0], delta1=earned[1], fees0=earned[0], fees1=earned[1])
    collect = ops.Collect(target=target)
    kept = reconciliation.retained_position(before, collect, paid, claimed, before.pool, True)
    assert kept == claimed
    # A claim that paid anything else is not the adopted position's.
    short = paid.model_copy(update={"fees0": earned[0] - 1})
    with pytest.raises(DataError, match="stored fee claims differ from accrued and paid"):
        reconciliation.retained_position(before, collect, short, claimed, before.pool, True)
    # A burn of every share pays principal only and leaves the fees recorded.
    close = ops.Close(
        target=target,
        shares=owned.position.shares,
        minimum0=AssetAmount(asset=target.assets[0], amount=995),
        minimum1=AssetAmount(asset=target.assets[1], amount=995),
        deadline=market.block.timestamp + 300,
    )
    burned = holding(target, shares=0, recorded=earned, indices=POOL_INDICES)
    released = Effects(delta0=1000, delta1=1000, shares_delta=-SHARES)
    after = reconciliation.retained_position(before, close, released, burned, before.pool, True)
    assert after == burned
    settled = reconciliation.owned_after(before, close, released, after, True)
    assert (settled.inventory0.amount, settled.inventory1.amount) == (1000, 1000)
    # An allocation with liquidity authority reports no holdings as unowned.
    assert reconciliation.unowned_holdings(target, claimed) == ()


def test_a_position_that_moved_beside_a_mined_step_stays_owned_whole_and_is_reported(variant):
    market, config, state = started(variant, stated("close-liquidity"), holds=True)
    target = config.target(ALLOCATION)
    (owned,) = state.owned
    before = seen(config, owned, market, position=owned.position)
    approval = ops.Approval(
        target=target,
        asset=target.pool.shares_asset,
        spender=target.contracts.router,
        amount=SHARES,
    )
    nothing = Effects(delta0=0, delta1=0)
    settle = reconciliation.adopted_position
    # A step that explains the whole position reports nothing.
    assert settle(config, before, approval, nothing, owned.position, before.pool, True) == (
        owned.position,
        (),
    )
    # A share sent to the wallet in the step's own block: the deployment owns the fund's
    # whole balance, so the stored position stays owned, and the outcome says so.
    more = holding(target, shares=SHARES + 1, recorded=(13, 13), indices=POOL_INDICES)
    position, issues = settle(config, before, approval, nothing, more, before.pool, True)
    (issue,) = issues
    assert position == more and issue.startswith(f"{target.pool.shares_asset}: ")
    assert "unrelated operation changed the fund LP checkpoint" in issue and "stays owned" in issue
    # A reverted step keeps its inventory and still holds what the pool stores.
    kept = reconciliation.with_position(owned, position, issues)
    assert (kept.position, kept.inventory0, kept.inventory1) == (
        more,
        owned.inventory0,
        owned.inventory1,
    )
    assert reconciliation.with_position(owned, position, ()) is owned
    # A burn whose shares do not add up is reported the same way.
    close = ops.Close(
        target=target,
        shares=owned.position.shares,
        minimum0=AssetAmount(asset=target.assets[0], amount=995),
        minimum1=AssetAmount(asset=target.assets[1], amount=995),
        deadline=market.block.timestamp + 300,
    )
    released = Effects(delta0=1000, delta1=1000, shares_delta=-SHARES)
    one_left = holding(target, shares=1, recorded=(13, 13), indices=POOL_INDICES)
    position, (issue,) = settle(config, before, close, released, one_left, before.pool, True)
    assert position == one_left and "observed LP shares differ" in issue
    # A strategy's allocation owns only what its own steps explain: it still refuses.
    plain = configuration(allocation_document(market, liquidity_enabled=True), market)
    assert plain.target(ALLOCATION) == target
    with pytest.raises(DataError, match="unrelated operation changed the fund LP checkpoint"):
        settle(plain, before, approval, nothing, more, before.pool, True)


def test_a_mined_deposit_with_a_share_sent_beside_it_is_reconciled_and_reported(
    variant, monkeypatch
):
    market, config, state, before, intent = admitted(variant, depositing())
    target = config.target(ALLOCATION)
    (owned,) = state.owned
    approved = seen(config, owned, market, allowed=(4000, 3000, 0))
    mint = checked(config, state, intent, before)(approved)
    taken = Effects(delta0=-3000, delta1=-3000, shares_delta=3 * 10**9)
    # One share more than the deposit minted.
    stored = holding(target, shares=3 * 10**9 + 1, recorded=(0, 0), indices=POOL_INDICES)
    asked = {}

    def snapshot(backend, name, block, settled, **options):
        asked.update(options)
        return seen(
            config, settled, market, wallets=(6000, 6000), allowed=(1000, 0, 0), position=stored
        )

    paid = AssetAmount(asset=evm_network(BASE).native_asset, amount=0)
    monkeypatch.setattr(reconciliation, "read_pool", lambda *read: approved.pool)
    monkeypatch.setattr(reconciliation, "movement", lambda *read: taken)
    monkeypatch.setattr(reconciliation, "stored_position", lambda *read: stored)
    monkeypatch.setattr(reconciliation, "snapshot", snapshot)
    monkeypatch.setattr(reconciliation, "charged_fees", lambda *read: (paid, None))
    monkeypatch.setattr(
        "almanak.connectors.aerodrome.state.Outcome", lambda **fields: SimpleNamespace(**fields)
    )
    pending = SimpleNamespace(
        operation=mint,
        before=approved,
        decision=SimpleNamespace(intent=intent),
        tx_hash="0x" + "ab" * 32,
        nonce=3,
        call=None,
        execution=None,
    )
    backend = SimpleNamespace(reader=None, config=config)
    outcome = reconciliation.reconcile(backend, pending, {"status": "0x1"}, market.block)
    after = outcome.after
    # The deposit's own effects, and the whole stored position owned.
    assert (outcome.success, outcome.delta0, outcome.delta1) == (True, -3000, -3000)
    assert (after.owned.inventory0.amount, after.owned.inventory1.amount) == (1000, 0)
    assert after.owned.position == after.position == stored
    (issue,) = outcome.accounting_issues
    assert "observed LP shares differ from retained receipt effects" in issue
    assert asked == {"quotes": False, "residual": False, "mined": True}


def test_a_burn_spends_its_share_approval_whole_and_a_deposit_leaves_what_it_did_not_use(variant):
    market, config, state = started(variant, stated("close-liquidity"), holds=True)
    target = config.target(ALLOCATION)
    (owned,) = state.owned
    approved = seen(config, owned, market, allowed=(4000, 3000, SHARES), position=owned.position)
    close = ops.Close(
        target=target,
        shares=owned.position.shares,
        minimum0=AssetAmount(asset=target.assets[0], amount=995),
        minimum1=AssetAmount(asset=target.assets[1], amount=995),
        deadline=market.block.timestamp + 300,
    )
    released = Effects(delta0=1000, delta1=1000, shares_delta=-SHARES)
    after = reconciliation.allowances_after(approved, close, released, True)
    assert [item.amount for item in after] == [4000, 3000, 0]
    # A deposit that took 3000 of each leaves 1000 of the first currency approved.
    mint = ops.Increase(
        target=target,
        maximum0=AssetAmount(asset=target.assets[0], amount=4000),
        maximum1=AssetAmount(asset=target.assets[1], amount=3000),
        minimum0=AssetAmount(asset=target.assets[0], amount=2985),
        minimum1=AssetAmount(asset=target.assets[1], amount=2985),
        minimum_shares=AssetAmount(asset=target.pool.shares_asset, amount=1),
        deadline=market.block.timestamp + 300,
    )
    taken = Effects(delta0=-3000, delta1=-3000, shares_delta=3 * 10**9)
    left = reconciliation.allowances_after(approved, mint, taken, True)
    assert [item.amount for item in left] == [1000, 0, SHARES]
    # A reverted step spends nothing.
    assert reconciliation.allowances_after(approved, mint, taken, False) == approved.allowances


def test_a_revocation_zeroes_each_allowance_shares_included_and_refuses_when_there_is_none(
    variant,
):
    request = stated("revoke-exchange")
    market, config, state, before, intent = admitted(
        variant, request, holds=True, allowed=(1000, 5, 9)
    )
    target = config.target(ALLOCATION)
    (owned,) = state.owned
    assert (intent.kind, intent.target) == ("revoke_exchange", target)
    step = checked(config, state, intent, before)
    held = {"position": owned.position}
    cleared = [
        step(seen(config, owned, market, allowed=allowed, **held))
        for allowed in ((1000, 5, 9), (0, 5, 9), (0, 0, 9))
    ]
    assert [(item.kind, item.amount) for item in cleared] == [("approve", 0)] * 3
    assert [item.asset for item in cleared] == [*target.assets, target.pool.shares_asset]
    # The wallet's shares are owned and untouched: a revocation only approves zero.
    clean = seen(config, owned, market, position=owned.position)
    with pytest.raises(ValueError, match="no allowance in either pool currency or in the pool"):
        direct.binding(CONNECTOR).intent(config, (clean,), request)


def test_the_report_holds_the_shares_what_moved_the_inventory_and_the_allowances(variant):
    market, config, state = started(variant, depositing("increase-liquidity"), holds=True)
    target = config.target(ALLOCATION)
    (owned,) = state.owned
    report = direct.binding(CONNECTOR).position
    assets = [str(asset) for asset in target.assets]

    def listed(values):
        return [
            {"asset": asset, "amount": str(value)}
            for asset, value in zip(assets, values, strict=True)
        ]

    shares = str(target.pool.shares_asset)
    held = {
        "shares": {"asset": shares, "amount": str(SHARES)},
        "fees_recorded": listed(RECORDED),
    }
    first = seen(config, owned, market, position=owned.position)
    fresh = SimpleNamespace(owned=(owned,), baseline=(first,), outcomes=())
    assert report(fresh) == {
        "position": held,
        "moved": listed((0, 0)),
        "inventory": listed((4000, 3000)),
        "allowances": [],
    }
    # After the two approvals and a deposit that took 3000 of each, and one reverted step.
    grown = holding(target, shares=SHARES + 3 * 10**9, recorded=(13, 13), indices=POOL_INDICES)
    after = owned.model_copy(
        update={
            "position": grown,
            "inventory0": AssetAmount(asset=target.assets[0], amount=1000),
            "inventory1": AssetAmount(asset=target.assets[1], amount=0),
        }
    )
    last = seen(config, after, market, allowed=(1000, 0, 5), position=grown)
    steps = (
        SimpleNamespace(success=True, delta0=0, delta1=0, after=first),
        SimpleNamespace(success=False, delta0=-9, delta1=-9, after=first),
        SimpleNamespace(success=True, delta0=-3000, delta1=-3000, after=last),
    )
    done = SimpleNamespace(owned=(after,), baseline=(first,), outcomes=steps)
    router = target.contracts.router
    assert report(done) == {
        "position": {
            "shares": {"asset": shares, "amount": str(SHARES + 3 * 10**9)},
            "fees_recorded": listed((13, 13)),
        },
        "moved": listed((-3000, -3000)),
        "inventory": listed((1000, 0)),
        "allowances": [
            {"asset": assets[0], "spender": router, "amount": "1000"},
            {"asset": shares, "spender": router, "amount": "5"},
        ],
    }
    closed = SimpleNamespace(
        owned=(after.model_copy(update={"position": None}),), baseline=(), outcomes=()
    )
    assert report(closed)["position"] is None and report(closed)["allowances"] == []


def test_the_command_builds_the_deployment_through_the_connectors_resolver(variant, monkeypatch):
    block, rows = retained()

    class Head(RetainedResolutionReader):
        def pin_block(self, selector):
            return block if selector == "latest" else super().pin_block(selector)

    monkeypatch.setattr(
        "almanak.gateway.ethereum.EthereumReader", lambda chain, endpoint: Head(chain, rows)
    )
    market = resolved(variant)
    wallet = {"account": WALLET.account, "address": WALLET.address}
    for request in (depositing(), stated("close-liquidity"), stated("revoke-exchange")):
        config = ax_actions.deployment(
            CONNECTOR,
            selection(variant),
            request,
            wallet,
            ax_actions.alchemy(BASE),
            "endpoint",
            LIMITS,
        )
        assert config == deployed(market, request)


def arguments(variant, command, *extra, without=()):
    given = {
        "--connector": CONNECTOR,
        "--chain": str(BASE),
        "--pool": POOLS[variant],
        "--wallet": "wallet",
        "--env-file": ".env",
        "--max-gas-price-wei": "7",
        "--max-transaction-fee": "9",
    }
    if command in ADDITIONS:
        given.update({"--amount0": "4000", "--amount1": "3000"})
    if command == "decrease-liquidity":
        given["--liquidity"] = "250"
    if command in PRICED:
        given["--slippage-bps"] = "50"
    flags = [part for name, value in given.items() if name not in without for part in (name, value)]
    return parse_validated(build_parser(), ["ax", command, *flags, *extra])


@pytest.mark.parametrize("command", list(COMMANDS))
def test_the_command_line_states_each_command_and_only_what_it_takes(variant, command, capsys):
    args = arguments(variant, command)
    assert (args.product, args.ax_command, args.execute) == ("operator-action", command, False)
    priced = command in PRICED
    adds = command in ADDITIONS
    assert ax_actions.action_request(args) == ActionRequest(
        command,
        slippage_bps=50 if priced else None,
        deadline_seconds=300 if priced else None,
        amount0=4000 if adds else None,
        amount1=3000 if adds else None,
        liquidity=250 if command == "decrease-liquidity" else None,
    )
    market = ax_actions.market_request(args)
    assert (market.connector, market.chain, market.pool) == (CONNECTOR, BASE, POOLS[variant])
    required = ["--chain", "--pool", "--wallet", "--max-gas-price-wei"]
    required += ["--amount0", "--amount1"] * adds
    required += ["--liquidity"] * (command == "decrease-liquidity")
    required += ["--slippage-bps"] * priced
    for missing in required:
        with pytest.raises(SystemExit):
            arguments(variant, command, without=(missing,))
    refused = [("--amount", "5"), ("--sell", "x"), ("--shares", "5")]
    refused += [("--liquidity", "0"), ("--slippage-bps", "0"), ("--slippage-bps", "10000")]
    if not priced:
        refused += [("--slippage-bps", "50"), ("--deadline-seconds", "60")]
    if not adds:
        refused += [("--amount0", "5")]
    for extra in refused:
        with pytest.raises(SystemExit):
            arguments(variant, command, *extra)
    capsys.readouterr()


def test_the_command_line_refuses_a_tick_range_and_a_token_id_by_name(variant, capsys):
    with pytest.raises(SystemExit):
        arguments(variant, "open-liquidity", "--tick-lower", "-60", "--tick-upper", "60")
    assert "aerodrome open-liquidity takes no --tick-lower, --tick-upper" in (
        capsys.readouterr().err
    )
    with pytest.raises(SystemExit):
        arguments(variant, "open-liquidity", "--tick-upper", "60")
    assert "open-liquidity takes no --tick-upper" in capsys.readouterr().err
    for command in (
        "increase-liquidity",
        "decrease-liquidity",
        "collect-liquidity-fees",
        "close-liquidity",
    ):
        with pytest.raises(SystemExit):
            arguments(variant, command, "--token-id", "7")
        assert f"{command} takes no --token-id" in capsys.readouterr().err
    # A connector whose position is an NFT in a range still requires both by name.
    v3 = ["ax", "close-liquidity", "--connector", "uniswap_v3", "--chain", str(BASE)]
    v3 += ["--pool", POOLS[variant], "--slippage-bps", "50", "--wallet", "w", "--env-file", "e"]
    v3 += ["--max-gas-price-wei", "7", "--max-transaction-fee", "9"]
    with pytest.raises(SystemExit):
        parse_validated(build_parser(), v3)
    assert "uniswap_v3 close-liquidity requires --token-id" in capsys.readouterr().err
    assert parse_validated(build_parser(), [*v3, "--token-id", "7"]).token_id == 7
    # Curve's per-coin amount and payout choice are not Aerodrome's, and a deposit
    # states both of its amounts.
    coin = f"{BASE}/erc20:0x4200000000000000000000000000000000000006=5"
    refusals = {
        ("open-liquidity", "--amount", coin): "aerodrome open-liquidity takes no --amount",
        (
            "close-liquidity",
            "--receive",
            "balanced",
        ): "aerodrome close-liquidity takes no --receive",
        ("increase-liquidity", "--amount1", "0"): "aerodrome increase-liquidity requires --amount1",
    }
    for (command, *extra), message in refusals.items():
        with pytest.raises(SystemExit):
            arguments(variant, command, *extra)
        assert message in capsys.readouterr().err


def test_the_command_refuses_an_undeclared_chain_and_passes_the_request_on(variant, monkeypatch):
    passed = {}

    def deployment(name, market, action, wallet, provider, endpoint, limits, **options):
        passed.update(connector=name, market=market, action=action)
        raise StopIteration

    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
    monkeypatch.setattr(ax_actions, "deployment", deployment)
    with pytest.raises(StopIteration):
        ax_actions.command(arguments(variant, "open-liquidity"))
    assert (passed["connector"], passed["market"].pool) == (CONNECTOR, POOLS[variant])
    assert passed["action"] == depositing()
    monkeypatch.setattr(ax_actions, "read_wallet", pytest.fail)
    with pytest.raises(
        ValueError, match="aerodrome serves no direct open-liquidity on eip155:31337"
    ):
        ax_actions.command(arguments(variant, "open-liquidity", "--chain", "eip155:31337"))


def position_arguments(variant, *extra, without=()):
    given = {
        "--connector": CONNECTOR,
        "--chain": str(BASE),
        "--pool": POOLS[variant],
        "--account": WALLET.address,
        "--env-file": ".env",
    }
    flags = [part for name, value in given.items() if name not in without for part in (name, value)]
    return parse_validated(build_parser(), ["ax", "liquidity-position", *flags, *extra])


def test_the_position_read_names_an_account_and_takes_no_token_id(variant, capsys, monkeypatch):
    for name in ("ALMANAK_AX_GATEWAY_URL", "ALMANAK_AX_GATEWAY_AUTH_FILE"):
        monkeypatch.delenv(name, raising=False)
    args = position_arguments(variant)
    assert (args.product, args.block) == ("ax-liquidity-position", "finalized")
    assert ax.parse_request(args) == PositionRequest(
        chain=BASE, pool=POOLS[variant], account=WALLET.address
    )
    gateway = ("--gateway-url", "http://127.0.0.1:1", "--gateway-auth-file", "token")
    assert position_arguments(variant, *gateway, without=("--env-file",)).gateway_url
    for missing in ("--pool", "--account", "--connector", "--env-file"):
        with pytest.raises(SystemExit):
            position_arguments(variant, without=(missing,))
    capsys.readouterr()
    with pytest.raises(SystemExit):
        position_arguments(variant, "--token-id", "7")
    assert "aerodrome takes no --token-id for this request" in capsys.readouterr().err
    for extra in (("--config", "request.json"), ("--wallet", "wallet")):
        with pytest.raises(SystemExit):
            position_arguments(variant, *extra)
    # A connector whose position is an NFT takes no account.
    v3 = ["ax", "liquidity-position", "--connector", "uniswap_v3", "--chain", str(BASE)]
    v3 += ["--pool", POOLS[variant], "--env-file", ".env"]
    with pytest.raises(SystemExit):
        parse_validated(build_parser(), [*v3, "--token-id", "7", "--account", WALLET.address])
    assert "uniswap_v3 takes no --account for this request" in capsys.readouterr().err
    assert parse_validated(build_parser(), [*v3, "--token-id", "7"]).token_id == 7
    values = {"chain": str(BASE), "pool": POOLS[variant], "account": WALLET.address}
    for refused in ({"account": ZERO_ADDRESS}, {"pool": ZERO_ADDRESS}, {"chain": "solana:5eykt4"}):
        with pytest.raises(ValueError):
            PositionRequest.model_validate_json(json.dumps({**values, **refused}))


def reading(variant, monkeypatch, position):
    """A reader of the retained resolution, with the pool and the account's stored
    position as the chain would return them."""
    market = resolved(variant)
    config = deployed(market, stated("close-liquidity"))
    target = config.target(ALLOCATION)
    pool = seen(config, created(config).owned[0], market).pool
    monkeypatch.setattr("almanak.connectors.aerodrome.reader.read_pool", lambda *read: pool)
    asked = []

    def stored(reader, holder, observed):
        asked.append((holder.fund.address, holder.pool, observed))
        return position(target)

    monkeypatch.setattr("almanak.connectors.aerodrome.observations.stored_position", stored)
    block, rows = retained()
    request = PositionRequest(chain=BASE, pool=POOLS[variant], account=WALLET.address)
    return market, pool, RetainedResolutionReader(block.chain, rows), request, asked


def test_the_position_read_reports_shares_principal_and_what_a_claim_pays(variant, monkeypatch):
    market, pool, reader, request, asked = reading(variant, monkeypatch, holding)
    report = read_liquidity_position(reader, request, market.block)
    assets = (market.asset0, market.asset1)

    def sides(values):
        return [
            {"asset": str(asset.identifier), "amount": str(value), "decimals": asset.decimals}
            for asset, value in zip(assets, values, strict=True)
        ]

    assert asked == [(WALLET.address, market.pool, pool)]
    assert (report["product"], report["support"]) == ("liquidity_position", "verified")
    assert report["request"] == {**request.model_dump(mode="json"), "connector": CONNECTOR}
    assert (report["owner"], report["block"]) == (
        WALLET.address,
        market.block.model_dump(mode="json"),
    )
    assert report["resolution"] == market.summary()
    assert report["position"] == {
        "shares": {"asset": str(market.pool.shares_asset), "amount": str(SHARES)},
        "liquidity": str(SHARES),
    }
    assert report["pool"] == {
        "stable": variant == "stable",
        "total_supply": str(SUPPLY),
        "balances": sides((RESERVE, RESERVE)),
    }
    # 10**9 of 10**18 shares of 10**12 of each currency.
    assert report["principal"] == sides((1000, 1000))
    # The recorded claims, plus the shares' part of the one index that moved.
    assert report["owed_recorded"] == sides(RECORDED)
    assert report["owed"] == sides((RECORDED[0] + 2, RECORDED[1]))


def test_the_position_read_of_an_account_without_shares_is_zero_everywhere(variant, monkeypatch):
    def none(target):
        return holding(target, shares=0, recorded=(0, 0), indices=(0, 0))

    market, _, reader, request, _ = reading(variant, monkeypatch, none)
    report = read_liquidity_position(reader, request, market.block)
    assert report["position"]["shares"]["amount"] == "0"
    for field in ("principal", "owed", "owed_recorded"):
        assert [item["amount"] for item in report[field]] == ["0", "0"]
    elsewhere = SimpleNamespace(chain=SimpleNamespace(chain_id="eip155:1"))
    with pytest.raises(ValueError, match="request chain differs from the reader chain"):
        read_liquidity_position(elsewhere, request, market.block)


def test_the_read_command_pins_its_block_and_passes_the_request_on(variant, monkeypatch):
    market = resolved(variant)
    passed = {}

    class Reader:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def pin_block(self, selector):
            passed["selector"] = selector
            return market.block

    def read(reader, request, block):
        passed.update(request=request, block=block)
        return {"product": "liquidity_position"}

    monkeypatch.setattr(ax, "observation_reader", lambda args, chain: Reader())
    monkeypatch.setattr(position_request, "read_liquidity_position", read)
    monkeypatch.setattr(ax, "read_liquidity_position", pytest.fail)
    args = position_arguments(variant, "--block", "latest")
    assert ax.liquidity_position(args) == {"product": "liquidity_position"}
    assert (passed["selector"], passed["block"]) == ("latest", market.block)
    assert passed["request"].account == WALLET.address
    with pytest.raises(ValueError, match="aerodrome declares no support on eip155:31337"):
        ax.liquidity_position(position_arguments(variant, "--chain", "eip155:31337"))
