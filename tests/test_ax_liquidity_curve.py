"""The direct liquidity commands of `almanak ax` for Curve.

Component evidence without signing or chain access, through the connector's own models,
builders, planning and validation: observations are built over the retained pool
observations of a two-coin StableSwap NG pool and a three-coin TriCrypto NG pool. The
fork rehearsal of the whole commands is tests/fork/test_ax_liquidity_curve.py.
"""

import json
from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak import ax, ax_actions
from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.curve import direct as curve_direct
from almanak.connectors.curve import liquidity_direct as liquidity
from almanak.connectors.curve import liquidity_quotes, position_request, receipts, reconciliation
from almanak.connectors.curve import reader as curve_reader
from almanak.connectors.curve import resolution as curve_resolution
from almanak.connectors.curve.models import Target
from almanak.connectors.curve.observations import Snapshot, held_shares, quotes_for
from almanak.connectors.curve.pool_abi import layout
from almanak.connectors.curve.positions import Position
from almanak.connectors.curve.simulation import validate_simulation
from almanak.connectors.curve.validation import validate_current
from almanak.connectors.direct import ALLOCATION, ActionRequest
from almanak.dex_intents import SwapExactInput
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.test_ax_swap_curve import (
    MARKETS,
    WALLET,
    Shares,
    amounts,
    configuration,
    created,
    operator_config,
    strategy_checkpoint,
)

COMMANDS = {
    "open-liquidity": "open_liquidity",
    "increase-liquidity": "increase_liquidity",
    "decrease-liquidity": "decrease_liquidity",
    "close-liquidity": "close_liquidity",
    "revoke-exchange": "revoke_exchange",
}
PRICED = tuple(name for name in COMMANDS if name != "revoke-exchange")
WINDOW, SLIPPAGE_BPS = 300, 50
# LP shares the wallet holds where a test states it holds some, and how many it burns.
HELD, BURNED = 10**16, 10**15
FOREIGN = "eip155:1/erc20:0x" + "ab" * 20


@pytest.fixture(params=tuple(MARKETS))
def market(request):
    return MARKETS[request.param]


def deposit(market):
    """A thousandth of a whole unit of the pool's first and last coin; none of a middle one."""
    coins, assets = market.pool.coins, market.observed.currencies
    ends = ((coins[0], assets[0]), (coins[-1], assets[-1]))
    return {coin: 10**asset.decimals // 1000 for coin, asset in ends}


def pairs(stated) -> tuple:
    return tuple((str(coin), amount) for coin, amount in stated.items())


def request(action, **named):
    priced = action in PRICED
    return ActionRequest(
        action,
        slippage_bps=SLIPPAGE_BPS if priced else None,
        deadline_seconds=WINDOW if priced else None,
        **named,
    )


def stated_request(market, action):
    """Each command as an operator states it for the market."""
    named = {
        "open-liquidity": {"amounts": pairs(deposit(market))},
        "increase-liquidity": {"amounts": pairs(deposit(market))},
        "decrease-liquidity": {"liquidity": BURNED, "receive": "balanced"},
        "close-liquidity": {"receive": "balanced"},
        "revoke-exchange": {},
    }
    return request(action, **named[action])


def positions(market, stated, held=0):
    resolved = SimpleNamespace(pool=market.pool)
    return liquidity.positions(Shares(held), market.block, resolved, WALLET, stated)


def config_for(market, stated, held=None):
    """The deployment the command builds, for a wallet that holds what the command needs."""
    if held is None:
        held = 0 if stated.action in ("open-liquidity", "revoke-exchange") else HELD
    return configuration(market, positions(market, stated, held))


def observation(market, config, owned, *, shares=0, allowances=None, held=None, spent=0, **fields):
    """An on-chain observation of the deployment's one position at the retained block.

    The wallet holds `held` of each coin, by default a hundred times the deposit, and
    `shares` of the pool's LP token, which a liquidity target reads as its position.
    """
    target = config.target(ALLOCATION)
    wallet = {coin: 100 * amount for coin, amount in deposit(market).items()}
    position = Position(shares=AssetAmount(asset=market.pool.shares_asset, amount=shares))
    return Snapshot(
        target=target,
        pool=market.observed,
        owned=owned,
        wallets=amounts(target, wallet if held is None else held),
        allowances=tuple(
            {"asset": coin, "spender": target.pool.pool, "amount": (allowances or {}).get(coin, 0)}
            for coin in target.assets
        ),
        native_balance=AssetAmount(
            asset=evm_network(market.pool.chain.chain_id).native_asset, amount=10**18 - spent
        ),
        payer=target.fund,
        **{"position": position if shares else None, **fields},
    )


def adopted(market, action, *, shares=None, **observed):
    """A deployment that has adopted what its first observation holds, and that observation."""
    stated = stated_request(market, action)
    config = config_for(market, stated)
    state = created(config)
    if shares is None:
        shares = 0 if action in ("open-liquidity", "revoke-exchange") else HELD
    found = connector_from_wire({"connector": "curve"})
    first = observation(market, config, state.owned[0], shares=shares, **observed)
    (owned,) = found.lifecycle.adopt(state.owned, (first,))
    seen = observation(market, config, owned, shares=shares, **observed)
    return stated, config, state.model_copy(update={"owned": (owned,)}), seen


def planning(config, state, intent, seen):
    """The planned operation of one observation, checked as the gateway checks it."""
    execution = connector_from_wire({"connector": "curve"}).execution
    deciding = SimpleNamespace(
        config=config,
        owned=state.owned,
        capabilities=state.capabilities,
        decision=SimpleNamespace(intent=intent, observations=(seen,)),
        outcomes=(),
        mode="running",
    )

    def step(before):
        operation = execution.operation(config, deciding, intent, before)
        execution.validate_operation(None, deciding, operation, before)
        return operation

    return deciding, step


def test_curve_serves_the_liquidity_commands_on_every_declared_chain():
    found = connector_from_wire({"connector": "curve"})
    assert set(COMMANDS) < set(direct.binding("curve").actions)
    cells = [cell for cell in found.chain_support if cell.direct_actions]
    assert len(cells) == len(found.chain_support) == 8
    for cell in cells:
        assert set(COMMANDS.values()) < {str(action) for action in cell.direct_actions}
        for action in COMMANDS:
            direct.require_direct("curve", cell.chain, ActionRequest(action))
    with pytest.raises(ValueError, match="curve serves no direct open-liquidity on eip155:31337"):
        direct.require_direct("curve", ChainId("eip155:31337"), ActionRequest("open-liquidity"))
    # Curve fees stay inside the share value: there is nothing to collect.
    with pytest.raises(ValueError, match="curve serves no direct collect-liquidity-fees"):
        direct.require_direct("curve", cells[0].chain, ActionRequest("collect-liquidity-fees"))
    for cell in cells:
        with pytest.raises(ValueError, match="unsupported as a direct action"):
            cell.require_direct("exit_exchange")


def test_a_deposit_is_one_adopting_liquidity_allocation_funded_with_the_stated_amounts(market):
    stated = deposit(market)
    for action in ("open-liquidity", "increase-liquidity"):
        config = config_for(market, stated_request(market, action))
        (allocation,) = config.allocations
        assert (allocation.name, allocation.account, allocation.pool) == (
            ALLOCATION,
            WALLET.account,
            market.pool,
        )
        # Liquidity authority, the fund's shares adopted, and nothing to swap with.
        assert (allocation.liquidity_enabled, allocation.adopt, allocation.entry_swap_bps) == (
            True,
            True,
            0,
        )
        capital = {value.asset: value.amount for value in allocation.capital.root}
        assert list(capital) == list(market.pool.coins)
        assert capital == {coin: stated.get(coin, 0) for coin in market.pool.coins}
        assert (allocation.slippage_bps, allocation.deadline_seconds) == (SLIPPAGE_BPS, WINDOW)
        assert (allocation.single_coin_exit, allocation.minimum_native_balance) == (None, 0)
        state = created(config)
        assert state.owned[0].inventory == allocation.capital and state.owned[0].position is None
        assert direct.binding_for(config) is direct.binding("curve")


def test_a_burn_brings_no_inventory_and_states_how_it_pays_out(market):
    coin = market.pool.coins[-1]
    for action in ("decrease-liquidity", "close-liquidity"):
        named = {"liquidity": BURNED} if action == "decrease-liquidity" else {}
        balanced = config_for(market, request(action, receive="balanced", **named))
        single = config_for(market, request(action, receive=str(coin), **named))
        for config, payout in ((balanced, None), (single, coin)):
            (allocation,) = config.allocations
            assert not any(value.amount for value in allocation.capital.root)
            assert (allocation.adopt, allocation.single_coin_exit) == (True, payout)
            assert config.target(ALLOCATION).single_coin_exit == payout
    revoking = config_for(market, request("revoke-exchange"))
    (allocation,) = revoking.allocations
    assert (allocation.slippage_bps, allocation.deadline_seconds) == (
        0,
        liquidity.ADMISSION_SECONDS,
    )


def test_only_an_operator_liquidity_allocation_adopts(market):
    config = config_for(market, stated_request(market, "open-liquidity"))
    wire = json.loads(config.model_dump_json())
    assert wire["allocations"][0]["adopt"] is True
    strategy = strategy_checkpoint().strategy
    with pytest.raises(ValueError, match="only an operator deployment adopts the fund's LP shares"):
        created(config, strategy)
    # A strategy allocation never carries the field, so its wire form is what it was.
    retained = json.loads(strategy_checkpoint().config.model_dump_json())
    assert "adopt" not in retained["allocations"][0]
    document = positions(market, stated_request(market, "open-liquidity"))
    for change in ({"liquidity_enabled": False}, {"entry_swap_bps": 5000}):
        broken = {"allocations": [{**document["allocations"][0], **change}]}
        with pytest.raises(ValueError, match="provides liquidity and swaps nothing"):
            configuration(market, broken)
    # Only an adopting allocation may bring no inventory at all.
    empty = positions(market, request("close-liquidity", receive="balanced"), HELD)
    with pytest.raises(ValueError, match="liquidity addition requires positive inventory"):
        configuration(market, {"allocations": [{**empty["allocations"][0], "adopt": False}]})


def test_a_command_states_its_amounts_its_payout_and_its_bounds(market):
    coins = [str(coin) for coin in market.pool.coins]
    refused = {
        "requires --amount": request("open-liquidity"),
        "requires --slippage-bps": ActionRequest("open-liquidity", amounts=((coins[0], 5),)),
        "requires --liquidity, --receive": request("decrease-liquidity"),
        "requires --receive": request("close-liquidity"),
        f"--amount names {FOREIGN}, which is not a coin of the pool": request(
            "open-liquidity", amounts=((FOREIGN, 5),)
        ),
        f"--amount names {coins[0]} more than once": request(
            "increase-liquidity", amounts=((coins[0], 5), (coins[0], 6))
        ),
        "open-liquidity requires an --amount above zero": request(
            "open-liquidity", amounts=((coins[0], 0),)
        ),
        "--receive must be `balanced` or one of the pool's coins": request(
            "close-liquidity", receive=FOREIGN
        ),
    }
    for message, stated in refused.items():
        reader = Shares(HELD)
        with pytest.raises(ValueError, match=message):
            liquidity.positions(
                reader, market.block, SimpleNamespace(pool=market.pool), WALLET, stated
            )
        # A statement that is wrong by itself is refused before any read.
        assert reader.asked == []
    # A coin stated twice is refused whatever its first amount.
    twice = request("open-liquidity", amounts=((coins[0], 0), (coins[0], 5)))
    with pytest.raises(ValueError, match="more than once"):
        liquidity.stated_amounts(twice, tuple(coins))
    # A command that deposits nothing brings none of any coin.
    assert liquidity.stated_amounts(request("close-liquidity"), tuple(coins)) == dict.fromkeys(
        coins, 0
    )


def test_the_wallets_lp_balance_decides_which_command_serves_it(market):
    reader = Shares(HELD)
    resolved = SimpleNamespace(pool=market.pool)
    opening = stated_request(market, "open-liquidity")
    with pytest.raises(ValueError, match=f"already holds {HELD} base units") as refusal:
        liquidity.positions(reader, market.block, resolved, WALLET, opening)
    assert "increase-liquidity adds to them" in str(refusal.value)
    # The balance read is the wallet's own, of the pool's LP token, at the resolved block.
    assert reader.asked == [(market.pool.pool, (WALLET.address,), market.block)]
    for action in ("increase-liquidity", "decrease-liquidity", "close-liquidity"):
        with pytest.raises(ValueError, match="holds none of this pool's LP token"):
            positions(market, stated_request(market, action), 0)
    for burned in (HELD, HELD + 1):
        stated = request("decrease-liquidity", liquidity=burned, receive="balanced")
        with pytest.raises(ValueError, match=f"must be below the wallet's {HELD} base units"):
            positions(market, stated, HELD)
    # A revocation acts on allowances, whatever the wallet holds.
    for held in (0, HELD):
        assert positions(market, request("revoke-exchange"), held)["allocations"][0]["adopt"]


def test_adoption_takes_the_wallets_whole_lp_balance_and_the_stated_coins_it_covers(market):
    found = connector_from_wire({"connector": "curve"})
    config = config_for(market, stated_request(market, "increase-liquidity"))
    owned = created(config).owned
    seen = observation(market, config, owned[0], shares=HELD)
    (adopting,) = found.lifecycle.adopt(owned, (seen,))
    # The position is the balance as observed; the shares were minted before, so the
    # entry is made. The stated coins are the inventory, and nothing else of the wallet.
    assert (adopting.position, adopting.entry_complete) == (seen.position, True)
    assert (adopting.position.shares.amount, adopting.inventory) == (HELD, owned[0].inventory)
    # Later observations of the same balance are the deployment's own.
    again = observation(market, config, adopting, shares=HELD)
    found.lifecycle.validate_owned(SimpleNamespace(owned=(adopting,)), (again,))
    # A wallet without shares adopts none, and one that does not cover the coins is refused.
    bare = observation(market, config, owned[0])
    assert found.lifecycle.adopt(owned, (bare,)) == owned
    short = {coin: amount - 1 for coin, amount in deposit(market).items()}
    with pytest.raises(DataError, match="the wallet holds") as refusal:
        found.lifecycle.adopt(owned, (observation(market, config, owned[0], held=short),))
    assert refusal.value.kind == "inventory"


def test_one_lp_balance_is_a_position_or_unowned_by_the_target_never_both(market):
    """A liquidity command owns the wallet's shares; a swap from the same wallet does not."""
    providing = config_for(market, stated_request(market, "close-liquidity"))
    trading = operator_config(market)
    shares = AssetAmount(asset=market.pool.shares_asset, amount=HELD)
    own = held_shares(Shares(HELD), providing.target(ALLOCATION), market.block)
    other = held_shares(Shares(HELD), trading.target(ALLOCATION), market.block)
    assert own == {"position": Position(shares=shares)}
    assert other == {"position": None, "unowned_shares": shares}
    # No observation can hold both readings, or the other target's reading.
    owned = created(providing).owned[0]
    with pytest.raises(ValueError, match="beside a trading-only target"):
        observation(market, providing, owned, shares=HELD, unowned_shares=shares)
    with pytest.raises(ValueError, match="liquidity authority"):
        observation(market, trading, created(trading).owned[0], shares=HELD)


def test_each_command_builds_its_one_intent_from_the_observation_it_is_admitted_with(market):
    for action, kind in COMMANDS.items():
        observed = {"allowances": {market.pool.coins[0]: 7}} if action == "revoke-exchange" else {}
        stated, config, state, seen = adopted(market, action, **observed)
        intent = curve_direct.intent(config, (seen,), stated)
        assert (intent.kind, intent.target) == (kind, config.target(ALLOCATION))
        connector_from_wire({"connector": "curve"}).codec.capability.admit(
            state.capabilities, intent
        )
        if action != "revoke-exchange":
            assert intent.deadline == market.block.timestamp + WINDOW


def test_a_deposit_offers_exactly_the_stated_amounts_and_a_burn_its_formula_less_slippage(market):
    stated, config, state, seen = adopted(market, "open-liquidity")
    opened = curve_direct.intent(config, (seen,), stated)
    assert opened.maximums == state.owned[0].inventory
    assert {v.asset: v.amount for v in opened.maximums.root if v.amount} == deposit(market)

    stated, config, state, seen = adopted(market, "decrease-liquidity")
    decreased = curve_direct.intent(config, (seen,), stated)
    paid = liquidity_quotes.removed(market.observed, BURNED, None)
    floors = [amount * (10000 - SLIPPAGE_BPS) // 10000 for amount in paid]
    assert (decreased.liquidity, decreased.output_asset) == (BURNED, None)
    assert [value.amount for value in decreased.minimums.root] == floors and min(paid) > 0

    # A burn paid in one coin asks for that coin alone, by the pool's one-coin formula.
    coin, index = market.pool.coins[-1], len(market.pool.coins) - 1
    single = request("close-liquidity", receive=str(coin))
    config = config_for(market, single)
    state = created(config)
    seen = observation(market, config, state.owned[0], shares=HELD)
    (owned,) = connector_from_wire({"connector": "curve"}).lifecycle.adopt(state.owned, (seen,))
    closed = curve_direct.intent(config, (observation(market, config, owned, shares=HELD),), single)
    alone = liquidity_quotes.removed(market.observed, HELD, index)[index]
    assert closed.output_asset == coin
    assert [value.amount for value in closed.minimums.root][index] == (
        alone * (10000 - SLIPPAGE_BPS) // 10000
    )
    assert not any(value.amount for value in closed.minimums.root[:index])


def test_the_observation_decides_again_what_the_wallets_balance_serves(market):
    """The same refusals as at resolution, against the observation a request is admitted with."""
    stated, config, state, seen = adopted(market, "open-liquidity", shares=HELD)
    with pytest.raises(ValueError, match=f"already holds {HELD} base units"):
        curve_direct.intent(config, (seen,), stated)
    for action in ("increase-liquidity", "decrease-liquidity", "close-liquidity"):
        stated, config, state, seen = adopted(market, action, shares=0)
        with pytest.raises(ValueError, match="holds none of this pool's LP token"):
            curve_direct.intent(config, (seen,), stated)
    stated, config, state, seen = adopted(market, "decrease-liquidity", shares=BURNED)
    with pytest.raises(ValueError, match=f"must be below the wallet's {BURNED} base units"):
        curve_direct.intent(config, (seen,), stated)
    stated, config, state, seen = adopted(market, "revoke-exchange")
    with pytest.raises(ValueError, match="no allowance in any of its coins"):
        curve_direct.intent(config, (seen,), stated)


def test_the_gateway_admits_one_exact_approval_per_coin_then_the_deposit_and_nothing_larger(
    market,
):
    for action, kind in (("open-liquidity", "mint"), ("increase-liquidity", "increase")):
        stated, config, state, seen = adopted(market, action)
        intent = curve_direct.intent(config, (seen,), stated)
        deciding, step = planning(config, state, intent, seen)
        wanted, held, pool = deposit(market), seen.owned.position, market.pool.pool
        shares = 0 if held is None else held.shares.amount
        granted: dict = {}
        for coin, amount in wanted.items():
            approval = step(
                observation(market, config, state.owned[0], shares=shares, allowances=granted)
            )
            assert (approval.kind, approval.asset, approval.spender, approval.amount) == (
                "approve",
                coin,
                pool,
                amount,
            )
            granted[coin] = amount
        approved = observation(market, config, state.owned[0], shares=shares, allowances=granted)
        added = step(approved)
        expected = liquidity_quotes.minted(
            market.observed, tuple(v.amount for v in intent.maximums.root)
        )
        # The pool is given exactly the stated amounts and must mint the formula's
        # shares less the stated slippage.
        assert (added.kind, added.amounts) == (kind, intent.maximums)
        assert added.minimum_shares.amount == expected * (10000 - SLIPPAGE_BPS) // 10000 > 0
        # An allowance of another size is reset before the exact one is granted.
        first = next(iter(wanted))
        stale = observation(market, config, state.owned[0], shares=shares, allowances={first: 1})
        assert (step(stale).kind, step(stale).amount) == ("approve", 0)

        # Nothing deposits more than the stated amounts: they are the whole inventory.
        larger = intent.model_copy(
            update={"maximums": amounts(intent.target, {first: wanted[first] + 1})}
        )
        deciding.decision = SimpleNamespace(intent=larger, observations=(seen,))
        execution = connector_from_wire({"connector": "curve"}).execution
        with pytest.raises(DataError, match="exceeds attributed input inventory"):
            execution.validate_operation(
                None, deciding, execution.operation(config, deciding, larger, approved), approved
            )


def test_the_gateway_admits_a_burn_of_owned_shares_at_no_less_than_its_minimums(market):
    execution = connector_from_wire({"connector": "curve"}).execution
    for action, kind, burned in (
        ("decrease-liquidity", "decrease", BURNED),
        ("close-liquidity", "close", HELD),
    ):
        stated, config, state, seen = adopted(market, action)
        intent = curve_direct.intent(config, (seen,), stated)
        deciding, step = planning(config, state, intent, seen)
        # A burn needs no approval: its one step is the pool's own removal.
        burn = step(seen)
        assert (burn.kind, burn.shares.amount, burn.minimums) == (kind, burned, intent.minimums)
        assert execution.call(config, burn).to == market.pool.pool

        # Minimums below the stated slippage are refused against the retained observation.
        wider = intent.model_copy(update={"minimums": amounts(intent.target, {})})
        deciding.decision = SimpleNamespace(intent=wider, observations=(seen,))
        with pytest.raises(ValueError, match="minima widen the configured slippage limit"):
            execution.validate_operation(
                None, deciding, execution.operation(config, deciding, wider, seen), seen
            )
        # A payout the pool's simulation answers below a minimum is refused.
        answer = [value.amount for value in burn.minimums.root]
        shape = (layout(market.pool).amounts,)
        validate_simulation(burn, "0x" + encode(shape, (answer,)).hex())
        answer[0] -= 1
        with pytest.raises(DataError, match="violates its ordered exit minima"):
            validate_simulation(burn, "0x" + encode(shape, (answer,)).hex())


def test_no_burn_takes_more_shares_than_the_deployment_owns(market):
    stated, config, state, seen = adopted(market, "decrease-liquidity")
    intent = curve_direct.intent(config, (seen,), stated)
    execution = connector_from_wire({"connector": "curve"}).execution
    for burned in (HELD, HELD + 1):
        whole = intent.model_copy(update={"liquidity": burned})
        deciding, _ = planning(config, state, whole, seen)
        with pytest.raises(ValueError, match="partial decrease must retain positive owned shares"):
            execution.operation(config, deciding, whole, seen)
    # Reconciliation refuses a burn the retained shares do not cover.
    target = config.target(ALLOCATION)
    effects = receipts.effects(target, [0] * len(target.assets), -(HELD + 1))
    with pytest.raises(DataError, match="burn exceeds retained LP shares"):
        reconciliation.owned_after(seen, None, effects, True)


def test_a_request_for_another_pool_or_target_is_refused(market):
    stated, config, state, seen = adopted(market, "close-liquidity")
    intent = curve_direct.intent(config, (seen,), stated)
    execution = connector_from_wire({"connector": "curve"}).execution
    other = next(found for found in MARKETS.values() if found is not market)
    foreign = adopted(other, "close-liquidity")
    elsewhere = curve_direct.intent(foreign[1], (foreign[3],), foreign[0])
    deciding, _ = planning(config, state, elsewhere, seen)
    with pytest.raises(ValueError, match="must retain its configured observation target"):
        execution.operation(config, deciding, elsewhere, seen)
    codec = connector_from_wire({"connector": "curve"}).codec
    with pytest.raises(ValueError, match="one exact retained target capability"):
        codec.capability.admit(state.capabilities, elsewhere)
    # The payout authority is the target's: a balanced deployment cannot pay one coin.
    single = intent.model_copy(update={"output_asset": market.pool.coins[0]})
    with pytest.raises(ValueError, match="single-coin removal lacks configured output authority"):
        codec.capability.admit(state.capabilities, single)


def test_a_liquidity_deployment_cannot_be_made_to_swap(market):
    """Its observations carry no quote, and the gateway admits a swap only along one."""
    stated, config, state, seen = adopted(market, "open-liquidity")
    owned = state.owned[0]

    def quote(amount, output):
        raise AssertionError("an adopting allocation asks the pool for no quote")

    assert quotes_for(config, owned, quote) == {}
    assert (seen.entry_quote, seen.exit_quotes) == (None, ())
    # A swap of inventory the deployment does hold is still refused.
    sold = next(value for value in owned.inventory.root if value.amount)
    bought = next(coin for coin in market.pool.coins if coin != sold.asset)
    swap = SwapExactInput[Target](
        target=config.target(ALLOCATION),
        amount=sold,
        minimum_output=AssetAmount(asset=bought, amount=1),
        deadline=market.block.timestamp + WINDOW,
    )
    connector = connector_from_wire({"connector": "curve"})
    deciding, _ = planning(config, state, swap, seen)
    approved = observation(market, config, owned, allowances={sold.asset: sold.amount})
    for before in (seen, approved):
        operation = connector.execution.operation(config, deciding, swap, before)
        with pytest.raises(ValueError, match="swap requires its exact-size retained pool quote"):
            connector.execution.validate_operation(None, deciding, operation, before)


def test_shares_that_moved_refuse_an_unsigned_step_and_are_an_issue_on_a_mined_one(market):
    stated, config, state, seen = adopted(market, "decrease-liquidity")
    intent = curve_direct.intent(config, (seen,), stated)
    deciding, step = planning(config, state, intent, seen)
    burn = step(seen)
    owned, target, gas = state.owned[0], config.target(ALLOCATION), 21000
    more = observation(market, config, owned, shares=HELD + 1)

    # Before a signature the step is refused, and the command is stated again.
    backend = SimpleNamespace(config=config)
    unsigned = SimpleNamespace(operation=burn, before=seen, decision=deciding.decision)
    for refused in (lambda: step(more), lambda: validate_current(backend, unsigned, more)):
        with pytest.raises(DataError, match="differ from retained fund ownership") as refusal:
            refused()
        assert (refusal.value.kind, f"holds {HELD + 1} base units" in str(refusal.value)) == (
            "ownership",
            True,
        )
    # Signed bytes cannot be recalled, so the balance does not hold them back.
    validate_current(backend, SimpleNamespace(**vars(unsigned), raw_transaction="0x01"), more)

    # The mined burn is reconciled from its own receipt, and ownership follows it.
    paid = list(liquidity_quotes.removed(market.observed, BURNED, None))
    effects = receipts.effects(target, paid, -BURNED)
    after = reconciliation.owned_after(seen, burn, effects, True)
    assert (after.position.shares.amount, [v.amount for v in after.inventory.root]) == (
        HELD - BURNED,
        paid,
    )
    wallet = {
        coin: seen.wallets.root[index].amount + paid[index]
        for index, coin in enumerate(target.assets)
    }
    pending = SimpleNamespace(before=seen, operation=burn)

    def mined(shares):
        return observation(market, config, after, shares=shares, held=wallet, spent=gas)

    # Adopted shares need no baseline: the burn reconciles without an issue.
    assert reconciliation.validate_balances(pending, mined(HELD - BURNED), effects, gas, True) == ()
    (issue,) = reconciliation.validate_balances(
        pending, mined(HELD - BURNED + 1), effects, gas, True
    )
    assert issue == (
        f"{market.pool.shares_asset}: wallet holds {HELD - BURNED + 1} base units of the "
        f"pool's LP token where this allocation's steps account for {HELD - BURNED}"
    )


def test_the_position_reports_the_shares_what_moved_the_inventory_and_the_allowances(market):
    stated, config, state, seen = adopted(market, "increase-liquidity")
    report = direct.binding("curve").position
    coins = [str(coin) for coin in market.pool.coins]
    lp = str(market.pool.shares_asset)
    wanted = {str(coin): amount for coin, amount in deposit(market).items()}
    before = report(state.model_copy(update={"initialized": True, "baseline": (seen,)}))
    assert before == {
        "shares": {"asset": lp, "amount": str(HELD)},
        "moved": [{"asset": asset, "amount": "0"} for asset in (*coins, lp)],
        "inventory": [{"asset": coin, "amount": str(wanted.get(coin, 0))} for coin in coins],
        "allowances": [],
    }
    # After a mined approval whose deposit never followed, the allowance is reported.
    first = market.pool.coins[0]
    approved = observation(
        market, config, state.owned[0], shares=HELD, allowances={first: wanted[str(first)]}
    )
    target = config.target(ALLOCATION)
    still = SimpleNamespace(
        success=True,
        after=approved,
        asset_changes=receipts.zero(target).asset_changes,
        share_change=receipts.zero(target).share_change,
    )
    stopped = SimpleNamespace(owned=state.owned, baseline=(seen,), outcomes=(still,))
    assert report(stopped)["allowances"] == [
        {"asset": str(first), "spender": market.pool.pool, "amount": str(wanted[str(first)])}
    ]

    # After the deposit: the shares it minted are added, and the inventory is used up.
    minted = 12345
    used = [-wanted.get(coin, 0) for coin in coins]
    effects = receipts.effects(target, used, minted)
    owned = reconciliation.owned_after(approved, None, effects, True)
    done = SimpleNamespace(
        success=True,
        after=observation(market, config, owned, shares=HELD + minted),
        asset_changes=effects.asset_changes,
        share_change=effects.share_change,
    )
    reverted = SimpleNamespace(success=False)
    finished = SimpleNamespace(owned=(owned,), baseline=(seen,), outcomes=(still, reverted, done))
    assert report(finished) == {
        "shares": {"asset": lp, "amount": str(HELD + minted)},
        "moved": [
            {"asset": asset, "amount": str(delta)}
            for asset, delta in zip((*coins, lp), (*used, minted), strict=True)
        ],
        "inventory": [{"asset": coin, "amount": "0"} for coin in coins],
        "allowances": [],
    }


POOL = "0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72"
USDC = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
CRVUSD = "eip155:1/erc20:0x4c9edd5852cd905f086c759e8383e09bff1e68b3"
STATED = {
    "open-liquidity": ("--amount", f"{USDC}=1000", "--amount", f"{CRVUSD}=7"),
    "increase-liquidity": ("--amount", f"{USDC}=1000"),
    "decrease-liquidity": ("--liquidity", "5", "--receive", "balanced"),
    "close-liquidity": ("--receive", USDC),
    "revoke-exchange": (),
}


def arguments(command, *extra, connector="curve", stated=None, without=()):
    named = {
        "--connector": connector,
        "--chain": "eip155:1",
        "--pool": POOL,
        "--wallet": "wallet",
        "--env-file": ".env",
        "--max-gas-price-wei": "7",
        "--max-transaction-fee": "9",
    }
    if command in PRICED:
        named["--slippage-bps"] = "50"
    flags = [part for name, value in named.items() if name not in without for part in (name, value)]
    position = STATED[command] if stated is None else stated
    return parse_validated(build_parser(), ["ax", command, *flags, *position, *extra])


@pytest.mark.parametrize("command", tuple(COMMANDS))
def test_the_command_line_states_each_command_for_curve(command, capsys):
    args = arguments(command)
    assert (args.product, args.ax_command, args.execute) == ("operator-action", command, False)
    stated = ax_actions.action_request(args)
    assert (stated.action, stated.kind, stated.amount) == (command, COMMANDS[command], None)
    market = ax_actions.market_request(args)
    assert (market.connector, str(market.chain), market.pool) == ("curve", "eip155:1", POOL)
    priced = command in PRICED
    assert (stated.slippage_bps, stated.deadline_seconds) == ((50, 300) if priced else (None, None))
    # No Curve command takes a range, an NFT, or an amount by currency index.
    refused = [("--tick-lower", "0"), ("--token-id", "5"), ("--amount0", "5"), ("--sell", USDC)]
    refused += [("--amount", "5"), ("--amount", f"{USDC}=x"), ("--amount", "=5")]
    for extra in refused:
        with pytest.raises(SystemExit):
            arguments(command, *extra)
    if priced:
        with pytest.raises(SystemExit):
            arguments(command, without=("--slippage-bps",))
    capsys.readouterr()


def test_the_command_line_names_each_deposited_coin_and_the_payout(capsys):
    opened = ax_actions.action_request(arguments("open-liquidity"))
    assert opened.amounts == ((USDC, 1000), (CRVUSD, 7)) and opened.receive is None
    decreased = ax_actions.action_request(arguments("decrease-liquidity"))
    assert (decreased.liquidity, decreased.receive, decreased.amounts) == (5, "balanced", None)
    assert ax_actions.action_request(arguments("close-liquidity")).receive == USDC
    # What each command requires for Curve is refused by the parser, before any read.
    missing = {
        "open-liquidity": "curve open-liquidity requires --amount",
        "increase-liquidity": "curve increase-liquidity requires --amount",
        "close-liquidity": "curve close-liquidity requires --receive",
    }
    for command, message in missing.items():
        with pytest.raises(SystemExit):
            arguments(command, stated=())
        assert message in capsys.readouterr().err
    with pytest.raises(SystemExit):
        arguments("decrease-liquidity", stated=("--liquidity", "5"))
    assert "curve decrease-liquidity requires --receive" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        arguments("open-liquidity", "--tick-lower", "0", "--tick-upper", "60")
    assert "curve open-liquidity takes no --tick-lower, --tick-upper" in capsys.readouterr().err
    for refused in (("--receive", "half"), ("--liquidity", "0")):
        with pytest.raises(SystemExit):
            arguments("decrease-liquidity", *refused)
    capsys.readouterr()


def test_a_concentrated_liquidity_connector_keeps_its_own_flags(capsys):
    base = "0xd0b53d9277642d899df5c87a3966a349a798f224"
    common = ["--connector", "uniswap_v3", "--chain", "eip155:8453", "--pool", base]
    common += ["--wallet", "w", "--env-file", ".env", "--max-gas-price-wei", "7"]
    common += ["--max-transaction-fee", "9", "--slippage-bps", "50"]

    def parse(command, *position):
        return parse_validated(build_parser(), ["ax", command, *common, *position])

    ticks = ("--tick-lower", "-60", "--tick-upper", "0", "--amount0", "5")
    assert parse("open-liquidity", *ticks).tick_upper == 0
    refused = {
        ("open-liquidity", "--amount0", "5"): "requires --tick-lower, --tick-upper",
        ("open-liquidity", *ticks, "--amount", f"{USDC}=5"): "takes no --amount",
        ("close-liquidity",): "uniswap_v3 close-liquidity requires --token-id",
        ("close-liquidity", "--token-id", "7", "--receive", "balanced"): "takes no --receive",
    }
    for (command, *position), message in refused.items():
        with pytest.raises(SystemExit):
            parse(command, *position)
        assert message in capsys.readouterr().err


def test_the_help_never_calls_the_seconds_an_on_chain_bound_for_curve(capsys):
    for command in PRICED:
        with pytest.raises(SystemExit):
            parse_validated(build_parser(), ["ax", command, "--help"])
        text = " ".join(capsys.readouterr().out.split())
        assert "a curve call carries no deadline" in text
        assert "stays valid until it is mined or its nonce is used" in text


def test_the_command_refuses_before_any_read_and_passes_the_statement_on(monkeypatch):
    seen = {}

    def deployment(name, market, action, wallet, provider, endpoint, limits, **options):
        seen.update(connector=name, action=action)
        raise StopIteration

    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
    monkeypatch.setattr(ax_actions, "deployment", deployment)
    with pytest.raises(StopIteration):
        ax_actions.command(arguments("open-liquidity"))
    assert (seen["connector"], seen["action"].amounts) == ("curve", ((USDC, 1000), (CRVUSD, 7)))
    monkeypatch.setattr(ax_actions, "read_wallet", pytest.fail)
    with pytest.raises(ValueError, match="curve serves no direct close-liquidity on eip155:31337"):
        ax_actions.command(arguments("close-liquidity", "--chain", "eip155:31337"))


def test_the_position_read_reports_the_accounts_shares_and_what_a_balanced_burn_pays(
    market, monkeypatch
):
    observed = market.observed
    resolved = SimpleNamespace(
        pool=market.pool,
        share=observed.shares,
        coins=observed.currencies,
        summary=lambda: {"pool": market.pool.pool},
    )
    reader = Shares(HELD)
    reader.chain = market.pool.chain
    monkeypatch.setattr(curve_resolution, "resolve_market", lambda *arguments: resolved)
    monkeypatch.setattr(curve_reader, "read_pool", lambda *arguments: observed)
    stated = position_request.PositionRequest(
        chain=market.pool.chain.chain_id, pool=market.pool.pool, account=WALLET.address
    )
    report = position_request.read_liquidity_position(reader, stated, market.block)
    paid = liquidity_quotes.removed(observed, HELD, None)
    assert (report["product"], report["owner"]) == ("liquidity_position", WALLET.address)
    assert report["request"] == {**stated.model_dump(mode="json"), "connector": "curve"}
    assert report["position"]["shares"] == {
        "asset": str(market.pool.shares_asset),
        "amount": str(HELD),
        "decimals": 18,
    }
    assert int(report["position"]["total_supply"]["amount"]) == observed.total_supply
    assert [(item["asset"], int(item["amount"])) for item in report["principal"]] == [
        (str(coin), amount) for coin, amount in zip(market.pool.coins, paid, strict=True)
    ]
    # An account without shares is reported with none, and a foreign chain is refused.
    reader.held = 0
    empty = position_request.read_liquidity_position(reader, stated, market.block)
    assert [item["amount"] for item in empty["principal"]] == ["0"] * len(market.pool.coins)
    elsewhere = stated.model_copy(update={"chain": ChainId("eip155:10")})
    with pytest.raises(ValueError, match="request chain differs from the reader chain"):
        position_request.read_liquidity_position(reader, elsewhere, market.block)
    for wrong in ({"chain": "solana:5eykt4"}, {"pool": "0x" + "00" * 20}):
        with pytest.raises(ValueError):
            position_request.PositionRequest.model_validate({**stated.model_dump(), **wrong})


def position_arguments(*extra, connector="curve", without=()):
    named = {
        "--connector": connector,
        "--chain": "eip155:1",
        "--pool": POOL,
        "--account": WALLET.address,
        "--env-file": ".env",
    }
    flags = [part for name, value in named.items() if name not in without for part in (name, value)]
    return parse_validated(build_parser(), ["ax", "liquidity-position", *flags, *extra])


def test_the_position_read_names_an_account_for_curve_and_an_nft_elsewhere(capsys, monkeypatch):
    for name in ("ALMANAK_AX_GATEWAY_URL", "ALMANAK_AX_GATEWAY_AUTH_FILE"):
        monkeypatch.delenv(name, raising=False)
    args = position_arguments()
    assert (args.product, args.block) == ("ax-liquidity-position", "latest")
    assert ax.parse_request(args) == position_request.PositionRequest(
        chain=ChainId("eip155:1"), pool=POOL, account=WALLET.address
    )
    gateway = ("--gateway-url", "http://127.0.0.1:1", "--gateway-auth-file", "token")
    assert position_arguments(*gateway, without=("--env-file",)).gateway_url
    for missing in ("--pool", "--account", "--env-file"):
        with pytest.raises(SystemExit):
            position_arguments(without=(missing,))
    with pytest.raises(SystemExit):
        position_arguments("--token-id", "5")
    assert "curve takes no --token-id for this request" in capsys.readouterr().err
    # A position that is an NFT is named by its token ID, never by an account.
    with pytest.raises(SystemExit):
        position_arguments("--token-id", "5", connector="uniswap_v3")
    assert "uniswap_v3 takes no --account for this request" in capsys.readouterr().err
    nft = position_arguments("--token-id", "5", connector="uniswap_v3", without=("--account",))
    assert ax.parse_request(nft).token_id == 5


def test_the_read_command_pins_its_block_and_reads_the_curve_position(monkeypatch):
    block = next(iter(MARKETS.values())).block
    seen = {}

    class Reader:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def pin_block(self, selector):
            seen["selector"] = selector
            return block

    def read(reader, stated, at):
        seen.update(request=stated, block=at)
        return {"product": "liquidity_position"}

    monkeypatch.setattr(ax, "observation_reader", lambda args, chain: Reader())
    monkeypatch.setattr(position_request, "read_liquidity_position", read)
    assert ax.liquidity_position(position_arguments("--block", "latest")) == {
        "product": "liquidity_position",
        "read_at": {"block": "latest"},
    }
    assert (seen["selector"], seen["block"], seen["request"].account) == (
        "latest",
        block,
        WALLET.address,
    )
    with pytest.raises(ValueError, match="curve declares no support on eip155:31337"):
        ax.liquidity_position(position_arguments("--chain", "eip155:31337"))
