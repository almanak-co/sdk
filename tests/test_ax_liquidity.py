"""The direct liquidity commands of `almanak ax` for the V3 pool family.

Component evidence without signing or chain access: each connector's pool is resolved
from its retained resolution reads, the position manager's answers are stated by the
test, and observations are built over that pool. The fork rehearsal of the whole
commands is tests/fork/test_ax_liquidity.py.
"""

import json
from dataclasses import dataclass, replace
from importlib import import_module
from types import SimpleNamespace
from typing import get_args

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak import ax, ax_actions
from almanak.caip import AssetId, ChainId
from almanak.chains import evm_network
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct, v3_pool_direct, v3_pool_request
from almanak.connectors import v3_pool_liquidity_direct as liquidity
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.concentrated_math import amounts, sqrt_at_tick
from almanak.connectors.direct import ALLOCATION, ActionRequest
from almanak.connectors.v3_pool_position_request import PositionRequest, read_liquidity_position
from almanak.connectors.v3_pool_positions import Position
from almanak.connectors.v3_pool_reader import position_key
from almanak.dex_intents import TickBounds
from almanak.gateway.ethereum import DataError, Reverted
from almanak.gateway.observation_gateway import ReadMethod
from almanak.gateway.resolution_evm import RetainedResolutionReader
from almanak.models import ZERO_ADDRESS, AssetAmount
from almanak.strategy import StrategyDefinition
from tests.test_ax_swap import (
    CONNECTORS,
    OBSERVED,
    POOLS,
    WALLET,
    configuration,
    created,
    resolved,
    retained,
)

pytestmark = pytest.mark.parametrize("connector", CONNECTORS)

COMMANDS = {
    "open-liquidity": "open_liquidity",
    "increase-liquidity": "increase_liquidity",
    "decrease-liquidity": "decrease_liquidity",
    "collect-liquidity-fees": "collect_liquidity_fees",
    "close-liquidity": "close_liquidity",
    "revoke-exchange": "revoke_exchange",
}
PRICED = ("open-liquidity", "increase-liquidity", "decrease-liquidity", "close-liquidity")
TOKEN, OTHER_TOKEN = 77, 78
AMOUNT0, AMOUNT1 = 4000, 3000
LIQUIDITY = 10**12
STRANGER = "0x" + "9a" * 20
Q128 = 2**128


def selector(signature):
    return keccak(text=signature)[:4]


OWNER_OF, POSITIONS = selector("ownerOf(uint256)"), selector("positions(uint256)")
GROWTH = (selector("feeGrowthGlobal0X128()"), selector("feeGrowthGlobal1X128()"))


@dataclass(frozen=True)
class Nft:
    """One NFT as the position manager records it."""

    owner: str
    token0: str
    token1: str
    key: int
    lower: int
    upper: int
    liquidity: int = LIQUIDITY
    growth: tuple[int, int] = (Q128, Q128)
    owed: tuple[int, int] = (11, 13)


class Manager(RetainedResolutionReader):
    """The retained resolution reads, plus what the test states the manager and pool answer."""

    def __init__(self, market, rows, nfts, growth=(5 * Q128, 7 * Q128)):
        # Every JSON-RPC method the reader was asked for.
        self.methods: set[str] = set()
        super().__init__(market.block.chain, rows)
        self.market, self.nfts, self.growth = market, nfts, growth
        self.pin_block(market.block.number)

    def _rpc(self, method, params, *, allow_none=False):
        self.methods.add(method)
        if method == "eth_call":
            answer = self.answer(params[0]["to"], bytes.fromhex(params[0]["data"][2:]))
            if answer is not None:
                return "0x" + answer.hex()
        return super()._rpc(method, params, allow_none=allow_none)

    def answer(self, to, data):
        if to == self.market.pool.pool and data[:4] in GROWTH:
            return encode(("uint256",), (self.growth[GROWTH.index(data[:4])],))
        if to != self.market.contracts.position_manager or data[:4] not in (OWNER_OF, POSITIONS):
            return None
        (token,) = decode(("uint256",), data[4:])
        if token not in self.nfts:
            raise Reverted("execution reverted", None)
        nft = self.nfts[token]
        if data[:4] == OWNER_OF:
            return encode(("address",), (nft.owner,))
        key_type, _ = position_key(self.market.pool)
        types = ("uint96", "address", "address", "address", key_type, "int24", "int24")
        types += ("uint128", "uint256", "uint256", "uint128", "uint128")
        record = (0, ZERO_ADDRESS, nft.token0, nft.token1, nft.key, nft.lower, nft.upper)
        return encode(types, (*record, nft.liquidity, *nft.growth, *nft.owed))


def ticks(market):
    """A range around tick zero, and one beside it, aligned to the pool's spacing."""
    spacing = market.pool.tick_spacing
    return (-10 * spacing, 10 * spacing), (10 * spacing, 20 * spacing)


def held(market, **changes):
    """The wallet's NFT in the resolved pool, around tick zero."""
    pool = market.pool
    lower, upper = ticks(market)[0]
    nft = Nft(
        owner=WALLET.address,
        token0=pool.currency0.reference,
        token1=pool.currency1.reference,
        key=position_key(pool)[1],
        lower=lower,
        upper=upper,
    )
    return replace(nft, **changes)


def manager(connector, market, nfts):
    return Manager(market, retained(connector)[1], nfts)


def nft_id(market, token=TOKEN):
    chain, contracts = market.pool.chain.chain_id, market.contracts
    return AssetId(f"{chain}/erc721:{contracts.position_manager}/{token}")


def request(action, **stated):
    bounds = {"slippage_bps": 50, "deadline_seconds": 300} if action in PRICED else {}
    return ActionRequest(action, **{**bounds, **stated})


def opening(market, **stated):
    lower, upper = ticks(market)[0]
    values = {"tick_lower": lower, "tick_upper": upper, "amount0": AMOUNT0, "amount1": AMOUNT1}
    return request("open-liquidity", **{**values, **stated})


def naming(action, **stated):
    """A command on the wallet's NFT, with what the command must state besides it."""
    values: dict = {"token_id": TOKEN}
    if action == "increase-liquidity":
        values.update(amount0=AMOUNT0, amount1=AMOUNT1)
    if action == "decrease-liquidity":
        values.update(liquidity=LIQUIDITY // 4)
    return request(action, **{**values, **stated})


def deployed(connector, market, stated, nfts=None):
    """The operator configuration the command builds for one request."""
    reader = None if nfts is None else manager(connector, market, nfts)
    positions = v3_pool_direct.positions(reader, market.block, market, WALLET, stated)
    return configuration(connector, positions, market)


def position(market, nft=None, token=TOKEN):
    """The position the connector's reader returns for an NFT of the configured range."""
    nft = nft or held(market)
    return Position(
        nft=nft_id(market, token),
        liquidity=nft.liquidity,
        owed0=nft.owed[0],
        owed1=nft.owed[1],
        fee_growth=nft.growth,
    )


def pool_state(connector, market, tick=0):
    snapshot = connector_from_wire({"connector": connector}).codec.snapshot
    return snapshot.model_fields["pool"].annotation(
        source=market.pool,
        block=market.block,
        currency0=market.asset0,
        currency1=market.asset1,
        sqrt_price_x96=sqrt_at_tick(tick),
        tick=tick,
        liquidity=10**18,
        unlocked=True,
        **OBSERVED[connector],
    )


def observation(connector, config, owned, market, *, wallets=(9000, 9000), allowed=None, **seen):
    """An on-chain observation of the deployment's one position at the resolved block."""
    snapshot = connector_from_wire({"connector": connector}).codec.snapshot
    target = config.target(ALLOCATION)
    spenders = (target.contracts.router, target.contracts.position_manager)
    allowed = allowed or {}
    return snapshot(
        target=target,
        pool=pool_state(connector, market, seen.pop("tick", 0)),
        owned=owned,
        wallet0=AssetAmount(asset=target.assets[0], amount=wallets[0]),
        wallet1=AssetAmount(asset=target.assets[1], amount=wallets[1]),
        allowances=tuple(
            {"asset": asset, "spender": spender, "amount": allowed.get((index, spender), 0)}
            for index, asset in enumerate(target.assets)
            for spender in spenders
        ),
        native_balance=AssetAmount(
            asset=evm_network(market.pool.chain.chain_id).native_asset, amount=10**18
        ),
        payer=target.fund,
        position=seen.pop("position", None),
        **seen,
    )


def started(connector, market, stated, nfts=None, **seen):
    """A deployment after its start: adopted from one baseline observation."""
    config = deployed(connector, market, stated, nfts)
    state = created(connector, config)
    found = None if nfts is None else position(market, nfts[stated.token_id], stated.token_id)
    baseline = observation(connector, config, state.owned[0], market, position=found, **seen)
    lifecycle = connector_from_wire({"connector": connector}).lifecycle
    owned = lifecycle.adopt(state.owned, (baseline,))
    state = state.model_copy(update={"owned": owned, "initialized": True, "baseline": (baseline,)})
    return config, state, observation(connector, config, owned[0], market, position=found, **seen)


def on_position(connector, market, action, nft=None, **seen):
    """A started deployment of one command on the wallet's NFT."""
    return started(connector, market, naming(action), {TOKEN: nft or held(market)}, **seen)


def checked(connector, config, state, intent, seen):
    """Plan one step and check it as the gateway checks it before anything is signed."""
    execution = connector_from_wire({"connector": connector}).execution
    deciding = SimpleNamespace(
        config=config,
        owned=state.owned,
        capabilities=state.capabilities,
        decision=SimpleNamespace(intent=intent, observations=(seen,)),
        outcomes=(),
        mode="running",
    )

    def step(before, operation=None):
        planned = execution.operation(config, deciding, intent, before)
        execution.validate_operation(None, deciding, operation or planned, before)
        return planned

    return step


def test_the_connector_serves_the_liquidity_commands_on_every_production_cell(connector):
    found = connector_from_wire({"connector": connector})
    assert set(COMMANDS) <= set(direct.binding(connector).actions)
    assert set(liquidity.ACTIONS) == set(COMMANDS) == set(liquidity.BUILDERS)
    cells = [cell for cell in found.chain_support if cell.direct_actions]
    assert cells
    for command, kind in COMMANDS.items():
        assert direct.COMMAND_INTENTS[command] == (kind,)
        assert command in ax_actions.POOL_ACTIONS and ax_actions.ACTIONS[command]
        for cell in cells:
            cell.require_direct(kind)
            direct.require_direct(connector, cell.chain, command)
        with pytest.raises(ValueError, match=f"{connector} serves no direct {command} on"):
            direct.require_direct(connector, ChainId("eip155:31337"), command)
    # An exit would swap what a close returns, so it is no direct action anywhere.
    for cell in cells:
        with pytest.raises(ValueError, match="unsupported as a direct action"):
            cell.require_direct("exit_exchange")


def test_an_open_states_its_range_and_both_amounts_and_swaps_nothing(connector):
    market = resolved(connector)
    lower, upper = ticks(market)[0]
    config = deployed(connector, market, opening(market))
    (allocation,) = config.allocations
    assert (allocation.name, allocation.account) == (ALLOCATION, WALLET.account)
    assert (allocation.pool, allocation.contracts) == (market.pool, market.contracts)
    assert (allocation.range.lower, allocation.range.upper, allocation.band) == (lower, upper, None)
    # The capital is the stated currency0 and the adoption's counter the stated currency1.
    assert allocation.capital == AssetAmount(asset=market.pool.currency0, amount=AMOUNT0)
    assert (allocation.adopt.counter, allocation.adopt.nft) == (AMOUNT1, None)
    assert (allocation.entry_swap_bps, allocation.slippage_bps) == (0, 50)
    assert (allocation.deadline_seconds, allocation.minimum_native_balance) == (300, 0)
    assert direct.binding_for(config) is direct.binding(connector)

    state = created(connector, config)
    target = config.target(ALLOCATION)
    assert [item.target for item in state.capabilities.requirements] == [target]
    assert (target.range, target.fund.address) == (allocation.range, WALLET.address)
    owned = state.owned[0]
    assert (owned.inventory0.amount, owned.inventory1.amount) == (AMOUNT0, AMOUNT1)
    assert (owned.position, owned.adopts, owned.entry_complete) == (None, None, False)

    # Either amount may be zero, not both.
    one = deployed(connector, market, opening(market, amount0=0)).allocations[0]
    assert (one.capital.amount, one.adopt.counter) == (0, AMOUNT1)


def test_an_open_refuses_what_its_command_line_cannot_state(connector):
    market = resolved(connector)
    spacing = market.pool.tick_spacing

    def refused(match, **stated):
        with pytest.raises(ValueError, match=match):
            deployed(connector, market, opening(market, **stated))

    refused("--amount0 or --amount1 above zero", amount0=0, amount1=0)
    refused("--amount0 or --amount1 above zero", amount0=None, amount1=None)
    refused(f"multiples of the pool's tick spacing, {spacing}", tick_lower=1 - spacing)
    refused(f"multiples of the pool's tick spacing, {spacing}", tick_upper=spacing + 1)
    refused("increasing tick bounds", tick_lower=spacing, tick_upper=spacing)
    refused(
        "open-liquidity requires --tick-lower, --slippage-bps", tick_lower=None, slippage_bps=None
    )
    refused("open-liquidity requires --deadline-seconds", deadline_seconds=None)


@pytest.mark.parametrize("action", liquidity.NFT_ACTIONS)
def test_a_command_on_a_position_takes_the_range_and_identity_of_the_named_nft(connector, action):
    market = resolved(connector)
    lower, upper = ticks(market)[1]
    nft = held(market, lower=lower, upper=upper)
    config = deployed(connector, market, naming(action), {TOKEN: nft})
    (allocation,) = config.allocations
    assert (allocation.range.lower, allocation.range.upper) == (lower, upper)
    assert allocation.adopt.nft == nft_id(market) and allocation.entry_swap_bps == 0
    adds = action == "increase-liquidity"
    assert allocation.capital.amount == (AMOUNT0 if adds else 0)
    assert allocation.adopt.counter == (AMOUNT1 if adds else 0)
    collects = action == "collect-liquidity-fees"
    assert allocation.slippage_bps == (0 if collects else 50)
    assert allocation.deadline_seconds == 300 == liquidity.ADMISSION_SECONDS

    # The NFT is not owned yet: the deployment adopts it at its first observation.
    owned = created(connector, config).owned[0]
    assert (owned.position, owned.adopts) == (None, nft_id(market))
    assert config.target(ALLOCATION).range == allocation.range


@pytest.mark.parametrize("action", liquidity.NFT_ACTIONS)
def test_the_token_id_is_refused_by_name_when_it_is_not_the_wallets_position_here(
    connector, action
):
    market = resolved(connector)
    pool = market.pool

    def refused(match, nfts, **stated):
        with pytest.raises(ValueError, match=match):
            deployed(connector, market, naming(action, **stated), nfts)

    refused(f"{action} requires --token-id", {}, token_id=None)
    refused(f"position manager {market.contracts.position_manager} holds no NFT {TOKEN}", {})
    refused(
        f"NFT {TOKEN} belongs to {STRANGER}, not to the wallet {WALLET.address}",
        {TOKEN: held(market, owner=STRANGER)},
    )
    elsewhere = (
        held(market, key=position_key(pool)[1] + 1),
        held(market, token0="0x" + "0b" * 20),
        held(market, token1="0x" + "0c" * 20),
    )
    for nft in elsewhere:
        refused(f"NFT {TOKEN} is a position of another pool, not of {pool.pool}", {TOKEN: nft})
    # Another account's position of another pool names the pool first.
    refused("is a position of another pool", {TOKEN: replace(elsewhere[0], owner=STRANGER)})
    if action != "increase-liquidity":
        refused(f"NFT {TOKEN} holds no liquidity; {action}", {TOKEN: held(market, liquidity=0)})
    if action in PRICED:
        refused(f"{action} requires --slippage-bps", {TOKEN: held(market)}, slippage_bps=None)
    if action == "decrease-liquidity":
        refused("requires --liquidity", {TOKEN: held(market)}, liquidity=None)
    if action == "increase-liquidity":
        refused("above zero", {TOKEN: held(market)}, amount0=0, amount1=0)
        emptied = deployed(connector, market, naming(action), {TOKEN: held(market, liquidity=0)})
        assert emptied.allocations[0].adopt.nft == nft_id(market)


def test_a_revocation_states_no_range_no_inventory_and_no_nft(connector):
    market = resolved(connector)
    (allocation,) = deployed(connector, market, request("revoke-exchange")).allocations
    assert (allocation.range, allocation.band, allocation.entry_swap_bps) == (None, None, 0)
    assert (allocation.capital.amount, allocation.adopt.counter, allocation.adopt.nft) == (
        0,
        0,
        None,
    )
    assert (allocation.slippage_bps, allocation.deadline_seconds) == (0, 300)


def test_an_adopting_allocation_is_an_operators_and_states_no_swap(connector):
    market = resolved(connector)
    config = deployed(connector, market, opening(market))
    document = config.model_dump(mode="json")
    (allocation,) = document["allocations"]
    model = type(config)

    def refused(match, **changes):
        changed = {**document, "allocations": [{**allocation, **changes}]}
        with pytest.raises(ValueError, match=match):
            model.model_validate_json(json.dumps(changed))

    refused("an adopting allocation states no entry swap and no band", entry_swap_bps=1)
    spacing = market.pool.tick_spacing
    refused("no entry swap and no band", range=None, band={"width": 2 * spacing})
    nft = str(nft_id(market))
    refused(
        "requires its position manager and a fixed range",
        range=None,
        adopt={"counter": 0, "nft": nft},
    )
    foreign = nft.replace(market.contracts.position_manager, STRANGER)
    refused("requires its position manager and a fixed range", adopt={"counter": 0, "nft": foreign})
    # Without an adoption the capital is funded and must be positive.
    plain = {key: value for key, value in allocation.items() if key != "adopt"}
    zero = {**plain, "capital": {**plain["capital"], "amount": 0}, "entry_swap_bps": 5000}
    with pytest.raises(ValueError, match="positive supported int128 quantity"):
        model.model_validate_json(json.dumps({**document, "allocations": [zero]}))
    # A strategy allocation never carries the field on the wire.
    funded = model.model_validate_json(
        json.dumps({**document, "allocations": [{**plain, "entry_swap_bps": 5000}]})
    )
    assert "adopt" not in funded.model_dump(mode="json")["allocations"][0]

    # A strategy deployment cannot be created over an adopting allocation.
    strategy = StrategyDefinition(filename="strategy.py", source="def decide(context):\n    pass\n")
    with pytest.raises(ValueError, match="only an operator deployment adopts"):
        created(connector, config, strategy)


def test_an_ownership_names_an_nft_to_adopt_only_before_it_owns_one(connector):
    market = resolved(connector)
    config = deployed(connector, market, naming("close-liquidity"), {TOKEN: held(market)})
    owned = created(connector, config).owned[0]
    assert owned.adopts == nft_id(market) and "adopts" in owned.model_dump()
    values = {
        "target": owned.target,
        "inventory0": owned.inventory0,
        "inventory1": owned.inventory1,
    }
    stranger = AssetId(str(nft_id(market)).replace(market.contracts.position_manager, STRANGER))
    trading = created(connector, deployed(connector, market, request("revoke-exchange"))).owned[0]
    for changed in (
        {"position": position(market), "adopts": nft_id(market)},
        {"adopts": stranger},
        {"target": trading.target, "adopts": nft_id(market)},
    ):
        with pytest.raises(ValueError, match="requires its manager, a fixed range and no position"):
            type(owned)(**{**values, **changed})
    # Once adopted, or for a strategy, the field is absent on the wire.
    assert "adopts" not in type(owned)(**values, position=position(market)).model_dump()


def test_an_open_adopts_exactly_the_stated_amounts_when_the_wallet_covers_both(connector):
    market = resolved(connector)
    config = deployed(connector, market, opening(market))
    lifecycle = connector_from_wire({"connector": connector}).lifecycle
    owned = created(connector, config).owned
    covered = observation(connector, config, owned[0], market, wallets=(AMOUNT0, AMOUNT1))
    assert lifecycle.adopt(owned, (covered,)) == owned
    for wallets, asset in (
        ((AMOUNT0 - 1, AMOUNT1), market.pool.currency0),
        ((AMOUNT0, AMOUNT1 - 1), market.pool.currency1),
    ):
        short = observation(connector, config, owned[0], market, wallets=wallets)
        with pytest.raises(DataError, match="less than the") as refusal:
            lifecycle.adopt(owned, (short,))
        assert refusal.value.kind == "inventory" and str(asset) in str(refusal.value)


def test_the_named_nft_is_adopted_as_the_observation_read_it(connector):
    market = resolved(connector)
    nft = held(market)
    config = deployed(connector, market, naming("increase-liquidity"), {TOKEN: nft})
    lifecycle = connector_from_wire({"connector": connector}).lifecycle
    owned = created(connector, config).owned
    seen = observation(connector, config, owned[0], market, position=position(market))
    (adopted,) = lifecycle.adopt(owned, (seen,))
    assert adopted.position == position(market) and adopted.adopts is None
    assert (adopted.position.liquidity, adopted.position.fee_growth) == (LIQUIDITY, nft.growth)
    assert (adopted.position.owed0, adopted.position.owed1) == nft.owed
    assert (adopted.inventory0.amount, adopted.inventory1.amount) == (AMOUNT0, AMOUNT1)
    assert adopted.entry_complete and adopted.target == owned[0].target
    # Later observations must find that exact position: the gateway's ownership check.
    lifecycle.validate_owned(
        SimpleNamespace(owned=(adopted,)),
        (observation(connector, config, adopted, market, position=position(market)),),
    )
    changed = position(market, replace(nft, liquidity=LIQUIDITY - 1))
    with pytest.raises(DataError, match="differs from retained ownership"):
        lifecycle.validate_owned(
            SimpleNamespace(owned=(adopted,)),
            (observation(connector, config, adopted, market, position=changed),),
        )

    # An observation without the NFT, or with another one, adopts nothing.
    for found in (None, position(market, token=OTHER_TOKEN)):
        missing = observation(connector, config, owned[0], market, position=found)
        with pytest.raises(DataError, match="does not hold the NFT to adopt") as refusal:
            lifecycle.adopt(owned, (missing,))
        assert refusal.value.kind == "ownership"
    short = observation(
        connector, config, owned[0], market, wallets=(0, 0), position=position(market)
    )
    with pytest.raises(DataError, match="less than the"):
        lifecycle.adopt(owned, (short,))


def test_the_observation_reads_and_verifies_the_nft_to_adopt(connector):
    market = resolved(connector)
    observations = import_module(f"almanak.connectors.{connector}.observations")
    read = observations._observations.observed_position  # noqa: SLF001

    def observed(nft, token=TOKEN):
        config = deployed(connector, market, naming("close-liquidity"), {TOKEN: held(market)})
        owned = created(connector, config).owned[0]
        reader = manager(connector, market, {token: nft})
        return read(reader, config.target(ALLOCATION), owned, market.block)

    assert observed(held(market)) == position(market)
    lower, upper = ticks(market)[1]
    for nft, match in (
        (held(market, owner=STRANGER), "not owned by the named fund"),
        (held(market, key=position_key(market.pool)[1] + 1), "differs from the configured V3 pool"),
        (held(market, token0="0x" + "0b" * 20), "differs from the configured V3 pool"),
        (held(market, lower=lower, upper=upper), "differs from the configured V3 range"),
    ):
        with pytest.raises(DataError, match=match) as refusal:
            observed(nft)
        assert refusal.value.kind == "ownership"
    with pytest.raises(Reverted):
        observed(held(market), token=OTHER_TOKEN)

    # Once adopted, only the retained NFT is read; after a close nothing is.
    config, state, seen = on_position(connector, market, "close-liquidity")
    target = config.target(ALLOCATION)
    reader = manager(connector, market, {TOKEN: held(market)})
    assert read(reader, target, state.owned[0], market.block) == position(market)
    closed = type(state.owned[0])(
        target=target, inventory0=state.owned[0].inventory0, inventory1=state.owned[0].inventory1
    )
    assert read(reader, target, closed, market.block) is None


def test_an_adopting_deployment_observes_no_quote(connector):
    market = resolved(connector)
    observations = import_module(f"almanak.connectors.{connector}.observations")

    def unreachable(amount):
        raise AssertionError("an adopting allocation measured a quote")

    # Before a mint the counter inventory is positive and no position is owned: a
    # strategy would be offered an exit quote here.
    config = deployed(connector, market, opening(market))
    owned = created(connector, config).owned[0]
    assert owned.inventory1.amount == AMOUNT1 and owned.position is None
    assert observations.quotes_for(config, owned, unreachable) == {}
    config, state, _ = on_position(connector, market, "close-liquidity")
    assert observations.quotes_for(config, state.owned[0], unreachable) == {}


def test_an_open_offers_the_stated_amounts_and_the_gateway_admits_only_that_mint(connector):
    market = resolved(connector)
    config, state, seen = started(connector, market, opening(market))
    found = connector_from_wire({"connector": connector})
    intent = v3_pool_direct.intent(config, (seen,), opening(market))
    assert intent.kind == "open_liquidity" and intent.target == config.target(ALLOCATION)
    assert (intent.maximum0.amount, intent.maximum1.amount) == (AMOUNT0, AMOUNT1)
    assert (intent.range, intent.deadline) == (None, market.block.timestamp + 300)
    found.codec.capability.admit(state.capabilities, intent)
    assert found.lifecycle.increases_exposure(intent)

    step = checked(connector, config, state, intent, seen)
    target = config.target(ALLOCATION)
    spender = target.contracts.position_manager
    first = step(seen)
    assert (first.kind, first.asset, first.spender, first.amount) == (
        "approve",
        target.assets[0],
        spender,
        AMOUNT0,
    )
    owned = state.owned[0]
    half = observation(connector, config, owned, market, allowed={(0, spender): AMOUNT0})
    second = step(half)
    assert (second.kind, second.asset, second.amount) == ("approve", target.assets[1], AMOUNT1)
    # An allowance of another size is reset before the exact one is granted.
    stale = observation(connector, config, owned, market, allowed={(0, spender): AMOUNT0 + 5})
    assert (step(stale).kind, step(stale).amount) == ("approve", 0)
    both = {(0, spender): AMOUNT0, (1, spender): AMOUNT1}
    approved = observation(connector, config, owned, market, allowed=both)
    mint = step(approved)
    assert (mint.kind, mint.range) == ("mint", None)
    assert (mint.maximum0.amount, mint.maximum1.amount) == (AMOUNT0, AMOUNT1)
    assert mint.minimum0.amount <= AMOUNT0 and mint.minimum1.amount <= AMOUNT1
    assert mint.minimum_liquidity > 1

    execution = found.execution
    lower, upper = ticks(market)[1]

    def refuse(changed, error, match):
        deciding = checked(connector, config, state, changed, seen)
        with pytest.raises(error, match=match):
            deciding(approved)

    # More than the stated amounts of either currency is beyond the inventory.
    for field, most in (("maximum0", AMOUNT0), ("maximum1", AMOUNT1)):
        larger = intent.model_copy(
            update={field: getattr(intent, field).model_copy(update={"amount": most + 1})}
        )
        refuse(larger, DataError, "exceeds attributed input inventory")
    # Another range is no part of the capability, and no mint operation can carry one.
    elsewhere = intent.model_copy(update={"range": TickBounds(lower=lower, upper=upper)})
    refuse(elsewhere, ValueError, "fixed-range liquidity cannot select another tick range")
    with pytest.raises(ValueError, match="range differs from the configured fixed range"):
        moved = {**mint.model_dump(mode="json"), "range": {"lower": lower, "upper": upper}}
        type(mint).model_validate_json(json.dumps(moved))
    # A step that is not the planned one is refused, here a mint with a looser minimum.
    loose = mint.model_copy(update={"minimum_liquidity": 1})
    assert loose != mint
    with pytest.raises(ValueError, match="differs from its retained economic intent or NFT"):
        step(approved, loose)
    # A liquidity deployment observes no quote, so no swap is admitted along one.
    swap = found.codec.actions.authoring.swap
    quoted = import_module(f"almanak.connectors.{connector}.positions").Quote(
        source=target,
        block=market.block,
        amount_in=owned.inventory1,
        amount_out=AssetAmount(asset=target.assets[0], amount=5),
        sqrt_price_after=2**96,
        initialized_ticks_crossed=0,
        calculation="contract_quoter",
    )
    refuse(
        swap(config.allocation(ALLOCATION), seen, quoted),
        ValueError,
        "exact-size retained contract quote",
    )
    assert execution.increases_exposure(config, mint)


def test_amounts_that_add_no_liquidity_are_refused_with_what_the_range_takes(connector):
    market = resolved(connector)
    inside, beside = ticks(market)
    spacing = market.pool.tick_spacing

    def refused(takes, tick, **stated):
        config, _, seen = started(connector, market, opening(market, **stated), tick=tick)
        match = f"add no liquidity: at the pool's tick {tick} .* takes {takes}"
        with pytest.raises(ValueError, match=match):
            v3_pool_direct.intent(config, (seen,), opening(market, **stated))

    stated = {"tick_lower": beside[0], "tick_upper": beside[1]}
    refused("only currency0", 0, amount0=0, **stated)
    refused("only currency1", beside[1], amount1=0, **stated)
    refused("only currency1", beside[1] + spacing, amount1=0, **stated)
    refused("both currencies", 0, amount0=0)
    refused("both currencies", 0, amount1=0)
    # One currency is enough beside the price.
    config, _, seen = started(connector, market, opening(market, amount1=0, **stated))
    intent = v3_pool_direct.intent(config, (seen,), opening(market, amount1=0, **stated))
    assert (intent.maximum0.amount, intent.maximum1.amount) == (AMOUNT0, 0)
    assert (inside[0], inside[1]) != beside


def test_an_increase_adds_the_stated_amounts_to_the_adopted_nft_only(connector):
    market = resolved(connector)
    config, state, seen = on_position(connector, market, "increase-liquidity")
    intent = v3_pool_direct.intent(config, (seen,), naming("increase-liquidity"))
    assert intent.kind == "increase_liquidity"
    assert (intent.maximum0.amount, intent.maximum1.amount) == (AMOUNT0, AMOUNT1)
    found = connector_from_wire({"connector": connector})
    found.codec.capability.admit(state.capabilities, intent)
    assert found.lifecycle.increases_exposure(intent)

    step = checked(connector, config, state, intent, seen)
    spender = config.target(ALLOCATION).contracts.position_manager
    assert (step(seen).kind, step(seen).spender) == ("approve", spender)
    both = {(0, spender): AMOUNT0, (1, spender): AMOUNT1}
    approved = observation(
        connector, config, state.owned[0], market, allowed=both, position=position(market)
    )
    increase = step(approved)
    assert (increase.kind, increase.nft) == ("increase", nft_id(market))
    # The same step on another NFT is not the planned one.
    other = increase.model_copy(update={"nft": nft_id(market, OTHER_TOKEN)})
    with pytest.raises(ValueError, match="differs from its retained economic intent or NFT"):
        step(approved, other)
    # A deployment that owns a position opens no second one.
    opened = found.codec.actions.authoring.open_liquidity(config.allocation(ALLOCATION), seen)
    with pytest.raises(ValueError, match="must preserve existing NFT ownership"):
        checked(connector, config, state, opened, seen)(approved)


def principal(market, amount=LIQUIDITY):
    lower, upper = ticks(market)[0]
    return amounts(amount, sqrt_at_tick(0), sqrt_at_tick(lower), sqrt_at_tick(upper))


@pytest.mark.parametrize(
    ("action", "kind", "removed"),
    [("decrease-liquidity", "decrease", LIQUIDITY // 4), ("close-liquidity", "close", LIQUIDITY)],
)
def test_a_removal_bounds_its_outputs_and_acts_on_the_adopted_nft_only(
    connector, action, kind, removed
):
    market = resolved(connector)
    config, state, seen = on_position(connector, market, action)
    intent = v3_pool_direct.intent(config, (seen,), naming(action))
    assert intent.kind == COMMANDS[action] and intent.deadline == market.block.timestamp + 300
    # Each minimum is the principal at the observed price less 50 basis points.
    expected = [value * 9950 // 10000 for value in principal(market, removed)]
    assert [intent.minimum0.amount, intent.minimum1.amount] == expected and min(expected) > 0
    assert getattr(intent, "liquidity", removed) == removed
    found = connector_from_wire({"connector": connector})
    found.codec.capability.admit(state.capabilities, intent)
    assert not found.lifecycle.increases_exposure(intent)

    step = checked(connector, config, state, intent, seen)
    operation = step(seen)
    assert (operation.kind, operation.nft, operation.liquidity) == (kind, nft_id(market), removed)
    other = operation.model_copy(update={"nft": nft_id(market, OTHER_TOKEN)})
    with pytest.raises(ValueError, match="differs from its retained economic intent or NFT"):
        step(seen, other)
    # Looser minimums than the stated slippage are refused against the observation.
    wider = intent.model_copy(update={"minimum0": intent.minimum0.model_copy(update={"amount": 0})})
    with pytest.raises(ValueError, match="minima widen the configured slippage limit"):
        checked(connector, config, state, wider, seen)(seen)


def test_a_decrease_must_leave_liquidity_and_names_the_close(connector):
    market = resolved(connector)
    config, _, seen = on_position(connector, market, "decrease-liquidity")
    for size in (LIQUIDITY, LIQUIDITY + 1):
        with pytest.raises(ValueError, match=f"below the position's {LIQUIDITY}; close-liquidity"):
            v3_pool_direct.intent(config, (seen,), naming("decrease-liquidity", liquidity=size))
    most = naming("decrease-liquidity", liquidity=LIQUIDITY - 1)
    assert v3_pool_direct.intent(config, (seen,), most).liquidity == LIQUIDITY - 1


def test_a_collection_keeps_the_liquidity_and_acts_on_the_adopted_nft_only(connector):
    market = resolved(connector)
    config, state, seen = on_position(connector, market, "collect-liquidity-fees")
    intent = v3_pool_direct.intent(config, (seen,), naming("collect-liquidity-fees"))
    assert intent.kind == "collect_liquidity_fees"
    assert intent.deadline == market.block.timestamp + liquidity.ADMISSION_SECONDS
    step = checked(connector, config, state, intent, seen)
    operation = step(seen)
    assert (operation.kind, operation.nft) == ("collect", nft_id(market))
    other = operation.model_copy(update={"nft": nft_id(market, OTHER_TOKEN)})
    with pytest.raises(ValueError, match="differs from its retained economic intent or NFT"):
        step(seen, other)
    # The command names the NFT the deployment adopted, and no other.
    with pytest.raises(ValueError, match="does not own the position the command names"):
        v3_pool_direct.intent(
            config, (seen,), naming("collect-liquidity-fees", token_id=OTHER_TOKEN)
        )
    with pytest.raises(ValueError, match="does not own the position the command names"):
        v3_pool_direct.intent(config, (seen,), opening(market))


@pytest.mark.parametrize("action", ["collect-liquidity-fees", "close-liquidity"])
def test_an_emptied_position_builds_no_collection_and_no_close(connector, action):
    market = resolved(connector)
    emptied = held(market, liquidity=0)
    # Adopted through an increase, which an emptied position still takes.
    config, _, seen = on_position(connector, market, "increase-liquidity", emptied)
    assert seen.owned.position.liquidity == 0
    with pytest.raises(ValueError, match=f"holds no liquidity; {action} is not served"):
        v3_pool_direct.intent(config, (seen,), naming(action))


def test_a_revocation_zeroes_each_allowance_and_refuses_when_there_is_none(connector):
    market = resolved(connector)
    config, state, bare = started(connector, market, request("revoke-exchange"))
    with pytest.raises(ValueError, match="no allowance in either pool currency"):
        v3_pool_direct.intent(config, (bare,), request("revoke-exchange"))
    target = config.target(ALLOCATION)
    spenders = (target.contracts.router, target.contracts.position_manager)
    left = {(0, spenders[1]): 5, (1, spenders[0]): 2**256 - 1}
    seen = observation(connector, config, state.owned[0], market, allowed=left)
    intent = v3_pool_direct.intent(config, (seen,), request("revoke-exchange"))
    assert intent.kind == "revoke_exchange"
    found = connector_from_wire({"connector": connector})
    found.codec.capability.admit(state.capabilities, intent)
    assert not found.lifecycle.increases_exposure(intent)
    step = checked(connector, config, state, intent, seen)
    first = step(seen)
    assert (first.kind, first.asset, first.spender, first.amount) == (
        "approve",
        target.assets[0],
        spenders[1],
        0,
    )
    rest = observation(connector, config, state.owned[0], market, allowed={(1, spenders[0]): 9})
    last = step(rest)
    assert (last.asset, last.spender, last.amount) == (target.assets[1], spenders[0], 0)
    assert not found.execution.increases_exposure(config, last)


def test_the_report_holds_the_position_what_moved_the_inventory_and_the_allowances(connector):
    market = resolved(connector)
    report = direct.binding(connector).position
    assets = [str(market.pool.currency0), str(market.pool.currency1)]

    def listed(values):
        return [
            {"asset": asset, "amount": str(value)}
            for asset, value in zip(assets, values, strict=True)
        ]

    config, state, seen = started(connector, market, opening(market))
    assert report(state) == {
        "position": None,
        "moved": listed((0, 0)),
        "inventory": listed((AMOUNT0, AMOUNT1)),
        "allowances": [],
    }

    # After the mint: the NFT, what the mint used, what is unused and still approved.
    lower, upper = ticks(market)[0]
    spender = config.target(ALLOCATION).contracts.position_manager
    minted = type(state.owned[0])(
        target=state.owned[0].target,
        inventory0=AssetAmount(asset=market.pool.currency0, amount=40),
        inventory1=AssetAmount(asset=market.pool.currency1, amount=0),
        position=position(market, replace(held(market), owed=(0, 0))),
        entry_complete=True,
    )
    after = observation(
        connector,
        config,
        minted,
        market,
        allowed={(0, spender): 40},
        position=minted.position,
    )
    reverted = SimpleNamespace(success=False, delta0=-7, delta1=-7, after=seen)
    mined = SimpleNamespace(success=True, delta0=40 - AMOUNT0, delta1=-AMOUNT1, after=after)
    done = SimpleNamespace(owned=(minted,), baseline=(seen,), outcomes=(reverted, mined))
    assert report(done) == {
        "position": {
            "nft": str(nft_id(market)),
            "token_id": str(TOKEN),
            "range": {"lower": lower, "upper": upper},
            "liquidity": str(LIQUIDITY),
            "owed_recorded": listed((0, 0)),
        },
        "moved": listed((40 - AMOUNT0, -AMOUNT1)),
        "inventory": listed((40, 0)),
        "allowances": [{"asset": assets[0], "spender": spender, "amount": "40"}],
    }

    # An adopted position is reported from the start, with what the manager recorded.
    _, adopted, _ = on_position(connector, market, "collect-liquidity-fees")
    held_report = report(adopted)
    assert held_report["position"]["owed_recorded"] == listed((11, 13))
    assert held_report["inventory"] == listed((0, 0)) and held_report["moved"] == listed((0, 0))
    # A revocation deployment has no range, so it reports no position at all.
    _, revoking, _ = started(connector, market, request("revoke-exchange"))
    assert set(report(revoking)) == {"inventory", "allowances"}


def test_the_command_builds_the_deployment_through_the_connectors_resolver(connector, monkeypatch):
    market = resolved(connector)
    block, rows = retained(connector)
    nfts = {TOKEN: held(market)}

    class Head(Manager):
        def pin_block(self, selector):
            return block if selector == "latest" else super().pin_block(selector)

    monkeypatch.setattr(
        "almanak.gateway.ethereum.EthereumReader", lambda chain, endpoint: Head(market, rows, nfts)
    )
    wallet = {"account": WALLET.account, "address": WALLET.address}
    model = ax_actions.resolution(connector).MarketSelection
    selection = model(connector=connector, chain=block.chain.chain_id, pool=POOLS[connector][1])
    limits = ax_actions.FeeLimits(max_gas_price_wei=7, max_transaction_fee=9)
    for stated in (opening(market), naming("close-liquidity"), request("revoke-exchange")):
        config = ax_actions.deployment(
            connector,
            selection,
            stated,
            wallet,
            ax_actions.alchemy(block.chain.chain_id),
            "endpoint",
            limits,
        )
        assert config == deployed(connector, market, stated, nfts)


def arguments(connector, command, *extra, without=()):
    stated = {
        "--connector": connector,
        "--chain": "eip155:8453",
        "--pool": POOLS[connector][1],
        "--wallet": "wallet",
        "--env-file": ".env",
        "--max-gas-price-wei": "7",
        "--max-transaction-fee": "9",
    }
    if command == "open-liquidity":
        stated.update({"--tick-lower": "-200", "--tick-upper": "400"})
    if command in liquidity.NFT_ACTIONS:
        stated["--token-id"] = str(TOKEN)
    if command in liquidity.ADDITIONS:
        stated.update({"--amount0": "4000", "--amount1": "3000"})
    if command == "decrease-liquidity":
        stated["--liquidity"] = "250"
    if command in PRICED:
        stated["--slippage-bps"] = "50"
    flags = [
        part for name, value in stated.items() if name not in without for part in (name, value)
    ]
    return parse_validated(build_parser(), ["ax", command, *flags, *extra])


@pytest.mark.parametrize("command", list(COMMANDS))
def test_the_command_line_states_each_command_and_only_what_it_takes(connector, command, capsys):
    args = arguments(connector, command)
    assert (args.product, args.ax_command, args.execute) == ("operator-action", command, False)
    stated = ax_actions.action_request(args)
    priced = command in PRICED
    adds = command in liquidity.ADDITIONS
    assert stated == ActionRequest(
        command,
        slippage_bps=50 if priced else None,
        deadline_seconds=300 if priced else None,
        amount0=4000 if adds else None,
        amount1=3000 if adds else None,
        tick_lower=-200 if command == "open-liquidity" else None,
        tick_upper=400 if command == "open-liquidity" else None,
        token_id=TOKEN if command in liquidity.NFT_ACTIONS else None,
        liquidity=250 if command == "decrease-liquidity" else None,
    )
    market = ax_actions.market_request(args)
    assert (market.connector, str(market.chain), market.pool) == (
        connector,
        "eip155:8453",
        POOLS[connector][1],
    )
    required = ["--chain", "--pool", "--wallet", "--max-gas-price-wei"]
    required += ["--tick-lower", "--tick-upper"] * (command == "open-liquidity")
    required += ["--token-id"] * (command in liquidity.NFT_ACTIONS)
    required += ["--liquidity"] * (command == "decrease-liquidity")
    required += ["--slippage-bps"] * priced
    for missing in required:
        with pytest.raises(SystemExit):
            arguments(connector, command, without=(missing,))
    # No liquidity command takes a swap's or a supply's options, or a band.
    refused = [("--amount", "5"), ("--sell", "x"), ("--config", "market.json"), ("--band", "1")]
    refused += [("--token-id", "0"), ("--token-id", "-1"), ("--token-id", str(2**256))]
    refused += [("--liquidity", "0"), ("--slippage-bps", "0"), ("--slippage-bps", "10000")]
    refused += [("--tick-lower", "1.5"), ("--tick-lower", "-887273"), ("--tick-upper", "887273")]
    refused += [("--amount0", "-1"), ("--amount1", "1.5"), ("--deadline-seconds", "0")]
    if not priced:
        refused += [("--slippage-bps", "50"), ("--deadline-seconds", "60")]
    if command != "open-liquidity":
        refused += [("--tick-lower", "0")]
    if command == "open-liquidity":
        refused += [("--token-id", "5")]
    for extra in refused:
        with pytest.raises(SystemExit):
            arguments(connector, command, *extra)
    capsys.readouterr()


def test_the_command_line_bounds_ticks_amounts_and_the_deadline(connector):
    args = arguments(
        connector,
        "open-liquidity",
        "--deadline-seconds",
        "60",
        "--execute",
        without=("--amount1",),
    )
    assert (args.amount0, args.amount1, args.deadline_seconds, args.execute) == (4000, 0, 60, True)
    edges = arguments(
        connector, "open-liquidity", "--tick-lower", "-887272", "--tick-upper", "887272"
    )
    assert (edges.tick_lower, edges.tick_upper) == (-887272, 887272)
    assert (
        arguments(connector, "close-liquidity", "--token-id", str(2**256 - 1)).token_id
        == 2**256 - 1
    )


def test_the_command_refuses_an_undeclared_chain_and_passes_the_request_on(connector, monkeypatch):
    seen = {}

    def deployment(name, market, action, wallet, provider, endpoint, limits, **options):
        seen.update(connector=name, market=market, action=action)
        raise StopIteration

    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
    monkeypatch.setattr(ax_actions, "deployment", deployment)
    with pytest.raises(StopIteration):
        ax_actions.command(arguments(connector, "decrease-liquidity"))
    assert (seen["connector"], seen["market"].pool) == (connector, POOLS[connector][1])
    assert (seen["action"].token_id, seen["action"].liquidity) == (TOKEN, 250)

    monkeypatch.setattr(ax_actions, "read_wallet", pytest.fail)
    for command in COMMANDS:
        with pytest.raises(ValueError, match=f"{connector} serves no direct {command} on"):
            ax_actions.command(arguments(connector, command, "--chain", "eip155:31337"))


def position_arguments(connector, *extra, without=()):
    stated = {
        "--connector": connector,
        "--chain": "eip155:8453",
        "--pool": POOLS[connector][1],
        "--token-id": str(TOKEN),
        "--env-file": ".env",
    }
    flags = [
        part for name, value in stated.items() if name not in without for part in (name, value)
    ]
    return parse_validated(build_parser(), ["ax", "liquidity-position", *flags, *extra])


def test_the_position_read_is_an_observation_with_the_reads_own_transports(
    connector, capsys, monkeypatch
):
    for name in ("ALMANAK_AX_GATEWAY_URL", "ALMANAK_AX_GATEWAY_AUTH_FILE"):
        monkeypatch.delenv(name, raising=False)
    args = position_arguments(connector)
    assert (args.product, args.ax_command, args.block) == (
        "ax-liquidity-position",
        "liquidity-position",
        "finalized",
    )
    stated = ax.parse_request(args)
    assert stated == PositionRequest(
        chain=ChainId("eip155:8453"), pool=POOLS[connector][1], token_id=TOKEN
    )
    assert position_arguments(connector, "--block", "latest").block == "latest"
    gateway = ("--gateway-url", "http://127.0.0.1:1", "--gateway-auth-file", "token")
    assert position_arguments(connector, *gateway, without=("--env-file",)).gateway_url
    for missing in ("--pool", "--token-id", "--connector", "--env-file"):
        with pytest.raises(SystemExit):
            position_arguments(connector, without=(missing,))
    for extra in (("--token-id", "0"), ("--config", "request.json"), ("--wallet", "wallet")):
        with pytest.raises(SystemExit):
            position_arguments(connector, *extra)
    capsys.readouterr()
    for refused in ({"token_id": 0}, {"pool": ZERO_ADDRESS}, {"chain": "solana:5eykt4"}):
        values = {"chain": "eip155:8453", "pool": POOLS[connector][1], "token_id": TOKEN}
        with pytest.raises(ValueError):
            PositionRequest.model_validate_json(json.dumps({**values, **refused}))


def reading(connector, market, nfts, monkeypatch, tick=0):
    """A reader and a family whose pool and tick reads are stated by the test."""
    real = v3_pool_request.family(connector)
    outside = {ticks(market)[0][0]: (Q128, 0), ticks(market)[0][1]: (0, 2 * Q128)}

    def read_tick(reader, source, block, tick):
        return (1, 1, *outside[tick])

    stated = v3_pool_request.Family(
        resolution=real.resolution,
        support=real.support,
        read_pool=lambda reader, source, block: pool_state(connector, market, tick),
        read_tick=read_tick,
        price_request=real.price_request,
    )
    monkeypatch.setattr(v3_pool_request, "family", lambda name: stated)
    return manager(connector, market, nfts)


def test_the_position_read_reports_owner_range_principal_and_what_a_collection_pays(
    connector, monkeypatch
):
    market = resolved(connector)
    chain = market.pool.chain.chain_id
    stated = PositionRequest(chain=chain, pool=market.pool.pool, token_id=TOKEN)
    # Another account's position is read as well: the report names its owner.
    nfts = {TOKEN: held(market, owner=STRANGER), OTHER_TOKEN: held(market, liquidity=0)}
    reader = reading(connector, market, nfts, monkeypatch)
    report = read_liquidity_position(reader, connector, stated, market.block)
    lower, upper = ticks(market)[0]
    assets = (market.asset0, market.asset1)

    def sides(values):
        return [
            {"asset": str(asset.identifier), "amount": str(value), "decimals": asset.decimals}
            for asset, value in zip(assets, values, strict=True)
        ]

    assert report["product"] == "liquidity_position" and report["support"] == "verified"
    assert report["request"] == {**stated.model_dump(mode="json"), "connector": connector}
    assert (report["owner"], report["block"]) == (STRANGER, market.block.model_dump(mode="json"))
    assert report["position"] == {
        "nft": str(nft_id(market)),
        "token_id": str(TOKEN),
        "range": {"lower": lower, "upper": upper},
        "liquidity": str(LIQUIDITY),
    }
    assert report["pool"] == {
        "sqrt_price_x96": str(sqrt_at_tick(0)),
        "tick": 0,
        "in_range": True,
    }
    assert report["principal"] == sides(principal(market)) and min(principal(market)) > 0
    # Growth inside the range is the pool's total less what lies outside each bound:
    # (5 - 1 - 0, 7 - 0 - 2) per unit of liquidity, less the position's checkpoint of 1.
    assert report["owed_recorded"] == sides((11, 13))
    assert report["owed"] == sides((11 + 3 * LIQUIDITY, 13 + 4 * LIQUIDITY))
    assert report["resolution"] == market.summary()
    # The read gateway already serves every method the read used: it needs no new one.
    assert reader.methods and reader.methods <= set(get_args(ReadMethod))

    # A position without liquidity earns nothing and needs no tick read.
    emptied = read_liquidity_position(
        reader, connector, replace_token(stated, OTHER_TOKEN), market.block
    )
    assert emptied["owner"] == WALLET.address and emptied["position"]["liquidity"] == "0"
    assert emptied["owed"] == emptied["owed_recorded"] == sides((11, 13))
    assert emptied["principal"] == sides((0, 0))


def replace_token(stated, token):
    return stated.model_copy(update={"token_id": token})


def test_the_position_read_refuses_a_missing_nft_another_pool_and_another_chain(
    connector, monkeypatch
):
    market = resolved(connector)
    chain = market.pool.chain.chain_id
    stated = PositionRequest(chain=chain, pool=market.pool.pool, token_id=TOKEN)
    elsewhere = held(market, key=position_key(market.pool)[1] + 1)
    reader = reading(connector, market, {OTHER_TOKEN: elsewhere}, monkeypatch, tick=-1)
    with pytest.raises(ValueError, match=f"holds no NFT {TOKEN}"):
        read_liquidity_position(reader, connector, stated, market.block)
    with pytest.raises(ValueError, match=f"NFT {OTHER_TOKEN} is a position of another pool"):
        read_liquidity_position(reader, connector, replace_token(stated, OTHER_TOKEN), market.block)
    other = "eip155:1" if str(chain) != "eip155:1" else "eip155:8453"
    with pytest.raises(ValueError, match="request chain differs from the reader chain"):
        foreign = PositionRequest(chain=ChainId(other), pool=market.pool.pool, token_id=TOKEN)
        read_liquidity_position(reader, connector, foreign, market.block)


def test_the_read_command_pins_its_block_and_passes_the_request_on(connector, monkeypatch):
    market = resolved(connector)
    seen = {}

    class Reader:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def pin_block(self, selector):
            seen["selector"] = selector
            return market.block

    def read(reader, name, stated, block):
        seen.update(connector=name, request=stated, block=block)
        return {"product": "liquidity_position"}

    monkeypatch.setattr(ax, "observation_reader", lambda args, chain: Reader())
    monkeypatch.setattr(ax, "read_liquidity_position", read)
    args = position_arguments(connector, "--block", "latest")
    assert ax.liquidity_position(args) == {"product": "liquidity_position"}
    assert (seen["selector"], seen["connector"], seen["block"]) == (
        "latest",
        connector,
        market.block,
    )
    assert seen["request"].token_id == TOKEN
    with pytest.raises(ValueError, match=f"{connector} declares no support on eip155:31337"):
        ax.liquidity_position(position_arguments(connector, "--chain", "eip155:31337"))
