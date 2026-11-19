"""`almanak ax swap`: a direct exact-input swap in one named V3-family pool.

Component evidence without signing or chain access: each connector's pool is resolved
from its retained resolution reads, and observations are built over that pool. The fork
rehearsal of the whole command is tests/fork/test_ax_swap.py.
"""

import gzip
import hashlib
import json
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import ax_actions
from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct, v3_pool_direct, v3_pool_reconciliation
from almanak.connectors.composition import connector_for, connector_from_wire
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
from almanak.gateway.ethereum import DataError
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.gateway.service import GatewayService
from almanak.models import AssetAmount, Block
from almanak.operator_deployment import supports_operator
from almanak.operator_runtime import OperatorRuntime
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from tests.test_operator_runtime import GatewayJournal

FIXTURES = Path(__file__).with_name("fixtures")
# Retained resolution reads and the pool they resolve, per connector.
POOLS = {
    "uniswap_v3": (
        "uniswap-v3-base-resolution-reads.json.gz",
        "0xd0b53d9277642d899df5c87a3966a349a798f224",
    ),
    "pancakeswap_v3": (
        "pancakeswap-v3-bnb-resolution-reads.json.gz",
        "0xc721deccd986d54b39e8c29428a1f06155c3671e",
    ),
    "aerodrome_slipstream": (
        "aerodrome-slipstream-base-resolution-reads.json.gz",
        "0x3fe04a59ebd38cf06080a6f60a98d124eb59392a",
    ),
}
# What each connector's pool observation holds beyond the shared price fields.
OBSERVED = {
    "uniswap_v3": {"protocol_fee": 0},
    "pancakeswap_v3": {"protocol_fee": 0, "lm_pool": "0x" + "00" * 20},
    "aerodrome_slipstream": {
        "swap_fee": 500,
        "unstaked_fee": 100000,
        "staked_liquidity": 0,
        "gauge": "0x" + "a1" * 20,
        "nft_manager": "0x" + "a2" * 20,
        "swap_fee_module": "0x" + "a3" * 20,
        "unstaked_fee_module": "0x" + "a4" * 20,
    },
}
# SHA-256 of a strategy checkpoint's JSON, taken before these checkpoints learned of
# operator deployments. The checkpoint is `strategy_checkpoint` below.
STRATEGY_WIRE = {
    "uniswap_v3": "9e922453d5d17e32b41d3d330893c7471a33fa3da300b25e2a66f7dcd9641c6c",
    "pancakeswap_v3": "0cb6e990f4c1170cdbfd534814155612224c371af9a9bffffc1a2ddaa793bd7e",
    "aerodrome_slipstream": "29716225027553a52ca32414ecaef09a624368a33a634dbdb8b3a238c7a96011",
}
CONNECTORS = tuple(POOLS)
WALLET = Wallet(account="fund", address="0x" + "12" * 20)
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=7, max_transaction_fee=9)
AMOUNT, QUOTED = 1000, 4321

pytestmark = pytest.mark.parametrize("connector", CONNECTORS)


def retained(connector):
    raw = json.loads(gzip.decompress((FIXTURES / POOLS[connector][0]).read_bytes()))
    block = Block.model_validate_json(json.dumps(raw["block"]))
    rows = tuple(ResolutionRead.model_validate_json(json.dumps(row)) for row in raw["evidence"])
    return block, rows


def selection(connector, block):
    model = ax_actions.resolution(connector).MarketSelection
    return model(connector=connector, chain=block.chain.chain_id, pool=POOLS[connector][1])


def resolved(connector):
    block, rows = retained(connector)
    resolve = ax_actions.resolution(connector).resolve_market
    return resolve(RetainedResolutionReader(block.chain, rows), selection(connector, block), block)


def request(market, *, sell=None, amount=AMOUNT, slippage_bps=50):
    return ActionRequest(
        "swap",
        amount,
        sell=str(market.pool.currency0) if sell is None else sell,
        slippage_bps=slippage_bps,
        deadline_seconds=300,
    )


def configuration(connector, positions, market):
    chain = market.pool.chain
    provider = ax_actions.alchemy(chain.chain_id)
    return ax_actions.configuration(connector, positions, WALLET, provider, chain, LIMITS)


def operator_config(connector, market, **stated):
    positions = v3_pool_direct.positions(None, None, market, WALLET, request(market, **stated))
    return configuration(connector, positions, market)


def observation(connector, config, owned, market, *, wallet=5000, bought=0, allowance=0, **quotes):
    """An on-chain observation of the deployment's one position at the resolved block."""
    snapshot = connector_from_wire({"connector": connector}).codec.snapshot
    target = config.target(ALLOCATION)
    pool = snapshot.model_fields["pool"].annotation(
        source=market.pool,
        block=market.block,
        currency0=market.asset0,
        currency1=market.asset1,
        sqrt_price_x96=2**96,
        tick=0,
        liquidity=10**18,
        unlocked=True,
        **OBSERVED[connector],
    )
    spenders = (target.contracts.router, target.contracts.position_manager)
    return snapshot(
        target=target,
        pool=pool,
        owned=owned,
        wallet0=AssetAmount(asset=target.assets[0], amount=wallet),
        wallet1=AssetAmount(asset=target.assets[1], amount=bought),
        allowances=tuple(
            {
                "asset": asset,
                "spender": spender,
                "amount": allowance if (asset, spender) == (target.assets[0], spenders[0]) else 0,
            }
            for asset in target.assets
            for spender in spenders
        ),
        native_balance=AssetAmount(
            asset=evm_network(market.pool.chain.chain_id).native_asset, amount=10**18
        ),
        payer=target.fund,
        position=None,
        **quotes,
    )


def quote(connector, owned, market, amount=AMOUNT, output=QUOTED):
    model = import_module(f"almanak.connectors.{connector}.positions").Quote
    return model(
        source=owned.target,
        block=market.block,
        amount_in=AssetAmount(asset=market.pool.currency0, amount=amount),
        amount_out=AssetAmount(asset=market.pool.currency1, amount=output),
        sqrt_price_after=2**96,
        initialized_ticks_crossed=0,
        calculation="contract_quoter",
    )


def created(connector, config, strategy=None):
    found = connector_from_wire({"connector": connector})
    capabilities = found.codec.capability.declared(config)
    return found.lifecycle.initial(config, strategy, capabilities, None)


def strategy_checkpoint(connector):
    """A strategy deployment after its start, built from the retained resolution only."""
    market = resolved(connector)
    document = {
        "allocations": [
            {
                "name": "position",
                "account": "fund",
                "pool": market.pool.model_dump(mode="json"),
                "contracts": market.contracts.model_dump(mode="json"),
                "range": None,
                "capital": {"asset": str(market.pool.currency0), "amount": AMOUNT},
                "entry_swap_bps": 10000,
                "slippage_bps": 50,
                "deadline_seconds": 300,
                "native_asset": str(evm_network(market.pool.chain.chain_id).native_asset),
                "minimum_native_balance": 0,
            }
        ]
    }
    config = configuration(connector, document, market)
    strategy = StrategyDefinition(filename="strategy.py", source="def decide(context):\n    pass\n")
    state = created(connector, config, strategy)
    seen = observation(connector, config, state.owned[0], market)
    return state.model_copy(
        update={"deployment_id": "0" * 32, "initialized": True, "baseline": (seen,)}
    )


def test_the_connector_supports_operator_deployments_and_serves_the_swap(connector):
    found = connector_from_wire({"connector": connector})
    assert supports_operator(found.codec.checkpoint)
    assert direct.binding(connector).actions == ("swap",)
    assert connector in direct.connectors_for("swap") and connector not in direct.NOT_DIRECT
    cells = [cell for cell in found.chain_support if cell.direct_actions]
    assert cells and all(
        [str(action) for action in cell.direct_actions] == ["swap_exact_input"] for cell in cells
    )
    for cell in cells:
        direct.require_direct(connector, cell.chain, direct.ActionRequest("swap", 5))
    with pytest.raises(ValueError, match=f"{connector} serves no direct swap on eip155:31337"):
        direct.require_direct(connector, ChainId("eip155:31337"), direct.ActionRequest("swap", 5))
    with pytest.raises(ValueError, match=f"{connector} serves no direct supply"):
        direct.require_direct(connector, cells[0].chain, direct.ActionRequest("supply", 5))


def test_the_swap_is_the_only_action_an_operator_request_is_admitted_for(connector):
    cells = connector_from_wire({"connector": connector}).chain_support
    others = [
        (cell, action) for cell in cells for action in cell.actions if action != "swap_exact_input"
    ]
    # Liquidity, exits and revocations stay strategy actions on every chain.
    assert {str(action) for _, action in others} >= {"open_liquidity", "exit_exchange"}
    for cell, action in others:
        with pytest.raises(ValueError, match="unsupported as a direct action"):
            cell.require_direct(action)


def test_a_strategy_checkpoint_keeps_its_wire_form(connector):
    state = strategy_checkpoint(connector)
    body = state.model_dump_json()
    assert hashlib.sha256(body.encode()).hexdigest() == STRATEGY_WIRE[connector]
    assert "operator" not in json.loads(body) and state.operator is False
    restored = type(state).model_validate_json(body)
    assert restored == state and restored.model_dump_json() == body


def test_an_operator_checkpoint_retains_no_strategy_and_round_trips(connector):
    market = resolved(connector)
    state = created(connector, operator_config(connector, market))
    assert (state.operator, state.strategy, state.initialized) == (True, None, False)
    body = state.model_dump_json()
    wire = json.loads(body)
    assert (wire["operator"], wire["strategy"]) == (True, None)
    assert type(state).model_validate_json(body) == state
    # The origin is exclusive: an operator deployment retains no strategy, and a
    # strategy deployment is never without one.
    strategy = strategy_checkpoint(connector).strategy
    with pytest.raises(ValueError, match="an operator deployment retains no strategy"):
        type(state).model_validate_json(json.dumps({**wire, "strategy": strategy.model_dump()}))
    with pytest.raises(ValueError, match="a strategy deployment retains its strategy"):
        type(state).model_validate_json(json.dumps({**wire, "operator": False}))
    report = connector_from_wire({"connector": connector}).projection.report(state)
    assert report["strategy"] is None and report["connector"] == connector


def test_the_deployment_is_one_trading_allocation_funded_with_the_amount_sold(connector):
    market = resolved(connector)
    sold = str(market.pool.currency1)
    config = operator_config(connector, market, sell=sold, amount=77, slippage_bps=25)
    (allocation,) = config.allocations
    assert (allocation.name, allocation.account) == (ALLOCATION, WALLET.account)
    assert (allocation.pool, allocation.contracts) == (market.pool, market.contracts)
    assert (str(allocation.capital.asset), allocation.capital.amount) == (sold, 77)
    # The whole capital is one entry, and there is no liquidity shape to mint into.
    assert (allocation.entry_swap_bps, allocation.range, allocation.band) == (10000, None, None)
    assert (allocation.slippage_bps, allocation.deadline_seconds) == (25, 300)
    assert allocation.minimum_native_balance == 0
    target = config.target(ALLOCATION)
    assert str(target.funding_asset) == sold and target.fund.address == WALLET.address
    assert direct.binding_for(config) is direct.binding(connector)
    state = created(connector, config)
    assert [item.target for item in state.capabilities.requirements] == [target]
    held = {str(v.asset): v.amount for v in (state.owned[0].inventory0, state.owned[0].inventory1)}
    assert held == {sold: 77, str(market.pool.currency0): 0}


def test_the_sold_asset_must_be_a_pool_currency_and_the_request_complete(connector):
    market = resolved(connector)
    chain = market.pool.chain.chain_id
    for sold in (str(evm_network(chain).native_asset), f"{chain}/erc20:0x" + "ab" * 20):
        with pytest.raises(ValueError, match="--sell must name one of the pool's currencies"):
            v3_pool_direct.positions(None, None, market, WALLET, request(market, sell=sold))
    incomplete = ActionRequest("swap", AMOUNT, sell=str(market.pool.currency0))
    with pytest.raises(ValueError, match="swap requires --amount, --slippage-bps"):
        v3_pool_direct.positions(None, None, market, WALLET, incomplete)


def test_the_command_resolves_the_named_pool_through_the_connectors_resolver(
    connector, monkeypatch
):
    block, rows = retained(connector)

    class Head(RetainedResolutionReader):
        def pin_block(self, selector):
            return block if selector == "latest" else super().pin_block(selector)

    monkeypatch.setattr(
        "almanak.gateway.ethereum.EthereumReader", lambda chain, endpoint: Head(chain, rows)
    )
    market = resolved(connector)
    wallet = {"account": WALLET.account, "address": WALLET.address}
    config = ax_actions.deployment(
        connector,
        selection(connector, block),
        request(market),
        wallet,
        ax_actions.alchemy(block.chain.chain_id),
        "endpoint",
        LIMITS,
    )
    assert config == operator_config(connector, market)
    assert config.production.gas[0].max_gas_price_wei == 7


def test_adoption_keeps_the_stated_inventory_only_when_the_wallet_covers_it(connector):
    market = resolved(connector)
    config = operator_config(connector, market)
    found = connector_from_wire({"connector": connector})
    owned = created(connector, config).owned
    covered = observation(connector, config, owned[0], market, wallet=AMOUNT)
    assert found.lifecycle.adopt(owned, (covered,)) == owned
    short = observation(connector, config, owned[0], market, wallet=AMOUNT - 1)
    with pytest.raises(DataError, match=f"the wallet holds {AMOUNT - 1} base units") as refusal:
        found.lifecycle.adopt(owned, (short,))
    assert refusal.value.kind == "inventory" and str(market.pool.currency0) in str(refusal.value)


def admitted(connector, **observed):
    """A deployment whose request is being admitted with one observation."""
    market = resolved(connector)
    config = operator_config(connector, market)
    state = created(connector, config)
    owned = state.owned[0]
    seen = observation(
        connector, config, owned, market, entry_quote=quote(connector, owned, market), **observed
    )
    return market, config, state, seen


def test_the_intent_follows_the_quote_of_the_observation_it_is_admitted_with(connector):
    market, config, state, seen = admitted(connector)
    intent = v3_pool_direct.intent(config, (seen,), request(market))
    assert intent.kind == "swap_exact_input" and intent.target == config.target(ALLOCATION)
    assert intent.amount == AssetAmount(asset=market.pool.currency0, amount=AMOUNT)
    # 50 basis points below the quoted output, rounded down.
    assert intent.minimum_output == AssetAmount(
        asset=market.pool.currency1, amount=QUOTED * 9950 // 10000
    )
    assert intent.deadline == market.block.timestamp + 300
    found = connector_from_wire({"connector": connector})
    found.codec.capability.admit(state.capabilities, intent)
    assert found.lifecycle.increases_exposure(intent)


def test_a_missing_or_differently_sized_quote_builds_no_intent(connector):
    market, config, state, seen = admitted(connector)
    owned = state.owned[0]
    bare = observation(connector, config, owned, market)
    with pytest.raises(ValueError, match="the pool quotes no output for the stated amount"):
        v3_pool_direct.intent(config, (bare,), request(market))
    unmeasured = observation(
        connector, config, owned, market, entry_quote_error="entry quote unavailable: reverted"
    )
    with pytest.raises(ValueError, match="entry quote unavailable: reverted"):
        v3_pool_direct.intent(config, (unmeasured,), request(market))
    for other in (request(market, amount=AMOUNT + 1), request(market, sell="other")):
        with pytest.raises(ValueError, match="not for the stated amount of the sold asset"):
            v3_pool_direct.intent(config, (seen,), other)


def planning(connector, config, state, intent, seen):
    """The planned operation of one observation, checked as the gateway checks it."""
    execution = connector_from_wire({"connector": connector}).execution
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


def test_the_gateway_admits_the_exact_approval_then_the_swap_and_nothing_larger(connector):
    market, config, state, seen = admitted(connector)
    intent = v3_pool_direct.intent(config, (seen,), request(market))
    deciding, step = planning(connector, config, state, intent, seen)
    owned, router = state.owned[0], config.target(ALLOCATION).contracts.router

    approval = step(seen)
    assert (approval.kind, approval.spender, approval.amount) == ("approve", router, AMOUNT)
    # A different allowance is reset before the exact one is granted.
    stale = observation(connector, config, owned, market, allowance=AMOUNT + 5)
    assert (step(stale).kind, step(stale).amount) == ("approve", 0)
    approved = observation(connector, config, owned, market, allowance=AMOUNT)
    swap = step(approved)
    assert (swap.kind, swap.amount, swap.minimum) == ("swap", intent.amount, intent.minimum_output)

    # Nothing sells more than the stated amount: inventory is that amount and no more.
    larger = intent.model_copy(
        update={"amount": AssetAmount(asset=market.pool.currency0, amount=AMOUNT + 1)}
    )
    deciding.decision = SimpleNamespace(intent=larger, observations=(seen,))
    execution = connector_from_wire({"connector": connector}).execution
    with pytest.raises(DataError, match="exceeds attributed input inventory"):
        execution.validate_operation(
            None, deciding, execution.operation(config, deciding, larger, approved), approved
        )
    # A minimum below the stated slippage is refused against the retained quote.
    wider = intent.model_copy(
        update={"minimum_output": AssetAmount(asset=market.pool.currency1, amount=1)}
    )
    deciding.decision = SimpleNamespace(intent=wider, observations=(seen,))
    with pytest.raises(ValueError, match="widens the configured slippage limit"):
        execution.validate_operation(
            None, deciding, execution.operation(config, deciding, wider, approved), approved
        )


def test_the_position_is_the_attributed_inventory_and_the_allowances_last_observed(connector):
    market, config, state, seen = admitted(connector)
    sold, bought = str(market.pool.currency0), str(market.pool.currency1)
    report = direct.binding(connector).position
    before = [{"asset": sold, "amount": str(AMOUNT)}, {"asset": bought, "amount": "0"}]
    assert report(state) == {"inventory": before, "allowances": []}
    started = state.model_copy(update={"initialized": True, "baseline": (seen,)})
    assert report(started) == {"inventory": before, "allowances": []}

    # After a mined approval whose swap never followed, the allowance is reported.
    owned = state.owned[0]
    approved = observation(connector, config, owned, market, allowance=AMOUNT)
    router = config.target(ALLOCATION).contracts.router
    stopped = SimpleNamespace(
        owned=state.owned, baseline=(seen,), outcomes=(SimpleNamespace(after=approved),)
    )
    assert report(stopped)["allowances"] == [
        {"asset": sold, "spender": router, "amount": str(AMOUNT)}
    ]

    swapped = owned.model_copy(
        update={
            "inventory0": AssetAmount(asset=market.pool.currency0, amount=0),
            "inventory1": AssetAmount(asset=market.pool.currency1, amount=QUOTED),
        }
    )
    done = SimpleNamespace(
        owned=(swapped,), baseline=(seen,), outcomes=(SimpleNamespace(after=seen),)
    )
    assert report(done) == {
        "inventory": [{"asset": sold, "amount": "0"}, {"asset": bought, "amount": str(QUOTED)}],
        "allowances": [],
    }


def arguments(connector, *extra, without=()):
    stated = {
        "--connector": connector,
        "--chain": "eip155:8453",
        "--pool": POOLS[connector][1],
        "--sell": "eip155:8453/erc20:0x4200000000000000000000000000000000000006",
        "--amount": "1000",
        "--slippage-bps": "50",
        "--wallet": "wallet",
        "--env-file": ".env",
        "--max-gas-price-wei": "7",
        "--max-transaction-fee": "9",
    }
    flags = [
        part for name, value in stated.items() if name not in without for part in (name, value)
    ]
    return parse_validated(build_parser(), ["ax", "swap", *flags, *extra])


def test_the_command_line_states_the_swap_and_its_bounds(connector, capsys):
    args = arguments(connector)
    assert (args.product, args.ax_command, args.execute) == ("operator-action", "swap", False)
    assert (args.amount, args.slippage_bps, args.deadline_seconds) == (1000, 50, 300)
    assert arguments(connector, "--deadline-seconds", "60", "--execute").deadline_seconds == 60
    assert ax_actions.action_request(args) == ActionRequest(
        "swap", 1000, sell=args.sell, slippage_bps=50, deadline_seconds=300
    )
    market = ax_actions.market_request(args)
    assert (market.connector, str(market.chain), market.pool) == (
        connector,
        "eip155:8453",
        POOLS[connector][1],
    )
    for missing in ("--chain", "--pool", "--sell", "--amount", "--slippage-bps", "--wallet"):
        with pytest.raises(SystemExit):
            arguments(connector, without=(missing,))
    # The price bound has no default and stays inside what the allocation accepts.
    for bound in ("1", "9999"):
        assert arguments(connector, "--slippage-bps", bound).slippage_bps == int(bound)
    for refused in ("0", "10000", "-1", "1.5"):
        with pytest.raises(SystemExit):
            arguments(connector, "--slippage-bps", refused)
    for extra in (("--amount", "0"), ("--deadline-seconds", "0"), ("--config", "market.json")):
        with pytest.raises(SystemExit):
            arguments(connector, *extra)
    assert "expected 1 to 9999 basis points" in capsys.readouterr().err


def test_the_command_refuses_an_undeclared_chain_and_passes_the_stated_swap_on(
    connector, monkeypatch
):
    seen = {}

    def deployment(name, market, action, wallet, provider, endpoint, limits, **options):
        seen.update(connector=name, market=market, action=action, limits=limits)
        raise StopIteration

    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
    monkeypatch.setattr(ax_actions, "deployment", deployment)
    with pytest.raises(StopIteration):
        ax_actions.command(arguments(connector))
    assert seen["limits"] == ax_actions.FeeLimits(7, 9, None)
    assert (seen["connector"], seen["market"].pool) == (connector, POOLS[connector][1])
    assert (seen["action"].sell, seen["action"].amount) == (arguments(connector).sell, 1000)

    monkeypatch.setattr(ax_actions, "read_wallet", pytest.fail)
    with pytest.raises(ValueError, match=f"{connector} serves no direct swap on eip155:31337"):
        ax_actions.command(arguments(connector, "--chain", "eip155:31337"))


class Observed:
    """A gateway double that answers every read with the one observation it was given."""

    def __init__(self, seen):
        self.seen = seen

    def observe(self):
        return (self.seen,)

    def verify_snapshot(self, observation):
        return self.seen

    def snapshot(self, allocation, block, observation):
        return self.seen


class SwapDesk:
    """An operator deployment behind the real gateway service, over one stated observation.

    The coordinator commits each update only after the service's own validation accepts
    it, so a request it retains was admitted by the gateway. `observe` builds the
    observation from the ownership the deployment was created with.
    """

    def __init__(self, tmp_path, config, observe):
        self.config = config
        self.store = Journal(tmp_path / "state.sqlite")
        self.store.create(config, None)
        self.gateway = Observed(observe(self.store.load().owned[0]))
        self.service = GatewayService(
            config, self.store.path, self.gateway, lease_seconds=2, operator_actions=True
        )

    def runtime(self):
        runtime = object.__new__(OperatorRuntime)
        runtime.connector = connector_for(self.config)
        runtime.journal = GatewayJournal(self.store, self.service)
        runtime.gateway = self.gateway
        runtime.closed = False
        runtime.clock = lambda: datetime(2026, 11, 19, tzinfo=UTC)
        runtime.fault = lambda *arguments: None
        runtime.poll = 0
        return runtime


def swap_desk(connector, tmp_path):
    market = resolved(connector)
    config = operator_config(connector, market)

    def observe(owned):
        return observation(
            connector, config, owned, market, entry_quote=quote(connector, owned, market)
        )

    return market, config, SwapDesk(tmp_path, config, observe)


def test_the_gateway_admits_the_swap_request_built_from_its_observation(connector, tmp_path):
    market, config, desk = swap_desk(connector, tmp_path)
    build = direct.binding(connector).intent
    stated = request(market)
    record = desk.runtime().submit(lambda state, observed: build(state.config, observed, stated))
    retained_state = desk.store.load()
    assert (record.intent.kind, record.trigger.kind) == ("swap_exact_input", "operator")
    assert retained_state.decision == record and retained_state.initialized
    assert record.intent.amount == AssetAmount(asset=market.pool.currency0, amount=AMOUNT)
    assert record.observations == (desk.gateway.seen,)


@pytest.mark.parametrize("refused", ["revoke_intent", "exit_intent"])
def test_the_gateway_refuses_an_operator_request_that_is_not_the_swap(connector, tmp_path, refused):
    _, config, desk = swap_desk(connector, tmp_path)
    actions = connector_from_wire({"connector": connector}).codec.actions
    other = getattr(actions, refused)(target=config.target(ALLOCATION))
    with pytest.raises(ValueError, match="unsupported as a direct action"):
        desk.runtime().submit(other)
    assert desk.store.load().decision is None


def test_an_operation_on_another_pool_is_refused_by_the_connectors_validation(connector):
    market, config, state, seen = admitted(connector)
    intent = v3_pool_direct.intent(config, (seen,), request(market))
    deciding, step = planning(connector, config, state, intent, seen)
    ready = observation(connector, config, state.owned[0], market, allowance=AMOUNT)
    swap = step(ready)
    bound = config.target(ALLOCATION)
    elsewhere = bound.pool.model_copy(update={"pool": "0x" + "cd" * 20})
    other = bound.model_copy(update={"pool": elsewhere})
    execution = connector_from_wire({"connector": connector}).execution
    # The bound request cannot carry an operation of another pool.
    foreign = swap.model_copy(update={"target": other})
    with pytest.raises(ValueError, match="operation and observation require the bound target"):
        execution.validate_operation(None, deciding, foreign, ready)
    # A request for another pool has no capability at all.
    deciding.decision = SimpleNamespace(
        intent=intent.model_copy(update={"target": other}), observations=(seen,)
    )
    with pytest.raises(ValueError, match="no exact retained target capability"):
        execution.validate_operation(None, deciding, foreign, ready)


def settled(connector, before_allowance, after_allowance):
    """The family's own balance reconciliation over a mined swap of AMOUNT."""
    market, config, state, _ = admitted(connector)
    owned, target = state.owned[0], config.target(ALLOCATION)
    before = observation(connector, config, owned, market, allowance=before_allowance)
    swapped = owned.model_copy(
        update={
            "inventory0": AssetAmount(asset=market.pool.currency0, amount=0),
            "inventory1": AssetAmount(asset=market.pool.currency1, amount=QUOTED),
            "entry_complete": True,
        }
    )
    after = observation(
        connector,
        config,
        swapped,
        market,
        wallet=5000 - AMOUNT,
        bought=QUOTED,
        allowance=after_allowance,
    )
    operations = import_module(f"almanak.connectors.{connector}.operations")
    operation = operations.Swap(
        target=target,
        amount=AssetAmount(asset=market.pool.currency0, amount=AMOUNT),
        minimum=AssetAmount(asset=market.pool.currency1, amount=1),
        deadline=market.block.timestamp + 300,
    )
    pending = SimpleNamespace(before=before, operation=operation)
    effects = SimpleNamespace(delta0=-AMOUNT, delta1=QUOTED)
    return v3_pool_reconciliation.validate_balances(pending, after, effects, 0, True)


def test_a_swap_spends_an_exact_router_allowance_and_never_a_standing_unlimited_one(connector):
    market, config, state, seen = admitted(connector)
    intent = v3_pool_direct.intent(config, (seen,), request(market))
    _, step = planning(connector, config, state, intent, seen)
    owned = state.owned[0]
    # An unlimited allowance differs from the amount: it is reset, then granted exactly.
    unlimited = observation(connector, config, owned, market, allowance=2**256 - 1)
    assert (step(unlimited).kind, step(unlimited).amount) == ("approve", 0)
    assert (step(seen).kind, step(seen).amount) == ("approve", AMOUNT)
    # One that already equals the amount is spent as it stands, and falls to zero.
    equal = observation(connector, config, owned, market, allowance=AMOUNT)
    assert step(equal).kind == "swap"
    assert settled(connector, AMOUNT, 0) == ()
    # The reconciliation admits no allowance that did not fall by the amount sold.
    for stood in ((AMOUNT, AMOUNT), (2**256 - 1, 2**256 - 1), (AMOUNT * 2, AMOUNT + 1)):
        with pytest.raises(DataError, match="allowance differs from the executed settlement"):
            settled(connector, *stood)


def released_state(connector):
    """A deployment whose signed swap was released past its deadline, nothing else done."""
    state = created(connector, operator_config(connector, resolved(connector)))
    swap = SimpleNamespace(operation=SimpleNamespace(kind="swap"), tx_hash="0x" + "ab" * 32)
    return SimpleNamespace(
        config=state.config,
        owned=state.owned,
        baseline=(),
        outcomes=(),
        pending=None,
        released=(swap,),
    )


def test_a_signed_step_released_as_expired_is_reported_and_is_not_a_success(
    connector, monkeypatch, capsys
):
    state = released_state(connector)
    result = ax_actions.executed_result(state, state.outcomes, None)
    released = [{"operation": "swap", "transaction": "0x" + "ab" * 32}]
    assert result["released_expired"] == released
    # A step was signed and sent, so the command did execute; the request did not finish.
    assert (result["executed"], result["completed"], "refused" in result) == (True, False, False)
    # A deployment that released nothing says nothing of it.
    state.released = ()
    assert "released_expired" not in ax_actions.executed_result(state, state.outcomes, None)

    # `ax recover` frees the wallet, and still does not report success for the swap.
    args = SimpleNamespace(record=Path("record"), env_file=None, public_rpc=None)
    resolved_record = {"resolved": True, "discarded_request": False}
    monkeypatch.setattr(ax_actions, "recover", lambda record, backend_for: dict(resolved_record))
    assert ax_actions.recover_command(args) == 0
    resolved_record["released_expired"] = released
    capsys.readouterr()
    assert ax_actions.recover_command(args) == 1
    assert json.loads(capsys.readouterr().out)["released_expired"] == released
