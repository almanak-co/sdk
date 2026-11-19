"""`almanak ax swap` for Curve: a direct exact-input swap between two named coins of one pool.

Component evidence without signing or chain access. Pools are resolved from the retained
Curve resolution reads, and observations are built over retained pool observations: a
two-coin StableSwap NG pool and a three-coin TriCrypto NG pool. The fork rehearsal of the
whole command is tests/fork/test_ax_swap_curve.py.
"""

import gzip
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import ax_actions
from almanak.asset_amounts import AssetAmounts
from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.curve import direct as curve_direct
from almanak.connectors.curve.observations import Snapshot
from almanak.connectors.curve.positions import Position, Quote
from almanak.connectors.curve.state import Checkpoint
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from almanak.connectors.curve.validation import validate_current
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import AssetAmount, Block
from almanak.operator_deployment import supports_operator
from tests.curve_retired import current_json
from tests.retained_fixtures import retained
from tests.test_curve_tricrypto_transitions import CASES as TRICRYPTO_CASES

FIXTURES = Path(__file__).with_name("fixtures")
RESOLUTION_READS = FIXTURES / "curve-ethereum-resolution-reads.json.gz"
# The pool each family's retained resolution reads resolve, and its coin count.
RESOLVED_POOLS = {
    "stableswap_ng": ("0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72", 2),
    "twocrypto_ng": ("0xa19bf6fbf05624282cb6ed498f4761f22e084edd", 2),
    "tricrypto_ng": ("0x7f86bf177dd4f3494b841a37e810a34dd56c829b", 3),
}
NATIVE = retained("2026-09-07-rollout") / "curve-native-stable-initial"
# SHA-256 of each retained strategy checkpoint's JSON as the current models write it,
# taken before the Curve checkpoint learned of operator deployments.
STRATEGY_WIRE = {
    "curve_ethereum_stable_trading-eoa-a423f094f6504809acb082e1ab594e98": (
        "fe2a4c06ad8a801a7817c9adc810c9292e3965d688c766073d5ae9fd81a3a39f"
    ),
    "curve_ethereum_stable_trading-59aac7cb3095429f903130fa3db694c6": (
        "65f1d948d07b0923b3e356a16efdd2512437fc1e057cf0aec59456ebc4c338c6"
    ),
    "curve_ethereum_stable_lp-eoa-0a20470d1ecf49a58a74b2b37330fd58": (
        "49a4bd299e15d0796f36b69b69b99e41bbdbc4013c2249c6b6282a2ede66983e"
    ),
    "curve_ethereum_stable_lp-eb8f261caa464d7d890dd3c147cdb6a5": (
        "706276e84a9be7dc84019f79290159ce3f419030ff5a5142456593881e596055"
    ),
}
TRADING = "curve_ethereum_stable_trading-eoa-a423f094f6504809acb082e1ab594e98"
WALLET = Wallet(account="fund", address="0x" + "12" * 20)
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=7, max_transaction_fee=9)
AMOUNT, QUOTED, WINDOW = 1000, 4321, 300


def strategy_checkpoint(name=TRADING):
    """A retained strategy deployment, read as the current models read it."""
    raw = gzip.decompress((NATIVE / name / "checkpoint.json.gz").read_bytes())
    return Checkpoint.model_validate_json(current_json(raw))


@dataclass(frozen=True)
class Market:
    """A retained pool observation and the pair a swap in it states."""

    observed: object
    sell: int
    buy: int

    @property
    def pool(self):
        return self.observed.source

    @property
    def block(self):
        return self.observed.block

    @property
    def coins(self):
        return tuple(str(coin) for coin in self.pool.coins)

    @property
    def sold(self):
        return self.pool.coins[self.sell]

    @property
    def bought(self):
        return self.pool.coins[self.buy]

    @property
    def others(self):
        return [coin for coin in self.pool.coins if coin not in (self.sold, self.bought)]


def on_mainnet(value):
    """The same retained facts under the mainnet environment a production deployment names.

    The captures were taken on identified forks, whose environment a production
    configuration refuses.
    """
    if isinstance(value, list):
        return [on_mainnet(item) for item in value]
    if not isinstance(value, dict):
        return value
    moved = {name: on_mainnet(item) for name, item in value.items()}
    return {**moved, "environment": "mainnet"} if "environment" in moved else moved


def markets():
    stable = strategy_checkpoint().outcomes[0].before.pool
    stable = type(stable).model_validate_json(
        json.dumps(on_mainnet(stable.model_dump(mode="json")))
    )
    three = TriCryptoObservation.model_validate_json(
        json.dumps(on_mainnet(TRICRYPTO_CASES[0]["observation"]))
    )
    # The three-coin swap sells the last coin for the first and leaves the middle one.
    return {"stableswap_ng": Market(stable, 1, 0), "tricrypto_ng": Market(three, 2, 0)}


MARKETS = markets()


@pytest.fixture(params=tuple(MARKETS))
def market(request):
    return MARKETS[request.param]


class Shares:
    """A reader that answers the wallet's LP balance in the pool and nothing else."""

    def __init__(self, held=0):
        self.held, self.asked = held, []

    def call(self, to, signature, block, *, inputs=(), args=()):
        assert (signature, tuple(inputs)) == ("balanceOf(address)", ("address",))
        self.asked.append((to, tuple(args), block))
        return (self.held,)


def request(market, *, amount=AMOUNT, slippage_bps=50, **stated):
    pair = {"sell": str(market.sold), "buy": str(market.bought), **stated}
    return ActionRequest("swap", amount, slippage_bps=slippage_bps, deadline_seconds=WINDOW, **pair)


def positions(market, stated=None, *, held=0):
    resolved = SimpleNamespace(pool=market.pool)
    stated = request(market) if stated is None else stated
    return curve_direct.positions(Shares(held), market.block, resolved, WALLET, stated)


def configuration(market, document):
    chain = market.pool.chain
    provider = ax_actions.alchemy(chain.chain_id)
    return ax_actions.configuration("curve", document, WALLET, provider, chain, LIMITS)


def operator_config(market, **stated):
    return configuration(market, positions(market, request(market, **stated)))


def created(config, strategy=None):
    found = connector_from_wire({"connector": "curve"})
    capabilities = found.codec.capability.declared(config)
    return found.lifecycle.initial(config, strategy, capabilities, None)


def amounts(target, held):
    return AssetAmounts(
        tuple(AssetAmount(asset=coin, amount=held.get(coin, 0)) for coin in target.assets)
    )


def observation(market, config, owned, *, wallet=5000, allowance=0, **fields):
    """An on-chain observation of the deployment's one position at the retained block."""
    target = config.target(ALLOCATION)
    return Snapshot(
        target=target,
        pool=market.observed,
        owned=owned,
        wallets=amounts(target, {market.sold: wallet}),
        allowances=tuple(
            {
                "asset": coin,
                "spender": target.pool.pool,
                "amount": allowance if coin == market.sold else 0,
            }
            for coin in target.assets
        ),
        native_balance=AssetAmount(
            asset=evm_network(market.pool.chain.chain_id).native_asset, amount=10**18
        ),
        payer=target.fund,
        **{"position": None, **fields},
    )


def quote(market, owned, amount=AMOUNT, output=QUOTED, *, bought=None):
    return Quote(
        source=owned.target,
        block=market.block,
        amount_in=AssetAmount(asset=market.sold, amount=amount),
        amount_out=AssetAmount(asset=market.bought if bought is None else bought, amount=output),
        calculation="contract_pool",
    )


def admitted(market, **observed):
    """A deployment whose request is being admitted with one observation."""
    config = operator_config(market)
    state = created(config)
    owned = state.owned[0]
    seen = observation(market, config, owned, entry_quote=quote(market, owned), **observed)
    return config, state, seen


def retained_reads():
    raw = json.loads(gzip.decompress(RESOLUTION_READS.read_bytes()))
    block = Block.model_validate_json(json.dumps(raw["block"]))
    rows = tuple(ResolutionRead.model_validate_json(json.dumps(row)) for row in raw["evidence"])
    return block, rows


def test_curve_supports_operator_deployments_and_serves_the_swap_on_every_declared_chain():
    found = connector_from_wire({"connector": "curve"})
    assert supports_operator(found.codec.checkpoint)
    assert direct.binding("curve").actions == ("swap",) and direct.binding("curve").names_bought
    assert "curve" in direct.connectors_for("swap") and "curve" not in direct.NOT_DIRECT
    cells = [cell for cell in found.chain_support if cell.direct_actions]
    assert cells and all(
        [str(action) for action in cell.direct_actions] == ["swap_exact_input"] for cell in cells
    )
    for cell in cells:
        direct.require_direct("curve", cell.chain, direct.ActionRequest("swap", 5))
    with pytest.raises(ValueError, match="curve serves no direct swap on eip155:31337"):
        direct.require_direct("curve", ChainId("eip155:31337"), direct.ActionRequest("swap", 5))
    with pytest.raises(ValueError, match="curve serves no direct supply"):
        direct.require_direct("curve", cells[0].chain, direct.ActionRequest("supply", 5))


def test_the_swap_is_the_only_action_an_operator_request_is_admitted_for():
    cells = connector_from_wire({"connector": "curve"}).chain_support
    others = [
        (cell, action) for cell in cells for action in cell.actions if action != "swap_exact_input"
    ]
    # Liquidity, exits and revocations stay strategy actions on every chain.
    assert {str(action) for _, action in others} >= {"open_liquidity", "exit_exchange"}
    for cell, action in others:
        with pytest.raises(ValueError, match="unsupported as a direct action"):
            cell.require_direct(action)


@pytest.mark.parametrize("name", tuple(STRATEGY_WIRE))
def test_a_strategy_checkpoint_keeps_its_wire_form(name):
    state = strategy_checkpoint(name)
    body = state.model_dump_json()
    assert hashlib.sha256(body.encode()).hexdigest() == STRATEGY_WIRE[name]
    assert "operator" not in json.loads(body) and state.operator is False
    assert state.outcomes and state.strategy is not None
    restored = Checkpoint.model_validate_json(body)
    assert restored == state and restored.model_dump_json() == body


def test_an_operator_checkpoint_retains_no_strategy_and_round_trips(market):
    state = created(operator_config(market))
    assert (state.operator, state.strategy, state.initialized) == (True, None, False)
    body = state.model_dump_json()
    wire = json.loads(body)
    assert (wire["operator"], wire["strategy"]) == (True, None)
    assert Checkpoint.model_validate_json(body) == state
    # The origin is exclusive: an operator deployment retains no strategy, and a
    # strategy deployment is never without one.
    strategy = strategy_checkpoint().strategy
    with pytest.raises(ValueError, match="an operator deployment retains no strategy"):
        Checkpoint.model_validate_json(json.dumps({**wire, "strategy": strategy.model_dump()}))
    with pytest.raises(ValueError, match="a strategy deployment retains its strategy"):
        Checkpoint.model_validate_json(json.dumps({**wire, "operator": False}))
    report = connector_from_wire({"connector": "curve"}).projection.report(state)
    assert report["strategy"] is None and report["connector"] == "curve"
    assert report["positions"][0]["shares"]["amount"] is None


def test_the_deployment_is_one_trading_allocation_funded_with_the_amount_sold(market):
    config = operator_config(market, amount=77, slippage_bps=25)
    (allocation,) = config.allocations
    assert (allocation.name, allocation.account, allocation.pool) == (
        ALLOCATION,
        WALLET.account,
        market.pool,
    )
    assert (allocation.funding_asset, allocation.trading_asset) == (market.sold, market.bought)
    # Trading only: no liquidity authority and no single-coin exit to remove shares by.
    assert (allocation.liquidity_enabled, allocation.single_coin_exit) == (False, None)
    # The whole capital is one entry: the stated amount of the sold coin, nothing else.
    capital = {str(value.asset): value.amount for value in allocation.capital.root}
    assert list(capital) == list(market.coins)
    assert capital == {coin: 77 if coin == str(market.sold) else 0 for coin in market.coins}
    assert (allocation.entry_swap_bps, allocation.slippage_bps) == (10000, 25)
    assert (allocation.deadline_seconds, allocation.minimum_native_balance) == (WINDOW, 0)
    target = config.target(ALLOCATION)
    assert target.fund.address == WALLET.address and not target.liquidity_enabled
    assert direct.binding_for(config) is direct.binding("curve")
    state = created(config)
    assert [item.target for item in state.capabilities.requirements] == [target]
    owned = state.owned[0]
    assert owned.position is None
    assert {str(v.asset): v.amount for v in owned.inventory.root} == capital


def test_both_coins_are_named_distinct_and_of_the_pool_and_the_request_complete(market):
    chain = market.pool.chain.chain_id
    with pytest.raises(ValueError, match="--buy must name the coin bought") as missing:
        positions(market, request(market, buy=None))
    assert all(coin in str(missing.value) for coin in market.coins)
    foreign = (str(evm_network(chain).native_asset), f"{chain}/erc20:0x" + "ab" * 20)
    for flag in ("sell", "buy"):
        for stated in foreign:
            with pytest.raises(ValueError, match=f"--{flag} must name one of the pool's coins"):
                positions(market, request(market, **{flag: stated}))
    with pytest.raises(ValueError, match="--sell and --buy must name two different coins"):
        positions(market, request(market, buy=str(market.sold)))
    incomplete = ActionRequest("swap", AMOUNT, sell=str(market.sold), buy=str(market.bought))
    with pytest.raises(ValueError, match="swap requires --amount, --slippage-bps"):
        positions(market, incomplete)


def test_a_wallet_that_holds_the_pools_lp_token_is_refused_before_a_deployment_exists(market):
    reader = Shares(held=5)
    resolved = SimpleNamespace(pool=market.pool)
    with pytest.raises(DataError, match="the wallet holds 5 base units") as refusal:
        curve_direct.positions(reader, market.block, resolved, WALLET, request(market))
    assert refusal.value.kind == "ownership"
    assert str(market.pool.shares_asset) in str(refusal.value)
    # The balance read is the wallet's own, of the pool's LP token, at the resolved block.
    assert reader.asked == [(market.pool.pool, (WALLET.address,), market.block)]
    # A statement that is wrong by itself is refused before any read.
    unread = Shares(held=5)
    with pytest.raises(ValueError, match="--buy must name the coin bought"):
        curve_direct.positions(unread, market.block, resolved, WALLET, request(market, buy=None))
    assert unread.asked == []


def test_the_connector_observes_no_lp_shares_beside_a_trading_allocation(market):
    """Why the wallet is refused: such an observation cannot be built at all."""
    config = operator_config(market)
    owned = created(config).owned[0]
    shares = Position(shares=AssetAmount(asset=market.pool.shares_asset, amount=5))
    with pytest.raises(ValueError, match="liquidity authority"):
        observation(market, config, owned, position=shares)


@pytest.mark.parametrize("family", tuple(RESOLVED_POOLS))
def test_the_command_resolves_the_named_pool_through_the_connectors_resolver(family, monkeypatch):
    block, rows = retained_reads()
    address, count = RESOLVED_POOLS[family]
    asked = []

    class Head(RetainedResolutionReader):
        def pin_block(self, selector):
            return block if selector == "latest" else super().pin_block(selector)

        def call(self, to, signature, at, **options):
            if (to, signature) == (address, "balanceOf(address)"):
                asked.append((options["args"], at))
                return (0,)
            return super().call(to, signature, at, **options)

    monkeypatch.setattr(
        "almanak.gateway.ethereum.EthereumReader", lambda chain, endpoint: Head(chain, rows)
    )
    resolution = ax_actions.resolution("curve")
    selection = resolution.MarketSelection(
        connector="curve", chain=block.chain.chain_id, pool=address
    )
    market = resolution.resolve_market(Head(block.chain, rows), selection, block)
    assert (market.pool.family, len(market.pool.coins)) == (family, count)
    sold, bought = str(market.pool.coins[-1]), str(market.pool.coins[0])
    stated = ActionRequest(
        "swap", AMOUNT, sell=sold, buy=bought, slippage_bps=50, deadline_seconds=WINDOW
    )
    wallet = {"account": WALLET.account, "address": WALLET.address}
    config = ax_actions.deployment(
        "curve",
        selection,
        stated,
        wallet,
        ax_actions.alchemy(block.chain.chain_id),
        "endpoint",
        LIMITS,
    )
    (allocation,) = config.allocations
    assert allocation.pool == market.pool and asked == [((WALLET.address,), block)]
    assert (str(allocation.funding_asset), str(allocation.trading_asset)) == (sold, bought)
    assert [value.amount for value in allocation.capital.root] == [0] * (count - 1) + [AMOUNT]
    assert config.production.gas[0].max_gas_price_wei == 7


def test_adoption_keeps_the_stated_inventory_only_when_the_wallet_covers_it(market):
    config = operator_config(market)
    found = connector_from_wire({"connector": "curve"})
    owned = created(config).owned
    covered = observation(market, config, owned[0], wallet=AMOUNT)
    assert found.lifecycle.adopt(owned, (covered,)) == owned
    short = observation(market, config, owned[0], wallet=AMOUNT - 1)
    with pytest.raises(DataError, match=f"the wallet holds {AMOUNT - 1} base units") as refusal:
        found.lifecycle.adopt(owned, (short,))
    assert refusal.value.kind == "inventory" and str(market.sold) in str(refusal.value)


def test_adoption_never_takes_lp_shares_into_an_operator_deployment(market):
    """An allocation with liquidity authority can observe shares; an operator adopts none."""
    document = positions(market)
    document["allocations"][0]["liquidity_enabled"] = True
    config = configuration(market, document)
    found = connector_from_wire({"connector": "curve"})
    owned = created(config).owned
    shares = Position(shares=AssetAmount(asset=market.pool.shares_asset, amount=5))
    holding = observation(market, config, owned[0], position=shares)
    with pytest.raises(DataError, match="a direct action adopts no LP shares") as refusal:
        found.lifecycle.adopt(owned, (holding,))
    assert refusal.value.kind == "ownership" and "holds 5 base units" in str(refusal.value)


def test_the_intent_follows_the_quote_of_the_observation_it_is_admitted_with(market):
    config, state, seen = admitted(market)
    intent = curve_direct.intent(config, (seen,), request(market))
    assert intent.kind == "swap_exact_input" and intent.target == config.target(ALLOCATION)
    assert intent.amount == AssetAmount(asset=market.sold, amount=AMOUNT)
    # 50 basis points below the quoted output, rounded down, in the coin bought.
    assert intent.minimum_output == AssetAmount(asset=market.bought, amount=QUOTED * 9950 // 10000)
    assert intent.deadline == market.block.timestamp + WINDOW
    found = connector_from_wire({"connector": "curve"})
    found.codec.capability.admit(state.capabilities, intent)
    assert found.lifecycle.increases_exposure(intent)


def test_a_missing_or_differently_sized_or_paired_quote_builds_no_intent(market):
    config, state, seen = admitted(market)
    owned = state.owned[0]
    bare = observation(market, config, owned)
    with pytest.raises(ValueError, match="the pool quotes no output for the stated amount"):
        curve_direct.intent(config, (bare,), request(market))
    unmeasured = observation(
        market, config, owned, entry_quote_error="entry quote unavailable: reverted"
    )
    with pytest.raises(ValueError, match="entry quote unavailable: reverted"):
        curve_direct.intent(config, (unmeasured,), request(market))
    others = [request(market, amount=AMOUNT + 1), request(market, sell="other")]
    others += [request(market, buy=str(coin)) for coin in market.others]
    for other in others:
        with pytest.raises(ValueError, match="not for the stated amount and pair of coins"):
            curve_direct.intent(config, (seen,), other)


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


def test_the_gateway_admits_the_exact_approval_to_the_pool_then_the_swap_and_nothing_larger(
    market,
):
    config, state, seen = admitted(market)
    intent = curve_direct.intent(config, (seen,), request(market))
    deciding, step = planning(config, state, intent, seen)
    owned, pool = state.owned[0], market.pool.pool

    approval = step(seen)
    assert (approval.kind, approval.asset, approval.spender, approval.amount) == (
        "approve",
        market.sold,
        pool,
        AMOUNT,
    )
    # A different allowance is reset before the exact one is granted.
    stale = observation(market, config, owned, allowance=AMOUNT + 5)
    assert (step(stale).kind, step(stale).amount) == ("approve", 0)
    approved = observation(market, config, owned, allowance=AMOUNT)
    swap = step(approved)
    assert (swap.kind, swap.amount, swap.minimum) == ("swap", intent.amount, intent.minimum_output)
    execution = connector_from_wire({"connector": "curve"}).execution
    # The approval goes to the sold coin and names the pool; the swap goes to the pool.
    calls = [execution.call(config, operation).to for operation in (approval, swap)]
    assert calls == [market.sold.reference, pool]

    # Nothing sells more than the stated amount: inventory is that amount and no more.
    larger = intent.model_copy(update={"amount": AssetAmount(asset=market.sold, amount=AMOUNT + 1)})
    deciding.decision = SimpleNamespace(intent=larger, observations=(seen,))
    with pytest.raises(DataError, match="exceeds attributed input inventory"):
        execution.validate_operation(
            None, deciding, execution.operation(config, deciding, larger, approved), approved
        )
    # A minimum below the stated slippage is refused against the retained quote.
    wider = intent.model_copy(update={"minimum_output": AssetAmount(asset=market.bought, amount=1)})
    deciding.decision = SimpleNamespace(intent=wider, observations=(seen,))
    with pytest.raises(ValueError, match="widens the configured slippage limit"):
        execution.validate_operation(
            None, deciding, execution.operation(config, deciding, wider, approved), approved
        )


def test_the_stated_seconds_bound_signing_and_nothing_after_it(market):
    """No Curve call carries a deadline: the window refuses late planning and signing,
    and signed bytes stay sendable after it."""
    config, state, seen = admitted(market)
    intent = curve_direct.intent(config, (seen,), request(market))
    owned = state.owned[0]
    approved = observation(market, config, owned, allowance=AMOUNT)

    def elapsed(seconds):
        """The request as it stands `seconds` after the block it was observed at.

        A retained pool observation is valid only at its own block time, so the request
        is moved back instead of the observation forward.
        """
        aged = intent.model_copy(update={"deadline": intent.deadline - seconds})
        return planning(config, state, aged, seen)

    deciding, step = elapsed(WINDOW - 1)
    swap = step(approved)
    assert (swap.kind, swap.deadline) == ("swap", approved.block.timestamp + 1)
    with pytest.raises(DataError, match="retained deadline expired") as refusal:
        elapsed(WINDOW)[1](approved)
    assert refusal.value.kind == "stale_plan"

    # The swap's calldata is the pool's `exchange` of the coin indexes, the amount and the
    # minimum output, paid to the wallet: the deadline is in none of its words.
    found = connector_from_wire({"connector": "curve"})
    data = found.execution.call(config, swap).data
    words = {int(data[10:][index : index + 64], 16) for index in range(0, len(data) - 10, 64)}
    assert {AMOUNT, intent.minimum_output.amount, market.sell, market.buy} <= words
    assert swap.deadline not in words

    # Signed in time, the same swap is still valid a thousand windows later.
    late, _ = elapsed(1000 * WINDOW)
    expired = swap.model_copy(update={"deadline": late.decision.intent.deadline})
    backend = SimpleNamespace(config=config)
    unsigned = SimpleNamespace(operation=expired, before=approved, decision=late.decision)
    with pytest.raises(DataError, match="retained deadline expired"):
        validate_current(backend, unsigned, approved)
    validate_current(backend, SimpleNamespace(**vars(unsigned), raw_transaction="0x01"), approved)
    # The connector declares no expiry, so the gateway never releases a signed swap for
    # its age and never reports one as past a protocol deadline.
    assert not hasattr(found.execution, "expired")
    assert EvmGateway.enforces_deadline(SimpleNamespace(connector=found), swap) is False


def test_the_position_is_the_attributed_inventory_and_the_allowances_last_observed(market):
    config, state, seen = admitted(market)
    report = direct.binding("curve").position
    before = [
        {"asset": coin, "amount": str(AMOUNT if coin == str(market.sold) else 0)}
        for coin in market.coins
    ]
    assert report(state) == {"inventory": before, "allowances": []}
    started = state.model_copy(update={"initialized": True, "baseline": (seen,)})
    assert report(started) == {"inventory": before, "allowances": []}

    # After a mined approval whose swap never followed, the allowance is reported.
    owned = state.owned[0]
    approved = observation(market, config, owned, allowance=AMOUNT)
    stopped = SimpleNamespace(
        owned=state.owned, baseline=(seen,), outcomes=(SimpleNamespace(after=approved),)
    )
    assert report(stopped)["allowances"] == [
        {"asset": str(market.sold), "spender": market.pool.pool, "amount": str(AMOUNT)}
    ]

    swapped = owned.model_copy(update={"inventory": amounts(owned.target, {market.bought: QUOTED})})
    done = SimpleNamespace(
        owned=(swapped,), baseline=(seen,), outcomes=(SimpleNamespace(after=seen),)
    )
    after = [
        {"asset": coin, "amount": str(QUOTED if coin == str(market.bought) else 0)}
        for coin in market.coins
    ]
    assert report(done) == {"inventory": after, "allowances": []}


USDC = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
CRVUSD = "eip155:1/erc20:0x4c9edd5852cd905f086c759e8383e09bff1e68b3"


def arguments(*extra, connector="curve", without=()):
    stated = {
        "--connector": connector,
        "--chain": "eip155:1",
        "--pool": RESOLVED_POOLS["stableswap_ng"][0],
        "--sell": USDC,
        "--buy": CRVUSD,
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


def test_the_command_line_states_both_coins_and_the_bounds():
    args = arguments()
    assert (args.product, args.ax_command, args.execute) == ("operator-action", "swap", False)
    assert (args.sell, args.buy, args.amount, args.slippage_bps) == (USDC, CRVUSD, 1000, 50)
    assert ax_actions.action_request(args) == ActionRequest(
        "swap", 1000, sell=USDC, buy=CRVUSD, slippage_bps=50, deadline_seconds=WINDOW
    )
    assert arguments("--deadline-seconds", "60").deadline_seconds == 60
    market = ax_actions.market_request(args)
    assert (market.connector, str(market.chain), market.pool) == (
        "curve",
        "eip155:1",
        RESOLVED_POOLS["stableswap_ng"][0],
    )
    assert ax_actions.action_request(arguments(without=("--buy",))).buy is None


def test_the_help_never_calls_the_seconds_an_on_chain_bound_for_curve(capsys):
    with pytest.raises(SystemExit):
        parse_validated(build_parser(), ["ax", "swap", "--help"])
    text = " ".join(capsys.readouterr().out.split())
    assert "a curve call carries no deadline" in text
    assert "stays valid until it is mined or its nonce is used" in text
    assert "required for curve" in text


def command_reaching(monkeypatch):
    """Let the command run up to the deployment and record what it would deploy."""
    seen = {}

    def deployment(name, market, action, wallet, provider, endpoint, limits, **options):
        seen.update(connector=name, market=market, action=action, limits=limits)
        raise StopIteration

    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
    monkeypatch.setattr(ax_actions, "deployment", deployment)
    return seen


def test_the_command_passes_the_stated_pair_on_and_refuses_an_undeclared_chain(monkeypatch):
    seen = command_reaching(monkeypatch)
    with pytest.raises(StopIteration):
        ax_actions.command(arguments())
    assert (seen["connector"], seen["market"].pool) == ("curve", RESOLVED_POOLS["stableswap_ng"][0])
    assert (seen["action"].sell, seen["action"].buy, seen["action"].amount) == (USDC, CRVUSD, 1000)

    monkeypatch.setattr(ax_actions, "read_wallet", pytest.fail)
    with pytest.raises(ValueError, match="curve serves no direct swap on eip155:31337"):
        ax_actions.command(arguments("--chain", "eip155:31337"))


def test_the_bought_coin_is_required_for_curve_and_refused_where_the_pool_decides_it(
    monkeypatch,
):
    # Both refusals come before the wallet or any endpoint is read.
    monkeypatch.setattr(ax_actions, "read_wallet", pytest.fail)
    monkeypatch.setattr(ax_actions, "transport", pytest.fail)
    with pytest.raises(ValueError, match="curve pools can hold more than two coins: --buy"):
        ax_actions.command(arguments(without=("--buy",)))
    for connector in ("uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream"):
        cell = next(
            cell
            for cell in connector_from_wire({"connector": connector}).chain_support
            if cell.direct_actions
        )
        stated = arguments("--chain", str(cell.chain), connector=connector)
        with pytest.raises(ValueError, match=f"{connector} pools hold two currencies.*--buy"):
            ax_actions.command(stated)
        # Without the flag the same command goes on to its deployment.
        direct.require_bought(connector, ax_actions.action_request(arguments(without=("--buy",))))
    direct.require_bought("curve", ax_actions.action_request(arguments()))
    # The other commands state no pair.
    direct.require_bought("aave_v4", ActionRequest("supply", 5))
