"""`almanak ax swap` in one named Aerodrome stable or volatile pool.

Component evidence without signing or chain access: each pool is resolved from the
retained resolution reads, and observations are built over that pool. The retained
checkpoints of the reference lifecycles show that a strategy checkpoint keeps its wire
form. The fork rehearsal of the whole command is tests/fork/test_ax_swap_aerodrome.py.
"""

import contextlib
import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak import ax_actions
from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct
from almanak.connectors.aerodrome import direct as aerodrome_direct
from almanak.connectors.aerodrome import execution, observations, reconciliation, validation
from almanak.connectors.aerodrome import operations as ops
from almanak.connectors.aerodrome.domain import PoolObservation
from almanak.connectors.aerodrome.observations import Snapshot
from almanak.connectors.aerodrome.positions import Position, Quote
from almanak.connectors.aerodrome.receipts import Effects
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
from almanak.gateway.accounts import prepare_call
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import AssetAmount, Block
from almanak.operator_deployment import supports_operator
from almanak.strategy import StrategyDefinition
from tests.exchange_scope import without_exchange_budgets
from tests.test_aerodrome_native import CASES, EVIDENCE

CONNECTOR = "aerodrome"
BASE = ChainId("eip155:8453")
READS = Path(__file__).with_name("fixtures") / "aerodrome-base-resolution-reads.json.gz"
# The pool of each invariant that the retained resolution reads cover.
POOLS = {
    "stable": "0x27a8afa3bd49406e48a074350fb7b2020c43b2bd",
    "volatile": "0xcdac0d6c6c59727a65f871236188350531885c43",
}
# SHA-256 of a strategy checkpoint's JSON, taken before this checkpoint learned of
# operator deployments. The checkpoint is `strategy_checkpoint` below.
STRATEGY_WIRE = {
    "stable": "a8baa112c6b80623a6f91e6a4700e0b9fbd71017f3149cf86bfc7f0fb863a2dd",
    "volatile": "7666e868e940863eafc5f9d0dd2c8171bfbcee48c3af9942ddf7752c076227f2",
}
# The same for every retained checkpoint of a closed reference lifecycle, EOA and Safe,
# trading and liquidity: SHA-256 of the JSON the checkpoint model wrote for it before
# the model learned of operator deployments.
RETAINED_WIRE = {
    "aerodrome_base_stable_lp-eoa-eoa": (
        "3df3a300ed1fb8ecfba90098f78f6318f0723c01b4e7d2f2646da940910fe6c6"
    ),
    "aerodrome_base_stable_trading-eoa-eoa": (
        "7eda8fad8883420fcc55d1fcee7378f95230150b72421325aa75c4ba55acb831"
    ),
    "aerodrome_base_volatile_lp-eoa-eoa": (
        "6e3c143b096d0f81d907a9d7eacd3d90a576e93a6ac344c3fdb35ef441bc611d"
    ),
    "aerodrome_base_volatile_trading-eoa-eoa": (
        "0f399643351fbd21ece3d10ab017829fcf2acc8585640d2cd843bb86d1ccb267"
    ),
    "aerodrome_base_stable_lp-safe-configured": (
        "33d7413f2eb619f8cc9e45e09c02f10dd1b0171330e8325fc50768885a3b6319"
    ),
    "aerodrome_base_stable_lp-safe-distinct": (
        "854c4057c3aa90aaf431f55eb21b7fb2d650ada9311f15fe5a7cec926f86dacc"
    ),
    "aerodrome_base_stable_trading-safe-configured": (
        "1fdc2347f981a8c0aaa1bd680e1ee94a4b828ee3d4f96fb0071edb752ffd5ff6"
    ),
    "aerodrome_base_stable_trading-safe-distinct": (
        "a70753286515e8bf732bc0cd2d68236304f21fd592e974c7fae1f445b5dd21f1"
    ),
    "aerodrome_base_volatile_lp-safe-distinct": (
        "422c521bcc8f4e09a57bcc28055f6108c6879629d0d7515692801e62b1d9142e"
    ),
    "aerodrome_base_volatile_lp-safe-configured": (
        "3146f2d578fcbaa03d9bc7374e6113682318222f64fc02e3aa96aa37ec5a039a"
    ),
    "aerodrome_base_volatile_trading-safe-distinct": (
        "93defecd9ab2383da4814dcd9a03f2dcb98f97cbe6eb3de7682a92292667d5d4"
    ),
    "aerodrome_base_volatile_trading-safe-configured": (
        "4d6591ce5bd2025fec26455ca1bdb3b8d2160ad57a68a6b0f46a6014059c6217"
    ),
}
WALLET = Wallet(account="fund", address="0x" + "12" * 20)
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=7, max_transaction_fee=9)
AMOUNT, QUOTED = 1000, 4321
STRATEGY = StrategyDefinition(filename="strategy.py", source="def decide(context):\n    pass\n")

pytestmark = pytest.mark.parametrize("variant", tuple(POOLS))


def retained():
    raw = json.loads(gzip.decompress(READS.read_bytes()))
    block = Block.model_validate_json(json.dumps(raw["block"]))
    rows = tuple(ResolutionRead.model_validate_json(json.dumps(row)) for row in raw["evidence"])
    return block, rows


def selection(variant):
    model = ax_actions.resolution(CONNECTOR).MarketSelection
    return model(connector=CONNECTOR, chain=BASE, pool=POOLS[variant])


def resolved(variant):
    block, rows = retained()
    resolve = ax_actions.resolution(CONNECTOR).resolve_market
    return resolve(RetainedResolutionReader(block.chain, rows), selection(variant), block)


def request(market, *, sell=None, amount=AMOUNT, slippage_bps=50):
    return ActionRequest(
        "swap",
        amount,
        sell=str(market.pool.currency0) if sell is None else sell,
        slippage_bps=slippage_bps,
        deadline_seconds=300,
    )


def configuration(positions, market):
    chain = market.pool.chain
    provider = ax_actions.alchemy(chain.chain_id)
    return ax_actions.configuration(CONNECTOR, positions, WALLET, provider, chain, LIMITS)


def operator_config(market, **stated):
    positions = aerodrome_direct.positions(None, None, market, WALLET, request(market, **stated))
    return configuration(positions, market)


def observation(
    config,
    owned,
    market,
    *,
    wallet=5000,
    received=0,
    native=10**18,
    allowance=0,
    shares_allowance=0,
    deposited=0,
    **more,
):
    """An on-chain observation of the deployment's one position at the resolved block."""
    target = config.target(ALLOCATION)
    pool = PoolObservation(
        source=market.pool,
        block=market.block,
        currency0=market.asset0,
        currency1=market.asset1,
        shares=market.share,
        reserve0=10**12,
        reserve1=10**12,
        balance0=10**12,
        balance1=10**12,
        deposited_shares=deposited,
        total_supply=10**18,
        fee_bps=5,
        index0=7,
        index1=9,
        pool_fees="0x" + "a1" * 20,
        paused=False,
    )
    granted = {target.assets[0]: allowance, target.pool.shares_asset: shares_allowance}
    return Snapshot(
        target=target,
        pool=pool,
        owned=owned,
        wallet0=AssetAmount(asset=target.assets[0], amount=wallet),
        wallet1=AssetAmount(asset=target.assets[1], amount=received),
        allowances=tuple(
            {"asset": asset, "spender": target.contracts.router, "amount": granted.get(asset, 0)}
            for asset in (*target.assets, target.pool.shares_asset)
        ),
        native_balance=AssetAmount(asset=evm_network(BASE).native_asset, amount=native),
        payer=target.fund,
        position=None,
        **more,
    )


def quote(owned, market, amount=AMOUNT, output=QUOTED):
    return Quote(
        source=owned.target,
        block=market.block,
        amount_in=AssetAmount(asset=market.pool.currency0, amount=amount),
        amount_out=AssetAmount(asset=market.pool.currency1, amount=output),
        calculation="contract_router",
    )


def created(config, strategy=None):
    found = connector_from_wire({"connector": CONNECTOR})
    capabilities = found.codec.capability.declared(config)
    return found.lifecycle.initial(config, strategy, capabilities, None)


def allocation_document(market, **changes):
    values = {
        "name": "position",
        "account": "fund",
        "pool": market.pool.model_dump(mode="json"),
        "contracts": market.contracts.model_dump(mode="json"),
        "liquidity_enabled": False,
        "capital": {"asset": str(market.pool.currency0), "amount": AMOUNT},
        "entry_swap_bps": 10000,
        "slippage_bps": 50,
        "deadline_seconds": 300,
        "native_asset": str(evm_network(BASE).native_asset),
        "minimum_native_balance": 0,
    }
    return {"allocations": [{**values, **changes}]}


def strategy_checkpoint(variant):
    """A strategy deployment after its start, built from the retained resolution only."""
    market = resolved(variant)
    config = configuration(allocation_document(market), market)
    state = created(config, STRATEGY)
    seen = observation(config, state.owned[0], market)
    return state.model_copy(
        update={"deployment_id": "0" * 32, "initialized": True, "baseline": (seen,)}
    )


def test_the_connector_supports_operator_deployments_and_serves_the_swap(variant):
    found = connector_from_wire({"connector": CONNECTOR})
    assert supports_operator(found.codec.checkpoint)
    assert "swap" in direct.binding(CONNECTOR).actions
    assert CONNECTOR in direct.connectors_for("swap") and CONNECTOR not in direct.NOT_DIRECT
    (cell,) = [cell for cell in found.chain_support if cell.direct_actions]
    assert "swap_exact_input" in [str(action) for action in cell.direct_actions]
    # The one production cell serves the swap in the pool's own invariant.
    assert cell.chain == BASE and resolved(variant).pool.variant in cell.variants
    direct.require_direct(CONNECTOR, BASE, direct.ActionRequest("swap", 5))
    with pytest.raises(ValueError, match="aerodrome serves no direct swap on eip155:31337"):
        direct.require_direct(CONNECTOR, ChainId("eip155:31337"), direct.ActionRequest("swap", 5))
    with pytest.raises(ValueError, match="aerodrome serves no direct supply"):
        direct.require_direct(CONNECTOR, BASE, direct.ActionRequest("supply", 5))


def test_an_exit_stays_a_strategy_action(variant):
    (cell,) = connector_from_wire({"connector": CONNECTOR}).chain_support
    undeclared = [action for action in cell.actions if action not in cell.direct_actions]
    assert [str(action) for action in undeclared] == ["exit_exchange"]
    with pytest.raises(ValueError, match="unsupported as a direct action"):
        cell.require_direct(undeclared[0])


def test_a_strategy_checkpoint_keeps_its_wire_form(variant):
    state = strategy_checkpoint(variant)
    body = state.model_dump_json()
    assert hashlib.sha256(body.encode()).hexdigest() == STRATEGY_WIRE[variant]
    assert "operator" not in json.loads(body) and state.operator is False
    restored = type(state).model_validate_json(body)
    assert restored == state and restored.model_dump_json() == body


def test_every_retained_strategy_checkpoint_keeps_its_wire_form(variant):
    cases = [case["name"] for case in CASES if f"_{variant}_" in case["name"]]
    assert sorted(cases) == sorted(name for name in RETAINED_WIRE if f"_{variant}_" in name)
    checkpoint = connector_from_wire({"connector": CONNECTOR}).codec.checkpoint
    for name in cases:
        raw = json.loads(gzip.decompress((EVIDENCE / name / "checkpoint.json.gz").read_bytes()))
        state = checkpoint.model_validate_json(json.dumps(without_exchange_budgets(raw)))
        body = state.model_dump_json()
        assert hashlib.sha256(body.encode()).hexdigest() == RETAINED_WIRE[name], name
        # A lifecycle that ran to its closure, with every outcome it recorded.
        assert (state.operator, state.mode) == (False, "closed") and state.outcomes
        assert "operator" not in json.loads(body) and state.strategy is not None


def test_an_operator_checkpoint_retains_no_strategy_and_round_trips(variant):
    market = resolved(variant)
    state = created(operator_config(market))
    assert (state.operator, state.strategy, state.initialized) == (True, None, False)
    body = state.model_dump_json()
    wire = json.loads(body)
    assert (wire["operator"], wire["strategy"]) == (True, None)
    assert type(state).model_validate_json(body) == state
    # The origin is exclusive: an operator deployment retains no strategy, and a
    # strategy deployment is never without one.
    strategy = strategy_checkpoint(variant).strategy
    with pytest.raises(ValueError, match="an operator deployment retains no strategy"):
        type(state).model_validate_json(json.dumps({**wire, "strategy": strategy.model_dump()}))
    with pytest.raises(ValueError, match="a strategy deployment retains its strategy"):
        type(state).model_validate_json(json.dumps({**wire, "operator": False}))
    report = connector_from_wire({"connector": CONNECTOR}).projection.report(state)
    assert report["strategy"] is None and report["connector"] == CONNECTOR
    (reported,) = report["positions"]
    assert reported["measurement_status"] == "unobserved"


def test_the_deployment_is_one_trading_allocation_funded_with_the_amount_sold(variant):
    market = resolved(variant)
    sold = str(market.pool.currency1)
    config = operator_config(market, sell=sold, amount=77, slippage_bps=25)
    (allocation,) = config.allocations
    assert (allocation.name, allocation.account) == (ALLOCATION, WALLET.account)
    assert (allocation.pool, allocation.contracts) == (market.pool, market.contracts)
    assert allocation.pool.stable is (variant == "stable")
    assert (str(allocation.capital.asset), allocation.capital.amount) == (sold, 77)
    # The whole capital is one entry, and the allocation has no liquidity authority.
    assert (allocation.entry_swap_bps, allocation.liquidity_enabled) == (10000, False)
    assert (allocation.slippage_bps, allocation.deadline_seconds) == (25, 300)
    assert (allocation.minimum_native_balance, allocation.hold_seconds) == (0, None)
    target = config.target(ALLOCATION)
    assert str(target.funding_asset) == sold and target.fund.address == WALLET.address
    assert direct.binding_for(config) is direct.binding(CONNECTOR)
    state = created(config)
    assert [item.target for item in state.capabilities.requirements] == [target]
    owned = state.owned[0]
    held = {str(v.asset): v.amount for v in (owned.inventory0, owned.inventory1)}
    assert held == {sold: 77, str(market.pool.currency0): 0} and owned.position is None


def test_the_sold_asset_must_be_a_pool_currency_and_the_request_complete(variant):
    market = resolved(variant)
    refused = (
        str(evm_network(BASE).native_asset),
        f"{BASE}/erc20:0x" + "ab" * 20,
        # The pool's own share token is not one of its two currencies.
        str(market.pool.shares_asset),
    )
    for sold in refused:
        with pytest.raises(ValueError, match="--sell must name one of the pool's currencies"):
            aerodrome_direct.positions(None, None, market, WALLET, request(market, sell=sold))
    incomplete = ActionRequest("swap", AMOUNT, sell=str(market.pool.currency0))
    with pytest.raises(ValueError, match="swap requires --amount, --slippage-bps"):
        aerodrome_direct.positions(None, None, market, WALLET, incomplete)


def test_the_command_resolves_the_named_pool_through_the_connectors_resolver(variant, monkeypatch):
    block, rows = retained()

    class Head(RetainedResolutionReader):
        def pin_block(self, selector):
            return block if selector == "latest" else super().pin_block(selector)

    monkeypatch.setattr(
        "almanak.gateway.ethereum.EthereumReader", lambda chain, endpoint: Head(chain, rows)
    )
    market = resolved(variant)
    wallet = {"account": WALLET.account, "address": WALLET.address}
    config = ax_actions.deployment(
        CONNECTOR,
        selection(variant),
        request(market),
        wallet,
        ax_actions.alchemy(BASE),
        "endpoint",
        LIMITS,
    )
    assert config == operator_config(market)
    assert config.production.gas[0].max_gas_price_wei == 7


def test_adoption_keeps_the_stated_inventory_only_when_the_wallet_covers_it(variant):
    market = resolved(variant)
    config = operator_config(market)
    found = connector_from_wire({"connector": CONNECTOR})
    owned = created(config).owned
    covered = observation(config, owned[0], market, wallet=AMOUNT)
    assert found.lifecycle.adopt(owned, (covered,)) == owned
    short = observation(config, owned[0], market, wallet=AMOUNT - 1)
    with pytest.raises(DataError, match=f"the wallet holds {AMOUNT - 1} base units") as refusal:
        found.lifecycle.adopt(owned, (short,))
    assert refusal.value.kind == "inventory" and str(market.pool.currency0) in str(refusal.value)


def test_a_swap_adopts_past_standing_allowances_and_a_stray_pool_deposit(variant):
    market = resolved(variant)
    config = operator_config(market)
    found = connector_from_wire({"connector": CONNECTOR})
    owned = created(config).owned
    # A swap never burns, so shares sitting in the pool's own balance do not matter to
    # it; allowances the wallet already gave the router are met by the planning.
    standing = observation(config, owned[0], market, allowance=3, shares_allowance=5, deposited=11)
    assert standing.needs_cleanup and standing.pool.deposited_shares == 11
    assert found.lifecycle.adopt(owned, (standing,)) == owned
    # A strategy deployment still refuses each of them at its own start.
    with pytest.raises(DataError, match="pool already holds LP shares from another transfer"):
        found.lifecycle.validate_initial(SimpleNamespace(owned=owned), (standing,))
    granted = observation(config, owned[0], market, allowance=3)
    with pytest.raises(ValueError, match="cannot adopt pre-existing spending authority"):
        found.lifecycle.validate_initial(SimpleNamespace(owned=owned), (granted,))

    # Liquidity authority without a stated adoption is no operator deployment: its
    # observations would carry quotes.
    liquid = configuration(allocation_document(market, liquidity_enabled=True), market)
    with pytest.raises(ValueError, match="liquidity authority states what it adopts"):
        created(liquid)


class Holdings:
    """A reader that answers only the fund's share balance and stored fee checkpoints."""

    def __init__(self, shares=0, claimable=0):
        self.shares, self.claimable = shares, claimable

    def call(self, to, signature, block, **options):
        if signature == "balanceOf(address)":
            return (self.shares,)
        return (self.claimable if signature.startswith("claimable") else 0,)


def test_a_wallet_holding_pool_shares_or_fee_claims_is_refused_when_observed(variant):
    market = resolved(variant)
    config = operator_config(market)
    target = config.target(ALLOCATION)
    seen = observation(config, created(config).owned[0], market)
    assert observations.position(Holdings(), target, seen.pool) is None
    for held in (Holdings(shares=1), Holdings(claimable=1)):
        with pytest.raises(DataError, match="a trading-only allocation cannot own") as refusal:
            observations.position(held, target, seen.pool)
        assert refusal.value.kind == "ownership" and POOLS[variant] in str(refusal.value)

    # An allocation with liquidity authority reads the same holdings as its position,
    # and the snapshot itself never carries one for a trading allocation.
    liquid = configuration(allocation_document(market, liquidity_enabled=True), market)
    position = observations.position(Holdings(shares=1), liquid.target(ALLOCATION), seen.pool)
    assert isinstance(position, Position) and position.shares.amount == 1
    with pytest.raises(ValueError, match="require the configured pool's liquidity authority"):
        Snapshot.model_validate({**seen.model_dump(), "position": position.model_dump()})


def test_holdings_seen_for_signed_bytes_are_left_out_of_a_trading_observation(variant):
    market = resolved(variant)
    config = operator_config(market)
    target = config.target(ALLOCATION)
    seen = observation(config, created(config).owned[0], market)
    # Nothing the fund holds can recall signed bytes or undo a mined step: the
    # observation carries no position, and the holding is reported by reconciliation.
    for held in (Holdings(), Holdings(shares=1), Holdings(claimable=1)):
        assert observations.position(held, target, seen.pool, signed=True) is None
    # An allocation with liquidity authority reads its position at every block.
    liquid = configuration(allocation_document(market, liquidity_enabled=True), market)
    for signed in (False, True):
        position = observations.position(
            Holdings(shares=1), liquid.target(ALLOCATION), seen.pool, signed=signed
        )
        assert position.shares.amount == 1


def trigger(kind):
    """A decision's trigger, of which these checks read only whose it is."""
    return SimpleNamespace(trigger=SimpleNamespace(kind=kind))


def test_a_plan_is_observed_as_the_rule_for_its_bytes_states(variant, monkeypatch):
    market, config, steps = trading_steps(variant)
    asked, checked = [], []

    def snapshot(backend, name, block, reference, **options):
        asked.append((name, block, reference, options))
        return reference

    monkeypatch.setattr(execution, "snapshot", snapshot)
    monkeypatch.setattr(
        execution, "validate_current", lambda backend, plan, current: checked.append(current)
    )
    backend = SimpleNamespace(config=config, reader=None)
    swap, approved = steps[1]
    # An operator's signed swap is sent past shares, a strategy's signed entry swap is
    # not, and nothing unsigned is.
    cases = ((None, "operator", False), ("0x02", "operator", True), ("0x02", "interval", False))
    for raw, kind, signed in cases:
        plan = SimpleNamespace(
            operation=swap, before=approved, raw_transaction=raw, decision=trigger(kind)
        )
        observe = execution.AerodromeExecution.current_observer(plan)
        assert observe(backend, plan, market.block) is approved
        execution.AerodromeExecution.current_checks(backend, plan, market.block)
        # Before the send the gateway reads the plan's own observation again, and the
        # connector's check its ownership: both as signed bytes, or neither.
        assert asked[-2:] == [
            (ALLOCATION, market.block, approved, {"signed": signed}),
            (ALLOCATION, market.block, approved.owned, {"quotes": False, "signed": signed}),
        ]
    assert checked == [approved.owned] * 3


def test_a_strategy_sends_past_shares_only_what_no_deadline_releases_or_lowers_exposure(variant):
    market, config, steps = trading_steps(variant)
    (approval, _), (entry, _) = steps
    target = config.target(ALLOCATION)
    revocation = approval.model_copy(update={"amount": 0})
    leaving = entry.model_copy(
        update={
            "amount": AssetAmount(asset=target.assets[1], amount=QUOTED),
            "minimum": AssetAmount(asset=target.assets[0], amount=1),
        }
    )

    def sent(operation, kind, raw="0x02"):
        plan = SimpleNamespace(operation=operation, raw_transaction=raw, decision=trigger(kind))
        return validation.sent_past_shares(config, plan)

    # The operator asked for each step; all of its signed bytes are sent.
    assert [sent(op, "operator") for op in (approval, entry, leaving, revocation)] == [True] * 4
    # A strategy's entry swap increases exposure and carries a deadline that releases
    # it: it is held back. Its approval carries none, and its exit lowers exposure.
    for kind in ("interval", "teardown"):
        assert [sent(op, kind) for op in (approval, entry, leaving, revocation)] == [
            True,
            False,
            True,
            True,
        ]
    # A transaction family can have a fee candidate signed next: it is unsigned work.
    family = SimpleNamespace(
        operation=approval, raw_transaction="0x02", decision=trigger("operator"), family=object()
    )
    assert not validation.sent_past_shares(config, family)
    # Nothing unsigned is observed as signed bytes, whoever asked for it.
    for kind in ("operator", "interval"):
        assert not any(sent(op, kind, raw=None) for op in (approval, entry, leaving))
    assert validation.carries_deadline(entry) and not validation.carries_deadline(approval)
    assert validation.expired(entry, SimpleNamespace(timestamp=entry.deadline + 1))
    assert not validation.expired(approval, SimpleNamespace(timestamp=2**64))
    assert validation.deadline_reached(entry, SimpleNamespace(timestamp=entry.deadline))
    assert not validation.deadline_reached(approval, SimpleNamespace(timestamp=2**64))


class Head(Holdings):
    """The chain head as the gateway reads it for one trading fund.

    It answers the token and pool reads of an observation and the simulation of the
    swap; the pool's own state is the retained one (`at_the_head`).
    """

    def __init__(self, target, block, *, shares=0, wallet=5000, allowance=AMOUNT, output=QUOTED):
        super().__init__(shares=shares)
        self.target, self.block, self.chain = target, block, block.chain
        self.wallet, self.allowance, self.output = wallet, allowance, output

    def pin_block(self, tag):
        return self.block

    def retained_pin(self):
        return contextlib.nullcontext()

    def call(self, to, signature, block, **options):
        sold = to == self.target.assets[0].reference
        if signature == "balanceOf(address)" and to != self.target.pool.shares_asset.reference:
            return (self.wallet if sold else 0,)
        if signature.startswith("allowance"):
            return (self.allowance if sold else 0,)
        return super().call(to, signature, block, **options)

    def _rpc(self, method, params, **options):
        if method == "eth_getBalance":
            return hex(10**18)
        assert method == "eth_call"
        return "0x" + encode(("uint256[]",), ([AMOUNT, self.output],)).hex()


class Gateway(EvmGateway):
    """The gateway's own signing and current-state checks over a `Head`.

    Left out is what needs a signer, a journal or a fee market: the history audit, the
    authority read and the fee budget. Every check of the fund's state is the real one.
    """

    def __init__(self, config, reader):
        self.config, self.reader = config, reader
        self.clock = lambda: reader.block.timestamp

    def guard(self):
        pass

    def audit_history(self, state):
        pass

    def verify_authority(self, manifest, account):
        pass

    def validate_plan_budget(self, state, plan):
        pass

    def validate_fee_budget(self, state, plan, balance):
        pass

    def retained_fee_exposure(self, plan):
        return 0


def at_the_head(monkeypatch, config, market, owned):
    """Observe through the connector's own snapshot, with the pool as retained."""
    seen = observation(config, owned, market)
    monkeypatch.setattr(observations.reads, "read_pool", lambda *read: seen.pool)
    for check in ("verify_router", "require_direct_fund", "verify"):
        monkeypatch.setattr(observations, check, lambda *read: None)
    monkeypatch.setattr(
        observations, "measured_quote", lambda reader, target, block, amount: quote(owned, market)
    )


def swap_plan(variant, *, kind="operator"):
    """The swap step of an admitted request as the gateway holds it before signing."""
    market, config, state, seen = admitted(variant)
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request(market))
    deciding, step = planning(config, state, intent, seen)
    deciding.decision.trigger = SimpleNamespace(kind=kind)
    approved = observation(
        config, state.owned[0], market, allowance=AMOUNT, entry_quote=quote(state.owned[0], market)
    )
    operation = step(approved)
    call = execution.AerodromeExecution.call(config, operation)
    plan = SimpleNamespace(
        attempt_id="attempt",
        operation=operation,
        before=approved,
        decision=deciding.decision,
        call=call,
        execution=prepare_call(config.accounts, operation.account, call),
        nonce=0,
        gas=200_000,
        gas_price=7,
        raw_transaction=None,
    )
    deciding.plan, deciding.pending, deciding.permissions = plan, None, None
    return market, config, deciding, plan


def signed(plan):
    return SimpleNamespace(**{**vars(plan), "raw_transaction": "0x02"})


def test_the_gateway_signs_nothing_for_a_fund_with_shares_and_sends_what_is_signed(
    variant, monkeypatch
):
    market, config, state, plan = swap_plan(variant)
    target = config.target(ALLOCATION)
    at_the_head(monkeypatch, config, market, plan.before.owned)

    clean = Gateway(config, Head(target, market.block))
    assert clean.authorize_signing(state)[0] == plan.operation
    held = Gateway(config, Head(target, market.block, shares=1))
    # Unsigned work is refused for the share by the gateway's own signing check.
    with pytest.raises(DataError, match="a trading-only allocation cannot own") as refusal:
        held.authorize_signing(state)
    assert refusal.value.kind == "ownership"
    with pytest.raises(DataError, match="a trading-only allocation cannot own"):
        held.validate_current(plan)
    # The same bytes once signed pass the gateway's current-state check, and the
    # observation they pass with carries no position.
    current = held.validate_current(signed(plan))
    assert (current.position, current.owned, current.wallet0.amount) == (
        None,
        plan.before.owned,
        5000,
    )


def test_a_signed_swap_sent_past_shares_keeps_every_other_current_state_check(variant, monkeypatch):
    market, config, state, plan = swap_plan(variant)
    target = config.target(ALLOCATION)
    at_the_head(monkeypatch, config, market, plan.before.owned)
    refusals = {
        "inventory": Head(target, market.block, shares=1, wallet=AMOUNT - 1),
        "precondition_changed": Head(target, market.block, shares=1, allowance=AMOUNT - 1),
        "simulation": Head(
            target, market.block, shares=1, output=plan.operation.minimum.amount - 1
        ),
    }
    for expected, head in refusals.items():
        with pytest.raises(DataError) as refusal:
            Gateway(config, head).validate_current(signed(plan))
        assert refusal.value.kind == expected


def test_a_strategys_signed_entry_swap_is_held_back_by_the_gateway_for_the_share(
    variant, monkeypatch
):
    market, config, state, plan = swap_plan(variant, kind="interval")
    at_the_head(monkeypatch, config, market, plan.before.owned)
    held = Gateway(config, Head(config.target(ALLOCATION), market.block, shares=1))
    with pytest.raises(DataError, match="a trading-only allocation cannot own") as refusal:
        held.validate_current(signed(plan))
    assert refusal.value.kind == "ownership"
    # Without the share the same signed bytes pass.
    clean = Gateway(config, Head(config.target(ALLOCATION), market.block))
    assert clean.validate_current(signed(plan)).position is None


def trading_steps(variant):
    """The approval and the swap of one admitted request, with the observations before them."""
    market, config, state, seen = admitted(variant)
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request(market))
    target = config.target(ALLOCATION)
    approval = ops.Approval(
        target=target, asset=intent.amount.asset, spender=target.contracts.router, amount=AMOUNT
    )
    swap = ops.Swap(
        target=target, amount=intent.amount, minimum=intent.minimum_output, deadline=intent.deadline
    )
    approved = observation(config, state.owned[0], market, allowance=AMOUNT)
    return market, config, ((approval, seen), (swap, approved))


def test_shares_on_a_trading_fund_never_change_what_its_mined_step_did(variant):
    market, config, steps = trading_steps(variant)
    target = config.target(ALLOCATION)
    spent = Effects(delta0=-AMOUNT, delta1=QUOTED)
    for holdings in (Holdings(shares=1), Holdings(claimable=1)):
        held = observations.stored_position(holdings, target, steps[0][1].pool)
        for operation, before in steps:
            for success in (True, False):
                # No position is attributed, whether the step succeeded or reverted.
                assert (
                    reconciliation.retained_position(
                        before, operation, spent, held, before.pool, success
                    )
                    is None
                )
        (issue,) = reconciliation.unowned_holdings(target, held)
        assert issue == (
            f"{target.pool.shares_asset}: fund holds LP shares or fee claims that a "
            "trading-only allocation does not own"
        )
    empty = observations.stored_position(Holdings(), target, steps[0][1].pool)
    assert reconciliation.unowned_holdings(target, empty) == ()


def test_an_allocation_with_liquidity_authority_still_refuses_a_changed_checkpoint(variant):
    market = resolved(variant)
    liquid = configuration(allocation_document(market, liquidity_enabled=True), market)
    target = liquid.target(ALLOCATION)
    state = created(liquid, STRATEGY)
    seen = observation(liquid, state.owned[0], market)
    held = observations.stored_position(Holdings(shares=1), target, seen.pool)
    approval = ops.Approval(
        target=target, asset=target.assets[0], spender=target.contracts.router, amount=AMOUNT
    )
    # Its shares are owned, so shares no step explains cannot be told from its own.
    with pytest.raises(DataError, match="unrelated operation changed the fund LP checkpoint"):
        reconciliation.retained_position(
            seen, approval, Effects(delta0=0, delta1=0), held, seen.pool, True
        )
    assert reconciliation.unowned_holdings(target, held) == ()


def reconciled(variant, monkeypatch, holdings, *, status="0x1"):
    """Reconcile a mined swap whose receipt block shows `holdings` on the fund.

    The chain reads are replaced by what they would return for an exact swap of the
    stated amount; the composition under test is the connector's own.
    """
    market, config, steps = trading_steps(variant)
    swap, approved = steps[1]
    success = status == "0x1"
    spent = Effects(delta0=-AMOUNT, delta1=QUOTED) if success else Effects(delta0=0, delta1=0)
    gas = AssetAmount(asset=evm_network(BASE).native_asset, amount=21)
    asked = {}

    def snapshot(backend, name, block, owned, **options):
        asked.update(options)
        return observation(
            config,
            owned,
            market,
            wallet=5000 + spent.delta0,
            received=spent.delta1,
            native=10**18 - gas.amount,
            allowance=0 if success else AMOUNT,
        )

    monkeypatch.setattr(reconciliation, "read_pool", lambda *read: approved.pool)
    monkeypatch.setattr(reconciliation, "movement", lambda *read: spent)
    monkeypatch.setattr(
        reconciliation,
        "stored_position",
        lambda reader, target, pool: observations.stored_position(holdings, target, pool),
    )
    monkeypatch.setattr(reconciliation, "snapshot", snapshot)
    monkeypatch.setattr(reconciliation, "charged_fees", lambda *read: (gas, None))
    monkeypatch.setattr(
        "almanak.connectors.aerodrome.state.Outcome", lambda **fields: SimpleNamespace(**fields)
    )
    pending = SimpleNamespace(
        operation=swap,
        before=approved,
        decision=SimpleNamespace(intent=SimpleNamespace(kind="swap_exact_input")),
        tx_hash="0x" + "ab" * 32,
        nonce=3,
        call=None,
        execution=None,
    )
    backend = SimpleNamespace(reader=None, config=config)
    outcome = reconciliation.reconcile(backend, pending, {"status": status}, market.block)
    return config.target(ALLOCATION), outcome, asked


def test_a_mined_swap_is_reconciled_from_its_own_effects_with_the_shares_reported(
    variant, monkeypatch
):
    target, outcome, asked = reconciled(variant, monkeypatch, Holdings(shares=1))
    held = {
        str(v.asset): v.amount
        for v in (outcome.after.owned.inventory0, outcome.after.owned.inventory1)
    }
    assert (outcome.success, outcome.delta0, outcome.delta1) == (True, -AMOUNT, QUOTED)
    assert held == {str(target.assets[0]): 0, str(target.assets[1]): QUOTED}
    # The shares are reported and never attributed: no position is owned or observed.
    assert (outcome.after.owned.position, outcome.after.position) == (None, None)
    assert outcome.accounting_issues == (
        f"{target.pool.shares_asset}: fund holds LP shares or fee claims that a "
        "trading-only allocation does not own",
    )
    # The observation after the step is read as one for signed bytes.
    assert asked == {"quotes": False, "residual": False, "signed": True}


def test_a_mined_swap_with_no_shares_on_the_fund_reports_no_issue(variant, monkeypatch):
    _, outcome, _ = reconciled(variant, monkeypatch, Holdings())
    assert (outcome.success, outcome.accounting_issues) == (True, ())


def test_a_reverted_swap_keeps_its_inventory_and_still_reports_the_shares(variant, monkeypatch):
    target, outcome, _ = reconciled(variant, monkeypatch, Holdings(shares=1), status="0x0")
    assert (outcome.success, outcome.delta0, outcome.delta1) == (False, 0, 0)
    assert outcome.after.owned.inventory0.amount == AMOUNT
    assert len(outcome.accounting_issues) == 1 and outcome.after.owned.position is None


def admitted(variant, **observed):
    """A deployment whose request is being admitted with one observation."""
    market = resolved(variant)
    config = operator_config(market)
    state = created(config)
    owned = state.owned[0]
    seen = observation(config, owned, market, entry_quote=quote(owned, market), **observed)
    return market, config, state, seen


def test_the_intent_follows_the_quote_of_the_observation_it_is_admitted_with(variant):
    market, config, state, seen = admitted(variant)
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request(market))
    assert intent.kind == "swap_exact_input" and intent.target == config.target(ALLOCATION)
    assert intent.amount == AssetAmount(asset=market.pool.currency0, amount=AMOUNT)
    # 50 basis points below the quoted output, rounded down.
    assert intent.minimum_output == AssetAmount(
        asset=market.pool.currency1, amount=QUOTED * 9950 // 10000
    )
    assert intent.deadline == market.block.timestamp + 300
    found = connector_from_wire({"connector": CONNECTOR})
    found.codec.capability.admit(state.capabilities, intent)
    assert found.lifecycle.increases_exposure(intent)


def test_a_missing_or_differently_sized_quote_builds_no_intent(variant):
    market, config, state, seen = admitted(variant)
    build = direct.binding(CONNECTOR).intent
    owned = state.owned[0]
    bare = observation(config, owned, market)
    with pytest.raises(ValueError, match="the pool quotes no output for the stated amount"):
        build(config, (bare,), request(market))
    unmeasured = observation(
        config, owned, market, entry_quote_error="entry quote unavailable: reverted"
    )
    with pytest.raises(ValueError, match="entry quote unavailable: reverted"):
        build(config, (unmeasured,), request(market))
    for other in (request(market, amount=AMOUNT + 1), request(market, sell="other")):
        with pytest.raises(ValueError, match="not for the stated amount of the sold asset"):
            build(config, (seen,), other)


def planning(config, state, intent, seen):
    """The planned operation of one observation, checked as the gateway checks it."""
    execution = connector_from_wire({"connector": CONNECTOR}).execution
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


def test_the_gateway_admits_the_exact_approval_then_the_swap_and_nothing_larger(variant):
    market, config, state, seen = admitted(variant)
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request(market))
    deciding, step = planning(config, state, intent, seen)
    owned, router = state.owned[0], config.target(ALLOCATION).contracts.router

    approval = step(seen)
    assert (approval.kind, approval.spender, approval.amount) == ("approve", router, AMOUNT)
    assert approval.asset == market.pool.currency0
    # A different allowance is reset before the exact one is granted.
    stale = observation(config, owned, market, allowance=AMOUNT + 5)
    assert (step(stale).kind, step(stale).amount) == ("approve", 0)
    # An allowance for the pool's shares is neither needed nor touched.
    approved = observation(config, owned, market, allowance=AMOUNT, shares_allowance=5)
    swap = step(approved)
    assert (swap.kind, swap.amount, swap.minimum) == ("swap", intent.amount, intent.minimum_output)

    # Nothing sells more than the stated amount: inventory is that amount and no more.
    larger = intent.model_copy(
        update={"amount": AssetAmount(asset=market.pool.currency0, amount=AMOUNT + 1)}
    )
    deciding.decision = SimpleNamespace(intent=larger, observations=(seen,))
    execution = connector_from_wire({"connector": CONNECTOR}).execution
    with pytest.raises(DataError, match="exceeds attributed input inventory"):
        execution.validate_operation(
            None, deciding, execution.operation(config, deciding, larger, approved), approved
        )
    # A minimum below the stated slippage is refused against the retained router quote.
    wider = intent.model_copy(
        update={"minimum_output": AssetAmount(asset=market.pool.currency1, amount=1)}
    )
    deciding.decision = SimpleNamespace(intent=wider, observations=(seen,))
    with pytest.raises(ValueError, match="widens the configured slippage limit"):
        execution.validate_operation(
            None, deciding, execution.operation(config, deciding, wider, approved), approved
        )


def test_a_mined_swap_spends_its_approval_whole_and_leaves_other_allowances(variant):
    market, config, state, seen = admitted(variant)
    intent = direct.binding(CONNECTOR).intent(config, (seen,), request(market))
    target = config.target(ALLOCATION)
    approved = observation(config, state.owned[0], market, allowance=AMOUNT, shares_allowance=5)
    swap = ops.Swap(
        target=target, amount=intent.amount, minimum=intent.minimum_output, deadline=intent.deadline
    )
    spent = Effects(delta0=-AMOUNT, delta1=QUOTED)
    after = reconciliation.allowances_after(approved, swap, spent, True)
    assert [(item.asset, item.amount) for item in after] == [
        (target.assets[0], 0),
        (target.assets[1], 0),
        (target.pool.shares_asset, 5),
    ]
    # A reverted swap spends nothing: the approval mined before it stands.
    assert reconciliation.allowances_after(approved, swap, spent, False) == approved.allowances


def test_the_position_is_the_attributed_inventory_and_the_allowances_last_observed(variant):
    market, config, state, seen = admitted(variant)
    sold, bought = str(market.pool.currency0), str(market.pool.currency1)
    report = direct.binding(CONNECTOR).position
    before = [{"asset": sold, "amount": str(AMOUNT)}, {"asset": bought, "amount": "0"}]
    assert report(state) == {"inventory": before, "allowances": []}
    started = state.model_copy(update={"initialized": True, "baseline": (seen,)})
    assert report(started) == {"inventory": before, "allowances": []}

    # After a mined approval whose swap never followed, the allowance is reported:
    # a swap refused after its approval, or an interruption recovered at the approval.
    owned = state.owned[0]
    approved = observation(config, owned, market, allowance=AMOUNT)
    router = config.target(ALLOCATION).contracts.router
    left = {"asset": sold, "spender": router, "amount": str(AMOUNT)}
    stopped = SimpleNamespace(
        owned=state.owned, baseline=(seen,), outcomes=(SimpleNamespace(after=approved),)
    )
    assert report(stopped) == {"inventory": before, "allowances": [left]}

    swapped = owned.model_copy(
        update={
            "inventory0": AssetAmount(asset=market.pool.currency0, amount=0),
            "inventory1": AssetAmount(asset=market.pool.currency1, amount=QUOTED),
        }
    )
    done = SimpleNamespace(
        owned=(swapped,), baseline=(seen,), outcomes=(SimpleNamespace(after=seen),)
    )
    after = [{"asset": sold, "amount": "0"}, {"asset": bought, "amount": str(QUOTED)}]
    assert report(done) == {"inventory": after, "allowances": []}

    # An allowance the wallet gave the router for the pool's shares is listed as it is.
    shares = observation(config, swapped, market, shares_allowance=5)
    kept = SimpleNamespace(
        owned=(swapped,), baseline=(seen,), outcomes=(SimpleNamespace(after=shares),)
    )
    assert report(kept)["allowances"] == [
        {"asset": str(market.pool.shares_asset), "spender": router, "amount": "5"}
    ]


def arguments(variant, *extra, without=()):
    stated = {
        "--connector": CONNECTOR,
        "--chain": str(BASE),
        "--pool": POOLS[variant],
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


def test_the_command_line_states_the_swap_and_its_bounds(variant):
    args = arguments(variant)
    assert (args.product, args.ax_command, args.execute) == ("operator-action", "swap", False)
    assert (args.amount, args.slippage_bps, args.deadline_seconds) == (1000, 50, 300)
    assert arguments(variant, "--deadline-seconds", "60", "--execute").deadline_seconds == 60
    assert ax_actions.action_request(args) == ActionRequest(
        "swap", 1000, sell=args.sell, slippage_bps=50, deadline_seconds=300
    )
    market = ax_actions.market_request(args)
    assert (market.connector, market.chain, market.pool) == (CONNECTOR, BASE, POOLS[variant])
    for missing in ("--chain", "--pool", "--sell", "--amount", "--slippage-bps", "--wallet"):
        with pytest.raises(SystemExit):
            arguments(variant, without=(missing,))
    for refused in (("--slippage-bps", "10000"), ("--amount", "0"), ("--deadline-seconds", "0")):
        with pytest.raises(SystemExit):
            arguments(variant, *refused)


def test_the_command_refuses_an_undeclared_chain_and_passes_the_stated_swap_on(
    variant, monkeypatch
):
    seen = {}

    def deployment(name, market, action, wallet, provider, endpoint, limits, **options):
        seen.update(connector=name, market=market, action=action, limits=limits)
        raise StopIteration

    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
    monkeypatch.setattr(ax_actions, "deployment", deployment)
    with pytest.raises(StopIteration):
        ax_actions.command(arguments(variant))
    assert seen["limits"] == ax_actions.FeeLimits(7, 9, None)
    assert (seen["connector"], seen["market"].pool) == (CONNECTOR, POOLS[variant])
    assert (seen["action"].sell, seen["action"].amount) == (arguments(variant).sell, 1000)

    monkeypatch.setattr(ax_actions, "read_wallet", pytest.fail)
    with pytest.raises(ValueError, match="aerodrome serves no direct swap on eip155:31337"):
        ax_actions.command(arguments(variant, "--chain", "eip155:31337"))
