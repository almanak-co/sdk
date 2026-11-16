"""The `hyperliquid` connector's types, authority and registration (blueprint 30, 13.1).

The gateway adapter's steps against the HyperCore model are `test_hyperliquid_execution.py`.
"""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from pydantic import TypeAdapter, ValidationError

from almanak.caip import AssetType, ChainId
from almanak.chains import HYPEREVM
from almanak.configuration.authoring import binding_for
from almanak.configuration.parameter_types import parameter_type
from almanak.connectors.composition import (
    CONNECTOR_IDS,
    author_configuration_from_json,
    connector_for,
    connector_from_wire,
)
from almanak.connectors.hyperliquid import corewriter as cw
from almanak.connectors.hyperliquid import positions
from almanak.connectors.hyperliquid import trading as rules
from almanak.connectors.hyperliquid.agents import AgentNames
from almanak.connectors.hyperliquid.api import Actions, EffectsResult
from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET, USDC
from almanak.connectors.hyperliquid.authority import admit, capability_codec, declared, validate
from almanak.connectors.hyperliquid.custody_requests import Deposit as DepositRequest
from almanak.connectors.hyperliquid.effects import Book, Effect
from almanak.connectors.hyperliquid.execution import Execution
from almanak.connectors.hyperliquid.fork import ForkConfig
from almanak.connectors.hyperliquid.lifecycle import Lifecycle, ownership
from almanak.connectors.hyperliquid.models import (
    Allocation,
    ClosePosition,
    Config,
    IncreasePosition,
    Intent,
    Parameters,
    SetLeverage,
    Target,
    Withdraw,
)
from almanak.connectors.hyperliquid.models import Deposit as DepositIntent
from almanak.connectors.hyperliquid.observations import Snapshot
from almanak.connectors.hyperliquid.operations import (
    AgentApproval,
    Approve,
    ClassTransfer,
    Deposit,
    StandardMode,
    VenueAction,
    Withdrawal,
)
from almanak.connectors.hyperliquid.permissions import Permissions, derive
from almanak.connectors.hyperliquid.positions import Holding, Ownership, holding, withdrawable
from almanak.connectors.hyperliquid.resolution import PerpMarket
from almanak.connectors.hyperliquid.state import Checkpoint, CustodyOutcome, VenueOutcome
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import (
    ONE_ALLOCATION_PER_FUND,
    ONE_ALLOCATION_PER_FUND_CONNECTORS,
    EconomicAction,
    RunMode,
)
from almanak.connectors.target_routing import allocation_connector, target_connector
from almanak.gateway.environments import ForkBinding
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_config import (
    AlchemyProvider,
    ForkAssetFunding,
    ForkEoaFund,
    ForkOperator,
    ForkSafeFund,
    ForkTarget,
)
from almanak.models import (
    Account,
    AccountBinding,
    AssetAmount,
    Block,
    Chain,
    SafeRoles,
    StrategyAccounts,
)
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalTrigger
from tests.test_hypercore_custody import allows, deposit_request

INSTANCE = "0x" + "11" * 32
MAINNET = Chain(chain_id=HYPEREVM.chain, environment="mainnet")
FORK = Chain(chain_id=HYPEREVM.chain, environment="anvil:" + INSTANCE)
ORIGIN = Block(chain=MAINNET, number=49017682, hash="0x" + "d6" * 32, timestamp=1_792_500_000)
SAFE = "0x" + "5a" * 20
OPERATOR = "0x" + "0b" * 20
AGENT = "0x" + "a1" * 20
HYPE = HYPEREVM.native_asset
T0 = 1_793_777_833_000
HYPERLIQUID = ConnectorId("hyperliquid")
PROVIDER = AlchemyProvider(
    kind="alchemy", network="hyperliquid-mainnet", api_key_env="ALCHEMY_API_KEY"
)
BTC = {"coin": "BTC", "asset": 0, "size_decimals": 5, "max_leverage": 40, "tier_bound": None}


def market(**values):
    return PerpMarket(**{**BTC, **values})


def profile(address=SAFE):
    return SafeRoles(
        kind="safe_roles",
        fund=Account(chain=FORK, address=address),
        operator=Account(chain=FORK, address=OPERATOR),
        roles_module="0x" + "34" * 20,
        role_key="0x" + "56" * 32,
    )


def allocation(name="perp", account="safe", **values):
    fields = {
        "name": name,
        "account": account,
        "chain": FORK,
        "market": market(),
        "capital": AssetAmount(asset=USDC, amount=30_000_000),
        "leverage": 10,
        "slippage_bps": 50,
    }
    return Allocation.model_validate({**fields, **values})


def accounts(*names):
    return StrategyAccounts(
        bindings=tuple(
            AccountBinding(name=name, profile=profile("0x" + f"{5 + i:02x}" * 20))
            for i, name in enumerate(names)
        )
        if len(names) > 1
        else (AccountBinding(name=names[0], profile=profile()),)
    )


def config(*allocations, names=("safe",)):
    return Config(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        accounts=accounts(*names),
        allocations=allocations or (allocation(),),
        fork_bindings=(ForkBinding(chain=FORK, origin=ORIGIN, instance=INSTANCE),),
    )


@pytest.fixture
def configured():
    return config()


@pytest.fixture
def target(configured):
    return configured.target("perp")


# Configuration.


def test_one_allocation_on_one_safe_is_the_whole_configuration(configured):
    assert configured.connector == "hyperliquid"
    assert configured.target("perp") == Target(
        allocation="perp",
        account="safe",
        fund=Account(chain=FORK, address=SAFE),
        market=market(),
    )
    with pytest.raises(ValidationError, match="one allocation per fund"):
        config(allocation(), allocation(name="second"))
    with pytest.raises(ValidationError, match="exactly one Safe"):
        config(allocation(), allocation(name="second", account="other"), names=("safe", "other"))
    with pytest.raises(ValidationError, match="explicit allocation"):
        config(allocation(), names=("safe", "other"))
    with pytest.raises(ValidationError, match="fund chain"):
        config(allocation(chain=MAINNET))


def test_allocation_bounds_leverage_and_slippage_and_keeps_an_optional_gas_reserve():
    with pytest.raises(ValidationError, match="maximum"):
        allocation(leverage=41)
    for leverage in (0, -1):
        with pytest.raises(ValidationError):
            allocation(leverage=leverage)
    for slippage in (0, 1_001):
        with pytest.raises(ValidationError):
            allocation(slippage_bps=slippage)
    other = AssetType("eip155:999/erc20:0x5555555555555555555555555555555555555555")
    with pytest.raises(ValidationError, match="USDC on eip155:999"):
        allocation(capital=AssetAmount(asset=other, amount=1))
    # The operator's HYPE reserve is none by default, which leaves a backtest's
    # configuration and its serialized form as phase 5 wrote them.
    assert allocation().minimum_native_balance == 0
    assert "minimum_native_balance" not in allocation().model_dump()
    reserved = allocation(minimum_native_balance=10**16)
    assert reserved.model_dump()["minimum_native_balance"] == 10**16
    assert allocation().native_asset == HYPE


def test_target_is_a_usdc_account_on_hyperevm():
    ethereum = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    fields = {"allocation": "perp", "account": "safe", "market": market()}
    with pytest.raises(ValidationError, match="eip155:999"):
        Target(fund=Account(chain=ethereum, address=SAFE), **fields)
    with pytest.raises(ValidationError, match="eip155:999"):
        Target(fund=Account(chain=FORK, address=SAFE), margin_asset=HYPE, **fields)
    target = Target(fund=Account(chain=FORK, address=SAFE), **fields)
    assert target.assets == (USDC, HYPE)


# The fork configuration: a funded Safe, its HYPE-paying operator and one fork target.


def funded(account="safe"):
    funding = ForkAssetFunding(
        balance=AssetAmount(asset=USDC, amount=30_000_000), method="local_storage"
    )
    return ForkSafeFund(account=account, operator="operator", assets=(funding,))


def fork_config(**values):
    fields = {
        "decision_trigger": IntervalTrigger(kind="interval", seconds=60),
        "forks": (ForkTarget(chain=MAINNET, provider=PROVIDER, block=ORIGIN),),
        "funds": (funded(),),
        "operators": (
            ForkOperator(
                name="operator",
                native_balance=AssetAmount(asset=HYPE, amount=5 * 10**18),
                foundry_account_index=0,
            ),
        ),
        "allocations": (allocation(chain=MAINNET),),
    }
    return ForkConfig.model_validate({**fields, **values})


def test_a_fork_funds_one_safe_on_its_hyperevm_target_through_its_operator():
    configured = fork_config()
    assert author_configuration_from_json(configured.model_dump_json()) == configured
    for values, refusal in (
        ({"allocations": (allocation(chain=MAINNET),) * 2}, "allocation names"),
        ({"allocations": (allocation(account="other", chain=MAINNET),)}, "funded Safes"),
        ({"allocations": (allocation(),)}, "explicit HyperEVM fork target"),
        ({"funds": (funded(), funded("other"))}, "funded Safes"),
    ):
        with pytest.raises(ValidationError, match=refusal):
            fork_config(**values)


def test_a_fork_refuses_an_eoa_fund_and_funding_short_of_the_capital():
    eoa = ForkEoaFund(
        account="safe",
        native_balance=AssetAmount(asset=HYPE, amount=10**18),
        assets=funded().assets,
        foundry_account_index=1,
    )
    with pytest.raises(ValidationError, match="fund a Safe"):
        fork_config(funds=(eoa,), operators=())
    short = ForkAssetFunding(balance=AssetAmount(asset=USDC, amount=1), method="local_storage")
    with pytest.raises(ValidationError, match="cover its configured capital"):
        fork_config(funds=(funded().model_copy(update={"assets": (short,)}),))
    ether = AssetAmount(asset=AssetType("eip155:1/slip44:60"), amount=10**18)
    operator = ForkOperator(name="operator", native_balance=ether, foundry_account_index=0)
    with pytest.raises(ValidationError, match="share HyperEVM"):
        fork_config(operators=(operator,))


def test_fork_preparation_moves_the_allocation_to_its_fixture_chain():
    # Every fork configuration's positions move through the shared remapping
    # (`configuration.parameters.fork_rescope`), single-connector ones included.
    from almanak.connectors.composed.fork import remap_allocation

    moved = remap_allocation(allocation(chain=MAINNET), FORK)
    assert moved == allocation()
    with pytest.raises(ValueError, match="origin CAIP identity"):
        remap_allocation(
            allocation(chain=MAINNET), Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
        )


# Intents, capabilities and the strategy API.


def test_intents_are_economic_actions_of_one_target(target):
    adapter: TypeAdapter = TypeAdapter(Intent)
    intents = (
        DepositIntent(target=target, amount=20_000_000),
        SetLeverage(target=target, leverage=10),
        IncreasePosition(target=target, side="long", size="0.002"),
        ClosePosition(target=target),
        Withdraw(target=target),
        Withdraw(target=target, amount=5_000_000),
    )
    for intent in intents:
        EconomicAction(intent.kind)
        assert adapter.validate_json(intent.model_dump_json()) == intent
    with pytest.raises(ValidationError):
        DepositIntent(target=target, amount=0)
    for size in ("0", "0.0", "-1", "NaN"):
        with pytest.raises(ValidationError):
            IncreasePosition(target=target, side="short", size=size)


def test_capabilities_are_declared_from_configuration_and_admit_only_their_targets(configured):
    capabilities = declared(configured)
    (capability,) = capabilities.requirements
    assert (capability.target, capability.leverage, capability.slippage_bps) == (
        configured.target("perp"),
        10,
        50,
    )
    validate(configured, capabilities)
    assert capability_codec().declared(configured) == capabilities
    changed = capabilities.model_copy(
        update={"requirements": (capability.model_copy(update={"leverage": 5}),)}
    )
    with pytest.raises(ValueError, match="differ"):
        validate(configured, changed)
    target = configured.target("perp")
    admit(capabilities, ClosePosition(target=target))
    admit(capabilities, SetLeverage(target=target, leverage=10))
    with pytest.raises(ValueError, match="configured cross leverage"):
        admit(capabilities, SetLeverage(target=target, leverage=5))
    foreign = target.model_copy(update={"allocation": "other"})
    with pytest.raises(ValueError, match="exactly one retained perp capability"):
        admit(capabilities, ClosePosition(target=foreign))
    # A fork run's prepared configuration decodes as itself, a backtest's as parameters.
    adapter = capability_codec().parameters
    assert type(adapter.validate_json(configured.model_dump_json())) is Config
    plain = Parameters.model_validate(
        {name: getattr(configured, name) for name in Parameters.model_fields}
    )
    assert type(adapter.validate_json(plain.model_dump_json())) is Parameters


def owning(target, position="0", hypercore="0"):
    """What the strategy owns, as the checkpoint projects it from the reconciled book."""
    return Ownership(
        target=target,
        inventory=(AssetAmount(asset=USDC, amount=0),),
        position=Decimal(position),
        hypercore=Decimal(hypercore),
    )


def snapshot(target, owned=None, wallet=30_000_000, allowance=0):
    """An EVM-only snapshot at one block: HyperCore state is the checkpoint's documents."""
    return Snapshot(
        target=target,
        block=Block(chain=FORK, number=ORIGIN.number + 1, hash="0x" + "e7" * 32, timestamp=1),
        owned=owned or owning(target),
        wallets=(AssetAmount(asset=USDC, amount=wallet),),
        allowance=allowance,
        native_balance=AssetAmount(asset=HYPE, amount=10**18),
        payer=Account(chain=FORK, address=OPERATOR),
    )


def test_actions_build_the_intents_a_backtest_simulates(configured, target):
    flat = snapshot(target)
    actions = Actions(SimpleNamespace(require_observation=lambda name: flat, config=configured))
    assert actions.deposit("perp") == DepositIntent(target=target, amount=30_000_000)
    five = AssetAmount(asset=USDC, amount=5)
    assert actions.deposit("perp", five) == DepositIntent(target=target, amount=5)
    assert actions.set_leverage("perp") == SetLeverage(target=target, leverage=10)
    assert actions.open("perp", side="long", size=0.002) == IncreasePosition(
        target=target, side="long", size="0.002"
    )
    assert actions.close("perp") == ClosePosition(target=target)
    assert actions.withdraw("perp") == Withdraw(target=target)
    assert actions.withdraw("perp", five) == Withdraw(target=target, amount=5)
    with pytest.raises(ValueError, match="USDC"):
        actions.deposit("perp", AssetAmount(asset=HYPE, amount=1))


# Lifecycle and the checkpoint.


@pytest.fixture
def state(configured):
    return Lifecycle.initial(
        configured,
        StrategyDefinition(filename="strategy.py", source=""),
        declared(configured),
        None,
    )


def test_initial_checkpoint_holds_the_safes_fresh_documents(state, configured):
    assert (state.custody.safe, state.trading.safe, state.book.safe) == (SAFE, SAFE, SAFE)
    assert state.trading.deployment == state.deployment_id
    (perp,) = state.trading.targets
    assert (perp.coin, perp.asset, perp.size_decimals, perp.max_leverage) == ("BTC", 0, 5, 40)
    assert (perp.leverage, perp.slippage_bps, perp.margin, perp.evidence) == (10, 50, "cross", None)
    assert state.book.cursor == 0 and not state.custody.requests and not state.trading.attempts
    (owned,) = state.owned
    assert owned.inventory == (configured.allocation("perp").capital,)
    assert owned.position == 0
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state


def test_checkpoint_refuses_documents_of_another_safe_or_market(state):
    from almanak.connectors.hyperliquid.custody import Custody

    other = "0x" + "77" * 20
    with pytest.raises(ValidationError, match="configured Safe"):
        state.model_validate({**dict(state), "custody": Custody(safe=other)})
    moved = state.trading.targets[0].model_copy(update={"leverage": 5})
    with pytest.raises(ValidationError, match="configured markets"):
        state.model_validate(
            {**dict(state), "trading": state.trading.model_copy(update={"targets": (moved,)})}
        )
    for figure in ({"position": Decimal("0.1")}, {"activated": True}, {"leverage": 10}):
        owned = state.owned[0].model_copy(update=figure)
        with pytest.raises(ValidationError, match="its documents project"):
            state.model_validate({**dict(state), "owned": (owned,)})


def test_ownership_fields_include_the_hypercore_documents(state, target):
    fields = ownership(state)
    assert set(fields) == {"owned", "custody", "trading", "book"}
    assert fields["custody"] is state.custody and fields["book"] is state.book
    _, attempt = rules.plan_leverage(state.trading, "BTC", AGENT, T0)
    resolved = attempt.model_copy(update={"status": "not_applied"})
    after = SimpleNamespace(target=state.owned[0].target, owned=state.owned[0])
    venue_outcome = VenueOutcome.model_construct(
        operation=VenueAction(target=target, attempt=attempt), attempt=resolved, after=after
    )
    # The outcome's attempt is retained as the trading rules retain a plan, then resolved.
    trading = ownership(state, venue_outcome)["trading"]
    assert trading.attempts == (resolved,) and trading.nonces == {AGENT: T0}
    # A custody outcome carries only the request it resolved, which takes its place.
    request = deposit_request()
    queued = state.model_copy(
        update={"custody": state.custody.model_copy(update={"requests": (request,)})}
    )
    resolved_request = request.model_copy(update={"status": "resolved"})
    custody_outcome = CustodyOutcome.model_construct(
        request=resolved_request, approval=None, after=after
    )
    changed = ownership(queued, custody_outcome)
    assert changed["custody"].requests == (resolved_request,)
    assert changed["trading"] is state.trading
    with pytest.raises(ValueError, match="only an outstanding custody request resolves"):
        ownership(state, custody_outcome)


def fill(key, time, size, px, start, owned=True):
    """One reconciled fill of BTC, the strategy's own unless `owned` is false."""
    return Effect(
        kind="fill",
        key=key,
        coin="BTC",
        time=time,
        size=Decimal(size),
        cash=Decimal(0),
        owned=owned,
        start=Decimal(start),
        entry={"px": px},
    )


def booked(*fills, cash="30", owned=None, ledger=()):
    venue = sum((f.size for f in fills), Decimal(0))
    held = venue if owned is None else Decimal(owned)
    return Book(
        safe=SAFE,
        cursor=0,
        positions={"BTC": venue} if venue else {},
        owned={"BTC": held} if held else {},
        cash=Decimal(cash),
        effects=(*fills, *ledger),
    )


def test_the_holding_is_priced_from_the_strategys_own_fills_as_a_backtest_prices_it(target):
    from almanak.connectors.hyperliquid import perp_math
    from almanak.connectors.hyperliquid.economic_state import HyperCoreAccount

    # Two entries average in and a reduction keeps the entry, as the economic model has it.
    book = booked(fill("a", 1_000, "0.001", "100", "0"), fill("b", 2_500, "0.002", "130", "0.001"))
    assert holding(book, "BTC") == Holding(szi="0.003", entry_px="120", changed_at=2)
    reduced = booked(*book.effects, fill("c", 4_000, "-0.001", "90", "0.003"))
    assert holding(reduced, "BTC") == Holding(szi="0.002", entry_px="120", changed_at=4)
    account = HyperCoreAccount(target=target, activated=True, perp_usdc=10**8, leverage=10)
    for size, price in (("0.001", "100"), ("0.002", "130"), ("-0.001", "90")):
        account, _ = perp_math.trade(account, Decimal(size), Decimal(price), 0, 4)
    assert account.holding == holding(reduced, "BTC")
    # The fills of one millisecond replay in the order their start positions chain.
    swapped = booked(
        fill("b", 1_000, "0.002", "130", "0.001"), fill("a", 1_000, "0.001", "100", "0")
    )
    assert holding(swapped, "BTC").entry_px == "120"
    # Only a complete chain prices it: listed C, A, B, the only chain is A, B, C, and an
    # outside fill between closes A's part, so C's own price is the entry.
    a = fill("a", 1_000, "0.001", "78000", "0")
    b = fill("b", 1_000, "-0.001", "77900", "0.001", owned=False)
    c = fill("c", 1_000, "0.002", "78200", "0")
    chain = booked(c, a, b, owned="0.002")
    assert holding(chain, "BTC") == Holding(szi="0.002", entry_px="78200", changed_at=1)
    # Orders that own the same exposure at different prices are refused, not picked.
    other = fill("c", 1_000, "0.001", "78200", "0")
    with pytest.raises(DataError, match="different holding prices") as refused:
        holding(booked(other, b, a, owned="0.001"), "BTC")
    assert refused.value.kind == "ambiguous"
    assert refused.value.details["prices"] == ["78000", "78200"]
    with pytest.raises(DataError, match="do not chain") as unchained:
        holding(booked(fill("x", 1_000, "0.001", "1", "0.5")), "BTC")
    assert unchained.value.kind == "inconsistent"
    # A liquidation (an outside fill) closes the owned part; a new entry starts afresh.
    liquidated = booked(*book.effects, fill("l", 5_000, "-0.003", "80", "0.003", owned=False))
    assert holding(liquidated, "BTC") is None
    again = booked(*liquidated.effects, fill("d", 7_000, "0.001", "150", "0"))
    assert holding(again, "BTC") == Holding(szi="0.001", entry_px="150", changed_at=7)
    # A flip starts the new side at its own price.
    flipped = booked(
        fill("a", 1_000, "0.001", "100", "0"), fill("f", 2_000, "-0.003", "90", "0.001")
    )
    assert holding(flipped, "BTC") == Holding(szi="-0.002", entry_px="90", changed_at=2)


def test_withdrawable_is_the_economic_models_rule_valued_at_the_entry():
    flat = booked(cash="28.861234")
    assert withdrawable(flat, None, 10) == 28_861_234
    # Spot USDC a withdrawal also sends counts; a backtest never holds any between steps.
    spot = {"type": "accountClassTransfer", "usdc": "2.5", "toPerp": False}
    moved = booked(cash="26.361234", ledger=(ledger_effect(spot),))
    assert withdrawable(moved, None, 10) == 28_861_234
    held = Holding(szi="0.002", entry_px="75000", changed_at=0)
    # Notional 150 at the entry: 10x and 20x keep a tenth of it, 5x its initial margin of 30.
    assert withdrawable(booked(cash="29"), held, 10) == 14_000_000
    assert withdrawable(booked(cash="29"), held, 20) == 14_000_000
    assert withdrawable(booked(cash="29"), held, 5) == 0
    assert withdrawable(booked(cash="10"), held, 10) == 0


def ledger_effect(delta):
    return Effect(
        kind="ledger",
        key="ledger:" + delta["type"],
        coin=None,
        time=1,
        size=Decimal(0),
        cash=Decimal(0),
        entry={"delta": delta, "hash": "0x" + "00" * 32, "time": 1},
    )


def test_projected_figures_come_from_the_documents_alone(state):
    custody, trading = state.custody, state.trading
    figures = positions.projected(custody, trading, state.book, "BTC")
    assert (figures["activated"], figures["leverage"], figures["holding"]) == (False, None, None)
    request = DepositRequest.model_validate(
        {
            "key": "d",
            "safe": SAFE,
            "submission": {"transaction": "0x" + "d1" * 32, "block": 1, "time": T0},
            "amount": 30_000_000,
            "credit": {
                "transaction": "0x" + "d1" * 32,
                "log_index": 0,
                "block": 1,
                "recipient": SAFE,
                "amount": 30_000_000,
            },
        }
    )
    assert not positions.activated(custody.model_copy(update={"requests": (request,)}))
    resolved = request.model_copy(update={"status": "resolved"})
    assert positions.activated(custody.model_copy(update={"requests": (resolved,)}))
    # Only an applied updateLeverage sets it, the last one counting.
    planned, attempt = rules.plan_leverage(trading, "BTC", AGENT, T0)
    assert positions.leverage(planned, "BTC") is None
    applied = attempt.model_copy(update={"status": "applied"})
    assert positions.leverage(rules.replaced(planned, applied), "BTC") == 10


def test_a_fork_snapshot_shows_what_a_backtests_shows_under_the_same_names():
    """The reference reads `owned.activated`, `owned.leverage`, `owned.holding.szi`,
    `withdrawable` and `effective_at` of each observation, in backtests and on forks."""
    from almanak.connectors.hyperliquid import economic_state

    live = Ownership.model_json_schema()["properties"]
    simulated = economic_state.HyperCoreAccount.model_json_schema()["properties"]
    assert all(live[name] == simulated[name] for name in ("activated", "leverage", "holding"))
    assert economic_state.Holding is Holding
    snapshot = economic_state.Snapshot.model_json_schema()["properties"]["withdrawable"]
    assert live["withdrawable"] == {**snapshot, "default": 0}
    assert all(isinstance(getattr(Snapshot, n), property) for n in ("withdrawable", "effective_at"))


def test_initialization_and_closure_read_the_documents_and_the_allowance(state, target):
    owned = state.owned[0]
    clean = snapshot(target, owned=owned)
    Lifecycle.validate_initial(state, (clean,))
    Lifecycle.validate_closure(state, (clean,))
    with pytest.raises(ValueError, match="allowance"):
        Lifecycle.validate_initial(state, (snapshot(target, owned=owned, allowance=1),))
    with pytest.raises(ValueError, match="allowance"):
        Lifecycle.validate_closure(state, (snapshot(target, owned=owned, allowance=1),))
    with pytest.raises(ValueError, match="no longer covers"):
        Lifecycle.validate_owned(state, (snapshot(target, owned=owned, wallet=1),))
    pending, _ = rules.plan_leverage(state.trading, "BTC", AGENT, T0)
    with pytest.raises(ValueError, match="unresolved attempt"):
        Lifecycle.validate_closure(
            state.model_copy(update={"trading": pending}), (snapshot(target, owned=owned),)
        )
    with pytest.raises(ValueError, match="fresh HyperCore documents"):
        Lifecycle.validate_initial(state.model_copy(update={"trading": pending}), (clean,))
    for held in (owning(target, position="0.002"), owning(target, hypercore="0.5")):
        changed = state.model_copy(update={"owned": (held,)})
        with pytest.raises(ValueError, match="no owned position and no HyperCore USDC"):
            Lifecycle.validate_closure(changed, (snapshot(target, owned=held),))
    outside = state.book.model_copy(update={"positions": {"BTC": Decimal("-0.1")}})
    with pytest.raises(DataError, match="outside the strategy's own") as halted:
        Lifecycle.validate_closure(state.model_copy(update={"book": outside}), (clean,))
    assert halted.value.kind == "teardown_incomplete"


def test_exposure_increases_on_deposits_leverage_and_entries_only(target):
    assert Lifecycle.increases_exposure(DepositIntent(target=target, amount=1))
    assert Lifecycle.increases_exposure(SetLeverage(target=target, leverage=10))
    assert Lifecycle.increases_exposure(IncreasePosition(target=target, side="short", size="1"))
    assert not Lifecycle.increases_exposure(Withdraw(target=target))
    assert not Lifecycle.increases_exposure(ClosePosition(target=target))


# Operations, their calls and the generated authority.


def custody_operations(target):
    usdc = AssetAmount(asset=USDC, amount=20_000_000)
    label = AgentNames().label("almanak1", T0 + 170 * 24 * 3600 * 1000)
    return (
        Approve(target=target, amount=usdc),
        Deposit(target=target, amount=usdc),
        Approve(target=target, amount=AssetAmount(asset=USDC, amount=0)),
        StandardMode(target=target),
        AgentApproval(target=target, agent=AGENT, label=label),
        ClassTransfer(target=target, amount=usdc, to_perp=True),
        ClassTransfer(target=target, amount=usdc, to_perp=False),
        Withdrawal(target=target, amount=usdc),
    )


def test_custody_operations_are_one_roles_call_each_within_the_generated_grants(configured):
    target = configured.target("perp")
    binding = configured.accounts.binding("safe").profile
    grants = derive(binding, declared(configured))
    operations = custody_operations(target)
    expected = (
        cw.approval(20_000_000),
        cw.deposit(20_000_000),
        cw.approval(0),
        cw.standard_mode(SAFE),
        cw.agent_approval(AGENT, operations[4].label),
        cw.class_transfer(20_000_000, True),
        cw.class_transfer(20_000_000, False),
        cw.withdrawal(20_000_000),
    )
    assert cw.deposit_calls(20_000_000) == expected[:3]
    for operation, core in zip(operations, expected, strict=True):
        call = Execution.call(configured, operation)
        assert (call.fund, call.to, call.value, call.data) == (target.fund, core.to, 0, core.data)
        (grant,) = (g for g in grants if (g.target, g.selector.hex()) == (call.to, call.data[2:10]))
        assert allows(grant.condition, bytes.fromhex(call.data[2:]))
    increases = [Execution.increases_exposure(configured, o) for o in operations]
    assert increases == [True, True, False, False, False, True, False, False]
    with pytest.raises(ValueError, match="nonzero amount"):
        Deposit(target=target, amount=AssetAmount(asset=USDC, amount=0))
    with pytest.raises(ValidationError):
        AgentApproval(target=target, agent=AGENT, label="almanak1")


def test_permissions_cover_the_profiles_safe_only(configured):
    capabilities = declared(configured)
    grants = derive(configured.accounts.binding("safe").profile, capabilities)
    assert {(g.target, g.signature) for g in grants} == {
        (cw.USDC_CONTRACT, "approve(address,uint256)"),
        (CORE_DEPOSIT_WALLET, "deposit(uint256,uint32)"),
        (cw.CORE_WRITER, cw.SEND_RAW_ACTION),
    }
    assert derive(profile("0x" + "77" * 20), capabilities) == ()
    identities = Permissions.identities(
        configured, "safe", SimpleNamespace(identity=lambda address: address)
    )
    assert identities == tuple(sorted((cw.USDC_CONTRACT, CORE_DEPOSIT_WALLET, cw.CORE_WRITER)))
    assert Permissions.identities(configured, "other", None) == ()
    assert any("D2" in check for check in Permissions.runtime_checks)


def test_venue_actions_carry_a_phase_3_attempt_of_their_market(state, target):
    _, attempt = rules.plan_leverage(state.trading, "BTC", AGENT, T0)
    action = VenueAction(target=target, attempt=attempt)
    with pytest.raises(ValueError, match="no HyperEVM call"):
        Execution.call(state.config, action)
    assert not Execution.increases_exposure(state.config, action)
    with pytest.raises(ValidationError, match="its own target's market"):
        VenueAction(target=target, attempt=attempt.model_copy(update={"coin": "ETH"}))
    with pytest.raises(ValidationError, match="leverage, entries and exits"):
        VenueAction(target=target, attempt=attempt.model_copy(update={"purpose": "protect"}))


def test_execution_counts_custody_gas_and_leaves_venue_steps_to_the_adapter(configured, target):
    (requirement,) = Execution.requirements(configured, "safe")
    assert (requirement.asset, requirement.amount) == (USDC, 30_000_000)
    # Approve (or revoke), deposit, standard mode, the first agent and the move to perp.
    assert Execution.entry_transactions(configured, "safe") == 5
    assert Execution.gas_lifecycle.kind("perp").exit == 2
    rows, ready = Execution.preflight(configured, "safe", (snapshot(target),))
    assert ready and rows[0]["funded"]
    held = owning(target, hypercore="3.0")
    _, ready = Execution.preflight(configured, "safe", (snapshot(target, owned=held),))
    assert not ready
    _, ready = Execution.preflight(configured, "safe", (snapshot(target, allowance=1),))
    assert not ready
    with pytest.raises(DataError, match="replacement"):
        Execution.validate_replacement(configured, None)
    # Planning reads the venue, which only the HyperCore gateway adapter can.
    with pytest.raises(ValueError, match="gateway adapter plans it"):
        Execution.operation(configured, None, None, None)


# Registration, support and resolution.


def test_connector_is_registered_for_hyperevm_production_forks_and_backtests(configured):
    assert "hyperliquid" in map(str, CONNECTOR_IDS)
    connector = connector_from_wire({"connector": "hyperliquid"})
    assert type(connector_for(configured).codec.config) is type(connector.codec.config)
    assert connector.supported_chains(RunMode.FORK) == (HYPEREVM.chain,)
    assert connector.supported_chains(RunMode.BACKTEST) == (HYPEREVM.chain,)
    assert connector.supported_chains(RunMode.PRODUCTION) == (HYPEREVM.chain,)
    support = connector.support_for(HYPEREVM.chain)
    assert support.account_profiles == ("safe_roles",)
    actions = ("deposit", "set_leverage", "increase_position", "close_position", "withdraw")
    assert support.actions == tuple(map(EconomicAction, actions))
    (historical,) = support.historical
    assert historical.actions == support.actions
    assert connector.economic.endswith("economic_binding:HyperliquidEconomic")
    assert ONE_ALLOCATION_PER_FUND in support.restrictions
    assert "hyperliquid" in ONE_ALLOCATION_PER_FUND_CONNECTORS
    support.require(RunMode.PRODUCTION, EconomicAction.DEPOSIT, "safe_roles")
    for mode in (RunMode.FORK, RunMode.PRODUCTION):
        with pytest.raises(ValueError, match="account profile"):
            support.require(mode, EconomicAction.DEPOSIT, "eoa")
    assert parameter_type(HYPERLIQUID) is Parameters
    assert allocation_connector(configured.allocation("perp")) == HYPERLIQUID
    assert target_connector(configured.target("perp")) == HYPERLIQUID


def test_plan_codec_defaults_to_a_custody_transaction():
    codec = connector_from_wire({"connector": "hyperliquid"}).codec
    with pytest.raises(ValidationError) as failure:
        codec.plan(attempt_id="a")
    assert "hyperliquid_venue_action" not in str(failure.value)


def test_author_binding_converts_the_operators_hype_reserve():
    from tests.hypercore_backtest_support import resolved

    binding = binding_for("hyperliquid")
    market = resolved()
    values = {
        "market": "btc",
        "account": "fund",
        "capital": {"asset": USDC, "quantity": "30"},
        "leverage": 10,
        "slippage_bps": 50,
    }
    plain = TypeAdapter(binding.allocation).validate_python(values).resolve("perp", market)
    assert plain.minimum_native_balance == 0
    reserved = (
        TypeAdapter(binding.allocation)
        .validate_python({**values, "minimum_native_balance": "0.05"})
        .resolve("perp", market)
    )
    assert reserved.minimum_native_balance == 5 * 10**16
    assert (reserved.market, reserved.chain) == (market.market, market.block.chain)


def test_qa_catalog_proves_the_fork_cell_with_the_reference_through_safe_roles():
    from tests.qa.catalog import catalog, required

    obligations = {o for o in required() if o.connector == HYPERLIQUID and o.mode == "fork"}
    cases = [c for c in catalog() if c.connector == HYPERLIQUID and c.mode == "fork"]
    assert obligations and not any(case.blocked_reason for case in cases)
    assert {(c.input_policy, c.profile, c.operator_mode) for c in cases} == {
        (policy, "safe_roles", operator)
        for policy in ("pinned", "fresh")
        for operator in ("configured", "distinct")
    }
    for case in cases:
        assert case.reference == "hyperliquid_hyperevm_perp" and case.lane == "fork"
        assert case.obligations() == obligations


def reference_strategy():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).parents[1] / "strategies/hyperliquid_hyperevm_perp/strategy.py"
    spec = importlib.util.spec_from_file_location("hyperliquid_reference", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def step(kind, *, complete=True, success=True, applied=True):
    """A stand-in outcome: a fork run's `ActionResult` or a backtest's result."""
    return SimpleNamespace(
        intent=SimpleNamespace(kind=kind, target=SimpleNamespace(allocation="perp")),
        complete=complete,
        success=success,
        applied=applied,
        effective_at=datetime(2026, 11, 6, tzinfo=UTC),
    )


def test_the_reference_records_only_its_completed_applied_entry_and_exit():
    strategy = reference_strategy()

    def remembered(result):
        return strategy.on_outcome(SimpleNamespace(result=result, memory=strategy.State()))

    # An agent renewal or leverage change before the order is an intermediate step, and
    # an `Ioc` order that matched nothing opened nothing.
    for result in (
        step("increase_position", complete=False),
        step("increase_position", applied=False),
        step("increase_position", success=False, complete=False),
        step("close_position", complete=False),
        EffectsResult.model_construct(),
    ):
        assert remembered(result) == strategy.State()
    opened = remembered(step("increase_position"))
    assert opened.opened_at == {"perp": 1_793_923_200} and opened.finished == ()
    assert remembered(step("close_position")).finished == ("perp",)
