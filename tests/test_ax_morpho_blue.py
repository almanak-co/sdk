"""Morpho Blue direct operator actions: the declaration, without signing or chain access.

Component evidence from literal market data and retained strategy checkpoints. The
whole command is rehearsed on forks in tests/fork/test_ax_morpho_blue.py.
"""

import json
from types import SimpleNamespace

import pytest

from almanak import ax_actions
from almanak.ax import lending_request
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct as declarations
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
from almanak.connectors.morpho_blue import capacity, direct
from almanak.connectors.morpho_blue.authority import admit, declared
from almanak.connectors.morpho_blue.direct import OTHER_LEG
from almanak.connectors.morpho_blue.domain import AccountObservation, Position, Reserve
from almanak.connectors.morpho_blue.lifecycle import MorphoLifecycle, validate_owned
from almanak.connectors.morpho_blue.observations import Ownership, Snapshot
from almanak.connectors.morpho_blue.operations import (
    ApproveOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.morpho_blue.planning import operation
from almanak.connectors.morpho_blue.reporting import report
from almanak.connectors.morpho_blue.state import Checkpoint
from almanak.connectors.morpho_blue.support import DIRECT_CHAINS, candidate_support
from almanak.connectors.support import EconomicAction
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount
from almanak.operator_deployment import adopts, supports_operator
from almanak.state import Journal
from tests.lending_captures import read_capture
from tests.morpho_blue_helpers import CASES, EVIDENCE
from tests.morpho_blue_helpers import candidate_morpho_support as candidate_morpho_support
from tests.morpho_blue_helpers import recorded_morpho as recorded_morpho
from tests.test_morpho_blue_offline import CHAIN, MARKET, SHARES, USDC, WETH, observation

WALLET = Wallet(account="treasury", address="0x" + "ab" * 20)
BLOCK = observation().block
# 1 WETH posted against 100 USDC of debt, beside 500 USDC lent.
INDEBTED = Position(
    supply_shares=500 * 10**6 * SHARES, borrow_shares=100 * 10**6 * SHARES, collateral=10**18
)
EMPTY = Position(supply_shares=0, borrow_shares=0, collateral=0)


class Positions:
    """A reader that answers the core's `position` call and records what was asked."""

    def __init__(self, held: Position):
        self.held, self.calls = held, []

    def call(self, to, signature, block, *, inputs, args, outputs):
        self.calls.append((to, signature, block, args))
        return (self.held.supply_shares, self.held.borrow_shares, self.held.collateral)


def bound(asset, held=EMPTY, action="supply"):
    reader = Positions(held)
    resolved = SimpleNamespace(reserve=Reserve(market=MARKET, asset=asset))
    document = direct.positions(reader, BLOCK, resolved, WALLET, ActionRequest(action, 1))
    return reader, document


def configured(asset, held=EMPTY):
    _, document = bound(asset, held)
    return ax_actions.configuration(
        "morpho_blue",
        document,
        WALLET,
        ax_actions.alchemy(CHAIN.chain_id),
        CHAIN,
        ax_actions.FeeLimits(max_gas_price_wei=5, max_transaction_fee=9),
    )


def observed(config, held, *, allowance=0):
    """Each configured leg as the gateway observes it, at one block."""
    snapshots = []
    for allocation in config.allocations:
        target = config.target(allocation.name)
        account = AccountObservation(market=observation(), fund=target.fund, position=held)
        snapshots.append(
            Snapshot(
                target=target,
                account=account,
                wallet_balance=10**24,
                allowance=allowance if allocation.name == ALLOCATION else 0,
                native_balance=AssetAmount(asset=native_asset(CHAIN.chain_id), amount=10**18),
                payer=target.fund,
            )
        )
    return tuple(snapshots)


def retained(config, intent):
    return SimpleNamespace(
        capabilities=declared(config),
        decision=SimpleNamespace(identity="retained", intent=intent),
        outcomes=(),
    )


def planned(config, snapshots, request):
    """The connector's own next step for a request, after its capacity checks."""
    intent = direct.intent(config, snapshots, request)
    before = direct.requested(snapshots)
    step = operation(config, retained(config, intent), intent, before)
    if isinstance(step, ApproveOperation):
        before = before.model_copy(update={"allowance": step.amount.amount})
        step = SupplyOperation(target=step.target, amount=step.amount)
    return step, capacity.validate(config, before, step)


def test_morpho_blue_declares_supply_and_full_withdrawal_where_it_is_rehearsed():
    assert supports_operator(Checkpoint)
    assert "morpho_blue" not in declarations.NOT_DIRECT
    assert declarations.binding("morpho_blue").actions == ("supply", "withdraw")
    cells = {str(cell.chain): cell for cell in candidate_support()}
    # Every production chain but HyperEVM, whose provider does not serve reads at the head.
    assert set(cells) - DIRECT_CHAINS == {"eip155:999"}
    for chain, cell in cells.items():
        expected = (
            (EconomicAction.SUPPLY, EconomicAction.WITHDRAW) if chain in DIRECT_CHAINS else ()
        )
        assert cell.direct_actions == expected
    for chain in DIRECT_CHAINS:
        declarations.require_direct("morpho_blue", cells[chain].chain, "supply")
        declarations.require_direct("morpho_blue", cells[chain].chain, "withdraw")
        with pytest.raises(ValueError, match="morpho_blue serves no direct borrow"):
            declarations.require_direct("morpho_blue", cells[chain].chain, "borrow")


def test_the_command_takes_a_market_and_a_leg():
    arguments = [
        "--connector",
        "morpho_blue",
        "--chain",
        "eip155:1",
        "--market-id",
        str(MARKET.market_id),
        "--leg",
        "collateral",
        "--wallet",
        "wallet",
        "--env-file",
        ".env",
        "--max-gas-price-wei",
        "5",
        "--max-transaction-fee",
        "9",
    ]
    supply = parse_validated(build_parser(), ["ax", "supply", *arguments, "--amount", "7"])
    assert (supply.product, supply.connector, supply.amount) == (
        "operator-action",
        "morpho_blue",
        7,
    )
    selected = lending_request(supply)
    assert (selected.market_id, selected.leg) == (MARKET.market_id, "collateral")
    withdraw = parse_validated(build_parser(), ["ax", "withdraw", *arguments])
    assert withdraw.amount is None and not withdraw.execute


@pytest.mark.parametrize("case", CASES, ids=lambda row: row["name"][-12:])
def test_a_strategy_checkpoint_keeps_its_wire_form(case):
    text = read_capture(EVIDENCE / case["name"] / "checkpoint.json.gz")
    state = Checkpoint.model_validate_json(text)
    wire = state.model_dump_json()
    # Field for field what the capture holds: no operator marker, and the strategy kept.
    assert json.loads(wire) == json.loads(text)
    assert not state.operator and "operator" not in json.loads(wire)
    assert Checkpoint.model_validate_json(wire).model_dump_json() == wire
    orphan = {**json.loads(wire), "strategy": None}
    with pytest.raises(ValueError, match="a strategy deployment retains its strategy"):
        Checkpoint.model_validate_json(json.dumps(orphan))


def test_an_operator_checkpoint_round_trips_without_a_strategy(tmp_path, recorded_morpho):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(recorded_morpho.config, None)
    state = journal.load()
    assert state.operator and state.strategy is None and not state.initialized
    assert state.capabilities == declared(recorded_morpho.config)
    assert all(item.empty for item in state.owned)
    wire = json.loads(state.model_dump_json())
    assert wire["operator"] is True and wire["strategy"] is None
    assert Checkpoint.model_validate_json(json.dumps(wire)) == state
    assert report(state)["strategy"] is None
    assert report(recorded_morpho)["strategy"]["sha256"] == recorded_morpho.strategy.sha256
    claimed = {**wire, "strategy": recorded_morpho.strategy.model_dump(mode="json")}
    with pytest.raises(ValueError, match="an operator deployment retains no strategy"):
        Checkpoint.model_validate_json(json.dumps(claimed))


def test_adoption_takes_ownership_from_the_observed_position(recorded_morpho):
    empty = MorphoLifecycle.initial(
        recorded_morpho.config, None, recorded_morpho.capabilities, None
    )
    assert MorphoLifecycle.adopt(empty.owned, recorded_morpho.baseline) == empty.owned
    for outcome in recorded_morpho.outcomes:
        owned = tuple(item for item in empty.owned if item.target == outcome.operation.target)
        adopted = MorphoLifecycle.adopt(owned, (outcome.after,))
        assert adopted == MorphoLifecycle.commit_owned(owned, outcome)
    # The observations of one decision taken while debt was open: every leg at one block.
    repay = next(item for item in recorded_morpho.outcomes if item.operation.kind == "repay")
    seen = repay.decision.observations
    adopted = MorphoLifecycle.adopt(empty.owned, seen)
    assert any(item.borrow_shares for item in adopted)
    assert any(item.collateral for item in adopted)
    holding = empty.model_copy(update={"initialized": True, "baseline": seen, "owned": adopted})
    assert adopts(empty, holding)
    validate_owned(holding, seen)
    with pytest.raises(DataError, match="must cover every configured Morpho allocation"):
        MorphoLifecycle.adopt(empty.owned, seen[:1])


@pytest.mark.parametrize(
    ("asset", "held", "legs"),
    [
        (USDC, EMPTY, [USDC]),
        (WETH, EMPTY, [WETH]),
        # The requested leg alone needs no second allocation.
        (USDC, Position(supply_shares=7, borrow_shares=3, collateral=0), [USDC]),
        (WETH, Position(supply_shares=0, borrow_shares=0, collateral=7), [WETH]),
        (USDC, Position(supply_shares=0, borrow_shares=0, collateral=7), [USDC, WETH]),
        (WETH, Position(supply_shares=7, borrow_shares=0, collateral=0), [WETH, USDC]),
        (WETH, Position(supply_shares=0, borrow_shares=3, collateral=7), [WETH, USDC]),
        (USDC, INDEBTED, [USDC, WETH]),
    ],
)
def test_the_deployment_binds_every_leg_the_wallet_holds(asset, held, legs):
    reader, document = bound(asset, held)
    assert reader.calls == [
        (
            MARKET.core,
            "position(bytes32,address)",
            BLOCK,
            (bytes.fromhex(str(MARKET.market_id)[2:]), WALLET.address),
        )
    ]
    assert document["connector"] == "morpho_blue"
    names = [ALLOCATION, OTHER_LEG][: len(legs)]
    assert [item["name"] for item in document["allocations"]] == names
    assert [item["reserve"]["asset"] for item in document["allocations"]] == [str(a) for a in legs]
    config = configured(asset, held)
    assert declarations.binding_for(config) is declarations.binding("morpho_blue")
    assert config.target(ALLOCATION).reserve == Reserve(market=MARKET, asset=asset)
    assert config.target(ALLOCATION).fund.address == WALLET.address
    for allocation in config.allocations:
        assert (allocation.account, allocation.minimum_native_balance) == ("treasury", 0)
        assert not allocation.borrow and allocation.minimum_health_factor is None
    assert not any(item.borrow for item in declared(config).requirements)
    # The ownership check, unchanged, accepts exactly this binding of the position.
    snapshots = observed(config, held)
    state = SimpleNamespace(owned=adopt(config, snapshots))
    validate_owned(state, snapshots)


def adopt(config, snapshots):
    empty = tuple(
        Ownership(target=config.target(a.name), supply_shares=0, borrow_shares=0, collateral=0)
        for a in config.allocations
    )
    return MorphoLifecycle.adopt(empty, snapshots)


def test_a_single_leg_binding_of_a_two_leg_position_is_still_refused():
    config = configured(USDC)
    held = Position(supply_shares=7, borrow_shares=0, collateral=7)
    snapshots = observed(config, held)
    state = SimpleNamespace(owned=adopt(config, snapshots))
    with pytest.raises(DataError, match="not owned by this deployment"):
        validate_owned(state, snapshots)


def test_an_action_states_its_amount_exactly_once():
    config = configured(USDC)
    snapshots = observed(config, EMPTY)
    supply = direct.intent(config, snapshots, ActionRequest("supply", 7))
    assert (supply.kind, supply.amount.amount, supply.target) == (
        "supply",
        7,
        config.target(ALLOCATION),
    )
    lent = observed(config, Position(supply_shares=7, borrow_shares=0, collateral=0))
    withdraw = direct.intent(config, lent, ActionRequest("withdraw"))
    assert (withdraw.kind, withdraw.target) == ("withdraw", config.target(ALLOCATION))
    with pytest.raises(ValueError, match="supply requires --amount"):
        direct.intent(config, snapshots, ActionRequest("supply"))
    with pytest.raises(ValueError, match="withdraw takes no --amount"):
        direct.intent(config, snapshots, ActionRequest("withdraw", 7))
    with pytest.raises(ValueError, match="requested position was not observed"):
        direct.intent(config, (), ActionRequest("withdraw"))


def test_a_withdrawal_needs_a_supplied_balance_or_an_allowance_to_revoke():
    config = configured(USDC)
    with pytest.raises(DataError, match="nothing is supplied on this leg") as refusal:
        direct.intent(config, observed(config, EMPTY), ActionRequest("withdraw"))
    assert refusal.value.kind == "no_position" and "owes" not in str(refusal.value)
    # Debt alone is not a supplied balance, and the refusal says what is open instead.
    owing = Position(supply_shares=0, borrow_shares=3, collateral=7)
    config = configured(USDC, owing)
    with pytest.raises(DataError, match="owes 3 borrow shares, which direct actions do not repay"):
        direct.intent(config, observed(config, owing), ActionRequest("withdraw"))
    stray = observed(config, owing, allowance=7)
    assert direct.intent(config, stray, ActionRequest("withdraw")).kind == "withdraw"


def test_a_supply_uses_no_allowance_or_exactly_its_own():
    config = configured(USDC)
    approved = observed(config, EMPTY, allowance=7)
    assert direct.intent(config, approved, ActionRequest("supply", 7)).amount.amount == 7
    with pytest.raises(DataError, match="the wallet approves 7 base units to Morpho") as refusal:
        direct.intent(config, approved, ActionRequest("supply", 8))
    assert refusal.value.kind == "allowance_mismatch"
    # A withdrawal is what clears a stray allowance, so it is never refused for one.
    assert direct.intent(config, approved, ActionRequest("withdraw")).kind == "withdraw"


def test_the_result_names_the_requested_leg_and_the_other_when_bound():
    config = configured(WETH, INDEBTED)
    owned = adopt(config, observed(config, INDEBTED))
    # Found by name: the order of the retained ownership does not matter.
    for order in (owned, owned[::-1]):
        assert direct.position(SimpleNamespace(owned=order)) == {
            "leg": "collateral",
            "collateral": str(10**18),
            "other_leg": {
                "leg": "loan",
                "supply_shares": str(500 * 10**6 * SHARES),
                "borrow_shares": str(100 * 10**6 * SHARES),
            },
        }
    single = configured(USDC)
    assert direct.position(SimpleNamespace(owned=adopt(single, observed(single, EMPTY)))) == {
        "leg": "loan",
        "supply_shares": "0",
        "borrow_shares": "0",
    }


def test_a_wallet_with_debt_cannot_withdraw_its_collateral():
    config = configured(WETH, INDEBTED)
    snapshots = observed(config, INDEBTED)
    with pytest.raises(DataError, match="direct actions do not repay") as refusal:
        direct.intent(config, snapshots, ActionRequest("withdraw"))
    assert refusal.value.kind == "open_debt"
    assert str(INDEBTED.borrow_shares) in str(refusal.value)
    # Without that refusal the connector's own check stops the step, less clearly: a
    # deployment that states no health floor tolerates no debt.
    target = config.target(ALLOCATION)
    step = WithdrawOperation(
        target=target, amount=AssetAmount(asset=WETH, amount=INDEBTED.collateral), full=True
    )
    with pytest.raises(ValueError, match="violates the configured fund/market health floor"):
        capacity.validate(config, direct.requested(snapshots), step)
    # The same wallet without debt withdraws its collateral.
    free = INDEBTED.model_copy(update={"borrow_shares": 0})
    step, result = planned(config, observed(config, free), ActionRequest("withdraw"))
    assert isinstance(step, WithdrawOperation) and result.position.collateral == 0


def test_a_wallet_with_debt_keeps_every_action_that_cannot_lower_its_health():
    posting = configured(WETH, INDEBTED)
    step, result = planned(posting, observed(posting, INDEBTED), ActionRequest("supply", 10**17))
    assert isinstance(step, SupplyOperation)
    assert result.position.collateral == INDEBTED.collateral + 10**17
    lending = configured(USDC, INDEBTED)
    snapshots = observed(lending, INDEBTED)
    step, result = planned(lending, snapshots, ActionRequest("supply", 10**6))
    assert isinstance(step, SupplyOperation)
    assert result.position.supply_shares > INDEBTED.supply_shares
    step, result = planned(lending, snapshots, ActionRequest("withdraw"))
    assert isinstance(step, WithdrawOperation) and step.shares == INDEBTED.supply_shares
    assert (result.position.supply_shares, result.position.borrow_shares) == (
        0,
        INDEBTED.borrow_shares,
    )
    # Neither leg can borrow or repay.
    for intent in (lending.borrow(ALLOCATION, 1), lending.repay(ALLOCATION)):
        with pytest.raises(ValueError, match="require a borrowing allocation"):
            admit(declared(lending), intent)
