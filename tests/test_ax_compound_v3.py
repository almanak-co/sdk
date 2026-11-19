"""Compound V3 direct operator actions: the deployment each `ax` lending command builds.

Component evidence against retained Comet reads and checkpoints, without signing or
chain access. The fork proof of the whole command is tests/fork/test_ax_compound_v3.py.
"""

import json
import re
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from almanak import ax_actions
from almanak.ax import lending_request
from almanak.caip import ChainId
from almanak.cli_arguments import build_parser, parse_validated
from almanak.connectors import direct as declarations
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.compound_v3 import capacity, direct, planning
from almanak.connectors.compound_v3.authority import declared
from almanak.connectors.compound_v3.domain import Reserve
from almanak.connectors.compound_v3.execution import validate_operation
from almanak.connectors.compound_v3.lifecycle import CompoundLifecycle, adopt_owned, validate_owned
from almanak.connectors.compound_v3.observations import Ownership, Snapshot, owned
from almanak.connectors.compound_v3.operations import SupplyOperation
from almanak.connectors.compound_v3.reader import read_account, read_market
from almanak.connectors.compound_v3.reporting import report
from almanak.connectors.compound_v3.state import Checkpoint
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.models import Account, AssetAmount
from almanak.operator_deployment import supports_operator
from almanak.state import operator_capabilities
from tests.compound_v3_helpers import candidate_compound_support as candidate_compound_support
from tests.compound_v3_helpers import recorded_compound as recorded_compound
from tests.test_compound_v3_lifecycle import positions as lifecycle_positions
from tests.test_compound_v3_reader import Replay, recorded

# The retained funded capture: a supplier of the base asset, and a borrower of it
# against one collateral asset.
SUPPLIER, BORROWER = 0, 1
NOBODY = "0x0000000000000000000000000000000000000001"
LIMITS = ax_actions.FeeLimits(max_gas_price_wei=5 * 10**9, max_transaction_fee=5 * 10**15)
AMOUNT = 10**6
# Health factors at wad precision: one the retained borrower clears, and one no
# position with debt reaches.
FLOOR = 15 * 10**17
UNREACHABLE = 10**60
SUPPLY = ActionRequest("supply", AMOUNT)
# One request of every intent kind the connector declares.
REQUESTS = (
    SUPPLY,
    ActionRequest("withdraw"),
    ActionRequest("withdraw", AMOUNT),
    ActionRequest("borrow", AMOUNT, FLOOR),
    ActionRequest("repay"),
    ActionRequest("repay", AMOUNT),
)


class Mainnet(Replay):
    """The retained fork reads under the identity of the chain the fork was taken from."""

    def _rpc(self, method, params, *, allow_none=False):
        if method == "eth_chainId":
            return "0x1"
        return super()._rpc(method, params, allow_none=allow_none)


@contextmanager
def comet(funded=True):
    """The retained mainnet Comet reads, as a command reads them from its endpoint."""
    raw, data = recorded("mainnet", funded)
    data = json.loads(re.sub(r'"anvil:0x[0-9a-f]+"', '"mainnet"', json.dumps(data)))
    with Mainnet(raw, data) as reader:
        block = reader.pin_block(reader.block.number)
        market = read_market(reader, reader.source, block)
        addresses = [a["fund"]["address"] for a in data["accounts"]] if funded else [NOBODY]
        yield SimpleNamespace(reader=reader, block=block, market=market, addresses=addresses)


def reserve(market, asset=None):
    source = market.source
    if asset is None or asset == source.base_asset:
        return Reserve(market=source, asset=source.base_asset, price_feed=source.base_price_feed)
    listed = next(c for c in market.collaterals if c.asset_type == asset)
    return Reserve(market=source, asset=asset, price_feed=listed.price_feed)


def held_collateral(account):
    return next(c.asset for c in account.collaterals if c.balance)


def unheld_collateral(account):
    return next(c.asset for c in account.collaterals if not c.balance)


class Deployment:
    """What one command builds for one wallet: its configuration and observations."""

    def __init__(self, venue, address, asset=None, request=SUPPLY, **snapshot):
        self.wallet = Wallet(account="treasury", address=address)
        self.fund = Account(chain=venue.market.source.chain, address=address)
        self.account = read_account(venue.reader, venue.market, self.fund)
        self.request = request
        resolved = SimpleNamespace(reserve=reserve(venue.market, asset))
        self.document = direct.positions(venue.reader, venue.block, resolved, self.wallet, request)
        self.config = self.configuration(self.document)
        self.snapshot = snapshot

    def configuration(self, document):
        chain = self.fund.chain
        provider = ax_actions.alchemy(chain.chain_id)
        return ax_actions.configuration(
            "compound_v3", document, self.wallet, provider, chain, LIMITS
        )

    def names(self):
        return [item.name for item in self.config.allocations]

    def floors(self):
        return [item.minimum_health_factor for item in self.config.allocations]

    def authority(self):
        return [item.borrow for item in self.config.allocations]

    def observed(self, config=None):
        config = config or self.config
        values = {"wallet_balance": 10**24, "allowance": 0, **self.snapshot}
        return tuple(
            Snapshot(
                target=config.target(item.name),
                account=self.account,
                native_balance=AssetAmount(
                    asset=native_asset(self.fund.chain.chain_id), amount=10**18
                ),
                payer=self.fund,
                # Only the requested position's token balance and allowance are varied.
                **(values if item.name == ALLOCATION else {"wallet_balance": 0, "allowance": 0}),
            )
            for item in config.allocations
        )

    def state(self, config=None):
        """The operator checkpoint after it adopted what the observations state."""
        config = config or self.config
        observed = self.observed(config)
        created = CompoundLifecycle.initial(config, None, declared(config), None)
        return created.model_copy(
            update={
                "initialized": True,
                "baseline": observed,
                "owned": CompoundLifecycle.adopt(created.owned, observed),
            }
        )

    def intent(self, action=None, amount=None, floor=None):
        """The intent of the deployment's own request, or of another one stated here."""
        request = self.request if action is None else ActionRequest(action, amount, floor)
        return direct.intent(self.config, self.observed(), request)

    def planned(self, intent=None):
        """The connector's own first step for an intent, and its admission checks."""
        intent = intent or self.intent()
        state = self.state()
        deciding = SimpleNamespace(
            **{name: getattr(state, name) for name in ("config", "capabilities", "owned")},
            mode="running",
            outcomes=(),
            baseline=state.baseline,
            decision=SimpleNamespace(intent=intent, identity="request"),
        )
        before = direct.requested(state.baseline)
        operation = planning.operation(self.config, deciding, intent, before)
        backend = SimpleNamespace(config=self.config, guard=lambda: None)
        validate_operation(backend, deciding, operation, before)
        return operation

    def without_debt(self, principal=0, supplied_base=0):
        """The same wallet as if it owed nothing, with an optional base supply."""
        values = {"principal": principal, "supplied_base": supplied_base, "borrowed_base": 0}
        self.account = self.account.model_copy(update=values)
        return self


def test_an_operator_checkpoint_round_trips_and_retains_no_strategy():
    with comet() as venue:
        state = Deployment(venue, venue.addresses[BORROWER]).state()
    assert state.operator and state.strategy is None
    wire = json.loads(state.model_dump_json())
    assert (wire["operator"], wire["strategy"]) == (True, None)
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state
    assert report(state)["strategy"] is None
    with pytest.raises(ValueError, match="a strategy deployment retains its strategy"):
        Checkpoint.model_validate_json(json.dumps({**wire, "operator": False}))


def test_a_strategy_checkpoint_keeps_its_wire_form(recorded_compound):
    text = recorded_compound.model_dump_json()
    wire = json.loads(text)
    # The marker is omitted when false, and nothing else sits between its neighbours.
    assert "operator" not in wire and not recorded_compound.operator
    names = list(wire)
    assert names[names.index("strategy") + 1] == "capabilities"
    assert Checkpoint.model_validate_json(text).model_dump_json() == text
    assert report(recorded_compound)["strategy"]["sha256"] == recorded_compound.strategy.sha256
    with pytest.raises(ValueError, match="an operator deployment retains no strategy"):
        Checkpoint.model_validate_json(json.dumps({**wire, "operator": True}))
    with pytest.raises(ValueError, match="a strategy deployment retains its strategy"):
        Checkpoint.model_validate_json(json.dumps({**wire, "strategy": None}))


def test_adoption_takes_each_position_from_its_observation():
    observations = lifecycle_positions()
    empty = tuple(Ownership(target=o.target, principal=0, collateral=0) for o in observations)
    adopted = adopt_owned(empty, observations)
    assert adopted == tuple(owned(o) for o in observations)
    # Collateral in base units, and the base principal signed: this wallet is in debt.
    assert adopted[0].collateral == 10**18 and adopted[1].principal < 0
    assert CompoundLifecycle.adopt is adopt_owned
    with pytest.raises(DataError, match="every configured Comet position"):
        adopt_owned(empty, observations[::-1])
    with pytest.raises(DataError, match="every configured Comet position"):
        adopt_owned(empty, observations[:1])


def test_a_wallet_with_nothing_gets_the_requested_position_only():
    with comet(funded=False) as venue:
        deployment = Deployment(venue, NOBODY)
    (allocation,) = deployment.config.allocations
    assert deployment.document["connector"] == deployment.config.connector == "compound_v3"
    assert (allocation.name, allocation.account) == (ALLOCATION, "treasury")
    assert allocation.reserve.is_base and not allocation.borrow
    assert allocation.minimum_health_factor is None and allocation.minimum_native_balance == 0
    assert allocation.native_asset == native_asset(ChainId("eip155:1"))
    assert declarations.binding_for(deployment.config) is direct.DIRECT
    assert direct.position(deployment.state()) == {
        "asset": str(allocation.reserve.asset),
        "base_principal": "0",
        "collateral": "0",
    }


def test_a_wallet_holding_only_the_requested_leg_gets_that_position_only():
    with comet() as venue:
        deployment = Deployment(venue, venue.addresses[SUPPLIER])
    assert deployment.names() == [ALLOCATION]
    state = deployment.state()
    validate_owned(state, state.baseline)
    assert direct.position(state)["base_principal"] == str(deployment.account.principal)
    assert deployment.account.principal > 0


def test_the_deployment_covers_every_other_leg_the_wallet_holds():
    with comet() as venue:
        address = venue.addresses[BORROWER]
        base = venue.market.source.base_asset
        loan = Deployment(venue, address)
        held = held_collateral(loan.account)
        collateral = Deployment(venue, address, held)
        other = Deployment(venue, address, unheld_collateral(loan.account))
    assert loan.names() == [ALLOCATION, direct.held_name(held)]
    assert collateral.names() == [ALLOCATION, direct.held_name(base)]
    assert other.names() == [ALLOCATION, direct.held_name(base), direct.held_name(held)]
    assert direct.held_name(held) == f"held-{held.reference}"
    for deployment in (loan, collateral, other):
        state = deployment.state()
        validate_owned(state, state.baseline)
        # A supply states no borrow authority and, without the flag, no floor.
        assert not any(deployment.authority()) and set(deployment.floors()) == {None}
    # Whole-market ownership refuses a deployment of the requested leg alone.
    alone = {**other.document, "allocations": other.document["allocations"][:1]}
    partial = other.state(other.configuration(alone))
    with pytest.raises(DataError, match="base principal is not owned by this deployment"):
        validate_owned(partial, partial.baseline)


def test_each_command_builds_the_intent_kind_its_amount_selects():
    with comet() as venue:
        supplier = Deployment(venue, venue.addresses[SUPPLIER])
        borrower = venue.addresses[BORROWER]
        borrow = Deployment(venue, borrower, request=REQUESTS[3])
        repay = Deployment(venue, borrower, request=REQUESTS[4])
    built = [
        supplier.intent("supply", AMOUNT),
        supplier.intent("withdraw"),
        supplier.intent("withdraw", AMOUNT),
        borrow.intent(),
        repay.intent(),
        repay.intent("repay", AMOUNT),
    ]
    # The gateway admits an operator request by this kind, as the chain declares it.
    assert [item.kind for item in built] == [request.kind for request in REQUESTS]
    assert all(item.target == supplier.config.target(ALLOCATION) for item in built[:3])
    assert all(item.target == borrow.config.target(ALLOCATION) for item in built[3:])
    amounts = [getattr(item, "amount", None) for item in built]
    assert [stated and stated.amount for stated in amounts] == [
        request.amount for request in REQUESTS
    ]
    for action in ("supply", "borrow"):
        with pytest.raises(ValueError, match=f"{action} requires --amount"):
            supplier.intent(action)
    assert supplier.planned(built[0]).kind == "approve"
    assert [supplier.planned(item).full for item in built[1:3]] == [True, False]


def test_the_position_reports_the_wallet_base_principal_beside_the_requested_asset():
    with comet() as venue:
        address = venue.addresses[BORROWER]
        held = held_collateral(Deployment(venue, address).account)
        deployment = Deployment(venue, address, held)
    state = deployment.state()
    # The debt is visible on a command for a collateral asset too.
    assert deployment.account.principal < 0
    assert direct.position(state) == {
        "asset": str(held),
        "base_principal": str(deployment.account.principal),
        "collateral": str(10**18),
    }
    # The requested position is found wherever it sits among the owned positions.
    moved = SimpleNamespace(config=state.config, owned=state.owned[::-1])
    assert direct.position(moved) == direct.position(state)


def test_a_borrow_states_its_authority_on_the_base_leg_and_one_floor_on_every_leg():
    with comet() as venue:
        address = venue.addresses[BORROWER]
        request = ActionRequest("borrow", AMOUNT, FLOOR)
        borrow = Deployment(venue, address, request=request)
        strict = Deployment(venue, address, request=ActionRequest("borrow", AMOUNT, UNREACHABLE))
        held = held_collateral(borrow.account)
        with pytest.raises(ValueError, match="borrow requires --min-health-factor"):
            Deployment(venue, address, request=ActionRequest("borrow", AMOUNT))
        with pytest.raises(ValueError, match="borrow names the Comet's base asset.*is collateral"):
            Deployment(venue, address, held, request=request)
        with pytest.raises(ValueError, match="supply collateral before borrowing"):
            Deployment(venue, venue.addresses[SUPPLIER], request=request)
    assert (borrow.authority(), borrow.floors()) == ([True, False], [FLOOR, FLOOR])
    capabilities = borrow.state().capabilities.requirements
    assert [item.borrow for item in capabilities] == [True, False]
    step = borrow.planned()
    assert (step.kind, step.amount.amount) == ("borrow", AMOUNT)
    with pytest.raises(DataError, match="below the configured minimum") as refused:
        strict.planned()
    assert refused.value.kind == "health_factor"


def test_a_borrow_never_withdraws_the_wallets_own_base_supply():
    with comet() as venue:
        request = ActionRequest("borrow", AMOUNT, FLOOR)
        borrow = Deployment(venue, venue.addresses[BORROWER], request=request)
    lender = borrow.without_debt(principal=5 * AMOUNT, supplied_base=6 * AMOUNT)
    with pytest.raises(DataError, match=f"has {6 * AMOUNT} base units.*use `ax withdraw`") as own:
        lender.intent()
    assert own.value.kind == "unsupported"
    with pytest.raises(ValueError, match="cannot silently withdraw existing base supply"):
        lender.planned(lender.config.borrow(ALLOCATION, AMOUNT))


def test_a_repayment_takes_borrow_authority_and_no_floor_can_refuse_it():
    with comet() as venue:
        address = venue.addresses[BORROWER]
        whole = Deployment(venue, address, request=ActionRequest("repay"))
        part = Deployment(venue, address, request=ActionRequest("repay", AMOUNT))
        stated = Deployment(venue, address, request=ActionRequest("repay", None, UNREACHABLE))
        held = held_collateral(whole.account)
        with pytest.raises(ValueError, match="repay names the Comet's base asset"):
            Deployment(venue, address, held, request=ActionRequest("repay"))
    for deployment in (whole, part):
        # The lowest floor the connector accepts, on every leg.
        assert deployment.authority() == [True, False]
        assert deployment.floors() == [10**18 + 1] * 2 == [direct.REPAY_FLOOR] * 2
    assert stated.floors() == [UNREACHABLE] * 2
    # The whole debt is repaid from an approval of the wallet's held balance.
    for deployment in (whole, stated):
        step = deployment.planned()
        assert (step.kind, step.purpose, step.amount.amount) == ("approve", "repay", 10**24)
    step = part.planned()
    assert (step.kind, step.purpose, step.amount.amount) == ("approve", "repay", AMOUNT)
    over = whole.account.borrowed_base + 1
    with pytest.raises(DataError, match="partial repayment exceeds the owned debt"):
        part.planned(part.intent("repay", over))


def test_a_repayment_needs_debt_and_an_allowance_it_can_use():
    with comet() as venue:
        request = ActionRequest("repay")
        nothing = Deployment(venue, venue.addresses[SUPPLIER], request=request)
        stale = Deployment(venue, venue.addresses[BORROWER], request=request, allowance=AMOUNT)
    # Without debt the position takes no borrow authority, and the request is refused.
    assert nothing.authority() == [False]
    with pytest.raises(DataError, match="nothing to repay: the wallet owes no ") as refused:
        nothing.intent()
    assert refused.value.kind == "no_position"
    with pytest.raises(DataError, match="a repayment uses an allowance.*repay the whole debt"):
        stale.intent("repay", AMOUNT + 1)
    assert stale.planned(stale.intent("repay", AMOUNT)).kind == "repay"
    # A full repayment replaces an allowance below the debt.
    assert stale.planned().kind == "revoke"


def test_a_supply_never_repays_and_a_base_withdrawal_never_borrows():
    with comet() as venue:
        loan = Deployment(venue, venue.addresses[BORROWER])
        lender = Deployment(venue, venue.addresses[SUPPLIER])
    owed = f"the wallet owes {loan.account.borrowed_base} base units of "
    with pytest.raises(DataError, match=owed + ".*would repay that debt; use `ax repay`") as repay:
        loan.intent("supply", AMOUNT)
    with pytest.raises(ValueError, match="cannot silently repay existing base debt"):
        loan.planned(loan.config.supply(ALLOCATION, AMOUNT))
    # In debt there is no base supply, so no base withdrawal is planned at all.
    for amount in (None, AMOUNT):
        with pytest.raises(DataError, match="nothing to withdraw.*" + owed) as nothing:
            loan.intent("withdraw", amount)
    with pytest.raises(DataError, match="closed with no remaining allowance"):
        loan.planned(loan.config.withdraw_amount(ALLOCATION, AMOUNT))
    held = lender.account.supplied_base
    with pytest.raises(DataError, match=f"has {held} base units.*would borrow") as beyond:
        lender.intent("withdraw", held + 1)
    with pytest.raises(ValueError, match="withdrawal exceeds its owned base supply"):
        lender.planned(lender.config.withdraw_amount(ALLOCATION, held + 1))
    assert lender.planned(lender.intent("withdraw", held)).amount.amount == held
    kinds = [caught.value.kind for caught in (repay, nothing, beyond)]
    assert kinds == ["unsupported", "no_position", "withdrawal_exceeds_supply"]


def test_collateral_leaves_a_wallet_in_debt_only_under_a_stated_floor():
    with comet() as venue:
        address = venue.addresses[BORROWER]
        held = held_collateral(Deployment(venue, address).account)
        bare = Deployment(venue, address, held, request=ActionRequest("withdraw", AMOUNT))
        floored = Deployment(venue, address, held, request=ActionRequest("withdraw", AMOUNT, FLOOR))
        strict = Deployment(
            venue, address, held, request=ActionRequest("withdraw", AMOUNT, UNREACHABLE)
        )
    for amount in (None, AMOUNT):
        with pytest.raises(ValueError, match="debt in this Comet.*state --min-health-factor"):
            bare.intent("withdraw", amount)
    # Unstated, the connector's own refusal reports a minimum nobody configured.
    with pytest.raises(DataError, match="below the configured minimum"):
        bare.planned(bare.config.withdraw_amount(ALLOCATION, AMOUNT))
    assert (floored.authority(), floored.floors()) == ([False, False], [FLOOR, FLOOR])
    step = floored.planned()
    assert (step.kind, step.full, step.amount.amount) == ("withdraw", False, AMOUNT)
    with pytest.raises(DataError, match="below the configured minimum"):
        strict.planned()
    # The whole collateral cannot leave while it backs the debt, whatever the floor.
    with pytest.raises(DataError, match="collateral cannot cover the projected debt"):
        floored.planned(floored.intent("withdraw", None, FLOOR))
    with pytest.raises(DataError, match=f"has {10**18} base units of .* supplied") as beyond:
        floored.intent("withdraw", 10**18 + 1, FLOOR)
    assert "would borrow" not in str(beyond.value)


def test_no_stated_floor_refuses_a_full_withdrawal_of_an_unencumbered_leg():
    with comet() as venue:
        request = ActionRequest("withdraw", None, UNREACHABLE)
        lender = Deployment(venue, venue.addresses[SUPPLIER], request=request)
        address = venue.addresses[BORROWER]
        held = held_collateral(Deployment(venue, address).account)
        collateral = Deployment(venue, address, held, request=request).without_debt()
    assert lender.floors() == [UNREACHABLE]
    step = lender.planned()
    supplied = lender.account.supplied_base
    assert (step.kind, step.full, step.amount.amount) == ("withdraw", True, supplied)
    step = collateral.planned()
    assert (step.kind, step.full, step.amount.amount) == ("withdraw", True, 10**18)


def test_a_wallet_in_debt_adds_collateral_without_a_floor():
    with comet() as venue:
        address = venue.addresses[BORROWER]
        loan = Deployment(venue, address)
        collateral = Deployment(venue, address, held_collateral(loan.account))
    # It is admitted as its approval, then passes the capacity check as the supply
    # that approval funds.
    supply = collateral.intent("supply", AMOUNT)
    approval = collateral.planned(supply)
    assert (approval.kind, approval.purpose) == ("approve", "supply")
    funded = direct.requested(collateral.observed()).model_copy(update={"allowance": AMOUNT})
    after = capacity.validate(
        collateral.config, funded, SupplyOperation(target=supply.target, amount=supply.amount)
    )
    assert after.principal == loan.account.principal < 0
    assert after.collaterals != loan.account.collaterals


def test_an_allowance_of_another_amount_is_refused_and_a_bare_one_is_revoked():
    with comet(funded=False) as venue:
        stale = Deployment(venue, NOBODY, allowance=AMOUNT)
        empty = Deployment(venue, NOBODY)
    wrong = f"spend {AMOUNT} base units.*State exactly that amount, or withdraw this asset first"
    with pytest.raises(DataError, match=wrong) as refused:
        stale.intent("supply", AMOUNT + 1)
    assert refused.value.kind == "allowance_mismatch"
    with pytest.raises(DataError, match="differs from the exact retained bound"):
        stale.planned(stale.config.supply(ALLOCATION, AMOUNT + 1))
    assert stale.planned(stale.intent("supply", AMOUNT)).kind == "supply"
    # With nothing supplied, a withdrawal clears the allowance; without one it has
    # nothing to do.
    for amount in (None, AMOUNT):
        assert stale.planned(stale.intent("withdraw", amount)).kind == "revoke"
    with pytest.raises(DataError, match="nothing to withdraw: the wallet has no .* supplied"):
        empty.intent("withdraw")


def test_the_connector_takes_operator_deployments_on_its_declared_chains():
    assert supports_operator(Checkpoint)
    with comet() as venue:
        deployment = Deployment(venue, venue.addresses[BORROWER])
    capabilities = operator_capabilities(deployment.config, None, None)
    assert capabilities == declared(deployment.config) == deployment.state().capabilities
    assert not any(item.borrow for item in capabilities.requirements)
    assert direct.DIRECT.actions == ("supply", "withdraw", "borrow", "repay")
    assert all("compound_v3" in declarations.connectors_for(a) for a in direct.DIRECT.actions)
    assert "compound_v3" not in declarations.connectors_for("collateral")
    assert "compound_v3" not in declarations.NOT_DIRECT
    cells = connector_from_wire({"connector": "compound_v3"}).chain_support
    assert [str(cell.chain) for cell in cells] == [
        "eip155:1",
        "eip155:8453",
        "eip155:42161",
        "eip155:10",
    ]
    for cell in cells:
        # Every action the cell supports is also a direct action there.
        assert cell.direct_actions == cell.actions
        assert sorted(map(str, cell.actions)) == sorted(request.kind for request in REQUESTS)
        for request in REQUESTS:
            declarations.require_direct("compound_v3", cell.chain, request)
    with pytest.raises(ValueError, match="compound_v3 serves no direct collateral"):
        declarations.require_direct(
            "compound_v3", ChainId("eip155:1"), ActionRequest("collateral", enable=True)
        )
    with pytest.raises(ValueError, match="compound_v3 serves no direct supply on eip155:137"):
        declarations.require_direct("compound_v3", ChainId("eip155:137"), SUPPLY)


SELECTED = [
    *("--connector", "compound_v3", "--chain", "eip155:8453"),
    *("--comet", "0xb125e6687d4313864e53df431d5425969c15eb2f"),
    *("--asset", "eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
    *("--wallet", "wallet", "--env-file", ".env"),
    *("--max-gas-price-wei", "5", "--max-transaction-fee", "7"),
]


def parsed(command, *stated):
    return parse_validated(build_parser(), ["ax", command, *SELECTED, *stated])


def stated_request(args):
    """The request `ax_actions.command` builds from parsed arguments."""
    floor = getattr(args, "min_health_factor", None)
    return ActionRequest(args.ax_command, args.amount, min_health_factor=floor)


def test_the_command_selects_a_comet_and_an_asset():
    supply = parsed("supply", "--amount", "9")
    withdraw = parsed("withdraw")
    assert (supply.product, supply.amount, supply.execute) == ("operator-action", 9, False)
    assert (withdraw.ax_command, withdraw.amount) == ("withdraw", None)
    request = lending_request(withdraw)
    assert (request.connector, str(request.chain)) == ("compound_v3", "eip155:8453")
    assert request.comet == "0xb125e6687d4313864e53df431d5425969c15eb2f"
    declarations.require_direct(withdraw.connector, request.chain, stated_request(withdraw))


def test_the_debt_commands_take_an_amount_and_a_floor_as_the_parser_admits_them(capsys):
    borrow = parsed("borrow", "--amount", "9", "--min-health-factor", "1.5")
    assert stated_request(borrow) == ActionRequest("borrow", 9, FLOOR)
    part = parsed("withdraw", "--amount", "4", "--min-health-factor", "2")
    assert stated_request(part) == ActionRequest("withdraw", 4, 2 * 10**18)
    assert stated_request(part).kind == "withdraw_amount"
    repayments = [stated_request(parsed("repay", *amount)) for amount in ((), ("--amount", "3"))]
    assert [request.kind for request in repayments] == ["repay", "repay_amount"]
    refused = (
        ("borrow", "--amount", "9"),
        ("borrow", "--min-health-factor", "1.5"),
        ("repay", "--min-health-factor", "1.5"),
        ("collateral", "--enable"),
    )
    for command in refused:
        with pytest.raises(SystemExit):
            parsed(*command)
    # Compound has no collateral switch, so the command does not offer the connector.
    assert "invalid choice: 'compound_v3'" in capsys.readouterr().err
