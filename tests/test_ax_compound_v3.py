"""Compound V3 direct operator actions: the deployment `ax supply` and `ax withdraw` build.

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
from almanak.lending_intents import SupplyIntent, WithdrawIntent
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

    def __init__(self, venue, address, asset=None, action="supply", **snapshot):
        self.wallet = Wallet(account="treasury", address=address)
        self.fund = Account(chain=venue.market.source.chain, address=address)
        self.account = read_account(venue.reader, venue.market, self.fund)
        request = ActionRequest(action, AMOUNT if action == "supply" else None)
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

    def intent(self, action, amount=None):
        return direct.intent(self.config, self.observed(), ActionRequest(action, amount))

    def planned(self, intent):
        """The connector's own first step for an intent, and its admission checks."""
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
        assert not any(item.borrow for item in deployment.config.allocations)
    # Whole-market ownership refuses a deployment of the requested leg alone.
    alone = {**other.document, "allocations": other.document["allocations"][:1]}
    partial = other.state(other.configuration(alone))
    with pytest.raises(DataError, match="base principal is not owned by this deployment"):
        validate_owned(partial, partial.baseline)


def test_a_supply_states_its_amount_and_a_withdrawal_takes_none():
    with comet() as venue:
        deployment = Deployment(venue, venue.addresses[SUPPLIER])
    supply = deployment.intent("supply", AMOUNT)
    assert isinstance(supply, SupplyIntent) and supply.amount.amount == AMOUNT
    assert supply.target == deployment.config.target(ALLOCATION)
    withdraw = deployment.intent("withdraw")
    assert isinstance(withdraw, WithdrawIntent) and withdraw.target == supply.target
    with pytest.raises(ValueError, match="supply requires --amount"):
        deployment.intent("supply")
    with pytest.raises(ValueError, match="withdraw takes no --amount"):
        deployment.intent("withdraw", AMOUNT)
    assert deployment.planned(supply).kind == "approve"
    assert deployment.planned(withdraw).kind == "withdraw"


def test_the_position_is_found_by_its_name():
    with comet() as venue:
        address = venue.addresses[BORROWER]
        held = held_collateral(Deployment(venue, address).account)
        deployment = Deployment(venue, address, held)
    state = deployment.state()
    assert direct.position(state) == {
        "asset": str(held),
        "base_principal": "0",
        "collateral": str(10**18),
    }
    # The requested position is found wherever it sits among the owned positions.
    moved = SimpleNamespace(config=state.config, owned=state.owned[::-1])
    assert direct.position(moved) == direct.position(state)


def test_a_wallet_in_debt_may_add_collateral_but_not_repay_or_release_it():
    with comet() as venue:
        address = venue.addresses[BORROWER]
        loan = Deployment(venue, address)
        held = held_collateral(loan.account)
        collateral = Deployment(venue, address, held)
    owed = f"the wallet owes {loan.account.borrowed_base} base units of "
    with pytest.raises(DataError, match=owed + ".*would repay that debt") as repaying:
        loan.intent("supply", AMOUNT)
    with pytest.raises(DataError, match="nothing to withdraw.*" + owed) as nothing:
        loan.intent("withdraw")
    with pytest.raises(DataError, match=owed + ".*needs a health floor") as releasing:
        collateral.intent("withdraw")
    kinds = [caught.value.kind for caught in (repaying, nothing, releasing)]
    assert kinds == ["unsupported", "no_position", "health_factor"]
    # Each refusal states what the connector itself refuses, less clearly.
    with pytest.raises(ValueError, match="cannot silently repay existing base debt"):
        loan.planned(loan.config.supply(ALLOCATION, AMOUNT))
    with pytest.raises(DataError, match="closed with no remaining allowance"):
        loan.planned(loan.config.withdraw(ALLOCATION))
    with pytest.raises(DataError, match="collateral cannot cover the projected debt"):
        collateral.planned(collateral.config.withdraw(ALLOCATION))
    # Adding collateral needs no health floor: it is admitted as its approval, then
    # passes the capacity check as the supply that approval funds.
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
    with pytest.raises(DataError, match=f"spend {AMOUNT} base units.*Supply exactly") as refused:
        stale.intent("supply", AMOUNT + 1)
    assert refused.value.kind == "allowance_mismatch"
    with pytest.raises(DataError, match="differs from the exact retained bound"):
        stale.planned(stale.config.supply(ALLOCATION, AMOUNT + 1))
    assert stale.planned(stale.intent("supply", AMOUNT)).kind == "supply"
    # With nothing supplied, a withdrawal clears the allowance; without one it has
    # nothing to do.
    assert stale.planned(stale.intent("withdraw")).kind == "revoke"
    with pytest.raises(DataError, match="nothing to withdraw: the wallet has no .* supplied"):
        empty.intent("withdraw")


def test_the_connector_takes_operator_deployments_on_its_declared_chains():
    assert supports_operator(Checkpoint)
    with comet() as venue:
        deployment = Deployment(venue, venue.addresses[BORROWER])
    capabilities = operator_capabilities(deployment.config, None, None)
    assert capabilities == declared(deployment.config) == deployment.state().capabilities
    assert not any(item.borrow for item in capabilities.requirements)
    assert direct.DIRECT.actions == ("supply", "withdraw")
    assert "compound_v3" in declarations.connectors_for("supply")
    assert "compound_v3" not in declarations.NOT_DIRECT
    for chain in ("eip155:1", "eip155:8453", "eip155:42161", "eip155:10"):
        for action in direct.DIRECT.actions:
            declarations.require_direct("compound_v3", ChainId(chain), action)
    with pytest.raises(ValueError, match="compound_v3 serves no direct borrow"):
        declarations.require_direct("compound_v3", ChainId("eip155:1"), "borrow")
    with pytest.raises(ValueError, match="eip155:137"):
        declarations.require_direct("compound_v3", ChainId("eip155:137"), "supply")


def test_the_command_selects_a_comet_and_an_asset():
    selected = [
        *("--connector", "compound_v3", "--chain", "eip155:8453"),
        *("--comet", "0xb125e6687d4313864e53df431d5425969c15eb2f"),
        *("--asset", "eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        *("--wallet", "wallet", "--env-file", ".env"),
        *("--max-gas-price-wei", "5", "--max-transaction-fee", "7"),
    ]
    parser = build_parser()
    supply = parse_validated(parser, ["ax", "supply", *selected, "--amount", "9"])
    withdraw = parse_validated(parser, ["ax", "withdraw", *selected])
    assert (supply.product, supply.amount, supply.execute) == ("operator-action", 9, False)
    assert (withdraw.ax_command, withdraw.amount) == ("withdraw", None)
    request = lending_request(withdraw)
    assert (request.connector, str(request.chain)) == ("compound_v3", "eip155:8453")
    assert request.comet == "0xb125e6687d4313864e53df431d5425969c15eb2f"
    declarations.require_direct(withdraw.connector, request.chain, withdraw.ax_command)
