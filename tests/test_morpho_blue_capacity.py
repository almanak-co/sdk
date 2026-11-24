"""Morpho capacity boundaries run without retained provider evidence."""

from types import SimpleNamespace

import pytest

from almanak.caip import AccountReference, AssetType, ChainId
from almanak.connectors.morpho_blue.capacity import (
    funding_requirement,
    operation_scope,
    projected_transition,
    transition_risk,
    validate,
)
from almanak.connectors.morpho_blue.domain import Accounting, Position, Reserve, Target
from almanak.connectors.morpho_blue.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.morpho_blue.transitions import Transition
from almanak.gateway.ethereum import DataError
from almanak.models import Account, AssetAmount, Chain

LOAN = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
COLLATERAL = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")


class CapacityConfig:
    def __init__(self, targets, minimum_health=2 * 10**18):
        self.targets = {target.allocation: target for target in targets}
        self.minimum_health = minimum_health

    def target(self, allocation):
        return self.targets[allocation]

    def allocation(self, allocation):
        assert allocation in self.targets
        return SimpleNamespace(minimum_health_factor=self.minimum_health)


def target(name, *, loan):
    chain = Chain(chain_id=ChainId("eip155:8453"), environment="mainnet")
    fund = Account(chain=chain, address=AccountReference("0x" + "11" * 20))
    market = SimpleNamespace(
        chain=chain,
        loan_asset=LOAN,
        collateral_asset=COLLATERAL,
        lltv=860_000_000_000_000_000,
    )
    reserve = Reserve.model_construct(market=market, asset=LOAN if loan else COLLATERAL)
    return Target.model_construct(allocation=name, account="fund", fund=fund, reserve=reserve)


def snapshot(
    selected,
    *,
    position=None,
    accounting=None,
    wallet=200_000,
    allowance=200_000,
    core_balance=2_000_000,
    price=2_000 * 10**24,
):
    accounting = accounting or Accounting(
        supply_assets=2_000_000,
        supply_shares=2_000_000_000_000,
        borrow_assets=1_000_000,
        borrow_shares=1_000_000_000_000,
        last_update=100,
        fee=0,
    )
    position = position or Position(supply_shares=0, borrow_shares=0, collateral=10**18)
    observed_market = SimpleNamespace(
        accounting=accounting,
        block=SimpleNamespace(timestamp=100),
        rate_at_target=0,
        core_loan_balance=core_balance,
        price=SimpleNamespace(price=price),
        source=selected.reserve.market,
    )
    account = SimpleNamespace(market=observed_market, position=position)
    return SimpleNamespace(
        target=selected,
        account=account,
        allowance=allowance,
        wallet_balance=wallet,
    )


def full_repayment(*, allowance=200_000, wallet=119_950):
    loan = target("loan", loan=True)
    accounting = Accounting(
        supply_assets=2_000_000,
        supply_shares=2_000_000_000_000,
        borrow_assets=1_014_485,
        borrow_shares=1_009_512_554_507,
        last_update=100,
        fee=0,
    )
    position = Position(supply_shares=0, borrow_shares=99_509_855_682, collateral=10**18)
    before = snapshot(
        loan,
        accounting=accounting,
        position=position,
        allowance=allowance,
        wallet=wallet,
    )
    operation = RepayOperation(
        target=loan,
        amount=AssetAmount(asset=LOAN, amount=allowance),
        shares=position.borrow_shares,
    )
    return CapacityConfig((loan,)), before, operation


def test_share_derived_full_repayment_keeps_approval_funding_and_share_identity():
    config, before, operation = full_repayment()
    result = validate(config, before, operation)
    assert result.assets == 100_001
    assert result.position.borrow_shares == 0
    assert funding_requirement(before, operation, result).amount == 100_001

    wrong_allowance = snapshot(
        before.target,
        accounting=before.account.market.accounting,
        position=before.account.position,
        allowance=199_999,
    )
    with pytest.raises(DataError, match="exact retained allowance") as mismatch:
        validate(config, wrong_allowance, operation)
    assert mismatch.value.kind == "allowance_mismatch"

    insufficient = snapshot(
        before.target,
        accounting=before.account.market.accounting,
        position=before.account.position,
        wallet=100_000,
    )
    with pytest.raises(DataError, match="needs 1 more") as unavailable:
        validate(config, insufficient, operation)
    assert unavailable.value.kind == "repayment_funding"


def test_share_derived_full_repayment_refuses_an_approval_below_the_debt_or_share_drift():
    config, before, operation = full_repayment(allowance=100_000)
    with pytest.raises(DataError, match="approved allowance") as unavailable:
        validate(config, before, operation)
    assert unavailable.value.kind == "funding_unavailable"

    wrong_shares = operation.model_copy(update={"shares": operation.shares - 1})
    with pytest.raises(DataError, match="every owned debt share") as ownership:
        validate(config, before, wrong_shares)
    assert ownership.value.kind == "ownership_mismatch"


def test_outgoing_loan_inventory_is_independent_from_incoming_funding():
    loan = target("loan", loan=True)
    config = CapacityConfig((loan,))
    before = snapshot(loan, allowance=0, wallet=0, core_balance=10)
    borrow = BorrowOperation(target=loan, amount=AssetAmount(asset=LOAN, amount=10))
    assert validate(config, before, borrow).position.borrow_shares > 0

    incoming = snapshot(loan, allowance=1, wallet=1)
    supply = SupplyOperation(target=loan, amount=AssetAmount(asset=LOAN, amount=1))
    assert validate(config, incoming, supply).position.supply_shares > 0

    unavailable = snapshot(loan, allowance=0, wallet=0, core_balance=9)
    with pytest.raises(DataError, match="required loan inventory") as liquidity:
        validate(config, unavailable, borrow)
    assert liquidity.value.kind == "liquidity_unavailable"


def test_operation_scope_refuses_unsupported_and_mismatched_targets():
    loan = target("loan", loan=True)
    other = target("other", loan=True)
    config = CapacityConfig((loan, other))
    before = snapshot(loan)
    approval = ApproveOperation(
        target=loan, amount=AssetAmount(asset=LOAN, amount=1), purpose="repay"
    )
    with pytest.raises(ValueError, match="economic operation"):
        operation_scope(config, before, approval)

    borrow = BorrowOperation(target=other, amount=AssetAmount(asset=LOAN, amount=1))
    with pytest.raises(ValueError, match="configured fund or market"):
        operation_scope(config, before, borrow)

    changed = CapacityConfig((loan,))
    changed.targets["loan"] = other
    borrow = BorrowOperation(target=loan, amount=AssetAmount(asset=LOAN, amount=1))
    with pytest.raises(ValueError, match="configured fund or market"):
        operation_scope(changed, before, borrow)


def test_transition_risk_preserves_rounding_and_health_boundaries():
    loan = target("loan", loan=True)
    config = CapacityConfig((loan,))
    before = snapshot(loan, allowance=1, wallet=1)
    supply = SupplyOperation(target=loan, amount=AssetAmount(asset=LOAN, amount=1))
    no_shares = Transition(
        accounting=before.account.market.accounting,
        position=before.account.position,
        assets=1,
    )
    with pytest.raises(DataError, match="without receiving shares") as rounding:
        transition_risk(config, before, supply, no_shares)
    assert rounding.value.kind == "rounding_unavailable"

    unhealthy = snapshot(loan, allowance=0, wallet=0, price=1)
    borrow = BorrowOperation(target=loan, amount=AssetAmount(asset=LOAN, amount=1))
    result = projected_transition(unhealthy, borrow)
    with pytest.raises(ValueError, match="health floor"):
        transition_risk(config, unhealthy, borrow, result)


def test_collateral_withdrawal_checks_health_but_loan_withdrawal_checks_liquidity():
    loan = target("loan", loan=True)
    collateral = target("collateral", loan=False)
    config = CapacityConfig((loan, collateral))
    unhealthy = snapshot(
        collateral,
        position=Position(supply_shares=1_000, borrow_shares=1_000, collateral=1),
        allowance=0,
        wallet=0,
        price=1,
    )
    withdraw_collateral = WithdrawOperation(
        target=collateral,
        amount=AssetAmount(asset=COLLATERAL, amount=1),
        full=True,
    )
    with pytest.raises(ValueError, match="health floor"):
        validate(config, unhealthy, withdraw_collateral)

    lender = snapshot(
        loan,
        position=Position(supply_shares=1_000_000_000, borrow_shares=0, collateral=0),
        allowance=0,
        wallet=0,
        core_balance=0,
    )
    withdraw_loan = WithdrawOperation(
        target=loan,
        amount=AssetAmount(asset=LOAN, amount=0),
        full=True,
        shares=1_000_000_000,
    )
    with pytest.raises(DataError, match="required loan inventory") as liquidity:
        validate(config, lender, withdraw_loan)
    assert liquidity.value.kind == "liquidity_unavailable"


def test_composed_caller_forwards_the_snapshot_to_inventory_validation(monkeypatch):
    """Full repayment inventory is checked against the projected assets, not the approval."""
    from almanak.connectors.composed import execution as composed
    from almanak.connectors.composed.models import wallet_key

    _, before, operation = full_repayment()
    inventory = (SimpleNamespace(key=wallet_key(operation.target, LOAN), amount=119_950),)
    state = SimpleNamespace(inventory=inventory, outcomes=(), owned=(), transfers=(), inbound=())
    # The composed caller builds a fresh connector per call, so patch the adapter class.
    adapter = type(composed.connector_for_target(operation.target).execution)
    forwarded = {}
    view = SimpleNamespace(config=None)
    monkeypatch.setattr(composed, "backend_view", lambda backend, target: view)
    monkeypatch.setattr(composed, "component_state", lambda current, identity: current)
    record = lambda *args: forwarded.update(before=args[3])  # noqa: E731
    monkeypatch.setattr(adapter, "validate_operation", staticmethod(record))
    monkeypatch.setattr(adapter, "increases_exposure", staticmethod(lambda config, op: False))
    composed.ComposedExecution.validate_operation(None, state, operation, before)
    assert forwarded["before"] is before

    # The wallet bounds a full repay, so a top-up beyond the retained inventory covers it.
    short = SimpleNamespace(
        inventory=(SimpleNamespace(key=inventory[0].key, amount=100_000),),
        outcomes=(),
        owned=(),
        transfers=(),
        inbound=(),
    )
    composed.ComposedExecution.validate_operation(None, short, operation, before)
    _, unfunded, _ = full_repayment(wallet=100_000)
    with pytest.raises(DataError, match="needs 1 more"):
        composed.ComposedExecution.validate_operation(None, short, operation, unfunded)


@pytest.mark.parametrize("debt", [0, 1_000])
def test_an_omitted_health_floor_admits_collateral_withdrawal_only_without_debt(debt):
    collateral = target("collateral", loan=False)
    before = snapshot(
        collateral,
        position=Position(supply_shares=0, borrow_shares=debt, collateral=10**18),
    )
    withdraw = WithdrawOperation(
        target=collateral, amount=AssetAmount(asset=COLLATERAL, amount=1), full=False
    )
    validate(CapacityConfig((collateral,)), before, withdraw)
    lend_only = CapacityConfig((collateral,), minimum_health=None)
    if debt:
        with pytest.raises(ValueError, match="health floor"):
            validate(lend_only, before, withdraw)
    else:
        validate(lend_only, before, withdraw)
