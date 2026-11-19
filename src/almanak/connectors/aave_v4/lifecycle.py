"""One execution lifecycle coordinator, including recovery and requested teardown."""

from uuid import uuid4

from almanak.caip import AssetType
from almanak.connectors.aave_v4.models import (
    Ownership,
)
from almanak.connectors.aave_v4.operations import (
    CollateralOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.aave_v4.validation import validate_owned
from almanak.connectors.ownership import protocol_ownership
from almanak.connectors.repayment import coverage
from almanak.lending_intents import BorrowIntent, SupplyIntent
from almanak.models import Account, AssetAmount
from almanak.reporting import report as report
from almanak.work import initial_work


def validate_initial_balances(state, observed):
    validate_observed_ownership(state, observed)
    if any(item.allowance for item in observed):
        raise RuntimeError("new deployment cannot adopt existing exposure or allowance")
    for item in observed:
        required = sum(
            allocation.capital
            for allocation in state.config.allocations
            if state.config.target(allocation.name).fund == item.target.fund
            and allocation.reserve.asset == item.target.reserve.asset
        )
        if state.mode == "running" and item.wallet_balance < required:
            raise RuntimeError("fund has insufficient assets for the combined allocation capital")
    validate_shared_balances(observed)


def validate_shared_balances(observed):
    balances: dict[tuple[Account, AssetType], tuple[int, AssetAmount]] = {}
    for item in observed:
        key = item.target.fund, item.target.reserve.asset
        value = item.wallet_balance, item.native_balance
        if balances.setdefault(key, value) != value:
            raise RuntimeError("shared wallet observations disagree at the pinned block")


def validate_observed_ownership(state, observed):
    if tuple(item.target for item in observed) != tuple(item.target for item in state.owned):
        raise RuntimeError("observations do not match the configured supply positions")
    for owned, item in zip(state.owned, observed, strict=True):
        validate_owned(owned, item)


def adopt_ownership(owned, observed):
    """Ownership of each position as its observation states it, for an operator deployment."""
    return tuple(
        Ownership(
            target=item.target,
            shares=seen.position.supplied_shares,
            drawn_shares=seen.drawn_shares,
            premium_shares=seen.premium_shares,
            premium_offset_ray=seen.premium_offset_ray,
            collateral_enabled=seen.collateral_enabled,
        )
        for item, seen in zip(owned, observed, strict=True)
    )


def apply_share_movement(owned, operation: SupplyOperation | WithdrawOperation, shares):
    delta = shares if isinstance(operation, SupplyOperation) else -shares
    return tuple(
        item.model_copy(update={"shares": item.shares + delta})
        if item.target == operation.target
        else item
        for item in owned
    )


def commit_ownership(owned, outcome):
    if not outcome.success:
        return owned
    after = outcome.after
    result = []
    for item in owned:
        if item.target == outcome.operation.target:
            item = Ownership(
                target=item.target,
                shares=after.position.supplied_shares,
                drawn_shares=after.drawn_shares,
                premium_shares=after.premium_shares,
                premium_offset_ray=after.premium_offset_ray,
                collateral_enabled=after.collateral_enabled,
            )
        if (
            isinstance(outcome.operation, CollateralOperation)
            and item.target == outcome.operation.collateral
        ):
            collateral = next(c for c in after.collateral if c.target == item.target)
            item = item.model_copy(update={"collateral_enabled": collateral.enabled})
        result.append(item)
    return tuple(result)


def validate_closure(state, observed):
    if any(
        item.shares
        or item.drawn_shares
        or item.premium_shares
        or item.premium_offset_ray
        or item.collateral_enabled
        for item in state.owned
    ) or any(item.allowance or item.position.total_debt for item in observed):
        raise RuntimeError("strategy cannot close with unresolved obligations")


def repayment_coverage(state, observed):
    """Coverage of each debt allocation whose observed debt is open.

    The Spoke's borrow rate is not observed, so no projection is reported.
    """
    config = state.config
    debts = {debt.name for debt in config.debts}
    rows = []
    for observation in observed:
        debt = observation.position.total_debt
        if observation.allocation not in debts or not debt:
            continue
        rows.append(
            coverage(
                observation.allocation,
                observation.position.reserve.source.asset,
                observation.position.reserve.block,
                debt,
                observation.wallet_balance,
            )
        )
    return tuple(rows)


class AaveLifecycle:
    repayment_coverage = staticmethod(repayment_coverage)
    ownership = staticmethod(protocol_ownership)
    validate_initial = staticmethod(validate_initial_balances)
    validate_owned = staticmethod(validate_observed_ownership)
    commit_owned = staticmethod(commit_ownership)
    validate_closure = staticmethod(validate_closure)
    adopt = staticmethod(adopt_ownership)

    @staticmethod
    def increases_exposure(intent):
        return isinstance(intent, (SupplyIntent, BorrowIntent))

    @staticmethod
    def initial(config, strategy, capabilities, permissions):
        from almanak.connectors.aave_v4.state import Checkpoint, Work
        from almanak.triggers import DecisionSchedule

        return Checkpoint(
            schedule=DecisionSchedule(),
            work=initial_work(config, Work),
            deployment_id=uuid4().hex,
            config=config,
            strategy=strategy,
            operator=strategy is None,
            capabilities=capabilities,
            permissions=permissions,
            mode="running",
            initialized=False,
            owned=tuple(
                Ownership(target=config.target(a.name), shares=0) for a in config.positions
            ),
            baseline=(),
            pending=None,
            outcomes=(),
            closure=(),
        )
