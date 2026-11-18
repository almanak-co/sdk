"""Offline economic Comet actions reuse deployed signed-principal transition rules."""

from types import SimpleNamespace

from almanak.connectors.compound_v3.authority import admit, declared
from almanak.connectors.compound_v3.capacity import paused
from almanak.connectors.compound_v3.economic_state import Position
from almanak.connectors.compound_v3.economic_views import account_view, risk_account
from almanak.connectors.compound_v3.math import (
    FACTOR_SCALE,
    mul_div,
    present_borrow,
    present_supply,
)
from almanak.connectors.compound_v3.operations import (
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.compound_v3.projection import health_factor, validate_sources
from almanak.connectors.compound_v3.risk import is_borrow_collateralized
from almanak.connectors.compound_v3.transitions import transition
from almanak.connectors.lending_health import health_floor
from almanak.lending_intents import (
    BorrowIntent,
    RepayAmountIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawAmountIntent,
    WithdrawIntent,
)
from almanak.models import AssetAmount, Model, UInt


class Change(Model):
    positions: tuple[Position, ...]
    inventory_delta: int
    assets: UInt
    principal_delta: int
    collateral_delta: int


def economic_operation(intent, before, market):
    if isinstance(intent, SupplyIntent):
        return SupplyOperation(target=intent.target, amount=intent.amount)
    if isinstance(intent, BorrowIntent):
        return BorrowOperation(target=intent.target, amount=intent.amount)
    if isinstance(intent, RepayIntent):
        debt = max(0, -market.balance(before.principal))
        amount = AssetAmount(asset=intent.target.asset, amount=debt)
        return RepayOperation(target=intent.target, amount=amount, full=True)
    if isinstance(intent, RepayAmountIntent):
        return RepayOperation(target=intent.target, amount=intent.amount, full=False)
    if isinstance(intent, (WithdrawIntent, WithdrawAmountIntent)):
        full = isinstance(intent, WithdrawIntent)
        quantity = (
            max(0, market.balance(before.principal))
            if intent.target.reserve.is_base
            else before.collateral
        )
        amount = (
            AssetAmount(asset=intent.target.asset, amount=quantity)
            if isinstance(intent, WithdrawIntent)
            else intent.amount
        )
        return WithdrawOperation(target=intent.target, amount=amount, full=full)
    raise ValueError("unsupported economic Comet intent")


def changed_position(before, operation, market, allocation):
    result = transition(
        before.principal, before.collateral, operation, (market.supply_index, market.borrow_index)
    )
    supplied, borrowed = before.supplied, before.borrowed
    if isinstance(operation, SupplyOperation):
        supplied += result.assets.amount
    if isinstance(operation, BorrowOperation):
        if not allocation.borrow:
            raise ValueError("economic Comet borrow is outside the allocation's authority")
        borrowed += result.assets.amount
    enabled = before.collateral_enabled
    if not operation.target.reserve.is_base:
        enabled = bool(result.collateral) and (enabled or not before.collateral)
    position = before.model_copy(
        update={
            "principal": result.principal,
            "collateral": result.collateral,
            "collateral_enabled": enabled,
            "supplied": supplied,
            "borrowed": borrowed,
            "withdrawn": before.withdrawn
            + (result.assets.amount if isinstance(operation, WithdrawOperation) else 0),
            "repaid": before.repaid
            + (result.assets.amount if isinstance(operation, RepayOperation) else 0),
        }
    )
    delta = (
        -result.assets.amount
        if isinstance(operation, (SupplyOperation, RepayOperation))
        else result.assets.amount
    )
    return Position.model_validate_json(position.model_dump_json()), delta


def apply(config, intent, positions, market, inventory, market_positions):
    if config.target(intent.target.allocation) != intent.target:
        raise ValueError("economic Comet intent differs from its configured target")
    admit(declared(config), intent)
    before = next((p for p in positions if p.target == intent.target), None)
    if before is None or inventory < 0:
        raise ValueError("economic Comet action requires its owned position and inventory")
    if any(
        p.target.fund != intent.target.fund or p.target.reserve.market != market.observed.source
        for p in positions
    ):
        raise ValueError("economic Comet action requires one exact fund/market group")
    allocation = config.allocation(intent.target.allocation)
    operation = economic_operation(intent, before, market)
    after, delta = changed_position(before, operation, market, allocation)
    if inventory + delta < 0:
        raise ValueError("economic Comet action cannot spend missing fund inventory")
    changed = tuple(after if p.target == before.target else p for p in positions)
    account = account_view(changed, market, intent.target.fund)
    validate_sources(config, account)
    paused(SimpleNamespace(account=account), operation, account)
    check_capacity(operation, before, after, market, market_positions, abs(delta))
    check_risk(operation, risk_account(account, market), allocation)
    return Change(
        positions=changed,
        inventory_delta=delta,
        assets=abs(delta),
        principal_delta=after.principal - before.principal,
        collateral_delta=after.collateral - before.collateral,
    )


def cash_delta(positions, source):
    return sum(
        p.supplied + p.repaid - p.withdrawn - p.borrowed
        for p in positions
        if p.target.reserve.market == source and p.target.reserve.is_base
    )


def check_capacity(operation, before, after, market, positions, amount):
    affected = tuple(p for p in positions if p.target.reserve.market == market.observed.source)
    if before not in affected:
        raise ValueError("economic Comet capacity lacks the action's retained position")
    updated = tuple(after if p.target == before.target else p for p in affected)
    if isinstance(operation, SupplyOperation) and not before.target.reserve.is_base:
        collateral = next(
            c for c in market.observed.collaterals if c.asset_type == before.target.asset
        )
        supplied = sum(p.collateral for p in updated if p.target.asset == collateral.asset_type)
        if collateral.total_supply + supplied > collateral.supply_cap:
            raise ValueError("economic Comet collateral exceeds its market supply cap")
    if (
        isinstance(operation, (BorrowOperation, WithdrawOperation))
        and before.target.reserve.is_base
    ):
        if amount > market.observed.base_available + cash_delta(affected, market.observed.source):
            raise ValueError("economic Comet base withdrawal exceeds market inventory")
    if isinstance(operation, BorrowOperation):
        if -market.balance(after.principal) < market.observed.base_borrow_minimum:
            raise ValueError("economic Comet resulting debt is below the market borrow minimum")
        check_utilization(market, updated)


def check_utilization(market, positions):
    if market.observed.controls is None:
        return
    accounting = market.observed.accounting
    supply = present_supply(accounting.total_supply_principal, accounting.supply_index)
    borrow = present_borrow(accounting.total_borrow_principal, accounting.borrow_index)
    balances = tuple(market.balance(p.principal) for p in positions if p.target.reserve.is_base)
    supply += sum(max(0, value) for value in balances)
    borrow += sum(max(0, -value) for value in balances)
    use = mul_div(borrow, FACTOR_SCALE, supply) if supply else 0
    if use > market.observed.controls.maximum_borrow_utilization:
        raise ValueError("economic Comet borrow exceeds the supported market utilization")


def check_risk(operation, account, allocation):
    needs = isinstance(operation, BorrowOperation) or (
        isinstance(operation, WithdrawOperation)
        and not operation.target.reserve.is_base
        and account.principal < 0
    )
    if not needs:
        return
    if not is_borrow_collateralized(account):
        raise ValueError("economic Comet collateral cannot cover projected debt")
    health = health_factor(account)
    if health is None or health < health_floor(allocation.minimum_health_factor):
        raise ValueError("economic Comet health is below the configured minimum")
