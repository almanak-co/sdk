"""Comet economic projections retain revision-specific accrual and fund-wide collateral."""

from almanak.connectors.compound_v3.accounting import indices
from almanak.connectors.compound_v3.domain import AccountObservation, Revision
from almanak.connectors.compound_v3.math import (
    FACTOR_SCALE,
    mul_div,
    present_value,
    repay_and_supply,
    signed,
    uint,
    withdraw_and_borrow,
)
from almanak.connectors.compound_v3.observations import owned, target_asset
from almanak.connectors.compound_v3.operations import (
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.compound_v3.risk import (
    active,
    base_liquidity,
    is_borrow_collateralized,
    require_price,
)
from almanak.connectors.compound_v3.transitions import transition
from almanak.connectors.lending_health import health_floor
from almanak.gateway.ethereum import DataError


def accrued_market(before, operation):
    market = before.account.market
    accounting = market.accounting
    if not operation.target.reserve.is_base and market.revision is Revision.STANDARD:
        return market
    if operation.target.reserve.is_base and isinstance(
        operation, (SupplyOperation, RepayOperation)
    ):
        # Base supply transfers inventory before accruing. Its empty-borrow rate branch
        # reads that post-transfer cash balance in the controlled implementation.
        amount = (
            before.account.borrowed_base
            if isinstance(operation, RepayOperation) and operation.full
            else operation.amount.amount
        )
        market = market.model_copy(update={"base_available": uint(market.base_available + amount)})
    supply, borrow = indices(market, before.block.timestamp)
    return market.model_copy(
        update={
            "accounting": accounting.model_copy(
                update={
                    "supply_index": supply,
                    "borrow_index": borrow,
                    "last_accrual_time": before.block.timestamp,
                }
            )
        }
    )


def base_account(before, operation, market, result):
    accounting = market.accounting
    old, new = before.account.principal, result.principal
    if isinstance(operation, (SupplyOperation, RepayOperation)):
        repaid, supplied = repay_and_supply(old, new)
        supply = uint(accounting.total_supply_principal + supplied, 104)
        borrow = uint(accounting.total_borrow_principal - repaid, 104)
    else:
        withdrawn, borrowed = withdraw_and_borrow(old, new)
        supply = uint(accounting.total_supply_principal - withdrawn, 104)
        borrow = uint(accounting.total_borrow_principal + borrowed, 104)
        market = market.model_copy(
            update={"base_available": uint(market.base_available - result.assets.amount)}
        )
    market = market.model_copy(
        update={
            "accounting": accounting.model_copy(
                update={"total_supply_principal": supply, "total_borrow_principal": borrow}
            )
        }
    )
    return before.account.model_copy(update={"market": market, "principal": new})


def collateral_account(before, market, result):
    balances = tuple(
        c.model_copy(update={"balance": result.collateral}) if c.asset == result.target.asset else c
        for c in before.account.collaterals
    )
    collateral = next(c for c in market.collaterals if c.asset_type == result.target.asset)
    delta = result.collateral - owned(before).collateral
    changed = collateral.model_copy(
        update={"total_supply": uint(collateral.total_supply + delta, 128)}
    )
    market = market.model_copy(
        update={
            "collaterals": tuple(
                changed if c.offset == changed.offset else c for c in market.collaterals
            )
        }
    )
    bitmap = before.account.assets_in | (before.account.extended_assets_in << 16)
    # Comet changes membership only on zero crossings, including after an absorb.
    if owned(before).collateral == 0 and result.collateral:
        bitmap |= 1 << collateral.offset
    elif owned(before).collateral and result.collateral == 0:
        bitmap &= ~(1 << collateral.offset)
    return before.account.model_copy(
        update={
            "market": market,
            "collaterals": balances,
            "assets_in": bitmap & 65535,
            "extended_assets_in": bitmap >> 16,
        }
    )


def projected_account(before, operation) -> AccountObservation:
    target_asset(operation.target, before.account)
    market = accrued_market(before, operation)
    accounting = market.accounting
    balance = owned(before)
    result = transition(
        before.account.principal,
        balance.collateral,
        operation,
        (accounting.supply_index, accounting.borrow_index),
    )
    account = (
        base_account(before, operation, market, result)
        if operation.target.reserve.is_base
        else collateral_account(before, market, result)
    )
    view_indices = indices(account.market, before.block.timestamp)
    present = present_value(account.principal, *view_indices)
    return account.model_copy(
        update={"supplied_base": max(0, present), "borrowed_base": max(0, -present)}
    )


def health_factor(account):
    if account.principal >= 0:
        return None
    debt = -base_liquidity(account)
    if not debt:
        return None
    capacity = 0
    for collateral in account.market.collaterals:
        if active(account, collateral.offset) and collateral.liquidate_factor:
            amount = account.collaterals[collateral.offset].balance
            value = mul_div(amount, require_price(collateral.price), collateral.scale)
            capacity = signed(
                capacity + signed(mul_div(value, collateral.liquidate_factor, FACTOR_SCALE))
            )
    return mul_div(capacity, FACTOR_SCALE, debt)


def validate_projection(config, before, operation):
    validate_sources(config, before.account)
    after = projected_account(before, operation)
    if requires_risk(operation, after):
        require_health(config.allocation(operation.allocation), after)
    return after


def requires_risk(operation, after) -> bool:
    if isinstance(operation, BorrowOperation):
        return True
    return (
        isinstance(operation, WithdrawOperation)
        and not operation.target.reserve.is_base
        and after.principal < 0
    )


def require_health(allocation, after):
    if not is_borrow_collateralized(after):
        raise DataError(
            "collateral_unavailable", "Comet collateral cannot cover the projected debt"
        )
    health = health_factor(after)
    if health is None or health < health_floor(allocation.minimum_health_factor):
        raise DataError("health_factor", "projected Comet health is below the configured minimum")


def validate_sources(config, account):
    for allocation in config.positions:
        target = config.target(allocation.name)
        if (
            target.fund == account.fund
            and target.reserve.market.comet == account.market.source.comet
        ):
            target_asset(target, account)
