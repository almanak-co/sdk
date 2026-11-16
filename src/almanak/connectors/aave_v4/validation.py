"""Runtime-only debt and collateral preconditions. These are not Roles predicates."""

from almanak.connectors.aave_v4.models import DebtAllocation
from almanak.connectors.aave_v4.operations import (
    BorrowOperation,
    CollateralOperation,
    RepayOperation,
)
from almanak.connectors.repayment import require_repayment_funding
from almanak.gateway.ethereum import DataError


def validate_owned(owned, before):
    if (
        before.position.supplied_shares,
        before.drawn_shares,
        before.premium_shares,
        before.premium_offset_ray,
        before.collateral_enabled,
    ) != (
        owned.shares,
        owned.drawn_shares,
        owned.premium_shares,
        owned.premium_offset_ray,
        owned.collateral_enabled,
    ):
        raise DataError(
            "ownership_mismatch",
            "observed supply, debt or collateral differs from durable ownership",
        )


def validate_collateral_ownership(state, before, debt):
    expected = tuple(state.config.target(name) for name in debt.collateral)
    if tuple(c.target for c in before.collateral) != expected:
        raise ValueError("collateral evidence does not cover the configured debt references")
    owned = {o.target: o for o in state.owned}
    for collateral in before.collateral:
        item = owned[collateral.target]
        if (collateral.supplied_shares, collateral.enabled) != (
            item.shares,
            item.collateral_enabled,
        ):
            raise DataError(
                "ownership_mismatch", "collateral differs from its durable owned position"
            )


def validate_debt_operation(state, operation, before, allocation):
    if not isinstance(allocation, DebtAllocation):
        raise ValueError("debt execution requires a configured debt capability")
    validate_collateral_ownership(state, before, allocation)
    if isinstance(operation, BorrowOperation) or (
        isinstance(operation, CollateralOperation) and operation.enabled
    ):
        validate_borrow_entry(state, operation, before, allocation)
    elif isinstance(operation, CollateralOperation):
        require_zero_debt(before)
    elif before.position.total_debt:
        validate_repayment_funding(before, allocation)


def validate_borrow_entry(state, operation, before, allocation):
    if any(item.accounting_issues for item in state.outcomes):
        raise DataError("accounting", "unexplained wallet movements block new borrowing")
    if state.mode != "running":
        raise ValueError("new borrowing requires running mode")
    reserve = before.position.reserve
    if reserve.paused or reserve.frozen or not reserve.borrowable:
        raise DataError("unavailable", "configured reserve is not available for borrowing")
    if any(not c.supplied_shares for c in before.collateral):
        raise DataError("unavailable", "borrowing requires owned collateral shares")


def require_zero_debt(before):
    if (
        before.position.total_debt
        or before.drawn_shares
        or before.premium_shares
        or before.premium_offset_ray
    ):
        raise DataError("residual_debt", "collateral release requires zero drawn and premium debt")


def validate_repayment_funding(before, debt):
    require_repayment_funding(
        debt.name, debt.reserve.asset, before.position.total_debt, before.wallet_balance
    )


def validate_debt_movement(pending, after, assets, shares, success=True):
    before = pending.before
    operation = pending.operation
    if not (before.drawn_shares or before.premium_shares or before.premium_offset_ray) and (
        after.position.total_debt and not isinstance(operation, BorrowOperation)
    ):
        raise DataError(
            "inconsistent", "unexpected debt does not reconcile with the admitted operation"
        )
    direction = 1 if isinstance(operation, BorrowOperation) else -1
    if isinstance(operation, (BorrowOperation, RepayOperation)):
        expected = before.drawn_shares + direction * shares
        if after.drawn_shares != expected:
            raise DataError(
                "inconsistent", "receipt drawn shares do not reconcile with debt ownership"
            )
    elif (after.drawn_shares, after.premium_shares, after.premium_offset_ray) != (
        before.drawn_shares,
        before.premium_shares,
        before.premium_offset_ray,
    ):
        raise DataError("inconsistent", "debt ownership changed during an unrelated operation")
    if success and isinstance(operation, CollateralOperation):
        states = {c.target: c for c in after.collateral}
        if states[operation.collateral].enabled != operation.enabled:
            raise DataError(
                "inconsistent", "collateral state differs from the successful operation"
            )
