"""Comet action controls and finite funding bounds before authorized execution."""

from almanak.connectors.compound_v3.math import present_value, utilization
from almanak.connectors.compound_v3.operations import (
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.compound_v3.projection import accrued_market, validate_projection
from almanak.connectors.repayment import require_repayment_funding
from almanak.gateway.ethereum import DataError


def paused(before, operation, after):
    market = before.account.market
    supplying = isinstance(operation, (SupplyOperation, RepayOperation))
    flag = 0 if supplying else 2
    if market.accounting.pause_flags & (1 << flag):
        raise DataError("paused", "Comet supply or withdrawal is paused")
    controls = market.controls
    if controls is None:
        return
    if operation.target.reserve.is_base:
        flag = 3 if supplying else 1 if after.principal < 0 else 0
        if controls.pause_flags & (1 << flag):
            raise DataError("paused", "Comet base action is paused")
        return
    collateral = next(c for c in market.collaterals if c.asset_type == operation.target.asset)
    flag = 2 if supplying else 7
    assets = controls.collateral_supply_flags if supplying else controls.collateral_withdraw_flags
    if controls.pause_flags & (1 << flag) or assets & (1 << collateral.offset):
        raise DataError("paused", "Comet collateral action is paused")


def funding(before, operation):
    if not isinstance(operation, (SupplyOperation, RepayOperation)):
        return
    required = (
        before.account.borrowed_base
        if isinstance(operation, RepayOperation) and operation.full
        else operation.amount.amount
    )
    if isinstance(operation, RepayOperation):
        target = operation.target
        require_repayment_funding(target.allocation, target.asset, required, before.wallet_balance)
    elif before.wallet_balance < required:
        raise DataError("funding_unavailable", "Comet supply lacks inventory for its amount")
    if before.allowance != operation.amount.amount or before.allowance < required:
        raise DataError(
            "allowance_mismatch", "Comet step requires an allowance covering its amount"
        )


def supply_cap(before, operation, after):
    if not isinstance(operation, SupplyOperation) or operation.target.reserve.is_base:
        return
    collateral = next(c for c in after.market.collaterals if c.asset_type == operation.target.asset)
    if collateral.total_supply > collateral.supply_cap:
        raise DataError("supply_cap", "Compound collateral supply exceeds its observed market cap")


def borrow_bounds(before, operation, after):
    if not isinstance(operation, BorrowOperation):
        return
    market = accrued_market(before, operation)
    accounting = market.accounting
    debt = operation.amount.amount - present_value(
        before.account.principal, accounting.supply_index, accounting.borrow_index
    )
    if debt < market.base_borrow_minimum:
        raise DataError(
            "borrow_minimum", "Compound resulting debt is below the market borrow minimum"
        )
    if market.controls is not None:
        accounting = after.market.accounting
        use = utilization(
            accounting.total_supply_principal,
            accounting.total_borrow_principal,
            (accounting.supply_index, accounting.borrow_index),
        )
        if use > market.controls.maximum_borrow_utilization:
            raise DataError(
                "utilization", "Compound borrow exceeds the supported market utilization"
            )


def validate(config, before, operation):
    if not isinstance(
        operation, (SupplyOperation, BorrowOperation, RepayOperation, WithdrawOperation)
    ):
        raise ValueError("Compound capacity check requires an economic operation")
    funding(before, operation)
    after = validate_projection(config, before, operation)
    paused(before, operation, after)
    supply_cap(before, operation, after)
    borrow_bounds(before, operation, after)
    return after
