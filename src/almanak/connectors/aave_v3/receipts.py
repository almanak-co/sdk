"""Pool event attribution and exact scaled-share reconciliation at the mined block."""

from almanak.connectors.aave_v3.math import (
    borrow_shares,
    debt_assets,
    normalized_indices,
    repay_shares,
    supplied_assets,
    supply_shares,
    withdraw_shares,
)
from almanak.connectors.aave_v3.operations import (
    BorrowOperation,
    CollateralOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.pool_lending_receipts import accounting_issues as accounting_issues
from almanak.connectors.pool_lending_receipts import allowance_after as allowance_after
from almanak.connectors.pool_lending_receipts import economic_movement as economic_movement
from almanak.connectors.pool_lending_receipts import entry_amount as entry_amount
from almanak.connectors.pool_lending_receipts import event as event
from almanak.connectors.pool_lending_receipts import indexed as indexed
from almanak.connectors.pool_lending_receipts import movement as movement
from almanak.connectors.pool_lending_receipts import values as values
from almanak.connectors.pool_lending_receipts import wallet_direction as wallet_direction
from almanak.gateway.ethereum import DataError


def expected_shares(operation, before, after, assets):
    supply_index, debt_index = normalized_indices(after.position.reserve, after.block.timestamp)
    if isinstance(operation, SupplyOperation):
        return supply_shares(assets, supply_index), 0
    if isinstance(operation, BorrowOperation):
        return 0, borrow_shares(assets, debt_index)
    if isinstance(operation, WithdrawOperation):
        expected_assets = (
            supplied_assets(before.position.supplied_scaled, supply_index)
            if operation.full
            else operation.amount.amount
        )
        if assets != expected_assets:
            raise DataError("receipt_mismatch", "withdrawal amount differs from owned shares")
        return -before.position.supplied_scaled if operation.full else -withdraw_shares(
            assets, supply_index
        ), 0
    if isinstance(operation, RepayOperation):
        expected_assets = min(
            operation.amount.amount, debt_assets(before.position.debt_scaled, debt_index)
        )
        if assets != expected_assets:
            raise DataError("receipt_mismatch", "repayment amount differs from bounded debt")
        return 0, -repay_shares(assets, debt_index)
    return 0, 0


def validate_effects(pending, after, assets, success):
    before = pending.before
    operation = pending.operation
    supply, debt = expected_shares(operation, before, after, assets) if success else (0, 0)
    expected_allowance = (
        allowance_after(operation, before.allowance, assets) if success else before.allowance
    )
    if (after.position.supplied_scaled, after.position.debt_scaled, after.allowance) != (
        before.position.supplied_scaled + supply,
        before.position.debt_scaled + debt,
        expected_allowance,
    ):
        raise DataError(
            "ownership_mismatch", "mined effects do not reconcile with owned shares and allowance"
        )
    enabled = before.position.collateral_enabled
    if success and isinstance(operation, CollateralOperation):
        enabled = operation.enabled
    if success and isinstance(operation, WithdrawOperation) and not after.position.supplied_scaled:
        enabled = False
    if success and isinstance(operation, SupplyOperation) and not before.position.supplied_scaled:
        enabled = after.position.reserve.configuration.first_supply_enables_collateral
    if after.position.collateral_enabled != enabled:
        raise DataError(
            "ownership_mismatch", "mined collateral flag differs from the Pool operation"
        )
    return supply, debt
