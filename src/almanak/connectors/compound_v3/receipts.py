"""Exact Comet event attribution and signed-principal reconciliation at the mined block."""

from eth_abi import decode
from eth_utils import keccak

from almanak.connectors.compound_v3.observations import owned
from almanak.connectors.compound_v3.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.compound_v3.transitions import transition
from almanak.gateway.ethereum import DataError


def event(receipt, address, signature, topics):
    signature_hash = "0x" + keccak(text=signature).hex()
    logs = [
        log
        for log in receipt["logs"]
        if log["address"].lower() == address
        and log["topics"]
        and log["topics"][0] == signature_hash
    ]
    if len(logs) != 1 or len(logs[0]["topics"]) != topics:
        raise DataError("receipt_mismatch", "expected exactly one attributed Comet event")
    return logs[0]


def indexed(log, index):
    return decode(("address",), bytes.fromhex(log["topics"][index][2:]))[0]


def amount(log):
    raw = bytes.fromhex(log["data"][2:])
    if len(raw) != 32:
        raise DataError("receipt_mismatch", "Comet event amount is not one ABI word")
    return decode(("uint256",), raw)[0]


def movement(receipt, operation):
    target = operation.target
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        log = event(receipt, target.asset.reference, "Approval(address,address,uint256)", 3)
        expected = operation.amount.amount if isinstance(operation, ApproveOperation) else 0
        if (indexed(log, 1), indexed(log, 2), amount(log)) != (
            target.fund.address,
            target.reserve.market.comet,
            expected,
        ):
            raise DataError(
                "receipt_mismatch", "Compound approval has another owner, spender or bound"
            )
        return 0
    supplying = isinstance(operation, (SupplyOperation, RepayOperation))
    name = "Supply" if supplying else "Withdraw"
    signature = (
        name + "(address,address,uint256)"
        if target.reserve.is_base
        else name + "Collateral(address,address,address,uint256)"
    )
    log = event(receipt, target.reserve.market.comet, signature, 3 if target.reserve.is_base else 4)
    if indexed(log, 1) != target.fund.address or indexed(log, 2) != target.fund.address:
        raise DataError(
            "receipt_mismatch", "Comet event initiator or recipient differs from the fund"
        )
    if not target.reserve.is_base and indexed(log, 3) != target.asset.reference:
        raise DataError("receipt_mismatch", "Comet event identifies another collateral asset")
    return amount(log)


def allowance_after(operation, previous, assets):
    if isinstance(operation, ApproveOperation):
        return operation.amount.amount
    if isinstance(operation, RevokeOperation):
        return 0
    if isinstance(operation, (SupplyOperation, RepayOperation)):
        return previous - assets
    return previous


def expected_ownership(before, after, operation, assets, success):
    previous = owned(before)
    if not success or isinstance(operation, (ApproveOperation, RevokeOperation)):
        if assets:
            raise DataError("receipt_mismatch", "non-economic Comet outcome cannot transfer assets")
        return previous
    accounting = after.account.market.accounting
    result = transition(
        before.account.principal,
        previous.collateral,
        operation,
        (accounting.supply_index, accounting.borrow_index),
    )
    if result.assets.amount != assets:
        raise DataError(
            "receipt_mismatch", "Comet event quantity differs from its economic transition"
        )
    return previous.model_copy(
        update={"principal": result.principal, "collateral": result.collateral}
    )


def validate_effects(pending, after, assets, success):
    before, operation = pending.before, pending.operation
    expected = expected_ownership(before, after, operation, assets, success)
    allowance = (
        allowance_after(operation, before.allowance, assets) if success else before.allowance
    )
    if owned(after) != expected or after.allowance != allowance:
        raise DataError(
            "ownership_mismatch", "Comet mined ownership or allowance does not reconcile"
        )
    validate_other_positions(before, after, operation, success)
    previous = owned(before)
    return expected.principal - previous.principal, expected.collateral - previous.collateral


def validate_other_positions(before, after, operation, success):
    base = operation.target.reserve.is_base
    if not base and before.account.principal != after.account.principal:
        raise DataError("ownership_mismatch", "collateral action changed Comet base principal")
    balances = tuple(
        (c.asset, c.balance)
        for c in before.account.collaterals
        if base or c.asset != operation.target.asset
    )
    current = tuple(
        (c.asset, c.balance)
        for c in after.account.collaterals
        if base or c.asset != operation.target.asset
    )
    if balances != current:
        raise DataError("ownership_mismatch", "Comet collateral changed outside the operated asset")
    expected = before.account.assets_in | (before.account.extended_assets_in << 16)
    if success and not base:
        collateral = next(
            c for c in before.account.market.collaterals if c.asset_type == operation.target.asset
        )
        previous, current_amount = owned(before).collateral, owned(after).collateral
        if previous == 0 and current_amount:
            expected |= 1 << collateral.offset
        elif previous and current_amount == 0:
            expected &= ~(1 << collateral.offset)
    if expected != after.account.assets_in | (after.account.extended_assets_in << 16):
        raise DataError("ownership_mismatch", "Comet collateral membership changed unexpectedly")


def wallet_direction(operation):
    if isinstance(operation, (SupplyOperation, RepayOperation)):
        return -1
    if isinstance(operation, (WithdrawOperation, BorrowOperation)):
        return 1
    return 0


def accounting_issues(before, after, operation, assets, gas_paid):
    issues = []
    if after.wallet_balance != before.wallet_balance + wallet_direction(operation) * assets:
        issues.append("fund token balance includes unexplained external movements")
    if (
        after.payer != before.payer
        or after.native_balance.amount != before.native_balance.amount - gas_paid
    ):
        issues.append("gas payer balance includes unexplained external movements")
    return tuple(issues)
