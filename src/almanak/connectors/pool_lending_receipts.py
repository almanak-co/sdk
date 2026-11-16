"""Compatible Pool event attribution and cash/allowance effects, without share math."""

from eth_abi import decode
from eth_utils import keccak

from almanak.connectors.pool_lending_operations import (
    ApproveOperation,
    BorrowOperation,
    CollateralOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.gateway.ethereum import DataError


def event(receipt, address, signature, count):
    topic = "0x" + keccak(text=signature).hex()
    logs = [
        log
        for log in receipt["logs"]
        if log["address"].lower() == address and log["topics"] and log["topics"][0] == topic
    ]
    if len(logs) != 1 or len(logs[0]["topics"]) != count:
        raise DataError("receipt_mismatch", "expected exactly one attributed Pool event")
    return logs[0]


def indexed(log, index, kind="address"):
    return decode((kind,), bytes.fromhex(log["topics"][index][2:]))[0]


def values(log, types):
    return decode(types, bytes.fromhex(log["data"][2:]))


def movement(receipt, operation):
    target = operation.target
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        log = event(receipt, target.asset.reference, "Approval(address,address,uint256)", 3)
        amount = operation.amount.amount if isinstance(operation, ApproveOperation) else 0
        if (indexed(log, 1), indexed(log, 2), values(log, ("uint256",))[0]) != (
            target.fund.address,
            target.reserve.pool,
            amount,
        ):
            raise DataError(
                "receipt_mismatch", "approval event has another owner, spender or bound"
            )
        return 0
    if isinstance(operation, CollateralOperation):
        name = (
            "ReserveUsedAsCollateralEnabled"
            if operation.enabled
            else "ReserveUsedAsCollateralDisabled"
        )
        log = event(receipt, target.reserve.pool, name + "(address,address)", 3)
        if (indexed(log, 1), indexed(log, 2), log["data"]) != (
            target.asset.reference,
            target.fund.address,
            "0x",
        ):
            raise DataError("receipt_mismatch", "collateral event differs from the owned reserve")
        return 0
    return economic_movement(receipt, operation)


def economic_movement(receipt, operation):
    signatures = {
        "supply": "Supply(address,address,address,uint256,uint16)",
        "borrow": "Borrow(address,address,address,uint256,uint8,uint256,uint16)",
        "withdraw": "Withdraw(address,address,address,uint256)",
        "repay": "Repay(address,address,address,uint256,bool)",
    }
    target = operation.target
    log = event(receipt, target.reserve.pool, signatures[operation.kind], 4)
    if indexed(log, 1) != target.asset.reference or indexed(log, 2) != target.fund.address:
        raise DataError("receipt_mismatch", "economic event has another reserve or fund")
    if isinstance(operation, (SupplyOperation, BorrowOperation)):
        return entry_amount(log, operation)
    if indexed(log, 3) != target.fund.address:
        raise DataError("receipt_mismatch", "withdrawal recipient or repayer is not the fund")
    if isinstance(operation, RepayOperation):
        amount, use_atokens = values(log, ("uint256", "bool"))
        if use_atokens:
            raise DataError("receipt_mismatch", "repayment unexpectedly consumed aTokens")
        return amount
    return values(log, ("uint256",))[0]


def entry_amount(log, operation):
    types = (
        ("address", "uint256")
        if isinstance(operation, SupplyOperation)
        else ("address", "uint256", "uint8", "uint256")
    )
    data = values(log, types)
    if data[0] != operation.target.fund.address or indexed(log, 3, "uint16") != 0:
        raise DataError(
            "receipt_mismatch", "Pool entry initiator or referral differs from its call"
        )
    if isinstance(operation, BorrowOperation) and data[2] != 2:
        raise DataError("receipt_mismatch", "borrowing did not create variable debt")
    if data[1] != operation.amount.amount:
        raise DataError("receipt_mismatch", "Pool entry amount differs from the admitted intent")
    return data[1]


def allowance_after(operation, previous, assets):
    if isinstance(operation, ApproveOperation):
        return operation.amount.amount
    if isinstance(operation, RevokeOperation):
        return 0
    if isinstance(operation, (SupplyOperation, RepayOperation)):
        return previous - assets
    return previous


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
    other_reserves = ~(3 << (2 * before.position.reserve.reserve_id))
    if (before.position.user_configuration & other_reserves) != (
        after.position.user_configuration & other_reserves
    ):
        issues.append("Pool exposure bitmap changed outside the operated reserve")
    return tuple(issues)
