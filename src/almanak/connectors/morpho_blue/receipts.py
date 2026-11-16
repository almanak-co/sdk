"""Morpho receipt events attribute exact assets and shares to the configured fund."""

from eth_utils import keccak

from almanak.connectors.morpho_blue.domain import Position, Uint128
from almanak.connectors.morpho_blue.math import uint
from almanak.connectors.morpho_blue.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.gateway import evm_logs
from almanak.gateway.ethereum import DataError
from almanak.models import Model


class Movement(Model):
    assets: Uint128
    shares: Uint128


def approval_event(receipt, operation):
    target = operation.target
    topic = "0x" + keccak(text="Approval(address,address,uint256)").hex()
    event = evm_logs.one_event(receipt, target.asset.reference, topic, 3)
    amount = evm_logs.data(event, ("uint256",))[0]
    expected = operation.amount.amount if isinstance(operation, ApproveOperation) else 0
    if (
        evm_logs.topic_address(event["topics"][1]),
        evm_logs.topic_address(event["topics"][2]),
        amount,
    ) != (target.fund.address, target.reserve.market.core, expected):
        raise DataError("receipt_mismatch", "Morpho approval has another owner, spender or bound")
    return Movement(assets=0, shares=0)


def economic_event(receipt, operation):
    target = operation.target
    incoming = isinstance(operation, (SupplyOperation, RepayOperation))
    loan = target.reserve.is_loan
    name = operation.kind.capitalize() + ("" if loan else "Collateral")
    fields = (
        "bytes32,address,address,"
        + ("" if incoming else "address,")
        + "uint256"
        + (",uint256" if loan else "")
    )
    topic = "0x" + keccak(text=f"{name}({fields})").hex()
    event = evm_logs.one_event(receipt, target.reserve.market.core, topic, 4)
    if "0x" + evm_logs.topic_bytes(event["topics"][1]).hex() != str(
        target.reserve.market.market_id
    ):
        raise DataError("receipt_mismatch", "Morpho event belongs to another market")
    if any(
        evm_logs.topic_address(event["topics"][index]) != target.fund.address for index in (2, 3)
    ):
        raise DataError(
            "receipt_mismatch", "Morpho event identifies another caller, owner or receiver"
        )
    types = (() if incoming else ("address",)) + ("uint256",) * (2 if loan else 1)
    values = evm_logs.data(event, types)
    if not incoming:
        if values[0] != target.fund.address:
            raise DataError("receipt_mismatch", "Morpho event caller differs from the fund")
        values = values[1:]
    return Movement(assets=values[0], shares=values[1] if loan else 0)


def movement(receipt, operation):
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        return approval_event(receipt, operation)
    result = economic_event(receipt, operation)
    shares = operation.shares if isinstance(operation, (RepayOperation, WithdrawOperation)) else 0
    if shares:
        if result.shares != shares:
            raise DataError(
                "receipt_mismatch", "Morpho full action changed its retained share count"
            )
        if isinstance(operation, RepayOperation) and result.assets > operation.amount.amount:
            raise DataError(
                "receipt_mismatch", "Morpho full repayment exceeded its allowance ceiling"
            )
    elif result.assets != operation.amount.amount:
        raise DataError(
            "receipt_mismatch", "Morpho event quantity differs from its explicit action"
        )
    return result


def allowance_after(operation, previous, assets):
    if isinstance(operation, ApproveOperation):
        return operation.amount.amount
    if isinstance(operation, RevokeOperation):
        return 0
    return (
        uint(previous - assets)
        if isinstance(operation, (SupplyOperation, RepayOperation))
        else previous
    )


def expected_position(before, operation, result):
    values = before.account.position.model_dump()
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        return before.account.position
    if operation.target.reserve.is_loan:
        key = (
            "borrow_shares"
            if isinstance(operation, (BorrowOperation, RepayOperation))
            else "supply_shares"
        )
        direction = 1 if isinstance(operation, (BorrowOperation, SupplyOperation)) else -1
        values[key] = uint(values[key] + direction * result.shares)
    else:
        direction = 1 if isinstance(operation, SupplyOperation) else -1
        values["collateral"] = uint(values["collateral"] + direction * result.assets, 128)
    return Position(**values)


def validate_effects(pending, after, result, success):
    before, operation = pending.before, pending.operation
    expected = expected_position(before, operation, result) if success else before.account.position
    allowance = (
        allowance_after(operation, before.allowance, result.assets) if success else before.allowance
    )
    if after.account.position != expected or after.allowance != allowance:
        raise DataError(
            "ownership_mismatch", "Morpho mined shares, collateral or allowance do not reconcile"
        )
    if not success and (result.assets or result.shares):
        raise DataError("receipt_mismatch", "failed Morpho execution cannot have economic effects")
    previous = before.account.position
    return (
        expected.supply_shares - previous.supply_shares,
        expected.borrow_shares - previous.borrow_shares,
        expected.collateral - previous.collateral,
    )


def wallet_direction(operation):
    if isinstance(operation, (SupplyOperation, RepayOperation)):
        return -1
    if isinstance(operation, (BorrowOperation, WithdrawOperation)):
        return 1
    return 0


def accounting_issues(before, after, operation, result, gas_paid):
    issues = []
    if after.wallet_balance != before.wallet_balance + wallet_direction(operation) * result.assets:
        issues.append("fund token balance includes unexplained external movements")
    if (
        after.payer != before.payer
        or after.native_balance.amount != before.native_balance.amount - gas_paid
    ):
        issues.append("gas payer balance includes unexplained external movements")
    if (
        operation.target.reserve.is_loan
        and isinstance(operation, (SupplyOperation, RepayOperation))
        and result.assets
        and not result.shares
    ):
        issues.append("loan assets transferred without changing any position shares")
    return tuple(issues)
