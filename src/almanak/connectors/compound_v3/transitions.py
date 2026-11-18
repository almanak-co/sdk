"""Economic Comet transitions forbid unintended crossings of signed base ownership."""

from almanak.connectors.compound_v3.domain import Principal, Target, Uint128
from almanak.connectors.compound_v3.math import present_value, principal_value, uint
from almanak.connectors.compound_v3.operations import (
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.models import AssetAmount, Model


class Transition(Model):
    target: Target
    principal: Principal
    collateral: Uint128
    assets: AssetAmount


def base_amount(principal, operation, indices):
    balance = present_value(principal, *indices)
    amount = operation.amount.amount
    if isinstance(operation, SupplyOperation):
        if principal < 0:
            raise ValueError("Compound supply cannot silently repay existing base debt")
        return balance + amount, amount
    if isinstance(operation, BorrowOperation):
        if principal > 0:
            raise ValueError("Compound borrowing cannot silently withdraw existing base supply")
        return balance - amount, amount
    if isinstance(operation, RepayOperation):
        if principal >= 0:
            raise ValueError("Compound repayment requires existing base debt")
        paid = -balance if operation.full else amount
        if not paid or paid > amount or paid > -balance:
            raise ValueError("Compound repayment exceeds the debt or its approved amount")
        return balance + paid, paid
    if isinstance(operation, WithdrawOperation):
        if principal <= 0:
            raise ValueError("Compound withdrawal requires existing base supply")
        withdrawn = balance if operation.full else amount
        if not withdrawn or withdrawn > balance:
            raise ValueError("Compound withdrawal exceeds its owned base supply")
        return balance - withdrawn, withdrawn
    raise ValueError("Compound transition requires an economic operation")


def transition(principal, collateral, operation, indices) -> Transition:
    target = operation.target
    if target.reserve.is_base:
        balance, amount = base_amount(principal, operation, indices)
        result = principal_value(balance, *indices)
        if result == principal:
            raise ValueError("Compound operation produces no principal movement")
        return Transition(
            target=target,
            principal=result,
            collateral=0,
            assets=AssetAmount(asset=target.asset, amount=amount),
        )
    quantity = collateral_transition(collateral, operation)
    return Transition(
        target=target,
        principal=0,
        collateral=quantity,
        assets=AssetAmount(asset=target.asset, amount=operation.amount.amount),
    )


def collateral_transition(collateral, operation) -> int:
    uint(collateral, 128)
    amount = operation.amount.amount
    if isinstance(operation, SupplyOperation):
        return uint(collateral + amount, 128)
    if isinstance(operation, WithdrawOperation):
        if amount > collateral or (operation.full and amount != collateral):
            raise ValueError("Compound collateral withdrawal differs from its owned balance")
        return collateral - amount
    raise ValueError("Compound collateral supports only economic supply and withdrawal")
