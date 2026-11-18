"""Integer market transitions applied after the operation's required interest accrual."""

from almanak.connectors.morpho_blue.domain import Accounting, Position, Uint256
from almanak.connectors.morpho_blue.math import to_assets, to_shares, uint
from almanak.connectors.morpho_blue.operations import (
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.models import Model


class Transition(Model):
    accounting: Accounting
    position: Position
    assets: Uint256


EconomicOperation = SupplyOperation | BorrowOperation | RepayOperation | WithdrawOperation


def loan_quantity(accounting: Accounting, operation: EconomicOperation):
    debt = isinstance(operation, (BorrowOperation, RepayOperation))
    total_assets = accounting.borrow_assets if debt else accounting.supply_assets
    total_shares = accounting.borrow_shares if debt else accounting.supply_shares
    shares = operation.shares if isinstance(operation, (RepayOperation, WithdrawOperation)) else 0
    if shares:
        assets = to_assets(shares, total_assets, total_shares, round_up=debt)
    else:
        assets = operation.amount.amount
        shares = to_shares(
            assets,
            total_assets,
            total_shares,
            round_up=isinstance(operation, (BorrowOperation, WithdrawOperation)),
        )
    return uint(assets, 128), uint(shares, 128)


def supply_transition(accounting, position, assets, shares, withdrawing):
    direction = -1 if withdrawing else 1
    market = accounting.model_dump()
    owned = position.model_dump()
    market["supply_assets"] = uint(accounting.supply_assets + direction * assets, 128)
    market["supply_shares"] = uint(accounting.supply_shares + direction * shares, 128)
    owned["supply_shares"] = uint(position.supply_shares + direction * shares)
    return Accounting(**market), Position(**owned)


def debt_transition(accounting, position, assets, shares, repaying):
    direction = -1 if repaying else 1
    market = accounting.model_dump()
    owned = position.model_dump()
    # Repaying with shares can round assets one unit above market debt.
    # Morpho clips this subtraction while still transferring the rounded amount.
    market["borrow_assets"] = uint(
        max(0, accounting.borrow_assets - assets)
        if repaying
        else accounting.borrow_assets + assets,
        128,
    )
    market["borrow_shares"] = uint(accounting.borrow_shares + direction * shares, 128)
    owned["borrow_shares"] = uint(position.borrow_shares + direction * shares, 128)
    return Accounting(**market), Position(**owned)


def transition(
    accounting: Accounting, position: Position, operation: EconomicOperation
) -> Transition:
    if not operation.target.reserve.is_loan:
        direction = 1 if isinstance(operation, SupplyOperation) else -1
        amount = operation.amount.amount
        owned = position.model_dump()
        owned["collateral"] = uint(position.collateral + direction * amount, 128)
        return Transition(accounting=accounting, position=Position(**owned), assets=amount)
    assets, shares = loan_quantity(accounting, operation)
    if isinstance(operation, (SupplyOperation, WithdrawOperation)):
        market, owned = supply_transition(
            accounting, position, assets, shares, isinstance(operation, WithdrawOperation)
        )
    else:
        market, owned = debt_transition(
            accounting, position, assets, shares, isinstance(operation, RepayOperation)
        )
    return Transition(accounting=market, position=owned, assets=assets)
