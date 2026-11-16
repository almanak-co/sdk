"""Economic actions require owned shares, bounded funding and independent market liquidity."""

from almanak.connectors.lending_health import health_floor
from almanak.connectors.morpho_blue.operations import (
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.morpho_blue.risk import accrued_accounting, require_health
from almanak.connectors.morpho_blue.transitions import transition
from almanak.connectors.repayment import require_repayment_funding
from almanak.gateway.ethereum import DataError


def retained_shares(before, operation):
    owned = before.account.position
    if (
        isinstance(operation, RepayOperation)
        and operation.full
        and operation.shares != owned.borrow_shares
    ):
        raise DataError(
            "ownership_mismatch", "Morpho full repayment must retain every owned debt share"
        )
    if (
        isinstance(operation, WithdrawOperation)
        and operation.full
        and operation.target.reserve.is_loan
        and operation.shares != owned.supply_shares
    ):
        raise DataError(
            "ownership_mismatch", "Morpho full withdrawal must retain every owned supply share"
        )


def projected_transition(before, operation):
    account = before.account
    accounting = (
        account.market.accounting
        if isinstance(operation, SupplyOperation) and not operation.target.reserve.is_loan
        else accrued_accounting(account)
    )
    return transition(accounting, account.position, operation)


def funding_requirement(before, operation, result=None):
    if not isinstance(operation, (SupplyOperation, RepayOperation)):
        raise ValueError("Morpho funding requires a supply or repayment operation")
    projected = projected_transition(before, operation) if result is None else result
    if projected.assets > operation.amount.amount:
        raise DataError(
            "funding_unavailable",
            "Morpho economic step exceeds its approved allowance",
        )
    return operation.amount.model_copy(update={"amount": projected.assets})


def incoming_funding(before, operation, result):
    if before.allowance != operation.amount.amount:
        raise DataError(
            "allowance_mismatch", "Morpho economic step requires its exact retained allowance"
        )
    needed = funding_requirement(before, operation, result).amount
    if isinstance(operation, RepayOperation):
        target = operation.target
        require_repayment_funding(target.allocation, target.asset, needed, before.wallet_balance)
    elif needed > before.wallet_balance:
        raise DataError("funding_unavailable", "Morpho supply exceeds the fund's inventory")


def core_liquidity(before, result):
    if result.assets > before.account.market.core_loan_balance:
        raise DataError(
            "liquidity_unavailable", "Morpho core cannot transfer the required loan inventory"
        )


def funding(before, operation, result):
    if isinstance(operation, (SupplyOperation, RepayOperation)):
        incoming_funding(before, operation, result)
    elif operation.target.reserve.is_loan:
        core_liquidity(before, result)


def economic_effect(before, operation, result):
    if not operation.target.reserve.is_loan:
        return
    owned = before.account.position
    if (
        isinstance(operation, SupplyOperation)
        and result.position.supply_shares <= owned.supply_shares
    ):
        raise DataError(
            "rounding_unavailable", "Morpho supply would transfer assets without receiving shares"
        )
    if (
        isinstance(operation, RepayOperation)
        and result.position.borrow_shares >= owned.borrow_shares
    ):
        raise DataError(
            "rounding_unavailable",
            "Morpho repayment would transfer assets without reducing debt shares",
        )


def operation_scope(config, before, operation):
    if not isinstance(
        operation, (SupplyOperation, BorrowOperation, RepayOperation, WithdrawOperation)
    ):
        raise ValueError("Morpho capacity requires an economic operation")
    if before.target != operation.target or operation.target != config.target(operation.allocation):
        raise ValueError("Morpho capacity cannot change its configured fund or market")
    retained_shares(before, operation)


def transition_risk(config, before, operation, result):
    account = before.account
    funding(before, operation, result)
    economic_effect(before, operation, result)
    if (
        isinstance(operation, BorrowOperation)
        or isinstance(operation, WithdrawOperation)
        and not operation.target.reserve.is_loan
    ):
        require_health(
            account,
            result.accounting,
            result.position,
            health_floor(config.allocation(operation.allocation).minimum_health_factor),
        )


def validate(config, before, operation):
    operation_scope(config, before, operation)
    result = projected_transition(before, operation)
    transition_risk(config, before, operation, result)
    return result
