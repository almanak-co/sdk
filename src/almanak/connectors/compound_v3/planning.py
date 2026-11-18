"""Economic Comet steps retain bounded allowance and never replay recorded effects."""

from almanak.connectors.compound_v3.authority import admit
from almanak.connectors.compound_v3.observations import owned
from almanak.connectors.compound_v3.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.repayment import require_repayment_funding
from almanak.gateway.ethereum import DataError
from almanak.lending_intents import (
    BorrowIntent,
    RepayAmountIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawAmountIntent,
    WithdrawIntent,
)
from almanak.models import AssetAmount


def economic_kind(intent):
    if isinstance(intent, (WithdrawIntent, WithdrawAmountIntent)):
        return "withdraw"
    if isinstance(intent, (RepayIntent, RepayAmountIntent)):
        return "repay"
    return intent.kind


def executed(state, intent):
    if state.decision is None or state.decision.intent != intent:
        raise ValueError("Compound planning requires the exact retained economic decision")
    return any(
        outcome.success
        and outcome.decision.identity == state.decision.identity
        and outcome.operation.kind == economic_kind(intent)
        for outcome in state.outcomes
    )


def validate_ownership(retained, before):
    if retained != owned(before):
        raise DataError(
            "ownership_mismatch", "observed Comet balance differs from retained ownership"
        )


def funded(target, allowance, amount, purpose):
    if not amount.amount:
        raise DataError("funding_unavailable", "Compound step requires explicit nonzero funding")
    if allowance == 0:
        return ApproveOperation(target=target, amount=amount, purpose=purpose)
    if allowance != amount.amount:
        raise DataError(
            "allowance_mismatch", "Comet allowance differs from the exact retained bound"
        )
    if purpose == "supply":
        return SupplyOperation(target=target, amount=amount)
    return RepayOperation(target=target, amount=amount, full=False)


def full_repayment(target, before):
    """Approve the held balance, then repay with the Comet debt sentinel.

    `supply(base, 2**256-1)` repays exactly the debt at inclusion, so approving the
    held balance rather than the observed debt absorbs interest accrued between the
    two transactions. The sentinel moves only the Safe's own debt.
    """
    debt = before.account.borrowed_base
    require_repayment_funding(target.allocation, target.asset, debt, before.wallet_balance)
    if before.allowance and before.allowance < debt:
        return RevokeOperation(target=target)
    if not before.allowance:
        amount = AssetAmount(asset=target.asset, amount=before.wallet_balance)
        return ApproveOperation(target=target, amount=amount, purpose="repay")
    amount = AssetAmount(asset=target.asset, amount=before.allowance)
    return RepayOperation(target=target, amount=amount, full=True)


def operation(config, state, intent, before):
    if before.target != intent.target or intent.target != config.target(intent.target.allocation):
        raise ValueError("Compound operation cannot change its configured target or snapshot scope")
    if state.capabilities is None:
        raise ValueError("Compound planning requires retained economic capabilities")
    admit(state.capabilities, intent)
    if executed(state, intent):
        if before.allowance:
            return RevokeOperation(target=intent.target)
        raise DataError("incomplete_action", "recorded Comet effect cannot be executed again")
    if isinstance(intent, SupplyIntent):
        return funded(intent.target, before.allowance, intent.amount, "supply")
    if isinstance(intent, BorrowIntent):
        return BorrowOperation(target=intent.target, amount=intent.amount)
    return exit_operation(intent, before)


def exit_operation(intent, before):
    if isinstance(intent, (WithdrawIntent, WithdrawAmountIntent)):
        quantity = (
            before.account.supplied_base
            if intent.target.reserve.is_base
            else owned(before).collateral
        )
        if not quantity:
            return residual_allowance(intent, before)
        full = isinstance(intent, WithdrawIntent)
        amount = (
            AssetAmount(asset=intent.target.asset, amount=quantity)
            if isinstance(intent, WithdrawIntent)
            else intent.amount
        )
        return WithdrawOperation(target=intent.target, amount=amount, full=full)
    if isinstance(intent, (RepayIntent, RepayAmountIntent)):
        if before.account.principal >= 0:
            return residual_allowance(intent, before)
        if isinstance(intent, RepayIntent):
            return full_repayment(intent.target, before)
        # Comet turns a repayment above the debt into supply, so it is refused here.
        if intent.amount.amount > before.account.borrowed_base:
            raise DataError("repayment_exceeds_debt", "partial repayment exceeds the owned debt")
        return funded(intent.target, before.allowance, intent.amount, "repay")
    raise ValueError("unsupported Compound economic intent")


def residual_allowance(intent, before):
    if not before.allowance:
        raise DataError("no_position", "Comet position is closed with no remaining allowance")
    return RevokeOperation(target=intent.target)
