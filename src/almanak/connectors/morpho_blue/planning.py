"""Morpho planning preserves exact owned shares through approvals and outcome delivery."""

from almanak.connectors.morpho_blue.authority import admit
from almanak.connectors.morpho_blue.observations import owned
from almanak.connectors.morpho_blue.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.morpho_blue.risk import borrowed_assets, supplied_assets
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
        raise ValueError("Morpho planning requires the exact retained economic decision")
    return any(
        outcome.success
        and outcome.decision.identity == state.decision.identity
        and outcome.operation.kind == economic_kind(intent)
        for outcome in state.outcomes
    )


def validate_ownership(retained, before):
    if retained != owned(before):
        raise DataError(
            "ownership_mismatch",
            "observed Morpho shares or collateral differ from retained ownership",
        )


def funded(target, allowance, amount, purpose, *, shares=0):
    if allowance == 0:
        return ApproveOperation(target=target, amount=amount, purpose=purpose)
    if allowance != amount.amount:
        raise DataError(
            "allowance_mismatch", "Morpho allowance differs from the exact retained bound"
        )
    if purpose == "supply":
        return SupplyOperation(target=target, amount=amount)
    return RepayOperation(target=target, amount=amount, shares=shares)


def operation(config, state, intent, before):
    if before.target != intent.target or intent.target != config.target(intent.target.allocation):
        raise ValueError("Morpho operation cannot change its configured target or snapshot scope")
    if state.capabilities is None:
        raise ValueError("Morpho planning requires retained economic capabilities")
    admit(state.capabilities, intent)
    if executed(state, intent):
        if before.allowance:
            return RevokeOperation(target=intent.target)
        raise DataError("incomplete_action", "recorded Morpho effect cannot be executed again")
    if isinstance(intent, SupplyIntent):
        return funded(intent.target, before.allowance, intent.amount, "supply")
    if isinstance(intent, BorrowIntent):
        return BorrowOperation(target=intent.target, amount=intent.amount)
    if isinstance(intent, (WithdrawIntent, WithdrawAmountIntent)):
        return withdrawal(intent, before)
    if isinstance(intent, (RepayIntent, RepayAmountIntent)):
        return repayment(intent, before)
    raise ValueError("unsupported Morpho economic intent")


def withdrawal(intent, before):
    ownership = owned(before)
    loan = intent.target.reserve.is_loan
    if not (ownership.supply_shares if loan else ownership.collateral):
        return residual_allowance(intent, before)
    full = isinstance(intent, WithdrawIntent)
    quantity = supplied_assets(before.account) if loan else ownership.collateral
    amount = AssetAmount(asset=intent.target.asset, amount=quantity) if full else intent.amount
    return WithdrawOperation(
        target=intent.target,
        amount=amount,
        full=full,
        shares=ownership.supply_shares if full and loan else 0,
    )


def repayment(intent, before):
    shares = before.account.position.borrow_shares
    if not shares:
        return residual_allowance(intent, before)
    debt = borrowed_assets(before.account, before.block.timestamp)
    if isinstance(intent, RepayAmountIntent):
        if intent.amount.amount > debt:
            raise DataError("repayment_exceeds_debt", "partial repayment exceeds the owned debt")
        return funded(intent.target, before.allowance, intent.amount, "repay")
    return full_repayment(intent.target, before, shares, debt)


def full_repayment(target, before, shares, debt):
    """Approve the held balance, then repay every owned debt share.

    Morpho pulls the assets those shares are worth at inclusion, so approving the
    held balance rather than the observed debt absorbs interest accrued between the
    two transactions.
    """
    require_repayment_funding(target.allocation, target.asset, debt, before.wallet_balance)
    if before.allowance and before.allowance < debt:
        return RevokeOperation(target=target)
    if not before.allowance:
        amount = AssetAmount(asset=target.asset, amount=before.wallet_balance)
        return ApproveOperation(target=target, amount=amount, purpose="repay")
    amount = AssetAmount(asset=target.asset, amount=before.allowance)
    return RepayOperation(target=target, amount=amount, shares=shares)


def residual_allowance(intent, before):
    if not before.allowance:
        raise DataError("no_position", "Morpho position is closed with no remaining allowance")
    return RevokeOperation(target=intent.target)
