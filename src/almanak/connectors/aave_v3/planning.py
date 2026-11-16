"""Economic lending steps; retained successful effects cannot be executed twice."""

from almanak.connectors.aave_v3.authority import admit
from almanak.connectors.aave_v3.math import (
    borrow_shares,
    debt_assets,
    supplied_assets,
    supply_shares,
    transition_indices,
    treasury_shares,
    uint,
    withdraw_shares,
)
from almanak.connectors.aave_v3.operations import (
    ApproveOperation,
    BorrowOperation,
    CollateralOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.pool_lending_lifecycle import validate_ownership as validate_ownership
from almanak.connectors.repayment import require_repayment_funding
from almanak.gateway.ethereum import DataError
from almanak.lending_intents import (
    BorrowIntent,
    CollateralIntent,
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
        raise ValueError("planning requires the exact retained economic decision")
    return any(
        outcome.success
        and outcome.decision.identity == state.decision.identity
        and outcome.operation.kind == economic_kind(intent)
        for outcome in state.outcomes
    )


def operation(config, state, intent, before):
    if before.target != intent.target or intent.target != config.target(intent.target.allocation):
        raise ValueError("operation cannot change the configured target or observation scope")
    if state.capabilities is None:
        raise ValueError("Aave V3 requires retained economic capabilities")
    admit(state.capabilities, intent)
    if executed(state, intent):
        if before.allowance:
            return RevokeOperation(target=intent.target)
        raise DataError("incomplete_action", "recorded economic effect cannot be executed again")
    if isinstance(intent, SupplyIntent):
        return funded(intent.target, before.allowance, intent.amount, "supply")
    if isinstance(intent, BorrowIntent):
        return BorrowOperation(target=intent.target, amount=intent.amount)
    if isinstance(intent, CollateralIntent):
        return CollateralOperation(target=intent.target, enabled=intent.enabled)
    return exit_operation(intent, before)


def funded(target, allowance, amount, purpose):
    if allowance == 0:
        return ApproveOperation(target=target, amount=amount, purpose=purpose)
    if allowance != amount.amount:
        raise DataError("allowance_mismatch", "allowance differs from the bounded economic step")
    if purpose == "supply":
        return SupplyOperation(target=target, amount=amount)
    return RepayOperation(target=target, amount=amount, full=False)


def full_repayment(target, before):
    """Approve the held balance, then repay with the Pool sentinel.

    The Pool pulls only the debt at inclusion, so approving the held balance rather
    than the observed debt absorbs interest accrued between the two transactions.
    """
    debt = before.position.debt_assets
    require_repayment_funding(target.allocation, target.asset, debt, before.wallet_balance)
    if before.allowance and before.allowance < debt:
        return RevokeOperation(target=target)
    if not before.allowance:
        amount = AssetAmount(asset=target.asset, amount=before.wallet_balance)
        return ApproveOperation(target=target, amount=amount, purpose="repay")
    amount = AssetAmount(asset=target.asset, amount=before.allowance)
    return RepayOperation(target=target, amount=amount, full=True)


def exit_operation(intent, before):
    if isinstance(intent, (WithdrawIntent, WithdrawAmountIntent)):
        if not before.position.supplied_scaled:
            return residual_allowance(intent, before)
        full = isinstance(intent, WithdrawIntent)
        amount = (
            AssetAmount(asset=intent.target.asset, amount=before.position.supplied_assets)
            if isinstance(intent, WithdrawIntent)
            else intent.amount
        )
        return WithdrawOperation(target=intent.target, amount=amount, full=full)
    if isinstance(intent, (RepayIntent, RepayAmountIntent)):
        if not before.position.debt_scaled:
            return residual_allowance(intent, before)
        if isinstance(intent, RepayIntent):
            return full_repayment(intent.target, before)
        if intent.amount.amount > before.position.debt_assets:
            raise DataError("repayment_exceeds_debt", "partial repayment exceeds the owned debt")
        return funded(intent.target, before.allowance, intent.amount, "repay")
    raise ValueError("unsupported Aave V3 economic intent")


def residual_allowance(intent, before):
    if not before.allowance:
        raise DataError("no_position", "requested position is already closed with no allowance")
    return RevokeOperation(target=intent.target)


def reserve_available(reserve, *, entry=False):
    config = reserve.configuration
    if not config.active or config.paused or (entry and config.frozen):
        raise DataError(
            "reserve_unavailable", "Aave V3 reserve is inactive, paused or frozen for entry"
        )


def supply_capacity(before, amount):
    reserve = before.position.reserve
    reserve_available(reserve, entry=True)
    index, _ = transition_indices(reserve, before.block.timestamp)
    scaled = supply_shares(amount, index)
    uint(before.position.supplied_scaled + scaled, 120)
    if not scaled or before.wallet_balance < amount:
        raise DataError(
            "supply_unavailable", "supply requires nonzero shares and sufficient inventory"
        )
    total = supplied_assets(
        reserve.total_scaled_supply + treasury_shares(reserve, before.block.timestamp) + scaled,
        index,
    )
    if reserve.configuration.supply_cap and total > reserve.configuration.supply_cap:
        raise DataError("supply_cap", "supply would exceed the observed Pool cap")


def borrow_capacity(before, amount):
    reserve = before.position.reserve
    reserve_available(reserve, entry=True)
    if not reserve.configuration.borrowing:
        raise DataError(
            "borrow_unavailable", "ordinary-mode borrowing is disabled for this reserve"
        )
    supply_index, index = transition_indices(reserve, before.block.timestamp)
    scaled = borrow_shares(amount, index)
    uint(before.position.debt_scaled + scaled, 120)
    if min(reserve.available_underlying, reserve.virtual_underlying_balance) < amount or (
        supplied_assets(reserve.total_scaled_supply, supply_index) < debt_assets(scaled, index)
    ):
        raise DataError("liquidity_unavailable", "reserve cannot settle the requested borrowing")
    total = debt_assets(reserve.total_scaled_debt + scaled, index)
    if reserve.configuration.borrow_cap and total > reserve.configuration.borrow_cap:
        raise DataError("borrow_cap", "borrowing would exceed the observed Pool cap")


def withdraw_capacity(before, operation):
    reserve = before.position.reserve
    reserve_available(reserve)
    index, _ = transition_indices(reserve, before.block.timestamp)
    scaled = (
        before.position.supplied_scaled
        if operation.full
        else withdraw_shares(operation.amount.amount, index)
    )
    amount = supplied_assets(scaled, index) if operation.full else operation.amount.amount
    if not scaled or scaled > before.position.supplied_scaled:
        raise DataError("ownership_mismatch", "withdrawal exceeds the owned reserve shares")
    if min(reserve.available_underlying, reserve.virtual_underlying_balance) < amount:
        raise DataError("liquidity_unavailable", "reserve cannot settle the owned withdrawal")


def repayment_capacity(before, operation):
    reserve_available(before.position.reserve)
    debt = before.position.debt_assets
    if not before.position.debt_scaled:
        raise DataError("repayment_unavailable", "repayment requires owned debt")
    target = operation.target
    needed = debt if operation.full else operation.amount.amount
    if not operation.full and needed > debt:
        raise DataError("repayment_exceeds_debt", "partial repayment exceeds the owned debt")
    require_repayment_funding(target.allocation, target.asset, needed, before.wallet_balance)
    if operation.full and operation.amount.amount < debt:
        raise DataError("allowance_mismatch", "repayment approval no longer covers the debt")
