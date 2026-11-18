"""Request-sized per-asset allowances precede one admitted asynchronous GMX swap."""

from almanak.connectors.gmx_v2.exchange_operations import (
    CancelExchangeOrder,
    CreateSwap,
    ExchangeApproval,
)
from almanak.dex_intents import RevokeExchange, SwapExactInput
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount


def admit(capability, intent):
    if isinstance(intent, RevokeExchange):
        return
    if not isinstance(intent, SwapExactInput):
        raise ValueError("unsupported GMX exchange action")
    if intent.amount.asset not in capability.target.assets:
        raise ValueError("GMX swap input must be a configured market currency")


def operation(config, state, intent, before):
    from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation

    allocation = config.allocation(intent.target.allocation)
    if isinstance(allocation, LiquidityAllocation):
        from almanak.connectors.gmx_v2.liquidity_planning import operation as liquidity_operation

        return liquidity_operation(config, state, intent, before)
    return _swap_operation(config, state, intent, before)


def _swap_operation(config, state, intent, before):
    from almanak.connectors.gmx_v2.authority import admit as admit_intent

    _validate_target(config, intent, before)
    if _has_unresolved_request(state):
        return cancellation(state, before)
    admit_intent(state.capabilities, intent)
    if isinstance(intent, RevokeExchange):
        return cleanup(before)
    if not isinstance(intent, SwapExactInput):
        raise ValueError("unsupported GMX exchange action")
    allocation = config.allocation(intent.target.allocation)
    validate_deadline(intent.deadline, before.block.timestamp, allocation.deadline_seconds)
    request = CreateSwap(
        target=intent.target,
        amount=intent.amount,
        minimum_output=intent.minimum_output,
        execution_fee=allocation.execution_fee,
        admission_deadline=intent.deadline,
    )
    validate_inventory(request, before)
    return approval(request, before.allowances) or request


def _validate_target(config, intent, before):
    if intent.target != before.target or intent.target != config.target(intent.target.allocation):
        raise ValueError("GMX exchange planning requires the exact configured intent target")


def _has_unresolved_request(state):
    return any(request.unresolved for request in state.requests)


def approval(request, allowances):
    """Each escrowed input is approved for exactly this request; a stale residual is revoked."""
    current = {value.asset: value.amount for value in allowances}
    for value in request.inputs:
        allowance = current[value.asset]
        if allowance != value.amount:
            return ExchangeApproval(
                target=request.target,
                amount=AssetAmount(asset=value.asset, amount=0 if allowance else value.amount),
            )
    return None


def validate_deadline(deadline, timestamp, maximum_seconds):
    if not timestamp < deadline <= timestamp + maximum_seconds:
        raise DataError(
            "stale_plan", "GMX swap deadline is outside its configured admission window"
        )


def cancellation(state, before):
    from almanak.connectors.gmx_v2.request_binding import active

    request = active(state)
    if state.mode != "teardown" or before.request != request or before.pending_order is None:
        raise ValueError("unresolved GMX swap permits only its exact admitted cancellation")
    if not before.pending_order.cancellable:
        raise DataError("stale_plan", "retained GMX swap is not currently cancellable")
    return CancelExchangeOrder(target=before.target, request_key=request.key)


def validate_inventory(request, before):
    inventory = {value.asset: value.amount for value in before.owned.inventory}
    if any(value.amount > inventory[value.asset] for value in request.inputs):
        raise DataError("inventory", "GMX swap exceeds its owned input inventory")


def cleanup(before):
    for allowance in before.allowances:
        if allowance.amount:
            return ExchangeApproval(
                target=before.target, amount=allowance.model_copy(update={"amount": 0})
            )
    raise ValueError("GMX exchange spending authority is already closed")
