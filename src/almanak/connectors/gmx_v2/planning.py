"""Admitted GMX economic intent selects a request-sized allowance and one request."""

from almanak.connectors.gmx_v2.abi import input_amount
from almanak.connectors.gmx_v2.authority import admit
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.funding import claimable
from almanak.connectors.gmx_v2.intents import (
    ClosePosition,
    DecreasePosition,
    IncreasePosition,
    RevokePosition,
)
from almanak.connectors.gmx_v2.operations import Approval, CancelOrder, ClaimFunding, CreateOrder
from almanak.connectors.gmx_v2.request_binding import active
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount


def operation(config, state, intent, before):
    if isinstance(intent.target, ExchangeTarget):
        from almanak.connectors.gmx_v2.exchange_planning import operation as exchange_operation

        return exchange_operation(config, state, intent, before)
    return _position_operation(config, state, intent, before)


def _position_operation(config, state, intent, before):
    _validate_target(config, intent, before)
    if _has_unresolved_request(state):
        return cancellation(state, before)
    admit(state.capabilities, intent)
    if isinstance(intent, RevokePosition):
        return cleanup(before)
    request = economic_order(config.allocation(intent.target.allocation), intent, before)
    validate_inventory(request, before)
    return _approval_or_request(request, before)


def _validate_target(config, intent, before):
    if intent.target != before.target or intent.target != config.target(intent.target.allocation):
        raise ValueError("GMX planning requires its exact configured intent target")


def _has_unresolved_request(state):
    return any(request.unresolved for request in state.requests)


def _approval_or_request(request, before):
    amount = input_amount(request)
    if before.allowance != amount:
        return Approval(
            target=before.target,
            amount=AssetAmount(
                asset=request.execution_fee.asset, amount=0 if before.allowance else amount
            ),
        )
    return request


def cancellation(state, before):
    request = active(state)
    if state.mode != "teardown" or before.request != request or before.pending_order is None:
        raise ValueError("unresolved GMX requests permit only their exact admitted cancellation")
    if not before.pending_order.cancellable:
        raise DataError("stale_plan", "retained GMX request is not currently cancellable")
    return CancelOrder(target=before.target, request_key=request.key)


def economic_order(allocation, intent, before):
    increase = isinstance(intent, IncreasePosition)
    if increase:
        if not intent.size_delta_usd_e30 and before.owned.position is None:
            raise ValueError("GMX collateral-only increase requires a retained position")
        size, collateral = intent.size_delta_usd_e30, intent.collateral
    else:
        size, collateral = decrease_amounts(intent, before)
    return CreateOrder(
        target=before.target,
        order_type=2 if increase else 4,
        collateral_delta=collateral,
        size_delta_usd_e30=size,
        execution_fee=allocation.execution_fee,
        acceptable_price=allocation.increase_acceptable_price
        if increase
        else allocation.decrease_acceptable_price,
    )


def decrease_amounts(intent, before):
    position = before.owned.position
    if position is None:
        raise ValueError("GMX decrease requires a retained aggregate position")
    if isinstance(intent, ClosePosition):
        return position.size_usd_e30, AssetAmount(asset=before.target.collateral_asset, amount=0)
    if not isinstance(intent, DecreasePosition):
        raise ValueError("unsupported GMX economic action")
    if (
        intent.size_delta_usd_e30 > position.size_usd_e30
        or intent.collateral_withdrawal.amount > position.collateral.amount
    ):
        raise ValueError("GMX decrease exceeds retained position ownership")
    return intent.size_delta_usd_e30, intent.collateral_withdrawal


def validate_inventory(request, before):
    inventory = next(
        a.amount for a in before.owned.inventory if a.asset == request.execution_fee.asset
    )
    if input_amount(request) > inventory:
        raise DataError("inventory", "GMX request exceeds its owned wrapped-native inventory")


def cleanup(before):
    if before.allowance:
        return Approval(
            target=before.target,
            amount=AssetAmount(asset=before.target.market.wrapped_native, amount=0),
        )
    if any(value.amount for value in before.claimable_funding):
        return ClaimFunding(target=before.target, amounts=claimable(before))
    raise ValueError("GMX spending authority and owned funding are already closed")
