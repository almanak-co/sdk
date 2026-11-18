"""GM requests check owned inventory and request-sized allowances before exact escrow."""

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.gmx_v2.exchange_planning import approval, cleanup, validate_deadline
from almanak.connectors.gmx_v2.liquidity_operations import (
    CancelLiquidityRequest,
    CreateDeposit,
    CreateWithdrawal,
)
from almanak.dex_intents import (
    CloseLiquidity,
    DecreaseLiquidity,
    IncreaseLiquidity,
    LiquidityBudget,
    LiquidityMinimum,
    OpenLiquidity,
    RevokeExchange,
)
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount


def admit(capability, intent):
    if isinstance(intent, RevokeExchange):
        return
    if isinstance(intent, LiquidityBudget):
        return
    if isinstance(intent, LiquidityMinimum):
        if any(
            actual.amount < required.amount
            for actual, required in zip(
                (intent.minimum0, intent.minimum1), capability.minimum_outputs.root, strict=True
            )
        ):
            raise ValueError("GM withdrawal output minimum is below its configured asset floor")
        return
    raise ValueError("GM liquidity capability cannot authorize a swap or another economic action")


def request_for(config, intent, before):
    allocation = config.allocation(intent.target.allocation)
    if isinstance(intent, LiquidityBudget):
        if isinstance(intent, OpenLiquidity) and before.owned.market_tokens.amount:
            raise ValueError("GM open requires no retained shares")
        if isinstance(intent, IncreaseLiquidity) and not before.owned.market_tokens.amount:
            raise ValueError("GM increase requires an existing owned share position")
        return CreateDeposit(
            target=intent.target,
            amounts=AssetAmounts((intent.maximum0, intent.maximum1)),
            minimum_market_tokens=allocation.minimum_market_tokens,
            execution_fee=allocation.execution_fee,
            admission_deadline=intent.deadline,
        )
    if not isinstance(intent, (DecreaseLiquidity, CloseLiquidity)):
        raise ValueError("unsupported GM liquidity request")
    amount = (
        before.owned.market_tokens.amount
        if isinstance(intent, CloseLiquidity)
        else intent.liquidity
    )
    if not 0 < amount <= before.owned.market_tokens.amount:
        raise ValueError("GM withdrawal requires a positive retained share balance")
    return CreateWithdrawal(
        target=intent.target,
        market_tokens=AssetAmount(asset=intent.target.market.share_asset, amount=amount),
        minimum_outputs=AssetAmounts((intent.minimum0, intent.minimum1)),
        execution_fee=allocation.execution_fee,
        admission_deadline=intent.deadline,
    )


def operation(config, state, intent, before):
    from almanak.connectors.gmx_v2.authority import admit as admit_intent

    _validate_target(config, intent, before)
    if _has_unresolved_request(state):
        return cancellation(state, before)
    admit_intent(state.capabilities, intent)
    if isinstance(intent, RevokeExchange):
        return cleanup(before)
    return _request_operation(config, state, intent, before)


def _validate_target(config, intent, before):
    if intent.target != before.target or intent.target != config.target(intent.target.allocation):
        raise ValueError("GM planning requires the exact configured economic target")


def _has_unresolved_request(state):
    return any(request.unresolved for request in state.requests)


def _request_operation(config, state, intent, before):
    allocation = config.allocation(intent.target.allocation)
    validate_deadline(intent.deadline, before.block.timestamp, allocation.deadline_seconds)
    request = request_for(config, intent, before)
    validate_inventory(request, before)
    return approval(request, before.allowances) or request


def validate_inventory(request, before):
    inventory = {
        value.asset: value.amount for value in (*before.owned.inventory, before.owned.market_tokens)
    }
    if any(value.amount > inventory[value.asset] for value in request.inputs):
        raise DataError("inventory", "GM request exceeds its owned currency or share inventory")


def cancellation(state, before):
    from almanak.connectors.gmx_v2.request_binding import active

    request = active(state)
    if state.mode != "teardown" or before.request != request or before.pending_liquidity is None:
        raise ValueError("unresolved GM request permits only its exact teardown cancellation")
    if not before.pending_liquidity.cancellable:
        raise DataError("stale_plan", "retained GM liquidity request is not currently cancellable")
    return CancelLiquidityRequest(
        target=before.target, request_kind=request.kind, request_key=request.key
    )
