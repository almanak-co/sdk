"""A GMX swap completes only with correlated request, route, output and transfer evidence."""

from almanak.connectors.gmx_v2.receipts import check_values, event_evidence, one_protocol_event
from almanak.connectors.gmx_v2.requests import Request
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS


def accepted_request(pending, receipt, block):
    operation, target = pending.operation, pending.operation.target
    event = one_protocol_event(
        receipt, target, "OrderCreated", target.market.contracts.order_handler
    )
    validate_created(event, operation)
    return Request(
        fund=target.fund,
        allocation=target.allocation,
        decision_identity=pending.decision.identity,
        kind="order",
        key=event.topics[0],
        market=target.market.share_asset,
        escrow=(operation.amount,),
        execution_fee=operation.execution_fee,
        admission_deadline=operation.admission_deadline,
        events=(event_evidence(event, block, receipt, "accepted"),),
    )


def validate_created(event, operation):
    target = operation.target
    check_values(
        event,
        "address",
        {
            "account": target.fund.address,
            "receiver": target.fund.address,
            "cancellationReceiver": target.fund.address,
            "callbackContract": ZERO_ADDRESS,
            "uiFeeReceiver": ZERO_ADDRESS,
            "market": ZERO_ADDRESS,
            "initialCollateralToken": operation.amount.asset.reference,
        },
    )
    check_values(
        event,
        "uint256",
        {
            "orderType": 0,
            "decreasePositionSwapType": 0,
            "sizeDeltaUsd": 0,
            "initialCollateralDeltaAmount": operation.amount.amount,
            "triggerPrice": 0,
            "acceptablePrice": 0,
            "executionFee": operation.execution_fee.amount,
            "callbackGasLimit": 0,
            "minOutputAmount": operation.minimum_output.amount,
            "uiFeeFactor": 0,
            "validFromTime": 0,
            "srcChainId": 0,
        },
    )
    check_values(
        event, "bool", {"isLong": False, "shouldUnwrapNativeToken": False, "autoCancel": False}
    )
    if event.array("address", "swapPath") != (target.market.share_asset.reference,):
        raise DataError("receipt", "GMX swap creation differs from its exact one-hop market route")


def executed_output(operation, request, receipt):
    target = operation.target
    event = one_protocol_event(receipt, target, "SwapInfo", target.market.contracts.swap_handler)
    check_values(event, "bytes32", {"orderKey": request.key})
    check_values(
        event,
        "address",
        {
            "market": target.market.share_asset.reference,
            "receiver": target.fund.address,
            "tokenIn": operation.amount.asset.reference,
            "tokenOut": operation.minimum_output.asset.reference,
        },
    )
    check_values(event, "uint256", {"amountIn": operation.amount.amount})
    amount = event.value("uint256", "amountOut")
    if amount < operation.minimum_output.amount:
        raise DataError("receipt", "GMX swap output is below its retained requested minimum")
    return amount


def validate_cash(operation, request, receipt, assets):
    if (request.escrow, request.execution_fee, request.admission_deadline) != (
        (operation.amount,),
        operation.execution_fee,
        operation.admission_deadline,
    ):
        raise DataError(
            "receipt", "GMX swap request changed its retained escrow, fee or admission deadline"
        )
    expected = {}
    if request.status == "accepted":
        expected = {value.asset: -value.amount for value in operation.inputs}
    elif request.status == "executed":
        expected = {operation.minimum_output.asset: executed_output(operation, request, receipt)}
    elif request.status == "cancelled":
        expected = {operation.amount.asset: operation.amount.amount}
    if any(
        value.amount != expected.get(value.key.asset, 0)
        for value in assets
        if value.key.asset.namespace != "slip44"
    ):
        raise DataError(
            "receipt", "GMX swap wallet effects differ from its exact request escrow or output"
        )
