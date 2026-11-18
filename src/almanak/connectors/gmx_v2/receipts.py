"""GMX receipt facts attribute escrow, keeper fees and refunds to explicit fund assets."""

from eth_utils import keccak

from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.requests import ReceiptIdentity, Request, RequestEvidence
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_logs import data, events, topic_address
from almanak.models import ZERO_ADDRESS, Model, UInt

TRANSFER = "0x" + keccak(text="Transfer(address,address,uint256)").hex()


class ExecutionFee(Model):
    paid: UInt
    refunded: UInt
    fund_received: UInt


def protocol_events(receipt, target):
    return tuple(
        event
        for log in receipt["logs"]
        if (event := decode_event(log, emitter=target.market.contracts.event_emitter)) is not None
    )


def one_protocol_event(receipt, target, name, sender):
    found = [e for e in protocol_events(receipt, target) if e.name == name]
    if len(found) != 1 or found[0].sender != sender:
        raise DataError(
            "receipt", f"GMX receipt requires one {name} from the verified emitting contract"
        )
    return found[0]


def event_evidence(event, block, receipt, status):
    return RequestEvidence(
        status=status,
        key=event.topics[0],
        receipt=receipt,
        identity=ReceiptIdentity(
            block=block,
            transaction_hash=event.transaction_hash,
            log_index=event.log_index,
            emitter=event.emitter,
            handler=event.sender,
        ),
    )


def accepted_request(pending, receipt, block):
    operation = pending.operation
    target = operation.target
    event = one_protocol_event(
        receipt, target, "OrderCreated", target.market.contracts.order_handler
    )
    validate_created(event, operation)
    escrow = (operation.collateral_delta,) if operation.order_type == 2 else ()
    return Request(
        fund=target.fund,
        allocation=target.allocation,
        decision_identity=pending.decision.identity,
        kind="order",
        key=event.topics[0],
        market=target.market.share_asset,
        escrow=escrow,
        execution_fee=operation.execution_fee,
        events=(event_evidence(event, block, receipt, "accepted"),),
    )


def validate_created(event, operation):
    target = operation.target
    addresses = {
        "account": target.fund.address,
        "receiver": target.fund.address,
        "cancellationReceiver": target.fund.address,
        "callbackContract": ZERO_ADDRESS,
        "uiFeeReceiver": ZERO_ADDRESS,
        "market": target.market.share_asset.reference,
        "initialCollateralToken": target.collateral_asset.reference,
    }
    check_values(event, "address", addresses)
    check_values(
        event,
        "uint256",
        {
            "orderType": operation.order_type,
            "decreasePositionSwapType": 0,
            "sizeDeltaUsd": operation.size_delta_usd_e30,
            "initialCollateralDeltaAmount": operation.collateral_delta.amount,
            "triggerPrice": 0,
            "acceptablePrice": operation.acceptable_price,
            "executionFee": operation.execution_fee.amount,
            "callbackGasLimit": 0,
            "minOutputAmount": 0,
            "uiFeeFactor": 0,
            "validFromTime": 0,
            "srcChainId": 0,
        },
    )
    check_values(
        event,
        "bool",
        {"isLong": target.side == "long", "shouldUnwrapNativeToken": False, "autoCancel": False},
    )
    if event.array("address", "swapPath") != ():
        raise DataError("receipt", "GMX position request unexpectedly includes a swap route")


def check_values(event, type_, expected):
    if any(event.value(type_, field) != value for field, value in expected.items()):
        raise DataError("receipt", f"GMX event {event.name} differs from admitted {type_} fields")


def token_movement(receipt, target, asset):
    delta = 0
    for log in events(receipt, asset.reference, TRANSFER):
        if len(log["topics"]) != 3:
            raise DataError("receipt", "GMX token transfer has an unexpected topic layout")
        sender, receiver = (topic_address(topic) for topic in log["topics"][1:])
        if target.fund.address not in (sender, receiver):
            continue
        delta += fund_transfer(log, target, sender, receiver)
    return delta


def fund_transfer(log, target, sender, receiver):
    counterparty = receiver if sender == target.fund.address else sender
    counterparties: tuple[str, ...] = (
        target.market.contracts.order_vault,
        target.market.share_asset.reference,
    )
    from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget

    if isinstance(target, ExchangeTarget):
        counterparties += (
            target.market.contracts.deposit_vault,
            target.market.contracts.withdrawal_vault,
        )
    if counterparty not in counterparties:
        raise DataError(
            "receipt", "GMX fund transfer identifies an unexpected protocol counterparty"
        )
    (amount,) = data(log, ("uint256",))
    return amount if receiver == target.fund.address else -amount


def execution_fee(receipt, target, remaining, *, handler=None):
    found = [
        e
        for e in protocol_events(receipt, target)
        if e.name in ("KeeperExecutionFee", "ExecutionFeeRefund")
    ]
    if not found and remaining == 0:
        return ExecutionFee(paid=0, refunded=0, fund_received=0)
    paid = one_protocol_event(
        receipt, target, "KeeperExecutionFee", handler or target.market.contracts.order_handler
    )
    paid_amount = paid.value("uint256", "executionFeeAmount")
    if paid_amount == remaining and len(found) == 1:
        native = paid_amount if paid.value("address", "keeper") == target.fund.address else 0
        return ExecutionFee(paid=paid_amount, refunded=0, fund_received=native)
    refund = one_protocol_event(
        receipt, target, "ExecutionFeeRefund", handler or target.market.contracts.order_handler
    )
    amounts = (
        paid_amount,
        refund.value("uint256", "refundFeeAmount"),
    )
    if sum(amounts) != remaining or refund.value("address", "receiver") != target.fund.address:
        raise DataError(
            "receipt", "GMX execution fee and refund do not reconcile with retained escrow"
        )
    native = amounts[1] + (
        amounts[0] if paid.value("address", "keeper") == target.fund.address else 0
    )
    return ExecutionFee(paid=amounts[0], refunded=amounts[1], fund_received=native)


def remaining_fee(request, target):
    remaining = request.execution_fee.amount
    for evidence in request.events[1:]:
        fee = execution_fee(
            evidence.receipt, target, remaining, handler=request.events[0].identity.handler
        )
        remaining -= fee.paid + fee.refunded
    return remaining
