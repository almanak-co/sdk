"""Liquidation receipts identify an unsolicited closure and its exact fund transfers."""

from almanak.connectors.gmx_v2 import funding
from almanak.connectors.gmx_v2.deployments import records
from almanak.connectors.gmx_v2.position_reader import position_key
from almanak.connectors.gmx_v2.receipts import check_values, one_protocol_event, protocol_events
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS


def handler(target):
    matches = [
        address
        for address, record in records(target.market).items()
        if record["contract"] == "LiquidationHandler"
    ]
    if len(matches) != 1:
        raise DataError("revision", "GMX requires one verified LiquidationHandler deployment")
    return matches[0]


def validate_events(receipt, target, before):
    liquidator = handler(target)
    created = one_protocol_event(receipt, target, "OrderCreated", liquidator)
    executed = one_protocol_event(receipt, target, "OrderExecuted", liquidator)
    decreased = one_protocol_event(
        receipt, target, "PositionDecrease", target.market.contracts.decrease_order_executor
    )
    key = decreased.value("bytes32", "orderKey")
    account_topic = "0x" + target.fund.address[2:].rjust(64, "0")
    if created.topics != (key, account_topic) or executed.topics != created.topics:
        raise DataError("receipt", "GMX liquidation order events differ from fund and order key")
    if (
        decreased.topics != (account_topic,)
        or not created.log_index < decreased.log_index < executed.log_index
    ):
        raise DataError("receipt", "GMX liquidation events have ambiguous execution order")
    check_values(created, "bytes32", {"key": key})
    check_values(executed, "bytes32", {"key": key})
    check_values(executed, "address", {"account": target.fund.address})
    check_values(
        created,
        "address",
        {
            "account": target.fund.address,
            "receiver": target.fund.address,
            "cancellationReceiver": target.fund.address,
            "callbackContract": ZERO_ADDRESS,
            "uiFeeReceiver": ZERO_ADDRESS,
            "market": target.market.share_asset.reference,
            "initialCollateralToken": target.collateral_asset.reference,
        },
    )
    check_values(
        created,
        "uint256",
        {
            "orderType": 7,
            "executionFee": 0,
            "initialCollateralDeltaAmount": 0,
            "sizeDeltaUsd": before.owned.position.size_usd_e30,
            "decreasePositionSwapType": 1,
        },
    )
    check_values(
        created, "bool", {"isLong": target.side == "long", "shouldUnwrapNativeToken": True}
    )
    if created.array("address", "swapPath"):
        raise DataError("receipt", "GMX liquidation unexpectedly used an external swap path")
    check_values(
        decreased,
        "address",
        {
            "account": target.fund.address,
            "market": target.market.share_asset.reference,
            "collateralToken": target.collateral_asset.reference,
        },
    )
    check_values(decreased, "bytes32", {"positionKey": "0x" + position_key(target).hex()})
    check_values(decreased, "bool", {"isLong": target.side == "long"})
    check_values(
        decreased,
        "uint256",
        {
            "orderType": 7,
            "sizeInUsd": 0,
            "sizeInTokens": 0,
            "collateralAmount": 0,
            "sizeDeltaUsd": before.owned.position.size_usd_e30,
            "sizeDeltaInTokens": before.owned.position.index_amount.amount,
        },
    )
    return created, executed, decreased


def native_received(trace, receipt, target):
    if (
        receipt.get("status") != "0x1"
        or trace.get("error")
        or trace.get("type") != "CALL"
        or not isinstance(trace.get("calls"), list)
        or not trace["calls"]
        or trace.get("to", "").lower() != handler(target)
        or trace.get("from", "").lower() != receipt.get("from", "").lower()
        or receipt.get("to", "").lower() != handler(target)
    ):
        raise DataError("trace", "GMX liquidation requires its successful handler call trace")
    return native_calls(trace, target)


def native_calls(frame, target):
    # Reverted subtrees contribute no transfers even if a nested CALL itself succeeded.
    if frame.get("error"):
        return 0
    amount = int(frame.get("value", "0x0"), 16)
    sender = frame.get("from", "").lower()
    receiver = frame.get("to", "").lower()
    fund = target.fund.address
    received = 0
    if amount and frame.get("type") in ("CALL", "CREATE", "CREATE2", "SELFDESTRUCT"):
        if sender == fund:
            raise DataError("trace", "GMX liquidation unexpectedly debited fund native inventory")
        if receiver == fund:
            if frame.get("type") != "CALL" or sender != target.market.share_asset.reference:
                raise DataError(
                    "trace", "GMX liquidation fund transfer lacks its market counterparty"
                )
            received = amount
    return received + sum(native_calls(child, target) for child in frame.get("calls", ()))


def fee_events(receipt, target, key):
    events = tuple(e for e in protocol_events(receipt, target) if e.name == "PositionFeesCollected")
    if len(events) != 1:
        raise DataError("receipt", "GMX liquidation requires one position fee accounting event")
    event = events[0]
    if event.sender != target.market.contracts.decrease_order_executor:
        raise DataError("receipt", "GMX liquidation fees came from an unverified executor")
    check_values(
        event, "bytes32", {"orderKey": key, "positionKey": "0x" + position_key(target).hex()}
    )
    check_values(
        event,
        "address",
        {
            "trader": target.fund.address,
            "market": target.market.share_asset.reference,
            "collateralToken": target.collateral_asset.reference,
        },
    )
    check_values(event, "bool", {"isIncrease": False})
    return events


def accrue_funding(before, after, receipt):
    target = before.target
    owned = {value.asset: value.amount for value in funding.owned_amounts(before)}
    observed = {value.asset: value.amount for value in before.claimable_funding}
    for event in funding.account_events(receipt, target, "ClaimableFundingUpdated"):
        asset = funding.validate_target(
            event, target, target.market.contracts.decrease_order_executor
        )
        delta = event.value("uint256", "delta")
        owned[asset] += delta
        observed[asset] += delta
        if observed[asset] != event.value("uint256", "nextValue"):
            raise DataError(
                "receipt", "GMX liquidation funding differs from prior financial evidence"
            )
    return funding.retained_claims(owned), funding.observed_issues(observed, after)
