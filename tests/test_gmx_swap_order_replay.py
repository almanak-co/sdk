"""Recorded swap creation constrains pending-order cancellation identity and behavior."""

import pytest

from almanak.connectors.gmx_v2.order_reader import validate_exchange_order
from almanak.gateway.ethereum import DataError
from tests.gmx_native_runtime import current_exchange_completed as current_exchange_completed


@pytest.mark.parametrize("frozen", [False, True])
def test_swap_cancellation_checks_recorded_recipients_route_and_disabled_features(
    completed, frozen
):
    outcome = completed.outcomes[1]
    event = outcome.request.events[0].event()
    addresses = tuple(
        event.value("address", name)
        for name in (
            "account",
            "receiver",
            "cancellationReceiver",
            "callbackContract",
            "uiFeeReceiver",
            "market",
            "initialCollateralToken",
        )
    ) + (event.array("address", "swapPath"),)
    numbers = tuple(
        event.value("uint256", name)
        for name in (
            "orderType",
            "decreasePositionSwapType",
            "sizeDeltaUsd",
            "initialCollateralDeltaAmount",
            "triggerPrice",
            "acceptablePrice",
            "executionFee",
            "callbackGasLimit",
            "minOutputAmount",
            "uiFeeFactor",
            "updatedAtTime",
            "validFromTime",
            "srcChainId",
        )
    )
    flags = tuple(
        frozen if name == "isFrozen" else event.value("bool", name)
        for name in ("isLong", "shouldUnwrapNativeToken", "isFrozen", "autoCancel")
    )
    validate_exchange_order(outcome.operation.target, addresses, numbers, flags, ())
    foreign = ("0x" + "ab" * 20, *addresses[1:])
    with pytest.raises(DataError, match="owner, recipients or market route"):
        validate_exchange_order(outcome.operation.target, foreign, numbers, flags, ())
    for index in (0, 1, 2, 4, 5, 7, 9, 11, 12):
        changed = tuple(1 if i == index else value for i, value in enumerate(numbers))
        with pytest.raises(DataError, match="local exact-input market swap"):
            validate_exchange_order(outcome.operation.target, addresses, changed, flags, ())
    with pytest.raises(DataError, match="local exact-input market swap"):
        validate_exchange_order(
            outcome.operation.target, addresses, numbers, flags, ("0x" + "00" * 32,)
        )
