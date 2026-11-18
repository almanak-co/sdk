"""Swap settlement must correlate actual native output with the exact admitted request."""

import pytest

from almanak.connectors.gmx_v2 import exchange_receipts
from almanak.connectors.gmx_v2.events import VALUE_TYPES
from almanak.connectors.gmx_v2.receipts import one_protocol_event
from almanak.gateway.ethereum import DataError
from tests.gmx_native_runtime import current_exchange_completed as current_exchange_completed


def changed(event, kind, key, replacement):
    groups = list(event.values)
    index = VALUE_TYPES.index(kind)
    scalars, arrays = groups[index]
    groups[index] = (
        tuple((name, replacement if name == key else value) for name, value in scalars),
        arrays,
    )
    return event.model_copy(update={"values": tuple(groups)})


@pytest.mark.parametrize(
    "field", ["orderKey", "receiver", "market", "tokenIn", "tokenOut", "amountIn", "amountOut"]
)
def test_native_swap_event_rejects_mismatched_or_insufficient_output(monkeypatch, completed, field):
    outcome = completed.outcomes[2]
    target = outcome.operation.target
    event = one_protocol_event(
        outcome.receipt, target, "SwapInfo", target.market.contracts.swap_handler
    )
    assert exchange_receipts.executed_output(
        outcome.operation, outcome.request, outcome.receipt
    ) == event.value("uint256", "amountOut")
    kind = (
        "bytes32" if field == "orderKey" else "uint256" if field.startswith("amount") else "address"
    )
    replacement = 0 if kind == "uint256" else "0x" + "ab" * (32 if kind == "bytes32" else 20)
    forged = changed(event, kind, field, replacement)
    monkeypatch.setattr(exchange_receipts, "one_protocol_event", lambda *args: forged)
    with pytest.raises(DataError):
        exchange_receipts.executed_output(outcome.operation, outcome.request, outcome.receipt)


def test_native_swap_creation_must_match_admitted_minimum_and_recipients(completed):
    outcome = completed.outcomes[1]
    event = outcome.request.events[0].event()
    exchange_receipts.validate_created(event, outcome.operation)
    for kind, field, value in (
        ("uint256", "minOutputAmount", 0),
        ("address", "receiver", "0x" + "ab" * 20),
        ("bool", "shouldUnwrapNativeToken", True),
    ):
        with pytest.raises(DataError):
            exchange_receipts.validate_created(
                changed(event, kind, field, value), outcome.operation
            )
