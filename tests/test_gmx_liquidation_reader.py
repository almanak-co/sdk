"""External-event ordering preserves admitted work and rejects ambiguous ownership."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import liquidation_reader as reader
from almanak.connectors.gmx_v2.receipts import protocol_events
from almanak.gateway.ethereum import DataError
from tests import test_gmx_liquidation_receipts

capture = test_gmx_liquidation_receipts.capture


def event(captured, target):
    return next(
        e for e in protocol_events(captured["receipt"], target) if e.name == "PositionDecrease"
    )


def state(**changes):
    return SimpleNamespace(outcomes=(), requests=(), pending=None, **changes)


def replace_value(event, kind, key, value):
    from almanak.connectors.gmx_v2.events import VALUE_TYPES

    values = list(event.values)
    scalars, arrays = values[VALUE_TYPES.index(kind)]
    values[VALUE_TYPES.index(kind)] = (
        tuple((k, value if k == key else v) for k, v in scalars),
        arrays,
    )
    return event.model_copy(update={"values": tuple(values)})


def test_liquidation_does_not_need_an_admitted_request(capture):
    before, captured = capture
    selected = event(captured, before.target)
    assert reader.selected_liquidation(state(), before.target, (selected,)) == (selected, False)


def test_other_position_cannot_be_adopted(capture):
    before, captured = capture
    selected = replace_value(
        event(captured, before.target), "bytes32", "positionKey", "0x" + "12" * 32
    )
    assert reader.selected_liquidation(state(), before.target, (selected,)) == (None, False)


def test_accepted_request_execution_reconciles_before_external_scan(capture):
    before, captured = capture
    selected = replace_value(event(captured, before.target), "uint256", "orderType", 4)
    current = state()
    current.requests = (
        SimpleNamespace(
            unresolved=True, key=selected.value("bytes32", "orderKey"), fund=before.target.fund
        ),
    )
    assert reader.selected_liquidation(current, before.target, (selected,)) == (None, True)


def test_unowned_normal_change_fails_closed(capture):
    before, captured = capture
    selected = replace_value(event(captured, before.target), "uint256", "orderType", 4)
    with pytest.raises(DataError, match="without retained"):
        reader.selected_liquidation(state(), before.target, (selected,))


def test_already_recorded_request_effect_is_not_replayed(capture):
    before, captured = capture
    selected = replace_value(event(captured, before.target), "uint256", "orderType", 4)
    current = state()
    current.outcomes = (SimpleNamespace(tx_hash=selected.transaction_hash, after=before),)
    assert reader.selected_liquidation(current, before.target, (selected,)) == (None, False)


def test_same_block_liquidation_and_other_change_is_ambiguous(capture):
    before, captured = capture
    selected = event(captured, before.target)
    another = selected.model_copy(update={"log_index": selected.log_index + 1})
    with pytest.raises(DataError, match="shares a block"):
        reader.selected_liquidation(state(), before.target, (selected, another))


@pytest.mark.parametrize("offset", [0, 1])
def test_liquidation_before_or_with_pending_receipt_retains_submission(capture, offset):
    before, captured = capture
    selected = event(captured, before.target)
    current = state()
    current.pending = SimpleNamespace(tx_hash="retained")
    block = before.block.model_copy(update={"number": selected.block_number + offset})
    provider = SimpleNamespace(
        _rpc=lambda *args, **kwargs: {"blockNumber": hex(block.number), "blockHash": block.hash},
        pin_block=lambda _: block,
    )
    with pytest.raises(DataError, match="precedes or shares"):
        reader.pending_precedes(provider, current, selected)
    assert current.pending.tx_hash == "retained"


def test_pending_earlier_receipt_is_reconciled_first(capture):
    before, captured = capture
    selected = event(captured, before.target)
    current = state()
    current.pending = SimpleNamespace(tx_hash="retained")
    block = before.block.model_copy(update={"number": selected.block_number - 1})
    provider = SimpleNamespace(
        _rpc=lambda *args, **kwargs: {"blockNumber": hex(block.number), "blockHash": block.hash},
        pin_block=lambda _: block,
    )
    assert reader.pending_precedes(provider, current, selected)


def test_unknown_pending_outcome_cannot_be_ordered(capture):
    before, captured = capture
    current = state()
    current.pending = SimpleNamespace(tx_hash="retained")
    with pytest.raises(DataError, match="unresolved local"):
        reader.pending_precedes(
            SimpleNamespace(_rpc=lambda *args, **kwargs: None),
            current,
            event(captured, before.target),
        )
