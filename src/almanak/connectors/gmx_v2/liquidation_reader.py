"""Bounded canonical position-event pages never infer liquidation from a missing balance."""

from eth_utils import keccak

from almanak.connectors.gmx_v2.events import EVENT_DATA, decode_event
from almanak.connectors.gmx_v2.liquidation_types import Scan, scope
from almanak.connectors.gmx_v2.position_reader import position_key
from almanak.connectors.identifiers import ConnectorId
from almanak.gateway.ethereum import DataError


def logs(reader, target, start, end):
    account = "0x" + target.fund.address[2:].rjust(64, "0")
    signatures = "0x" + keccak(text=f"EventLog1(address,string,string,bytes32,{EVENT_DATA})").hex()
    names = ["0x" + keccak(text=name).hex() for name in ("PositionIncrease", "PositionDecrease")]
    rows = reader._rpc(
        "eth_getLogs",
        [
            {
                "address": target.market.contracts.event_emitter,
                "fromBlock": hex(start.number),
                "toBlock": hex(end.number),
                "topics": [signatures, names, account],
            }
        ],
    )
    events = tuple(decode_event(row, emitter=target.market.contracts.event_emitter) for row in rows)
    for event in events:
        if (
            event is None
            or event.name not in ("PositionIncrease", "PositionDecrease")
            or event.topics != (account,)
            or not start.number <= event.block_number <= end.number
            or event.value("address", "account") != target.fund.address
        ):
            raise DataError("integrity", "GMX external scan returned an event outside its query")
    ordered = tuple(
        sorted((e for e in events if e is not None), key=lambda e: (e.block_number, e.log_index))
    )
    if len({(e.transaction_hash, e.log_index) for e in ordered}) != len(ordered):
        raise DataError("integrity", "GMX external scan returned duplicate event identities")
    return ordered


def financial_before(state, target):
    return next((o.after for o in reversed(state.outcomes) if o.after.target == target), None)


def anchor(state, target):
    # A later approval snapshot may already observe an unrecorded liquidation; only an
    # owned opening transition establishes this generation's external-event cursor.
    return next(
        (
            o.after.block
            for o in reversed(state.outcomes)
            if o.after.target == target
            and o.before.owned.position is None
            and o.after.owned.position is not None
        ),
        None,
    )


def bounds(reader, state, target, reference):
    opening = anchor(state, target)
    if opening is None:
        raise DataError("ownership", "GMX external scan lacks its owned position opening evidence")
    pages = [page for page in state.external_scans if page.target == target]
    first = max(opening.number, pages[-1].end.number + 1 if pages else 0)
    span = state.config.allocation(target.allocation).maximum_event_block_span
    if reference is not None:
        start, end = reference.start, reference.end
        if start.number != first or end.number - first >= span:
            raise DataError("integrity", "GMX external scan changed its retained bounded cursor")
    else:
        head = reader.pin_block("latest")
        if first > head.number:
            return None
        start, end = reader.pin_block(first), reader.pin_block(min(first + span - 1, head.number))
    if reader.pin_block(start.number) != start or reader.pin_block(end.number) != end:
        raise DataError("reorganization", "GMX external scan boundaries are no longer canonical")
    return start, end


def relevant(events, target):
    key = "0x" + position_key(target).hex()
    return tuple(e for e in events if e.value("bytes32", "positionKey") == key)


def selected_liquidation(state, target, events):
    recorded = {o.tx_hash for o in state.outcomes if o.after.target == target}
    selected = tuple(e for e in relevant(events, target) if e.transaction_hash not in recorded)
    if not selected:
        return None, False
    event = selected[0]
    if event.name != "PositionDecrease" or event.value("uint256", "orderType") != 7:
        key = event.value("bytes32", "orderKey")
        if any(r.unresolved and r.key == key and r.fund == target.fund for r in state.requests):
            return None, True
        raise DataError("ownership", "GMX position changed without retained SDK request authority")
    if any(other.block_number == event.block_number for other in selected[1:]):
        raise DataError("ordering", "GMX liquidation shares a block with another position change")
    return event, False


def pending_precedes(reader, state, event):
    if state.pending is None:
        return False
    receipt = reader._rpc("eth_getTransactionReceipt", [state.pending.tx_hash], allow_none=True)
    if receipt is None:
        raise DataError("ordering", "GMX liquidation cannot order an unresolved local submission")
    block = reader.pin_block(int(receipt["blockNumber"], 16))
    if block.hash != receipt["blockHash"]:
        raise DataError(
            "reorganization", "GMX local receipt preceding liquidation is not canonical"
        )
    if block.number >= event.block_number:
        raise DataError(
            "ordering", "GMX liquidation precedes or shares a block with local submission"
        )
    return True


def page(backend, state, target, reference=None):
    interval = bounds(backend.reader, state, target, reference)
    if interval is None:
        return None
    start, end = interval
    events = logs(backend.reader, target, start, end)
    event, wait = selected_liquidation(state, target, events)
    if liquidation_wait(backend.reader, state, event, wait):
        return None
    end, events, outcome = liquidation_outcome(backend, state, target, start, end, events, event)
    return Scan(
        connector=ConnectorId("gmx_v2"),
        scope=scope(target),
        target=target,
        start=start,
        end=end,
        events=events,
        outcome=outcome,
    )


def liquidation_wait(reader, state, event, wait):
    return wait or (event is not None and pending_precedes(reader, state, event))


def liquidation_outcome(backend, state, target, start, end, events, event):
    if event is None:
        return end, events, None
    from almanak.connectors.gmx_v2.liquidation_reconciliation import reconcile

    if event.block_number < end.number:
        end = backend.reader.pin_block(event.block_number)
        events = logs(backend.reader, target, start, end)
    return end, events, reconcile(backend, state, target, event, end)
