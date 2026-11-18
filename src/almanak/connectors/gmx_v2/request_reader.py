"""Canonical request event reads. Disappearance alone never establishes settlement."""

from eth_utils import keccak

from almanak.connectors.gmx_v2.events import EVENT_DATA, decode_event
from almanak.connectors.gmx_v2.requests import ReceiptIdentity, Request, RequestEvidence
from almanak.gateway.ethereum import DataError


def lifecycle_topics(kind):
    prefix = {"order": "Order", "deposit": "Deposit", "withdrawal": "Withdrawal"}[kind]
    return {
        "0x" + keccak(text=prefix + suffix).hex(): status
        for suffix, status in (
            ("Frozen", "frozen"),
            ("Executed", "executed"),
            ("Cancelled", "cancelled"),
        )
    }


def canonical_receipt(reader, identity, *, committed=False):
    block = reader.pin_block(identity.block.number)
    if block != identity.block:
        raise DataError("reorganization", "retained GMX request event block is no longer canonical")
    receipt = reader._rpc("eth_getTransactionReceipt", [identity.transaction_hash], allow_none=True)
    if receipt is None:
        raise DataError(
            "reorganization" if committed else "unavailable",
            "GMX request receipt is absent from canonical history",
        )
    return receipt


def audit_request(reader, request: Request):
    for event in request.events:
        audit_event(reader, event)


def audit_event(reader, event):
    receipt = canonical_receipt(reader, event.identity, committed=True)
    try:
        refreshed = RequestEvidence(
            status=event.status, key=event.key, identity=event.identity, receipt=receipt
        )
        matching = refreshed.event() == event.event()
    except ValueError:
        matching = False
    if not matching:
        raise DataError(
            "reorganization", "retained GMX request event differs from canonical history"
        )


def evidence_from_log(reader, request, log, statuses):
    identity = request.events[0].identity
    event = decode_event(log, emitter=identity.emitter, sender=identity.handler)
    if event is None or not event.topics or event.topics[0] != request.key:
        raise DataError("integrity", "GMX request log query returned a different event identity")
    block = reader.pin_block(event.block_number)
    receipt_identity = ReceiptIdentity(
        block=block,
        transaction_hash=event.transaction_hash,
        log_index=event.log_index,
        emitter=event.emitter,
        handler=event.sender,
    )
    receipt = canonical_receipt(reader, receipt_identity)
    result = RequestEvidence(
        status=statuses[log["topics"][1]],
        key=request.key,
        identity=receipt_identity,
        receipt=receipt,
    )
    if result.event() != event:
        raise DataError("integrity", "GMX queried event differs from its canonical receipt")
    request.append(result)
    return result


def next_event(reader, request: Request, head, *, maximum_block_span: int):
    """Return one later event; runtime must durably commit it before requesting another."""
    if type(maximum_block_span) is not int or maximum_block_span <= 0:
        raise ValueError("GMX event collection requires an explicit positive block span")
    if head.chain != request.fund.chain or head.number < request.events[-1].identity.block.number:
        raise ValueError("GMX request reconciliation head differs from retained execution scope")
    if reader.pin_block(head.number) != head:
        raise DataError("integrity", "GMX request reconciliation head is no longer canonical")
    audit_request(reader, request)
    if not request.unresolved:
        return None
    statuses = lifecycle_topics(request.kind)
    last = request.events[-1].identity
    for start in range(last.block.number, head.number + 1, maximum_block_span):
        end = min(start + maximum_block_span - 1, head.number)
        logs = request_logs(reader, request, statuses, start, end)
        candidates = [log for log in logs if log_order(log) > (last.block.number, last.log_index)]
        if candidates:
            return evidence_from_log(reader, request, min(candidates, key=log_order), statuses)
    return None


def log_order(log):
    return int(log["blockNumber"], 16), int(log["logIndex"], 16)


def request_logs(reader, request, statuses, start, end):
    signatures = [
        "0x" + keccak(text=f"EventLog{count}(address,string,string,{keys},{EVENT_DATA})").hex()
        for count, keys in ((1, "bytes32"), (2, "bytes32,bytes32"))
    ]
    return reader._rpc(
        "eth_getLogs",
        [
            {
                "address": request.events[0].identity.emitter,
                "fromBlock": hex(start),
                "toBlock": hex(end),
                "topics": [signatures, list(statuses), request.key],
            }
        ],
    )
