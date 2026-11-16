"""Read canonical captured GMX receipts without treating request disappearance as execution."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.caip import AccountReference, AssetType, ChainId
from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.request_reader import next_event
from almanak.connectors.gmx_v2.requests import ReceiptIdentity, Request, RequestEvidence
from almanak.gateway.ethereum import DataError
from almanak.models import Account, AssetAmount, Block, Chain


class CapturedReader:
    def __init__(self):
        path = Path(__file__).parents[1] / (
            "docs/internal/reports/evidence/2026-09-08-gmx-request-fixtures/"
            "arbitrum-eoa-open-close-cancel.json.gz"
        )
        self.proof = json.loads(gzip.decompress(path.read_bytes()))
        self.chain = Chain(
            chain_id=ChainId("eip155:42161"),
            environment="anvil:" + self.proof["metadata"]["instanceId"],
        )
        self.receipts = {
            a["receipt"]["transactionHash"]: a["receipt"] for a in self.proof["actions"]
        }
        self.blocks = {int(r["blockNumber"], 16): self.block(r) for r in self.receipts.values()}
        self.logs = [log for r in self.receipts.values() for log in r["logs"]]
        self.queries = []

    def block(self, receipt):
        return Block(
            chain=self.chain,
            number=int(receipt["blockNumber"], 16),
            hash=receipt["blockHash"],
            timestamp=receipt["blockTimestamp"],
        )

    def pin_block(self, number):
        return self.blocks[number]

    def _rpc(self, method, params, *, allow_none=False):
        if method == "eth_getTransactionReceipt":
            return self.receipts.get(params[0])
        assert method == "eth_getLogs"
        query = params[0]
        self.queries.append(query)
        return [log for log in self.logs if matches(log, query)]


def matches(log, query):
    if log["address"] != query["address"]:
        return False
    if not int(query["fromBlock"], 16) <= int(log["blockNumber"], 16) <= int(query["toBlock"], 16):
        return False
    return all(
        topic in allowed if isinstance(allowed, list) else topic == allowed
        for topic, allowed in zip(log["topics"], query["topics"], strict=False)
    )


def accepted_request(reader):
    event = next(
        e
        for log in reader.logs
        if (e := decode_event(log, emitter="0xc8ee91a54287db53897056e12d9819156d3822fb"))
        and e.name == "OrderCreated"
    )
    receipt = reader.receipts[event.transaction_hash]
    asset = AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1")
    return Request(
        fund=Account(
            chain=reader.chain, address=AccountReference(event.value("address", "account"))
        ),
        allocation="Main",
        decision_identity="f" * 32,
        kind="order",
        key=event.topics[0],
        market=AssetType("eip155:42161/erc20:" + event.value("address", "market")),
        escrow=(AssetAmount(asset=asset, amount=10**17),),
        execution_fee=AssetAmount(asset=asset, amount=2 * 10**16),
        events=(
            RequestEvidence(
                status="accepted",
                key=event.topics[0],
                receipt=receipt,
                identity=ReceiptIdentity(
                    block=reader.block(receipt),
                    transaction_hash=event.transaction_hash,
                    log_index=event.log_index,
                    emitter=event.emitter,
                    handler=event.sender,
                ),
            ),
        ),
    )


def test_real_two_topic_keeper_event_reconciles_once_with_exact_request_query():
    reader = CapturedReader()
    request = accepted_request(reader)
    head = reader.blocks[max(reader.blocks)]
    event = next_event(reader, request, head, maximum_block_span=2)
    assert event.status == "executed"
    assert len(event.event().topics) == 2
    assert all(q["topics"][2] == request.key for q in reader.queries)
    settled = request.append(event)
    assert next_event(reader, settled, head, maximum_block_span=2) is None


def test_missing_terminal_evidence_remains_unresolved():
    reader = CapturedReader()
    request = accepted_request(reader)
    reader.logs = []
    assert (
        next_event(reader, request, reader.blocks[max(reader.blocks)], maximum_block_span=2) is None
    )
    assert request.unresolved


def test_reorged_acceptance_and_unavailable_receipt_refuse_reconciliation():
    reader = CapturedReader()
    request = accepted_request(reader)
    head = reader.blocks[max(reader.blocks)]
    identity = request.events[0].identity
    reader.receipts.pop(identity.transaction_hash)
    with pytest.raises(DataError, match="absent from canonical history") as missing:
        next_event(reader, request, head, maximum_block_span=2)
    assert missing.value.kind == "reorganization"
    reader.blocks[identity.block.number] = identity.block.model_copy(
        update={"hash": "0x" + "11" * 32}
    )
    with pytest.raises(DataError, match="no longer canonical") as replaced:
        next_event(reader, request, head, maximum_block_span=2)

    assert replaced.value.kind == "reorganization"
