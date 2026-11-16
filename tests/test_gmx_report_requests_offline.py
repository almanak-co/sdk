"""Keeper report sources and request event pages refuse noncanonical evidence offline.

Logs, receipts and blocks are invented stand-ins; the retained fork captures exercise
the same paths against real keeper publications.
"""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import report_source, request_reader
from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.fresh_reports import FreshReports
from almanak.connectors.gmx_v2.report_source import CalldataSuffix, SourceSelection
from almanak.gateway.ethereum import DataError
from tests import gmx_synthetic_frames as frames
from tests.gmx_event_logs import event_log, word

MARKET = frames.perpetual_target().market
EMITTER = MARKET.contracts.event_emitter
ORACLE = "0x" + "5a" * 20
SELECTION = SourceSelection(
    handler="0x" + "5b" * 20,
    oracle=ORACLE,
    kind="order",
    calldata_suffix=CalldataSuffix(prefix="0x", length=0),
)
TRANSACTION = "0x" + "cd" * 32


def block(number, tag="ab"):
    return frames.block(MARKET, number=number).model_copy(update={"hash": "0x" + tag * 32})


class Reader:
    def __init__(self, responses=None, replaced=()):
        self.chain = MARKET.chain
        self.responses, self.replaced = responses or {}, set(replaced)
        self.calls = []

    def pin_block(self, number):
        return block(number, "ee" if number in self.replaced else "ab")

    def _rpc(self, method, args, allow_none=False):
        self.calls.append((method, args))
        value = self.responses[method]
        return value(args) if callable(value) else value


def update(number=11, sender=ORACLE, name="OraclePriceUpdate", transaction=TRANSACTION, **kw):
    return event_log(
        name,
        emitter=EMITTER,
        sender=sender,
        topics=(word(MARKET.index_asset.reference),),
        transaction=transaction,
        block_hash=block(number).hash,
        number=number,
        **kw,
    )


# Canonical keeper receipts


def mined(number=11, **changes):
    return {
        "hash": TRANSACTION,
        "blockNumber": hex(number),
        "blockHash": block(number).hash,
    } | changes


def receipt(number=11, **changes):
    return {
        "transactionHash": TRANSACTION,
        "blockHash": block(number).hash,
        "blockNumber": hex(number),
        "status": "0x1",
        "logs": [],
    } | changes


def test_canonical_receipt_returns_a_successful_later_publication():
    reader = Reader({"eth_getTransactionReceipt": receipt()})
    assert report_source.canonical_receipt(reader, mined(), block(10)) == (block(11), receipt())


def test_canonical_receipt_refuses_unmined_reorganized_or_early_publications():
    with pytest.raises(DataError, match="no mined publication"):
        report_source.canonical_receipt(Reader(), mined(blockNumber=None), block(10))
    reorganized = Reader({"eth_getTransactionReceipt": receipt()}, replaced={11})
    with pytest.raises(DataError, match="no longer canonical"):
        report_source.canonical_receipt(reorganized, mined(), block(10))
    stale = Reader({"eth_getTransactionReceipt": receipt()})
    with pytest.raises(DataError, match="no longer canonical"):
        report_source.canonical_receipt(stale, mined(blockHash="0x" + "01" * 32), block(10))
    with pytest.raises(DataError, match="strictly after the fresh origin"):
        report_source.canonical_receipt(stale, mined(), block(11))
    failed = Reader({"eth_getTransactionReceipt": receipt(status="0x0")})
    with pytest.raises(DataError, match="strictly after the fresh origin"):
        report_source.canonical_receipt(failed, mined(), block(10))


def test_source_events_keep_the_selected_oracle_updates_of_one_receipt():
    logs = [update(), update(sender="0x" + "01" * 20), update(name="OrderExecuted")]
    events = report_source.source_events(receipt(logs=logs), MARKET, SELECTION)
    assert [(e.name, e.sender) for e in events] == [("OraclePriceUpdate", ORACLE)]
    with pytest.raises(DataError, match="canonical receipt"):
        report_source.source_events(receipt(logs=[update(number=12)]), MARKET, SELECTION)


# Fresh report source pages


def fresh(reader):
    value = FreshReports.__new__(FreshReports)
    value.reader, value.selection = reader, SELECTION
    value.seen, value.candidates = set(), []
    return value


def test_admit_event_queues_each_selected_oracle_transaction_once():
    source = fresh(Reader())
    event = decode_event(update(), emitter=EMITTER)
    source.admit_event(event, 10, 12)
    source.admit_event(event, 10, 12)
    source.admit_event(None, 10, 12)
    source.admit_event(decode_event(update(sender="0x" + "01" * 20), emitter=EMITTER), 10, 12)
    assert source.candidates == [TRANSACTION]


def test_admit_event_refuses_out_of_page_renamed_or_reorganized_logs():
    event = decode_event(update(), emitter=EMITTER)
    with pytest.raises(DataError, match="outside the requested page"):
        fresh(Reader()).admit_event(event, 12, 14)
    renamed = event.model_copy(update={"name": "OrderExecuted"})
    with pytest.raises(DataError, match="outside the requested page"):
        fresh(Reader()).admit_event(renamed, 10, 12)
    with pytest.raises(DataError, match="no longer canonical"):
        fresh(Reader(replaced={11})).admit_event(event, 10, 12)


# Request event pages

KEY = "0x" + "4b" * 32
EXECUTED = "0x" + request_reader.keccak(text="OrderExecuted").hex()


def request(last=10, index=1, unresolved=True):
    identity = SimpleNamespace(block=block(last), log_index=index, emitter=EMITTER)
    return SimpleNamespace(
        fund=SimpleNamespace(chain=MARKET.chain),
        events=(SimpleNamespace(identity=identity),),
        unresolved=unresolved,
        kind="order",
        key=KEY,
    )


def log(number, index):
    return {"blockNumber": hex(number), "logIndex": hex(index), "topics": [None, EXECUTED, KEY]}


@pytest.fixture
def pages(monkeypatch):
    audited, delivered = [], []
    monkeypatch.setattr(
        request_reader, "audit_request", lambda reader, value: audited.append(value)
    )
    monkeypatch.setattr(
        request_reader,
        "evidence_from_log",
        lambda reader, value, item, statuses: (
            delivered.append((item, statuses[item["topics"][1]])) or item
        ),
    )
    return SimpleNamespace(audited=audited, delivered=delivered)


def paged(logs):
    def read(args):
        query = args[0]
        start, end = int(query["fromBlock"], 16), int(query["toBlock"], 16)
        return [item for item in logs if start <= int(item["blockNumber"], 16) <= end]

    return Reader({"eth_getLogs": read})


def test_next_event_returns_the_first_later_event_across_pages(pages):
    reader = paged([log(10, 1), log(10, 0), log(15, 2), log(14, 3)])
    found = request_reader.next_event(reader, request(), block(20), maximum_block_span=3)
    assert found == log(14, 3)
    assert pages.delivered == [(log(14, 3), "executed")]
    assert [int(args[0]["fromBlock"], 16) for _, args in reader.calls] == [10, 13]
    query = reader.calls[0][1][0]
    assert query["address"] == EMITTER and query["topics"][2] == KEY


def test_next_event_returns_none_without_later_events_or_open_requests(pages):
    reader = paged([log(10, 1)])
    assert request_reader.next_event(reader, request(), block(20), maximum_block_span=5) is None
    assert len(reader.calls) == 3
    closed = request(unresolved=False)
    assert request_reader.next_event(Reader(), closed, block(20), maximum_block_span=5) is None
    assert pages.audited[-1] is closed


@pytest.mark.parametrize("span", [0, -1, True, 2.0])
def test_next_event_requires_an_explicit_positive_span(pages, span):
    with pytest.raises(ValueError, match="explicit positive block span"):
        request_reader.next_event(Reader(), request(), block(20), maximum_block_span=span)


def test_next_event_refuses_another_scope_or_noncanonical_head(pages):
    with pytest.raises(ValueError, match="retained execution scope"):
        request_reader.next_event(Reader(), request(), block(9), maximum_block_span=5)
    avalanche = frames.perpetual_target("avalanche").market
    elsewhere = frames.block(avalanche, number=20)
    with pytest.raises(ValueError, match="retained execution scope"):
        request_reader.next_event(Reader(), request(), elsewhere, maximum_block_span=5)
    with pytest.raises(DataError, match="no longer canonical"):
        request_reader.next_event(Reader(replaced={20}), request(), block(20), maximum_block_span=5)
    assert pages.audited == []
