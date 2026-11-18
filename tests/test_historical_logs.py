"""Historical log authorization, canonical ordering, capture and offline reuse."""

import pytest

from almanak.backtest.evm import EvmHistory
from almanak.backtest.history import (
    Capture,
    HistoricalGateway,
    ReadEvidence,
    ReadRequest,
    validate_read,
)
from almanak.backtest.history_logs import MAX_LOG_BLOCKS, LogScope, read_log_windows, read_logs
from almanak.gateway.ethereum import DataError
from almanak.models import Block
from tests.test_backtest_history import CHAIN, TARGET

TOPIC = "0x" + "44" * 32
SCOPE = LogScope(address=TARGET, topics=(TOPIC,), first_block=10, last_block=3_000)


def query(**changes):
    return {
        "fromBlock": "0xa",
        "toBlock": "0xf",
        "address": TARGET,
        "topics": [[TOPIC]],
    } | changes


def request(value=None):
    return ReadRequest(chain=CHAIN, method="eth_getLogs", params=(value or query(),))


def block(number):
    return Block(
        chain=CHAIN, number=number, hash="0x" + f"{number:064x}", timestamp=100 + number * 12
    )


def event(number=11, index=0, **changes):
    return {
        "blockNumber": hex(number),
        "blockHash": block(number).hash,
        "transactionHash": "0x" + "22" * 32,
        "transactionIndex": "0x0",
        "logIndex": hex(index),
        "removed": False,
        "address": TARGET,
        "topics": [TOPIC],
        "data": "0x",
    } | changes


@pytest.mark.parametrize(
    "changes",
    [
        {"fromBlock": "latest"},
        {"fromBlock": "0x0a"},
        {"fromBlock": "0x10"},
        {"toBlock": hex(10 + MAX_LOG_BLOCKS)},
        {"address": "0x" + "99" * 20},
        {"address": [TARGET]},
        {"topics": []},
        {"topics": [None]},
        {"topics": [[]]},
        {"topics": ["0x1234"]},
        {"topics": [["0x" + "99" * 32]]},
        {"topics": [TOPIC, "0x1234"]},
        {"blockHash": block(10).hash},
        {"fromBlock": "0x9"},
        {"fromBlock": "0xbb8", "toBlock": "0xbb9"},
    ],
)
def test_unbounded_unscoped_and_malformed_log_queries_are_refused(changes):
    with pytest.raises(ValueError):
        validate_read(request(query(**changes)), (TARGET,), (SCOPE,))


def test_logs_require_separate_explicit_gateway_authority():
    with pytest.raises(ValueError, match="outside"):
        validate_read(request(), (TARGET,))
    validate_read(request(), (), (SCOPE,))
    validate_read(request(query(topics=[TOPIC, None, "0x" + "11" * 32])), (), (SCOPE,))


class Reader:
    def __init__(self, logs):
        self.logs = logs
        self.calls = []

    def pin_block(self, number):
        return block(100 if number == "finalized" else number)

    def _rpc(self, method, params):
        self.calls.append((method, params))
        return self.logs


def test_read_logs_pins_headers_and_keeps_native_order():
    reader = Reader([event(), event(index=1), event(12)])
    history = EvmHistory(reader)
    result = read_logs(history, SCOPE, 10, 15)
    assert [item.order for item in result] == [(11, 0, 0), (11, 0, 1), (12, 0, 0)]
    assert result[0].block == block(11)
    assert {10, 11, 12, 15} <= set(history.headers)
    assert len(reader.calls) == 1
    with pytest.raises(ValueError, match="finalized"):
        read_logs(history, SCOPE, 10, 101)
    assert len(reader.calls) == 1


@pytest.mark.parametrize(
    "events",
    [
        [event(16)],
        [event(blockHash="0x" + "99" * 32)],
        [event(removed=True)],
        [event(address="0x" + "99" * 20)],
        [event(topics=["0x" + "99" * 32])],
        [event(data="0xa")],
        [event(), event()],
        [event(12), event()],
        [event(index=1), event(index=0, transactionIndex="0x1")],
    ],
)
def test_rejects_changed_removed_duplicate_or_out_of_order_log_evidence(events):
    with pytest.raises(ValueError):
        read_logs(EvmHistory(Reader(events)), SCOPE, 10, 15)


def test_empty_logs_retain_both_range_boundaries():
    history = EvmHistory(Reader([]))
    assert read_logs(history, SCOPE, 10, 15) == ()
    assert {10, 15} <= set(history.headers)


def test_log_evidence_survives_provider_replacement_without_network_calls(tmp_path):
    capture = Capture(tmp_path / "observations.sqlite")
    reader = Reader([event()])
    gateway = HistoricalGateway(capture, {CHAIN: reader}, {CHAIN: ()}, {CHAIN: (SCOPE,)})
    result = gateway.dispatch("read", request())
    assert capture.activity()["attempts"] == 1
    assert gateway.dispatch("read", request()) == result
    offline = HistoricalGateway(Capture(capture.path), {}, {CHAIN: ()}, {CHAIN: (SCOPE,)})
    assert offline.dispatch("read", request()) == result
    assert len(reader.calls) == 1 and capture.activity()["attempts"] == 1
    with pytest.raises(DataError, match="missing"):
        offline.dispatch("read", request(query(toBlock="0x10")))


def test_provider_failures_and_interrupted_attempts_are_measured(tmp_path):
    capture = Capture(tmp_path / "observations.sqlite")

    class Failing(Reader):
        def _rpc(self, method, params):
            raise DataError("transport", "sanitized transport failure")

    gateway = HistoricalGateway(capture, {CHAIN: Failing([])}, {CHAIN: ()}, {CHAIN: (SCOPE,)})
    for _ in range(2):
        with pytest.raises(DataError):
            gateway.dispatch("read", request())
    capture.begin_attempt(request())
    activity = Capture(capture.path).activity()
    assert activity["attempts"] == 3 and activity["retries"] == 2
    assert activity["unknown_outcomes"] == 1
    assert capture.count() == 0


class WindowReader:
    """Serves headers and one event per log window, alone or ahead in batches."""

    chain = CHAIN

    def __init__(self, batches=True, malformed=False):
        self.cache = {}
        self.alone = []
        self.batches = []
        self.malformed = malformed
        if batches:
            self.prefetch = self.read_ahead

    def answer(self, method, params):
        if method == "eth_getBlockByNumber":
            return params[0]
        low = int(params[0]["fromBlock"], 16)
        return "0x" if self.malformed else [event(low + 1)]

    def read_ahead(self, requests):
        self.batches.append([r.method for r in requests])
        for r in requests:
            self.cache[r.key] = ReadEvidence(
                request=r, result=self.answer(r.method, list(r.params)), retrieved_at=1.0
            )

    def pin_block(self, number):
        if number != "finalized":
            key = ReadRequest(
                chain=CHAIN, method="eth_getBlockByNumber", params=(hex(number), False)
            ).key
            if key not in self.cache:
                self.alone.append(("eth_getBlockByNumber", number))
        return block(10_000 if number == "finalized" else number)

    def _rpc(self, method, params):
        key = ReadRequest(chain=CHAIN, method=method, params=tuple(params)).key
        if key in self.cache:
            return self.cache[key].result
        self.alone.append((method, params[0]["fromBlock"]))
        return self.answer(method, params)


def test_log_windows_are_read_ahead_with_their_boundary_and_event_headers(monkeypatch):
    monkeypatch.setattr("almanak.backtest.history_logs.LOG_WINDOWS_AHEAD", 2)
    scope = LogScope(address=TARGET, topics=(TOPIC,), first_block=10, last_block=9_000)
    alone, ahead = WindowReader(batches=False), WindowReader()
    expected = list(read_log_windows(EvmHistory(alone), scope, 10, 5_000))
    assert list(read_log_windows(EvmHistory(ahead), scope, 10, 5_000)) == expected
    assert [[log.block.number for log in window] for window in expected] == [[11], [2011], [4011]]
    # Two windows per group: their filters, their four boundary headers, then the
    # headers of the blocks that hold events. No read is left to be made alone.
    assert ahead.batches == [
        ["eth_getLogs"] * 2,
        ["eth_getBlockByNumber"] * 4,
        ["eth_getBlockByNumber"] * 2,
        ["eth_getLogs"],
        ["eth_getBlockByNumber"] * 2,
        ["eth_getBlockByNumber"],
    ]
    assert ahead.alone == [] and len(alone.alone) == 12


def test_a_malformed_log_answer_read_ahead_is_refused_by_its_window():
    scope = LogScope(address=TARGET, topics=(TOPIC,), first_block=10, last_block=9_000)
    with pytest.raises(ValueError, match="must be an array"):
        list(read_log_windows(EvmHistory(WindowReader(malformed=True)), scope, 10, 100))
