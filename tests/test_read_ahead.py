"""Reading ahead warms later samples with request shapes only, in bounded groups."""

from types import SimpleNamespace

from almanak.backtest import read_ahead
from almanak.backtest.history import ReadRequest
from almanak.backtest.read_ahead import ReadAhead, block_reference, shape
from almanak.gateway.ethereum import DataError
from almanak.models import Block
from tests.test_backtest_history import CHAIN, TARGET


def block(number):
    return Block(chain=CHAIN, number=number, hash="0x" + f"{number:064x}", timestamp=number)


def call(data, at):
    return ReadRequest(
        chain=CHAIN,
        method="eth_call",
        params=({"to": TARGET, "data": data}, block_reference(at)),
    )


def reads(batch):
    """A warmed batch as (block number, calldata); shapes keep no order within a block."""
    return sorted((int(r.params[1]["blockHash"], 16), r.params[0]["data"]) for r in batch)


class Reader:
    def __init__(self):
        self.used: set[str] = set()
        self.cache: dict[str, SimpleNamespace] = {}
        self.batches: list[tuple[ReadRequest, ...]] = []

    def prefetch(self, requests):
        self.batches.append(tuple(requests))

    def use(self, *requests):
        """The reads one sample made; a sampler clears the previous sample's first."""
        self.used = {request.key for request in requests}
        self.cache.update({r.key: SimpleNamespace(request=r) for r in requests})


class History:
    def __init__(self, limit=None):
        self.reader = Reader()
        self.anchored: list[tuple[int, ...]] = []
        self.limit = limit

    def prefetch_floors(self, times):
        self.anchored.append(tuple(times))

    def floor(self, at):
        if self.limit is not None and at > self.limit:
            raise DataError("unavailable_history", "requested time exceeds pinned history")
        return block(at)


def test_anchors_are_resolved_in_groups_and_nothing_is_warmed_without_shapes(monkeypatch):
    monkeypatch.setattr(read_ahead, "ANCHORS_AHEAD", 4)
    history = History()
    ahead = ReadAhead(history, range(100, 110))
    for at in range(100, 110):
        ahead.before(at)
    assert history.anchored == [(100, 101, 102, 103), (104, 105, 106, 107), (108, 109)]
    assert history.reader.batches == []


def test_only_block_pinned_shapes_the_sample_used_are_carried_to_later_blocks(monkeypatch):
    monkeypatch.setattr(read_ahead, "ANCHORS_AHEAD", 8)
    monkeypatch.setattr(read_ahead, "READS_AHEAD", 6)
    history = History()
    reader = history.reader
    ahead = ReadAhead(history, range(100, 110))
    ahead.before(100)
    header = ReadRequest(chain=CHAIN, method="eth_getBlockByNumber", params=("0x64", False))
    stale = call("0xcc", block(99))
    reader.use(call("0xaa", block(100)), call("0xbb", block(100)), call("0xdd", block(100)))
    reader.cache.update({r.key: SimpleNamespace(request=r) for r in (header, stale)})
    reader.used |= {header.key}
    ahead.learn(block(100), keep=lambda request: request.params[0]["data"] != "0xdd")
    assert sorted(ahead.shapes) == sorted(
        shape(call(data, block(100))) for data in ("0xaa", "0xbb")
    )

    ahead.before(101)
    # Six reads at two shapes: three samples, each at its own block.
    assert reads(reader.batches[0]) == [
        (at, data) for at in (101, 102, 103) for data in ("0xaa", "0xbb")
    ]
    ahead.before(102)
    ahead.before(103)
    assert len(reader.batches) == 1

    # A sample that repeats one shape and adds another warms only the new one, for the
    # rest of the group.
    reader.use(call("0xaa", block(102)), call("0xee", block(102)))
    ahead.learn(block(102))
    ahead.before(103)
    assert reads(reader.batches[1]) == [(103, "0xee")]
    ahead.before(104)
    assert reads(reader.batches[2]) == [
        (at, data) for at in (104, 105, 106) for data in ("0xaa", "0xee")
    ]
    assert history.anchored == [tuple(range(100, 108))]


def test_a_time_the_history_cannot_resolve_is_left_to_its_sample():
    history = History(limit=102)
    ahead = ReadAhead(history, range(100, 106))
    ahead.before(100)
    history.reader.use(call("0xaa", block(100)))
    ahead.learn(block(100))
    ahead.before(101)
    assert reads(history.reader.batches[0]) == [(101, "0xaa"), (102, "0xaa")]


def test_a_refused_batch_leaves_the_samples_to_read_alone():
    history = History()
    history.reader.prefetch = lambda requests: (_ for _ in ()).throw(
        DataError("missing_capture", "offline")
    )
    ahead = ReadAhead(history, range(100, 103))
    ahead.before(100)
    history.reader.use(call("0xaa", block(100)))
    ahead.learn(block(100))
    ahead.before(101)
    ahead.before(102)
