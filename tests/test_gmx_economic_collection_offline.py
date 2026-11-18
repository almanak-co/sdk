"""Prepared GMX range collection and publication-source discovery on stand-in histories.

Readers, histories and dataset writers are in-memory stand-ins. The cases pin the
range limits, finalized-anchor checks and evidence assembly that the retained
preparation captures exercise with real provider reads.
"""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import economic_authority, economic_collection
from almanak.connectors.gmx_v2 import economic_data as data
from almanak.gateway.ethereum import DataError
from tests import gmx_synthetic_frames as frames

TARGET = frames.exchange_target()
MARKET = TARGET.market
START, DEADLINE = 1_000, 2_000


def block(number, tag="ab"):
    return frames.block(MARKET, number=number, timestamp=START + number).model_copy(
        update={"hash": "0x" + tag * 32}
    )


class Reader:
    def __init__(self, finalized=30, replaced=(), responses=None):
        self.used, self.cache, self.calls = set(), {}, []
        self.finalized, self.replaced = finalized, set(replaced)
        self.responses = responses or {}

    def pin_block(self, number):
        if number == "finalized":
            number = self.finalized
        return block(number, "ee" if number in self.replaced else "ab")

    def _rpc(self, method, args):
        self.calls.append((method, args))
        self.used.add(method)
        self.cache[method] = SimpleNamespace(retrieved_at=77)
        value = self.responses.get(method)
        return value(args) if callable(value) else value

    def call(self, address, signature, at, **kwargs):
        self.calls.append((address, signature, at))
        return self.responses[signature](address)


class History:
    def __init__(self, reader, first=10, last=12, upper=30):
        self.reader, self.upper = reader, block(upper)
        self.bounds = {START: block(first), DEADLINE: block(last)}
        self.headers = {n: block(n) for n in range(first, upper + 1)}
        self.pinned = []

    def floor(self, time):
        return self.bounds[time]

    def header(self, number):
        return self.headers[number]

    def pin(self, header):
        self.pinned.append(header.number)
        return header


class Writer:
    def __init__(self, missing=True):
        self.missing_keys, self.committed = missing, []

    def missing(self, key):
        return self.missing_keys

    def commit_range(self, acquired, records, raw):
        self.committed.append((acquired, records, raw))


def settings(maximum=10):
    return SimpleNamespace(
        schedule=SimpleNamespace(start=START, deadline=DEADLINE),
        data=SimpleNamespace(maximum_risk_blocks=maximum),
    )


def digest(name):
    return SimpleNamespace(digest=name)


@pytest.fixture
def collected(monkeypatch):
    """Stub the separately tested publication scan, frame reads and evidence capture."""
    captures = iter([(digest("boundary"),), (digest("final"), digest("boundary"))])
    monkeypatch.setattr(economic_collection, "evidence", lambda reader: next(captures))
    monkeypatch.setattr(
        economic_collection,
        "publication_candidates",
        lambda history, target, settings, first, last: {"tx": (first.number, "hash", 0)},
    )
    frame_reads = []

    def read_frames(history, target, settings, first, last, candidates):
        frame_reads.append((first.number, last.number, candidates))
        return (
            (SimpleNamespace(block=block(10)), (digest("a"),)),
            (SimpleNamespace(block=block(11)), (digest("b"), digest("boundary"))),
        )

    monkeypatch.setattr(economic_collection, "read_frames", read_frames)
    monkeypatch.setattr(data, "Record", SimpleNamespace)
    monkeypatch.setattr(economic_collection, "AcquiredRange", SimpleNamespace)
    return frame_reads


def test_collect_commits_one_record_per_frame_with_deduplicated_evidence(collected):
    reader = Reader()
    history, writer = History(reader), Writer()
    economic_collection.collect(writer, history, TARGET, settings())
    ((acquired, records, raw),) = writer.committed
    assert acquired.key == data.key(TARGET)
    assert (acquired.start, acquired.end) == (START, DEADLINE)
    assert acquired.evidence == ("boundary", "a", "b", "final")
    assert [value.digest for value in raw] == list(acquired.evidence)
    assert [r.reference for r in records] == [block(10), block(11)]
    assert [r.evidence for r in records] == [("boundary", "a"), ("boundary", "b")]
    assert {r.retrieved_at for r in records} == {77}
    assert records[0].available_at == max(START, block(10).timestamp)
    assert collected == [(10, 12, {"tx": (10, "hash", 0)})]
    # Every retained header, both following boundary headers, then the range ends.
    assert history.pinned[-4:] == [11, 13, 10, 12]
    assert reader.calls[0] == ("eth_chainId", [])


def test_collect_skips_a_range_already_retained(collected):
    reader = Reader()
    economic_collection.collect(Writer(missing=False), History(reader), TARGET, settings())
    assert reader.calls == [] and collected == []


def test_collect_refuses_a_moved_finalized_anchor(collected):
    with pytest.raises(ValueError, match="finalized history anchor"):
        economic_collection.collect(Writer(), History(Reader(finalized=31)), TARGET, settings())


def test_collect_refuses_a_range_beyond_its_block_limit(collected):
    with pytest.raises(ValueError, match="explicit block limit"):
        economic_collection.collect(Writer(), History(Reader()), TARGET, settings(maximum=2))


def test_collect_requires_a_later_finalized_boundary_header(collected):
    reader = Reader(finalized=12)
    with pytest.raises(ValueError, match="later finalized boundary header"):
        economic_collection.collect(Writer(), History(reader, upper=12), TARGET, settings())


# Publication-source discovery

SOURCE = frames.source()


def discovery(monkeypatch, candidates, reports):
    monkeypatch.setattr(
        economic_authority,
        "publication_candidates",
        lambda history, target, settings, first, last: candidates,
    )
    monkeypatch.setattr(
        economic_authority, "report_vectors", lambda value, source: reports.get(value["hash"])
    )


def source_settings():
    return SimpleNamespace(data=SimpleNamespace(publication=lambda allocation: SOURCE))


def transactions(**overrides):
    def lookup(args):
        (transaction,) = args
        value = {"hash": transaction, "blockHash": block(int(transaction[-2:], 16)).hash}
        return overrides.get(transaction, value)

    return {"eth_getTransactionByHash": lookup}


def tx(number):
    return "0x" + f"{number:02x}" * 32


def test_selected_source_blocks_keep_blocks_of_selected_source_transactions(monkeypatch):
    candidates = {tx(11): (11, block(11).hash, 0), tx(12): (12, block(12).hash, 0)}
    candidates[tx(13)] = (13, block(13).hash, 0)
    discovery(monkeypatch, candidates, {tx(11): {}, tx(13): {}})
    history = SimpleNamespace(reader=Reader(responses=transactions()))
    found = economic_authority.selected_source_blocks(history, TARGET, source_settings(), (1, 2))
    assert found == (block(11), block(13))


@pytest.mark.parametrize(
    "replaced, overrides, message",
    [
        ({11}, {}, "changed its canonical block"),
        (set(), {tx(11): None}, "exact transaction"),
        (set(), {tx(11): {"hash": tx(12)}}, "exact transaction"),
        (set(), {tx(11): {"hash": tx(11), "blockHash": "0x" + "01" * 32}}, "canonical block"),
    ],
    ids=["reorganized", "missing", "other-hash", "moved"],
)
def test_selected_source_blocks_refuse_noncanonical_transactions(
    monkeypatch, replaced, overrides, message
):
    discovery(monkeypatch, {tx(11): (11, block(11).hash, 0)}, {tx(11): {}})
    reader = Reader(replaced=replaced, responses=transactions(**overrides))
    with pytest.raises(DataError, match=message):
        economic_authority.selected_source_blocks(
            SimpleNamespace(reader=reader), TARGET, source_settings(), (1, 2)
        )


def provider_reader(store=MARKET.contracts.data_store, oracle=SOURCE.oracle, replaced=()):
    return Reader(
        replaced=replaced,
        responses={"dataStore()": lambda a: (store,), "oracle()": lambda a: (oracle,)},
    )


def providers(monkeypatch, reader, blocks):
    monkeypatch.setattr(economic_authority, "targets", lambda config: (TARGET,))
    monkeypatch.setattr(economic_authority, "selected_source_blocks", lambda *args: blocks)
    monkeypatch.setattr(
        economic_authority,
        "stored",
        lambda reader, at, store, kind, name, types, args: "0x" + args[1][2:4] * 20,
    )
    histories = {TARGET.fund.chain: SimpleNamespace(reader=reader)}
    return economic_authority.provider_scopes(
        histories, None, source_settings(), {TARGET.fund.chain: (1, 2)}
    )


def test_provider_scopes_name_each_oracle_provider_once_in_block_order(monkeypatch):
    found = providers(monkeypatch, provider_reader(), (block(12), block(11)))
    store = MARKET.contracts.data_store
    oracles = {SOURCE.oracle, MARKET.contracts.oracle}
    assets = {MARKET.index_asset, MARKET.long_asset, MARKET.short_asset}
    expected_providers = {"0x" + asset.reference[2:4] * 20 for asset in assets}
    assert [value[0].number for value in found] == sorted(value[0].number for value in found)
    assert {value[0] for value in found} == {block(11), block(12)}
    assert {value[1] for value in found} == {store}
    assert {value[2] for value in found} == oracles
    assert {value[3] for value in found} == expected_providers
    assert len(found) == 2 * len(oracles) * len(expected_providers)


def test_provider_scopes_refuse_moved_blocks_and_other_handler_bindings(monkeypatch):
    with pytest.raises(DataError, match="changed its pinned block"):
        providers(monkeypatch, provider_reader(replaced={11}), (block(11),))
    with pytest.raises(ValueError, match="selected Oracle binding"):
        providers(monkeypatch, provider_reader(oracle=MARKET.contracts.oracle), (block(11),))
