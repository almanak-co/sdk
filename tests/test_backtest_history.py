"""Historical boundary, source and capture checks, with no online provider access."""

import json
import threading
from pathlib import Path

import httpx
import pydantic
import pytest

from almanak.backtest.evm import EvmHistory
from almanak.backtest.history import (
    Capture,
    HistoricalGateway,
    HistoricalReader,
    ReadEvidence,
    ReadRequest,
    validate_read,
)
from almanak.backtest.types import UnixTime
from almanak.caip import ChainId
from almanak.connectors.aave_v4.history import Market, read_market, verify_conversion
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.service import http_server
from almanak.models import Chain

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
TARGET = "0x" + "11" * 20
HASH = "0x" + "22" * 32
SELECTOR = {"blockHash": HASH, "requireCanonical": True}
FIXTURE = Path(__file__).parent / "fixtures/aave-v4-historical-market.json"


def header_client(timestamps, posts=None):
    """Answer header reads, alone or as a JSON-RPC batch; `posts` counts HTTP requests."""

    def answer(body):
        if body["method"] == "eth_chainId":
            result = "0x1"
        else:
            tag = body["params"][0]
            number = len(timestamps) - 1 if tag == "finalized" else int(tag, 16)
            result = {
                "number": hex(number),
                "hash": "0x" + f"{number:064x}",
                "timestamp": hex(timestamps[number]),
            }
        return {"jsonrpc": "2.0", "id": body["id"], "result": result}

    def handle(request):
        if posts is not None:
            posts.append(request)
        body = json.loads(request.content)
        return httpx.Response(
            200, json=[answer(item) for item in body] if isinstance(body, list) else answer(body)
        )

    return httpx.Client(transport=httpx.MockTransport(handle))


def test_floor_and_next_block_at_irregular_boundaries():
    with header_client([100, 112, 136, 148, 160]) as client:
        history = EvmHistory(EthereumReader(CHAIN, "https://rpc.invalid", client=client))
        for time, number in [(100, 0), (111, 0), (112, 1), (135, 1), (136, 2), (160, 4)]:
            assert history.floor(UnixTime(time)).number == number
        assert history.after(UnixTime(112)).number == 2
        assert history.pin(history.floor(UnixTime(135))).timestamp == 112
        for time in (99, 161):
            with pytest.raises(DataError, match="history"):
                history.floor(UnixTime(time))
        with pytest.raises(DataError, match="eligible"):
            history.after(UnixTime(160))


class CountingReader(EthereumReader):
    """Serves each read once from the provider; records the reads the caller consumed."""

    def __init__(self, client):
        super().__init__(CHAIN, "https://rpc.invalid", client=client)
        self.answers: dict[str, object] = {}
        self.consumed: list[str] = []

    def _rpc(self, method, params, *, allow_none=False):
        key = json.dumps([method, list(params)])
        self.consumed.append(key)
        if key not in self.answers:
            self.answers[key] = super()._rpc(method, params, allow_none=allow_none)
        return self.answers[key]


class BatchingReader(CountingReader):
    """Prefetches as one JSON-RPC batch per call, like the historical gateway's read_many."""

    def __init__(self, client, refusal=None):
        super().__init__(client)
        self.refusal = refusal
        self.batches: list[int] = []

    def prefetch(self, requests):
        if self.refusal is not None:
            raise self.refusal
        self.batches.append(len(requests))
        body = [
            {"jsonrpc": "2.0", "id": i, "method": r.method, "params": list(r.params)}
            for i, r in enumerate(requests)
        ]
        rows = self._client.post(self._endpoint, json=body).json()
        for request, row in zip(requests, sorted(rows, key=lambda row: row["id"]), strict=True):
            self.answers.setdefault(
                json.dumps([request.method, list(request.params)]), row["result"]
            )


TIMESTAMPS = [100 + 3 * i - (i % 2) for i in range(1000)]
TIMES = [UnixTime(t) for t in (100, 101, 1234, 1235, 1300, 2000, 3095, 3096)]


@pytest.mark.parametrize(
    "refusal",
    [
        None,
        DataError("missing_capture", "offline"),
        DataError("invalid_request", "gateway request refused"),
        ValueError("unbatched gateway"),
    ],
)
def test_floor_search_prefetch_keeps_the_search_path(refusal):
    alone_posts, batched_posts = [], []
    with (
        header_client(TIMESTAMPS, alone_posts) as alone_client,
        header_client(TIMESTAMPS, batched_posts) as batched_client,
    ):
        alone = CountingReader(alone_client)
        batched = BatchingReader(batched_client, refusal)
        alone_history, batched_history = EvmHistory(alone), EvmHistory(batched)
        expected = [alone_history.floor(t) for t in TIMES]
        assert [batched_history.floor(t) for t in TIMES] == expected
    # The same headers are consumed in the same order; only the HTTP requests change.
    assert batched.consumed == alone.consumed
    if refusal is None:
        assert batched.batches and max(batched.batches) <= 15
        assert len(batched_posts) < len(alone_posts) / 2
    else:
        assert len(batched_posts) == len(alone_posts)


@pytest.mark.parametrize(
    "refusal",
    [None, DataError("missing_capture", "offline"), ValueError("unbatched gateway")],
)
def test_bulk_floor_prefetch_reads_only_the_search_paths(refusal):
    # Beyond the pinned history: left for `floor` to refuse.
    times = [*TIMES, UnixTime(TIMESTAMPS[-1] + 1)]
    alone_posts, batched_posts = [], []
    with (
        header_client(TIMESTAMPS, alone_posts) as alone_client,
        header_client(TIMESTAMPS, batched_posts) as batched_client,
    ):
        alone = CountingReader(alone_client)
        batched = BatchingReader(batched_client, refusal)
        alone_history, batched_history = EvmHistory(alone), EvmHistory(batched)
        expected = [alone_history.floor(t) for t in TIMES]
        batched_history.prefetch_floors(times)
        resolved = len(batched_posts)
        assert [batched_history.floor(t) for t in TIMES] == expected
        with pytest.raises(DataError, match="exceeds pinned history"):
            batched_history.floor(times[-1])
    # No header is read that the searches would not read alone.
    assert set(batched.consumed) == set(alone.consumed)
    if refusal is None:
        # One batch per search level, and one for the unprobed first block, resolved
        # every timestamp; the searches then read nothing.
        assert len(batched_posts) == resolved
        assert len(batched.batches) == 11
        assert sum(batched.batches) == len(set(alone.consumed)) - 2
        assert len(batched_posts) < len(alone_posts) / 3
    else:
        assert batched.batches == []


def test_bulk_floor_prefetch_needs_a_batching_reader():
    with header_client(TIMESTAMPS) as client:
        reader = CountingReader(client)
        history = EvmHistory(reader)
        consumed = len(reader.consumed)
        history.prefetch_floors(TIMES)
        assert len(reader.consumed) == consumed


@pytest.mark.parametrize(
    "kind", ["source_mismatch", "malformed", "corrupt_capture", "changed_history"]
)
def test_floor_search_raises_an_inconsistent_prefetch(kind):
    with header_client(TIMESTAMPS) as client:
        history = EvmHistory(BatchingReader(client, DataError(kind, "batch identity differs")))
        with pytest.raises(DataError, match="batch identity differs"):
            history.floor(TIMES[2])


def test_floor_search_raises_a_malformed_prefetch_row():
    # Pydantic's ValidationError is a ValueError, but a malformed row is not a refusal.
    with pytest.raises(pydantic.ValidationError) as malformed:
        ReadEvidence.model_validate_json("{}")
    with header_client(TIMESTAMPS) as client:
        history = EvmHistory(BatchingReader(client, malformed.value))
        with pytest.raises(pydantic.ValidationError):
            history.floor(TIMES[2])


def test_event_anchor_resolves_last_block_of_same_second_after_cache_loss():
    with header_client([100, 112, 112, 112, 160]) as client:
        history = EvmHistory(EthereumReader(CHAIN, "https://rpc.invalid", client=client))
        event = history.header(1)
        expected = history.floor(UnixTime(112))
        history.headers.clear()
        assert history.event_floor(event) == expected and expected.number == 3
        assert history.event_floor(history.upper) == history.upper
        with pytest.raises(DataError, match="outside finalized"):
            history.event_floor(event.model_copy(update={"number": 5}))
        with pytest.raises(DataError, match="changed"):
            history.event_floor(event.model_copy(update={"hash": HASH}))
    with header_client([100, 112, 111, 160]) as client:
        history = EvmHistory(EthereumReader(CHAIN, "https://rpc.invalid", client=client))
        with pytest.raises(DataError, match="backwards"):
            history.event_floor(history.header(1))


@pytest.mark.parametrize(
    "method,params",
    [
        ("eth_chainId", ["unexpected"]),
        ("eth_getBlockByNumber", ["latest", False]),
        ("eth_getBlockByNumber", ["0x01", False]),
        ("eth_getBlockByNumber", ["0x1", True]),
        ("eth_call", [{"to": TARGET, "data": "0x"}, "latest"]),
        ("eth_call", [{"to": TARGET, "data": "0x", "from": TARGET}, SELECTOR]),
        ("eth_getCode", [TARGET, {"blockHash": HASH, "requireCanonical": False}]),
        ("eth_getCode", ["0x" + "33" * 20, SELECTOR]),
    ],
)
def test_gateway_refuses_unbound_or_unpinned_reads(method, params):
    request = ReadRequest(chain=CHAIN, method=method, params=tuple(params))
    with pytest.raises(ValueError):
        validate_read(request, (TARGET,))


def test_gateway_capture_is_durable_and_offline_missing_is_an_error(tmp_path):
    capture = Capture(tmp_path / "capture.sqlite")
    request = ReadRequest(chain=CHAIN, method="eth_chainId", params=())
    evidence = capture.put(request, "0x1")
    restored = Capture(capture.path)
    assert restored.get(request) == evidence
    assert restored.count() == 1
    backend = HistoricalGateway(restored, {}, {CHAIN: (TARGET,)})
    assert backend.dispatch("read", request) == evidence
    with pytest.raises(DataError, match="missing"):
        backend.dispatch(
            "read", ReadRequest(chain=CHAIN, method="eth_getCode", params=(TARGET, SELECTOR))
        )
    with pytest.raises(DataError, match="conflicting"):
        capture.put(request, "0x2")
    with pytest.raises(ValueError):
        backend.dispatch("broadcast", request)


def test_http_authentication_and_capture_round_trip(tmp_path):
    token = tmp_path / "token"
    token.write_text("ab" * 32)
    token.chmod(0o600)
    capture = Capture(tmp_path / "capture.sqlite")
    capture.put(ReadRequest(chain=CHAIN, method="eth_chainId", params=()), "0x1")
    gateway = HistoricalGateway(capture, {}, {CHAIN: (TARGET,)})
    server = http_server(gateway, token, 0, request_type=ReadRequest, fields={"read"})
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    try:
        with HistoricalReader(CHAIN, endpoint, token.read_text()) as reader:
            reader.verify_chain()
            assert len(reader.used) == 1
            assert reader._rpc("eth_chainId", []) == "0x1"
            assert len(reader.cache) == 1
            with pytest.raises(DataError, match="missing a required observation") as missing:
                reader._rpc("eth_getCode", [TARGET, SELECTOR])
            assert missing.value.kind == "missing_capture"
        with httpx.Client() as client:
            assert client.post(endpoint + "/read", json={}).status_code == 401
            assert (
                client.post(
                    endpoint + "/broadcast",
                    json={},
                    headers={"Authorization": "Bearer " + token.read_text()},
                ).status_code
                == 400
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_captured_real_aave_market_reads_and_rounded_conversions():
    fixture = json.loads(FIXTURE.read_text())
    records = [ReadEvidence.model_validate_json(json.dumps(row)) for row in fixture["requests"]]
    responses = {record.request.key: record.result for record in records}

    def handle(request):
        body = json.loads(request.content)
        read = ReadRequest(chain=CHAIN, method=body["method"], params=tuple(body["params"]))
        return httpx.Response(
            200, json={"jsonrpc": "2.0", "id": body["id"], "result": responses[read.key]}
        )

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        reader = EthereumReader(CHAIN, "https://fixture.invalid", client=client)
        for raw in fixture["markets"]:
            expected = Market.model_validate_json(json.dumps(raw))
            block = reader.pin_block(expected.reserve.block.number)
            market = read_market(reader, expected.reserve.source, block)
            assert market == expected
            verify_conversion(reader, market, 10**16, 0)
            minted = market.supplied_shares(10**16)
            redeemed = market.supplied_assets(minted)
            assert redeemed <= 10**16
            assert market.burned_shares(redeemed) <= minted


def test_proxy_code_reads_require_retained_same_block_pointer_evidence(tmp_path):
    from almanak.gateway.roles_storage import IMPLEMENTATION_SLOT

    capture = Capture(tmp_path / "observations.sqlite")
    anchor = {"blockHash": "0x" + "12" * 32, "requireCanonical": True}
    implementation = "0x" + "34" * 20
    request = ReadRequest(chain=CHAIN, method="eth_getCode", params=(implementation, anchor))
    with pytest.raises(ValueError, match="outside"):
        validate_read(request, (TARGET,), implementation_lookup=capture.get)
    pointer = ReadRequest(
        chain=CHAIN, method="eth_getStorageAt", params=(TARGET, hex(IMPLEMENTATION_SLOT), anchor)
    )
    capture.put(pointer, "0x" + "0" * 24 + implementation[2:])
    validate_read(request, (TARGET,), implementation_lookup=capture.get)
    different_block = request.model_copy(
        update={"params": (implementation, {**anchor, "blockHash": "0x" + "56" * 32})}
    )
    with pytest.raises(ValueError, match="outside"):
        validate_read(different_block, (TARGET,), implementation_lookup=capture.get)
    call = request.model_copy(
        update={
            "method": "eth_call",
            "params": ({"to": implementation, "data": "0x12345678"}, anchor),
        }
    )
    with pytest.raises(ValueError, match="outside"):
        validate_read(call, (TARGET,), implementation_lookup=capture.get)


def test_beacon_proxy_reads_follow_retained_same_block_beacon_evidence(tmp_path):
    from almanak.backtest.history import BEACON_IMPLEMENTATION
    from almanak.gateway.roles_storage import BEACON_SLOT

    capture = Capture(tmp_path / "observations.sqlite")
    anchor = {"blockHash": "0x" + "12" * 32, "requireCanonical": True}
    beacon, implementation = "0x" + "5a" * 20, "0x" + "7b" * 20

    def read(method, *params):
        return ReadRequest(chain=CHAIN, method=method, params=(*params, anchor))

    beacon_code = read("eth_getCode", beacon)
    lookup = read("eth_call", {"to": beacon, "data": BEACON_IMPLEMENTATION})
    implementation_code = read("eth_getCode", implementation)
    for request in (beacon_code, lookup, implementation_code):
        with pytest.raises(ValueError, match="outside"):
            validate_read(request, (TARGET,), implementation_lookup=capture.get)
    capture.put(read("eth_getStorageAt", TARGET, hex(BEACON_SLOT)), "0x" + "0" * 24 + beacon[2:])
    validate_read(beacon_code, (TARGET,), implementation_lookup=capture.get)
    validate_read(lookup, (TARGET,), implementation_lookup=capture.get)
    other_call = read("eth_call", {"to": beacon, "data": "0x12345678"})
    with pytest.raises(ValueError, match="outside"):
        validate_read(other_call, (TARGET,), implementation_lookup=capture.get)
    # The implementation's code needs the beacon's retained answer naming it.
    with pytest.raises(ValueError, match="outside"):
        validate_read(implementation_code, (TARGET,), implementation_lookup=capture.get)
    capture.put(lookup, "0x" + "0" * 24 + implementation[2:])
    validate_read(implementation_code, (TARGET,), implementation_lookup=capture.get)
    with pytest.raises(ValueError, match="outside"):
        validate_read(
            read("eth_call", {"to": implementation, "data": BEACON_IMPLEMENTATION}),
            (TARGET,),
            implementation_lookup=capture.get,
        )
    # A checksummed request address names the same contract as the captured pointer.
    for request in (
        read("eth_getCode", beacon.upper().replace("0X", "0x")),
        read("eth_call", {"to": beacon.upper().replace("0X", "0x"), "data": BEACON_IMPLEMENTATION}),
        read("eth_getCode", implementation.upper().replace("0X", "0x")),
    ):
        validate_read(request, (TARGET,), implementation_lookup=capture.get)


def test_reader_cache_retains_active_evidence_and_bounds_old_inputs():
    from collections import OrderedDict

    from almanak.backtest.history import HistoricalReader

    reader = object.__new__(HistoricalReader)
    reader.cache = OrderedDict((str(i), None) for i in range(5000))
    reader.used = {"0", "1"}
    reader.trim_cache()
    assert len(reader.cache) == 4098
    assert {"0", "1"} <= reader.cache.keys()
    reader.used.clear()
    reader.trim_cache()
    assert len(reader.cache) == 4096
    assert "0" not in reader.cache


def test_capture_wal_keeps_full_durability_and_concurrent_response_identity(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    from almanak.backtest.history import Capture, ReadRequest
    from almanak.caip import ChainId
    from almanak.models import Chain

    capture = Capture(tmp_path / "concurrent.sqlite")
    chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    request = ReadRequest(chain=chain, method="eth_chainId", params=())
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = tuple(pool.map(lambda _: capture.put(request, "0x1"), range(12)))
    assert len({r.model_dump_json() for r in results}) == 1
    with capture.connect() as db:
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    capture.close()
    restored = Capture(capture.path)
    assert restored.get(request) == results[0]
    restored.close()
