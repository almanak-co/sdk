"""Batched JSON-RPC reads: id matching, fail-closed faults and bounded provider pushback."""

import json
import threading
import time
from collections import Counter
from types import SimpleNamespace

import httpx
import pytest
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.gateway import ethereum
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.roles_storage import AuthorityReader, batch_size
from almanak.models import Chain

START = 1_000
MAINNET = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")


class FakeChain:
    """Linked block headers from block 0 through `head`."""

    def __init__(self, head):
        self.head = head
        self.headers = {}
        parent = "0x" + "00" * 32
        for number in range(head + 1):
            block_hash = "0x" + keccak(text=f"block:{number}").hex()
            self.headers[number] = {
                "number": hex(number),
                "hash": block_hash,
                "parentHash": parent,
                "timestamp": hex(number),
            }
            parent = block_hash

    def result(self, method, params):
        if method == "eth_chainId":
            return "0x1"
        assert method == "eth_getBlockByNumber"
        number = self.head if params[0] == "latest" else int(params[0], 16)
        return self.headers.get(number)


class Transport:
    def __init__(self, chain):
        self.chain = chain
        self.member = self.batch = None
        # Batch requests answered with HTTP 429 before any is served.
        self.busy, self.retry_after = 0, None
        self.methods: Counter[str] = Counter()
        self.sizes: list[int] = []

    def answer(self, body):
        self.methods[body["method"]] += 1
        row = {"jsonrpc": "2.0", "id": body["id"]}
        row["result"] = self.chain.result(body["method"], body["params"])
        return self.member(body, row) if self.member else row

    def __call__(self, request):
        body = json.loads(request.content)
        if not isinstance(body, list):
            return httpx.Response(200, json=self.answer(body))
        self.sizes.append(len(body))
        if self.busy:
            self.busy -= 1
            headers = {} if self.retry_after is None else {"Retry-After": self.retry_after}
            return httpx.Response(429, headers=headers)
        rows = [self.answer(member) for member in body]
        return httpx.Response(200, json=self.batch(rows) if self.batch else rows)


@pytest.fixture
def connect():
    clients = []

    def create(chain, *, member=None, batch=None, retry_budget=0.0):
        transport = Transport(chain)
        client = httpx.Client(transport=httpx.MockTransport(transport))
        clients.append(client)
        reader = EthereumReader(
            MAINNET,
            "https://fixture.invalid",
            client=client,
            retry_budget=retry_budget,
        )
        authority = AuthorityReader(reader, reader.pin_block("latest"))
        # Faults and counts start after the observation is pinned.
        transport.member, transport.batch = member, batch
        transport.methods.clear()
        return authority, transport

    yield create
    for client in clients:
        client.close()


def read_headers(reader, first, last, in_flight=1):
    """Headers `first` through `last` in batches of the reader's batch size, in order."""
    calls = [("eth_getBlockByNumber", [hex(n), False]) for n in range(first, last + 1)]
    size = batch_size(reader)
    batches = (calls[offset : offset + size] for offset in range(0, len(calls), size))
    return [header for batch in reader.batches(batches, in_flight) for header in batch]


def expected(chain, first, last):
    return [chain.headers[n] for n in range(first, last + 1)]


@pytest.fixture
def no_backoff(monkeypatch):
    # Waits are recorded and advance a fake clock instead of sleeping.
    waits = []
    monkeypatch.setattr(ethereum, "sleep", waits.append)
    monkeypatch.setattr(ethereum, "monotonic", lambda: sum(waits))
    return waits


def test_rpc_cap_below_the_batch_size_shrinks_batches(connect):
    chain = FakeChain(START + 30)
    reader, transport = connect(chain)
    admitted = []
    reader.reader.provider_limit = SimpleNamespace(maximum=8, admit=admitted.append)
    assert read_headers(reader, START, START + 30) == expected(chain, START, START + 30)
    assert max(transport.sizes) == 8
    assert sum(admitted) == transport.methods.total()


def test_responses_are_matched_by_id_not_position(connect):
    chain = FakeChain(START + 150)
    reader, _ = connect(chain, batch=lambda rows: rows[::-1])
    assert read_headers(reader, START, START + 150) == expected(chain, START, START + 150)


def drop_block(number):
    def batch(rows):
        return [r for r in rows if not (r.get("result") or {}).get("number") == hex(number)]

    return batch


def reject(response):
    def member(body, row):
        return {key: value for key, value in {**row, **response}.items() if key != "result"}

    return member


@pytest.mark.parametrize(
    ("faults", "kind", "message"),
    [
        ({"batch": drop_block(START + 50)}, "malformed", "invalid response count"),
        ({"batch": lambda rows: rows[:1] + rows[:-1]}, "malformed", "invalid response envelope"),
        (
            {"batch": lambda rows: [*rows[:-1], {**rows[-1], "id": 10**9}]},
            "malformed",
            "response identity differs",
        ),
        (
            {"member": reject({"error": {"code": -32000}})},
            "rpc",
            r"RPC rejected the requested read \(JSON-RPC -32000\)",
        ),
        ({"member": reject({"result": None})}, "unavailable", "requested data is unavailable"),
        (
            {"batch": lambda rows: {"jsonrpc": "2.0", "id": None, "error": {"code": -32600}}},
            "rpc",
            "rejected the JSON-RPC batch",
        ),
    ],
)
def test_incomplete_or_rejected_batches_fail_closed(connect, faults, kind, message):
    reader, _ = connect(FakeChain(START + 60), **faults)
    with pytest.raises(DataError, match=message) as refused:
        read_headers(reader, START, START + 60)
    assert refused.value.kind == kind


def test_a_busy_provider_is_retried_within_bounds(connect, no_backoff):
    # 51 headers: one batch in flight, so the waits follow one batch's attempts.
    chain = FakeChain(START + 50)
    reader, transport = connect(chain)
    admitted = []
    reader.reader.provider_limit = SimpleNamespace(maximum=None, admit=admitted.append)
    before = reader.reader.rpc_count
    transport.busy = 2
    assert read_headers(reader, START, START + 50) == expected(chain, START, START + 50)
    assert no_backoff == [0.5, 1.0]
    # Each re-sent batch passed admission again and counts as new operations.
    assert sum(admitted) == transport.methods.total() + 2 * 51
    assert reader.reader.rpc_count - before == sum(admitted)


def test_a_persistently_busy_provider_fails_the_read(connect, no_backoff):
    reader, transport = connect(FakeChain(START + 50))
    transport.busy = 100
    with pytest.raises(ethereum.ProviderBusy, match="HTTP 429") as refused:
        read_headers(reader, START, START + 50)
    assert refused.value.kind == "http"
    assert no_backoff == [0.5, 1.0, 2.0, 4.0, 8.0, 8.0]
    assert transport.sizes == [51] * (ethereum.BUSY_RETRIES + 1)


def throttling(code, *, times):
    """A batch hook that answers HTTP 200 but throttles every other member `times` times."""
    left = [times]

    def batch(rows):
        if not left[0]:
            return rows
        left[0] -= 1
        throttled = rows if len(rows) == 1 else rows[1::2]
        for row in throttled:
            row.pop("result")
            row["error"] = {"code": code, "message": "exceeded compute units per second capacity"}
        return rows

    return batch


@pytest.mark.parametrize("code", [429, -32005])
def test_throttled_members_are_resent_alone_and_answered_ones_kept(connect, no_backoff, code):
    chain = FakeChain(START + 50)
    reader, transport = connect(chain, batch=throttling(code, times=2))
    admitted = []
    reader.reader.provider_limit = SimpleNamespace(maximum=None, admit=admitted.append)
    assert read_headers(reader, START, START + 50) == expected(chain, START, START + 50)
    # 51 headers: 25 throttled, then 12 of those, then the last 12 answered.
    assert transport.sizes == [51, 25, 12]
    assert no_backoff == [0.5, 1.0]
    # Every re-sent member passed admission again.
    assert sum(admitted) == transport.methods.total()


def test_persistently_throttled_members_fail_the_read_with_the_code(connect, no_backoff):
    reader, transport = connect(FakeChain(START), batch=throttling(429, times=100))
    with pytest.raises(ethereum.MembersBusy, match=r"JSON-RPC 429") as refused:
        read_headers(reader, START, START)
    assert refused.value.kind == "http"
    assert no_backoff == [0.5, 1.0, 2.0, 4.0, 8.0, 8.0]
    assert transport.sizes == [1] * (ethereum.BUSY_RETRIES + 1)


def test_a_refused_member_is_not_retried_and_names_its_code(connect, no_backoff):
    def refuse(rows):
        rows[0].pop("result")
        rows[0]["error"] = {"code": -32000, "message": "https://fixture.invalid/v2/secret"}
        return rows

    reader, transport = connect(FakeChain(START + 50), batch=refuse)
    with pytest.raises(DataError) as refused:
        read_headers(reader, START, START + 50)
    assert (
        str(refused.value)
        == "eth_getBlockByNumber: RPC rejected the requested read (JSON-RPC -32000)"
    )
    assert "secret" not in str(refused.value)
    assert (refused.value.kind, refused.value.code) == ("rpc", -32000)
    assert no_backoff == [] and transport.sizes == [51]


def test_a_long_requested_wait_fails_instead_of_waiting(connect, no_backoff):
    reader, transport = connect(FakeChain(START + 50))
    transport.busy, transport.retry_after = 1, "31"
    with pytest.raises(ethereum.ProviderBusy):
        read_headers(reader, START, START + 50)
    assert no_backoff == []
    reader, transport = connect(FakeChain(START + 50))
    transport.busy, transport.retry_after = 1, "2.5"
    read_headers(reader, START, START + 50)
    assert no_backoff == [2.5]


def single_reads(reader, *answers):
    """Answer a reader's single reads with `answers` in turn, then with the chain id."""
    sent, left = [], list(answers)

    def respond(request):
        body = json.loads(request.content)
        sent.append(body["method"])
        answer = left.pop(0) if left else {"result": "0x1"}
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], **answer})

    reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    return sent


@pytest.mark.parametrize(
    "throttle",
    [
        httpx.Response(429),
        {"error": {"code": 429, "message": "compute units exceeded"}},
        {"error": {"code": -32005, "message": "limit exceeded"}},
    ],
)
def test_a_throttled_single_read_fails_by_default(connect, no_backoff, throttle):
    # Execution, fork and backtest readers: a throttle fails the read at once.
    authority, _ = connect(FakeChain(START))
    sent = single_reads(authority.reader, throttle)
    with pytest.raises(DataError) as refused:
        authority.reader._rpc("eth_chainId", [])
    assert refused.value.code in ethereum.RATE_LIMITED
    assert sent == ["eth_chainId"] and no_backoff == []


@pytest.mark.parametrize(
    "throttle",
    [
        httpx.Response(429),
        {"error": {"code": 429, "message": "compute units exceeded"}},
        {"error": {"code": -32005, "message": "limit exceeded"}},
    ],
)
def test_an_observation_reader_retries_a_throttled_single_read(connect, no_backoff, throttle):
    authority, _ = connect(FakeChain(START), retry_budget=ethereum.BUSY_WAIT_LIMIT)
    reader = authority.reader
    admitted = []
    reader.provider_limit = SimpleNamespace(maximum=None, admit=admitted.append)
    before = reader.rpc_count
    sent = single_reads(reader, throttle, throttle)
    assert reader._rpc("eth_chainId", []) == "0x1"
    assert no_backoff == [0.5, 1.0] and sent == ["eth_chainId"] * 3
    # Each attempt passed admission again and counts as a new operation.
    assert admitted == [1, 1, 1] and reader.rpc_count - before == 3


def test_a_persistently_throttled_single_read_fails_with_the_providers_answer(connect, no_backoff):
    authority, _ = connect(FakeChain(START), retry_budget=ethereum.BUSY_WAIT_LIMIT)
    reader = authority.reader
    sent = single_reads(reader, *[httpx.Response(429)] * 10)
    with pytest.raises(ethereum.ProviderBusy, match="HTTP 429"):
        reader._rpc("eth_chainId", [])
    assert no_backoff == [0.5, 1.0, 2.0, 4.0, 8.0, 8.0]
    assert len(sent) == ethereum.BUSY_RETRIES + 1
    member = {"error": {"code": -32005, "message": "limit exceeded"}}
    no_backoff.clear()
    sent = single_reads(reader, *[member] * 10)
    with pytest.raises(DataError) as refused:
        reader._rpc("eth_chainId", [])
    assert (refused.value.kind, refused.value.code) == ("rpc", -32005)
    assert len(sent) == ethereum.BUSY_RETRIES + 1


def test_an_observation_read_waits_at_most_the_total_budget(connect, no_backoff):
    authority, _ = connect(FakeChain(START), retry_budget=ethereum.BUSY_WAIT_LIMIT)
    reader = authority.reader
    sent = single_reads(reader, *[httpx.Response(429, headers={"Retry-After": "10"})] * 10)
    with pytest.raises(ethereum.ProviderBusy):
        reader._rpc("eth_chainId", [])
    assert no_backoff == [10.0, 10.0, 10.0] and len(sent) == 4
    no_backoff.clear()
    sent = single_reads(reader, *[httpx.Response(429, headers={"Retry-After": "20"})] * 10)
    with pytest.raises(ethereum.ProviderBusy):
        reader._rpc("eth_chainId", [])
    assert no_backoff == [20.0] and len(sent) == 2
    assert sum(no_backoff) <= ethereum.BUSY_WAIT_LIMIT


@pytest.mark.parametrize(("latency", "sends"), [(0.1, 7), (5.0, 5), (14.9, 2)])
def test_an_observation_read_stops_within_the_budget_from_its_first_attempt(
    connect, monkeypatch, latency, sends
):
    # Each throttle takes `latency` to answer; the budget counts that time as well.
    clock = [0.0]
    monkeypatch.setattr(ethereum, "monotonic", lambda: clock[0])
    monkeypatch.setattr(ethereum, "sleep", lambda wait: clock.__setitem__(0, clock[0] + wait))
    authority, _ = connect(FakeChain(START), retry_budget=ethereum.BUSY_WAIT_LIMIT)
    reader = authority.reader
    sent = []

    def respond(request):
        clock[0] += latency
        sent.append(clock[0])
        return httpx.Response(429)

    reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    with pytest.raises(ethereum.ProviderBusy):
        reader._rpc("eth_chainId", [])
    assert len(sent) == sends
    assert sent[-1] <= ethereum.BUSY_WAIT_LIMIT + latency


def test_single_refusals_long_waits_and_submissions_are_not_retried(connect, no_backoff):
    authority, _ = connect(FakeChain(START), retry_budget=ethereum.BUSY_WAIT_LIMIT)
    reader = authority.reader
    sent = single_reads(reader, {"error": {"code": -32000, "message": "execution reverted"}})
    with pytest.raises(DataError, match="JSON-RPC -32000"):
        reader._rpc("eth_chainId", [])
    sent = single_reads(reader, httpx.Response(429, headers={"Retry-After": "31"}))
    with pytest.raises(ethereum.ProviderBusy):
        reader._rpc("eth_chainId", [])
    assert sent == ["eth_chainId"]
    sent = single_reads(reader, httpx.Response(429))
    with pytest.raises(ethereum.ProviderBusy):
        reader._rpc("eth_sendRawTransaction", ["0x00"])
    assert sent == ["eth_sendRawTransaction"] and no_backoff == []


def test_provider_pushback_stops_overlapping_batches(connect, no_backoff, monkeypatch):
    chain = FakeChain(START + 999)
    reader, _ = connect(chain)
    lock, active, overlaps, pushed = threading.Lock(), [0], [], []
    serve = Transport.__call__

    def observed(self, request):
        body = json.loads(request.content)
        if not isinstance(body, list):
            return serve(self, request)
        first = body[0]["params"][0]
        with lock:
            if first == hex(START) and not pushed:
                # The first batch meets the provider's burst limit.
                pushed.append(len(overlaps))
                overlaps.append(0)
                return httpx.Response(429)
            active[0] += 1
            overlaps.append(active[0])
        # Keeps concurrent batches overlapping long enough to observe.
        time.sleep(0.02)
        try:
            return serve(self, request)
        finally:
            with lock:
                active[0] -= 1

    monkeypatch.setattr(Transport, "__call__", observed)
    # Four batches in flight at first; the reader must collapse the window after a 429.
    assert read_headers(reader, START, START + 999, in_flight=4) == expected(
        chain, START, START + 999
    )
    # The three sent beside the rejected one and its re-send may still overlap; every
    # request after them is sent alone.
    assert pushed == [0] and max(overlaps[:5]) > 1
    assert overlaps[5:] == [1] * (len(overlaps) - 5)
    # Ten batches of 100 headers and the one re-send.
    assert len(overlaps) == 10 + 1


def test_create_waits_longer_than_other_gateway_requests(monkeypatch):
    from almanak.gateway import client

    timeouts = []

    class Http:
        def post(self, url, *, headers, json, timeout):
            timeouts.append((url.rsplit("/", 1)[1], timeout))
            return httpx.Response(200, json={"result": {}})

    gateway = client.GatewayClient.__new__(client.GatewayClient)
    gateway.endpoint, gateway.token, gateway.http = "http://127.0.0.1:1", "token", Http()
    config = object()
    monkeypatch.setattr(gateway, "configuration", lambda: config)
    monkeypatch.setattr(client, "checkpoint_from_json", lambda _: SimpleNamespace(config=config))
    gateway.create(config, None)
    gateway.request("state")
    assert timeouts == [("create", client.CREATE_TIMEOUT), ("state", httpx.USE_CLIENT_DEFAULT)]
    assert client.CREATE_TIMEOUT >= 300 and httpx.Client(timeout=60).timeout.read == 60


def header(number, block_hash="0x" + "11" * 32):
    return {"result": {"number": hex(number), "hash": block_hash, "timestamp": "0x1"}}


@pytest.mark.parametrize(
    ("method", "error", "expected"),
    [
        ("eth_getCode", ethereum.ProviderBusy("eth_getCode", None), True),
        ("eth_call", DataError("rpc", "limit", code=-32005), True),
        ("eth_getBalance", DataError("transport", "transport failed"), True),
        ("eth_getLogs", ethereum.ProviderFault("http", "HTTP 503"), True),
        ("eth_getBlockByNumber", DataError("unavailable", "null"), True),
        ("eth_getBlockByHash", DataError("unavailable", "null"), True),
        # A null answer to any other read is data, not provider lag.
        ("eth_call", DataError("unavailable", "null"), False),
        ("eth_getCode", DataError("http", "HTTP 400", code=-32602), False),
        ("eth_call", DataError("rpc", "execution reverted", code=3), False),
        ("eth_getBlockByNumber", DataError("malformed", "bad envelope"), False),
        # Submissions and fork control are never re-sent.
        ("eth_sendRawTransaction", DataError("transport", "transport failed"), False),
        ("eth_sendRawTransaction", ethereum.ProviderBusy("send", None), False),
        ("anvil_setBalance", ethereum.ProviderBusy("anvil", None), False),
    ],
)
def test_transient_read_classification(method, error, expected):
    assert ethereum.transient(method, error) is expected


def test_http_5xx_is_a_provider_fault(connect):
    authority, _ = connect(FakeChain(START))
    single_reads(authority.reader, httpx.Response(502))
    with pytest.raises(ethereum.ProviderFault) as fault:
        authority.reader._rpc("eth_chainId", [])
    # A reader without a retry budget fails at once and does not hold the coordinator.
    assert fault.value.kind == "http" and not fault.value.transient


@pytest.mark.parametrize(
    "failure",
    [
        httpx.Response(429),
        httpx.Response(503),
        {"error": {"code": -32005, "message": "limit exceeded"}},
        {"result": None},
    ],
)
def test_an_execution_reader_waits_out_a_lagging_or_failing_block_read(
    connect, no_backoff, failure
):
    authority, _ = connect(FakeChain(START), retry_budget=ethereum.EXECUTION_RETRY_BUDGET)
    reader = authority.reader
    sent = single_reads(reader, failure, failure, header(START + 1))
    block = reader.pin_block(START + 1)
    assert block.number == START + 1 and reader._pinned == block
    assert no_backoff == [0.5, 1.0] and sent == ["eth_getBlockByNumber"] * 3


def test_a_block_that_stays_unavailable_fails_transient_within_the_execution_budget(
    connect, no_backoff
):
    authority, _ = connect(FakeChain(START), retry_budget=ethereum.EXECUTION_RETRY_BUDGET)
    sent = single_reads(authority.reader, *[{"result": None}] * 10)
    with pytest.raises(DataError) as unavailable:
        authority.reader.pin_block(START + 1)
    assert unavailable.value.kind == "unavailable" and unavailable.value.transient
    # 0.5 + 1 + 2 + 4 fits the 8 s budget; the next 8 s wait does not.
    assert no_backoff == [0.5, 1.0, 2.0, 4.0] and len(sent) == 5
    assert sum(no_backoff) <= ethereum.EXECUTION_RETRY_BUDGET


def test_a_reader_without_a_budget_neither_retries_nor_marks(connect, no_backoff):
    authority, _ = connect(FakeChain(START))
    sent = single_reads(authority.reader, {"result": None})
    with pytest.raises(DataError) as unavailable:
        authority.reader.pin_block(START + 1)
    assert not unavailable.value.transient and sent == ["eth_getBlockByNumber"]
    assert no_backoff == []


def test_a_local_fork_reader_fails_at_once_even_with_a_budget(no_backoff):
    # A crashed or reset anvil must stop a fork run, not hold it.
    reader = EthereumReader(
        MAINNET,
        "http://127.0.0.1:8545",
        client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(503))),
        local_transport=True,
        retry_budget=ethereum.EXECUTION_RETRY_BUDGET,
    )
    with pytest.raises(ethereum.ProviderFault) as fault:
        reader._rpc("eth_chainId", [])
    assert not fault.value.transient and no_backoff == []


def test_a_throttled_batch_that_outlasts_its_bounds_is_transient(connect, no_backoff):
    chain = FakeChain(START + 10)
    reader, transport = connect(chain)
    transport.busy = ethereum.BUSY_RETRIES + 1
    with pytest.raises(ethereum.ProviderBusy) as busy:
        read_headers(reader, START, START + 10)
    assert busy.value.transient


def test_a_throttled_batch_outside_the_read_allowlist_is_not_transient(connect, no_backoff):
    reader, transport = connect(FakeChain(START))
    transport.busy = ethereum.BUSY_RETRIES + 1
    batches = [[("anvil_setBalance", ["0x" + "00" * 20, "0x1"])]]
    with pytest.raises(ethereum.ProviderBusy) as busy:
        list(reader.reader._rpc_batches(batches, 1))
    assert not busy.value.transient


@pytest.mark.parametrize(
    ("code", "message", "expected"),
    [
        (-32000, "header not found", True),
        (-32000, "Unknown block", True),
        (-32001, "Block not found: 0xabc", True),
        (-32000, "execution reverted", False),
        (-32000, "missing trie node", False),
        (-32602, "header not found", False),
    ],
)
def test_a_block_the_backend_has_not_served_is_transient(
    connect, no_backoff, code, message, expected
):
    authority, _ = connect(FakeChain(START), retry_budget=ethereum.EXECUTION_RETRY_BUDGET)
    reader = authority.reader
    error = {"error": {"code": code, "message": message}}
    sent = single_reads(reader, error, {"result": "0x01"})
    pinned = {"blockHash": "0x" + "11" * 32, "requireCanonical": True}
    if expected:
        assert reader._rpc("eth_call", [{"to": "0x" + "00" * 20}, pinned]) == "0x01"
        assert sent == ["eth_call", "eth_call"] and no_backoff == [0.5]
        return
    with pytest.raises(DataError) as refused:
        reader._rpc("eth_call", [{"to": "0x" + "00" * 20}, pinned])
    assert refused.value.kind == "rpc" and not refused.value.transient
    assert sent == ["eth_call"]


def test_a_block_not_served_message_keeps_only_the_code(connect, no_backoff):
    authority, _ = connect(FakeChain(START))
    secret = "header not found at https://base.example/v2/KEY"
    single_reads(authority.reader, {"error": {"code": -32000, "message": secret}})
    with pytest.raises(ethereum.BlockNotServed) as lagging:
        authority.reader._rpc("eth_getBalance", ["0x" + "00" * 20, "latest"])
    assert "KEY" not in str(lagging.value) and lagging.value.code == -32000


def test_retries_stop_at_the_gateway_request_read_deadline(connect, no_backoff):
    """Each read has its own budget, but no wait may end past the request's deadline."""
    from almanak.gateway.request_scope import request_scope

    authority, _ = connect(FakeChain(START), retry_budget=ethereum.EXECUTION_RETRY_BUDGET)
    reader = authority.reader
    throttle = httpx.Response(429)
    with request_scope() as scope:
        # The fake clock is the sum of waits; the request may wait 10 s in total.
        scope.read_deadline = 10.0
        # The first read waits 0.5 + 1 + 2 + 4 within its own budget, then passes.
        sent = single_reads(reader, *[throttle] * 4)
        assert reader._rpc("eth_chainId", []) == "0x1" and len(sent) == 5
        # The next read has 2.5 s of the request left: it waits 0.5 and 1, then the
        # 2 s wait would end past the deadline, so it fails transient at once.
        sent = single_reads(reader, *[throttle] * 10)
        with pytest.raises(ethereum.ProviderBusy) as busy:
            reader._rpc("eth_chainId", [])
    assert busy.value.transient and len(sent) == 3
    assert no_backoff == [0.5, 1.0, 2.0, 4.0, 0.5, 1.0]
    assert sum(no_backoff) <= 10.0


def test_a_re_send_ends_by_the_read_deadline_and_a_first_attempt_keeps_its_timeout(
    connect, no_backoff
):
    """A re-send's provider timeout is what the request has left; none is sent past it."""
    from almanak.gateway.request_scope import request_scope

    authority, _ = connect(FakeChain(START), retry_budget=ethereum.EXECUTION_RETRY_BUDGET)
    reader = authority.reader
    timeouts = []

    def answer(*answers):
        sent = single_reads(reader, *answers)
        post = reader._client.post

        def recording(*args, **kwargs):
            timeouts.append(kwargs["timeout"])
            return post(*args, **kwargs)

        reader._client.post = recording
        return sent

    throttle = httpx.Response(429)
    with request_scope() as scope:
        # The fake clock is the sum of waits; 4 s of the request remain after the first.
        scope.read_deadline = 4.5
        answer(throttle)
        assert reader._rpc("eth_chainId", []) == "0x1"
        assert timeouts == [httpx.USE_CLIENT_DEFAULT, 4.0]
        # A request already past its deadline reads once, then fails transient at once.
        scope.read_deadline = 0.0
        timeouts.clear()
        sent = answer(throttle, throttle)
        with pytest.raises(ethereum.ProviderBusy) as busy:
            reader._rpc("eth_chainId", [])
    assert busy.value.transient and len(sent) == 1 and timeouts == [httpx.USE_CLIENT_DEFAULT]


@pytest.fixture
def clock(monkeypatch):
    """A fake clock that both waits and provider attempts advance; nothing sleeps."""
    now, waits = [0.0], []

    def wait(seconds):
        waits.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(ethereum, "sleep", wait)
    monkeypatch.setattr(ethereum, "monotonic", lambda: now[0])
    return now, waits


def timed_reads(reader, now, *attempts):
    """Answer a reader's single reads in turn; each attempt is (seconds taken, answer).

    An answer of None is a provider timeout. `sent` records each attempt's method and
    the read timeout the client gave it.
    """
    sent, left = [], list(attempts)

    def respond(request):
        body = json.loads(request.content)
        sent.append((body["method"], request.extensions["timeout"]["read"]))
        took, answer = left.pop(0)
        now[0] += took
        if answer is None:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], **answer})

    reader._client = httpx.Client(
        transport=httpx.MockTransport(respond), timeout=ethereum.PROVIDER_TIMEOUT
    )
    return sent


TIMEOUT = ethereum.PROVIDER_TIMEOUT
BUDGET = ethereum.EXECUTION_RETRY_BUDGET


def test_a_read_that_timed_out_is_sent_once_more(connect, clock):
    """The timeout outlasts the execution budget, and the read still gets one re-send."""
    now, waits = clock
    authority, _ = connect(FakeChain(START), retry_budget=BUDGET)
    sent = timed_reads(authority.reader, now, (TIMEOUT, None), (0.2, {"result": "0x1"}))
    assert authority.reader._rpc("eth_chainId", []) == "0x1"
    # The re-send follows the first backoff and may take the budget, not the full timeout.
    assert sent == [("eth_chainId", TIMEOUT), ("eth_chainId", BUDGET)]
    assert waits == [ethereum.BUSY_BACKOFF]


def test_a_read_that_keeps_timing_out_ends_within_its_stated_bound(connect, clock):
    now, waits = clock
    authority, _ = connect(FakeChain(START), retry_budget=BUDGET)
    attempts = [(TIMEOUT, None), (BUDGET, None), (0.1, {"result": "0x1"})]
    sent = timed_reads(authority.reader, now, *attempts)
    with pytest.raises(DataError) as failed:
        authority.reader._rpc("eth_chainId", [])
    assert failed.value.kind == "transport" and failed.value.transient
    assert len(sent) == 2 and waits == [ethereum.BUSY_BACKOFF]
    assert now[0] == TIMEOUT + ethereum.BUSY_BACKOFF + BUDGET == 23.5


@pytest.mark.parametrize(
    ("took", "resend"),
    [
        # Budget left for the first backoff: the bounds are the ones a quick failure has.
        (BUDGET - ethereum.BUSY_BACKOFF, TIMEOUT),
        # Less than the first backoff left: the one re-send, bounded by the budget.
        (BUDGET - ethereum.BUSY_BACKOFF + 0.1, BUDGET),
    ],
)
def test_only_a_first_attempt_that_spent_the_budget_gets_the_bounded_re_send(
    connect, clock, took, resend
):
    now, waits = clock
    authority, _ = connect(FakeChain(START), retry_budget=BUDGET)
    sent = timed_reads(authority.reader, now, (took, None), (0.1, {"result": "0x1"}))
    assert authority.reader._rpc("eth_chainId", []) == "0x1"
    assert [timeout for _, timeout in sent] == [TIMEOUT, resend]
    assert waits == [ethereum.BUSY_BACKOFF]


def test_a_later_attempt_past_the_budget_is_not_sent_again(connect, clock):
    """The one re-send belongs to the first attempt; a later slow failure ends the read."""
    now, waits = clock
    authority, _ = connect(FakeChain(START), retry_budget=BUDGET)
    attempts = [(1.0, None), (TIMEOUT, None), (0.1, {"result": "0x1"})]
    sent = timed_reads(authority.reader, now, *attempts)
    with pytest.raises(DataError) as failed:
        authority.reader._rpc("eth_chainId", [])
    assert failed.value.transient and len(sent) == 2
    assert [timeout for _, timeout in sent] == [TIMEOUT, TIMEOUT]


def test_a_submission_that_timed_out_is_never_sent_again(connect, clock):
    now, waits = clock
    authority, _ = connect(FakeChain(START), retry_budget=BUDGET)
    sent = timed_reads(authority.reader, now, (TIMEOUT, None), (0.1, {"result": "0x1"}))
    with pytest.raises(DataError) as failed:
        authority.reader._rpc("eth_sendRawTransaction", ["0x00"])
    assert failed.value.kind == "transport" and not failed.value.transient
    assert len(sent) == 1 and waits == []


def test_a_reader_without_a_budget_does_not_re_send_a_timeout(connect, clock):
    now, waits = clock
    authority, _ = connect(FakeChain(START))
    sent = timed_reads(authority.reader, now, (TIMEOUT, None), (0.1, {"result": "0x1"}))
    with pytest.raises(DataError) as failed:
        authority.reader._rpc("eth_chainId", [])
    assert not failed.value.transient and len(sent) == 1 and waits == []


@pytest.mark.parametrize(
    ("deadline", "timeouts"),
    [
        # 4.5 s of the request remain after the backoff, less than the budget.
        (TIMEOUT + 5.0, [TIMEOUT, 4.5]),
        # The backoff would end past the request's deadline: nothing is re-sent.
        (TIMEOUT + 0.2, [TIMEOUT]),
    ],
)
def test_the_re_send_after_a_timeout_ends_by_the_read_deadline(connect, clock, deadline, timeouts):
    from almanak.gateway.request_scope import request_scope

    now, waits = clock
    authority, _ = connect(FakeChain(START), retry_budget=BUDGET)
    sent = timed_reads(authority.reader, now, (TIMEOUT, None), (4.5, None))
    with request_scope() as scope:
        scope.read_deadline = deadline
        with pytest.raises(DataError) as failed:
            authority.reader._rpc("eth_chainId", [])
    assert failed.value.transient and [timeout for _, timeout in sent] == timeouts
    assert now[0] <= deadline or len(sent) == 1


def test_pruned_state_is_a_named_refusal_and_is_not_sent_again(connect, no_backoff):
    authority, _ = connect(FakeChain(START), retry_budget=BUDGET)
    message = "missing trie node 5f3a (path ) state 0x9c is not available at https://x/KEY"
    sent = single_reads(authority.reader, {"error": {"code": -32000, "message": message}})
    pinned = {"blockHash": "0x" + "11" * 32, "requireCanonical": True}
    with pytest.raises(DataError) as refused:
        authority.reader._rpc("eth_call", [{"to": "0x" + "00" * 20}, pinned])
    assert not isinstance(refused.value, ethereum.BlockNotServed)
    assert refused.value.kind == "rpc" and not refused.value.transient
    assert str(refused.value) == (
        "eth_call: RPC rejected the requested read"
        " (JSON-RPC -32000: the endpoint does not serve state at that block)"
    )
    assert sent == ["eth_call"] and no_backoff == []


def test_an_empty_batch_sends_nothing(connect):
    """JSON-RPC 2.0 refuses an empty batch, so no calls answer with no results."""
    authority, transport = connect(FakeChain(START))
    assert authority.reader.batch([]) == []
    assert not transport.methods and transport.sizes == []
