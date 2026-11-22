"""Read-only observation gateway: served reads, the client reader and the CLI seam."""

import json
import secrets
import threading
from types import SimpleNamespace

import httpx
import pytest

from almanak import ax, ax_lending_markets
from almanak.ax_lending_markets import MarketQuery
from almanak.caip import ChainId
from almanak.gateway import observation_gateway
from almanak.gateway.auth import loopback_endpoint
from almanak.gateway.ethereum import (
    DataError,
    EthereumReader,
    ProviderBusy,
    Reverted,
    throttled,
)
from almanak.gateway.observation_gateway import (
    GatewayReader,
    GatewayRequest,
    ObservationGateway,
    ObservationRead,
    gateway_reader,
)
from almanak.gateway.service import http_server
from almanak.models import Chain

ARBITRUM = ChainId("eip155:42161")
CHAIN = Chain(chain_id=ARBITRUM, environment="mainnet")
BLOCK_NUMBER = 25908472
BLOCK_HASH = "0x" + "ab" * 32
TARGET = "0x" + "42" * 20


class FakeProvider(EthereumReader):
    """Stands in for the Alchemy transport inside the gateway process."""

    def __init__(self, chain_hex="0xa4b1"):
        super().__init__(CHAIN, "http://provider.invalid")
        self.chain_hex = chain_hex
        self.calls: list[tuple[str, list]] = []

    def _rpc(self, method, params, *, allow_none=False):
        self.calls.append((method, params))
        if method == "eth_chainId":
            return self.chain_hex
        if method == "eth_getBlockByNumber":
            return {"number": hex(BLOCK_NUMBER), "hash": BLOCK_HASH, "timestamp": "0x6553f100"}
        if method == "eth_call":
            return "0x" + (7).to_bytes(32, "big").hex()
        if method == "eth_getBalance":
            return None
        if method == "eth_getLogs":
            raise DataError("rate_limit", "provider allowance exhausted")
        raise AssertionError(f"unexpected provider read {method}")


@pytest.fixture
def token_file(tmp_path):
    path = tmp_path / "read-gateway.token"
    path.write_text(secrets.token_hex(32))
    path.chmod(0o600)
    return path


@pytest.fixture
def provider(monkeypatch):
    fake = FakeProvider()
    monkeypatch.setattr(observation_gateway, "verified_reader", lambda chain, env_file: fake)
    return fake


@pytest.fixture
def served(tmp_path, token_file, provider):
    gateway = ObservationGateway(tmp_path / "operator.env")
    server = http_server(
        gateway, token_file, 0, request_type=GatewayRequest, fields={"read", "lookup"}
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", gateway
    finally:
        server.shutdown()
        server.server_close()
        gateway.close()


def test_reads_flow_through_the_gateway_with_one_verified_transport(served, token_file, provider):
    endpoint, gateway = served
    with gateway_reader(ARBITRUM, endpoint, token_file) as reader:
        block = reader.pin_block(BLOCK_NUMBER)
        assert block.hash == BLOCK_HASH
        assert reader.call(TARGET, "totalSupply()", block) == (7,)
        assert reader._rpc("eth_getBalance", [TARGET], allow_none=True) is None
        with pytest.raises(DataError, match="unavailable"):
            reader._rpc("eth_getBalance", [TARGET])
    assert [method for method, _ in provider.calls] == [
        "eth_chainId",
        "eth_getBlockByNumber",
        "eth_call",
        "eth_getBalance",
        "eth_getBalance",
    ]
    assert list(gateway.readers) == [ARBITRUM]


def test_gateway_failures_keep_their_category(served, token_file):
    endpoint, _ = served
    with gateway_reader(ARBITRUM, endpoint, token_file) as reader:
        with pytest.raises(DataError) as limited:
            reader._rpc("eth_getLogs", [{}])
        assert limited.value.kind == "rate_limit"
        with pytest.raises(DataError) as unsupported:
            reader._rpc("eth_sendRawTransaction", ["0x00"])
        assert unsupported.value.kind == "unsupported"


def test_gateway_refuses_bad_tokens_and_other_chains(served, token_file, tmp_path):
    endpoint, _ = served
    wrong = tmp_path / "wrong.token"
    wrong.write_text(secrets.token_hex(32))
    wrong.chmod(0o600)
    with pytest.raises(DataError) as refused:
        gateway_reader(ARBITRUM, endpoint, wrong)
    assert refused.value.kind == "unauthorized"
    with pytest.raises(DataError, match="chain"):
        gateway_reader(ChainId("eip155:1"), endpoint, token_file)


def test_client_verifies_the_answered_request(monkeypatch, token_file):
    reader = GatewayReader(CHAIN, "http://127.0.0.1:1", "ab" * 32)
    other = ObservationRead(chain=ARBITRUM, method="eth_chainId", params=("latest",))
    monkeypatch.setattr(
        reader,
        "send",
        lambda request: observation_gateway.ObservationResult(request=other, result="0xa4b1"),
    )
    with pytest.raises(DataError, match="different read"):
        reader._rpc("eth_chainId", [])


def test_client_reports_unreachable_and_malformed_gateways(served, token_file):
    endpoint, _ = served
    unreachable = GatewayReader(CHAIN, "http://127.0.0.1:1", "ab" * 32)
    with pytest.raises(DataError) as failed:
        unreachable._rpc("eth_chainId", [])
    assert failed.value.kind == "gateway_unavailable"

    class Response:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    class Client:
        def __init__(self, payload):
            self.payload = payload

        def post(self, *args, **kwargs):
            return Response(self.payload)

    for payload in (["not", "a", "dict"], {"result": {"request": "nope"}}, {}):
        reader = GatewayReader(CHAIN, endpoint, "ab" * 32)
        reader._client = Client(payload)
        with pytest.raises(DataError) as malformed:
            reader._rpc("eth_chainId", [])
        assert malformed.value.kind == "malformed"


def test_every_reply_logs_its_operation_status_and_duration(served, token_file, capsys):
    endpoint, _ = served
    with gateway_reader(ARBITRUM, endpoint, token_file) as reader:
        with pytest.raises(DataError):
            reader._rpc("eth_getLogs", [{}])
    with pytest.raises(DataError):
        gateway_reader(ARBITRUM, endpoint, foreign_token(token_file))
    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert [(line["operation"], line["status"]) for line in lines] == [
        ("read", 200),
        ("read", 429),
        ("read", 401),
    ]
    for line in lines:
        # Request diagnostics add only counts, durations and whitelisted method names.
        diagnostics = set(line) - {"event", "operation", "status", "duration_ms"}
        assert diagnostics <= {"rpc_calls", "rpc_round_trips", "rpc_ms", "rpc"}
        assert set(line.get("rpc", {})) <= {"eth_getLogs"}
        assert line["event"] == "gateway_request"
        assert isinstance(line["duration_ms"], float) and line["duration_ms"] >= 0


def foreign_token(token_file):
    other = token_file.with_name("other.token")
    other.write_text(secrets.token_hex(32))
    other.chmod(0o600)
    return other


def test_gateway_serves_only_its_operations(tmp_path, provider):
    gateway = ObservationGateway(tmp_path / "operator.env")
    request = GatewayRequest(ObservationRead(chain=ARBITRUM, method="eth_chainId", params=()))
    # Each operation answers only its own request shape.
    for method in ("sign", "lookup", "info"):
        with pytest.raises(ValueError, match="unsupported"):
            gateway.dispatch(method, request)
    assert gateway.dispatch("read", request) == {
        "request": {"chain": "eip155:42161", "method": "eth_chainId", "params": []},
        "result": "0xa4b1",
    }


def test_loopback_endpoint_is_required():
    assert loopback_endpoint("http://127.0.0.1:8300/") == "http://127.0.0.1:8300"
    for endpoint in (
        "https://127.0.0.1:8300",
        "http://localhost:8300",
        "http://127.0.0.1",
        "http://127.0.0.1:8300/read",
        "http://user@127.0.0.1:8300",
    ):
        with pytest.raises(ValueError, match="loopback"):
            loopback_endpoint(endpoint)


def test_command_serves_until_interrupted(monkeypatch, tmp_path, capsys):
    events = []

    class Server:
        server_port = 8300

        def serve_forever(self):
            events.append("serving")
            raise KeyboardInterrupt

        def server_close(self):
            events.append("closed")

    def fake_server(gateway, token_file, port, *, request_type, fields):
        assert isinstance(gateway, ObservationGateway)
        assert (token_file, port, request_type, fields) == (
            tmp_path / "token",
            0,
            GatewayRequest,
            {"read", "lookup", "info"},
        )
        return Server()

    monkeypatch.setattr(observation_gateway, "http_server", fake_server)
    args = SimpleNamespace(
        env_file=tmp_path / "operator.env", auth_token_file=tmp_path / "token", port=0
    )
    for port in (-1, 65536):
        with pytest.raises(ValueError, match="between 0 and 65535"):
            observation_gateway.command(SimpleNamespace(**{**vars(args), "port": port}))
    assert events == []
    # An invalid provider cap stops the gateway before it serves anything.
    for variable in (
        "ALMANAK_GATEWAY_MORPHO_REQUESTS_PER_MINUTE",
        "ALMANAK_GATEWAY_HYPERLIQUID_WEIGHT_PER_MINUTE",
        "ALMANAK_GATEWAY_RPC_REQUESTS_PER_SECOND",
    ):
        with monkeypatch.context() as invalid:
            invalid.setenv(variable, "1.5")
            with pytest.raises(ValueError, match=f"{variable} must be a non-negative integer"):
                observation_gateway.command(args)
    assert events == []
    assert observation_gateway.command(args) == 0
    assert events == ["serving", "closed"]
    assert json.loads(capsys.readouterr().out) == {
        "event": "gateway_ready",
        "port": 8300,
        "authority": "read_only",
    }


def test_ax_picks_the_transport_from_the_invocation(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.setattr(
        ax, "verified_reader", lambda chain, env_file: seen.setdefault("local", (chain, env_file))
    )
    monkeypatch.setattr(
        ax,
        "gateway_reader",
        lambda chain, url, token: seen.setdefault("gateway", (chain, url, token)),
    )
    local = SimpleNamespace(env_file=tmp_path / "op.env", gateway_url=None, gateway_auth_file=None)
    ax.observation_reader(local, ARBITRUM)
    assert seen["local"] == (ARBITRUM, tmp_path / "op.env")
    hosted = SimpleNamespace(
        env_file=None, gateway_url="http://127.0.0.1:8300", gateway_auth_file=tmp_path / "token"
    )
    ax.observation_reader(hosted, ARBITRUM)
    assert seen["gateway"] == (ARBITRUM, "http://127.0.0.1:8300", tmp_path / "token")
    bare = SimpleNamespace(
        env_file=None, gateway_url="http://127.0.0.1:8300", gateway_auth_file=None
    )
    with pytest.raises(ValueError, match="--gateway-auth-file"):
        ax.observation_reader(bare, ARBITRUM)


def pinned_reads(reader, tag):
    block = reader.pin_block(tag)
    assert (block.number, block.hash) == (BLOCK_NUMBER, BLOCK_HASH)
    assert reader.call(TARGET, "totalSupply()", block) == (7,)


@pytest.mark.parametrize("tag", ["latest", "finalized"])
def test_ax_block_tags_pin_one_concrete_header_through_the_gateway(
    served, token_file, provider, tag
):
    endpoint, _ = served
    with gateway_reader(ARBITRUM, endpoint, token_file) as reader:
        pinned_reads(reader, tag)
    # The gateway forwards the tag once; every later read names the pinned hash.
    assert provider.calls[1:] == [
        ("eth_getBlockByNumber", [tag, False]),
        (
            "eth_call",
            [
                {"to": TARGET, "data": provider.calls[2][1][0]["data"]},
                {"blockHash": BLOCK_HASH, "requireCanonical": True},
            ],
        ),
    ]


@pytest.mark.parametrize("tag", ["latest", "finalized"])
def test_ax_block_tags_pin_one_concrete_header_on_a_direct_provider(tag):
    requests = []

    def handle(request):
        body = json.loads(request.content)
        requests.append((body["method"], body["params"]))
        replies = {
            "eth_chainId": "0xa4b1",
            "eth_getBlockByNumber": {
                "number": hex(BLOCK_NUMBER),
                "hash": BLOCK_HASH,
                "timestamp": "0x6553f100",
            },
            "eth_call": "0x" + (7).to_bytes(32, "big").hex(),
        }
        return httpx.Response(
            200, json={"jsonrpc": "2.0", "id": body["id"], "result": replies[body["method"]]}
        )

    client = httpx.Client(transport=httpx.MockTransport(handle))
    with EthereumReader(CHAIN, "https://rpc.invalid", client=client) as reader:
        pinned_reads(reader, tag)
    client.close()
    assert requests[1] == ("eth_getBlockByNumber", [tag, False])
    assert requests[2][1][1] == {"blockHash": BLOCK_HASH, "requireCanonical": True}


@pytest.mark.parametrize("code", [3, -32000])
def test_a_contract_revert_keeps_its_verdict_and_code_through_the_gateway(
    served, token_file, provider, monkeypatch, code
):
    # A Spoke without reserves reverts getReserveCount(), under JSON-RPC 3 or, without
    # data, -32000; a listing through the gateway must still tell that from a read
    # that failed.
    endpoint, _ = served
    treasury = "0x" + "52" * 20
    answer = provider._rpc

    def reverting(method, params, *, allow_none=False):
        if method == "eth_call" and params[0]["to"].lower() == treasury:
            raise Reverted(
                f"eth_call: RPC rejected the requested read (JSON-RPC {code})", None, code
            )
        return answer(method, params, allow_none=allow_none)

    monkeypatch.setattr(provider, "_rpc", reverting)
    monkeypatch.setattr(ax_lending_markets, "hubs_for", lambda chain: (TARGET,))
    monkeypatch.setattr(ax_lending_markets, "served_spokes", lambda *_: ([treasury], []))
    with gateway_reader(ARBITRUM, endpoint, token_file) as reader:
        block = reader.pin_block(BLOCK_NUMBER)
        with pytest.raises(DataError) as reverted:
            reader.call(treasury, "getReserveCount()", block)
        assert (reverted.value.kind, reverted.value.code) == ("rpc", code)
        assert reverted.value.reverted is True
        query = MarketQuery(connector="aave_v4", asset=None, collateral=None, limit=10)
        source = ax_lending_markets.spoke_reserves(reader, None, block, query).source
    assert (source["listed"], source["without_reserves"], source["unreadable"]) == (0, 1, [])


def test_a_refused_read_under_the_revert_code_is_no_revert_through_the_gateway(
    served, token_file, provider, monkeypatch
):
    # -32000 also answers a read the node could not serve; that Spoke stays unread.
    endpoint, _ = served
    spoke = "0x" + "52" * 20
    answer = provider._rpc
    message = "eth_call: RPC rejected the requested read (JSON-RPC -32000)"

    def refusing(method, params, *, allow_none=False):
        if method == "eth_call" and params[0]["to"].lower() == spoke:
            raise DataError("rpc", message, code=-32000)
        return answer(method, params, allow_none=allow_none)

    monkeypatch.setattr(provider, "_rpc", refusing)
    monkeypatch.setattr(ax_lending_markets, "hubs_for", lambda chain: (TARGET,))
    monkeypatch.setattr(ax_lending_markets, "served_spokes", lambda *_: ([spoke], []))
    with gateway_reader(ARBITRUM, endpoint, token_file) as reader:
        block = reader.pin_block(BLOCK_NUMBER)
        with pytest.raises(DataError) as refused:
            reader.call(spoke, "getReserveCount()", block)
        assert (refused.value.code, refused.value.reverted) == (-32000, False)
        query = MarketQuery(connector="aave_v4", asset=None, collateral=None, limit=10)
        source = ax_lending_markets.spoke_reserves(reader, None, block, query).source
    assert (source["listed"], source["without_reserves"]) == (0, 0)
    assert source["unreadable"] == [
        {"spoke": spoke, "status": "refused", "reason": "rpc: " + message}
    ]


def test_a_provider_throttle_keeps_its_code_through_the_gateway(
    served, token_file, provider, monkeypatch
):
    endpoint, _ = served
    answer = provider._rpc

    def busy(method, params, *, allow_none=False):
        if method == "eth_call":
            raise ProviderBusy(method, None)
        return answer(method, params, allow_none=allow_none)

    monkeypatch.setattr(provider, "_rpc", busy)
    with gateway_reader(ARBITRUM, endpoint, token_file) as reader:
        block = reader.pin_block(BLOCK_NUMBER)
        with pytest.raises(DataError) as throttle:
            reader.call(TARGET, "getReserveCount()", block)
    assert (throttle.value.kind, throttle.value.code) == ("http", 429)
    assert throttled(throttle.value)


def test_a_gateway_refusal_code_must_be_an_integer(monkeypatch):
    reader = GatewayReader(CHAIN, "http://127.0.0.1:1", "ab" * 32)

    class Response:
        status_code = 409

        def __init__(self, code):
            self.code = code

        def json(self):
            return {"error": "rpc", "message": "eth_call: refused", "code": self.code}

    for code, kind in ((3, "rpc"), ("3", "malformed"), (True, "malformed")):
        monkeypatch.setattr(reader._client, "post", lambda *a, code=code, **k: Response(code))
        with pytest.raises(DataError) as refused:
            reader._rpc("eth_chainId", [])
        assert refused.value.kind == kind
        assert refused.value.code == (3 if kind == "rpc" else None)
