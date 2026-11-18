"""CoinGecko on-chain lookups: closed request shapes, vendor answers and both transports."""

import secrets
import threading

import httpx
import pytest

from almanak.caip import ChainId
from almanak.gateway import onchain_lookup
from almanak.gateway.ethereum import DataError
from almanak.gateway.observation_gateway import GatewayRequest, ObservationGateway
from almanak.gateway.onchain_lookup import (
    GatewayLookups,
    LookupRequest,
    LookupResult,
    OnchainLookups,
    PoolSearch,
    TokenPools,
    Tokens,
    VendorLookups,
    answered,
    document,
    endpoint,
    network,
)
from almanak.gateway.service import http_server

BASE = ChainId("eip155:8453")
WETH = "0x4200000000000000000000000000000000000006"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def test_each_lookup_names_one_fixed_vendor_path():
    assert endpoint(TokenPools(chain=BASE, token=WETH, page=2)) == (
        f"/networks/base/tokens/{WETH}/pools",
        {"page": "2"},
    )
    assert endpoint(PoolSearch(chain=BASE, query="cbBTC")) == (
        "/search/pools",
        {"query": "cbBTC", "network": "base", "page": "1"},
    )
    assert endpoint(Tokens(chain=BASE, tokens=(WETH, USDC))) == (
        f"/networks/base/tokens/multi/{WETH},{USDC}",
        {},
    )


def test_lookups_refuse_shapes_that_could_reach_other_paths():
    for query in ("../pools", "a/b", "x?y=1", "", " leading"):
        with pytest.raises(ValueError):
            PoolSearch(chain=BASE, query=query)
    with pytest.raises(ValueError):
        TokenPools(chain=BASE, token="0x12", page=1)
    with pytest.raises(ValueError):
        TokenPools(chain=BASE, token=WETH, page=11)
    with pytest.raises(ValueError):
        Tokens(chain=BASE, tokens=(WETH,) * 31)
    with pytest.raises(DataError, match="no CoinGecko on-chain network"):
        network(ChainId("eip155:31337"))


def respond(status, body=None, text=None):
    request = httpx.Request("GET", onchain_lookup.API)
    if text is not None:
        return httpx.Response(status, text=text, request=request)
    return httpx.Response(status, json=body, request=request)


def test_vendor_answers_keep_their_category():
    assert document(respond(200, {"data": [{"id": "x"}]})) == {"data": [{"id": "x"}]}
    # A 404 is the vendor's missing data: marked not indexed, never a measured empty list.
    assert document(respond(404, {"errors": []})) == {"data": [], "indexed": False}
    cases = [
        (respond(429, {}), "rate_limit"),
        (respond(500, {}), "unavailable"),
        (respond(200, text="not json"), "malformed"),
        (respond(200, {"data": {}}), "malformed"),
        (respond(200, []), "malformed"),
    ]
    for response, kind in cases:
        with pytest.raises(DataError) as raised:
            document(response)
        assert raised.value.kind == kind


@pytest.fixture
def env_file(tmp_path):
    path = tmp_path / "operator.env"
    path.write_text("COINGECKO_API_KEY=test-key\n")
    return path


def test_direct_lookups_require_the_key(tmp_path):
    empty = tmp_path / "empty.env"
    empty.write_text("ALCHEMY_API_KEY=x\n")
    with pytest.raises(ValueError, match="COINGECKO_API_KEY is required"):
        OnchainLookups(empty)


def test_direct_lookups_send_the_key_to_the_fixed_host_only(env_file, monkeypatch):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"data": []})

    with OnchainLookups(env_file) as lookups:
        lookups.client = httpx.Client(transport=httpx.MockTransport(handler))
        request = LookupRequest(lookup=TokenPools(chain=BASE, token=WETH))
        assert lookups.fetch(request) == LookupResult(request=request, result={"data": []})
    (sent,) = seen
    assert str(sent.url) == f"{onchain_lookup.API}/networks/base/tokens/{WETH}/pools?page=1"
    assert sent.headers["x-cg-pro-api-key"] == "test-key"


def test_direct_lookups_report_transport_failures(env_file):
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    with OnchainLookups(env_file) as lookups:
        lookups.client = httpx.Client(transport=httpx.MockTransport(handler))
        with pytest.raises(DataError, match="request failed"):
            lookups.fetch(LookupRequest(lookup=Tokens(chain=BASE, tokens=(WETH,))))


class FakeLookups:
    def __init__(self):
        self.requests = []

    def fetch(self, request):
        self.requests.append(request)
        if request.lookup.kind == "tokens":
            raise DataError("rate_limit", "coingecko_onchain: vendor rate limit")
        return LookupResult(request=request, result={"data": [{"id": "pool"}]})

    def close(self):
        pass


@pytest.fixture
def served(tmp_path, monkeypatch):
    token = tmp_path / "read-gateway.token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    fake = FakeLookups()
    gateway = ObservationGateway(tmp_path / "operator.env")
    gateway.lookups = fake
    server = http_server(gateway, token, 0, request_type=GatewayRequest, fields={"read", "lookup"})
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", token.read_text(), fake
    finally:
        server.shutdown()
        server.server_close()


def test_lookups_flow_through_the_read_gateway(served):
    url, token, fake = served
    request = LookupRequest(lookup=PoolSearch(chain=BASE, query="WETH"))
    with GatewayLookups(url, token) as lookups:
        assert lookups.fetch(request).result == {"data": [{"id": "pool"}]}
        with pytest.raises(DataError) as raised:
            lookups.fetch(LookupRequest(lookup=Tokens(chain=BASE, tokens=(WETH,))))
    assert raised.value.kind == "rate_limit"
    assert fake.requests[0] == request


def test_morpho_lookups_round_trip_through_the_read_gateway(served):
    from almanak.gateway.morpho_lookup import MorphoMarkets

    url, token, fake = served
    request = LookupRequest(lookup=MorphoMarkets(chain=BASE, loan=USDC, collateral=WETH, skip=20))
    with GatewayLookups(url, token) as lookups:
        assert lookups.fetch(request).request == request
    assert fake.requests[-1] == request


def test_gateway_client_refuses_bad_answers(served):
    url, _, _ = served
    request = LookupRequest(lookup=PoolSearch(chain=BASE, query="WETH"))
    with GatewayLookups(url, "wrong-token") as lookups, pytest.raises(DataError) as raised:
        lookups.fetch(request)
    assert raised.value.kind == "unauthorized"
    with GatewayLookups("http://127.0.0.1:9", "token") as lookups, pytest.raises(DataError) as down:
        lookups.fetch(request)
    assert down.value.kind == "gateway_unavailable"
    other = LookupRequest(lookup=PoolSearch(chain=BASE, query="USDC"))
    with pytest.raises(DataError, match="different lookup"):
        answered(request, {"request": other.model_dump(mode="json"), "result": {}})
    for result in (None, {"request": {}}, "text"):
        with pytest.raises(DataError, match="invalid gateway response"):
            answered(request, result)


def test_a_gateway_without_the_key_names_it_and_still_serves_reads(tmp_path):
    env = tmp_path / "operator.env"
    env.write_text("ALCHEMY_API_KEY=x\n")
    gateway = ObservationGateway(env)
    request = GatewayRequest(LookupRequest(lookup=PoolSearch(chain=BASE, query="WETH")))
    with pytest.raises(DataError) as raised:
        gateway.dispatch("lookup", request)
    assert raised.value.kind == "unavailable"
    assert "COINGECKO_API_KEY is required" in str(raised.value)
    gateway.close()


def test_gateway_reads_the_vendor_key_once_and_only_for_lookups(tmp_path, env_file, monkeypatch):
    built = []
    monkeypatch.setattr(
        "almanak.gateway.onchain_lookup.OnchainLookups",
        lambda path: built.append(path) or FakeLookups(),
    )
    gateway = ObservationGateway(env_file)
    request = GatewayRequest(LookupRequest(lookup=PoolSearch(chain=BASE, query="WETH")))
    assert gateway.dispatch("lookup", request)["result"] == {"data": [{"id": "pool"}]}
    gateway.dispatch("lookup", request)
    assert built == [env_file]
    gateway.close()


def test_vendor_lookups_route_each_kind_and_need_the_key_only_for_coingecko(tmp_path, monkeypatch):
    from almanak.gateway.morpho_lookup import MorphoMarkets

    env = tmp_path / "operator.env"
    env.write_text("ALCHEMY_API_KEY=x\n")
    answered_by = []

    class Morpho:
        def fetch(self, lookup):
            answered_by.append(lookup.kind)
            return {"pageInfo": {"countTotal": 0}, "items": []}

        def close(self):
            answered_by.append("closed")

    monkeypatch.setattr(onchain_lookup, "MorphoApi", Morpho)
    with VendorLookups(env) as lookups:
        morpho = LookupRequest(lookup=MorphoMarkets(chain=BASE, loan=USDC))
        assert lookups.fetch(morpho).result == {"pageInfo": {"countTotal": 0}, "items": []}
        with pytest.raises(DataError) as missing:
            lookups.fetch(LookupRequest(lookup=PoolSearch(chain=BASE, query="WETH")))
    assert missing.value.kind == "unavailable"
    assert "COINGECKO_API_KEY is required" in str(missing.value)
    assert answered_by == ["morpho_markets", "closed"]
    # An invalid Morpho cap is the gateway's configuration, named as such.
    monkeypatch.undo()
    monkeypatch.setenv("ALMANAK_GATEWAY_MORPHO_REQUESTS_PER_MINUTE", "1.5")
    with VendorLookups(env) as lookups, pytest.raises(DataError) as invalid:
        lookups.fetch(LookupRequest(lookup=MorphoMarkets(chain=BASE, loan=USDC)))
    assert invalid.value.kind == "unavailable"
    assert str(invalid.value) == (
        "lookup: ALMANAK_GATEWAY_MORPHO_REQUESTS_PER_MINUTE must be a non-negative integer"
    )


def test_coingecko_paths_are_never_built_for_another_vendor():
    from almanak.gateway.morpho_lookup import MorphoMarkets

    with pytest.raises(ValueError, match="not a CoinGecko on-chain lookup"):
        endpoint(MorphoMarkets(chain=BASE, loan=USDC))


def test_a_chain_the_vendor_lacks_keeps_its_reason_through_the_gateway(tmp_path, monkeypatch):
    # The real endpoint() runs inside the gateway; its refusal must not become a bare 400.
    class Routed:
        def fetch(self, request):
            onchain_lookup.endpoint(request.lookup)
            raise AssertionError("an unsupported chain never reaches the vendor")

        def close(self):
            pass

    token = tmp_path / "read-gateway.token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    gateway = ObservationGateway(tmp_path / "operator.env")
    monkeypatch.setattr(gateway, "lookups", Routed())
    server = http_server(gateway, token, 0, request_type=GatewayRequest, fields={"read", "lookup"})
    threading.Thread(target=server.serve_forever, daemon=True).start()
    lookup = TokenPools(chain=ChainId("eip155:31337"), token=WETH)
    try:
        with GatewayLookups(f"http://127.0.0.1:{server.server_port}", token.read_text()) as lookups:
            with pytest.raises(DataError) as refused:
                lookups.fetch(LookupRequest(lookup=lookup))
    finally:
        server.shutdown()
        server.server_close()
    assert refused.value.kind == "unsupported"
    assert "no CoinGecko on-chain network is configured for eip155:31337" in str(refused.value)
