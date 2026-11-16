"""Provider admission is process scoped, thread safe and before external I/O."""

import json
import multiprocessing
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer

import httpx
import pytest

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.fork_fixture import fork_read_handler
from almanak.gateway.morpho_lookup import MorphoApi, MorphoMarkets
from almanak.gateway.rate_limits import VARIABLES, ProviderLimit
from almanak.models import Chain
from tests.test_history_batch import batch_gateway as batch_gateway
from tests.test_vendor_prices import binding as binding


@pytest.fixture(autouse=True)
def clean_limits(monkeypatch):
    for variable, _ in VARIABLES.values():
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr("almanak.gateway.rate_limits._windows", {})
    monkeypatch.delenv("ALMANAK_GATEWAY_RATE_LIMIT_STATE", raising=False)


def configure(monkeypatch, rpc="3", coingecko="2"):
    monkeypatch.setenv(VARIABLES["rpc"][0], rpc)
    monkeypatch.setenv(VARIABLES["coingecko"][0], coingecko)


def attempt(limit):
    try:
        limit.admit()
        return True
    except DataError as error:
        assert error.kind == "rate_limit"
        return False


def child_admit(result):
    result.put(attempt(ProviderLimit("coingecko")))


def test_unset_zero_and_invalid_settings(monkeypatch, tmp_path):
    ProviderLimit("rpc").admit(100000)
    assert not list(tmp_path.iterdir())
    configure(monkeypatch, rpc="0")
    assert not attempt(ProviderLimit("rpc"))
    for value in ("", "-1", "1.5", "nan", "inf", " 1", "true", "١"):
        monkeypatch.setenv(VARIABLES["rpc"][0], value)
        with pytest.raises(ValueError, match="non-negative integer"):
            ProviderLimit("rpc")


def test_sliding_window_batch_atomicity_and_expiry(monkeypatch):
    configure(monkeypatch)
    clock = [100.0]
    monkeypatch.setattr("almanak.gateway.rate_limits.monotonic", lambda: clock[0])
    limit = ProviderLimit("rpc")
    limit.admit(2)
    with pytest.raises(DataError):
        limit.admit(2)
    assert attempt(limit)
    clock[0] = 100.999
    assert not attempt(limit)
    clock[0] = 101.0
    limit.admit(3)
    for units in (0, -1, True, 1.5):
        with pytest.raises(ValueError, match="positive integer"):
            limit.admit(units)


def test_new_transports_share_concurrent_allowance_and_categories_are_independent(monkeypatch):
    configure(monkeypatch, coingecko="7")
    limits = [ProviderLimit("coingecko") for _ in range(4)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(attempt, limits * 8)) == 7
    assert not attempt(ProviderLimit("coingecko"))
    ProviderLimit("rpc").admit(3)
    monkeypatch.setenv(VARIABLES["coingecko"][0], "8")
    with pytest.raises(ValueError, match="restart"):
        ProviderLimit("coingecko")


def test_morpho_lookups_have_their_own_allowance(monkeypatch):
    configure(monkeypatch, coingecko="1")
    monkeypatch.setenv(VARIABLES["morpho"][0], "2")
    assert attempt(ProviderLimit("coingecko")) and not attempt(ProviderLimit("coingecko"))
    morpho = ProviderLimit("morpho")
    assert attempt(morpho) and attempt(morpho) and not attempt(morpho)


def test_hyperliquid_queries_spend_their_own_weight_allowance(monkeypatch):
    configure(monkeypatch, coingecko="1")
    monkeypatch.setenv(VARIABLES["hyperliquid"][0], "80")
    assert attempt(ProviderLimit("coingecko")) and not attempt(ProviderLimit("coingecko"))
    hyperliquid = ProviderLimit("hyperliquid")
    hyperliquid.admit(60)
    hyperliquid.admit(20)
    with pytest.raises(DataError) as refused:
        hyperliquid.admit(2)
    assert refused.value.kind == "rate_limit"
    assert attempt(ProviderLimit("rpc"))


def test_a_reservation_returns_what_its_request_did_not_use(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr("almanak.gateway.rate_limits.monotonic", lambda: clock[0])
    monkeypatch.setenv(VARIABLES["hyperliquid"][0], "10")
    limit = ProviderLimit("hyperliquid")
    reservation = limit.reserve(8)
    assert not attempt_units(limit, 3)
    reservation.settle(5)
    assert attempt_units(limit, 5) and not attempt_units(limit, 1)
    # A reservation that expired before it settled has nothing left to return.
    clock[0] += 61
    late = limit.reserve(10)
    reservation.settle(0)
    late.settle(10)
    assert not attempt_units(limit, 1)
    late.settle(4)
    assert attempt_units(limit, 6) and not attempt_units(limit, 1)


def attempt_units(limit, units):
    try:
        limit.admit(units)
        return True
    except DataError as error:
        assert error.kind == "rate_limit"
        return False


def test_morpho_lookups_are_refused_before_any_request_past_the_cap(monkeypatch):
    sent = []

    def api(request):
        sent.append(request)
        return httpx.Response(200, json={"data": {"markets": {"items": []}}})

    lookup = MorphoMarkets(chain=ChainId("eip155:8453"), loan="0x" + "11" * 20)
    for cap, answered in (("0", 0), ("1", 1)):
        monkeypatch.setattr("almanak.gateway.rate_limits._windows", {})
        monkeypatch.setenv(VARIABLES["morpho"][0], cap)
        sent.clear()
        morpho = MorphoApi()
        morpho.client = httpx.Client(transport=httpx.MockTransport(api))
        for _ in range(answered):
            morpho.fetch(lookup)
        with pytest.raises(DataError) as refused:
            morpho.fetch(lookup)
        morpho.close()
        assert refused.value.kind == "rate_limit" and len(sent) == answered


def test_new_gateway_process_gets_its_own_allowance(monkeypatch):
    configure(monkeypatch, coingecko="1")
    ProviderLimit("coingecko").admit()
    context = multiprocessing.get_context("spawn")
    result = context.Queue()
    process = context.Process(target=child_admit, args=(result,))
    process.start()
    try:
        assert result.get(timeout=15) is True
        process.join(timeout=15)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        result.close()


def test_rpc_timeouts_still_consume_budget_and_anvil_is_local(monkeypatch, tmp_path):
    configure(monkeypatch, rpc="1")
    calls = []

    def respond(request):
        calls.append(request)
        raise httpx.ReadTimeout("fixture", request=request)

    chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        reader = EthereumReader(chain, "https://fixture.invalid", client=client)
        for expected in ("transport", "rate_limit"):
            with pytest.raises(DataError) as error:
                reader._rpc("eth_chainId", [])
            assert error.value.kind == expected
        local = EthereumReader(
            chain.model_copy(update={"environment": "anvil:fixture"}),
            "http://localhost:1234",
            client=client,
            local_transport=True,
        )
        with pytest.raises(DataError) as error:
            local._rpc("eth_chainId", [])
        assert error.value.kind == "transport"
        forged = EthereumReader(local.chain, "https://fixture.invalid", client=client)
        with pytest.raises(DataError) as error:
            forged._rpc("eth_chainId", [])
        assert error.value.kind == "rate_limit"
    assert len(calls) == 2


def test_fork_proxy_charges_each_batch_member(monkeypatch, tmp_path):
    configure(monkeypatch, rpc="2")
    calls = []

    def respond(request):
        body = json.loads(request.content)
        calls.append(body)
        return httpx.Response(200, json=body)

    with httpx.Client(transport=httpx.MockTransport(respond)) as upstream:
        handler = fork_read_handler("https://fixture.invalid", upstream, ProviderLimit("rpc"))
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            with httpx.Client(trust_env=False) as client:
                url = f"http://127.0.0.1:{server.server_port}"
                body = [{"method": "eth_call", "id": n} for n in range(2)]
                assert client.post(url, json=body).status_code == 200
                assert client.post(url, json=body).status_code == 429
                assert (
                    client.post(url, json={"method": "eth_sendRawTransaction"}).status_code == 403
                )
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
    assert len(calls) == 1


def test_historical_batches_cannot_bypass_cap_and_cache_is_free(
    monkeypatch, tmp_path, batch_gateway
):
    from almanak.backtest.history_batch import ReadBatch

    configure(monkeypatch, rpc="2")
    fixture = batch_gateway
    fixture.reader.provider_limit = ProviderLimit("rpc")
    batch = ReadBatch(requests=fixture.requests)
    retained = fixture.gateway.dispatch("read_many", batch)
    assert fixture.gateway.dispatch("read_many", batch) == retained
    another = fixture.requests[0].model_copy(update={"method": "eth_getBalance"})
    with pytest.raises(DataError) as error:
        fixture.gateway.dispatch("read", another)
    assert error.value.kind == "rate_limit"
    assert len(fixture.calls) == 1
    assert fixture.capture.activity()["unknown_outcomes"] == 0


def test_historical_batch_size_respects_gateway_cap(monkeypatch, tmp_path, batch_gateway):
    from almanak.backtest.history_batch import ReadBatch

    configure(monkeypatch, rpc="1")
    fixture = batch_gateway
    fixture.reader.provider_limit = ProviderLimit("rpc")
    with pytest.raises(DataError) as error:
        fixture.gateway.dispatch("read_many", ReadBatch(requests=fixture.requests))
    assert error.value.kind == "rate_limit"
    assert len(fixture.calls) == 1
    assert len(fixture.calls[0]) == 1
    assert fixture.capture.get(fixture.requests[0]) is not None
    assert fixture.capture.get(fixture.requests[1]) is None


def test_vendor_discovery_and_history_share_cap_and_cache_is_free(monkeypatch, tmp_path, binding):
    from tests.test_vendor_discovery import PLATFORMS, auto_binding, collector_for, metadata

    configure(monkeypatch, coingecko="3")
    selected = auto_binding(binding)
    collector, requests = collector_for(tmp_path, (selected,))
    calls = []

    def respond(request):
        calls.append(request)
        if request.url.path.endswith("asset_platforms"):
            return httpx.Response(200, json=PLATFORMS)
        if "/contract/" in request.url.path:
            return httpx.Response(200, json=metadata(selected.asset, "wrapped-bitcoin"))
        return httpx.Response(200, json={"prices": []})

    collector.client.close()
    collector.client = httpx.Client(transport=httpx.MockTransport(respond))
    try:
        retained = collector.read(requests[0])
        assert collector.read(requests[0]) == retained
        with pytest.raises(DataError) as error:
            collector.lookup(requests[0], "asset_platforms")
        assert error.value.kind == "rate_limit"
        assert len(calls) == 3
        assert collector.capture.activity()["unknown_outcomes"] == 0
    finally:
        collector.close()


def test_vendor_retries_cannot_exceed_operator_cap(monkeypatch, tmp_path, binding):
    from tests.test_vendor_discovery import collector_for

    configure(monkeypatch, coingecko="1")
    monkeypatch.setattr("almanak.backtest.vendor.wait_to_retry", lambda *_: None)
    collector, requests = collector_for(tmp_path, (binding,))
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(503)

    collector.client.close()
    collector.client = httpx.Client(transport=httpx.MockTransport(respond))
    try:
        with pytest.raises(DataError) as error:
            collector.read(requests[0])
        assert error.value.kind == "rate_limit"
        assert len(calls) == 1
        assert collector.capture.activity()["unknown_outcomes"] == 0
    finally:
        collector.close()
