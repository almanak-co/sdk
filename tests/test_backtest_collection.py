"""Concurrent collection preserves evidence while bounding provider work."""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.collection import CollectionLimits, ProviderGate, bounded_map
from almanak.backtest.history_batch import ReadBatch, TransientError, retry_delay
from tests.test_history_batch import batch_gateway as batch_gateway


def test_bounded_map_preserves_order_and_backpressure():
    lock = threading.Lock()
    active = maximum = consumed = 0

    def work(i):
        nonlocal active, maximum
        with lock:
            active += 1
            maximum = max(active, maximum)
        time.sleep(0.005 * (4 - i % 4))
        with lock:
            active -= 1
        return i * i

    def inputs():
        nonlocal consumed
        for i in range(20):
            consumed += 1
            yield i

    result = bounded_map(work, inputs(), 3)
    assert next(result) == 0
    assert consumed == 3
    assert list(result) == [i * i for i in range(1, 20)]
    assert maximum == 3
    with pytest.raises(ZeroDivisionError):
        list(bounded_map(lambda n: 1 / n, (1, 0, 2), 2))


def test_shared_provider_gate_paces_and_bounds(monkeypatch):
    clock = [0.0]
    waits = []
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", waits.append)
    gate = ProviderGate(CollectionLimits(max_in_flight=2, requests_per_second=2.0))
    for _ in range(3):
        with gate.admit():
            pass
    assert waits == [0.5, 1.0]
    assert retry_delay(httpx.Response(429, headers={"Retry-After": "2"})) == 2
    assert retry_delay(httpx.Response(429, headers={"Retry-After": "invalid"})) == 0


def test_a_held_gate_delays_every_admission_and_a_throttle_halves_the_overlap(monkeypatch):
    clock = [0.0]
    waits = []
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(time, "sleep", waits.append)
    gate = ProviderGate(CollectionLimits(max_in_flight=4))
    with gate.admit():
        pass
    gate.hold(2.0)
    assert gate.allowed == 4
    with gate.admit():
        pass
    assert waits == [2.0]
    clock[0] = 5.0
    for allowed in (2, 1, 1):
        gate.hold(0.5, throttled=True)
        assert gate.allowed == allowed
    # A shorter hold never shortens one already in place.
    gate.hold(0.1)
    with gate.admit():
        pass
    assert waits == [2.0, 0.5]


def test_a_throttled_gate_recovers_one_overlap_at_a_time(monkeypatch):
    from almanak.backtest import collection

    monkeypatch.setattr(collection, "RECOVERY_REQUESTS", 3)
    monkeypatch.setattr(time, "sleep", lambda _: None)
    gate = ProviderGate(CollectionLimits(max_in_flight=4))
    gate.hold(0.0, throttled=True)
    gate.hold(0.0, throttled=True)
    assert gate.allowed == 1
    seen = []
    for index in range(10):
        try:
            with gate.admit():
                if index == 1:
                    # A failed request is not evidence that the provider has room.
                    raise ValueError("failed")
        except ValueError:
            pass
        seen.append(gate.allowed)
    assert seen == [1, 1, 1, 2, 2, 2, 3, 3, 3, 4]
    for _ in range(10):
        with gate.admit():
            pass
    assert gate.allowed == 4


def large_batch(f, count):
    requests = tuple(
        f.requests[0].model_copy(
            update={
                "params": (
                    f.requests[0].params[0],
                    {"blockHash": "0x" + f"{i + 16:064x}", "requireCanonical": True},
                )
            }
        )
        for i in range(count)
    )
    return ReadBatch(requests=requests)


def test_one_large_batch_overlaps_provider_batches_within_the_provider_bound(batch_gateway):
    f = batch_gateway
    f.gateway.limits[f.requests[0].chain] = CollectionLimits(batch_size=8, max_in_flight=3)
    f.gateway.provider_gates[f.requests[0].chain] = ProviderGate(
        f.gateway.limits[f.requests[0].chain]
    )
    lock = threading.Lock()
    active = maximum = 0
    sizes = []
    # The first three batches meet here before any answers, so the test proves they
    # were in flight together instead of relying on a sleep to make them overlap. A
    # gateway that dispatches fewer at once breaks the barrier after the timeout.
    first_wave = threading.Barrier(3, timeout=10)

    def respond(request):
        nonlocal active, maximum
        body = json.loads(request.content)
        with lock:
            active += 1
            maximum = max(active, maximum)
            sizes.append(len(body))
            in_first_wave = len(sizes) <= 3
        if in_first_wave:
            first_wave.wait()
        # Holding each batch open lets a batch admitted beyond the bound be counted.
        # Scheduler delay can only hide such a batch, never fail a correct gateway.
        time.sleep(0.02)
        with lock:
            active -= 1
        return httpx.Response(
            200,
            json=[
                {"jsonrpc": "2.0", "id": r["id"], "result": r["params"][1]["blockHash"][-4:]}
                for r in body
            ],
        )

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    batch = large_batch(f, 60)
    rows = f.gateway.dispatch("read_many", batch)
    assert tuple(r.request for r in rows) == batch.requests
    assert [r.result for r in rows] == [r.params[1]["blockHash"][-4:] for r in batch.requests]
    assert sorted(sizes) == [4, 8, 8, 8, 8, 8, 8, 8]
    assert maximum == 3
    # A second dispatch is served from the capture alone.
    assert f.gateway.dispatch("read_many", batch) == rows and len(sizes) == 8


def test_a_capped_provider_keeps_its_batches_sequential(batch_gateway):
    f = batch_gateway
    chain = f.requests[0].chain
    f.gateway.limits[chain] = CollectionLimits(batch_size=8, max_in_flight=4)
    f.reader.provider_limit = SimpleNamespace(maximum=5, admit=lambda units: None)
    lock = threading.Lock()
    active = maximum = 0
    sizes = []

    def respond(request):
        nonlocal active, maximum
        body = json.loads(request.content)
        with lock:
            active += 1
            maximum = max(active, maximum)
            sizes.append(len(body))
        time.sleep(0.01)
        with lock:
            active -= 1
        return httpx.Response(
            200, json=[{"jsonrpc": "2.0", "id": r["id"], "result": "0x01"} for r in body]
        )

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    assert len(f.gateway.dispatch("read_many", large_batch(f, 12))) == 12
    # The cap also bounds each batch to what one period admits.
    assert sizes == [5, 5, 2] and maximum == 1


def test_a_throttled_provider_batch_retries_alone_and_narrows_the_overlap(
    batch_gateway, monkeypatch
):
    f = batch_gateway
    chain = f.requests[0].chain
    monkeypatch.setattr(time, "sleep", lambda _: None)
    f.gateway.limits[chain] = CollectionLimits(batch_size=8, max_in_flight=4)
    gate = f.gateway.provider_gates[chain] = ProviderGate(f.gateway.limits[chain])
    lock = threading.Lock()
    throttled = []

    def respond(request):
        body = json.loads(request.content)
        first = body[0]["params"][1]["blockHash"]
        with lock:
            again = first in throttled
            throttled.append(first)
        if first.endswith(f"{16 + 8:x}") and not again:
            return httpx.Response(
                200,
                json=[{"jsonrpc": "2.0", "id": r["id"], "error": {"code": 429}} for r in body],
            )
        return httpx.Response(
            200, json=[{"jsonrpc": "2.0", "id": r["id"], "result": "0x01"} for r in body]
        )

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    rows = f.gateway.dispatch("read_many", large_batch(f, 24))
    assert [r.result for r in rows] == ["0x01"] * 24
    # Three provider batches, and only the throttled one was sent again.
    assert len(throttled) == 4
    assert gate.allowed == 2
    assert f.capture.activity()["attempts"] == 32


def test_a_rate_limit_is_waited_out_beyond_the_transient_retry_budget(batch_gateway, monkeypatch):
    f = batch_gateway
    chain = f.requests[0].chain
    monkeypatch.setattr(time, "sleep", lambda _: None)
    f.gateway.limits[chain] = CollectionLimits(max_retries=1)
    calls = []

    def respond(request):
        calls.append(json.loads(request.content))
        if len(calls) <= 6:
            return httpx.Response(429)
        return httpx.Response(
            200, json=[{"jsonrpc": "2.0", "id": i, "result": "0x01"} for i in range(2)]
        )

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    rows = f.gateway.dispatch("read_many", ReadBatch(requests=f.requests))
    assert [r.result for r in rows] == ["0x01", "0x01"] and len(calls) == 7

    # A provider that never stops throttling still ends the batch.
    calls.clear()
    f.reader._client = httpx.Client(
        transport=httpx.MockTransport(lambda r: calls.append(r) or httpx.Response(429))
    )
    with pytest.raises(TransientError, match="HTTP 429"):
        f.gateway.dispatch("read_many", large_batch(f, 2))
    # Two reads, sent once and again for each of the eight throttles waited out.
    assert len(calls) == 9 and f.capture.activity()["attempts"] == 14 + 2 * 9


def test_a_batch_beyond_the_gateway_bound_is_refused():
    from almanak.backtest.history import MAX_BATCH_READS, ReadRequest
    from almanak.caip import ChainId
    from almanak.models import Chain

    chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    requests = tuple(
        ReadRequest(chain=chain, method="eth_getBlockByNumber", params=(hex(i), False))
        for i in range(MAX_BATCH_READS + 1)
    )
    assert len(ReadBatch(requests=requests[:-1]).requests) == MAX_BATCH_READS
    with pytest.raises(ValueError):
        ReadBatch(requests=requests)


def test_duplicate_concurrent_reads_have_one_provider_attempt(batch_gateway):
    f = batch_gateway
    with ThreadPoolExecutor(max_workers=4) as pool:
        rows = list(pool.map(lambda _: f.gateway.dispatch("read", f.requests[0]), range(12)))
    assert all(row == rows[0] for row in rows)
    assert len(f.calls) == f.capture.activity()["attempts"] == 1


def test_transient_partial_batch_retries_only_missing_reads(batch_gateway, monkeypatch):
    f = batch_gateway
    calls = []
    monkeypatch.setattr(time, "sleep", lambda _: None)

    def respond(request):
        body = json.loads(request.content)
        calls.append(body)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        if len(calls) == 2:
            return httpx.Response(
                200,
                json=[
                    {"jsonrpc": "2.0", "id": 0, "result": "0x01"},
                    {"jsonrpc": "2.0", "id": 1, "error": {"code": -32005}},
                ],
            )
        return httpx.Response(200, json=[{"jsonrpc": "2.0", "id": 0, "result": "0x02"}])

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    rows = f.gateway.dispatch("read_many", ReadBatch(requests=f.requests))
    assert [r.result for r in rows] == ["0x01", "0x02"]
    assert [len(c) for c in calls] == [2, 2, 1]
    assert f.capture.activity()["attempts"] == 5
    assert f.capture.activity()["unknown_outcomes"] == 0


def test_retry_exhaustion_and_batch_size(batch_gateway, monkeypatch):
    f = batch_gateway
    monkeypatch.setattr(time, "sleep", lambda _: None)
    f.gateway.limits[f.requests[0].chain] = CollectionLimits(batch_size=1, max_retries=1)
    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(503)))
    with pytest.raises(TransientError):
        f.gateway.dispatch("read_many", ReadBatch(requests=f.requests))
    # Both single-read provider batches were in flight, each with its own retry.
    assert f.capture.activity()["attempts"] == 4


def test_sampling_overrides_keep_other_products_and_operational_identity():
    from almanak.backtest.config import EconomicDataPolicy, financial_policy
    from almanak.backtest.dataset import DataKey
    from almanak.backtest.sampling import SamplingOverride
    from almanak.backtest.types import ConnectorId
    from almanak.caip import ChainId
    from almanak.models import Chain

    key = DataKey(
        connector=ConnectorId("other_lender"),
        chain=Chain(chain_id=ChainId("cosmos:cosmoshub-4"), environment="mainnet"),
        source="market",
        product="interest",
    )
    data = EconomicDataPolicy(
        source="aave_v4_archive",
        rate_sample_seconds=300,
        risk_prices="spoke_oracle_events",
        sampling_overrides=(SamplingOverride(key=key, sample_seconds=900, max_age_seconds=920),),
    )
    assert data.sampling(key).sample_seconds == 900
    assert data.sampling(key.model_copy(update={"source": "other"})).sample_seconds == 300
    assert financial_policy(data) != financial_policy(
        data.model_copy(update={"rate_sample_seconds": 3600})
    )
    with pytest.raises(ValueError, match="maximum age"):
        SamplingOverride(key=key, sample_seconds=900, max_age_seconds=100)
    raw = data.model_dump(mode="json")
    raw["sampling_overrides"] *= 2
    with pytest.raises(ValueError, match="distinct"):
        EconomicDataPolicy.model_validate_json(json.dumps(raw))


def test_retry_budget_never_retries_before_provider_instructions(monkeypatch):
    from almanak.backtest.history_batch import wait_to_retry
    from almanak.gateway.ethereum import DataError

    with pytest.raises(DataError, match="budget"):
        wait_to_retry(TransientError("http", "limited", 61), 0)


def test_batch_size_one_supports_providers_without_rpc_batches(batch_gateway):
    f = batch_gateway
    f.gateway.limits[f.requests[0].chain] = CollectionLimits(batch_size=1)
    calls = []

    def respond(request):
        body = json.loads(request.content)
        assert isinstance(body, dict)
        calls.append(body)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": "0x1234"})

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    assert len(f.gateway.dispatch("read_many", ReadBatch(requests=f.requests))) == 2
    assert len(calls) == 2
