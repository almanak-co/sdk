"""Batch transport preserves individual source evidence and authorization."""

import json
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.history import Capture, ReadRequest
from almanak.backtest.history_batch import (
    BatchedHistoricalGateway,
    HistoricalRequest,
    ReadBatch,
    validate_response,
)
from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Chain


@pytest.fixture
def batch_gateway(tmp_path):
    chain = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
    requests = tuple(
        ReadRequest(
            chain=chain,
            method="eth_getCode",
            params=(
                "0x" + "11" * 20,
                {"blockHash": "0x" + f"{i:064x}", "requireCanonical": True},
            ),
        )
        for i in (1, 2)
    )
    calls = []

    def respond(request):
        body = json.loads(request.content)
        calls.append(body)
        return httpx.Response(
            200, json=[dict(jsonrpc="2.0", id=r["id"], result="0x1234") for r in reversed(body)]
        )

    reader = EthereumReader(
        chain,
        "https://archive.invalid",
        client=httpx.Client(transport=httpx.MockTransport(respond)),
    )
    capture = Capture(tmp_path / "data.sqlite")
    gateway = BatchedHistoricalGateway(capture, {chain: reader}, {chain: ("0x" + "11" * 20,)})
    return SimpleNamespace(
        gateway=gateway, capture=capture, reader=reader, requests=requests, calls=calls
    )


def test_batch_is_scoped_and_replacement_uses_identical_individual_evidence(batch_gateway):
    f = batch_gateway
    batch = ReadBatch(requests=f.requests)
    result = f.gateway.dispatch("read_many", HistoricalRequest(root=batch))
    assert tuple(r.request for r in result) == f.requests
    assert len(f.calls) == 1
    assert f.capture.activity()["attempts"] == 2
    restored = BatchedHistoricalGateway(f.capture, {}, f.gateway.contracts)
    assert restored.dispatch("read_many", batch) == result
    assert restored.dispatch("read", f.requests[0]) == result[0]
    bad = f.requests[0].model_copy(update={"params": ("0x" + "22" * 20, f.requests[0].params[1])})
    with pytest.raises(ValueError):
        restored.dispatch("read_many", ReadBatch(requests=(bad,)))
    with pytest.raises(ValueError):
        ReadBatch(requests=(f.requests[0],) * 2)
    assert len(f.calls) == 1


@pytest.mark.parametrize(
    "payload",
    [
        {},
        [],
        [{"jsonrpc": "2.0", "id": 0}],
        [{"jsonrpc": "2.0", "id": 0}] * 2,
        [{"jsonrpc": "2.0", "id": False}, {"jsonrpc": "2.0", "id": 1}],
        [{"jsonrpc": "1.0", "id": 0}, {"jsonrpc": "2.0", "id": 1}],
    ],
)
def test_invalid_batch_envelopes_fail(payload):
    with pytest.raises(DataError):
        validate_response(payload, 2)


def test_partial_batch_failure_retains_success_and_failed_attempts(batch_gateway):
    f = batch_gateway

    def respond(request):
        return httpx.Response(
            200,
            json=[
                dict(jsonrpc="2.0", id=1, error={"message": "must not escape"}),
                dict(jsonrpc="2.0", id=0, result="0x1234"),
            ],
        )

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    with pytest.raises(DataError, match="rejected or unavailable"):
        f.gateway.dispatch("read_many", ReadBatch(requests=f.requests))
    assert f.capture.get(f.requests[0]).result == "0x1234"
    assert f.capture.get(f.requests[1]) is None
    assert f.capture.activity()["unknown_outcomes"] == 0


def test_coded_rejection_retries_then_surfaces(batch_gateway):
    from almanak.backtest.history_batch import TransientError

    f = batch_gateway

    def respond(request):
        body = json.loads(request.content)
        f.calls.append(body)
        out = []
        for row in body:
            if row["params"][1]["blockHash"].endswith("2"):
                out.append(
                    dict(jsonrpc="2.0", id=row["id"], error={"code": -32000, "message": "capacity"})
                )
            else:
                out.append(dict(jsonrpc="2.0", id=row["id"], result="0x1234"))
        return httpx.Response(200, json=out)

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    with pytest.raises(TransientError, match=r"rejected reads \(codes \[-32000\]\)"):
        f.gateway.dispatch("read_many", ReadBatch(requests=f.requests))
    # Bounded retries exhausted: one initial attempt plus the configured retries.
    assert len(f.calls) == 4
    assert f.capture.get(f.requests[0]).result == "0x1234"
    assert f.capture.get(f.requests[1]) is None


def test_permanent_protocol_rejection_fails_without_retry(batch_gateway):
    f = batch_gateway

    def respond(request):
        body = json.loads(request.content)
        f.calls.append(body)
        out = []
        for row in body:
            if row["params"][1]["blockHash"].endswith("2"):
                out.append(
                    dict(
                        jsonrpc="2.0",
                        id=row["id"],
                        error={"code": -32601, "message": "method not found"},
                    )
                )
            else:
                out.append(dict(jsonrpc="2.0", id=row["id"], result="0x1234"))
        return httpx.Response(200, json=out)

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    with pytest.raises(DataError, match="rejected or unavailable"):
        f.gateway.dispatch("read_many", ReadBatch(requests=f.requests))
    assert len(f.calls) == 1
    assert f.capture.get(f.requests[0]).result == "0x1234"
    assert f.capture.get(f.requests[1]) is None


def test_batch_implementation_reads_require_retained_same_block_pointer(batch_gateway):
    from almanak.gateway.roles_storage import IMPLEMENTATION_SLOT

    fixture = batch_gateway
    base = fixture.requests[0]
    target, anchor = base.params
    implementation = "0x" + "34" * 20
    request = base.model_copy(update={"params": (implementation, anchor)})
    with pytest.raises(ValueError, match="outside"):
        fixture.gateway.dispatch("read", request)
    assert fixture.calls == []
    pointer = ReadRequest(
        chain=base.chain,
        method="eth_getStorageAt",
        params=(target, hex(IMPLEMENTATION_SLOT), anchor),
    )
    fixture.capture.put(pointer, "0x" + "0" * 24 + implementation[2:])
    evidence = fixture.gateway.dispatch("read", request)
    assert evidence.request == request
    other = request.model_copy(update={"params": (implementation, fixture.requests[1].params[1])})
    with pytest.raises(ValueError, match="outside"):
        fixture.gateway.dispatch("read", other)
    assert len(fixture.calls) == 1


def test_a_batch_answered_with_a_non_json_page_is_sent_again(batch_gateway):
    f = batch_gateway
    pages = iter([httpx.Response(200, text="<html>busy</html>")])

    def respond(request):
        body = json.loads(request.content)
        f.calls.append(body)
        return next(pages, None) or httpx.Response(
            200, json=[dict(jsonrpc="2.0", id=r["id"], result="0x1234") for r in body]
        )

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    result = f.gateway.dispatch("read_many", ReadBatch(requests=f.requests))
    assert [r.result for r in result] == ["0x1234", "0x1234"]
    assert len(f.calls) == 2


def single_read(batch_gateway, responses):
    """The archive reader answers single reads from `responses`, then a header."""
    f = batch_gateway
    request = ReadRequest(
        chain=f.reader.chain, method="eth_getBlockByNumber", params=("0x10", False)
    )
    header = {"number": "0x10", "hash": "0x" + "ab" * 32}

    def respond(http):
        body = json.loads(http.content)
        f.calls.append(body)
        return next(responses, None) or httpx.Response(
            200, json=dict(jsonrpc="2.0", id=body["id"], result=header)
        )

    f.reader._client.close()
    f.reader._client = httpx.Client(transport=httpx.MockTransport(respond))
    return request, header


def test_a_read_outside_the_batches_waits_out_a_rate_limit(batch_gateway):
    f = batch_gateway
    throttles = iter([httpx.Response(429, headers={"Retry-After": "0"})] * 2)
    request, header = single_read(f, throttles)
    assert f.gateway.fetch(f.reader, request) == header
    assert len(f.calls) == 3
    assert f.gateway.provider_gates[request.chain].allowed == 1


def test_a_read_outside_the_batches_fails_at_once_on_a_refusal(batch_gateway):
    f = batch_gateway
    request, _ = single_read(f, iter([httpx.Response(403)]))
    with pytest.raises(DataError, match="HTTP 403") as failure:
        f.gateway.fetch(f.reader, request)
    assert not failure.value.transient and len(f.calls) == 1


def test_a_read_outside_the_batches_surfaces_a_failure_that_outlasts_its_retries(batch_gateway):
    from almanak.backtest.history_batch import TransientError

    f = batch_gateway
    request, _ = single_read(f, iter([httpx.Response(503)] * 8))
    with pytest.raises(TransientError, match="HTTP 503"):
        f.gateway.fetch(f.reader, request)
    # One attempt plus the default three retries.
    assert len(f.calls) == 4


def test_a_read_outside_the_batches_reads_a_page_that_is_not_json_again(batch_gateway):
    f = batch_gateway
    request, header = single_read(f, iter([httpx.Response(200, text="<html>busy</html>")]))
    assert f.gateway.fetch(f.reader, request) == header
    assert len(f.calls) == 2
