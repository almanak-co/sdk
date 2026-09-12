"""Curve identity diagnostics through the real gateway response decoder."""

import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.connectors.curve import pool_resolver
from almanak.connectors.curve.pool_identity import identify_pool_payload
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.gateway.proto import gateway_pb2

TOKEN = "0x02fca66c1d1afb4e2a7884261eb00f63598a7436"
REGISTRY = "0x0000000000000000000000000000000000000002"


@pytest.fixture
def client():
    pool_resolver._clear_cache()
    gateway = GatewayClient(GatewayClientConfig())
    gateway._channel = MagicMock()
    gateway._rpc_stub = MagicMock()
    yield gateway
    pool_resolver._clear_cache()


def _answer_with_failure(client, failure):
    def call(request, timeout):
        target = json.loads(request.params)[0]
        if target["data"].startswith(pool_resolver._GET_ADDRESS_SEL):
            return gateway_pb2.RpcResponse(success=True, result=json.dumps("0x" + REGISTRY[2:].zfill(64)))
        if isinstance(failure, Exception):
            raise failure
        return failure

    client._rpc_stub.Call.side_effect = call


def _identify(client):
    return identify_pool_payload(SimpleNamespace(protocol="curve"), "bsc", TOKEN, gateway_client=client)


def test_expected_registry_revert_is_quiet_and_definitive(client, caplog):
    _answer_with_failure(
        client,
        gateway_pb2.RpcResponse(success=False, error='{"code": 3, "message": "execution reverted: no registry"}'),
    )
    with caplog.at_level(logging.WARNING):
        assert _identify(client) is None
    assert not caplog.records
    assert pool_resolver.resolution_is_definitive("bsc", TOKEN)


@pytest.mark.parametrize(
    "failure",
    [
        gateway_pb2.RpcResponse(success=False, error='{"code": -32005, "message": "rate limit exceeded"}'),
        gateway_pb2.RpcResponse(success=False, error='{"code": -32603, "message": "provider internal error"}'),
        gateway_pb2.RpcResponse(success=True, result='"0xnothex"'),
        gateway_pb2.RpcResponse(success=True, result='{"invalid": "result"}'),
        *[gateway_pb2.RpcResponse(success=True, result=json.dumps(value)) for value in [False, 0, {}, [], None, ""]],
        grpc.RpcError("UNAVAILABLE: connection reset"),
        gateway_pb2.RpcResponse(
            success=False,
            error=json.dumps({"code": -32603, "message": 'HTTP 500: failed request {"data": "0x12345678"}'}),
        ),
        gateway_pb2.RpcResponse(
            success=False,
            error=json.dumps({"code": -32603, "message": "provider internal error", "data": "0x12345678"}),
        ),
    ],
    ids=[
        "rate-limit",
        "provider-error",
        "malformed-hex",
        "malformed-result-type",
        "false-result",
        "zero-result",
        "empty-object",
        "empty-array",
        "null-result",
        "empty-string",
        "grpc-unavailable",
        "echoed-calldata",
        "ambiguous-data",
    ],
)
def test_unanswered_probe_remains_actionable_despite_healthy_other_reads(client, caplog, failure):
    _answer_with_failure(client, failure)
    with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError, match="Curve identity probe indeterminate"):
        _identify(client)
    assert "Curve MetaRegistry read inconclusive" in caplog.text
    assert not pool_resolver.resolution_is_definitive("bsc", TOKEN)

    _answer_with_failure(
        client,
        gateway_pb2.RpcResponse(success=False, error='{"code": 3, "message": "execution reverted: no registry"}'),
    )
    assert _identify(client) is None
    assert pool_resolver.resolution_is_definitive("bsc", TOKEN)


def test_plain_gateway_calls_still_log_rpc_failures(client, caplog):
    client._rpc_stub.Call.return_value = gateway_pb2.RpcResponse(success=False, error="provider unavailable")
    with caplog.at_level(logging.WARNING):
        assert client.eth_call(chain="bsc", to=TOKEN, data="0x12345678") is None
    assert "eth_call failed: provider unavailable" in caplog.text


def test_transient_required_read_recovers_on_bounded_retry(client):
    from tests.unit.connectors.curve.test_pool_resolver import DAI, USDC, FakeMetaRegistryGateway

    healthy = FakeMetaRegistryGateway(coins=[DAI, USDC], decimals=[18, 6], n_coins=2, gamma=10**11)
    n_coins_calls = 0

    def call(request, timeout):
        nonlocal n_coins_calls
        target = json.loads(request.params)[0]
        if target["data"].startswith(pool_resolver._GET_N_COINS_SEL):
            n_coins_calls += 1
            if n_coins_calls == 1:
                return gateway_pb2.RpcResponse(success=False, error='{"code": -32005, "message": "rate limit"}')
        raw = healthy.eth_call(chain=request.chain, to=target["to"], data=target["data"])
        return gateway_pb2.RpcResponse(success=True, result=json.dumps(raw))

    client._rpc_stub.Call.side_effect = call
    payload = _identify(client)
    assert payload["kind"] == "pool"
    assert payload["pool_type"] == "cryptoswap"
    assert n_coins_calls == 2
    assert pool_resolver.resolution_is_definitive("bsc", TOKEN)


def test_one_healthy_miss_does_not_erase_first_transient(client):
    failures = iter(
        [
            gateway_pb2.RpcResponse(success=False, error='{"code": -32005, "message": "rate limit"}'),
            gateway_pb2.RpcResponse(success=False, error='{"code": 3, "message": "execution reverted: no registry"}'),
        ]
    )
    registry_calls = 0

    def call(request, timeout):
        nonlocal registry_calls
        target = json.loads(request.params)[0]
        if target["data"].startswith(pool_resolver._GET_ADDRESS_SEL):
            return gateway_pb2.RpcResponse(success=True, result=json.dumps("0x" + REGISTRY[2:].zfill(64)))
        registry_calls += 1
        return next(failures)

    client._rpc_stub.Call.side_effect = call
    with pytest.raises(RuntimeError, match="Curve identity probe indeterminate"):
        _identify(client)
    assert registry_calls == 2
    assert not pool_resolver.resolution_is_definitive("bsc", TOKEN)
