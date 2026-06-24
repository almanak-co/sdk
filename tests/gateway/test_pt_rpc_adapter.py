"""VIB-5348 — gateway-native PT eth_call transport.

The gateway prices a Pendle PT by reading ``pt_to_asset_rate`` / ``expiry``
on-chain through the connector's ``PendleOnChainReader`` running in GATEWAY
mode. Before VIB-5348 the gateway forced the reader's direct ``Web3.HTTPProvider``
path; now it injects :class:`GatewayPtRpcClient`, a duck-typed
``gateway_client`` backed by the gateway's own async ``aiohttp`` eth_call. These
tests prove:

* a measured rate flows end to end through the gateway-native transport with NO
  ``HTTPProvider`` instantiated (``reader.web3 is None``);
* a transport failure surfaces as ``PendleOnChainError`` → ``_read_pt_market``
  reports ``rate is None`` (UNMEASURED, Empty≠Zero — never at-par);
* the sync↔async bridge works from a worker thread with no running event loop
  (the ``asyncio.to_thread`` execution context the gateway uses).
"""

from __future__ import annotations

import asyncio
import threading
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.pendle.on_chain_reader import (
    GET_ORACLE_STATE_SELECTOR,
    GET_PT_TO_ASSET_RATE_SELECTOR,
    PendleOnChainError,
    PendleOnChainReader,
)
from almanak.gateway.services.pt_rpc_adapter import (
    GatewayPtRpcClient,
    GatewayPtRpcError,
    _NativeEthCall,
)

_MARKET = "0x1234567890123456789012345678901234567890"

# getOracleState → (increaseCardinalityRequired=False, cardinality=100,
# oldestObservationSatisfied=True): oracle is ready.
_ORACLE_READY = "0x" + format(0, "064x") + format(100, "064x") + format(1, "064x")

# getPtToAssetRate → one uint256 word, 1e18-scaled. 0.97 → 970000000000000000.
_RATE_WEI = int(Decimal("0.97") * Decimal("1000000000000000000"))
_RATE_WORD = "0x" + format(_RATE_WEI, "064x")


def _fake_eth_call_router(rate_word: str = _RATE_WORD):
    """Build an async ``eth_call`` stub that answers by calldata selector.

    Mirrors the on-chain contract surface the reader exercises: the PT oracle's
    ``getOracleState`` (readiness gate) and ``getPtToAssetRate`` (the rate).
    """

    async def _eth_call(to: str, data: str) -> str:
        selector = data[:10]  # 0x + 4 bytes
        if selector == GET_ORACLE_STATE_SELECTOR:
            return _ORACLE_READY
        if selector == GET_PT_TO_ASSET_RATE_SELECTOR:
            return rate_word
        raise AssertionError(f"unexpected selector {selector} to {to}")

    return _eth_call


def _gateway_reader(eth_call_stub) -> PendleOnChainReader:
    """A real reader in gateway mode whose transport is the given async stub."""
    client = GatewayPtRpcClient(chain="ethereum", network="mainnet")
    # Replace the native aiohttp call with the in-test stub; everything else
    # (request shaping in _gateway_eth_call, the .rpc.Call adapter, the sync
    # bridge) runs for real.
    client._source.eth_call = eth_call_stub  # type: ignore[method-assign]
    return PendleOnChainReader(chain="ethereum", gateway_client=client)


class TestGatewayNativeRateRead:
    def test_measured_rate_flows_through_native_transport_no_httpprovider(self):
        """End to end: a measured PT rate via the gateway-native eth_call, with
        no ``HTTPProvider`` ever instantiated (``reader.web3 is None``)."""
        reader = _gateway_reader(_fake_eth_call_router())
        assert reader.web3 is None  # gateway mode — no raw web3 provider

        rate = reader.get_pt_to_asset_rate(_MARKET)
        assert rate == Decimal("0.97")

    def test_transport_failure_surfaces_as_unmeasured_never_at_par(self):
        """A JSON-RPC / transport failure → PendleOnChainError, which the gateway
        maps to UNMEASURED (rate is None). The at-par (1.0) fabrication is the bug
        this guards against (Empty≠Zero)."""

        async def _failing_call(to: str, data: str) -> str:
            # Readiness gate passes; the rate read fails at the transport.
            if data[:10] == GET_ORACLE_STATE_SELECTOR:
                return _ORACLE_READY
            raise GatewayPtRpcError("JSON-RPC error: execution reverted")

        reader = _gateway_reader(_failing_call)
        with pytest.raises(PendleOnChainError):
            reader.get_pt_to_asset_rate(_MARKET)

    def test_empty_result_surfaces_as_unmeasured(self):
        """An empty ``0x`` rate result (gateway-mode contract) → PendleOnChainError,
        never a silent zero/at-par."""

        async def _empty_rate(to: str, data: str) -> str:
            if data[:10] == GET_ORACLE_STATE_SELECTOR:
                return _ORACLE_READY
            # The native transport raises on empty 0x; emulate that contract.
            raise GatewayPtRpcError("empty eth_call result")

        reader = _gateway_reader(_empty_rate)
        with pytest.raises(PendleOnChainError):
            reader.get_pt_to_asset_rate(_MARKET)


class TestRpcCallAdapter:
    def test_call_unpacks_eth_call_and_returns_rpc_result(self):
        """``rpc.Call`` unpacks the reader's RpcRequest and returns a duck-typed
        RpcResponse (``.success`` / ``.result`` JSON-encoded hex)."""
        import json

        from almanak.gateway.proto import gateway_pb2

        client = GatewayPtRpcClient(chain="ethereum", network="mainnet")

        async def _ok(to: str, data: str) -> str:
            return _RATE_WORD

        client._source.eth_call = _ok  # type: ignore[method-assign]

        req = gateway_pb2.RpcRequest(
            chain="ethereum",
            method="eth_call",
            params=json.dumps([{"to": _MARKET, "data": GET_PT_TO_ASSET_RATE_SELECTOR}, "latest"]),
            id="t",
        )
        resp = client.rpc.Call(req, timeout=30.0)
        assert resp.success is True
        assert json.loads(resp.result) == _RATE_WORD
        assert resp.error == ""

    def test_call_non_eth_call_method_is_failed_not_silent(self):
        from almanak.gateway.proto import gateway_pb2

        client = GatewayPtRpcClient(chain="ethereum", network="mainnet")
        req = gateway_pb2.RpcRequest(chain="ethereum", method="eth_getBalance", params="[]", id="t")
        resp = client.rpc.Call(req)
        assert resp.success is False
        assert "unsupported RPC method" in resp.error

    def test_call_transport_error_is_failed_result(self):
        from almanak.gateway.proto import gateway_pb2

        client = GatewayPtRpcClient(chain="ethereum", network="mainnet")

        async def _boom(to: str, data: str) -> str:
            raise GatewayPtRpcError("eth_call transport error: connection reset")

        client._source.eth_call = _boom  # type: ignore[method-assign]
        req = gateway_pb2.RpcRequest(
            chain="ethereum",
            method="eth_call",
            params='[{"to": "0xabc", "data": "0xdeadbeef"}, "latest"]',
            id="t",
        )
        resp = client.rpc.Call(req)
        assert resp.success is False
        assert "transport error" in resp.error


class TestSyncBridgeSafety:
    def test_bridge_runs_from_worker_thread_with_no_loop(self):
        """The bridge (asyncio.run) must succeed from a thread with no running
        loop — the exact context MarketService uses via ``asyncio.to_thread``."""
        import json

        from almanak.gateway.proto import gateway_pb2

        client = GatewayPtRpcClient(chain="ethereum", network="mainnet")

        async def _ok(to: str, data: str) -> str:
            return _RATE_WORD

        client._source.eth_call = _ok  # type: ignore[method-assign]

        result: dict[str, object] = {}

        def worker() -> None:
            req = gateway_pb2.RpcRequest(
                chain="ethereum",
                method="eth_call",
                params=json.dumps([{"to": _MARKET, "data": GET_PT_TO_ASSET_RATE_SELECTOR}, "latest"]),
                id="t",
            )
            resp = client.rpc.Call(req)
            result["success"] = resp.success
            result["result"] = resp.result

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=10.0)
        assert result.get("success") is True
        assert json.loads(result["result"]) == _RATE_WORD  # type: ignore[arg-type]

    def test_multiple_sequential_calls_each_get_fresh_loop_and_session(self):
        """Two sequential reads must both succeed — the per-call session/loop must
        not leak a closed-loop session into the next call (the bug a cached
        aiohttp session would cause under per-call ``asyncio.run``)."""
        reader = _gateway_reader(_fake_eth_call_router())
        reader.clear_cache()
        first = reader.get_pt_to_asset_rate(_MARKET)
        reader.clear_cache()
        second = reader.get_pt_to_asset_rate(_MARKET)
        assert first == second == Decimal("0.97")


class TestNativeEthCallNoCreds:
    def test_missing_rpc_url_raises_transport_error(self):
        """No RPC URL (no gateway creds) → GatewayPtRpcError when a call runs,
        which the reader maps to UNMEASURED — never a fabricated price."""
        with patch("almanak.gateway.services.pt_rpc_adapter.get_rpc_url", side_effect=ValueError("no creds")):
            source = _NativeEthCall(chain="ethereum", network="mainnet", request_timeout=5.0)
        assert source._rpc_url is None

        with pytest.raises(GatewayPtRpcError, match="no RPC URL"):
            asyncio.run(source.eth_call(_MARKET, GET_PT_TO_ASSET_RATE_SELECTOR))


# ---------------------------------------------------------------------------
# Real-transport branches of _NativeEthCall.eth_call
#
# The fakes above replace ``_source.eth_call`` wholesale, so they never enter the
# real ``aiohttp`` body — the HTTP-status / JSON-RPC-error / empty-result / success
# branches stayed uncovered (CRAP gate flagged ``eth_call`` at 12%). The gateway
# egress is the security boundary, so these branches are pinned here directly by
# driving the REAL method against a mocked ``aiohttp.ClientSession`` (VIB-5348, CR).
# ---------------------------------------------------------------------------


class _FakeResp:
    """Async-CM mimicking the slice of ``aiohttp`` response the method touches:
    ``.status``, ``await .text()``, ``await .json()``."""

    def __init__(self, *, status: int = 200, json_body: dict | None = None, text_body: str = "") -> None:
        self.status = status
        self._json_body = {} if json_body is None else json_body
        self._text_body = text_body

    async def __aenter__(self) -> _FakeResp:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def json(self) -> dict:
        return self._json_body

    async def text(self) -> str:
        return self._text_body


class _FakeSession:
    """Async-CM session whose ``post()`` returns a fixed ``_FakeResp`` (or raises a
    supplied transport error to drive the ``except Exception`` branch)."""

    def __init__(self, *, resp: _FakeResp | None = None, post_exc: Exception | None = None) -> None:
        self._resp = resp
        self._post_exc = post_exc
        self.calls: list[tuple[str, dict | None]] = []

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    def post(self, url, *, json=None, timeout=None):  # noqa: A002 — mirrors aiohttp.post
        self.calls.append((url, json))
        if self._post_exc is not None:
            raise self._post_exc
        return self._resp


def _run_real_eth_call(fake_session: _FakeSession) -> str:
    """Drive the REAL ``_NativeEthCall.eth_call`` against ``fake_session`` (a valid
    RPC URL is stubbed so the early no-URL guard is skipped). ``aiohttp`` session /
    connector and the SSL-context build are patched out so no socket / SSL work runs."""
    with patch(
        "almanak.gateway.services.pt_rpc_adapter.get_rpc_url",
        return_value="https://rpc.example/key",
    ):
        source = _NativeEthCall(chain="ethereum", network="mainnet", request_timeout=5.0)
    with (
        patch("aiohttp.ClientSession", return_value=fake_session),
        patch("aiohttp.TCPConnector", return_value=MagicMock()),
        patch("almanak.gateway.services.pt_rpc_adapter.build_ssl_context", return_value=None),
    ):
        return asyncio.run(source.eth_call(_MARKET, GET_PT_TO_ASSET_RATE_SELECTOR))


class TestNativeEthCallRealTransport:
    """Pin the real ``aiohttp`` branches of the gateway-egress ``eth_call``."""

    def test_success_returns_hex_result(self):
        session = _FakeSession(resp=_FakeResp(json_body={"jsonrpc": "2.0", "id": 1, "result": _RATE_WORD}))
        assert _run_real_eth_call(session) == _RATE_WORD
        # The audited egress posted a JSON-RPC eth_call to the resolved RPC URL.
        assert session.calls and session.calls[0][0] == "https://rpc.example/key"
        assert session.calls[0][1]["method"] == "eth_call"

    def test_non_200_raises_http_error(self):
        session = _FakeSession(resp=_FakeResp(status=503, text_body="upstream unavailable"))
        with pytest.raises(GatewayPtRpcError, match="RPC HTTP 503"):
            _run_real_eth_call(session)

    def test_jsonrpc_error_body_raises(self):
        session = _FakeSession(
            resp=_FakeResp(json_body={"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "reverted"}})
        )
        with pytest.raises(GatewayPtRpcError, match="JSON-RPC error"):
            _run_real_eth_call(session)

    def test_empty_0x_result_raises(self):
        # Empty != Zero: an empty result is UNMEASURED, never a fabricated 0/at-par.
        session = _FakeSession(resp=_FakeResp(json_body={"jsonrpc": "2.0", "id": 1, "result": "0x"}))
        with pytest.raises(GatewayPtRpcError, match="empty eth_call result"):
            _run_real_eth_call(session)

    def test_transport_exception_is_wrapped(self):
        session = _FakeSession(post_exc=OSError("connection reset"))
        with pytest.raises(GatewayPtRpcError, match="transport error"):
            _run_real_eth_call(session)
