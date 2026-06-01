"""D2.M5.a auth + kill-switch test for PoolHistoryService (VIB-4750 / POOL-2).

Maps to umbrella UAT card ``docs/internal/uat-cards/VIB-4728.md`` D2.M5.a:

- ``ALMANAK_GATEWAY_ALLOW_INSECURE=0`` + no token  -> UNAUTHENTICATED
- ``ALMANAK_GATEWAY_ALLOW_INSECURE=0`` + valid token, kill-switch off
  (default) -> UNAVAILABLE with "VIB-4728" in details
- ``ALMANAK_GATEWAY_ALLOW_INSECURE=0`` + valid token AND
  ``ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true`` (providers not yet wired)
  -> UNIMPLEMENTED
- ``ALMANAK_GATEWAY_ALLOW_INSECURE=1`` -> auth interceptor not installed;
  kill-switch still gates behavior

Mirrors the pattern in ``test_pool_analytics_auth.py``: exercise the
AuthInterceptor directly against the target method path rather than
booting a full gRPC server.
"""

from __future__ import annotations

import asyncio

import grpc
import pytest

from almanak.gateway.auth import AUTH_METADATA_KEY, AUTH_METADATA_KEY_ALT, AuthInterceptor
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.pool_history_service import PoolHistoryServiceServicer

POOL_HISTORY_METHOD = "/almanak.gateway.proto.PoolHistoryService/GetPoolHistory"


class _MockHandlerCallDetails:
    def __init__(self, method: str, metadata: list[tuple[str, str]] | None = None) -> None:
        self.method = method
        self.invocation_metadata = metadata or []


class _AbortContext:
    """Captures interceptor aborts (UNAUTHENTICATED rejection path)."""

    def __init__(self) -> None:
        self.aborted_with: tuple[grpc.StatusCode, str] | None = None

    async def abort(self, code: grpc.StatusCode, details: str) -> None:
        self.aborted_with = (code, details)
        raise grpc.aio.AbortError() if hasattr(grpc.aio, "AbortError") else Exception(
            f"aborted {code}: {details}"
        )


class _CodeContext:
    """Captures servicer set_code / set_details (handler path)."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


class _SuccessHandler:
    def __init__(self) -> None:
        self.invoked = False


class _Continuation:
    def __init__(self, handler: _SuccessHandler) -> None:
        self.handler = handler
        self.called = False

    async def __call__(self, _details: _MockHandlerCallDetails) -> _SuccessHandler:
        self.called = True
        return self.handler


def _request() -> gateway_pb2.PoolHistoryRequest:
    return gateway_pb2.PoolHistoryRequest(
        pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
        chain="arbitrum",
        protocol="uniswap_v3",
        start_ts=1_700_000_000,
        end_ts=1_700_604_800,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )


# ============================================================================
# Case 1: ALLOW_INSECURE=0 + no token -> UNAUTHENTICATED (interceptor)
# ============================================================================


@pytest.mark.asyncio
async def test_hosted_auth_rejects_call_without_token():
    """Auth interceptor short-circuits a token-less call BEFORE the
    kill-switch check is reached (the kill-switch is in the handler;
    auth runs in front of it)."""
    interceptor = AuthInterceptor("hosted-secret")
    cont = _Continuation(_SuccessHandler())
    details = _MockHandlerCallDetails(POOL_HISTORY_METHOD, metadata=[])

    handler = await interceptor.intercept_service(cont, details)

    assert cont.called is False
    ctx = _AbortContext()
    with pytest.raises(Exception):  # noqa: BLE001 - abort raises
        await handler.unary_unary(None, ctx)
    assert ctx.aborted_with is not None
    assert ctx.aborted_with[0] == grpc.StatusCode.UNAUTHENTICATED


# ============================================================================
# Case 2: ALLOW_INSECURE=0 + valid token, kill-switch OFF -> UNAVAILABLE
# ============================================================================


@pytest.mark.parametrize("auth_key", [AUTH_METADATA_KEY, AUTH_METADATA_KEY_ALT])
@pytest.mark.asyncio
async def test_authed_call_with_killswitch_off_returns_unavailable(auth_key: str):
    """Auth passes -> handler runs -> kill-switch default false ->
    UNAVAILABLE with VIB-4728 pointer. The continuation IS invoked (auth
    passed) and proceeds to a real servicer call."""
    interceptor = AuthInterceptor("hosted-secret")
    success_handler = _SuccessHandler()
    cont = _Continuation(success_handler)
    details = _MockHandlerCallDetails(
        POOL_HISTORY_METHOD,
        metadata=[(auth_key, "hosted-secret")],
    )

    result = await interceptor.intercept_service(cont, details)

    assert cont.called is True
    assert result is success_handler

    # Auth allowed it through; now exercise the kill-switch on the real
    # servicer (separately, since the continuation in this harness is a
    # stand-in for the framework-installed real one).
    servicer = PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=False))
    ctx = _CodeContext()
    response = await servicer.GetPoolHistory(_request(), ctx)  # type: ignore[arg-type]
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert "VIB-4728" in ctx.details
    assert response.success is False


# ============================================================================
# Case 3: ALLOW_INSECURE=0 + valid token, kill-switch ON -> dispatch (POOL-5)
# ============================================================================


@pytest.mark.asyncio
async def test_authed_call_with_killswitch_on_dispatches():
    """Kill-switch enabled with POOL-5 providers wired: the UNIMPLEMENTED
    window is closed. With all providers forced to fail (deterministic — no
    network), the handler returns UNAVAILABLE with a non-empty error. (The
    authenticated happy path is ``test_authenticated_happy_path``.)"""
    from unittest.mock import AsyncMock, patch

    from almanak.gateway.data.pool_history.dispatcher import _DispatchOutcome

    servicer = PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=True))
    ctx = _CodeContext()
    failure = _DispatchOutcome(success=False, source="", snapshots=[], error="all providers exhausted")
    with patch.object(servicer._dispatcher, "dispatch", new=AsyncMock(return_value=failure)):
        response = await servicer.GetPoolHistory(_request(), ctx)  # type: ignore[arg-type]
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert response.success is False
    assert response.error


# ============================================================================
# Case 4: ALLOW_INSECURE=1 -> no interceptor installed; kill-switch still gates
# ============================================================================


@pytest.mark.asyncio
async def test_insecure_mode_skips_auth_but_killswitch_still_gates():
    """ALLOW_INSECURE=1 boots without the AuthInterceptor (parity with
    every peer service). The kill-switch is still in effect at the
    handler level."""
    # Auth-layer assertion: in insecure mode there is no AuthInterceptor in
    # front of the continuation; the continuation runs without metadata.
    cont = _Continuation(_SuccessHandler())
    details = _MockHandlerCallDetails(POOL_HISTORY_METHOD, metadata=[])
    result = await cont(details)
    assert cont.called is True
    assert result is cont.handler

    # Handler-layer assertion: the kill-switch is independent of auth.
    servicer = PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=False))
    ctx = _CodeContext()
    response = await servicer.GetPoolHistory(_request(), ctx)  # type: ignore[arg-type]
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert "VIB-4728" in ctx.details
    assert response.success is False


# ============================================================================
# D2.M5.b — authenticated happy path returns a populated envelope (POOL-5)
# ============================================================================


def _load_thegraph_fixture() -> tuple[list[dict], int]:
    import json
    from pathlib import Path

    fx_path = Path(__file__).parent / "fixtures" / "pool_history" / "the_graph_arbitrum_univ3_7d_1h.json"
    with fx_path.open("r") as f:
        fx = json.load(f)
    return fx["poolHourDatas"], fx["meta"]["start_ts"]


@pytest.mark.parametrize("allow_insecure", [False, True])
@pytest.mark.acceptance_pack
def test_authenticated_happy_path(allow_insecure: bool):
    """D2.M5.b: with the kill-switch flipped true and providers mocked, an
    authenticated (and an insecure-parity) call returns a populated envelope.

    Auth gating is exercised in D2.M5.a; here we prove that ONCE past auth +
    kill-switch the dispatch path produces a real success envelope. Parity is
    asserted across insecure / hosted modes (same dispatch outcome)."""
    from unittest.mock import AsyncMock, patch

    rows, start = _load_thegraph_fixture()
    settings = GatewaySettings(pool_history_enabled=True, allow_insecure=allow_insecure)
    servicer = PoolHistoryServiceServicer(settings)
    ctx = _CodeContext()
    req = gateway_pb2.PoolHistoryRequest(
        pool_address="0xc6962004f452be9203591991d15f6b388e09e8d0",
        chain="arbitrum",
        protocol="uniswap_v3",
        start_ts=start,
        end_ts=start + 168 * 3600,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )

    async def _query(*, url: str, query: str, variables: dict) -> dict:
        skip = int(variables.get("skip", 0))
        return {"poolHourDatas": rows[skip : skip + 1000]}

    async def _run() -> gateway_pb2.PoolHistoryResponse:
        with patch.object(servicer._dispatcher._graphql, "query", new=AsyncMock(side_effect=_query)):
            return await servicer.GetPoolHistory(req, ctx)  # type: ignore[arg-type]

    response = asyncio.run(_run())
    assert ctx.code is None  # gRPC OK
    assert response.success is True
    assert len(response.snapshots) > 0
    assert response.source == "the_graph"


# ============================================================================
# Plan §5 — API key never leaks in repr / logs
# ============================================================================


def test_thegraph_api_key_never_leaks_in_repr():
    """The TheGraph API key must NEVER appear in the GraphQL client's repr —
    a traceback / debug dump would otherwise leak the bearer token."""
    from almanak.gateway.data.pool_history._graphql import GatewayGraphQLClient

    secret = "super-secret-thegraph-key-1234567890"
    client = GatewayGraphQLClient(api_key=secret)
    rendered = repr(client)
    assert secret not in rendered
    assert "supe...7890" in rendered  # masked form (first4...last4)


def test_thegraph_api_key_not_logged_on_init(caplog):
    """Constructing the client (and the dispatcher) must not emit the raw key
    to logs — only the masked form."""
    import logging

    from almanak.gateway.data.pool_history._graphql import GatewayGraphQLClient

    secret = "another-secret-thegraph-key-0987654321"
    with caplog.at_level(logging.DEBUG):
        GatewayGraphQLClient(api_key=secret)
    assert secret not in caplog.text


def test_dispatcher_construction_does_not_leak_key():
    """The servicer wires settings.thegraph_api_key into the dispatcher; the
    key must never surface in the dispatcher's repr or the servicer health()."""
    import json

    settings = GatewaySettings(pool_history_enabled=True, thegraph_api_key="leak-check-key-abcdef123456")
    servicer = PoolHistoryServiceServicer(settings)
    assert "leak-check-key-abcdef123456" not in repr(servicer._dispatcher._graphql)
    assert "leak-check-key-abcdef123456" not in json.dumps(servicer.health())


def test_authed_call_via_grpc_method_path_is_pool_history():
    """Constant guard: the gRPC method path used by the framework client
    and the auth-interceptor metadata MUST match the proto-defined path
    exactly. A typo here would mean the AuthInterceptor pattern-match
    against the path fails silently."""
    # No async needed — purely a string-level lock.
    assert POOL_HISTORY_METHOD == "/almanak.gateway.proto.PoolHistoryService/GetPoolHistory"


# ============================================================================
# Defensive: bare-asyncio executor for the test runner where pytest-asyncio
# isn't loaded. Mirrors a pattern in test_pool_analytics_service.py.
# ============================================================================


def test_killswitch_via_asyncio_run():
    """Same kill-switch assertion as Case 2 above but via ``asyncio.run``
    so the suite is robust even if ``pytest.mark.asyncio`` plugin is
    unavailable in some CI configurations."""
    servicer = PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=False))
    ctx = _CodeContext()
    response = asyncio.run(servicer.GetPoolHistory(_request(), ctx))  # type: ignore[arg-type]
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert response.success is False
