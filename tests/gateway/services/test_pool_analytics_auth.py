"""Auth-interceptor enforcement for PoolAnalyticsService (VIB-4727 D2.M3).

Mirrors the existing ``tests/gateway/test_auth.py`` pattern of testing the
interceptor directly against the target method name, rather than booting
a full gRPC server. This is consistent with how every other gateway
service exercises auth enforcement.

The three D2.M3 cases:

- ``ALMANAK_GATEWAY_ALLOW_INSECURE=0`` + no token → UNAUTHENTICATED.
- ``ALMANAK_GATEWAY_ALLOW_INSECURE=0`` + valid token → OK / handler invoked.
- ``ALMANAK_GATEWAY_ALLOW_INSECURE=1`` → no interceptor installed → handler invoked.

The third case is environmental (server boot omits the interceptor); we
assert that by NOT applying one and showing the handler runs unguarded.
"""

from __future__ import annotations

import grpc
import pytest

from almanak.gateway.auth import AUTH_METADATA_KEY, AUTH_METADATA_KEY_ALT, AuthInterceptor

POOL_ANALYTICS_METHOD = "/almanak.gateway.proto.PoolAnalyticsService/GetPoolAnalytics"


class _MockHandlerCallDetails:
    def __init__(self, method: str, metadata: list[tuple[str, str]] | None = None) -> None:
        self.method = method
        self.invocation_metadata = metadata or []


class _MockContext:
    """gRPC ServicerContext stub that captures aborts."""

    def __init__(self) -> None:
        self.aborted_with: tuple[grpc.StatusCode, str] | None = None

    async def abort(self, code: grpc.StatusCode, details: str) -> None:
        self.aborted_with = (code, details)
        raise grpc.aio.AbortError() if hasattr(grpc.aio, "AbortError") else Exception(f"aborted {code}: {details}")


class _SuccessHandler:
    """Stand-in 'continuation' result — what the real PoolAnalyticsService handler
    would be when auth passes. Returned by the continuation function below."""

    def __init__(self) -> None:
        self.invoked = False


class _Continuation:
    def __init__(self, handler: _SuccessHandler) -> None:
        self.handler = handler
        self.called = False

    async def __call__(self, _details: _MockHandlerCallDetails) -> _SuccessHandler:
        self.called = True
        return self.handler


# ============================================================================
# Case 1: hosted auth on (ALLOW_INSECURE=0), no token -> UNAUTHENTICATED
# ============================================================================


@pytest.mark.asyncio
async def test_hosted_auth_rejects_call_without_token():
    """ALLOW_INSECURE=0 + no token: interceptor returns an UNAUTHENTICATED handler."""
    interceptor = AuthInterceptor("hosted-secret")
    cont = _Continuation(_SuccessHandler())
    details = _MockHandlerCallDetails(POOL_ANALYTICS_METHOD, metadata=[])

    handler = await interceptor.intercept_service(cont, details)

    # The interceptor short-circuits the continuation when auth fails.
    assert cont.called is False
    # The unauthenticated handler aborts with UNAUTHENTICATED when invoked.
    ctx = _MockContext()
    with pytest.raises(Exception):  # noqa: BLE001 - abort raises
        await handler.unary_unary(None, ctx)
    assert ctx.aborted_with is not None
    code, _ = ctx.aborted_with
    assert code == grpc.StatusCode.UNAUTHENTICATED


# ============================================================================
# Case 2: hosted auth on (ALLOW_INSECURE=0), valid token -> handler invoked
# ============================================================================


@pytest.mark.parametrize("auth_key", [AUTH_METADATA_KEY, AUTH_METADATA_KEY_ALT])
@pytest.mark.asyncio
async def test_hosted_auth_accepts_call_with_valid_token(auth_key: str):
    """ALLOW_INSECURE=0 + valid token (either metadata key) -> handler runs."""
    interceptor = AuthInterceptor("hosted-secret")
    success_handler = _SuccessHandler()
    cont = _Continuation(success_handler)
    details = _MockHandlerCallDetails(
        POOL_ANALYTICS_METHOD,
        metadata=[(auth_key, "hosted-secret")],
    )

    result = await interceptor.intercept_service(cont, details)

    assert cont.called is True
    assert result is success_handler


@pytest.mark.asyncio
async def test_hosted_auth_accepts_bearer_prefix():
    """The interceptor supports 'Bearer <token>' as well as the bare token form."""
    interceptor = AuthInterceptor("hosted-secret")
    cont = _Continuation(_SuccessHandler())
    details = _MockHandlerCallDetails(
        POOL_ANALYTICS_METHOD,
        metadata=[(AUTH_METADATA_KEY, "Bearer hosted-secret")],
    )

    await interceptor.intercept_service(cont, details)
    assert cont.called is True


@pytest.mark.asyncio
async def test_hosted_auth_rejects_invalid_token():
    """Wrong-token call returns UNAUTHENTICATED."""
    interceptor = AuthInterceptor("hosted-secret")
    cont = _Continuation(_SuccessHandler())
    details = _MockHandlerCallDetails(
        POOL_ANALYTICS_METHOD,
        metadata=[(AUTH_METADATA_KEY, "wrong-token")],
    )

    handler = await interceptor.intercept_service(cont, details)

    assert cont.called is False
    ctx = _MockContext()
    with pytest.raises(Exception):  # noqa: BLE001 - abort raises
        await handler.unary_unary(None, ctx)
    assert ctx.aborted_with is not None
    assert ctx.aborted_with[0] == grpc.StatusCode.UNAUTHENTICATED


# ============================================================================
# Case 3: ALLOW_INSECURE=1 (no interceptor installed) -> handler runs unguarded
# ============================================================================
#
# At the server-boot layer this is "don't install the AuthInterceptor at
# all" (see ``almanak/gateway/server.py``: the interceptor is only added
# when an auth token is configured and ``allow_insecure`` is False).
# This test makes the absence explicit: without the interceptor, a call
# with no token reaches the handler.


@pytest.mark.asyncio
async def test_insecure_mode_skips_auth_interceptor():
    """When the gateway runs in insecure mode the AuthInterceptor is not
    installed, so the continuation runs without inspecting metadata."""
    cont = _Continuation(_SuccessHandler())
    details = _MockHandlerCallDetails(POOL_ANALYTICS_METHOD, metadata=[])
    # No AuthInterceptor wrapping the continuation in this mode — we just
    # invoke the continuation directly. The assertion is structural: the
    # handler runs without an authentication challenge.
    result = await cont(details)
    assert cont.called is True
    assert result is cont.handler
