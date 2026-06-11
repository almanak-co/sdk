"""Tests for gateway authentication interceptor."""

import hmac

import grpc
import pytest

from almanak.gateway.auth import AUTH_METADATA_KEY, AUTH_METADATA_KEY_ALT, AuthInterceptor


class MockHandlerCallDetails:
    """Mock gRPC handler call details for testing."""

    def __init__(self, method: str, metadata: list[tuple[str, str]] | None = None):
        self.method = method
        self.invocation_metadata = metadata or []


class MockContinuation:
    """Mock continuation function for testing."""

    def __init__(self, return_value=None):
        self.called = False
        self.return_value = return_value
        self.call_details = None

    async def __call__(self, handler_call_details):
        self.called = True
        self.call_details = handler_call_details
        return self.return_value


class MockAbortContext:
    """Mock ServicerContext that records the abort status code."""

    def __init__(self):
        self.code = None
        self.message = None

    async def abort(self, code, message):
        self.code = code
        self.message = message
        raise RuntimeError("aborted")


async def _abort_code(handler) -> grpc.StatusCode:
    """Invoke a rejection handler's unary-unary behavior; return the abort code."""
    ctx = MockAbortContext()
    with pytest.raises(RuntimeError):
        await handler.unary_unary(None, ctx)
    return ctx.code


class FakeClock:
    """Injectable monotonic clock for throttle tests."""

    def __init__(self, start: float = 1000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now


class TestAuthInterceptorInit:
    """Test AuthInterceptor initialization."""

    def test_init_stores_token(self):
        """Test that token is stored on initialization."""
        interceptor = AuthInterceptor("test-token-123")
        assert interceptor.token == "test-token-123"


class TestAuthInterceptorHealthBypass:
    """Test that health checks bypass authentication."""

    @pytest.mark.asyncio
    async def test_health_check_bypasses_auth(self):
        """Test that health check requests bypass authentication."""
        interceptor = AuthInterceptor("secret-token")
        continuation = MockContinuation(return_value="handler")

        # No auth token provided but health check should pass
        details = MockHandlerCallDetails("/grpc.health.v1.Health/Check", metadata=[])

        result = await interceptor.intercept_service(continuation, details)

        assert continuation.called
        assert result == "handler"

    @pytest.mark.asyncio
    async def test_health_watch_bypasses_auth(self):
        """Test that health watch requests bypass authentication."""
        interceptor = AuthInterceptor("secret-token")
        continuation = MockContinuation(return_value="handler")

        details = MockHandlerCallDetails("/grpc.health.v1.Health/Watch", metadata=[])

        result = await interceptor.intercept_service(continuation, details)

        assert continuation.called
        assert result == "handler"


class TestAuthInterceptorReflectionBypass:
    """Test that reflection service bypasses authentication."""

    @pytest.mark.asyncio
    async def test_reflection_bypasses_auth(self):
        """Test that reflection requests bypass authentication."""
        interceptor = AuthInterceptor("secret-token")
        continuation = MockContinuation(return_value="handler")

        details = MockHandlerCallDetails(
            "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo", metadata=[]
        )

        result = await interceptor.intercept_service(continuation, details)

        assert continuation.called
        assert result == "handler"


class TestAuthInterceptorTokenValidation:
    """Test token validation in AuthInterceptor."""

    @pytest.mark.asyncio
    async def test_valid_token_allows_request(self):
        """Test that a valid token allows the request through."""
        interceptor = AuthInterceptor("correct-token")
        continuation = MockContinuation(return_value="handler")

        details = MockHandlerCallDetails(
            "/almanak.gateway.MarketService/GetPrice",
            metadata=[(AUTH_METADATA_KEY, "correct-token")],
        )

        result = await interceptor.intercept_service(continuation, details)

        assert continuation.called
        assert result == "handler"

    @pytest.mark.asyncio
    async def test_valid_token_via_alt_key(self):
        """Test that token via x-auth-token key is accepted."""
        interceptor = AuthInterceptor("correct-token")
        continuation = MockContinuation(return_value="handler")

        details = MockHandlerCallDetails(
            "/almanak.gateway.MarketService/GetPrice",
            metadata=[(AUTH_METADATA_KEY_ALT, "correct-token")],
        )

        result = await interceptor.intercept_service(continuation, details)

        assert continuation.called
        assert result == "handler"

    @pytest.mark.asyncio
    async def test_bearer_token_format_accepted(self):
        """Test that Bearer token format is properly parsed."""
        interceptor = AuthInterceptor("my-secret-token")
        continuation = MockContinuation(return_value="handler")

        details = MockHandlerCallDetails(
            "/almanak.gateway.MarketService/GetPrice",
            metadata=[(AUTH_METADATA_KEY, "Bearer my-secret-token")],
        )

        result = await interceptor.intercept_service(continuation, details)

        assert continuation.called
        assert result == "handler"

    @pytest.mark.asyncio
    async def test_missing_token_returns_unauthenticated_handler(self):
        """Test that missing token returns an unauthenticated handler."""
        interceptor = AuthInterceptor("secret-token")
        continuation = MockContinuation(return_value="handler")

        details = MockHandlerCallDetails("/almanak.gateway.MarketService/GetPrice", metadata=[])

        result = await interceptor.intercept_service(continuation, details)

        # Continuation should not be called
        assert not continuation.called
        # Result should be an RPC handler that will abort
        assert result is not None

    @pytest.mark.asyncio
    async def test_invalid_token_returns_unauthenticated_handler(self):
        """Test that invalid token returns an unauthenticated handler."""
        interceptor = AuthInterceptor("correct-token")
        continuation = MockContinuation(return_value="handler")

        details = MockHandlerCallDetails(
            "/almanak.gateway.MarketService/GetPrice",
            metadata=[(AUTH_METADATA_KEY, "wrong-token")],
        )

        result = await interceptor.intercept_service(continuation, details)

        # Continuation should not be called
        assert not continuation.called
        # Result should be an RPC handler that will abort
        assert result is not None


class TestAuthInterceptorAllServices:
    """Test that auth is enforced for all gateway services."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method",
        [
            "/almanak.gateway.MarketService/GetPrice",
            "/almanak.gateway.MarketService/GetBalance",
            "/almanak.gateway.StateService/GetState",
            "/almanak.gateway.StateService/SetState",
            "/almanak.gateway.ExecutionService/Execute",
            "/almanak.gateway.ObserveService/Log",
            "/almanak.gateway.RpcService/Call",
            "/almanak.gateway.IntegrationService/BinanceGetTicker",
        ],
    )
    async def test_auth_required_for_all_services(self, method: str):
        """Test that authentication is required for all gateway services."""
        interceptor = AuthInterceptor("secret-token")
        continuation = MockContinuation(return_value="handler")

        # Request without token
        details = MockHandlerCallDetails(method, metadata=[])

        result = await interceptor.intercept_service(continuation, details)

        # Should not call continuation for unauthenticated requests
        assert not continuation.called
        # Should return a handler that will abort
        assert result is not None


class TestAuthMetadataKeys:
    """Test that metadata keys are correctly defined."""

    def test_auth_metadata_key_value(self):
        """Test that the primary auth metadata key is 'authorization'."""
        assert AUTH_METADATA_KEY == "authorization"

    def test_auth_metadata_key_alt_value(self):
        """Test that the alternate auth metadata key is 'x-auth-token'."""
        assert AUTH_METADATA_KEY_ALT == "x-auth-token"


class TestConstantTimeCompare:
    """The token comparison must go through hmac.compare_digest on bytes."""

    @pytest.mark.asyncio
    async def test_compare_digest_used_for_validation(self, monkeypatch):
        calls = []
        real = hmac.compare_digest

        def recording(a, b):
            calls.append((a, b))
            return real(a, b)

        monkeypatch.setattr("almanak.gateway.auth.hmac.compare_digest", recording)
        interceptor = AuthInterceptor("correct-token")
        continuation = MockContinuation(return_value="handler")
        details = MockHandlerCallDetails(
            "/almanak.gateway.MarketService/GetPrice",
            metadata=[(AUTH_METADATA_KEY, "correct-token")],
        )

        result = await interceptor.intercept_service(continuation, details)

        assert continuation.called
        assert result == "handler"
        assert calls == [(b"correct-token", b"correct-token")]

    @pytest.mark.asyncio
    async def test_non_ascii_token_rejected_cleanly(self):
        """Non-ASCII metadata must be rejected, not crash str compare_digest."""
        interceptor = AuthInterceptor("correct-token")
        continuation = MockContinuation(return_value="handler")
        details = MockHandlerCallDetails(
            "/almanak.gateway.MarketService/GetPrice",
            metadata=[(AUTH_METADATA_KEY, "sécret-token")],
        )

        handler = await interceptor.intercept_service(continuation, details)

        assert not continuation.called
        assert await _abort_code(handler) == grpc.StatusCode.UNAUTHENTICATED

    @pytest.mark.asyncio
    async def test_invalid_token_aborts_with_unauthenticated_code(self):
        interceptor = AuthInterceptor("correct-token")
        continuation = MockContinuation(return_value="handler")
        details = MockHandlerCallDetails(
            "/almanak.gateway.MarketService/GetPrice",
            metadata=[(AUTH_METADATA_KEY, "wrong-token")],
        )

        handler = await interceptor.intercept_service(continuation, details)

        assert not continuation.called
        assert await _abort_code(handler) == grpc.StatusCode.UNAUTHENTICATED


class TestAuthFailureThrottle:
    """Global sliding-window throttle on the failure path (10 per 60s)."""

    METHOD = "/almanak.gateway.MarketService/GetPrice"

    def _interceptor(self, clock):
        return AuthInterceptor("correct-token", clock=clock)

    async def _fail_once(self, interceptor, metadata):
        continuation = MockContinuation(return_value="handler")
        details = MockHandlerCallDetails(self.METHOD, metadata=metadata)
        handler = await interceptor.intercept_service(continuation, details)
        assert not continuation.called
        return await _abort_code(handler)

    @pytest.mark.asyncio
    async def test_failures_below_threshold_return_unauthenticated(self):
        interceptor = self._interceptor(FakeClock())
        for _ in range(10):
            code = await self._fail_once(interceptor, [(AUTH_METADATA_KEY, "wrong")])
            assert code == grpc.StatusCode.UNAUTHENTICATED

    @pytest.mark.asyncio
    async def test_throttle_trips_after_max_failures(self):
        interceptor = self._interceptor(FakeClock())
        for _ in range(10):
            await self._fail_once(interceptor, [(AUTH_METADATA_KEY, "wrong")])
        code = await self._fail_once(interceptor, [(AUTH_METADATA_KEY, "wrong")])
        assert code == grpc.StatusCode.RESOURCE_EXHAUSTED

    @pytest.mark.asyncio
    async def test_missing_token_counts_toward_throttle(self):
        interceptor = self._interceptor(FakeClock())
        for _ in range(10):
            await self._fail_once(interceptor, [])
        code = await self._fail_once(interceptor, [])
        assert code == grpc.StatusCode.RESOURCE_EXHAUSTED

    @pytest.mark.asyncio
    async def test_valid_token_passes_while_throttled(self):
        interceptor = self._interceptor(FakeClock())
        for _ in range(11):
            await self._fail_once(interceptor, [(AUTH_METADATA_KEY, "wrong")])
        continuation = MockContinuation(return_value="handler")
        details = MockHandlerCallDetails(
            self.METHOD, metadata=[(AUTH_METADATA_KEY, "correct-token")]
        )
        result = await interceptor.intercept_service(continuation, details)
        assert continuation.called
        assert result == "handler"

    @pytest.mark.asyncio
    async def test_window_expiry_resets_throttle(self):
        clock = FakeClock()
        interceptor = self._interceptor(clock)
        for _ in range(11):
            await self._fail_once(interceptor, [(AUTH_METADATA_KEY, "wrong")])
        clock.now += 61.0
        code = await self._fail_once(interceptor, [(AUTH_METADATA_KEY, "wrong")])
        assert code == grpc.StatusCode.UNAUTHENTICATED
