"""Tests for gateway authentication interceptor."""

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
