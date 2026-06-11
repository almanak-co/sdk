"""Gateway authentication interceptor for gRPC.

This module provides token-based authentication for the gateway gRPC server.
When ALMANAK_GATEWAY_AUTH_TOKEN is set, clients must provide the token
in request metadata to access gateway services.
"""

import hmac
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable

import grpc

logger = logging.getLogger(__name__)

# Metadata keys for authentication token
AUTH_METADATA_KEY = "authorization"
AUTH_METADATA_KEY_ALT = "x-auth-token"

# Failed-auth throttle: after MAX_AUTH_FAILURES failed attempts within
# AUTH_FAILURE_WINDOW_SECONDS, further failed attempts are rejected with
# RESOURCE_EXHAUSTED instead of UNAUTHENTICATED. Valid tokens are never
# throttled (counted on the failure path only), so the gateway's single
# legitimate client cannot be locked out by an attacker.
MAX_AUTH_FAILURES = 10
AUTH_FAILURE_WINDOW_SECONDS = 60.0


class AuthInterceptor(grpc.aio.ServerInterceptor):
    """gRPC server interceptor that enforces token-based authentication.

    When a token is configured, all incoming requests must include the token
    in their metadata (either 'authorization' or 'x-auth-token' key).
    Requests without a valid token are rejected with UNAUTHENTICATED status.

    The health check service is exempt from authentication to allow
    container orchestrators to probe the gateway.
    """

    def __init__(
        self,
        token: str,
        *,
        max_failures: int = MAX_AUTH_FAILURES,
        window_seconds: float = AUTH_FAILURE_WINDOW_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ):
        """Initialize the auth interceptor.

        Args:
            token: The shared secret token that clients must provide.
            max_failures: Failed attempts tolerated per sliding window before
                further failures are rejected with RESOURCE_EXHAUSTED.
            window_seconds: Length of the sliding failure window in seconds.
            clock: Monotonic time source; injectable for tests.
        """
        self.token = token
        self._token_bytes = token.encode("utf-8")
        self._max_failures = max_failures
        self._window_seconds = window_seconds
        self._clock = clock
        # Bounded: we only ever need to know whether the window holds MORE
        # than max_failures entries, so max_failures + 1 timestamps suffice.
        self._failure_times: deque[float] = deque(maxlen=max_failures + 1)

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        """Intercept gRPC calls to enforce authentication.

        Args:
            continuation: The next interceptor or handler
            handler_call_details: Details about the call including metadata

        Returns:
            The RPC method handler if authenticated

        Raises:
            Rejects with UNAUTHENTICATED if token is missing or invalid
        """
        # Skip authentication for health checks - needed for container probes
        method = handler_call_details.method
        if "/grpc.health.v1.Health/" in method:
            return await continuation(handler_call_details)

        # Also skip gRPC reflection for development tooling
        if "/grpc.reflection.v1alpha.ServerReflection/" in method:
            return await continuation(handler_call_details)

        # Extract token from metadata
        metadata = dict(handler_call_details.invocation_metadata or [])
        provided_token = metadata.get(AUTH_METADATA_KEY) or metadata.get(AUTH_METADATA_KEY_ALT)

        # Handle "Bearer <token>" format
        if provided_token and provided_token.startswith("Bearer "):
            provided_token = provided_token[7:]

        # Validate token
        if not provided_token:
            if self._record_failure():
                return self._create_throttled_handler(method)
            logger.warning("Authentication failed: no token provided for method %s", method)
            return self._create_abort_handler(grpc.StatusCode.UNAUTHENTICATED, "No authentication token provided")

        # Constant-time comparison over UTF-8 bytes: a plain `!=` on str
        # short-circuits on the first mismatched byte and leaks a timing
        # oracle on the shared secret.
        if not hmac.compare_digest(provided_token.encode("utf-8"), self._token_bytes):
            if self._record_failure():
                return self._create_throttled_handler(method)
            logger.warning("Authentication failed: invalid token for method %s", method)
            return self._create_abort_handler(grpc.StatusCode.UNAUTHENTICATED, "Invalid authentication token")

        # Token is valid - proceed with the request
        return await continuation(handler_call_details)

    def _record_failure(self) -> bool:
        """Record a failed auth attempt; return True when the throttle is tripped.

        Sliding window over a monotonic clock. Single-threaded by construction:
        grpc.aio runs interceptors on one event loop and there is no await
        between the deque operations, so no lock is needed.
        """
        now = self._clock()
        cutoff = now - self._window_seconds
        while self._failure_times and self._failure_times[0] <= cutoff:
            self._failure_times.popleft()
        self._failure_times.append(now)
        return len(self._failure_times) > self._max_failures

    def _create_throttled_handler(self, method: str) -> grpc.RpcMethodHandler:
        """Reject a brute-force burst with RESOURCE_EXHAUSTED (operator signal)."""
        logger.warning(
            "Authentication throttled: %d failed attempts within %.0fs window; "
            "rejecting method %s with RESOURCE_EXHAUSTED",
            len(self._failure_times),
            self._window_seconds,
            method,
        )
        return self._create_abort_handler(
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            "Too many failed authentication attempts",
        )

    def _create_abort_handler(self, code: grpc.StatusCode, message: str) -> grpc.RpcMethodHandler:
        """Create a handler that returns the given gRPC status code.

        Args:
            code: The gRPC status code to abort with.
            message: Error message to include in the response

        Returns:
            An RPC method handler that rejects with the given status code
        """

        async def abort_unary_unary(request, context: grpc.aio.ServicerContext):
            await context.abort(code, message)

        async def abort_unary_stream(request, context: grpc.aio.ServicerContext):
            await context.abort(code, message)
            # This won't be reached but is needed for the generator type
            return
            yield  # noqa: RET507 - Required for async generator type

        async def abort_stream_unary(request_iterator, context: grpc.aio.ServicerContext):
            await context.abort(code, message)

        async def abort_stream_stream(request_iterator, context: grpc.aio.ServicerContext):
            await context.abort(code, message)
            return
            yield  # noqa: RET507 - Required for async generator type

        # Return a handler that works for any RPC type
        return grpc.unary_unary_rpc_method_handler(abort_unary_unary)
