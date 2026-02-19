"""Gateway authentication interceptor for gRPC.

This module provides token-based authentication for the gateway gRPC server.
When ALMANAK_GATEWAY_AUTH_TOKEN is set, clients must provide the token
in request metadata to access gateway services.
"""

import logging
from collections.abc import Awaitable, Callable

import grpc

logger = logging.getLogger(__name__)

# Metadata keys for authentication token
AUTH_METADATA_KEY = "authorization"
AUTH_METADATA_KEY_ALT = "x-auth-token"


class AuthInterceptor(grpc.aio.ServerInterceptor):
    """gRPC server interceptor that enforces token-based authentication.

    When a token is configured, all incoming requests must include the token
    in their metadata (either 'authorization' or 'x-auth-token' key).
    Requests without a valid token are rejected with UNAUTHENTICATED status.

    The health check service is exempt from authentication to allow
    container orchestrators to probe the gateway.
    """

    def __init__(self, token: str):
        """Initialize the auth interceptor.

        Args:
            token: The shared secret token that clients must provide.
        """
        self.token = token

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
            logger.warning("Authentication failed: no token provided for method %s", method)
            return self._create_unauthenticated_handler("No authentication token provided")

        if provided_token != self.token:
            logger.warning("Authentication failed: invalid token for method %s", method)
            return self._create_unauthenticated_handler("Invalid authentication token")

        # Token is valid - proceed with the request
        return await continuation(handler_call_details)

    def _create_unauthenticated_handler(self, message: str) -> grpc.RpcMethodHandler:
        """Create a handler that returns UNAUTHENTICATED error.

        Args:
            message: Error message to include in the response

        Returns:
            An RPC method handler that rejects with UNAUTHENTICATED
        """

        async def abort_unary_unary(request, context: grpc.aio.ServicerContext):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, message)

        async def abort_unary_stream(request, context: grpc.aio.ServicerContext):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, message)
            # This won't be reached but is needed for the generator type
            return
            yield  # noqa: RET507 - Required for async generator type

        async def abort_stream_unary(request_iterator, context: grpc.aio.ServicerContext):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, message)

        async def abort_stream_stream(request_iterator, context: grpc.aio.ServicerContext):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, message)
            return
            yield  # noqa: RET507 - Required for async generator type

        # Return a handler that works for any RPC type
        return grpc.unary_unary_rpc_method_handler(abort_unary_unary)
