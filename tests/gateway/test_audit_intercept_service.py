"""Tests for AuditInterceptor.intercept_service handler dispatch.

Builds fake HandlerCallDetails + continuation pairs (no gRPC server) and
covers: the disabled early-out, the Health-service skip, the None-handler
passthrough, all four RPC-kind wrap branches (including serializer
propagation and audit-record emission through a wrapped unary-unary
behavior), and the no-kind fall-through that returns the handler as-is.

Wrapper error/cancellation semantics are covered by test_audit.py.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import grpc
import pytest

from almanak.gateway.audit import AuditInterceptor

METHOD = "/almanak.gateway.MarketService/GetPrice"
HEALTH_METHOD = "/grpc.health.v1.Health/Check"


class FakeCallDetails:
    """Minimal grpc.HandlerCallDetails stand-in."""

    def __init__(self, method: str = METHOD) -> None:
        self.method = method


def _continuation(handler):
    """Async continuation returning a fixed handler; records lookups."""
    calls: list = []

    async def continuation(details):
        calls.append(details)
        return handler

    return continuation, calls


def _intercept(interceptor, handler, method: str = METHOD):
    continuation, calls = _continuation(handler)
    result = asyncio.run(interceptor.intercept_service(continuation, FakeCallDetails(method)))
    return result, calls


async def _behavior(request, context):
    return "resp"


async def _stream_behavior(request, context):
    yield "chunk"


@pytest.fixture
def interceptor():
    return AuditInterceptor()


class TestPassthroughBranches:
    def test_disabled_returns_continuation_result_unwrapped(self):
        handler = grpc.unary_unary_rpc_method_handler(_behavior)
        result, calls = _intercept(AuditInterceptor(enabled=False), handler)
        assert result is handler
        assert len(calls) == 1

    def test_health_service_skips_audit_wrapping(self, interceptor):
        handler = grpc.unary_unary_rpc_method_handler(_behavior)
        result, calls = _intercept(interceptor, handler, method=HEALTH_METHOD)
        assert result is handler
        assert len(calls) == 1

    def test_none_handler_returned_as_is(self, interceptor):
        result, calls = _intercept(interceptor, None)
        assert result is None
        assert len(calls) == 1

    def test_handler_without_any_rpc_kind_falls_through(self, interceptor):
        handler = SimpleNamespace(
            unary_unary=None,
            unary_stream=None,
            stream_unary=None,
            stream_stream=None,
            request_deserializer=None,
            response_serializer=None,
        )
        result, _ = _intercept(interceptor, handler)
        assert result is handler


class TestWrapBranches:
    def test_unary_unary_wrapped_and_emits_audit_record(self, interceptor):
        deserializer = MagicMock(name="deserializer")
        serializer = MagicMock(name="serializer")
        handler = grpc.unary_unary_rpc_method_handler(
            _behavior,
            request_deserializer=deserializer,
            response_serializer=serializer,
        )

        result, _ = _intercept(interceptor, handler)

        assert result is not handler
        assert result.unary_unary is not _behavior
        assert result.request_deserializer is deserializer
        assert result.response_serializer is serializer

        with patch("almanak.gateway.audit.log_audit_record") as mock_log:
            response = asyncio.run(result.unary_unary(MagicMock(), MagicMock()))

        assert response == "resp"
        record = mock_log.call_args[0][0]
        assert record.service == "MarketService"
        assert record.method == "GetPrice"
        assert record.success is True

    def test_unary_stream_wrapped_with_serializers(self, interceptor):
        deserializer = MagicMock(name="deserializer")
        serializer = MagicMock(name="serializer")
        handler = grpc.unary_stream_rpc_method_handler(
            _stream_behavior,
            request_deserializer=deserializer,
            response_serializer=serializer,
        )

        result, _ = _intercept(interceptor, handler)

        assert result is not handler
        assert result.unary_stream is not _stream_behavior
        assert result.unary_unary is None
        assert result.request_deserializer is deserializer
        assert result.response_serializer is serializer

    def test_stream_unary_wrapped_with_serializers(self, interceptor):
        deserializer = MagicMock(name="deserializer")
        serializer = MagicMock(name="serializer")
        handler = grpc.stream_unary_rpc_method_handler(
            _behavior,
            request_deserializer=deserializer,
            response_serializer=serializer,
        )

        result, _ = _intercept(interceptor, handler)

        assert result is not handler
        assert result.stream_unary is not _behavior
        assert result.unary_unary is None
        assert result.request_deserializer is deserializer
        assert result.response_serializer is serializer

    def test_stream_stream_wrapped_with_serializers(self, interceptor):
        deserializer = MagicMock(name="deserializer")
        serializer = MagicMock(name="serializer")
        handler = grpc.stream_stream_rpc_method_handler(
            _stream_behavior,
            request_deserializer=deserializer,
            response_serializer=serializer,
        )

        result, _ = _intercept(interceptor, handler)

        assert result is not handler
        assert result.stream_stream is not _stream_behavior
        assert result.unary_unary is None
        assert result.request_deserializer is deserializer
        assert result.response_serializer is serializer
