"""Tests for service-side upstream-error mapping (VIB-3800)."""

from __future__ import annotations

import grpc
import pytest

from almanak.framework.data.interfaces import (
    AllDataSourcesFailed,
    DataSourceRateLimited,
    DataSourceTimeout,
    DataSourceUnavailable,
    data_source_error_from_grpc,
)
from almanak.framework.grpc.error_details import unpack_status_details
from almanak.gateway.integrations.base import IntegrationError, IntegrationRateLimitError
from almanak.gateway.services._grpc_errors import set_error_from_upstream


class _FakeContext:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str | None = None
        self.trailing: list[tuple[str, bytes]] | None = None

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details

    def set_trailing_metadata(self, trailing) -> None:
        self.trailing = list(trailing)


class _FakeRpcError(Exception):
    def __init__(self, trailing) -> None:
        self._trailing = trailing

    def trailing_metadata(self):
        return self._trailing


class TestUpstreamMapping:
    def test_rate_limit_with_retry_after(self) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            IntegrationRateLimitError("geckoterminal", retry_after=3.0),
            upstream="geckoterminal",
        )
        assert ctx.code == grpc.StatusCode.RESOURCE_EXHAUSTED

        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_RATE_LIMITED"
        assert details.retry_delay_seconds == pytest.approx(3.0)
        assert details.upstream == "geckoterminal"

    def test_timeout_code(self) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", "request timed out", code="TIMEOUT"),
            upstream="binance",
        )
        assert ctx.code == grpc.StatusCode.DEADLINE_EXCEEDED
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_TIMEOUT"

    def test_network_error(self) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", "connection reset", code="NETWORK_ERROR"),
            upstream="binance",
        )
        assert ctx.code == grpc.StatusCode.UNAVAILABLE
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_NETWORK_ERROR"

    @pytest.mark.parametrize("status", [500, 502, 503, 504])
    def test_http_5xx_maps_to_unavailable(self, status: int) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", f"HTTP {status}: down", code=f"HTTP_{status}"),
            upstream="binance",
        )
        assert ctx.code == grpc.StatusCode.UNAVAILABLE
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_HTTP_5XX"
        assert details.metadata.get("integration_code") == f"HTTP_{status}"

    def test_http_429_falls_back_to_rate_limited(self) -> None:
        # Defensive — should normally be IntegrationRateLimitError, but if a
        # caller bypasses that we still classify correctly.
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", "HTTP 429", code="HTTP_429"),
            upstream="binance",
        )
        assert ctx.code == grpc.StatusCode.RESOURCE_EXHAUSTED
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_RATE_LIMITED"

    @pytest.mark.parametrize("status", [400, 401, 403, 404])
    def test_http_4xx_maps_to_failed_precondition(self, status: int) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", f"HTTP {status}", code=f"HTTP_{status}"),
            upstream="binance",
        )
        assert ctx.code == grpc.StatusCode.FAILED_PRECONDITION
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_HTTP_4XX"

    @pytest.mark.parametrize("suffix", ["5XX", "5xx"])
    def test_http_5xx_wildcard_maps_to_unavailable(self, suffix: str) -> None:
        # Defensive: integrations that know the class but not the specific
        # status emit HTTP_5XX. Pre-fix this fell into UPSTREAM_UNKNOWN
        # because int("5XX") raised ValueError.
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", "server failure", code=f"HTTP_{suffix}"),
            upstream="binance",
        )
        assert ctx.code == grpc.StatusCode.UNAVAILABLE
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_HTTP_5XX"

    @pytest.mark.parametrize("suffix", ["4XX", "4xx"])
    def test_http_4xx_wildcard_maps_to_failed_precondition(self, suffix: str) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", "client error", code=f"HTTP_{suffix}"),
            upstream="binance",
        )
        assert ctx.code == grpc.StatusCode.FAILED_PRECONDITION
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_HTTP_4XX"

    def test_unknown_integration_code_maps_to_unavailable(self) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", "weird", code="WAT"),
            upstream="binance",
        )
        assert ctx.code == grpc.StatusCode.UNAVAILABLE
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_UNKNOWN"

    def test_data_source_rate_limited_propagates_retry(self) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            DataSourceRateLimited(source="upstream", retry_after=4.0),
            upstream="upstream",
        )
        assert ctx.code == grpc.StatusCode.RESOURCE_EXHAUSTED
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_RATE_LIMITED"
        assert details.retry_delay_seconds == pytest.approx(4.0)

    def test_data_source_timeout(self) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            DataSourceTimeout(source="upstream", timeout_seconds=10.0),
            upstream="upstream",
        )
        assert ctx.code == grpc.StatusCode.DEADLINE_EXCEEDED

    def test_data_source_timeout_propagates_retry_after(self) -> None:
        # Regression for CodeRabbit finding: DataSourceTimeout.retry_after was
        # being dropped on the way out (hardcoded to None). It must reach the
        # RetryInfo trailer so clients can honor the upstream-advised delay.
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            DataSourceTimeout(source="upstream", timeout_seconds=10.0, retry_after=2.5),
            upstream="upstream",
        )
        assert ctx.code == grpc.StatusCode.DEADLINE_EXCEEDED
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_TIMEOUT"
        assert details.retry_delay_seconds == pytest.approx(2.5)

    def test_data_source_unavailable(self) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            DataSourceUnavailable(source="upstream", reason="down", retry_after=None),
            upstream="upstream",
        )
        assert ctx.code == grpc.StatusCode.UNAVAILABLE
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_UNAVAILABLE"

    def test_all_sources_failed(self) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(
            ctx,
            AllDataSourcesFailed(errors={"a": "x", "b": "y"}),
            upstream="price_aggregator",
        )
        assert ctx.code == grpc.StatusCode.UNAVAILABLE
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "ALL_SOURCES_FAILED"
        assert details.upstream == "price_aggregator"

    def test_python_timeout_error(self) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(ctx, TimeoutError("blew through deadline"), upstream="upstream")
        assert ctx.code == grpc.StatusCode.DEADLINE_EXCEEDED
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "UPSTREAM_TIMEOUT"

    def test_unknown_exception_maps_to_internal(self) -> None:
        ctx = _FakeContext()
        set_error_from_upstream(ctx, ValueError("boom"), upstream="upstream")
        assert ctx.code == grpc.StatusCode.INTERNAL
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert details.reason == "GATEWAY_INTERNAL"

    def test_internal_does_not_leak_exception_text(self) -> None:
        # Regression for CodeRabbit finding: the INTERNAL fallback used to
        # echo str(exc) across the trust boundary. The fixed message must NOT
        # contain the original exception text — it leaks stack frames,
        # secrets, or library internals to clients.
        ctx = _FakeContext()
        secret_marker = "do-not-leak-this-string-12345"
        set_error_from_upstream(ctx, ValueError(secret_marker), upstream="upstream")
        assert ctx.code == grpc.StatusCode.INTERNAL
        assert secret_marker not in (ctx.details or "")
        details = unpack_status_details(_FakeRpcError(ctx.trailing))
        assert details is not None
        assert secret_marker not in details.message


class TestDataSourceErrorFromGrpc:
    """Round-trip: gateway packs typed error → client unpacks to typed exception."""

    def _ctx_to_rpc_error(self) -> _FakeContext:
        return _FakeContext()

    def test_rate_limited_round_trip(self) -> None:
        ctx = self._ctx_to_rpc_error()
        set_error_from_upstream(
            ctx,
            IntegrationRateLimitError("geckoterminal", retry_after=2.0),
            upstream="geckoterminal",
        )
        rpc_error = _FakeRpcError(ctx.trailing)
        typed = data_source_error_from_grpc(rpc_error)
        assert isinstance(typed, DataSourceRateLimited)
        assert typed.source == "geckoterminal"
        assert typed.retry_after == pytest.approx(2.0)

    def test_timeout_round_trip(self) -> None:
        ctx = self._ctx_to_rpc_error()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", "timed out", code="TIMEOUT"),
            upstream="binance",
        )
        rpc_error = _FakeRpcError(ctx.trailing)
        typed = data_source_error_from_grpc(rpc_error)
        assert isinstance(typed, DataSourceTimeout)
        assert typed.source == "binance"
        # Reconstructed timeout has no observed duration — pre-fix this
        # silently smuggled retry_delay into timeout_seconds (wrong field).
        assert typed.timeout_seconds == 0.0

    def test_timeout_round_trip_preserves_retry_after(self) -> None:
        # End-to-end: gateway packs DataSourceTimeout(retry_after=1.5) and
        # client unpacks to a typed DataSourceTimeout where retry_after is
        # set, timeout_seconds is 0 (unknown), and the values do NOT swap.
        ctx = self._ctx_to_rpc_error()
        set_error_from_upstream(
            ctx,
            DataSourceTimeout(source="binance", timeout_seconds=10.0, retry_after=1.5),
            upstream="binance",
        )
        rpc_error = _FakeRpcError(ctx.trailing)
        typed = data_source_error_from_grpc(rpc_error)
        assert isinstance(typed, DataSourceTimeout)
        assert typed.retry_after == pytest.approx(1.5)
        assert typed.timeout_seconds == 0.0

    def test_unavailable_round_trip(self) -> None:
        ctx = self._ctx_to_rpc_error()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", "HTTP 503", code="HTTP_503"),
            upstream="binance",
        )
        rpc_error = _FakeRpcError(ctx.trailing)
        typed = data_source_error_from_grpc(rpc_error)
        assert isinstance(typed, DataSourceUnavailable)
        assert typed.source == "binance"

    def test_no_typed_trailer_returns_none(self) -> None:
        rpc_error = _FakeRpcError([])
        assert data_source_error_from_grpc(rpc_error) is None

    def test_failed_precondition_returns_none(self) -> None:
        # 4xx errors are caller bugs, not data-source errors — caller should
        # decide how to handle, not auto-retry.
        ctx = self._ctx_to_rpc_error()
        set_error_from_upstream(
            ctx,
            IntegrationError("binance", "bad request", code="HTTP_400"),
            upstream="binance",
        )
        rpc_error = _FakeRpcError(ctx.trailing)
        assert data_source_error_from_grpc(rpc_error) is None
