"""Unit tests for `_RateLimitRetryInterceptor` (ALM-2883 / ALM-3025).

The gateway answers a saturated rate-limit bucket with `RESOURCE_EXHAUSTED`
*and the exact time to come back*. Nothing acted on that hint: a running
deployment silently skipped its price-impact guard (ALM-2883) and a
managed-Anvil GMX lifecycle abandoned settlement 37s short of recovery
(ALM-3025). These tests pin the retry behaviour and — more importantly — the
three cases that must NOT retry.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import grpc
import pytest

from almanak.framework.gateway_client import _RateLimitRetryInterceptor, _service_name
from almanak.framework.grpc.error_details import pack_status_details

_RPC_METHOD = "/almanak.gateway.proto.RpcService/Call"
_EXECUTE_METHOD = "/almanak.gateway.proto.ExecutionService/Execute"


class _FakeRpcError(grpc.RpcError):
    """Stand-in for `_InactiveRpcError` (which cannot be constructed directly)."""

    def __init__(
        self,
        code: grpc.StatusCode,
        details: str = "",
        trailing_metadata: list[tuple[str, bytes]] | None = None,
    ):
        self._code = code
        self._details = details
        self._trailing = trailing_metadata or []

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def trailing_metadata(self):
        return self._trailing

    def __str__(self) -> str:
        return f"{self._code}: {self._details}"


def _rate_limited(seconds: float = 37.71) -> _FakeRpcError:
    """The RpcService shape: plain-text hint, no typed trailer."""
    return _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, f"Rate limited, retry after {seconds:.2f}s")


def _typed_rate_limited(seconds: float | None, reason: str | None = "UPSTREAM_RATE_LIMITED") -> _FakeRpcError:
    """The MarketService / IntegrationService shape: google.rpc typed trailer."""
    _code, message, trailing = pack_status_details(
        code=grpc.StatusCode.RESOURCE_EXHAUSTED,
        message="upstream rate limited",
        retry_delay_seconds=seconds,
        reason=reason,
    )
    return _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, message, trailing)


def _details(method: str = _RPC_METHOD) -> MagicMock:
    call_details = MagicMock()
    call_details.method = method
    return call_details


def _run(interceptor, outcomes, method: str = _RPC_METHOD):
    """Drive the interceptor over a scripted sequence of continuation outcomes."""
    calls: list[object] = []

    def continuation(details, request):
        calls.append(details)
        return outcomes[len(calls) - 1]

    with patch("almanak.framework.gateway_client.time.sleep") as sleep:
        result = interceptor.intercept_unary_unary(continuation, _details(method), object())
    return result, calls, sleep


class TestRetriesRateLimits:
    def test_retries_after_the_server_hinted_delay(self):
        """The text hint is honoured verbatim, and the retry's result is returned."""
        ok = MagicMock(name="ok")
        result, calls, sleep = _run(_RateLimitRetryInterceptor(), [_rate_limited(37.71), ok])

        assert result is ok
        assert len(calls) == 2
        sleep.assert_called_once_with(pytest.approx(37.71))

    def test_honours_the_typed_retryinfo_trailer(self):
        """RetryInfo takes precedence over text parsing (VIB-3800 contract)."""
        ok = MagicMock(name="ok")
        result, calls, sleep = _run(_RateLimitRetryInterceptor(), [_typed_rate_limited(12.5), ok])

        assert result is ok
        assert len(calls) == 2
        sleep.assert_called_once_with(pytest.approx(12.5))

    def test_backs_off_by_default_when_flagged_without_a_delay(self):
        """An upstream 429 with no Retry-After still retries, on our own backoff."""
        ok = MagicMock(name="ok")
        interceptor = _RateLimitRetryInterceptor()
        result, calls, sleep = _run(interceptor, [_typed_rate_limited(None), ok])

        assert result is ok
        assert len(calls) == 2
        sleep.assert_called_once_with(pytest.approx(interceptor._DEFAULT_BACKOFF_SECONDS))

    def test_zero_second_hint_retries_immediately(self):
        """The bucket can free up between check and reply; 0s means "now", not "give up"."""
        ok = MagicMock(name="ok")
        result, calls, sleep = _run(_RateLimitRetryInterceptor(), [_rate_limited(0.0), ok])

        assert result is ok
        assert len(calls) == 2
        sleep.assert_called_once_with(pytest.approx(0.0))

    def test_gives_up_after_max_attempts_and_surfaces_the_error(self):
        """A persistent rate limit is returned, not retried forever."""
        interceptor = _RateLimitRetryInterceptor()
        final = _rate_limited(1.0)
        result, calls, sleep = _run(interceptor, [_rate_limited(1.0), _rate_limited(1.0), final])

        assert result is final
        assert len(calls) == interceptor._MAX_ATTEMPTS + 1
        assert sleep.call_count == interceptor._MAX_ATTEMPTS

    def test_total_sleep_is_capped(self):
        """Cumulative backoff cannot exceed the budget, whatever the server asks."""
        interceptor = _RateLimitRetryInterceptor()
        _result, _calls, sleep = _run(interceptor, [_rate_limited(60.0), _rate_limited(60.0), _rate_limited(60.0)])

        assert sum(call.args[0] for call in sleep.call_args_list) <= interceptor._MAX_TOTAL_SLEEP_SECONDS


class TestDoesNotRetry:
    def test_success_passes_straight_through(self):
        ok = MagicMock(name="ok")
        result, calls, sleep = _run(_RateLimitRetryInterceptor(), [ok])

        assert result is ok
        assert len(calls) == 1
        sleep.assert_not_called()

    def test_auth_throttle_is_not_retried(self):
        """`gateway/auth.py` rejects a brute-force burst with a bare
        RESOURCE_EXHAUSTED — no delay hint, no reason. Retrying would hammer a
        lockout, so the bare shape must pass straight through."""
        lockout = _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "Too many failed authentication attempts")
        result, calls, sleep = _run(_RateLimitRetryInterceptor(), [lockout])

        assert result is lockout
        assert len(calls) == 1
        sleep.assert_not_called()

    @pytest.mark.parametrize(
        "code",
        [
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.UNAUTHENTICATED,
            grpc.StatusCode.INVALID_ARGUMENT,
        ],
    )
    def test_other_status_codes_are_not_retried(self, code):
        """Only rate limits. DEADLINE_EXCEEDED especially: a timed-out
        `eth_sendTransaction` may still be in flight, so a blind retry could
        double-submit."""
        error = _FakeRpcError(code, "retry after 5s")
        result, calls, sleep = _run(_RateLimitRetryInterceptor(), [error])

        assert result is error
        assert len(calls) == 1
        sleep.assert_not_called()

    def test_execution_service_is_never_retried(self):
        """ExecutionService signs and submits; `Execute` is not idempotent."""
        rate_limited = _rate_limited(1.0)
        result, calls, sleep = _run(_RateLimitRetryInterceptor(), [rate_limited], method=_EXECUTE_METHOD)

        assert result is rate_limited
        assert len(calls) == 1
        sleep.assert_not_called()

    def test_execution_service_exclusion_survives_a_package_rename(self):
        """The guard matches the bare service name, so it cannot fail open if
        the proto package moves."""
        assert _service_name("/almanak.gateway.proto.ExecutionService/Execute") == "ExecutionService"
        assert _service_name("/some.other.pkg.ExecutionService/Execute") == "ExecutionService"
        assert _service_name("/ExecutionService/Execute") == "ExecutionService"
        assert _service_name(_RPC_METHOD) == "RpcService"

    def test_execution_service_name_matches_the_generated_stub(self):
        """Pin the exclusion to the real generated method path, so a proto
        rename breaks this test rather than silently re-enabling retries."""
        from almanak.gateway.proto import gateway_pb2_grpc

        channel = MagicMock()
        gateway_pb2_grpc.ExecutionServiceStub(channel)
        paths = [call.args[0] for call in channel.unary_unary.call_args_list]

        assert any(path.endswith("/Execute") for path in paths)
        for path in paths:
            assert _service_name(path) in _RateLimitRetryInterceptor._NO_RETRY_SERVICES
