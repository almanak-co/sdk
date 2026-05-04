"""Shared gRPC harness for gateway RPC characterization tests.

Provides reusable helpers for asserting servicer behaviour without spinning up
a real gRPC server. Used by Phase 8.3 characterization tests covering:

- ExecutionService.CompileIntent (Phase 8.3a)
- StateService.SavePortfolioMetrics (Phase 8.3b)
- GatewayServer.RegisterChains (Phase 8.3c/d)

The harness is intentionally small - it wraps ``unittest.mock.MagicMock`` with a
``grpc.aio.ServicerContext`` spec and exposes a single ``assert_grpc_error``
helper that captures the most common assertion shape across RPC tests: verify
that the servicer set a specific ``StatusCode``, returned an error response
(``success=False``), and optionally surfaced the expected ``error_code`` and
``error`` substring.

Nothing in this module touches real network, DB, or filesystem state.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import grpc


def make_grpc_context() -> MagicMock:
    """Build a mock ``grpc.aio.ServicerContext``.

    Uses ``spec=grpc.aio.ServicerContext`` so attribute typos on the test side
    fail loudly instead of silently auto-creating attributes on the mock.
    """
    return MagicMock(spec=grpc.aio.ServicerContext)


def assert_grpc_error(
    context: MagicMock,
    response: Any,
    *,
    expected_status: grpc.StatusCode,
    expected_error_code: str | None = None,
    error_substring: str | None = None,
) -> None:
    """Assert the canonical error shape returned by gateway RPCs.

    Every error-path response in the gateway follows the same pattern:

    1. ``context.set_code(<StatusCode>)`` is called once.
    2. ``context.set_details(<message>)`` is called with a human-readable error.
    3. The response proto is returned with ``success=False`` and the
       appropriate ``error`` string and structured ``error_code``.

    Args:
        context: Mock ``ServicerContext`` produced by :func:`make_grpc_context`.
        response: The proto response returned by the RPC (e.g.
            ``CompilationResult``, ``SaveMetricsResponse``).
        expected_status: The ``grpc.StatusCode`` that should have been set.
        expected_error_code: If given, asserts ``response.error_code`` equals
            this value. Omit if the RPC does not populate ``error_code``.
        error_substring: If given, asserts the substring appears in
            ``response.error`` (case-insensitive).
    """
    assert response.success is False, f"Expected success=False, got {response!r}"
    # Error paths must set the gRPC status exactly once. Using
    # ``assert_called_once_with`` guards against regressions that set a status
    # code multiple times or overwrite one code with another before returning.
    context.set_code.assert_called_once_with(expected_status)
    # Every error path also attaches a human-readable ``set_details`` payload.
    context.set_details.assert_called_once()

    if expected_error_code is not None:
        actual_code = getattr(response, "error_code", "")
        assert actual_code == expected_error_code, f"Expected error_code={expected_error_code!r}, got {actual_code!r}"

    if error_substring is not None:
        # Validate the substring against BOTH the proto ``error`` field and
        # the ``set_details`` payload - they should agree, and either failing
        # is a regression worth flagging.
        err_text = getattr(response, "error", "") or ""
        details_text = str(context.set_details.call_args.args[0]) if context.set_details.call_args else ""
        needle = error_substring.lower()
        assert needle in err_text.lower() or needle in details_text.lower(), (
            f"Expected {error_substring!r} in error/details, got error={err_text!r} details={details_text!r}"
        )


def assert_set_code_not_called(context: MagicMock) -> None:
    """Assert the RPC did not call set_code (i.e. returned success)."""
    context.set_code.assert_not_called()
