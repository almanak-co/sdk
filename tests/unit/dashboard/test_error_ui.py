"""Unit tests for the shared gateway-error UI helper (VIB-4047).

Covers the classification of gRPC failures (auth vs unreachable vs other) and
the loud + clean rendering contract: a banner always shows, the raw error text
/ traceback is never leaked into a user-facing pane unless debug is enabled.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.dashboard import error_ui
from almanak.framework.dashboard.error_ui import (
    GatewayErrorKind,
    classify_gateway_error,
    render_gateway_error,
)


class _FakeRpc(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, detail: str = "") -> None:
        self._code = code
        self._detail = detail

    def code(self) -> grpc.StatusCode:
        return self._code

    def __str__(self) -> str:
        return self._detail or self._code.name


def _chain(message: str, cause: BaseException) -> Exception:
    err = RuntimeError(message)
    err.__cause__ = cause
    return err


# --------------------------------------------------------------------------- #
# classify_gateway_error
# --------------------------------------------------------------------------- #


def test_classify_auth_via_grpc_code_on_cause() -> None:
    exc = _chain("GetPositions failed", _FakeRpc(grpc.StatusCode.UNAUTHENTICATED, "invalid token"))
    assert classify_gateway_error(exc) is GatewayErrorKind.AUTH


def test_classify_permission_denied_is_auth() -> None:
    exc = _FakeRpc(grpc.StatusCode.PERMISSION_DENIED)
    assert classify_gateway_error(exc) is GatewayErrorKind.AUTH


def test_classify_unavailable_via_code() -> None:
    exc = _chain("GetPnLSummary failed", _FakeRpc(grpc.StatusCode.UNAVAILABLE))
    assert classify_gateway_error(exc) is GatewayErrorKind.UNAVAILABLE


def test_classify_auth_via_text_fallback_when_cause_not_chained() -> None:
    # A call site that forgot to chain the cause still classifies from the text.
    assert classify_gateway_error(Exception("... StatusCode.UNAUTHENTICATED ...")) is GatewayErrorKind.AUTH
    assert classify_gateway_error(Exception("Invalid authentication token")) is GatewayErrorKind.AUTH


def test_classify_unavailable_via_text() -> None:
    assert classify_gateway_error(Exception("Failed to connect to gateway")) is GatewayErrorKind.UNAVAILABLE
    assert classify_gateway_error(Exception("Not connected to gateway")) is GatewayErrorKind.UNAVAILABLE


def test_classify_other() -> None:
    assert classify_gateway_error(Exception("boom")) is GatewayErrorKind.OTHER


def test_classify_does_not_hang_on_cyclic_cause() -> None:
    a = Exception("a")
    b = Exception("b")
    a.__cause__ = b
    b.__cause__ = a
    assert classify_gateway_error(a) is GatewayErrorKind.OTHER


# --------------------------------------------------------------------------- #
# render_gateway_error — loud + clean
# --------------------------------------------------------------------------- #


@pytest.fixture
def stub_st(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    stub = SimpleNamespace(
        error=MagicMock(),
        warning=MagicMock(),
        info=MagicMock(),
        code=MagicMock(),
        session_state={},
    )
    monkeypatch.setattr(error_ui, "st", stub)
    # Default: debug off (no ALMANAK_DASHBOARD_DEBUG, empty session_state).
    monkeypatch.delenv("ALMANAK_DASHBOARD_DEBUG", raising=False)
    return stub


def test_auth_error_renders_red_banner_no_raw_leak(stub_st: SimpleNamespace) -> None:
    exc = _chain(
        "GetPositions failed: <_InactiveRpcError ... status = StatusCode.UNAUTHENTICATED ...>",
        _FakeRpc(grpc.StatusCode.UNAUTHENTICATED),
    )
    kind = render_gateway_error(exc, context="Positions", raw=str(exc))

    assert kind is GatewayErrorKind.AUTH
    stub_st.error.assert_called_once()
    stub_st.warning.assert_not_called()
    stub_st.info.assert_not_called()
    msg = stub_st.error.call_args.args[0]
    assert "authenticate" in msg.lower()
    # Clean: the raw _InactiveRpcError text is never shown to the user.
    assert "_InactiveRpcError" not in msg
    # Debug off → no code/traceback expander content emitted.
    stub_st.code.assert_not_called()


def test_unavailable_renders_error_banner(stub_st: SimpleNamespace) -> None:
    kind = render_gateway_error(Exception("Failed to connect to gateway"), context="PnL")
    assert kind is GatewayErrorKind.UNAVAILABLE
    stub_st.error.assert_called_once()
    assert "unreachable" in stub_st.error.call_args.args[0].lower()


def test_other_error_renders_warning_banner(stub_st: SimpleNamespace) -> None:
    kind = render_gateway_error(Exception("boom raw"), context="the strategy list")
    assert kind is GatewayErrorKind.OTHER
    stub_st.warning.assert_called_once()
    stub_st.error.assert_not_called()
    assert "boom raw" not in stub_st.warning.call_args.args[0]


def test_debug_flag_enables_raw_expander(monkeypatch: pytest.MonkeyPatch, stub_st: SimpleNamespace) -> None:
    monkeypatch.setenv("ALMANAK_DASHBOARD_DEBUG", "1")
    expander_cm = MagicMock()
    expander_cm.__enter__ = MagicMock(return_value=None)
    expander_cm.__exit__ = MagicMock(return_value=False)
    stub_st.expander = MagicMock(return_value=expander_cm)

    render_gateway_error(Exception("boom"), context="Positions", raw="raw detail")

    stub_st.expander.assert_called_once()
    # The raw detail + traceback go to the debug expander (st.code), not the banner.
    assert stub_st.code.called
