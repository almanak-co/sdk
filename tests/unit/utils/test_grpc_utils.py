import grpc

from almanak.framework.utils.grpc_utils import (
    _RETRY_AFTER_MAX_SECONDS,
    TRANSIENT_GRPC_CODES,
    get_grpc_retry_after_seconds,
    get_grpc_status_code,
    is_transient_grpc_error,
)


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode | None, details: str = "") -> None:
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        if self._code is None:
            raise ValueError("no code")
        return self._code

    def details(self) -> str:
        return self._details

    def __str__(self) -> str:
        return self._details


class _NoCodeRpcError(grpc.RpcError):
    """Bare RpcError subclass without a .code() method."""


# ---------------------------------------------------------------------------
# get_grpc_status_code
# ---------------------------------------------------------------------------


def test_get_grpc_status_code_returns_valid_code() -> None:
    exc = _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED)
    assert get_grpc_status_code(exc) == grpc.StatusCode.RESOURCE_EXHAUSTED


def test_get_grpc_status_code_returns_none_when_code_raises() -> None:
    exc = _FakeRpcError(None)  # .code() raises ValueError
    assert get_grpc_status_code(exc) is None


def test_get_grpc_status_code_returns_none_when_no_code_attr() -> None:
    exc = _NoCodeRpcError()
    assert get_grpc_status_code(exc) is None


class _BadCodeTypeRpcError(grpc.RpcError):
    def code(self) -> str:  # type: ignore[override]
        return "UNAVAILABLE"


def test_get_grpc_status_code_returns_none_when_code_not_status_code() -> None:
    exc = _BadCodeTypeRpcError()
    assert get_grpc_status_code(exc) is None


# ---------------------------------------------------------------------------
# is_transient_grpc_error
# ---------------------------------------------------------------------------


def test_is_transient_for_resource_exhausted() -> None:
    assert is_transient_grpc_error(_FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED))


def test_is_transient_for_unavailable() -> None:
    assert is_transient_grpc_error(_FakeRpcError(grpc.StatusCode.UNAVAILABLE))


def test_is_not_transient_for_invalid_argument() -> None:
    assert not is_transient_grpc_error(_FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT))


def test_is_not_transient_for_permission_denied() -> None:
    assert not is_transient_grpc_error(_FakeRpcError(grpc.StatusCode.PERMISSION_DENIED))


def test_is_transient_for_unknown_code() -> None:
    # Unknown code (code() raises) treated as transient per PR #1676 decision.
    assert is_transient_grpc_error(_FakeRpcError(None))


def test_all_transient_codes_covered() -> None:
    for code in TRANSIENT_GRPC_CODES:
        assert is_transient_grpc_error(_FakeRpcError(code)), f"{code} should be transient"


# ---------------------------------------------------------------------------
# get_grpc_retry_after_seconds
# ---------------------------------------------------------------------------


def test_retry_after_extracted_from_details() -> None:
    exc = _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "Rate limited, retry after 51.72s")
    assert get_grpc_retry_after_seconds(exc) == 51.72


def test_retry_after_extracted_from_str_repr() -> None:
    exc = _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "Retry after 5s please")
    assert get_grpc_retry_after_seconds(exc) == 5.0


def test_retry_after_capped_at_max() -> None:
    exc = _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, f"retry after {_RETRY_AFTER_MAX_SECONDS + 300}s")
    assert get_grpc_retry_after_seconds(exc) == _RETRY_AFTER_MAX_SECONDS


def test_retry_after_returns_none_when_absent() -> None:
    exc = _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "connection reset")
    assert get_grpc_retry_after_seconds(exc) is None


class _DetailsRaisesRpcError(grpc.RpcError):
    def details(self) -> str:
        raise RuntimeError("details unavailable")

    def __str__(self) -> str:
        return "gateway overloaded, retry after 7s"


def test_retry_after_falls_back_to_str_when_details_raises() -> None:
    exc = _DetailsRaisesRpcError()
    assert get_grpc_retry_after_seconds(exc) == 7.0
