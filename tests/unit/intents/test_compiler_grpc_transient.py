from types import SimpleNamespace

import grpc

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def __str__(self) -> str:
        return self._details


def _compiler() -> IntentCompiler:
    return IntentCompiler(
        chain="avalanche",
        price_oracle={},
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


def test_compile_marks_resource_exhausted_as_transient_with_retry_after(monkeypatch) -> None:
    compiler = _compiler()

    def fail(_intent):
        raise _FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "Rate limited, retry after 51.72s")

    monkeypatch.setattr(compiler, "_compile_hold", fail)
    intent = SimpleNamespace(intent_type=IntentType.HOLD, intent_id="hold_1")

    result = compiler.compile(intent)

    assert result.status.value == "FAILED"
    assert result.is_transient is True
    assert result.retry_after_seconds == 51.72
    assert "Rate limited" in result.error


def test_compile_marks_permanent_grpc_failure_non_transient(monkeypatch) -> None:
    compiler = _compiler()

    def fail(_intent):
        raise _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, "bad request")

    monkeypatch.setattr(compiler, "_compile_hold", fail)
    intent = SimpleNamespace(intent_type=IntentType.HOLD, intent_id="hold_1")

    result = compiler.compile(intent)

    assert result.status.value == "FAILED"
    assert result.is_transient is False
    assert result.retry_after_seconds is None
