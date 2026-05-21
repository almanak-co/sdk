from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus


class _Compiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Reference connector that exercises the base helpers via its own compile()."""

    protocols = frozenset({"dummy"})
    intents = frozenset()

    def compile(self, ctx, intent):
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == "SWAP":
            return CompilationResult(status=CompilationStatus.SUCCESS, intent_id="swap")
        if intent_type == "LP_OPEN":
            return CompilationResult(status=CompilationStatus.SUCCESS, intent_id="lp_open")
        if intent_type == "LP_CLOSE":
            return CompilationResult(status=CompilationStatus.SUCCESS, intent_id="lp_close")
        if intent_type == "LP_COLLECT_FEES":
            return CompilationResult(status=CompilationStatus.SUCCESS, intent_id="collect")
        return self._unsupported(intent)


def _ctx() -> BaseCompilerContext:
    return BaseCompilerContext(
        chain="arbitrum",
        wallet_address="0x0000000000000000000000000000000000000001",
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=False,
        allow_placeholder_prices=True,
        token_resolver=None,
        gateway_client=None,
        price_oracle={},
        cache={},
        services=MagicMock(),
    )


def test_base_protocol_compiler_dispatches_supported_intents():
    compiler = _Compiler()
    ctx = _ctx()

    assert compiler.compile(ctx, SimpleNamespace(intent_type="SWAP")).intent_id == "swap"
    assert compiler.compile(ctx, SimpleNamespace(intent_type="LP_OPEN")).intent_id == "lp_open"
    assert compiler.compile(ctx, SimpleNamespace(intent_type="LP_CLOSE")).intent_id == "lp_close"
    assert compiler.compile(ctx, SimpleNamespace(intent_type="LP_COLLECT_FEES")).intent_id == "collect"


def test_check_context_rejects_wrong_context_type():
    result = _Compiler().compile(MagicMock(), SimpleNamespace(intent_type="SWAP", intent_id="bad-context"))

    assert result.status is CompilationStatus.FAILED
    assert result.intent_id == "bad-context"
    assert "requires BaseCompilerContext" in result.error


def test_unsupported_helper_returns_failed_with_intent_type():
    result = _Compiler().compile(_ctx(), SimpleNamespace(intent_type="BORROW", intent_id="bad-intent"))

    assert result.status is CompilationStatus.FAILED
    assert result.intent_id == "bad-intent"
    assert "does not support intent type BORROW" in result.error
