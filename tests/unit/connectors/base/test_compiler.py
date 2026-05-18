from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus


class _Compiler(BaseProtocolCompiler[BaseCompilerContext]):
    protocols = frozenset({"dummy"})
    intents = frozenset()

    def compile_swap(self, ctx, intent):
        return CompilationResult(status=CompilationStatus.SUCCESS, intent_id="swap")

    def compile_lp_open(self, ctx, intent):
        return CompilationResult(status=CompilationStatus.SUCCESS, intent_id="lp_open")

    def compile_lp_close(self, ctx, intent):
        return CompilationResult(status=CompilationStatus.SUCCESS, intent_id="lp_close")

    def compile_collect_fees(self, ctx, intent):
        return CompilationResult(status=CompilationStatus.SUCCESS, intent_id="collect")


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


def test_base_protocol_compiler_rejects_wrong_context_type():
    result = _Compiler().compile(MagicMock(), SimpleNamespace(intent_type="SWAP", intent_id="bad-context"))

    assert result.status is CompilationStatus.FAILED
    assert result.intent_id == "bad-context"
    assert "requires BaseCompilerContext" in result.error


def test_base_protocol_compiler_rejects_unsupported_intent_type():
    result = _Compiler().compile(_ctx(), SimpleNamespace(intent_type="BORROW", intent_id="bad-intent"))

    assert result.status is CompilationStatus.FAILED
    assert result.intent_id == "bad-intent"
    assert "does not support intent type BORROW" in result.error
