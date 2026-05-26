"""Base contract for connector-owned lending intent compilers."""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.intents.compiler_models import CompilationResult
from almanak.framework.intents.vocabulary import BorrowIntent, IntentType, RepayIntent, SupplyIntent, WithdrawIntent


class BaseLendingCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Lending compiler ABC for SUPPLY, WITHDRAW, BORROW, REPAY, and DELEVERAGE.

    ``DELEVERAGE`` is structurally identical to ``REPAY`` at the protocol
    level (the on-chain transaction is the same repay call), so :meth:`compile`
    routes it into :meth:`compile_repay`. It is declared in ``intents`` so
    registry/introspection stays in sync with the actual dispatch paths.
    """

    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {
            IntentType.SUPPLY,
            IntentType.WITHDRAW,
            IntentType.BORROW,
            IntentType.REPAY,
            IntentType.DELEVERAGE,
        }
    )
    context_type: ClassVar[type[BaseCompilerContext]] = BaseCompilerContext

    def compile(self, ctx: BaseCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.SUPPLY:
            return self.compile_supply(ctx, intent)
        if intent_type == IntentType.WITHDRAW:
            return self.compile_withdraw(ctx, intent)
        if intent_type == IntentType.BORROW:
            return self.compile_borrow(ctx, intent)
        if intent_type in (IntentType.REPAY, IntentType.DELEVERAGE):
            return self.compile_repay(ctx, intent)
        return self._unsupported(intent)

    @abstractmethod
    def compile_supply(self, ctx: BaseCompilerContext, intent: SupplyIntent) -> CompilationResult: ...

    @abstractmethod
    def compile_withdraw(self, ctx: BaseCompilerContext, intent: WithdrawIntent) -> CompilationResult: ...

    @abstractmethod
    def compile_borrow(self, ctx: BaseCompilerContext, intent: BorrowIntent) -> CompilationResult: ...

    @abstractmethod
    def compile_repay(self, ctx: BaseCompilerContext, intent: RepayIntent) -> CompilationResult: ...


__all__ = ["BaseLendingCompiler"]
