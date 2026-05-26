"""Connector-owned compiler for Jupiter Lend Solana lending intents."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, ClassVar

from almanak.framework.connectors.base.compiler import BaseCompilerContext
from almanak.framework.connectors.base.lending import BaseLendingCompiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


class JupiterLendCompiler(BaseLendingCompiler):
    """Compile Jupiter Lend lending intents on Solana."""

    protocols: ClassVar[frozenset[str]] = frozenset({"jupiter_lend"})
    chains: ClassVar[frozenset[str]] = frozenset({"solana"})

    def compile_supply(self, ctx: BaseCompilerContext, intent: SupplyIntent) -> CompilationResult:
        return _compile_bundle(
            "Jupiter Lend supply",
            intent.intent_id,
            lambda: self._get_adapter(ctx).compile_supply_intent(intent),
        )

    def compile_withdraw(self, ctx: BaseCompilerContext, intent: WithdrawIntent) -> CompilationResult:
        return _compile_bundle(
            "Jupiter Lend withdraw",
            intent.intent_id,
            lambda: self._get_adapter(ctx).compile_withdraw_intent(intent),
        )

    def compile_borrow(self, ctx: BaseCompilerContext, intent: BorrowIntent) -> CompilationResult:
        return _compile_bundle(
            "Jupiter Lend borrow",
            intent.intent_id,
            lambda: self._get_adapter(ctx).compile_borrow_intent(intent),
        )

    def compile_repay(self, ctx: BaseCompilerContext, intent: RepayIntent) -> CompilationResult:
        return _compile_bundle(
            "Jupiter Lend repay",
            intent.intent_id,
            lambda: self._get_adapter(ctx).compile_repay_intent(intent),
        )

    def _get_adapter(self, ctx: BaseCompilerContext) -> Any:
        adapter = ctx.cache.get("jupiter_lend_adapter")
        if adapter is None:
            from almanak.framework.connectors.jupiter_lend import JupiterLendAdapter, JupiterLendConfig

            adapter = JupiterLendAdapter(
                config=JupiterLendConfig(wallet_address=ctx.wallet_address),
                token_resolver=ctx.token_resolver,
            )
            ctx.cache["jupiter_lend_adapter"] = adapter
        return adapter


def _compile_bundle(label: str, intent_id: str, build: Callable[[], ActionBundle]) -> CompilationResult:
    result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent_id)
    try:
        bundle = build()
        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as exc:
        logger.exception("%s compilation failed: %s", label, exc)
        result.status = CompilationStatus.FAILED
        result.error = str(exc)
    return result


__all__ = ["JupiterLendCompiler"]
