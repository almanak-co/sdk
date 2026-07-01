"""Connector-owned compiler for Orca Whirlpools LP intents.

Orca Whirlpools is a concentrated-liquidity AMM on Solana using Q64.64 tick
math (shared with Raydium CLMM) and Anchor-style instruction encoding.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import IntentType

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

logger = logging.getLogger(__name__)


def _is_solana_chain(chain: str) -> bool:
    """Return True when ``chain`` is in the Solana family."""
    from almanak.core.chains import ChainRegistry
    from almanak.core.enums import ChainFamily

    return ChainRegistry.family_of(chain) is ChainFamily.SOLANA


class OrcaCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compiler for Orca Whirlpools LP intents on Solana."""

    protocols: ClassVar[frozenset[str]] = frozenset({"orca_whirlpools"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.LP_OPEN, IntentType.LP_CLOSE})
    chains: ClassVar[frozenset[str]] = frozenset({"solana"})
    context_type: ClassVar[type[BaseCompilerContext]] = BaseCompilerContext

    def compile(self, ctx: BaseCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        if not _is_solana_chain(ctx.chain):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error="Orca Whirlpools is only supported on Solana",
            )
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.LP_OPEN:
            return self._compile_lp_open(ctx, intent)
        if intent_type == IntentType.LP_CLOSE:
            return self._compile_lp_close(ctx, intent)
        return self._unsupported(intent)

    def _get_adapter(self, ctx: BaseCompilerContext, *, needs_rpc: bool) -> Any:
        """Return a cached OrcaAdapter (keyed by ``needs_rpc``) via ``ctx.cache``."""
        cache: dict[bool, Any] = ctx.cache.setdefault("orca_lp_adapter", {})
        adapter = cache.get(needs_rpc)
        if adapter is None:
            from almanak.connectors.orca.adapter import OrcaAdapter, OrcaConfig

            config_kwargs: dict[str, Any] = {"wallet_address": ctx.wallet_address}
            if needs_rpc:
                config_kwargs["rpc_url"] = ctx.rpc_url or ""
            config = OrcaConfig(**config_kwargs)
            adapter = OrcaAdapter(config=config, token_resolver=ctx.token_resolver)
            cache[needs_rpc] = adapter
        return adapter

    def _compile_lp_open(self, ctx: BaseCompilerContext, intent: LPOpenIntent) -> CompilationResult:
        try:
            adapter = self._get_adapter(ctx, needs_rpc=False)
            bundle = adapter.compile_lp_open_intent(intent)
            if bundle.metadata.get("error"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=bundle.metadata["error"],
                )
            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=bundle,
            )
        except Exception as e:
            logger.exception(f"Orca LP open compilation failed: {e}")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )

    def _compile_lp_close(self, ctx: BaseCompilerContext, intent: LPCloseIntent) -> CompilationResult:
        try:
            adapter = self._get_adapter(ctx, needs_rpc=True)
            bundle = adapter.compile_lp_close_intent(intent)
            if bundle.metadata.get("error"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=bundle.metadata["error"],
                )
            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=bundle,
            )
        except Exception as e:
            logger.exception(f"Orca LP close compilation failed: {e}")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )


__all__ = ["OrcaCompiler"]
