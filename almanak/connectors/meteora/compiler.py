"""Connector-owned compiler for Meteora DLMM LP intents.

Meteora DLMM uses discrete price bins (not continuous ticks) and
non-transferable Keypair-based position accounts. Only supported on Solana.
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
    try:
        from almanak.core.enums import Chain, ChainFamily, get_chain_family

        return get_chain_family(Chain(chain.upper())) == ChainFamily.SOLANA
    except (ValueError, KeyError):
        return False


class MeteoraCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compiler for Meteora DLMM LP intents on Solana."""

    protocols: ClassVar[frozenset[str]] = frozenset({"meteora_dlmm"})
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
                error="Meteora DLMM is only supported on Solana",
            )
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.LP_OPEN:
            return self._compile_lp_open(ctx, intent)
        if intent_type == IntentType.LP_CLOSE:
            return self._compile_lp_close(ctx, intent)
        return self._unsupported(intent)

    def _get_adapter(self, ctx: BaseCompilerContext, *, needs_rpc: bool) -> Any:
        """Return a cached MeteoraAdapter (keyed by ``needs_rpc``) via ``ctx.cache``."""
        cache: dict[bool, Any] = ctx.cache.setdefault("meteora_lp_adapter", {})
        adapter = cache.get(needs_rpc)
        if adapter is None:
            from almanak.connectors.meteora.adapter import MeteoraAdapter, MeteoraConfig

            config_kwargs: dict[str, Any] = {"wallet_address": ctx.wallet_address}
            if needs_rpc:
                config_kwargs["rpc_url"] = ctx.rpc_url or ""
            config = MeteoraConfig(**config_kwargs)
            adapter = MeteoraAdapter(config=config, token_resolver=ctx.token_resolver)
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
            logger.exception(f"Meteora LP open compilation failed: {e}")
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
            logger.exception(f"Meteora LP close compilation failed: {e}")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )


__all__ = ["MeteoraCompiler"]
