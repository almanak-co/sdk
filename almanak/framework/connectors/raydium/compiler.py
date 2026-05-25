"""Connector-owned compiler for Raydium CLMM LP intents.

Raydium CLMM is a concentrated-liquidity AMM on Solana with NFT-backed
positions. Unlike Jupiter/Kamino, Raydium CLMM builds instructions locally
using ``solders`` and submits via SolanaExecutionPlanner.

Raydium CLMM is the default LP protocol on Solana — the dispatch site in
``IntentCompiler._compile_lp_open`` / ``_compile_lp_close`` normalises
``intent.protocol is None`` to ``raydium_clmm`` on Solana chains *before*
the connector registry lookup.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler
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


class RaydiumCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compiler for Raydium CLMM LP intents on Solana."""

    protocols: ClassVar[frozenset[str]] = frozenset({"raydium_clmm"})
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
                error="Raydium CLMM is only supported on Solana",
            )
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.LP_OPEN:
            return self._compile_lp_open(ctx, intent)
        if intent_type == IntentType.LP_CLOSE:
            return self._compile_lp_close(ctx, intent)
        return self._unsupported(intent)

    def _get_adapter(self, ctx: BaseCompilerContext, *, needs_rpc: bool) -> Any:
        """Return a cached RaydiumAdapter (keyed by ``needs_rpc``) via ``ctx.cache``."""
        cache: dict[bool, Any] = ctx.cache.setdefault("raydium_lp_adapter", {})
        adapter = cache.get(needs_rpc)
        if adapter is None:
            from almanak.framework.connectors.raydium.adapter import RaydiumAdapter, RaydiumConfig

            config_kwargs: dict[str, Any] = {"wallet_address": ctx.wallet_address}
            if needs_rpc:
                config_kwargs["rpc_url"] = ctx.rpc_url or ""
            config = RaydiumConfig(**config_kwargs)
            adapter = RaydiumAdapter(config=config, token_resolver=ctx.token_resolver)
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
            logger.exception(f"Raydium LP open compilation failed: {e}")
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
            logger.exception(f"Raydium LP close compilation failed: {e}")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )


__all__ = ["RaydiumCompiler"]
