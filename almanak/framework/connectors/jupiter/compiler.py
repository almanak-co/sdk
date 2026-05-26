"""Connector-owned compiler for Jupiter Solana swaps."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import IntentType

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import SwapIntent

logger = logging.getLogger(__name__)


def _is_solana_chain(chain: str) -> bool:
    try:
        from almanak.core.enums import Chain, ChainFamily, get_chain_family

        return get_chain_family(Chain(chain.upper())) == ChainFamily.SOLANA
    except (ValueError, KeyError):
        return False


class JupiterCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compile Jupiter SWAP intents on Solana."""

    protocols: ClassVar[frozenset[str]] = frozenset({"jupiter"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.SWAP})
    chains: ClassVar[frozenset[str]] = frozenset({"solana"})

    def compile(self, ctx: BaseCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        if not _is_solana_chain(ctx.chain):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=getattr(intent, "intent_id", ""),
                error="Jupiter swaps are only supported on Solana",
            )
        if getattr(intent, "intent_type", None) != IntentType.SWAP:
            return self._unsupported(intent)
        return self.compile_swap(ctx, intent)

    def compile_swap(self, ctx: BaseCompilerContext, intent: SwapIntent) -> CompilationResult:
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        try:
            adapter = self._get_adapter(ctx)
            bundle = adapter.compile_swap_intent(intent, price_oracle=ctx.price_oracle)
            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as exc:
            logger.exception("Jupiter swap compilation failed: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)
        return result

    def _get_adapter(self, ctx: BaseCompilerContext) -> Any:
        # Key by the context fields that change adapter behaviour so a cached
        # adapter is immutable relative to the context that produced it.
        # Mutating ``price_provider`` / ``allow_placeholder_prices`` on a
        # cache hit (previous behaviour) silently flipped a production compile
        # into test-only placeholder pricing when ``ctx.price_oracle`` differed
        # between calls.
        cache_key = (
            "jupiter_adapter",
            ctx.wallet_address,
            ctx.rpc_url,
            ctx.allow_placeholder_prices,
        )
        adapter = ctx.cache.get(cache_key)
        if adapter is None:
            from almanak.framework.connectors.jupiter import JupiterAdapter, JupiterConfig

            config = JupiterConfig(wallet_address=ctx.wallet_address)
            adapter = JupiterAdapter(
                config=config,
                price_provider=ctx.price_oracle,
                allow_placeholder_prices=ctx.allow_placeholder_prices,
                token_resolver=ctx.token_resolver,
                rpc_url=ctx.rpc_url,
            )
            ctx.cache[cache_key] = adapter
        return adapter


__all__ = ["JupiterCompiler"]
