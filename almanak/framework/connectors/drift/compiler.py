"""Connector-owned compiler for Drift perpetual intents."""

from __future__ import annotations

import logging
from typing import ClassVar

from almanak.framework.chain_family import SvmFamily, family_for
from almanak.framework.connectors.base.compiler import BasePerpCompiler, PerpCompilerContext
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.intent_errors import InvalidCollateralForMarketError
from almanak.framework.intents.vocabulary import IntentType, PerpCloseIntent, PerpOpenIntent

from .adapter import DriftAdapter
from .market_rules import validate_drift_collateral
from .models import DriftConfig

logger = logging.getLogger(__name__)


class DriftCompiler(BasePerpCompiler):
    """Compile Drift PERP_OPEN and PERP_CLOSE intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"drift"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.PERP_OPEN, IntentType.PERP_CLOSE})
    chains: ClassVar[frozenset[str]] = frozenset({"solana"})

    def compile_perp_open(self, ctx: PerpCompilerContext, intent: PerpOpenIntent) -> CompilationResult:
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        try:
            if not isinstance(family_for(ctx.chain), SvmFamily):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Drift is only supported on Solana",
                    intent_id=intent.intent_id,
                )

            try:
                validate_drift_collateral(intent.collateral_token)
            except InvalidCollateralForMarketError as exc:
                return CompilationResult(status=CompilationStatus.FAILED, error=str(exc), intent_id=intent.intent_id)

            if intent.collateral_amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="collateral_amount='all' must be resolved before compilation.",
                    intent_id=intent.intent_id,
                )

            adapter = self._build_adapter(ctx)
            bundle = adapter.compile_perp_open_intent(intent, price_oracle=ctx.price_oracle)
            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as exc:
            logger.exception("Drift perp open compilation failed: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)
        return result

    def compile_perp_close(self, ctx: PerpCompilerContext, intent: PerpCloseIntent) -> CompilationResult:
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        try:
            if not isinstance(family_for(ctx.chain), SvmFamily):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Drift is only supported on Solana",
                    intent_id=intent.intent_id,
                )

            adapter = self._build_adapter(ctx)
            bundle = adapter.compile_perp_close_intent(intent, price_oracle=ctx.price_oracle)
            if bundle.metadata.get("error"):
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata["error"]
            else:
                result.action_bundle = bundle
        except Exception as exc:
            logger.exception("Drift perp close compilation failed: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)
        return result

    def _build_adapter(self, ctx: PerpCompilerContext) -> DriftAdapter:
        return DriftAdapter(
            DriftConfig(
                wallet_address=ctx.wallet_address,
                gateway_client=ctx.gateway_client,
            ),
            token_resolver=ctx.token_resolver,
        )


__all__ = ["DriftCompiler"]
