"""Connector-owned compiler for Gimo staking intents."""

from __future__ import annotations

import logging
from typing import ClassVar

from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext, BaseStakingCompiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import IntentType, StakeIntent, UnstakeIntent

from .adapter import GimoAdapter, GimoConfig

logger = logging.getLogger(__name__)


class GimoCompiler(BaseStakingCompiler):
    """Compile Gimo STAKE and UNSTAKE intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"gimo"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.STAKE, IntentType.UNSTAKE})
    chains: ClassVar[frozenset[str]] = frozenset({"zerog"})

    def compile_stake(self, ctx: BaseCompilerContext, intent: StakeIntent) -> CompilationResult:
        try:
            action_bundle = GimoAdapter(
                GimoConfig(chain=ctx.chain, wallet_address=ctx.wallet_address),
                token_resolver=ctx.token_resolver,
            ).compile_stake_intent(intent)
            result = self._bundle_to_result(intent, action_bundle, tx_type="stake")
            if result.status == CompilationStatus.SUCCESS:
                logger.info(
                    "Compiled STAKE intent: %s %s via gimo, %s txs, %s gas",
                    intent.amount,
                    intent.token_in,
                    len(result.transactions),
                    result.total_gas_estimate,
                )
            return result
        except Exception as exc:
            logger.exception("Failed to compile Gimo STAKE intent: %s", exc)
            return CompilationResult(status=CompilationStatus.FAILED, error=str(exc), intent_id=intent.intent_id)

    def compile_unstake(self, ctx: BaseCompilerContext, intent: UnstakeIntent) -> CompilationResult:
        try:
            action_bundle = GimoAdapter(
                GimoConfig(chain=ctx.chain, wallet_address=ctx.wallet_address),
                token_resolver=ctx.token_resolver,
            ).compile_unstake_intent(intent)
            result = self._bundle_to_result(intent, action_bundle, tx_type="unstake")
            if result.status == CompilationStatus.SUCCESS:
                logger.info(
                    "Compiled UNSTAKE intent: %s %s via gimo, %s txs, %s gas",
                    intent.amount,
                    intent.token_in,
                    len(result.transactions),
                    result.total_gas_estimate,
                )
            return result
        except Exception as exc:
            logger.exception("Failed to compile Gimo UNSTAKE intent: %s", exc)
            return CompilationResult(status=CompilationStatus.FAILED, error=str(exc), intent_id=intent.intent_id)


__all__ = ["GimoCompiler"]
