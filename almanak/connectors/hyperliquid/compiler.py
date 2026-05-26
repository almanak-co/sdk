"""Connector-owned compiler surface for Hyperliquid perpetual intents."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._strategy_base.base.compiler import BasePerpCompiler, PerpCompilerContext
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import IntentType, PerpCloseIntent, PerpOpenIntent


class HyperliquidCompiler(BasePerpCompiler):
    """Fail-closed compiler for Hyperliquid's off-chain execution surface.

    Hyperliquid orders are signed off-chain API actions, not gateway-submitted
    EVM/Solana transactions. Until the execution pipeline has an off-chain order
    lane, the connector compiler must fail explicitly rather than falling
    through to GMX/Aster error messages.
    """

    protocols: ClassVar[frozenset[str]] = frozenset({"hyperliquid"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.PERP_OPEN, IntentType.PERP_CLOSE})
    chains: ClassVar[frozenset[str]] = frozenset({"hyperliquid"})

    def compile_perp_open(self, ctx: PerpCompilerContext, intent: PerpOpenIntent) -> CompilationResult:
        return _unsupported_offchain(intent.intent_id)

    def compile_perp_close(self, ctx: PerpCompilerContext, intent: PerpCloseIntent) -> CompilationResult:
        return _unsupported_offchain(intent.intent_id)


def _unsupported_offchain(intent_id: str) -> CompilationResult:
    return CompilationResult(
        status=CompilationStatus.FAILED,
        intent_id=intent_id,
        error=(
            "Hyperliquid PERP intents are off-chain signed orders and are not yet "
            "representable as gateway ActionBundle transactions."
        ),
    )


__all__ = ["HyperliquidCompiler"]
