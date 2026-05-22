"""Connector-owned compiler for jupiter lend (Solana lending).

Thin wrapper that delegates to ``compiler_solana.py``. Phase 5 of the
connector-folding program owns the proper Solana fold; until then the
``_SolanaCompilerAdapter`` here bridges ``BaseCompilerContext`` to the
legacy compiler shape that ``compiler_solana.py`` expects. The adapter
lives in this file (not in the framework) to keep its blast radius scoped
to this one connector.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.framework.connectors.base.compiler import BaseCompilerContext
from almanak.framework.connectors.base.lending import BaseLendingCompiler
from almanak.framework.intents.compiler_models import CompilationResult
from almanak.framework.intents.vocabulary import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent


class _SolanaCompilerAdapter:
    """Bridge ctx to the legacy compiler shape expected by compiler_solana.py.

    Phase 5 will fold compiler_solana.py and remove this adapter.
    """

    __slots__ = (
        "chain",
        "wallet_address",
        "price_oracle",
        "rpc_timeout",
        "_gateway_client",
        "_token_resolver",
        "_cached_jupiter_lend_adapter",
        "_cached_kamino_adapter",
        "_cached_kamino_adapter_with_rpc",
    )

    def __init__(self, ctx: BaseCompilerContext) -> None:
        self.chain = ctx.chain
        self.wallet_address = ctx.wallet_address
        self.price_oracle = ctx.price_oracle
        self.rpc_timeout = ctx.rpc_timeout
        self._gateway_client = ctx.gateway_client
        self._token_resolver = ctx.token_resolver
        self._cached_jupiter_lend_adapter = None
        self._cached_kamino_adapter = None
        self._cached_kamino_adapter_with_rpc = None


class JupiterLendCompiler(BaseLendingCompiler):
    """Compile jupiter lend lending intents (Solana)."""

    protocols: ClassVar[frozenset[str]] = frozenset({"jupiter_lend"})
    chains: ClassVar[frozenset[str]] = frozenset({"solana"})

    def compile_supply(self, ctx: BaseCompilerContext, intent: SupplyIntent) -> CompilationResult:
        from almanak.framework.intents.compiler_solana import compile_jupiter_lend_supply

        return compile_jupiter_lend_supply(_SolanaCompilerAdapter(ctx), intent)

    def compile_withdraw(self, ctx: BaseCompilerContext, intent: WithdrawIntent) -> CompilationResult:
        from almanak.framework.intents.compiler_solana import compile_jupiter_lend_withdraw

        return compile_jupiter_lend_withdraw(_SolanaCompilerAdapter(ctx), intent)

    def compile_borrow(self, ctx: BaseCompilerContext, intent: BorrowIntent) -> CompilationResult:
        from almanak.framework.intents.compiler_solana import compile_jupiter_lend_borrow

        return compile_jupiter_lend_borrow(_SolanaCompilerAdapter(ctx), intent)

    def compile_repay(self, ctx: BaseCompilerContext, intent: RepayIntent) -> CompilationResult:
        from almanak.framework.intents.compiler_solana import compile_jupiter_lend_repay

        return compile_jupiter_lend_repay(_SolanaCompilerAdapter(ctx), intent)


__all__ = ["JupiterLendCompiler"]
