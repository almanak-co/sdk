"""Connector-owned compiler for benqi lending intents."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, ClassVar

from almanak.framework.connectors.base.compiler import BaseCompilerContext
from almanak.framework.connectors.base.lending import BaseLendingCompiler
from almanak.framework.connectors.base.lending import aave_helpers as _aave_helpers
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent


class _LendingCompilerAdapter:
    """Per-call adapter exposing the legacy IntentCompiler surface that the
    relocated lending bodies in ``aave_helpers`` still consume.

    Mirrors :class:`almanak.framework.connectors.pendle.compiler._PendleCompileImpl`
    -- the connector-owned per-call ctx→compiler bridge documented as known
    transitional debt. Lending bodies will fully migrate to ``ctx`` in a
    follow-up; this adapter is the bounded, single-connector seam in the
    meantime.
    """

    __slots__ = (
        "_ctx",
        "chain",
        "wallet_address",
        "rpc_url",
        "rpc_timeout",
        "price_oracle",
        "_gateway_client",
        "_token_resolver",
    )

    def __init__(self, ctx: BaseCompilerContext) -> None:
        self._ctx = ctx
        self.chain = ctx.chain
        self.wallet_address = ctx.wallet_address
        self.rpc_url = ctx.rpc_url
        self.rpc_timeout = ctx.rpc_timeout
        self.price_oracle = ctx.price_oracle
        self._gateway_client = ctx.gateway_client
        self._token_resolver = ctx.token_resolver

    # Lending preflight caches read these attribute names off the compiler.
    # We back them with ``ctx.cache`` so the framework owns lifetime.
    @property
    def _aave_collateral_eligibility_cache(self) -> dict:
        return self._ctx.cache.setdefault("aave_collateral_eligibility", {})

    @property
    def _lending_reserve_active_cache(self) -> dict:
        return self._ctx.cache.setdefault("lending_reserve_active", {})

    @property
    def _lending_borrowable_cache(self) -> dict:
        return self._ctx.cache.setdefault("lending_borrowable", {})

    @property
    def _lending_borrow_capacity_cache(self) -> dict:
        return self._ctx.cache.setdefault("lending_borrow_capacity", {})

    def _resolve_token(self, token: str) -> Any:
        return self._ctx.services.resolve_token(token)

    def _build_approve_tx(self, token_address: str, spender: str, amount: int) -> list:
        return self._ctx.services.build_approve_tx(token_address, spender, amount)

    def _format_amount(self, amount: int, decimals: int) -> str:
        return self._ctx.services.format_amount(amount, decimals)

    def _query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None:
        return self._ctx.services.query_erc20_balance(token_address, wallet_address)

    def _get_wrapped_native_address(self) -> str | None:
        return self._ctx.services.get_wrapped_native_address()

    def _get_chain_rpc_url(self) -> str | None:
        return self._ctx.rpc_url


def _failed(intent: Any, error: str) -> CompilationResult:
    return CompilationResult(
        status=CompilationStatus.FAILED,
        error=error,
        intent_id=getattr(intent, "intent_id", ""),
    )


class BenqiCompiler(BaseLendingCompiler):
    """Compile benqi lending intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"benqi"})
    chains: ClassVar[frozenset[str]] = frozenset({"avalanche"})

    def compile_supply(self, ctx: BaseCompilerContext, intent: SupplyIntent) -> CompilationResult:
        adapter = _LendingCompilerAdapter(ctx)
        supply_token = adapter._resolve_token(intent.token)
        if supply_token is None:
            return _failed(intent, f"Unknown token: {intent.token}")
        if intent.amount == "all":
            return _failed(
                intent,
                "amount='all' for supply must be resolved to a wallet balance before compilation. "
                "This should be done by the strategy runner or teardown manager.",
            )
        amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
        return _aave_helpers._compile_supply_benqi(adapter, intent, supply_token, amount_decimal)

    def compile_withdraw(self, ctx: BaseCompilerContext, intent: WithdrawIntent) -> CompilationResult:
        adapter = _LendingCompilerAdapter(ctx)
        withdraw_token = adapter._resolve_token(intent.token)
        if withdraw_token is None:
            return _failed(intent, f"Unknown token: {intent.token}")
        initial_warnings: list[str] = []
        withdraw_amount_decimal: Decimal | None
        if intent.withdraw_all:
            withdraw_amount_decimal = None
            initial_warnings.append("Withdrawing all available balance")
        elif intent.amount == "all":
            withdraw_amount_decimal = None
            intent = intent.model_copy(update={"withdraw_all": True})
            initial_warnings.append("Withdrawing all available balance (amount='all' fallback)")
        else:
            withdraw_amount_decimal = intent.amount  # type: ignore[assignment]
        return _aave_helpers._compile_withdraw_benqi(
            adapter, intent, withdraw_token, withdraw_amount_decimal, initial_warnings
        )

    def compile_borrow(self, ctx: BaseCompilerContext, intent: BorrowIntent) -> CompilationResult:
        adapter = _LendingCompilerAdapter(ctx)
        collateral_token = adapter._resolve_token(intent.collateral_token)
        borrow_token = adapter._resolve_token(intent.borrow_token)
        if collateral_token is None:
            return _failed(intent, f"Unknown collateral token: {intent.collateral_token}")
        if borrow_token is None:
            return _failed(intent, f"Unknown borrow token: {intent.borrow_token}")
        if intent.collateral_amount == "all":
            return _failed(
                intent,
                "collateral_amount='all' must be resolved before compilation. "
                "Use Intent.set_resolved_amount() to resolve chained amounts.",
            )
        collateral_amount_decimal: Decimal = intent.collateral_amount  # type: ignore[assignment]
        return _aave_helpers._compile_borrow_benqi(
            adapter, intent, collateral_token, borrow_token, collateral_amount_decimal
        )

    def compile_repay(self, ctx: BaseCompilerContext, intent: RepayIntent) -> CompilationResult:
        adapter = _LendingCompilerAdapter(ctx)
        repay_token = adapter._resolve_token(intent.token)
        if repay_token is None:
            return _failed(intent, f"Unknown repay token: {intent.token}")
        initial_warnings: list[str] = []
        repay_amount_decimal: Decimal | None
        if intent.repay_full:
            repay_amount_decimal = None
            amount_description = "full debt"
            initial_warnings.append("Repaying full debt - ensure sufficient balance to cover interest")
        elif intent.amount == "all":
            repay_amount_decimal = None
            intent = intent.model_copy(update={"repay_full": True})
            amount_description = "full debt"
            initial_warnings.append("Repaying full debt (amount='all' fallback)")
        else:
            repay_amount_decimal = intent.amount  # type: ignore[assignment]
            amount_description = f"{repay_amount_decimal} {repay_token.symbol}"
        return _aave_helpers._compile_repay_benqi(
            adapter, intent, repay_token, repay_amount_decimal, amount_description, initial_warnings
        )


__all__ = ["BenqiCompiler"]
