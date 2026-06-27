"""Connector-owned compiler for euler v2 lending intents."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import (
    BaseCompilerContext,
    PreflightOutcome,
    PreflightVerdict,
)
from almanak.connectors._strategy_base.base.lending import BaseLendingCompiler
from almanak.connectors._strategy_base.base.lending import aave_helpers as _aave_helpers
from almanak.connectors.euler_v2.adapter import EULER_V2_VAULTS_BY_CHAIN
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import BorrowIntent, IntentType, RepayIntent, SupplyIntent, WithdrawIntent

logger = logging.getLogger(__name__)

#: ``LTVBorrow(address collateral)`` on an Euler EVK controller (borrow) vault,
#: returning the borrowing LTV in 1e4 scale (10000 == 100%). A zero LTV means the
#: collateral vault is NOT enabled as collateral for that borrow vault, so a
#: borrow against it would revert on the EVC solvency check.
_LTV_BORROW_SELECTOR = "0xbf58094d"
_LTV_SCALE = 10000


class _LendingCompilerAdapter:
    """Per-call adapter exposing the legacy IntentCompiler surface that the
    relocated lending bodies in ``aave_helpers`` still consume.

    Mirrors :class:`almanak.connectors.pendle.compiler._PendleCompileImpl`
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


def _decode_uint(hex_result: str | None) -> int | None:
    """Decode a single uint256 word from an ``eth_call`` hex result, or ``None``.

    Empty / ``0x`` / un-parseable results return ``None`` (UNMEASURED — Empty≠Zero)
    so the caller degrades to UNAVAILABLE rather than treating a read gap as a 0 LTV.
    """
    if not hex_result or hex_result in ("0x", "0X"):
        return None
    try:
        return int(hex_result, 16)
    except (ValueError, TypeError):
        return None


class EulerV2Compiler(BaseLendingCompiler):
    """Compile euler v2 lending intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"euler_v2"})
    chains: ClassVar[frozenset[str]] = frozenset(EULER_V2_VAULTS_BY_CHAIN.keys())

    #: Stable prefix strategies + the retry-classification keyword table match on.
    BORROW_INFEASIBLE_ERROR_PREFIX: ClassVar[str] = "EULER_BORROW_INFEASIBLE"

    def preflight(self, ctx: BaseCompilerContext, intent: Any) -> PreflightVerdict:
        """Reject a structurally-doomed Euler V2 BORROW at compile time (VIB-5374 / 2795).

        Two structural failures the EVC reverts on (today only a ``logger.warning``
        existed — the borrow compiled and reverted, burning gas):

        1. The collateral vault is not enabled as collateral for the borrow vault
           (``LTVBorrow(collateral) == 0``).
        2. The requested borrow value exceeds the collateral value at the borrow
           LTV (over-LTV), so the EVC solvency check fails.

        The EVC ``LTVBorrow`` read rides on ``ctx.services.eth_call`` (VIB-5374
        gateway-backed passthrough — no new ``almanak/gateway/`` surface). Only
        ``BORROW`` is gated; SUPPLY/WITHDRAW/REPAY are never feasibility-blocked.
        A read or price gap yields ``UNAVAILABLE`` / FEASIBLE (fail-open), never a
        false reject.
        """
        if getattr(intent, "intent_type", None) != IntentType.BORROW:
            return PreflightVerdict.feasible()
        if ctx.chain not in self.chains:
            return PreflightVerdict.feasible()
        try:
            return self._euler_borrow_verdict(ctx, intent)
        except Exception as exc:  # noqa: BLE001 - feasibility must never harden into a false reject
            logger.warning("Euler borrow preflight could not evaluate; deferring: %s", exc)
            return PreflightVerdict.feasible()

    def _euler_borrow_verdict(self, ctx: BaseCompilerContext, intent: Any) -> PreflightVerdict:
        """Core Euler BORROW feasibility check (see :meth:`preflight`)."""
        from almanak.connectors.euler_v2.adapter import EulerV2Adapter, EulerV2Config

        adapter = _LendingCompilerAdapter(ctx)
        collateral_token = adapter._resolve_token(intent.collateral_token)
        borrow_token = adapter._resolve_token(intent.borrow_token)
        if collateral_token is None or borrow_token is None:
            return PreflightVerdict.feasible()  # compile path emits the real "unknown token" error
        if intent.collateral_amount == "all":
            return PreflightVerdict.feasible()  # resolved upstream; defer to compile

        euler_adapter = EulerV2Adapter(EulerV2Config(chain=ctx.chain, wallet_address=ctx.wallet_address))
        collateral_vault = euler_adapter.find_vault_for_asset(collateral_token.symbol.upper())
        borrow_vault = euler_adapter.find_vault_for_asset(borrow_token.symbol.upper())
        if collateral_vault is None or borrow_vault is None:
            return PreflightVerdict.feasible()  # compile path emits the real "no vault" error

        # LTVBorrow(collateral_vault) is read on the BORROW (controller) vault.
        calldata = _LTV_BORROW_SELECTOR + collateral_vault.vault_address.lower().replace("0x", "").zfill(64)
        raw = ctx.services.eth_call(borrow_vault.vault_address, calldata, chain=ctx.chain)
        ltv_borrow = _decode_uint(raw)
        if ltv_borrow is None:
            return PreflightVerdict(
                outcome=PreflightOutcome.UNAVAILABLE,
                reason=(
                    f"could not read Euler LTVBorrow({collateral_vault.vault_symbol}) "
                    f"on {borrow_vault.vault_symbol} ({ctx.chain})"
                ),
            )
        if ltv_borrow == 0:
            return PreflightVerdict(
                outcome=PreflightOutcome.INFEASIBLE,
                error_prefix=self.BORROW_INFEASIBLE_ERROR_PREFIX,
                reason=(
                    f"{collateral_token.symbol} is not enabled as collateral for borrowing "
                    f"{borrow_token.symbol} on Euler V2 ({ctx.chain}); the EVC borrow would revert"
                ),
            )

        return self._euler_capacity_verdict(ctx, intent, collateral_token, borrow_token, ltv_borrow)

    def _euler_capacity_verdict(
        self,
        ctx: BaseCompilerContext,
        intent: Any,
        collateral_token: Any,
        borrow_token: Any,
        ltv_borrow: int,
    ) -> PreflightVerdict:
        """Check requested borrow value vs collateral value at LTVBorrow."""
        # A borrow that supplies no NEW collateral (collateral_amount == 0) draws
        # against collateral already deposited on-chain in a prior step — the canonical
        # supply-then-borrow lifecycle. The intent carries nothing to size capacity
        # from, so the "new collateral" lower bound would be 0 and any positive borrow
        # would be a guaranteed false reject. Defer to the on-chain EVC solvency check
        # (fail-open, consistent with the price-unavailable branch below). The LTV-
        # enabled structural check in _euler_borrow_verdict has already run.
        if intent.collateral_amount == Decimal("0"):
            return PreflightVerdict.feasible()
        try:
            collateral_price = ctx.services.require_token_price(collateral_token.symbol)
            borrow_price = ctx.services.require_token_price(borrow_token.symbol)
        except Exception as exc:  # noqa: BLE001 - missing price → can't size capacity, fail-open
            logger.debug("Euler capacity preflight: price unavailable (%s); deferring", exc)
            return PreflightVerdict.feasible()
        if collateral_price <= 0 or borrow_price <= 0:
            return PreflightVerdict.feasible()

        collateral_amount: Decimal = intent.collateral_amount  # resolved Decimal (guarded by caller)
        borrow_amount: Decimal = intent.borrow_amount
        # New collateral supplied by this intent. (Existing on-chain collateral
        # would only ADD capacity, so using just the new collateral is the
        # conservative — never a false reject — lower bound.)
        max_borrow_value = (collateral_amount * collateral_price) * (Decimal(ltv_borrow) / Decimal(_LTV_SCALE))
        requested_borrow_value = borrow_amount * borrow_price
        if requested_borrow_value > max_borrow_value:
            return PreflightVerdict(
                outcome=PreflightOutcome.INFEASIBLE,
                error_prefix=self.BORROW_INFEASIBLE_ERROR_PREFIX,
                reason=(
                    f"requested borrow ${requested_borrow_value:.2f} exceeds max ${max_borrow_value:.2f} "
                    f"at LTVBorrow {Decimal(ltv_borrow) / Decimal(_LTV_SCALE):.2%} against "
                    f"{collateral_amount} {collateral_token.symbol} collateral on {ctx.chain}; "
                    f"the EVC solvency check would fail"
                ),
            )
        return PreflightVerdict.feasible()

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
        return _aave_helpers._compile_supply_euler_v2(adapter, intent, supply_token, amount_decimal)

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
        return _aave_helpers._compile_withdraw_euler_v2(
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
        return _aave_helpers._compile_borrow_euler_v2(
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
        return _aave_helpers._compile_repay_euler_v2(
            adapter, intent, repay_token, repay_amount_decimal, amount_description, initial_warnings
        )


__all__ = ["EulerV2Compiler"]
