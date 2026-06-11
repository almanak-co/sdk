"""Connector-owned compiler for Fluid DEX swaps.

Fluid is SWAP-only at this compile boundary (Phase 1, VIB-5029). Fluid DEX
has **no router** — each pool is its own contract and ``swapIn`` executes
directly on it, so the approve target and the swap target are both the
per-pair pool address resolved at compile time via the DexReservesResolver.

LP intents are intentionally NOT supported: direct pool deposits are
whitelist-gated at Fluid's Liquidity layer (``DexT1__UserSupplyInNotOn``,
verified Phase 0 / VIB-5028 §V4) — retail LP access goes through
SmartLending wrappers or smart vaults, which is Phase-4 scope (VIB-5032).

Quoting goes through ``DexReservesResolver.estimateSwapIn`` — Fluid's
official quote surface; quotes match on-chain execution to the wei
(Phase-0 V1.4). The connector was previously disabled (VIB-2822) due to a
broken eth_call state-override quote shim, not a protocol issue.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import (
    BaseCompilerContext,
    BaseProtocolCompiler,
    SwapCompilerContext,
)
from almanak.framework.intents._compiler_helpers import (
    PriceImpactDecision,
    assemble_action_bundle,
    check_price_impact,
    choose_safer_quote,
    compute_min_amount_out,
    sum_transaction_gas,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from almanak.framework.utils.log_formatters import _emojis_enabled, format_percentage, format_token_amount

logger = logging.getLogger(__name__)


class FluidCompiler(BaseProtocolCompiler[SwapCompilerContext]):
    """Fluid DEX swap compiler. SWAP-only, routerless (per-pool targets).

    Subclasses ``BaseProtocolCompiler[SwapCompilerContext]`` because the
    swap slippage / price-impact guard reads ``max_price_impact_pct`` and
    ``using_placeholders`` — same shape as ``CamelotCompiler``.
    """

    protocols: ClassVar[frozenset[str]] = frozenset({"fluid"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.SWAP})
    chains: ClassVar[frozenset[str]] = frozenset({"arbitrum", "base", "ethereum", "polygon"})
    context_type: ClassVar[type[BaseCompilerContext]] = SwapCompilerContext

    def compile(self, ctx: SwapCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        if getattr(intent, "intent_type", None) == IntentType.SWAP:
            return self.compile_swap(ctx, intent)
        return self._unsupported(intent)

    def compile_swap(self, ctx: SwapCompilerContext, intent: SwapIntent) -> CompilationResult:
        """Compile a Fluid DEX exact-input swap.

        Pipeline: resolve tokens (native legs map to Fluid's ``0xEeee…``
        sentinel — Fluid pools pair raw native, not WETH) → resolve the
        per-pair pool + direction on-chain → quote via the reserves
        resolver → price-impact guard → approve (ERC-20 input only) +
        ``swapIn`` on the pool.
        """
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            inputs = self._resolve_swap_inputs(ctx, intent)
            if isinstance(inputs, CompilationResult):
                return inputs
            from_token, to_token, amount_in, expected_output = inputs

            from almanak.connectors.fluid.sdk import (
                FLUID_NATIVE_TOKEN,
                FluidMinAmountError,
                FluidSDK,
                FluidSDKError,
            )

            sdk = self._build_sdk(ctx, FluidSDK, intent)
            if isinstance(sdk, CompilationResult):
                return sdk

            # Fluid pools hold the chain's native gas token directly — no
            # WETH wrapping on either leg.
            value = 0
            fluid_from = from_token.address
            if from_token.is_native:
                fluid_from = FLUID_NATIVE_TOKEN
                value = amount_in
                warnings.append("Native-input swap: amount sent as msg.value to the pool (no approve)")
            fluid_to = to_token.address
            if to_token.is_native:
                fluid_to = FLUID_NATIVE_TOKEN
                warnings.append("Native-output swap: pool pays raw native token to the wallet")

            try:
                found = sdk.find_pool_for_pair(fluid_from, fluid_to)
            except FluidSDKError as exc:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Fluid pool not found for {intent.from_token}->{intent.to_token} on "
                        f"{ctx.chain}: pool enumeration failed ({exc}). Pool discovery requires "
                        f"an on-chain lookup (RPC or gateway)."
                    ),
                    intent_id=intent.intent_id,
                )
            if found is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"No Fluid DEX pool exists for {intent.from_token}->{intent.to_token} "
                        f"on {ctx.chain}. Fluid pools are per-pair contracts; this pair is not "
                        f"deployed. Use a routed protocol (uniswap_v3, enso) for arbitrary pairs."
                    ),
                    intent_id=intent.intent_id,
                )
            pool_address, swap0to1 = found

            quoter_amount: int | None = None
            try:
                quoter_amount = sdk.get_swap_quote(pool_address, swap0to1, amount_in)
            except FluidMinAmountError as exc:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Fluid swap size limit-gated on {ctx.chain} pool {pool_address}: {exc} "
                        f"(Fluid's Liquidity-layer limits expand over time — this is retryable, "
                        f"not a permanent failure.)"
                    ),
                    intent_id=intent.intent_id,
                )
            except FluidSDKError as exc:
                logger.warning("Fluid resolver quote failed, price-impact guard decides: %s", exc)

            clamped_expected, used_quoter = choose_safer_quote(expected_output, quoter_amount)
            if used_quoter:
                logger.info(
                    "Fluid resolver quote (%s) is lower than price oracle estimate (%s) — "
                    "using resolver quote as slippage basis for safer execution",
                    quoter_amount,
                    expected_output,
                )

            offline_mode = ctx.using_placeholders or ctx.permission_discovery
            impact = check_price_impact(
                oracle_estimate=expected_output,
                quoter_amount=quoter_amount,
                intent_max_impact=intent.max_price_impact,
                config_max_impact=ctx.max_price_impact_pct,
                offline_mode=offline_mode,
                using_placeholders=ctx.using_placeholders,
            )
            if impact.decision is PriceImpactDecision.IMPACT_TOO_HIGH:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Price impact too high: resolver quote implies "
                        f"{impact.price_impact:.1%} price impact "
                        f"(oracle estimate: {expected_output}, resolver: {quoter_amount}). "
                        f"Maximum allowed: {impact.effective_max_impact:.0%}. "
                        f"Likely cause: Fluid pool {pool_address} has insufficient depth for "
                        f"{intent.from_token}->{intent.to_token} at this size."
                    ),
                    intent_id=intent.intent_id,
                )
            if impact.decision is PriceImpactDecision.QUOTER_MISSING_FAIL_CLOSED:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Price impact guard: Fluid reserves resolver returned no quote for "
                        f"{intent.from_token}->{intent.to_token}. Cannot verify pool liquidity. "
                        f"Refusing to compile a Fluid swap backed only by the oracle price."
                    ),
                    intent_id=intent.intent_id,
                )

            min_output = compute_min_amount_out(clamped_expected, intent.max_slippage)
            quoted_for_metrics = quoter_amount if quoter_amount is not None else expected_output
            expected_output_human = Decimal(str(quoted_for_metrics)) / Decimal(10**to_token.decimals)

            if not from_token.is_native:
                transactions.extend(ctx.services.build_approve_tx(from_token.address, pool_address, amount_in))

            swap_tx_dict = sdk.build_swap_tx(
                dex_address=pool_address,
                swap0to1=swap0to1,
                amount_in=amount_in,
                amount_out_min=min_output,
                to=ctx.wallet_address,
                value=value,
            )
            transactions.append(
                TransactionData(
                    to=swap_tx_dict["to"],
                    value=swap_tx_dict["value"],
                    data=swap_tx_dict["data"],
                    gas_estimate=swap_tx_dict["gas"],
                    description=(
                        f"Swap {ctx.services.format_amount(amount_in, from_token.decimals)} "
                        f"{from_token.symbol} -> {to_token.symbol} via Fluid pool {pool_address} "
                        f"(min: {ctx.services.format_amount(min_output, to_token.decimals)})"
                    ),
                    tx_type="swap",
                )
            )

            total_gas = sum_transaction_gas(transactions)
            result.action_bundle = assemble_action_bundle(
                intent_type=IntentType.SWAP.value,
                transactions=transactions,
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "min_amount_out": str(min_output),
                    "expected_output_human": str(expected_output_human),
                    "slippage": str(intent.max_slippage),
                    "protocol": "fluid",
                    "pool": pool_address,
                    "swap0to1": swap0to1,
                    "chain": ctx.chain,
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            amount_in_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            expected_out_fmt = format_token_amount(clamped_expected, to_token.symbol, to_token.decimals)
            min_out_fmt = format_token_amount(min_output, to_token.symbol, to_token.decimals)
            slippage_fmt = format_percentage(intent.max_slippage)
            ok = "✅" if _emojis_enabled() else "[OK]"
            logger.info(
                "%s Compiled Fluid SWAP: %s → %s (min: %s, pool: %s)",
                ok,
                amount_in_fmt,
                expected_out_fmt,
                min_out_fmt,
                pool_address,
            )
            logger.info("   Slippage: %s | Txs: %d | Gas: %s", slippage_fmt, len(transactions), f"{total_gas:,}")
        except Exception as e:
            logger.exception("Failed to compile Fluid SWAP intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    @staticmethod
    def _build_sdk(ctx: SwapCompilerContext, sdk_cls: type, intent: SwapIntent) -> Any | CompilationResult:
        """Construct ``FluidSDK`` with gateway-preferred transport.

        Mirrors the connector's historical transport selection: a connected
        gateway client wins; otherwise fall back to the context RPC URL.
        """
        gateway_client = ctx.gateway_client
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            gateway_client = None
        if gateway_client is None and not ctx.rpc_url:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "Fluid pool not found: pool discovery and quoting require a connected "
                    "gateway client or RPC URL (Fluid is routerless — the per-pair pool "
                    "address is resolved on-chain at compile time)."
                ),
                intent_id=intent.intent_id,
            )
        return sdk_cls(
            chain=ctx.chain,
            rpc_url=None if gateway_client is not None else ctx.rpc_url,
            gateway_client=gateway_client,
        )

    @staticmethod
    def _resolve_swap_inputs(
        ctx: SwapCompilerContext, intent: SwapIntent
    ) -> tuple[Any, Any, int, int] | CompilationResult:
        """Resolve tokens + amount_in + oracle expected output.

        Returns a 4-tuple ``(from_token, to_token, amount_in, expected_output)``
        on success, or a FAILED ``CompilationResult`` on any setup failure.
        Same shape as ``CamelotCompiler._resolve_swap_inputs``.
        """
        from_token = ctx.services.resolve_token(intent.from_token)
        if from_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {intent.from_token}",
                intent_id=intent.intent_id,
            )
        to_token = ctx.services.resolve_token(intent.to_token)
        if to_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {intent.to_token}",
                intent_id=intent.intent_id,
            )

        if intent.amount_usd is not None:
            amount_in = ctx.services.usd_to_token_amount(intent.amount_usd, from_token)
        elif intent.amount is not None:
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "amount='all' must be resolved before compilation. "
                        "Use Intent.set_resolved_amount() to resolve chained amounts."
                    ),
                    intent_id=intent.intent_id,
                )
            # mypy can't narrow Decimal | Literal["all"] through an `==` check;
            # assignment-narrowing matches CamelotCompiler._resolve_swap_inputs.
            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Either amount_usd or amount must be provided",
                intent_id=intent.intent_id,
            )

        try:
            expected_output = ctx.services.calculate_expected_output(amount_in, from_token, to_token)
        except ValueError as e:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Cannot calculate slippage protection for "
                    f"{from_token.symbol} -> {to_token.symbol}: {e}. "
                    f"The price oracle does not have a price for one of the tokens. "
                    f"Ensure the token price is available via market.price() before swapping."
                ),
                intent_id=intent.intent_id,
            )

        return from_token, to_token, amount_in, expected_output


__all__ = ["FluidCompiler"]
