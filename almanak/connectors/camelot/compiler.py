"""Connector-owned compiler for Camelot Algebra V3 swaps.

Camelot is intentionally **not** a CL-family compiler — Camelot pools are
concentrated-liquidity on-chain, but this connector only compiles swaps.
LP / collect-fees intents are NOT supported by this connector; ``compile``
fails-close on any non-SWAP intent type, and the ``intents`` ClassVar
declares exactly what's supported for framework introspection.

The swap pipeline is implemented natively here (no inheritance from
``UniswapV3Compiler``) so the compiler's class shape honestly reflects
what Camelot supports. The Algebra-vs-V3 calldata divergence is handled
by ``DefaultSwapAdapter`` based on the protocol string
(``SWAP_ROUTER_ALGEBRA_PROTOCOLS`` in ``compiler_constants``) — Camelot
just instantiates the adapter directly rather than going through the
CL-context factory.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import (
    BaseCompilerContext,
    BaseProtocolCompiler,
    SwapCompilerContext,
)
from almanak.connectors._strategy_base.base.swap_adapter import DefaultSwapAdapter
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


class CamelotCompiler(BaseProtocolCompiler[SwapCompilerContext]):
    """Camelot Algebra V3 swap compiler. Arbitrum, SWAP-only.

    Subclasses ``BaseProtocolCompiler[SwapCompilerContext]`` — not the CL
    base — so the class shape doesn't claim any LP capability. Uses
    ``SwapCompilerContext`` (not ``BaseCompilerContext``) because it reads
    ``max_price_impact_pct`` and ``using_placeholders`` for the swap
    slippage / price-impact guard; those knobs are not on the universal
    base since lending / perp / bridge compilers don't have them.
    """

    protocols: ClassVar[frozenset[str]] = frozenset({"camelot"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.SWAP})
    chains: ClassVar[frozenset[str]] = frozenset({"arbitrum"})
    context_type: ClassVar[type[BaseCompilerContext]] = SwapCompilerContext

    def compile(self, ctx: SwapCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        if getattr(intent, "intent_type", None) == IntentType.SWAP:
            return self.compile_swap(ctx, intent)
        return self._unsupported(intent)

    def compile_swap(self, ctx: SwapCompilerContext, intent: SwapIntent) -> CompilationResult:
        """Compile a Camelot Algebra V3 swap intent.

        Algebra V3's ``exactInputSingle`` lacks a ``fee`` parameter (fees are
        set dynamically by the pool); ``DefaultSwapAdapter`` encodes the
        correct Algebra-shaped struct based on ``protocol="camelot"`` via
        ``SWAP_ROUTER_ALGEBRA_PROTOCOLS``. There is no pool-validation step
        (Algebra has no fee tier to validate against) — the on-chain quoter
        is the source of truth for swap viability.
        """
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            inputs = self._resolve_swap_inputs(ctx, intent)
            if isinstance(inputs, CompilationResult):
                return inputs
            from_token, to_token, amount_in, expected_output = inputs

            # Algebra V3: no fixed fee tiers, no pool selection mode — the
            # adapter knows about Camelot via SWAP_ROUTER_ALGEBRA_PROTOCOLS
            # and emits the right struct shape from the protocol string alone.
            adapter = DefaultSwapAdapter(
                ctx.chain,
                "camelot",
                pool_selection_mode="auto",
                fixed_fee_tier=None,
                rpc_url=ctx.rpc_url,
                rpc_timeout=ctx.rpc_timeout,
                gateway_client=ctx.gateway_client,
            )
            router_address = adapter.get_router_address()
            if router_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown Camelot router on {ctx.chain}.",
                    intent_id=intent.intent_id,
                )

            if not from_token.is_native:
                transactions.extend(ctx.services.build_approve_tx(from_token.address, router_address, amount_in))

            deadline = int(datetime.now(UTC).timestamp()) + ctx.default_deadline_seconds

            value = 0
            actual_from_token = from_token.address
            if from_token.is_native:
                value = amount_in
                actual_from_token = ctx.services.get_wrapped_native_address() or from_token.address
                warnings.append("Native token swap: will wrap to WETH before swapping")
            actual_to_token = to_token.address
            if to_token.is_native:
                actual_to_token = ctx.services.get_wrapped_native_address() or to_token.address
                warnings.append("Native token output: will receive WETH, unwrap separately")

            # Populates ``last_quoted_amount_out`` for the price-impact guard.
            # Algebra ignores fee-tier selection here; the call is a no-op
            # for fee selection but does kick off the on-chain quoter call.
            try:
                adapter.select_fee_tier(actual_from_token, actual_to_token, amount_in)
            except Exception as exc:
                logger.warning("Algebra quoter pre-selection failed, falling back to oracle estimate: %s", exc)

            quoter_amount = adapter.get_quoted_amount_out()
            clamped_expected, used_quoter = choose_safer_quote(expected_output, quoter_amount)
            if used_quoter:
                logger.info(
                    "Algebra quoter amount (%s) is lower than price oracle estimate (%s) — "
                    "using quoter amount as slippage basis for safer execution",
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
                        f"Price impact too high: quoter returned amount implying "
                        f"{impact.price_impact:.1%} price impact "
                        f"(oracle estimate: {expected_output}, quoter: {quoter_amount}). "
                        f"Maximum allowed: {impact.effective_max_impact:.2%}. "
                        f"Likely cause: pool has insufficient liquidity for "
                        f"{intent.from_token}->{intent.to_token} on Camelot."
                    ),
                )
            if impact.decision is PriceImpactDecision.QUOTER_MISSING_FAIL_CLOSED:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Price impact guard: Algebra quoter returned no amount for "
                        f"{intent.from_token}->{intent.to_token}. Cannot verify pool liquidity. "
                        f"Refusing to compile a Camelot swap backed only by the oracle price. "
                        f"Check RPC availability and pool liquidity at the Algebra V3 pool."
                    ),
                    intent_id=intent.intent_id,
                )

            min_output = compute_min_amount_out(clamped_expected, intent.max_slippage)
            quoted_for_metrics = quoter_amount if quoter_amount is not None else expected_output
            expected_output_human = Decimal(str(quoted_for_metrics)) / Decimal(10**to_token.decimals)

            swap_calldata = adapter.get_swap_calldata(
                from_token=actual_from_token,
                to_token=actual_to_token,
                amount_in=amount_in,
                min_amount_out=min_output,
                recipient=ctx.wallet_address,
                deadline=deadline,
            )

            swap_gas = adapter.estimate_gas(actual_from_token, actual_to_token)
            swap_tx = TransactionData(
                to=router_address,
                value=value,
                data="0x" + swap_calldata.hex(),
                gas_estimate=swap_gas,
                description=(
                    f"Swap {ctx.services.format_amount(amount_in, from_token.decimals)} "
                    f"{from_token.symbol} -> {to_token.symbol} via Camelot "
                    f"(min: {ctx.services.format_amount(min_output, to_token.decimals)})"
                ),
                tx_type="swap",
            )
            transactions.append(swap_tx)

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
                    "protocol": "camelot",
                    "router": router_address,
                    # Algebra has no fixed fee tiers; the pool selection mode
                    # is always "auto" (the adapter relies on the on-chain
                    # quoter rather than a configured fee tier).
                    "pool_selection_mode": "auto",
                    # Surfaced from the adapter for observability + downstream
                    # parsers (e.g. test_camelot_swap_compile_succeeds_with_quoter
                    # pins fee_selection_source == "algebra_quoter").
                    "selected_fee_tier": adapter.last_fee_selection.get("selected_fee_tier"),
                    "fee_tier_candidates": adapter.last_fee_selection.get("candidate_fee_tiers"),
                    "fee_selection_source": adapter.last_fee_selection.get("source"),
                    "deadline": deadline,
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
                "%s Compiled Camelot SWAP: %s → %s (min: %s)",
                ok,
                amount_in_fmt,
                expected_out_fmt,
                min_out_fmt,
            )
            logger.info("   Slippage: %s | Txs: %d | Gas: %s", slippage_fmt, len(transactions), f"{total_gas:,}")
        except Exception as e:
            logger.exception("Failed to compile Camelot SWAP intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    @staticmethod
    def _resolve_swap_inputs(
        ctx: SwapCompilerContext, intent: SwapIntent
    ) -> tuple[Any, Any, int, int] | CompilationResult:
        """Resolve tokens + amount_in + oracle expected output.

        Returns a 4-tuple ``(from_token, to_token, amount_in, expected_output)``
        on success, or a FAILED ``CompilationResult`` on any setup failure.
        Extracted out of ``compile_swap`` so the main method stays under the
        CRAP cyclomatic-complexity threshold while keeping each setup step
        self-explanatory.
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
            # mirror UniswapV3Compiler._resolve_swap_amount_in's assignment-narrowing.
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
