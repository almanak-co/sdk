"""Connector-owned compiler for the Uniswap V3 protocol family."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._strategy_base.base.cl_math import (
    compute_lp_slippage_mins,
    maybe_recompute_lp_amounts_from_slot0,
)
from almanak.connectors._strategy_base.base.compiler import (
    BaseConcentratedLiquidityCompiler,
    CLAdapterFactoryContext,
    CLCompilerContext,
)
from almanak.connectors._strategy_base.swap_quote_registry import (
    SwapQuoteRequest,
    SwapQuoteResult,
    SwapQuoteUnavailable,
)
from almanak.connectors._strategy_swap_quote_registry import SWAP_QUOTE_REGISTRY, ensure_swap_quote_registry_loaded
from almanak.framework.execution.simulator.config import is_local_rpc
from almanak.framework.intents._compiler_helpers import (
    PriceImpactDecision,
    assemble_action_bundle,
    check_price_impact,
    choose_safer_quote,
    compute_min_amount_out,
    sum_transaction_gas,
)
from almanak.framework.intents.compiler_constants import (
    MAX_UINT128,
    SWAP_ROUTER_ALGEBRA_PROTOCOLS,
    get_gas_estimate,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo, TransactionData
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType, LPCloseIntent, LPOpenIntent, SwapIntent
from almanak.framework.utils.log_formatters import _emojis_enabled, format_percentage, format_token_amount

logger = logging.getLogger(__name__)


class UniswapV3Compiler(BaseConcentratedLiquidityCompiler):
    """Compiler for Uniswap V3 and compile-identical V3 forks."""

    protocols: ClassVar[frozenset[str]] = frozenset({"uniswap_v3", "sushiswap_v3", "pancakeswap_v3", "agni_finance"})
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES}
    )

    def build_lp_adapter_factory(self, factory_context: CLAdapterFactoryContext) -> Callable[[str], Any]:
        """Build the connector-owned V3 LP adapter factory."""
        from almanak.connectors.uniswap_v3.adapter import UniswapV3LPAdapter

        def factory(protocol: str) -> Any:
            return UniswapV3LPAdapter(factory_context.chain, protocol)

        return factory

    def compile_swap(self, ctx: CLCompilerContext, intent: SwapIntent) -> CompilationResult:
        protocol = self._protocol(ctx, intent.protocol)
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            tokens_or_fail = self._resolve_swap_tokens(ctx, intent)
            if isinstance(tokens_or_fail, CompilationResult):
                return tokens_or_fail
            from_token, to_token = tokens_or_fail

            amount_or_fail = self._resolve_swap_amount_in(ctx, intent, from_token)
            if isinstance(amount_or_fail, CompilationResult):
                return amount_or_fail
            amount_in = amount_or_fail

            try:
                expected_output = ctx.services.calculate_expected_output(amount_in, from_token, to_token)
            except ValueError as e:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Cannot calculate slippage protection for {from_token.symbol} -> {to_token.symbol}: {e}. "
                        f"The price oracle does not have a price for one of the tokens. "
                        f"Ensure the token price is available via market.price() before swapping."
                    ),
                    intent_id=intent.intent_id,
                )

            adapter = ctx.default_swap_adapter_factory(protocol)
            router_address = adapter.get_router_address()
            if router_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown router for protocol {protocol} on {ctx.chain}.",
                    intent_id=intent.intent_id,
                )

            if not from_token.is_native:
                transactions.extend(ctx.services.build_approve_tx(from_token.address, router_address, amount_in))

            deadline = int(datetime.now(UTC).timestamp()) + ctx.default_deadline_seconds
            value, actual_from_token, actual_to_token = self._resolve_swap_wrap_addresses(
                ctx=ctx,
                from_token=from_token,
                to_token=to_token,
                amount_in=amount_in,
                warnings=warnings,
            )

            quoter_amount = self._quote_swap_via_registry(
                ctx=ctx,
                protocol=protocol,
                from_token=from_token,
                to_token=to_token,
                actual_from_token=actual_from_token,
                actual_to_token=actual_to_token,
                amount_in=amount_in,
                adapter=adapter,
            )
            if quoter_amount is None:
                try:
                    adapter.select_fee_tier(actual_from_token, actual_to_token, amount_in)
                except Exception as exc:
                    logger.warning("Fee tier pre-selection failed, falling back to oracle estimate: %s", exc)
                quoter_amount = adapter.get_quoted_amount_out()

            slippage_or_fail = self._apply_swap_slippage_and_impact(
                ctx=ctx,
                intent=intent,
                oracle_estimate=expected_output,
                quoter_amount=quoter_amount,
            )
            if isinstance(slippage_or_fail, CompilationResult):
                return slippage_or_fail
            min_output, quoted_output_for_metrics, clamped_expected = slippage_or_fail
            expected_output_human = Decimal(str(quoted_output_for_metrics)) / Decimal(10**to_token.decimals)

            swap_calldata = adapter.get_swap_calldata(
                from_token=actual_from_token,
                to_token=actual_to_token,
                amount_in=amount_in,
                min_amount_out=min_output,
                recipient=ctx.wallet_address,
                deadline=deadline,
            )

            pool_failed = self._validate_swap_pool_after_fee_selection(
                ctx=ctx,
                adapter=adapter,
                protocol=protocol,
                actual_from_token=actual_from_token,
                actual_to_token=actual_to_token,
                intent_id=intent.intent_id,
            )
            if pool_failed is not None:
                return pool_failed

            swap_gas = adapter.estimate_gas(actual_from_token, actual_to_token)
            swap_tx = TransactionData(
                to=router_address,
                value=value,
                data="0x" + swap_calldata.hex(),
                gas_estimate=swap_gas,
                description=(
                    f"Swap {ctx.services.format_amount(amount_in, from_token.decimals)} {from_token.symbol} -> "
                    f"{to_token.symbol} (min: {ctx.services.format_amount(min_output, to_token.decimals)})"
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
                    "protocol": protocol,
                    "router": router_address,
                    "pool_selection_mode": ctx.swap_pool_selection_mode,
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
            logger.info("%s Compiled SWAP: %s → %s (min: %s)", ok, amount_in_fmt, expected_out_fmt, min_out_fmt)
            logger.info("   Slippage: %s | Txs: %d | Gas: %s", slippage_fmt, len(transactions), f"{total_gas:,}")
        except Exception as e:
            logger.exception("Failed to compile SWAP intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def compile_lp_open(self, ctx: CLCompilerContext, intent: LPOpenIntent) -> CompilationResult:
        protocol = self._protocol(ctx, intent.protocol)
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            adapter = ctx.lp_adapter_factory(protocol)
            position_manager = adapter.get_position_manager_address()
            if position_manager == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown position manager for protocol {protocol} on {ctx.chain}",
                    intent_id=intent.intent_id,
                )

            resolved_pool = self._resolve_lp_pool_and_amounts(ctx, intent)
            if isinstance(resolved_pool, CompilationResult):
                return resolved_pool
            token0_info, token1_info, fee_tier, range_lower, range_upper, amount0, amount1 = resolved_pool

            from almanak.connectors.uniswap_v3.pool_validation import validate_v3_pool

            pool_check = validate_v3_pool(
                ctx.chain,
                protocol,
                token0_info.address,
                token1_info.address,
                fee_tier,
                ctx.rpc_url,
                gateway_client=ctx.gateway_client,
            )
            failed = ctx.services.validate_pool(pool_check, intent.intent_id)
            if failed is not None:
                return failed

            amount0_desired = int(amount0 * Decimal(10**token0_info.decimals))
            amount1_desired = int(amount1 * Decimal(10**token1_info.decimals))

            ticks_or_fail = self._compute_lp_ticks(
                ctx=ctx,
                range_lower=range_lower,
                range_upper=range_upper,
                fee_tier=fee_tier,
                token0_info=token0_info,
                token1_info=token1_info,
                intent_id=intent.intent_id,
            )
            if isinstance(ticks_or_fail, CompilationResult):
                return ticks_or_fail
            tick_lower, tick_upper, tick_spacing = ticks_or_fail
            logger.debug(
                "LP tick calculation: price_range=[%.8f, %.8f], decimals=(%s, %s), ticks=[%s, %s], spacing=%s",
                range_lower,
                range_upper,
                token0_info.decimals,
                token1_info.decimals,
                tick_lower,
                tick_upper,
                tick_spacing,
            )

            slot0 = self._fetch_lp_pool_slot0(ctx, pool_check)
            recomputed_or_fail = maybe_recompute_lp_amounts_from_slot0(
                fetch_slot0=lambda pc: self._fetch_lp_pool_slot0(ctx, pc),
                pool_check=pool_check,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
                intent_id=intent.intent_id,
                slot0=slot0,
            )
            if isinstance(recomputed_or_fail, CompilationResult):
                return recomputed_or_fail
            amount0_desired, amount1_desired = recomputed_or_fail

            preflight = self._preflight_lp_liquidity(
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
                intent_id=intent.intent_id,
                slot0=slot0,
            )
            if preflight is not None:
                return preflight

            amount0_min, amount1_min = compute_lp_slippage_mins(
                intent=intent,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
                default_lp_slippage=ctx.default_lp_slippage,
            )
            self._extend_lp_approvals(
                ctx=ctx,
                transactions=transactions,
                token0_info=token0_info,
                token1_info=token1_info,
                position_manager=position_manager,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
            )

            deadline = int(datetime.now(UTC).timestamp()) + ctx.default_deadline_seconds
            mint_calldata = adapter.get_mint_calldata(
                token0=token0_info.address,
                token1=token1_info.address,
                fee=fee_tier,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
                amount0_min=amount0_min,
                amount1_min=amount1_min,
                recipient=ctx.wallet_address,
                deadline=deadline,
            )

            value, native_warning = self._resolve_lp_native_value(
                token0_info=token0_info,
                token1_info=token1_info,
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
            )
            if native_warning:
                warnings.append(native_warning)

            transactions.append(
                TransactionData(
                    to=position_manager,
                    value=value,
                    data="0x" + mint_calldata.hex(),
                    gas_estimate=adapter.estimate_mint_gas(),
                    description=(
                        f"Mint LP position: "
                        f"{ctx.services.format_amount(amount0_desired, token0_info.decimals)} "
                        f"{token0_info.symbol} + "
                        f"{ctx.services.format_amount(amount1_desired, token1_info.decimals)} "
                        f"{token1_info.symbol} "
                        f"[{intent.range_lower:.2f} - {intent.range_upper:.2f}]"
                    ),
                    tx_type="lp_mint",
                )
            )

            total_gas = sum_transaction_gas(transactions)
            result.action_bundle = assemble_action_bundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=transactions,
                metadata={
                    "pool": intent.pool,
                    "token0": token0_info.to_dict(),
                    "token1": token1_info.to_dict(),
                    "fee_tier": fee_tier,
                    "tick_lower": tick_lower,
                    "tick_upper": tick_upper,
                    "range_lower": str(intent.range_lower),
                    "range_upper": str(intent.range_upper),
                    "amount0_desired": str(amount0_desired),
                    "amount1_desired": str(amount1_desired),
                    "amount0_min": str(amount0_min),
                    "amount1_min": str(amount1_min),
                    "protocol": protocol,
                    "position_manager": position_manager,
                    "deadline": deadline,
                    "chain": ctx.chain,
                    # VIB-4614: surface the intent's registry_handle so the
                    # pre-execution registry-collision preflight can tell an
                    # auto-mode open (handle is None) — which the
                    # ix_registry_auto_mode partial unique index guards — from a
                    # handle-supplied open (which the index excludes, so it must
                    # NOT be preflight-blocked). None for the common auto-mode
                    # case; a string when the author disambiguated explicitly.
                    "registry_handle": intent.registry_handle,
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings
            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                "Compiled LP_OPEN intent: %s/%s, range [%.2f-%.2f], %d txs%s, %d gas",
                token0_info.symbol,
                token1_info.symbol,
                intent.range_lower,
                intent.range_upper,
                len(transactions),
                tx_summary,
                total_gas,
            )
        except Exception as e:
            logger.exception("Failed to compile LP_OPEN intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def compile_lp_close(self, ctx: CLCompilerContext, intent: LPCloseIntent) -> CompilationResult:
        protocol = self._protocol(ctx, intent.protocol)
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            adapter = ctx.lp_adapter_factory(protocol)
            position_manager = adapter.get_position_manager_address()
            if position_manager == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown position manager for protocol {protocol} on {ctx.chain}",
                    intent_id=intent.intent_id,
                )

            try:
                token_id = int(intent.position_id)
            except ValueError:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid position ID (must be integer): {intent.position_id}",
                    intent_id=intent.intent_id,
                )

            deadline = int(datetime.now(UTC).timestamp()) + ctx.default_deadline_seconds
            state_or_fail = self._query_lp_close_position_state(
                ctx=ctx,
                position_manager=position_manager,
                token_id=token_id,
                intent_id=intent.intent_id,
                warnings=warnings,
            )
            if isinstance(state_or_fail, CompilationResult):
                return state_or_fail
            liquidity, position_has_activity = state_or_fail

            self._extend_lp_close_transactions(
                ctx=ctx,
                transactions=transactions,
                warnings=warnings,
                adapter=adapter,
                position_manager=position_manager,
                token_id=token_id,
                liquidity=liquidity,
                position_has_activity=position_has_activity,
                collect_fees=intent.collect_fees,
                deadline=deadline,
            )

            total_gas = sum_transaction_gas(transactions)
            no_op = not transactions
            metadata: dict[str, Any] = {
                "position_id": intent.position_id,
                "token_id": token_id,
                "pool": intent.pool,
                "collect_fees": intent.collect_fees,
                "protocol": protocol,
                "position_manager": position_manager,
                "deadline": deadline,
                "chain": ctx.chain,
            }
            if no_op:
                metadata["no_op"] = True
                metadata["reason"] = f"Position #{token_id} already closed (0 liquidity, 0 tokens owed); LP_CLOSE no-op"

            result.action_bundle = assemble_action_bundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=transactions,
                metadata=metadata,
            )
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings
            tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
            tx_summary = f" ({tx_types})" if tx_types else ""
            logger.info(
                "Compiled LP_CLOSE intent: position #%d, collect_fees=%s, %d txs%s, %d gas",
                token_id,
                intent.collect_fees,
                len(transactions),
                tx_summary,
                total_gas,
            )
        except Exception as e:
            logger.exception("Failed to compile LP_CLOSE intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def compile_collect_fees(self, ctx: CLCompilerContext, intent: CollectFeesIntent) -> CompilationResult:
        protocol = self._protocol(ctx, intent.protocol)
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)

        try:
            protocol_params = getattr(intent, "protocol_params", None) or {}
            position_id = protocol_params.get("position_id") or getattr(intent, "position_id", None)
            if not position_id:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"{protocol} LP_COLLECT_FEES requires 'position_id' in protocol_params (NFT tokenId of the position).",
                    intent_id=intent.intent_id,
                )
            try:
                token_id = int(position_id)
            except (TypeError, ValueError):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid position_id (must be integer): {position_id}",
                    intent_id=intent.intent_id,
                )

            adapter = ctx.lp_adapter_factory(protocol)
            position_manager = adapter.get_position_manager_address()
            if position_manager == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown position manager for protocol {protocol} on {ctx.chain}",
                    intent_id=intent.intent_id,
                )

            collect_calldata = adapter.get_collect_calldata(
                token_id=token_id,
                recipient=ctx.wallet_address,
                amount0_max=MAX_UINT128,
                amount1_max=MAX_UINT128,
            )
            tx = TransactionData(
                to=position_manager,
                value=0,
                data="0x" + collect_calldata.hex(),
                gas_estimate=get_gas_estimate(ctx.chain, "lp_collect"),
                description=f"Collect fees: position #{token_id} ({protocol})",
                tx_type="lp_collect_fees",
            )
            transactions = [tx]
            result.action_bundle = assemble_action_bundle(
                intent_type=IntentType.LP_COLLECT_FEES.value,
                transactions=transactions,
                metadata={
                    "position_id": position_id,
                    "token_id": token_id,
                    "protocol": protocol,
                    "position_manager": position_manager,
                    "chain": ctx.chain,
                    "pool": intent.pool,
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = tx.gas_estimate
            logger.info("Compiled V3-fork LP_COLLECT_FEES intent: position #%d, protocol=%s", token_id, protocol)
        except Exception as e:
            logger.exception("Failed to compile V3-fork LP_COLLECT_FEES intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    @staticmethod
    def _protocol(ctx: CLCompilerContext, protocol: str | None) -> str:
        return ctx.protocol

    @staticmethod
    def _resolve_swap_tokens(
        ctx: CLCompilerContext, intent: SwapIntent
    ) -> tuple[TokenInfo, TokenInfo] | CompilationResult:
        from_token = ctx.services.resolve_token(intent.from_token)
        to_token = ctx.services.resolve_token(intent.to_token)
        if from_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED, error=f"Unknown token: {intent.from_token}", intent_id=intent.intent_id
            )
        if to_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED, error=f"Unknown token: {intent.to_token}", intent_id=intent.intent_id
            )
        return from_token, to_token

    @staticmethod
    def _resolve_swap_amount_in(
        ctx: CLCompilerContext, intent: SwapIntent, from_token: TokenInfo
    ) -> int | CompilationResult:
        if intent.amount_usd is not None:
            return ctx.services.usd_to_token_amount(intent.amount_usd, from_token)
        if intent.amount is not None:
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "amount='all' must be resolved before compilation. "
                        "Use Intent.set_resolved_amount() to resolve chained amounts."
                    ),
                    intent_id=intent.intent_id,
                )
            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            return int(amount_decimal * Decimal(10**from_token.decimals))
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error="Either amount_usd or amount must be provided",
            intent_id=intent.intent_id,
        )

    @staticmethod
    def _resolve_swap_wrap_addresses(
        *,
        ctx: CLCompilerContext,
        from_token: TokenInfo,
        to_token: TokenInfo,
        amount_in: int,
        warnings: list[str],
    ) -> tuple[int, str, str]:
        value = 0
        actual_from = from_token.address
        if from_token.is_native:
            value = amount_in
            actual_from = ctx.services.get_wrapped_native_address() or from_token.address
            warnings.append("Native token swap: will wrap to WETH before swapping")

        actual_to = to_token.address
        if to_token.is_native:
            actual_to = ctx.services.get_wrapped_native_address() or to_token.address
            warnings.append("Native token output: will receive WETH, unwrap separately")
        return value, actual_from, actual_to

    @staticmethod
    def _quote_swap_via_registry(
        *,
        ctx: CLCompilerContext,
        protocol: str,
        from_token: TokenInfo,
        to_token: TokenInfo,
        actual_from_token: str,
        actual_to_token: str,
        amount_in: int,
        adapter: Any,
    ) -> int | None:
        request = SwapQuoteRequest(
            chain=ctx.chain,
            protocol=protocol,
            token_in=actual_from_token,
            token_out=actual_to_token,
            amount_in=amount_in,
            token_in_symbol=from_token.symbol,
            token_out_symbol=to_token.symbol,
            token_in_decimals=from_token.decimals,
            token_out_decimals=to_token.decimals,
            fee_tier=ctx.fixed_swap_fee_tier if ctx.swap_pool_selection_mode == "fixed" else None,
        )
        ensure_swap_quote_registry_loaded()
        try:
            quote = SWAP_QUOTE_REGISTRY.quote_swap(ctx, request)
        except SwapQuoteUnavailable as exc:
            logger.warning("Swap quote provider unavailable for %s, falling back to adapter quote: %s", protocol, exc)
            return None
        except Exception as exc:
            logger.warning("Swap quote provider failed for %s, falling back to adapter quote: %s", protocol, exc)
            return None

        if quote is None:
            return None

        selected_fee = quote.metadata.get("fee_tier")
        if selected_fee is None:
            logger.warning("Swap quote provider for %s returned no fee tier, falling back to adapter quote", protocol)
            return None

        UniswapV3Compiler._apply_external_quote_selection(adapter, quote, int(selected_fee))
        return quote.amount_out

    @staticmethod
    def _apply_external_quote_selection(adapter: Any, quote: SwapQuoteResult, selected_fee: int) -> None:
        fee_selection_raw = quote.metadata.get("fee_selection")
        fee_selection = dict(fee_selection_raw) if isinstance(fee_selection_raw, Mapping) else None
        apply_selection = getattr(adapter, "apply_external_quote_selection", None)
        if callable(apply_selection):
            apply_selection(
                fee_tier=selected_fee,
                amount_out=quote.amount_out,
                source=quote.source,
                fee_selection=fee_selection,
            )
            return

        adapter._cached_fee = selected_fee
        adapter.last_quoted_amount_out = quote.amount_out
        adapter.last_fee_selection = fee_selection or {
            "mode": "auto",
            "source": quote.source,
            "selected_fee_tier": selected_fee,
            "candidate_fee_tiers": [selected_fee],
        }

    @staticmethod
    def _apply_swap_slippage_and_impact(
        *,
        ctx: CLCompilerContext,
        intent: SwapIntent,
        oracle_estimate: int,
        quoter_amount: int | None,
    ) -> tuple[int, int, int] | CompilationResult:
        if quoter_amount is not None:
            clamped_expected = quoter_amount
            logger.info(
                "Using executable quoter amount (%s) instead of price oracle estimate (%s) as swap slippage basis",
                quoter_amount,
                oracle_estimate,
            )
        else:
            clamped_expected, used_quoter = choose_safer_quote(oracle_estimate, quoter_amount)
            if used_quoter:
                logger.info(
                    "Quoter amount (%s) is lower than price oracle estimate (%s); "
                    "using quoter amount as slippage basis for safer execution",
                    quoter_amount,
                    oracle_estimate,
                )

        offline_mode = ctx.using_placeholders or ctx.permission_discovery
        impact: PriceImpactDecision | None = None
        impact_result = None
        if quoter_amount is not None and UniswapV3Compiler._is_local_anvil_rpc(ctx.rpc_url):
            logger.info(
                "Skipping oracle price-impact guard for local Anvil fork quote "
                "(rpc_url=%s). Fork block pool state and live oracle prices are not time-aligned.",
                ctx.rpc_url,
            )
        else:
            impact_result = check_price_impact(
                oracle_estimate=oracle_estimate,
                quoter_amount=quoter_amount,
                intent_max_impact=intent.max_price_impact,
                config_max_impact=ctx.max_price_impact_pct,
                offline_mode=offline_mode,
                using_placeholders=ctx.using_placeholders,
            )
            impact = impact_result.decision
        if impact is PriceImpactDecision.IMPACT_TOO_HIGH and impact_result is not None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Price impact too high: quoter returned amount implying "
                    f"{impact_result.price_impact:.1%} price impact "
                    f"(oracle estimate: {oracle_estimate}, quoter: {quoter_amount}). "
                    f"Maximum allowed: {impact_result.effective_max_impact:.0%}. "
                    f"Likely cause: pool has insufficient liquidity for "
                    f"{intent.from_token}->{intent.to_token}."
                ),
            )
        if impact is PriceImpactDecision.QUOTER_MISSING_FAIL_CLOSED:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Price impact guard: on-chain quoter returned no amount for "
                    f"{intent.from_token}->{intent.to_token}. Cannot verify pool liquidity "
                    f"or price impact. Refusing to compile a swap backed only by the oracle price. "
                    f"Check RPC availability and that the pool has liquidity at the selected fee tier."
                ),
                intent_id=intent.intent_id,
            )

        quoted_for_metrics = quoter_amount if quoter_amount is not None else oracle_estimate
        min_output = compute_min_amount_out(clamped_expected, intent.max_slippage)
        return min_output, quoted_for_metrics, clamped_expected

    @staticmethod
    def _is_local_anvil_rpc(rpc_url: str | None) -> bool:
        return is_local_rpc(rpc_url)

    @staticmethod
    def _validate_swap_pool_after_fee_selection(
        *,
        ctx: CLCompilerContext,
        adapter: Any,
        protocol: str,
        actual_from_token: str,
        actual_to_token: str,
        intent_id: str,
    ) -> CompilationResult | None:
        if protocol in SWAP_ROUTER_ALGEBRA_PROTOCOLS:
            return None
        selected_fee = adapter.last_fee_selection.get("selected_fee_tier")
        if selected_fee is None:
            return None
        from almanak.connectors.uniswap_v3.pool_validation import validate_v3_pool

        pool_check = validate_v3_pool(
            ctx.chain,
            protocol,
            actual_from_token,
            actual_to_token,
            selected_fee,
            ctx.rpc_url,
            gateway_client=ctx.gateway_client,
        )
        return ctx.services.validate_pool(pool_check, intent_id)

    @staticmethod
    def _resolve_lp_pool_and_amounts(
        ctx: CLCompilerContext, intent: LPOpenIntent
    ) -> tuple[TokenInfo, TokenInfo, int, Decimal, Decimal, Decimal, Decimal] | CompilationResult:
        pool_info = ctx.services.parse_pool_info(intent.pool)
        if pool_info is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Could not parse pool info: {intent.pool}",
                intent_id=intent.intent_id,
            )
        token0_info, token1_info, fee_tier, tokens_swapped = pool_info
        range_lower = intent.range_lower
        range_upper = intent.range_upper
        amount0 = intent.amount0
        amount1 = intent.amount1
        if tokens_swapped:
            range_lower = Decimal(1) / intent.range_upper
            range_upper = Decimal(1) / intent.range_lower
            amount0, amount1 = amount1, amount0
            logger.debug(
                "Tokens swapped: inverted price range [%s, %s] -> [%.10f, %.10f], swapped amounts",
                intent.range_lower,
                intent.range_upper,
                range_lower,
                range_upper,
            )
        return token0_info, token1_info, fee_tier, range_lower, range_upper, amount0, amount1

    @staticmethod
    def _compute_lp_ticks(
        *,
        ctx: CLCompilerContext,
        range_lower: Decimal,
        range_upper: Decimal,
        fee_tier: int,
        token0_info: TokenInfo,
        token1_info: TokenInfo,
        intent_id: str,
    ) -> tuple[int, int, int] | CompilationResult:
        tick_lower = ctx.services.price_to_tick(
            range_lower,
            token0_decimals=token0_info.decimals,
            token1_decimals=token1_info.decimals,
        )
        tick_upper = ctx.services.price_to_tick(
            range_upper,
            token0_decimals=token0_info.decimals,
            token1_decimals=token1_info.decimals,
        )
        tick_spacing = ctx.services.get_tick_spacing(fee_tier)
        tick_lower = (tick_lower // tick_spacing) * tick_spacing
        tick_upper = (tick_upper // tick_spacing) * tick_spacing
        if tick_lower >= tick_upper:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "LP_OPEN tick range collapsed after applying pool tick spacing. "
                    "Widen the price range so lower and upper ticks differ."
                ),
                intent_id=intent_id,
            )
        return tick_lower, tick_upper, tick_spacing

    # _fetch_lp_pool_slot0 is inherited from BaseConcentratedLiquidityCompiler
    # (shared V3-family slot0 read); the override was lifted to the base so the
    # V3 forks reuse it without importing this connector.

    @staticmethod
    def _preflight_lp_liquidity(
        *,
        tick_lower: int,
        tick_upper: int,
        amount0_desired: int,
        amount1_desired: int,
        intent_id: str,
        slot0: tuple[int, int] | None = None,
    ) -> CompilationResult | None:
        if amount0_desired == 0 and amount1_desired == 0:
            return None
        from almanak.framework.intents.intent_errors import LpOpenZeroLiquidityError
        from almanak.framework.intents.lp_math import (
            liquidity_for_amounts_at_sqrt_price,
            range_midpoint_sqrt_price_x96,
        )

        sqrt_price_x96: int | None = None
        used_live = False
        if slot0 is not None:
            candidate, _current_tick = slot0
            if candidate and candidate > 0:
                sqrt_price_x96 = candidate
                used_live = True

        if sqrt_price_x96 is None:
            if amount0_desired == 0 or amount1_desired == 0:
                return None
            sqrt_price_x96 = range_midpoint_sqrt_price_x96(tick_lower, tick_upper)
            if sqrt_price_x96 == 0:
                return None

        liquidity = liquidity_for_amounts_at_sqrt_price(
            sqrt_price_x96,
            tick_lower,
            tick_upper,
            amount0_desired,
            amount1_desired,
        )
        if liquidity <= 0:
            reason_suffix = (
                "(checked against live pool sqrtPriceX96)"
                if used_live
                else "(checked against range geometric midpoint; no live sqrt-price available)"
            )
            err = LpOpenZeroLiquidityError(
                amount0_desired=amount0_desired,
                amount1_desired=amount1_desired,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                reason=(
                    f"getLiquidityForAmounts returned 0 {reason_suffix}. "
                    f"Either widen the tick range, choose a wider fee tier, or "
                    f"increase the deposit amounts."
                ),
            )
            return CompilationResult(status=CompilationStatus.FAILED, error=str(err), intent_id=intent_id)
        return None

    @staticmethod
    def _extend_lp_approvals(
        *,
        ctx: CLCompilerContext,
        transactions: list[TransactionData],
        token0_info: TokenInfo,
        token1_info: TokenInfo,
        position_manager: str,
        amount0_desired: int,
        amount1_desired: int,
    ) -> None:
        if amount0_desired > 0 and not token0_info.is_native:
            transactions.extend(ctx.services.build_approve_tx(token0_info.address, position_manager, amount0_desired))
        if amount1_desired > 0 and not token1_info.is_native:
            transactions.extend(ctx.services.build_approve_tx(token1_info.address, position_manager, amount1_desired))

    @staticmethod
    def _resolve_lp_native_value(
        *,
        token0_info: TokenInfo,
        token1_info: TokenInfo,
        amount0_desired: int,
        amount1_desired: int,
    ) -> tuple[int, str | None]:
        if token0_info.is_native:
            return amount0_desired, "Native token in LP: sending ETH value"
        if token1_info.is_native:
            return amount1_desired, "Native token in LP: sending ETH value"
        return 0, None

    @staticmethod
    def _query_lp_close_position_state(
        *,
        ctx: CLCompilerContext,
        position_manager: str,
        token_id: int,
        intent_id: str,
        warnings: list[str],
    ) -> tuple[int, bool] | CompilationResult:
        liquidity = ctx.services.query_position_liquidity(position_manager, token_id)
        if liquidity is None:
            if ctx.permission_discovery:
                logger.debug("Permission discovery mode: using synthetic liquidity for position #%d", token_id)
                return 10**18, True
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Could not query liquidity for position #{token_id}. Ensure rpc_url is provided to IntentCompiler.",
                intent_id=intent_id,
            )

        tokens_owed0, tokens_owed1 = ctx.services.query_position_tokens_owed(position_manager, token_id)
        tokens_owed_unknown = tokens_owed0 is None or tokens_owed1 is None
        if tokens_owed_unknown:
            warnings.append(f"Could not query tokens owed for position #{token_id} - collecting anyway")
        elif tokens_owed0 == 0 and tokens_owed1 == 0:
            warnings.append(f"Position #{token_id} has no tokens owed pre-decrease - will still collect after close")

        position_has_activity = (
            liquidity > 0
            or tokens_owed_unknown
            or (tokens_owed0 is not None and tokens_owed1 is not None and (tokens_owed0 > 0 or tokens_owed1 > 0))
        )
        return liquidity, position_has_activity

    @staticmethod
    def _extend_lp_close_transactions(
        *,
        ctx: CLCompilerContext,
        transactions: list[TransactionData],
        warnings: list[str],
        adapter: Any,
        position_manager: str,
        token_id: int,
        liquidity: int,
        position_has_activity: bool,
        collect_fees: bool,
        deadline: int,
    ) -> None:
        if liquidity == 0:
            warnings.append(f"Position #{token_id} has 0 liquidity - skipping decreaseLiquidity step")
        else:
            decrease_calldata = adapter.get_decrease_liquidity_calldata(
                token_id=token_id,
                liquidity=liquidity,
                amount0_min=0,
                amount1_min=0,
                deadline=deadline,
            )
            transactions.append(
                TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + decrease_calldata.hex(),
                    gas_estimate=get_gas_estimate(ctx.chain, "lp_decrease_liquidity"),
                    description=f"Decrease liquidity: position #{token_id} (remove all)",
                    tx_type="lp_decrease_liquidity",
                )
            )

        # Collect is mandatory on LP_CLOSE whenever the position has activity:
        # decreaseLiquidity moves principal into the position's tokensOwed, and
        # NonfungiblePositionManager.burn() reverts ("Not cleared") unless both
        # liquidity and tokensOwed are zero. Gating collect on collect_fees here
        # would strand principal and revert the subsequent burn. collect_fees is
        # therefore informational on a close (mirrors Aerodrome Slipstream /
        # Uniswap V4, where collect is always part of the close path).
        if position_has_activity:
            if not collect_fees:
                warnings.append(
                    f"collect_fees=False ignored for LP_CLOSE position #{token_id}: "
                    "collect is required to return principal and satisfy the burn precondition"
                )
            collect_calldata = adapter.get_collect_calldata(
                token_id=token_id,
                recipient=ctx.wallet_address,
                amount0_max=MAX_UINT128,
                amount1_max=MAX_UINT128,
            )
            transactions.append(
                TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + collect_calldata.hex(),
                    gas_estimate=get_gas_estimate(ctx.chain, "lp_collect"),
                    description=f"Collect tokens and fees: position #{token_id}",
                    tx_type="lp_collect",
                )
            )
        else:
            warnings.append(f"Skipping collect for position #{token_id} - position appears already closed")

        if position_has_activity:
            burn_calldata = adapter.get_burn_calldata(token_id=token_id)
            transactions.append(
                TransactionData(
                    to=position_manager,
                    value=0,
                    data="0x" + burn_calldata.hex(),
                    gas_estimate=get_gas_estimate(ctx.chain, "lp_burn"),
                    description=f"Burn position NFT: #{token_id}",
                    tx_type="lp_burn",
                )
            )
        else:
            warnings.append(f"Position #{token_id} appears already closed (0 liquidity, 0 tokens owed) - skipping burn")


__all__ = ["UniswapV3Compiler"]
