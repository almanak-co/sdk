"""Connector-owned compiler for LiFi aggregator swaps."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, ClassVar

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler, SwapCompilerContext
from almanak.framework.intents._compiler_helpers import (
    assemble_action_bundle,
    choose_lifi_gas_estimate,
    parse_lifi_tx_value,
    sum_transaction_gas,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo, TransactionData
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from almanak.framework.utils.log_formatters import format_percentage, format_token_amount

logger = logging.getLogger(__name__)


class LiFiCompiler(BaseProtocolCompiler[SwapCompilerContext]):
    """Compile same-chain and cross-chain LiFi SWAP intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"lifi"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.SWAP})
    chains: ClassVar[frozenset[str]] = frozenset(
        {"ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb"}
    )
    context_type: ClassVar[type[BaseCompilerContext]] = SwapCompilerContext

    def compile(self, ctx: SwapCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        if getattr(intent, "intent_type", None) == IntentType.SWAP:
            return self.compile_swap(ctx, intent)
        return self._unsupported(intent)

    def compile_swap(self, ctx: SwapCompilerContext, intent: SwapIntent) -> CompilationResult:
        from almanak.framework.connectors.lifi import CHAIN_MAPPING, LiFiAdapter, LiFiConfig
        from almanak.framework.connectors.lifi.client import NATIVE_TOKEN_ADDRESS as LIFI_NATIVE_ADDRESS

        transactions: list[TransactionData] = []

        try:
            chain_check = self._validate_lifi_chains(ctx, intent, CHAIN_MAPPING)
            if isinstance(chain_check, CompilationResult):
                return chain_check
            source_chain, dest_chain, from_chain_id, to_chain_id, is_cross_chain = chain_check

            tokens_check = self._resolve_lifi_tokens_and_amount(ctx, intent, source_chain, dest_chain)
            if isinstance(tokens_check, CompilationResult):
                return tokens_check
            from_token, to_token, amount_in = tokens_check

            lifi_from_address = LIFI_NATIVE_ADDRESS if from_token.is_native else from_token.address
            lifi_to_address = LIFI_NATIVE_ADDRESS if to_token.is_native else to_token.address

            logger.info(
                "Getting LiFi quote: %s@%s -> %s@%s, amount=%s",
                from_token.symbol,
                source_chain,
                to_token.symbol,
                dest_chain,
                amount_in,
            )
            adapter = LiFiAdapter(
                LiFiConfig(chain_id=from_chain_id, wallet_address=ctx.wallet_address),
                price_provider=ctx.price_oracle,
                allow_placeholder_prices=ctx.using_placeholders,
            )
            slippage = float(intent.max_slippage)
            quote = adapter.client.get_quote(
                from_chain_id=from_chain_id,
                to_chain_id=to_chain_id,
                from_token=lifi_from_address,
                to_token=lifi_to_address,
                from_amount=str(amount_in),
                from_address=ctx.wallet_address,
                slippage=slippage,
            )

            approval_address = quote.estimate.approval_address if quote.estimate else ""
            if approval_address and not from_token.is_native:
                transactions.extend(ctx.services.build_approve_tx(from_token.address, approval_address, amount_in))

            swap_or_err = self._build_lifi_swap_transaction(
                ctx=ctx,
                intent=intent,
                quote=quote,
                from_token=from_token,
                to_token=to_token,
                amount_in=amount_in,
                is_cross_chain=is_cross_chain,
            )
            if isinstance(swap_or_err, CompilationResult):
                return swap_or_err
            transactions.append(swap_or_err)

            amount_out = quote.get_to_amount()
            amount_out_min = quote.get_to_amount_min()
            expected_output_human = _compute_lifi_expected_output_human(amount_out, to_token)

            total_gas = sum_transaction_gas(transactions)
            action_bundle = assemble_action_bundle(
                intent_type=IntentType.SWAP.value,
                transactions=transactions,
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "min_amount_out": str(amount_out_min),
                    "expected_output_human": str(expected_output_human) if expected_output_human else None,
                    "slippage": str(intent.max_slippage),
                    "protocol": "lifi",
                    "tool": quote.tool,
                    "from_chain_id": from_chain_id,
                    "to_chain_id": to_chain_id,
                    "is_cross_chain": is_cross_chain,
                    "deferred_swap": True,
                    "route_params": {
                        "from_chain_id": from_chain_id,
                        "to_chain_id": to_chain_id,
                        "from_token": lifi_from_address,
                        "to_token": lifi_to_address,
                        "from_amount": str(amount_in),
                        "from_address": ctx.wallet_address,
                        "to_address": ctx.services.resolve_dest_wallet(dest_chain)
                        if is_cross_chain
                        else ctx.wallet_address,
                        "slippage": slippage,
                    },
                },
            )

            amount_in_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            amount_out_fmt = format_token_amount(amount_out, to_token.symbol, to_token.decimals)
            min_out_fmt = format_token_amount(amount_out_min, to_token.symbol, to_token.decimals)
            slippage_fmt = format_percentage(intent.max_slippage)
            chain_info = f"{source_chain}->{dest_chain}" if is_cross_chain else source_chain
            logger.info(
                "Compiled SWAP (LiFi/%s): %s -> %s (min: %s) [%s]",
                quote.tool,
                amount_in_fmt,
                amount_out_fmt,
                min_out_fmt,
                chain_info,
            )
            logger.info("   Slippage: %s | Txs: %d | Gas: %s", slippage_fmt, len(transactions), f"{total_gas:,}")

            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=action_bundle,
                transactions=transactions,
                total_gas_estimate=total_gas,
                warnings=[],
            )

        except Exception as e:
            logger.exception("Failed to compile LiFi SWAP intent")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )

    @staticmethod
    def _validate_lifi_chains(
        ctx: SwapCompilerContext,
        intent: SwapIntent,
        chain_mapping: dict[str, int],
    ) -> tuple[str, str, int, int, bool] | CompilationResult:
        source_chain = intent.chain or ctx.chain
        dest_chain = intent.destination_chain or source_chain

        for chain in (source_chain, dest_chain):
            if chain.lower() not in chain_mapping:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"LiFi does not support chain: {chain}. Supported: {', '.join(chain_mapping.keys())}",
                    intent_id=intent.intent_id,
                )

        from_chain_id = chain_mapping[source_chain.lower()]
        to_chain_id = chain_mapping[dest_chain.lower()]
        is_cross_chain = from_chain_id != to_chain_id
        return source_chain, dest_chain, from_chain_id, to_chain_id, is_cross_chain

    @staticmethod
    def _resolve_lifi_tokens_and_amount(
        ctx: SwapCompilerContext,
        intent: SwapIntent,
        source_chain: str,
        dest_chain: str,
    ) -> tuple[TokenInfo, TokenInfo, int] | CompilationResult:
        from_token = ctx.services.resolve_token(intent.from_token, chain=source_chain)
        if from_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token on {source_chain}: {intent.from_token}",
                intent_id=intent.intent_id,
            )
        to_token = ctx.services.resolve_token(intent.to_token, chain=dest_chain)
        if to_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token on {dest_chain}: {intent.to_token}",
                intent_id=intent.intent_id,
            )

        if intent.amount_usd is not None:
            amount_in = ctx.services.usd_to_token_amount(intent.amount_usd, from_token)
        elif intent.amount is not None:
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation.",
                    intent_id=intent.intent_id,
                )
            amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
            amount_in = int(amount_decimal * Decimal(10**from_token.decimals))
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Either amount_usd or amount must be provided",
                intent_id=intent.intent_id,
            )
        return from_token, to_token, amount_in

    @staticmethod
    def _build_lifi_swap_transaction(
        *,
        ctx: SwapCompilerContext,
        intent: SwapIntent,
        quote: Any,
        from_token: TokenInfo,
        to_token: TokenInfo,
        amount_in: int,
        is_cross_chain: bool,
    ) -> TransactionData | CompilationResult:
        tx_request = quote.transaction_request
        if tx_request is None or not tx_request.to or not tx_request.data:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="LiFi quote missing transaction_request data",
                intent_id=intent.intent_id,
            )

        tx_type = "bridge_deferred" if is_cross_chain else "swap_deferred"
        description_action = "Bridge" if is_cross_chain else "Swap"

        value = parse_lifi_tx_value(tx_request.value)
        gas_estimate = choose_lifi_gas_estimate(
            total_gas_estimate=(quote.estimate.total_gas_estimate if quote.estimate else 0),
            gas_limit=tx_request.gas_limit,
        )

        return TransactionData(
            to=tx_request.to,
            value=value,
            data=tx_request.data,
            gas_estimate=gas_estimate,
            description=(
                f"{description_action} via LiFi ({quote.tool}): "
                f"{ctx.services.format_amount(amount_in, from_token.decimals)} {from_token.symbol} -> "
                f"{to_token.symbol}"
            ),
            tx_type=tx_type,
        )


def _compute_lifi_expected_output_human(amount_out: object | None, to_token: TokenInfo) -> Decimal | None:
    if not amount_out:
        return None
    try:
        amount_out_int = int(str(amount_out))
    except (TypeError, ValueError):
        return None
    if amount_out_int <= 0:
        return None
    return Decimal(str(amount_out_int)) / Decimal(10**to_token.decimals)


__all__ = ["LiFiCompiler"]
