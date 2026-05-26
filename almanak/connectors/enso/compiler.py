"""Connector-owned compiler for Enso aggregator swaps."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import (
    BaseCompilerContext,
    BaseProtocolCompiler,
    SwapCompilerContext,
)
from almanak.framework.intents._compiler_helpers import assemble_action_bundle, sum_transaction_gas
from almanak.framework.intents.compiler_constants import MAX_UINT256
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo, TransactionData
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from almanak.framework.utils.log_formatters import (
    _emojis_enabled,
    format_percentage,
    format_slippage_bps,
    format_token_amount,
)

logger = logging.getLogger(__name__)


class EnsoCompiler(BaseProtocolCompiler[SwapCompilerContext]):
    """Compile same-chain and cross-chain Enso SWAP intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"enso"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.SWAP})
    chains: ClassVar[frozenset[str]] = frozenset(
        {"ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb", "berachain"}
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
        if intent.is_cross_chain:
            return self._compile_cross_chain_swap(ctx, intent)
        return self._compile_same_chain_swap(ctx, intent)

    def _compile_same_chain_swap(self, ctx: SwapCompilerContext, intent: SwapIntent) -> CompilationResult:
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            from_token = ctx.services.resolve_token(intent.from_token)
            to_token = ctx.services.resolve_token(intent.to_token)
            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.from_token}",
                    intent_id=intent.intent_id,
                )
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            amount_check = _resolve_swap_amount(ctx, intent, from_token)
            if isinstance(amount_check, CompilationResult):
                return amount_check
            amount_in = amount_check

            logger.info("Getting Enso route: %s -> %s, amount=%s", from_token.symbol, to_token.symbol, amount_in)
            slippage_bps = int(intent.max_slippage * 10000)
            route_data = self._get_enso_route(
                ctx,
                from_token.address,
                to_token.address,
                str(amount_in),
                slippage_bps,
            )

            router_address = route_data["to"]
            if not from_token.is_native:
                transactions.extend(ctx.services.build_approve_tx(from_token.address, router_address, MAX_UINT256))

            value = int(route_data["value"]) if route_data["value"] else 0
            transactions.append(
                TransactionData(
                    to=route_data["to"],
                    value=value,
                    data=route_data["data"],
                    gas_estimate=route_data["gas"] if route_data["gas"] else 200000,
                    description=(
                        f"Swap via Enso: {ctx.services.format_amount(amount_in, from_token.decimals)} "
                        f"{from_token.symbol} -> {to_token.symbol}"
                    ),
                    tx_type="swap_deferred",
                )
            )

            total_gas = sum_transaction_gas(transactions)
            amount_out = int(route_data["amount_out"]) if route_data["amount_out"] else 0
            min_output = int(Decimal(str(amount_out)) * (Decimal("1") - intent.max_slippage))
            expected_output_human = Decimal(str(amount_out)) / Decimal(10**to_token.decimals) if amount_out else None

            result.action_bundle = assemble_action_bundle(
                intent_type=IntentType.SWAP.value,
                transactions=transactions,
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "min_amount_out": str(min_output),
                    "expected_output_human": str(expected_output_human) if expected_output_human else None,
                    "slippage": str(intent.max_slippage),
                    "protocol": "enso",
                    "chain": ctx.chain,
                    "router": router_address,
                    "price_impact_bps": route_data.get("price_impact", 0),
                    "deferred_swap": True,
                    "route_params": {
                        "token_in": from_token.address,
                        "token_out": to_token.address,
                        "amount_in": str(amount_in),
                        "slippage_bps": slippage_bps,
                    },
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            amount_in_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            amount_out_fmt = format_token_amount(amount_out, to_token.symbol, to_token.decimals)
            min_out_fmt = format_token_amount(min_output, to_token.symbol, to_token.decimals)
            slippage_fmt = format_percentage(intent.max_slippage)
            price_impact_val = route_data.get("price_impact")
            price_impact_fmt = format_slippage_bps(price_impact_val) if price_impact_val is not None else "N/A"

            ok = "\u2705" if _emojis_enabled() else "[OK]"
            logger.info("%s Compiled SWAP (Enso): %s -> %s (min: %s)", ok, amount_in_fmt, amount_out_fmt, min_out_fmt)
            logger.info(
                "   Slippage: %s | Impact: %s | Txs: %d | Gas: %s",
                slippage_fmt,
                price_impact_fmt,
                len(transactions),
                f"{total_gas:,}",
            )
        except Exception as e:
            logger.exception("Failed to compile Enso SWAP intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _compile_cross_chain_swap(self, ctx: SwapCompilerContext, intent: SwapIntent) -> CompilationResult:
        from almanak.connectors.enso import CHAIN_MAPPING

        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            source_chain = intent.chain or ctx.chain
            dest_chain = intent.destination_chain
            if not dest_chain:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Cross-chain swap requires destination_chain to be set",
                    intent_id=intent.intent_id,
                )

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

            amount_check = _resolve_swap_amount(
                ctx,
                intent,
                from_token,
                all_error="amount='all' must be resolved before compilation for cross-chain swaps.",
            )
            if isinstance(amount_check, CompilationResult):
                return amount_check
            amount_in = amount_check

            logger.info(
                "Getting cross-chain route: %s %s -> %s %s, amount=%s",
                source_chain,
                from_token.symbol,
                dest_chain,
                to_token.symbol,
                amount_in,
            )
            slippage_bps = int(intent.max_slippage * 10000)
            dest_chain_id = CHAIN_MAPPING.get(dest_chain.lower())
            if dest_chain_id is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unsupported destination chain: {dest_chain}",
                    intent_id=intent.intent_id,
                )

            dest_wallet = ctx.services.resolve_dest_wallet(dest_chain)
            route_data = self._get_enso_route(
                ctx,
                from_token.address,
                to_token.address,
                str(amount_in),
                slippage_bps,
                chain=source_chain,
                destination_chain_id=dest_chain_id,
                receiver=dest_wallet,
                refund_receiver=dest_wallet,
            )

            router_address = route_data["to"]
            if not from_token.is_native:
                transactions.extend(ctx.services.build_approve_tx(from_token.address, router_address, amount_in))

            value = int(route_data["value"]) if route_data["value"] else 0
            transactions.append(
                TransactionData(
                    to=route_data["to"],
                    value=value,
                    data=route_data["data"],
                    gas_estimate=route_data["gas"] if route_data["gas"] else 300000,
                    description=(
                        f"Cross-chain swap via Enso: {ctx.services.format_amount(amount_in, from_token.decimals)} "
                        f"{from_token.symbol} ({source_chain}) -> {to_token.symbol} ({dest_chain})"
                    ),
                    tx_type="cross_chain_swap",
                )
            )

            total_gas = sum_transaction_gas(transactions)
            amount_out = int(route_data["amount_out"]) if route_data["amount_out"] else 0
            bridge_fee = route_data.get("bridge_fee")
            estimated_time = route_data.get("estimated_time")
            expected_output_human = Decimal(str(amount_out)) / Decimal(10**to_token.decimals) if amount_out else None

            result.action_bundle = assemble_action_bundle(
                intent_type=IntentType.SWAP.value,
                transactions=transactions,
                metadata={
                    "from_token": from_token.to_dict(),
                    "to_token": to_token.to_dict(),
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "expected_output_human": str(expected_output_human) if expected_output_human else None,
                    "slippage": str(intent.max_slippage),
                    "protocol": "enso",
                    "router": router_address,
                    "source_chain": source_chain,
                    "destination_chain": dest_chain,
                    "is_cross_chain": True,
                    "bridge_fee": bridge_fee,
                    "estimated_time": estimated_time,
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                "Compiled cross-chain SWAP intent: %s (%s) -> %s (%s), %d txs, bridge_fee=%s, est_time=%ss",
                from_token.symbol,
                source_chain,
                to_token.symbol,
                dest_chain,
                len(transactions),
                bridge_fee,
                estimated_time,
            )
        except Exception as e:
            logger.exception("Failed to compile cross-chain SWAP intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def _get_enso_route(
        self,
        ctx: SwapCompilerContext,
        token_in: str,
        token_out: str,
        amount_in: str,
        slippage_bps: int,
        *,
        chain: str | None = None,
        destination_chain_id: int | None = None,
        receiver: str | None = None,
        refund_receiver: str | None = None,
    ) -> dict[str, Any]:
        if ctx.gateway_client is not None:
            if not ctx.gateway_client.is_connected:
                raise RuntimeError(
                    "Gateway client is configured but not connected; cannot fetch Enso route. "
                    "Ensure the gateway is running before compiling Enso intents."
                )
            return self._get_enso_route_via_gateway(
                ctx,
                token_in,
                token_out,
                amount_in,
                slippage_bps,
                chain=chain,
                destination_chain_id=destination_chain_id,
                receiver=receiver,
                refund_receiver=refund_receiver,
            )

        from almanak.framework.deployment import is_hosted

        if is_hosted():
            raise RuntimeError(
                "Enso route request failed: no gateway client configured. "
                "In deployed mode, all Enso API calls must go through the gateway."
            )

        return self._get_enso_route_direct(
            ctx,
            token_in,
            token_out,
            int(amount_in),
            slippage_bps,
            chain=chain,
            destination_chain_id=destination_chain_id,
            receiver=receiver,
            refund_receiver=refund_receiver,
        )

    def _get_enso_route_via_gateway(
        self,
        ctx: SwapCompilerContext,
        token_in: str,
        token_out: str,
        amount_in: str,
        slippage_bps: int,
        *,
        chain: str | None = None,
        destination_chain_id: int | None = None,
        receiver: str | None = None,
        refund_receiver: str | None = None,
    ) -> dict[str, Any]:
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.EnsoRouteRequest(
            chain=chain or ctx.chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            from_address=ctx.wallet_address,
            slippage_bps=slippage_bps,
            routing_strategy="router",
            destination_chain_id=destination_chain_id or 0,
            receiver=receiver or "",
            refund_receiver=refund_receiver or "",
        )
        response = ctx.gateway_client.enso.GetRoute(request, timeout=30.0)

        if not response.success:
            from almanak.connectors.enso import check_known_router_revert

            check_known_router_revert(
                response.error,
                chain=chain or ctx.chain,
                route_summary=f"{token_in} -> {token_out}",
            )
            raise RuntimeError(f"Gateway Enso GetRoute failed: {response.error}")

        gas_str = response.gas or response.gas_estimate
        result = {
            "to": response.to,
            "data": response.data,
            "value": response.value,
            "gas": int(gas_str) if gas_str and gas_str != "0" else None,
            "amount_out": response.amount_out,
            "price_impact": response.price_impact,
        }

        if response.is_cross_chain:
            result["bridge_fee"] = response.bridge_fee
            result["estimated_time"] = response.estimated_time
            result["is_cross_chain"] = True

        return result

    def _get_enso_route_direct(
        self,
        ctx: SwapCompilerContext,
        token_in: str,
        token_out: str,
        amount_in: int,
        slippage_bps: int,
        *,
        chain: str | None = None,
        destination_chain_id: int | None = None,
        receiver: str | None = None,
        refund_receiver: str | None = None,
    ) -> dict[str, Any]:
        from almanak.connectors.enso import EnsoClient, EnsoConfig

        config = EnsoConfig(
            chain=chain or ctx.chain,
            wallet_address=ctx.wallet_address,
        )
        client = EnsoClient(config)
        route = client.get_route(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage_bps=slippage_bps,
            destination_chain_id=destination_chain_id,
            receiver=receiver,
            refund_receiver=refund_receiver,
        )

        result: dict[str, Any] = {
            "to": route.tx.to,
            "data": route.tx.data,
            "value": str(route.tx.value) if route.tx.value else "0",
            "gas": int(route.gas) if route.gas else None,
            "amount_out": str(route.get_amount_out_wei()),
            "price_impact": route.price_impact,
        }

        if destination_chain_id:
            result["bridge_fee"] = getattr(route, "bridge_fee", None)
            result["estimated_time"] = getattr(route, "estimated_time", None)
            result["is_cross_chain"] = True

        return result


def _resolve_swap_amount(
    ctx: SwapCompilerContext,
    intent: SwapIntent,
    from_token: TokenInfo,
    *,
    all_error: str = "amount='all' must be resolved before compilation.",
) -> int | CompilationResult:
    if intent.amount_usd is not None:
        return ctx.services.usd_to_token_amount(intent.amount_usd, from_token)
    if intent.amount is not None:
        if intent.amount == "all":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=all_error,
                intent_id=intent.intent_id,
            )
        amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
        return int(amount_decimal * Decimal(10**from_token.decimals))
    return CompilationResult(
        status=CompilationStatus.FAILED,
        error="Either amount_usd or amount must be provided",
        intent_id=intent.intent_id,
    )


__all__ = ["EnsoCompiler"]
