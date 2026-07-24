"""Connector-owned compiler for LiFi aggregator swaps."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import (
    BaseCompilerContext,
    BaseProtocolCompiler,
    SwapCompilerContext,
)
from almanak.framework.intents._compiler_helpers import (
    assemble_action_bundle,
    choose_lifi_gas_estimate,
    parse_lifi_tx_value,
    sum_transaction_gas,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo, TransactionData
from almanak.framework.intents.vocabulary import IntentType, SwapIntent
from almanak.framework.utils.log_formatters import format_percentage, format_token_amount

if TYPE_CHECKING:
    from almanak.framework.intents.bridge import BridgeIntent

logger = logging.getLogger(__name__)


class LiFiCompiler(BaseProtocolCompiler[SwapCompilerContext]):
    """Compile same-chain and cross-chain LiFi SWAP intents, and BRIDGE intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"lifi"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.SWAP, IntentType.BRIDGE})
    chains: ClassVar[frozenset[str]] = frozenset(
        {"ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb"}
    )
    context_type: ClassVar[type[BaseCompilerContext]] = SwapCompilerContext

    def compile(self, ctx: SwapCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.SWAP:
            return self.compile_swap(ctx, intent)
        if intent_type == IntentType.BRIDGE:
            return self.compile_bridge(ctx, intent)
        return self._unsupported(intent)

    def compile_swap(self, ctx: SwapCompilerContext, intent: SwapIntent) -> CompilationResult:
        from almanak.connectors.lifi import CHAIN_MAPPING

        try:
            chain_check = self._validate_lifi_chains(ctx, intent, CHAIN_MAPPING)
            if isinstance(chain_check, CompilationResult):
                return chain_check
            source_chain, dest_chain, from_chain_id, to_chain_id, is_cross_chain = chain_check

            tokens_check = self._resolve_lifi_tokens_and_amount(ctx, intent, source_chain, dest_chain)
            if isinstance(tokens_check, CompilationResult):
                return tokens_check
            from_token, to_token, amount_in = tokens_check

            logger.info(
                "Getting LiFi quote: %s@%s -> %s@%s, amount=%s",
                from_token.symbol,
                source_chain,
                to_token.symbol,
                dest_chain,
                amount_in,
            )
            dest_wallet = ctx.services.resolve_dest_wallet(dest_chain) if is_cross_chain else ctx.wallet_address
            route = self._fetch_lifi_route(
                ctx,
                intent=intent,
                from_token=from_token,
                to_token=to_token,
                amount_in=amount_in,
                from_chain_id=from_chain_id,
                to_chain_id=to_chain_id,
                is_cross_chain=is_cross_chain,
                dest_wallet=dest_wallet,
            )
            if isinstance(route, CompilationResult):
                return route
            transactions, quote, route_params = route

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
                    "route_params": route_params,
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

    def compile_bridge(self, ctx: SwapCompilerContext, intent: BridgeIntent) -> CompilationResult:
        """Compile a BRIDGE intent through LiFi's cross-chain quote path.

        ``BridgeIntent`` is the bridge-vocabulary twin of the cross-chain
        SwapIntent path: same token symbol on both chains, LiFi's route picker
        selects the underlying bridge tool. The emitted ActionBundle carries
        the ``BridgeCompiler`` metadata contract (``from_chain`` / ``to_chain``
        / ``token`` / ``amount`` / ``bridge``) so ResultEnricher's BRIDGE
        enrichment threads those hints into
        ``LiFiReceiptParser.extract_bridge_data``, plus the LiFi
        deferred-execution keys (``deferred_swap`` / ``route_params``) so the
        pre-execution route refresh applies exactly as it does for swaps.
        """
        from almanak.connectors.lifi import CHAIN_MAPPING

        try:
            source_chain = intent.from_chain.lower()
            dest_chain = intent.to_chain.lower()
            for chain in (source_chain, dest_chain):
                if chain not in CHAIN_MAPPING:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"LiFi does not support chain: {chain}. Supported: {', '.join(CHAIN_MAPPING.keys())}",
                        intent_id=intent.intent_id,
                    )
            from_chain_id = CHAIN_MAPPING[source_chain]
            to_chain_id = CHAIN_MAPPING[dest_chain]

            from_token = ctx.services.resolve_token(intent.token, chain=source_chain)
            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token on {source_chain}: {intent.token}",
                    intent_id=intent.intent_id,
                )
            to_token = ctx.services.resolve_token(intent.token, chain=dest_chain)
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token on {dest_chain}: {intent.token}",
                    intent_id=intent.intent_id,
                )

            # ``IntentCompiler.compile`` resolves amount="all" (wallet balance)
            # before dispatch; reaching here unresolved means a direct-compile
            # caller skipped that step.
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation.",
                    intent_id=intent.intent_id,
                )
            if not isinstance(intent.amount, Decimal):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Bridge amount must be Decimal or 'all', got: {type(intent.amount).__name__}",
                    intent_id=intent.intent_id,
                )
            amount_decimal = intent.amount
            amount_in = int(amount_decimal * Decimal(10**from_token.decimals))

            logger.info(
                "Getting LiFi bridge quote: %s %s@%s -> %s, amount=%s",
                amount_decimal,
                intent.token,
                source_chain,
                dest_chain,
                amount_in,
            )
            dest_wallet = intent.destination_address or ctx.services.resolve_dest_wallet(dest_chain)
            route = self._fetch_lifi_route(
                ctx,
                intent=intent,
                from_token=from_token,
                to_token=to_token,
                amount_in=amount_in,
                from_chain_id=from_chain_id,
                to_chain_id=to_chain_id,
                is_cross_chain=True,
                dest_wallet=dest_wallet,
            )
            if isinstance(route, CompilationResult):
                return route
            transactions, quote, route_params = route

            amount_out = quote.get_to_amount()
            amount_out_min = quote.get_to_amount_min()
            expected_output_human = _compute_lifi_expected_output_human(amount_out, to_token)

            total_gas = sum_transaction_gas(transactions)
            action_bundle = assemble_action_bundle(
                intent_type=IntentType.BRIDGE.value,
                transactions=transactions,
                metadata={
                    # BridgeCompiler metadata contract — ResultEnricher's
                    # bridge_data hints and the runner's bridge lanes key on
                    # these (see _build_extract_kwargs in result_enricher.py).
                    "from_chain": source_chain,
                    "to_chain": dest_chain,
                    "token": intent.token,
                    "amount": str(amount_decimal),
                    "bridge": "lifi",
                    "estimated_time": int(quote.estimate.execution_duration) if quote.estimate else 0,
                    "is_cross_chain": True,
                    "route": {"from_chain": source_chain, "to_chain": dest_chain},
                    # Expected destination-side output in human units —
                    # threaded to extract_bridge_data as expected_amount_out.
                    "output_amount": str(expected_output_human) if expected_output_human else None,
                    # LiFi deferred-execution keys (same as the cross-chain
                    # SWAP path): fresh calldata is fetched at execute time.
                    "protocol": "lifi",
                    "tool": quote.tool,
                    "from_chain_id": from_chain_id,
                    "to_chain_id": to_chain_id,
                    "amount_in": str(amount_in),
                    "amount_out": str(amount_out),
                    "min_amount_out": str(amount_out_min),
                    "slippage": str(intent.max_slippage),
                    "deferred_swap": True,
                    "route_params": route_params,
                },
            )

            amount_fmt = format_token_amount(amount_in, from_token.symbol, from_token.decimals)
            min_out_fmt = format_token_amount(amount_out_min, to_token.symbol, to_token.decimals)
            logger.info(
                "Compiled BRIDGE (LiFi/%s): %s %s->%s (min out: %s) | Txs: %d | Gas: %s",
                quote.tool,
                amount_fmt,
                source_chain,
                dest_chain,
                min_out_fmt,
                len(transactions),
                f"{total_gas:,}",
            )

            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=action_bundle,
                transactions=transactions,
                total_gas_estimate=total_gas,
                warnings=[],
            )

        except Exception as e:
            logger.exception("Failed to compile LiFi BRIDGE intent")
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=str(e),
            )

    def _fetch_lifi_route(
        self,
        ctx: SwapCompilerContext,
        *,
        intent: SwapIntent | BridgeIntent,
        from_token: TokenInfo,
        to_token: TokenInfo,
        amount_in: int,
        from_chain_id: int,
        to_chain_id: int,
        is_cross_chain: bool,
        dest_wallet: str,
    ) -> tuple[list[TransactionData], Any, dict[str, Any]] | CompilationResult:
        """Shared LiFi quote -> approve -> deferred-tx -> route_params core.

        Both ``compile_swap`` and ``compile_bridge`` funnel through here once
        they have resolved their intent-specific inputs (chains, tokens,
        amount, destination wallet); only the ActionBundle metadata contract
        differs per intent type. Returns ``(transactions, quote,
        route_params)`` on success or a FAILED ``CompilationResult``.

        ``dest_wallet`` is threaded into BOTH the initial quote's
        ``to_address`` and ``route_params["to_address"]`` so the compiled
        calldata and the execute-time deferred refresh always agree on the
        recipient.
        """
        from almanak.connectors.lifi import LiFiAdapter, LiFiConfig
        from almanak.connectors.lifi.client import NATIVE_TOKEN_ADDRESS as LIFI_NATIVE_ADDRESS

        lifi_from_address = LIFI_NATIVE_ADDRESS if from_token.is_native else from_token.address
        lifi_to_address = LIFI_NATIVE_ADDRESS if to_token.is_native else to_token.address

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
            to_address=dest_wallet,
            slippage=slippage,
        )

        transactions: list[TransactionData] = []
        approval_address = quote.estimate.approval_address if quote.estimate else ""
        if approval_address and not from_token.is_native:
            transactions.extend(ctx.services.build_approve_tx(from_token.address, approval_address, amount_in))

        tx_or_err = self._build_lifi_swap_transaction(
            ctx=ctx,
            intent=intent,
            quote=quote,
            from_token=from_token,
            to_token=to_token,
            amount_in=amount_in,
            is_cross_chain=is_cross_chain,
        )
        if isinstance(tx_or_err, CompilationResult):
            return tx_or_err
        transactions.append(tx_or_err)

        route_params = {
            "from_chain_id": from_chain_id,
            "to_chain_id": to_chain_id,
            "from_token": lifi_from_address,
            "to_token": lifi_to_address,
            "from_amount": str(amount_in),
            "from_address": ctx.wallet_address,
            "to_address": dest_wallet,
            "slippage": slippage,
        }
        return transactions, quote, route_params

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
        intent: SwapIntent | BridgeIntent,
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
