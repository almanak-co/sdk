"""Connector-owned compiler for bridge intents."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import BaseBridgeCompiler, BaseCompilerContext
from almanak.framework.intents.compiler_constants import get_gas_estimate
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.models.reproduction_bundle import ActionBundle

if TYPE_CHECKING:
    from almanak.framework.intents.bridge import BridgeIntent
    from almanak.framework.intents.bridge_selector import BridgeSelector

logger = logging.getLogger(__name__)


class BridgeCompiler(BaseBridgeCompiler):
    """Compile BRIDGE intents through the registered bridge adapters."""

    protocols: ClassVar[frozenset[str]] = frozenset({"across", "stargate"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.BRIDGE})
    chains: ClassVar[frozenset[str]] = frozenset(
        {
            "arbitrum",
            "avalanche",
            "base",
            "bnb",
            "ethereum",
            "optimism",
            "polygon",
        }
    )

    def compile_bridge(self, ctx: BaseCompilerContext, intent: BridgeIntent) -> CompilationResult:  # noqa: C901
        """Compile a BRIDGE intent into an ActionBundle."""
        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )

        try:
            from_chain = intent.from_chain.lower()
            to_chain = intent.to_chain.lower()
            token_symbol = intent.token

            token_info = ctx.services.resolve_token(token_symbol, chain=from_chain)
            if token_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown token for bridge on {from_chain}: {token_symbol}",
                    intent_id=intent.intent_id,
                )

            amount_decimal: Decimal
            if intent.amount == "all":
                amount_decimal_result = self._resolve_all_amount(ctx, intent, from_chain, token_symbol, token_info)
                if isinstance(amount_decimal_result, CompilationResult):
                    return amount_decimal_result
                amount_decimal = amount_decimal_result
            elif isinstance(intent.amount, Decimal):
                amount_decimal = intent.amount
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Bridge amount must be Decimal or 'all', got: {type(intent.amount).__name__}",
                    intent_id=intent.intent_id,
                )

            selector = self._build_selector(ctx)
            selection = self._select_bridge(selector, intent, token_symbol, amount_decimal, from_chain, to_chain)
            if not selection.is_success or selection.bridge is None or selection.quote is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No bridge available for {token_symbol} from {from_chain} to {to_chain}",
                    intent_id=intent.intent_id,
                )

            quote = selection.quote
            bridge = selection.bridge
            dest_wallet = getattr(intent, "destination_address", None) or ctx.services.resolve_dest_wallet(to_chain)
            bridge_tx = bridge.build_deposit_tx(quote=quote, recipient=dest_wallet)

            amount_in_wei: int | None = None
            if quote.route_data and "amount_wei" in quote.route_data:
                try:
                    amount_in_wei = int(quote.route_data["amount_wei"])
                except (ValueError, TypeError):
                    amount_in_wei = None
            if amount_in_wei is None:
                amount_in_wei = int(amount_decimal * Decimal(10**token_info.decimals))

            transactions: list[TransactionData] = []
            if not token_info.is_native:
                transactions.extend(
                    ctx.services.build_approve_tx(
                        token_address=token_info.address,
                        spender=bridge_tx["to"],
                        amount=amount_in_wei,
                    )
                )

            bridge_transaction = TransactionData(
                to=bridge_tx["to"],
                value=int(bridge_tx.get("value", 0)),
                data=bridge_tx["data"],
                gas_estimate=int(bridge_tx.get("gas_estimate", get_gas_estimate(from_chain, "bridge_deposit"))),
                description=f"Bridge {amount_decimal} {token_symbol} from {from_chain} to {to_chain} via {bridge.name}",
                tx_type="bridge_deposit",
            )
            transactions.append(bridge_transaction)

            metadata: dict[str, Any] = {
                "from_chain": from_chain,
                "to_chain": to_chain,
                "token": token_symbol,
                "amount": str(amount_decimal),
                "bridge": bridge.name,
                "estimated_time": int(quote.estimated_time_seconds),
                "fee": str(quote.fee_amount),
                "is_cross_chain": from_chain != to_chain,
                "route": {"from_chain": quote.from_chain, "to_chain": quote.to_chain},
                "quote_id": quote.quote_id,
            }

            action_bundle = ActionBundle(
                intent_type=IntentType.BRIDGE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata=metadata,
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                "Compiled BRIDGE intent: %s %s %s->%s via %s, %s txs",
                amount_decimal,
                token_symbol,
                from_chain,
                to_chain,
                bridge.name,
                len(transactions),
            )
        except Exception as e:
            logger.exception("Failed to compile BRIDGE intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)
        return result

    def _resolve_all_amount(
        self,
        ctx: BaseCompilerContext,
        intent: BridgeIntent,
        from_chain: str,
        token_symbol: str,
        token_info: Any,
    ) -> Decimal | CompilationResult:
        if token_info.is_native:
            balance_wei = ctx.services.query_native_balance_for_chain(ctx.wallet_address, from_chain)
        else:
            balance_wei = ctx.services.query_erc20_balance_for_chain(
                token_info.address,
                ctx.wallet_address,
                from_chain,
            )
        if balance_wei is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Failed to query {token_symbol} balance on {from_chain} - RPC unavailable",
                intent_id=intent.intent_id,
            )
        if balance_wei <= 0:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"No {token_symbol} balance to bridge on {from_chain}",
                intent_id=intent.intent_id,
            )
        if token_info.is_native:
            gas_reserve_wei = int(Decimal("0.001") * Decimal(10**token_info.decimals))
            balance_wei = max(balance_wei - gas_reserve_wei, 0)
            if balance_wei <= 0:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Native balance too low to bridge {token_symbol} on {from_chain} after reserving gas",
                    intent_id=intent.intent_id,
                )
        return Decimal(balance_wei) / Decimal(10**token_info.decimals)

    def _build_selector(self, ctx: BaseCompilerContext) -> BridgeSelector:
        from almanak.connectors.across.adapter import AcrossBridgeAdapter
        from almanak.connectors.stargate.adapter import StargateBridgeAdapter
        from almanak.framework.intents.bridge_selector import BridgeSelector

        bridges = [
            AcrossBridgeAdapter(token_resolver=ctx.token_resolver),
            StargateBridgeAdapter(token_resolver=ctx.token_resolver),
        ]
        return BridgeSelector(bridges=bridges)

    def _select_bridge(
        self,
        selector: BridgeSelector,
        intent: BridgeIntent,
        token_symbol: str,
        amount_decimal: Decimal,
        from_chain: str,
        to_chain: str,
    ) -> Any:
        preferred = getattr(intent, "preferred_bridge", None)
        excluded = None
        if preferred:
            excluded = [bridge.name.lower() for bridge in selector.bridges if bridge.name.lower() != preferred.lower()]

        if excluded:
            return selector.select_bridge_with_fallback(
                token=token_symbol,
                amount=amount_decimal,
                from_chain=from_chain,
                to_chain=to_chain,
                max_slippage=intent.max_slippage,
                excluded_bridges=excluded,
            )
        return selector.select_bridge(
            token=token_symbol,
            amount=amount_decimal,
            from_chain=from_chain,
            to_chain=to_chain,
            max_slippage=intent.max_slippage,
        )


__all__ = ["BridgeCompiler"]
