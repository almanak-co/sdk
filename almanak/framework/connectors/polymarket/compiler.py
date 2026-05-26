"""Connector-owned compiler for Polymarket prediction intents."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.models.reproduction_bundle import ActionBundle

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import PredictionBuyIntent, PredictionRedeemIntent, PredictionSellIntent

logger = logging.getLogger(__name__)


class PolymarketCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compile Polymarket prediction intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"polymarket"})
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {IntentType.PREDICTION_BUY, IntentType.PREDICTION_SELL, IntentType.PREDICTION_REDEEM}
    )
    chains: ClassVar[frozenset[str]] = frozenset({"polygon"})

    def compile(self, ctx: BaseCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        if ctx.chain.lower() != "polygon":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Prediction market intents are only supported on Polygon, not {ctx.chain}",
                intent_id=getattr(intent, "intent_id", ""),
            )

        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.PREDICTION_BUY:
            return self._compile_buy(ctx, intent)
        if intent_type == IntentType.PREDICTION_SELL:
            return self._compile_sell(ctx, intent)
        if intent_type == IntentType.PREDICTION_REDEEM:
            return self._compile_redeem(ctx, intent)
        return self._unsupported(intent)

    def _compile_buy(self, ctx: BaseCompilerContext, intent: PredictionBuyIntent) -> CompilationResult:
        adapter = self._get_adapter(ctx, intent.intent_id, "PredictionBuyIntent")
        if isinstance(adapter, CompilationResult):
            return adapter
        return _compile_offchain_order(
            "PREDICTION_BUY",
            intent.intent_id,
            lambda: adapter.compile_intent(intent),
            "Compiled PREDICTION_BUY: market=%s, outcome=%s, amount_usd=%s, shares=%s",
            intent.market_id,
            intent.outcome,
            intent.amount_usd,
            intent.shares,
        )

    def _compile_sell(self, ctx: BaseCompilerContext, intent: PredictionSellIntent) -> CompilationResult:
        adapter = self._get_adapter(ctx, intent.intent_id, "PredictionSellIntent")
        if isinstance(adapter, CompilationResult):
            return adapter
        return _compile_offchain_order(
            "PREDICTION_SELL",
            intent.intent_id,
            lambda: adapter.compile_intent(intent),
            "Compiled PREDICTION_SELL: market=%s, outcome=%s, shares=%s",
            intent.market_id,
            intent.outcome,
            intent.shares,
        )

    def _compile_redeem(self, ctx: BaseCompilerContext, intent: PredictionRedeemIntent) -> CompilationResult:
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMarketNotResolvedError

        adapter = self._get_adapter(ctx, intent.intent_id, "PredictionRedeemIntent")
        if isinstance(adapter, CompilationResult):
            return adapter

        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        try:
            action_bundle = adapter.compile_intent(intent)
            if "error" in action_bundle.metadata:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=action_bundle.metadata["error"],
                    intent_id=intent.intent_id,
                )

            transactions = [
                TransactionData(
                    to=tx_dict.get("to", ""),
                    value=int(tx_dict.get("value", 0)),
                    data=tx_dict.get("data", ""),
                    gas_estimate=tx_dict.get("gas_estimate", 200_000),
                    description=tx_dict.get("description", "Redeem prediction market positions"),
                    tx_type=tx_dict.get("tx_type", "redeem"),
                )
                for tx_dict in action_bundle.transactions
            ]
            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)
            logger.info(
                "Compiled PREDICTION_REDEEM: market=%s, outcome=%s, txs=%s",
                intent.market_id,
                intent.outcome,
                len(transactions),
            )
        except PolymarketMarketNotResolvedError as exc:
            logger.warning("Cannot redeem - market not resolved: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)
        except Exception as exc:
            logger.exception("Failed to compile PREDICTION_REDEEM intent: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)
        return result

    def _get_adapter(self, ctx: BaseCompilerContext, intent_id: str, intent_name: str) -> Any | CompilationResult:
        adapter = ctx.cache.get("polymarket_adapter")
        if adapter is not None:
            return adapter
        if ctx.gateway_client is None or not ctx.gateway_client.is_connected:
            logger.warning("%s requires a gateway-backed Polymarket client on Polygon.", intent_name)
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "PolymarketAdapter not initialized. "
                    "Connect the compiler to the gateway to enable prediction intents."
                ),
                intent_id=intent_id,
            )

        try:
            from web3 import Web3

            from almanak.framework.connectors.polymarket.adapter import PolymarketAdapter
            from almanak.framework.connectors.polymarket.gateway_client import GatewayPolymarketClient
            from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

            # Include rpc_timeout in the cache key so different compiler timeouts
            # don't share a cached Web3 wired to the wrong timeout.
            web3_cache_key = ("polymarket_web3", ctx.chain, ctx.rpc_timeout)
            web3_instance = ctx.cache.get(web3_cache_key)
            if web3_instance is None:
                web3_instance = Web3(
                    GatewayWeb3Provider(
                        ctx.gateway_client,
                        chain=ctx.chain,
                        request_timeout=ctx.rpc_timeout,
                    )
                )
                ctx.cache[web3_cache_key] = web3_instance
            adapter = PolymarketAdapter(
                client=GatewayPolymarketClient(ctx.gateway_client),
                wallet_address=ctx.wallet_address,
                web3=web3_instance,
            )
            ctx.cache["polymarket_adapter"] = adapter
            logger.info("PolymarketAdapter initialized for wallet=%s...", ctx.wallet_address[:10])
            return adapter
        except Exception as exc:
            logger.warning(
                "Failed to initialize PolymarketAdapter: %s. Prediction market intents will not be available.", exc
            )
            return CompilationResult(status=CompilationStatus.FAILED, error=str(exc), intent_id=intent_id)


def _compile_offchain_order(
    intent_type: str,
    intent_id: str,
    build: Callable[[], ActionBundle],
    log_message: str,
    *log_args: Any,
) -> CompilationResult:
    result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent_id)
    try:
        action_bundle = build()
        if "error" in action_bundle.metadata:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=action_bundle.metadata["error"],
                intent_id=intent_id,
            )
        result.action_bundle = action_bundle
        result.transactions = []
        result.total_gas_estimate = 0
        logger.info(log_message, *log_args)
    except Exception as exc:
        logger.exception("Failed to compile %s intent: %s", intent_type, exc)
        result.status = CompilationStatus.FAILED
        result.error = str(exc)
    return result


__all__ = ["PolymarketCompiler"]
