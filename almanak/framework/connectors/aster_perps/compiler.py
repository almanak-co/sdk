"""Connector-owned compiler for Aster and PancakeSwap perpetual intents."""

from __future__ import annotations

import logging
import warnings
from decimal import Decimal
from typing import ClassVar

from almanak.core.contracts import ASTER_PERPS_TOKENS
from almanak.framework.connectors.base.compiler import BasePerpCompiler, PerpCompilerContext
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import IntentType, PerpCloseIntent, PerpOpenIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

from .adapter import AsterPerpsAdapter, AsterPerpsConfig
from .sdk import ASTER_BROKER_RAW, NATIVE_BNB_ADDRESS, PCS_BROKER_ID

logger = logging.getLogger(__name__)

_PCS_PERPS_KEY_WARNED = False


def _warn_pcs_perps_protocol_key_once() -> None:
    global _PCS_PERPS_KEY_WARNED
    if _PCS_PERPS_KEY_WARNED:
        return
    _PCS_PERPS_KEY_WARNED = True
    warnings.warn(
        "protocol='pancakeswap_perps' is deprecated; use protocol='aster_perps' "
        "unless you intentionally need PancakeSwap broker attribution.",
        DeprecationWarning,
        stacklevel=4,
    )


class AsterPerpsCompiler(BasePerpCompiler):
    """Compile raw Aster and PancakeSwap broker-shim perp intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"aster_perps", "pancakeswap_perps"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.PERP_OPEN, IntentType.PERP_CLOSE})
    chains: ClassVar[frozenset[str]] = frozenset({"bsc"})

    def compile_perp_open(self, ctx: PerpCompilerContext, intent: PerpOpenIntent) -> CompilationResult:  # noqa: C901
        broker_id = self._broker_id(ctx.protocol)
        if broker_id is None:
            return self._unsupported_protocol(ctx, intent.intent_id, "PERP_OPEN")
        if ctx.chain != "bsc":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Aster Perps Phase 1 requires chain='bsc', got '{ctx.chain}'",
            )
        if intent.collateral_amount == "all":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    "collateral_amount='all' must be resolved before compilation. "
                    "Use Intent.set_resolved_amount() to resolve chained amounts."
                ),
            )

        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings_list: list[str] = []

        try:
            adapter = AsterPerpsAdapter(
                AsterPerpsConfig(broker_id=broker_id, chain=ctx.chain, wallet_address=ctx.wallet_address)
            )
            mark_price = self._resolve_mark_price(ctx, intent.market)
            normalized_collateral, resolver_key = self._normalize_collateral(intent.collateral_token)

            validation_error = self._validate_margin_token(ctx, intent.collateral_token, resolver_key, intent.intent_id)
            if validation_error is not None:
                return validation_error

            decimals_or_error = self._resolve_collateral_decimals(
                ctx, intent.collateral_token, resolver_key, intent.intent_id
            )
            if isinstance(decimals_or_error, CompilationResult):
                return decimals_or_error
            collateral_decimals = decimals_or_error

            order = adapter.build_open(
                market=intent.market,
                collateral_token=normalized_collateral,
                collateral_amount=intent.collateral_amount,  # type: ignore[arg-type]
                collateral_decimals=collateral_decimals,
                size_usd=intent.size_usd,
                mark_price=mark_price,
                is_long=intent.is_long,
                max_slippage=intent.max_slippage,
            )
            if not order.success or order.tx is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=order.error or "Adapter failed to build open transaction",
                )

            if not order.native and order.margin_token_address:
                transactions.extend(
                    ctx.services.build_approve_tx(
                        token_address=order.margin_token_address,
                        spender=order.tx.to,
                        amount=order.amount_in_wei,
                    )
                )

            transactions.append(
                TransactionData(
                    to=order.tx.to,
                    value=order.tx.value,
                    data="0x" + order.tx.data.hex(),
                    gas_estimate=order.tx.gas_estimate,
                    description=order.tx.description,
                    tx_type="perp_open",
                )
            )

            result.action_bundle = ActionBundle(
                intent_type=IntentType.PERP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "market": intent.market,
                    "pair_base": order.pair_base,
                    "collateral_token": intent.collateral_token,
                    "collateral_amount": str(intent.collateral_amount),
                    "size_usd": str(intent.size_usd),
                    "is_long": intent.is_long,
                    "max_slippage": str(intent.max_slippage),
                    "qty_1e10": order.qty,
                    "limit_price_1e8": order.limit_price,
                    "native_margin": order.native,
                    "chain": ctx.chain,
                    "broker_id": adapter.config.broker_id,
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)
            result.warnings = warnings_list
        except Exception as exc:
            logger.exception("Failed to compile Aster Perps PERP_OPEN: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)

        return result

    def compile_perp_close(self, ctx: PerpCompilerContext, intent: PerpCloseIntent) -> CompilationResult:
        broker_id = self._broker_id(ctx.protocol)
        if broker_id is None:
            return self._unsupported_protocol(ctx, intent.intent_id, "PERP_CLOSE")
        if ctx.chain != "bsc":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=f"Aster Perps Phase 1 requires chain='bsc', got '{ctx.chain}'",
            )

        position_id = intent.position_id
        if not position_id:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    "Aster Perps PERP_CLOSE requires intent.position_id (the bytes32 "
                    "tradeHash returned from the open). Strategies must persist the tradeHash "
                    "from on_intent_executed(result.position_id) after the open."
                ),
            )

        pid_error = self._validate_trade_hash(position_id, intent.intent_id)
        if pid_error is not None:
            return pid_error

        if intent.size_usd is not None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent.intent_id,
                error=(
                    "Aster Perps does not support partial PERP_CLOSE via size_usd. "
                    "Omit size_usd to close the full position identified by position_id."
                ),
            )

        try:
            adapter = AsterPerpsAdapter(
                AsterPerpsConfig(broker_id=broker_id, chain=ctx.chain, wallet_address=ctx.wallet_address)
            )
            close_tx = adapter.build_close(trade_hash=position_id)
        except Exception as exc:
            logger.exception("Failed to build Aster Perps close transaction: %s", exc)
            return CompilationResult(status=CompilationStatus.FAILED, intent_id=intent.intent_id, error=str(exc))

        tx = TransactionData(
            to=close_tx.to,
            value=close_tx.value,
            data="0x" + close_tx.data.hex(),
            gas_estimate=close_tx.gas_estimate,
            description=close_tx.description,
            tx_type="perp_close",
        )
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        result.action_bundle = ActionBundle(
            intent_type=IntentType.PERP_CLOSE.value,
            transactions=[tx.to_dict()],
            metadata={
                "protocol": intent.protocol,
                "market": intent.market,
                "collateral_token": intent.collateral_token,
                "is_long": intent.is_long,
                "max_slippage": str(intent.max_slippage),
                "position_id": position_id,
                "chain": ctx.chain,
                "broker_id": broker_id,
            },
        )
        result.transactions = [tx]
        result.total_gas_estimate = tx.gas_estimate
        result.warnings = []
        return result

    def _broker_id(self, protocol: str) -> int | None:
        if protocol == "pancakeswap_perps":
            _warn_pcs_perps_protocol_key_once()
            return PCS_BROKER_ID
        if protocol == "aster_perps":
            return ASTER_BROKER_RAW
        return None

    def _unsupported_protocol(self, ctx: PerpCompilerContext, intent_id: str, primitive: str) -> CompilationResult:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent_id,
            error=(
                f"Protocol '{ctx.protocol}' is not supported for {primitive} on "
                f"{ctx.chain}. Supported: aster_perps, pancakeswap_perps (bsc)."
            ),
        )

    def _resolve_mark_price(self, ctx: PerpCompilerContext, market: str) -> Decimal:
        base_symbol = market.split("/")[0] if "/" in market else market
        bsc_perp_price_alias = {"BTC": "WBTC", "ETH": "WETH", "BNB": "WBNB"}
        try:
            return ctx.services.require_token_price(base_symbol)
        except ValueError:
            wrapped = bsc_perp_price_alias.get(base_symbol.upper())
            if not wrapped:
                raise
            return ctx.services.require_token_price(wrapped)

    def _normalize_collateral(self, raw_collateral: str) -> tuple[str, str]:
        if (
            isinstance(raw_collateral, str)
            and raw_collateral.startswith("0x")
            and raw_collateral.lower() == NATIVE_BNB_ADDRESS.lower()
        ):
            return "BNB", "BNB"
        if isinstance(raw_collateral, str) and raw_collateral.startswith("0x"):
            return raw_collateral, raw_collateral
        return raw_collateral, raw_collateral.upper()

    def _validate_margin_token(
        self, ctx: PerpCompilerContext, original: str, resolver_key: str, intent_id: str
    ) -> CompilationResult | None:
        supported_tokens = ASTER_PERPS_TOKENS.get(ctx.chain, {})
        allowed_symbols = {"BNB", "NATIVE"} | set(supported_tokens.keys())
        allowed_addresses = {addr.lower() for addr in supported_tokens.values()}
        if resolver_key.startswith("0x"):
            if resolver_key.lower() not in allowed_addresses:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent_id,
                    error=(
                        f"Collateral address '{original}' is not a supported "
                        f"Aster Perps margin token on {ctx.chain}. "
                        f"Allowed: BNB (native) + {sorted(supported_tokens.keys())}."
                    ),
                )
        elif resolver_key not in allowed_symbols:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent_id,
                error=(
                    f"Collateral symbol '{original}' is not a supported "
                    f"Aster Perps margin token on {ctx.chain}. "
                    f"Allowed: BNB (native) + {sorted(supported_tokens.keys())}."
                ),
            )
        return None

    def _resolve_collateral_decimals(
        self, ctx: PerpCompilerContext, original: str, resolver_key: str, intent_id: str
    ) -> int | CompilationResult:
        if resolver_key in ("BNB", "NATIVE", "WBNB"):
            return 18
        try:
            return ctx.token_resolver.get_decimals(ctx.chain, resolver_key)
        except Exception as exc:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent_id,
                error=f"Could not resolve decimals for collateral token '{original}' on {ctx.chain}: {exc}",
            )

    def _validate_trade_hash(self, position_id: str, intent_id: str) -> CompilationResult | None:
        pid_clean = position_id.lower()
        if not pid_clean.startswith("0x") or len(pid_clean) != 66:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent_id,
                error=(
                    f"Aster Perps requires a 0x-prefixed bytes32 tradeHash "
                    f"(66 chars total). Got: '{position_id}' (len={len(position_id)})."
                ),
            )
        try:
            int(pid_clean[2:], 16)
        except ValueError:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=intent_id,
                error=f"Aster Perps requires position_id to be a valid hex bytes32 tradeHash. Got: '{position_id}'.",
            )
        return None


__all__ = ["AsterPerpsCompiler"]
