"""Connector-owned compiler for GMX V2 perpetual intents."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import ClassVar

from almanak.framework.connectors.base.compiler import BasePerpCompiler, PerpCompilerContext
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.intent_errors import InvalidCollateralForMarketError
from almanak.framework.intents.vocabulary import IntentType, PerpCloseIntent, PerpOpenIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

from .adapter import GMX_V2_MARKETS, GMXv2Adapter, GMXv2Config
from .market_rules import validate_collateral
from .sdk import GMX_V2_TOKENS, GMXV2SDK, GMXV2OrderParams, PositionQueryError

logger = logging.getLogger(__name__)


class GMXV2Compiler(BasePerpCompiler):
    """Compile GMX V2 PERP_OPEN and PERP_CLOSE intents."""

    protocols: ClassVar[frozenset[str]] = frozenset({"gmx_v2"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.PERP_OPEN, IntentType.PERP_CLOSE})
    chains: ClassVar[frozenset[str]] = frozenset({"arbitrum", "avalanche"})

    def compile_perp_open(self, ctx: PerpCompilerContext, intent: PerpOpenIntent) -> CompilationResult:  # noqa: C901
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            if ctx.chain not in self.chains:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"GMX v2 not supported on chain: {ctx.chain}",
                    intent_id=intent.intent_id,
                )

            try:
                validate_collateral(chain=ctx.chain, market=intent.market, collateral_token=intent.collateral_token)
            except InvalidCollateralForMarketError as exc:
                return CompilationResult(status=CompilationStatus.FAILED, error=str(exc), intent_id=intent.intent_id)

            slippage_bps = int(intent.max_slippage * 10000)
            adapter = GMXv2Adapter(
                GMXv2Config(
                    chain=ctx.chain,
                    wallet_address=ctx.wallet_address,
                    default_slippage_bps=slippage_bps,
                )
            )

            acceptable_price = Decimal(10**30) if intent.is_long else Decimal("0")

            if intent.collateral_amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "collateral_amount='all' must be resolved before compilation. "
                        "Use Intent.set_resolved_amount() to resolve chained amounts."
                    ),
                    intent_id=intent.intent_id,
                )

            order_result = adapter.open_position(
                market=intent.market,
                collateral_token=intent.collateral_token,
                collateral_amount=intent.collateral_amount,  # type: ignore[arg-type]
                size_delta_usd=intent.size_usd,
                is_long=intent.is_long,
                acceptable_price=acceptable_price,
            )
            if not order_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=order_result.error or "Failed to create position order",
                    intent_id=intent.intent_id,
                )

            sdk_or_error = self._build_sdk(ctx, intent.intent_id)
            if isinstance(sdk_or_error, CompilationResult):
                return sdk_or_error
            sdk = sdk_or_error

            market_or_error = self._resolve_market(ctx, sdk, intent.market, intent.intent_id)
            if isinstance(market_or_error, CompilationResult):
                return market_or_error
            market_address = market_or_error

            collateral_or_error = self._resolve_collateral(ctx, intent.collateral_token, intent.intent_id)
            if isinstance(collateral_or_error, CompilationResult):
                return collateral_or_error
            collateral_address = collateral_or_error

            collateral_token_upper = intent.collateral_token.upper()
            collateral_decimals = self._resolve_collateral_decimals(
                ctx, intent.collateral_token, collateral_token_upper
            )
            collateral_amount_decimal: Decimal = intent.collateral_amount  # type: ignore[assignment]
            collateral_wei = int(collateral_amount_decimal * Decimal(10**collateral_decimals))
            size_delta_usd = int(intent.size_usd * Decimal(10**30))
            execution_fee = sdk.get_execution_fee(order_type="increase")

            order_params = GMXV2OrderParams(
                from_address=ctx.wallet_address,
                market=market_address,
                initial_collateral_token=collateral_address,
                initial_collateral_delta_amount=collateral_wei,
                size_delta_usd=size_delta_usd,
                is_long=intent.is_long,
                acceptable_price=int(acceptable_price),
                execution_fee=execution_fee,
            )
            tx_data = sdk.build_increase_order_multicall(order_params)

            is_native_collateral = collateral_token_upper in ("WETH", "ETH", "WAVAX", "AVAX")
            if not is_native_collateral and collateral_wei > 0:
                transactions.extend(
                    ctx.services.build_approve_tx(
                        token_address=collateral_address,
                        spender=sdk.ROUTER_ADDRESS,
                        amount=collateral_wei,
                    )
                )

            transactions.append(
                TransactionData(
                    to=tx_data.to,
                    value=tx_data.value,
                    data=tx_data.data,
                    gas_estimate=tx_data.gas_estimate,
                    description=(
                        f"Open {'LONG' if intent.is_long else 'SHORT'} {intent.market} position: "
                        f"${intent.size_usd} size, {intent.collateral_amount} collateral"
                    ),
                    tx_type="perp_open",
                )
            )

            result.action_bundle = ActionBundle(
                intent_type=IntentType.PERP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "market": intent.market,
                    "collateral_token": intent.collateral_token,
                    "collateral_amount": str(intent.collateral_amount),
                    "size_usd": str(intent.size_usd),
                    "is_long": intent.is_long,
                    "leverage": str(intent.leverage),
                    "max_slippage": str(intent.max_slippage),
                    "order_key": order_result.order_key,
                    "chain": ctx.chain,
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)
            result.warnings = warnings
        except Exception as exc:
            logger.exception("Failed to compile GMX V2 PERP_OPEN intent: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)

        return result

    def compile_perp_close(self, ctx: PerpCompilerContext, intent: PerpCloseIntent) -> CompilationResult:  # noqa: C901
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []
        warnings: list[str] = []

        try:
            if ctx.chain not in self.chains:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"GMX v2 not supported on chain: {ctx.chain}",
                    intent_id=intent.intent_id,
                )

            try:
                validate_collateral(chain=ctx.chain, market=intent.market, collateral_token=intent.collateral_token)
            except InvalidCollateralForMarketError as exc:
                return CompilationResult(status=CompilationStatus.FAILED, error=str(exc), intent_id=intent.intent_id)

            slippage_bps = int(intent.max_slippage * 10000)
            adapter = GMXv2Adapter(
                GMXv2Config(
                    chain=ctx.chain,
                    wallet_address=ctx.wallet_address,
                    default_slippage_bps=slippage_bps,
                )
            )
            acceptable_price = Decimal("0") if intent.is_long else Decimal(10**30)

            sdk_or_error = self._build_sdk(ctx, intent.intent_id)
            if isinstance(sdk_or_error, CompilationResult):
                return sdk_or_error
            sdk = sdk_or_error

            market_or_error = self._resolve_market(ctx, sdk, intent.market, intent.intent_id)
            if isinstance(market_or_error, CompilationResult):
                return market_or_error
            market_address = market_or_error

            collateral_or_error = self._resolve_collateral(ctx, intent.collateral_token, intent.intent_id)
            if isinstance(collateral_or_error, CompilationResult):
                return collateral_or_error
            collateral_address = collateral_or_error

            resolved_size_usd = intent.size_usd
            if intent.size_usd:
                size_delta_usd = int(intent.size_usd * Decimal(10**30))
            else:
                queried_size = self._get_position_size_onchain(
                    ctx, sdk, market_address, collateral_address, intent.is_long
                )
                if queried_size is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            "Cannot close full GMX V2 position: unable to read position size on-chain. "
                            "Either specify size_usd explicitly or ensure RPC/API connectivity. "
                            "Refusing to guess — incorrect sizes burn keeper execution fees."
                        ),
                        intent_id=intent.intent_id,
                    )
                size_delta_usd = queried_size
                resolved_size_usd = Decimal(size_delta_usd) / Decimal(10**30)

            order_result = adapter.close_position(
                market=intent.market,
                collateral_token=intent.collateral_token,
                is_long=intent.is_long,
                size_delta_usd=resolved_size_usd,
                acceptable_price=acceptable_price,
            )
            if not order_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=order_result.error or "Failed to create close order",
                    intent_id=intent.intent_id,
                )

            execution_fee = sdk.get_execution_fee(order_type="decrease")
            order_params = GMXV2OrderParams(
                from_address=ctx.wallet_address,
                market=market_address,
                initial_collateral_token=collateral_address,
                initial_collateral_delta_amount=0,
                size_delta_usd=size_delta_usd,
                is_long=intent.is_long,
                acceptable_price=int(acceptable_price),
                execution_fee=execution_fee,
            )
            tx_data = sdk.build_decrease_order_multicall(order_params)

            size_desc = f"${intent.size_usd}" if intent.size_usd else "full position"
            transactions.append(
                TransactionData(
                    to=tx_data.to,
                    value=tx_data.value,
                    data=tx_data.data,
                    gas_estimate=tx_data.gas_estimate,
                    description=f"Close {'LONG' if intent.is_long else 'SHORT'} {intent.market} position: {size_desc}",
                    tx_type="perp_close",
                )
            )

            result.action_bundle = ActionBundle(
                intent_type=IntentType.PERP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "market": intent.market,
                    "collateral_token": intent.collateral_token,
                    "is_long": intent.is_long,
                    "size_usd": str(intent.size_usd) if intent.size_usd else None,
                    "close_full_position": intent.close_full_position,
                    "max_slippage": str(intent.max_slippage),
                    "order_key": order_result.order_key,
                    "chain": ctx.chain,
                },
            )
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)
            result.warnings = warnings
        except Exception as exc:
            logger.exception("Failed to compile GMX V2 PERP_CLOSE intent: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)

        return result

    def _build_sdk(self, ctx: PerpCompilerContext, intent_id: str) -> GMXV2SDK | CompilationResult:
        gateway_client = ctx.gateway_client
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            gateway_client = None
        rpc_url = None if gateway_client is not None else ctx.rpc_url or ctx.services.get_chain_rpc_url()
        if gateway_client is None and not rpc_url:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"GMX V2 requires either a connected gateway_client or an RPC URL. "
                    f"Set ALMANAK_{ctx.chain.upper()}_RPC_URL, RPC_URL, ALCHEMY_API_KEY, "
                    "or use GatewayExecutionOrchestrator."
                ),
                intent_id=intent_id,
            )
        return GMXV2SDK(rpc_url=rpc_url, chain=ctx.chain, gateway_client=gateway_client)

    def _resolve_market(
        self, ctx: PerpCompilerContext, sdk: GMXV2SDK, market: str, intent_id: str
    ) -> str | CompilationResult:
        market_address = GMX_V2_MARKETS.get(ctx.chain, {}).get(market)
        if market_address:
            return market_address
        try:
            return sdk.get_market_address(market)
        except ValueError:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown market: {market}",
                intent_id=intent_id,
            )

    def _resolve_collateral(
        self, ctx: PerpCompilerContext, collateral_token: str, intent_id: str
    ) -> str | CompilationResult:
        collateral_upper = collateral_token.upper()
        chain_tokens = GMX_V2_TOKENS.get(ctx.chain, {})
        collateral_address = next((addr for sym, addr in chain_tokens.items() if sym.upper() == collateral_upper), None)
        if collateral_address:
            return collateral_address
        # Accept both ``0x`` and ``0X`` prefixes — case-insensitive, matching
        # ``market_rules.validate_collateral`` which already treats either form
        # as a raw address. A lowercase-only check would reject valid ``0X...``
        # inputs as "unknown collateral".
        if collateral_token[:2].lower() == "0x":
            return collateral_token
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Unknown collateral token: {collateral_token}",
            intent_id=intent_id,
        )

    def _resolve_collateral_decimals(
        self, ctx: PerpCompilerContext, collateral_token: str, collateral_upper: str
    ) -> int:
        collateral_token_info = None
        if ctx.token_resolver is not None:
            try:
                collateral_token_info = ctx.services.resolve_token(collateral_token)
            except AttributeError:
                collateral_token_info = None
        if collateral_token_info is not None:
            return collateral_token_info.decimals
        if collateral_upper in ("WETH", "WETH.E", "ETH", "WAVAX", "AVAX"):
            return 18
        if collateral_upper in ("WBTC", "BTC.B", "WBTC.E"):
            return 8
        return 6

    def _get_position_size_onchain(
        self,
        ctx: PerpCompilerContext,
        sdk: GMXV2SDK,
        market_address: str,
        collateral_address: str,
        is_long: bool,
    ) -> int | None:
        try:
            positions = sdk.get_account_positions(ctx.wallet_address)
        except PositionQueryError as exc:
            logger.warning("GMX V2 position query failed: %s", exc)
            return None
        except Exception as exc:
            logger.warning("Unexpected error querying GMX V2 positions: %s", exc)
            return None

        if not positions:
            logger.warning("No GMX V2 positions found for %s", ctx.wallet_address)
            return None

        market_lower = market_address.lower()
        collateral_lower = collateral_address.lower()
        for pos in positions:
            if (
                pos.get("market", "").lower() == market_lower
                and pos.get("collateral_token", "").lower() == collateral_lower
                and pos.get("is_long") == is_long
                and pos.get("size_in_usd", 0) > 0
            ):
                size_in_usd = pos["size_in_usd"]
                logger.info(
                    "Read on-chain GMX V2 position size: %s (30-decimal) for market=%s is_long=%s",
                    size_in_usd,
                    market_address,
                    is_long,
                )
                return int(size_in_usd)

        logger.warning(
            "No matching GMX V2 position found for market=%s collateral=%s is_long=%s",
            market_address,
            collateral_address,
            is_long,
        )
        return None


__all__ = ["GMXV2Compiler"]
