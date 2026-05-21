"""Connector-owned compiler for Curve Finance."""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType, LPCloseIntent, LPOpenIntent, SwapIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


class CurveCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compiler for Curve pool-based swaps and fungible LP positions."""

    protocols: ClassVar[frozenset[str]] = frozenset({"curve"})
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {
            IntentType.SWAP,
            IntentType.LP_OPEN,
            IntentType.LP_CLOSE,
        }
    )

    def compile(self, ctx: BaseCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.SWAP:
            return self.compile_swap(ctx, intent)
        if intent_type == IntentType.LP_OPEN:
            return self.compile_lp_open(ctx, intent)
        if intent_type == IntentType.LP_CLOSE:
            return self.compile_lp_close(ctx, intent)
        if intent_type == IntentType.LP_COLLECT_FEES:
            return self.compile_collect_fees(ctx, intent)
        return self._unsupported(intent)

    def compile_swap(self, ctx: BaseCompilerContext, intent: SwapIntent) -> CompilationResult:  # noqa: C901
        """Compile SWAP intent for Curve Finance."""
        from almanak.framework.connectors.curve.adapter import (
            CURVE_ADDRESSES,
            CURVE_POOLS,
            CurveAdapter,
            CurveConfig,
        )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[Any] = []

        try:
            if ctx.chain not in CURVE_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Curve is not supported on {ctx.chain}. Supported chains: {list(CURVE_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            from_token = ctx.services.resolve_token(intent.from_token)
            to_token = ctx.services.resolve_token(intent.to_token)

            if from_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown from_token: {intent.from_token}",
                    intent_id=intent.intent_id,
                )
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Unknown to_token: {intent.to_token}",
                    intent_id=intent.intent_id,
                )

            if intent.amount_usd is not None:
                price = ctx.services.require_token_price(from_token.symbol)
                amount_decimal = intent.amount_usd / price
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
                amount_decimal = Decimal(str(intent.amount))
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Either amount_usd or amount must be provided",
                    intent_id=intent.intent_id,
                )

            swap_params = intent.swap_params if hasattr(intent, "swap_params") and intent.swap_params else {}
            pool_address: str | None = swap_params.get("pool")
            pool_name: str = ""

            if not pool_address:
                chain_pools = CURVE_POOLS.get(ctx.chain, {})
                for name, pool_data in chain_pools.items():
                    coins_upper = [c.upper() for c in pool_data["coins"]]
                    if from_token.symbol.upper() in coins_upper and to_token.symbol.upper() in coins_upper:
                        pool_address = pool_data["address"]
                        pool_name = name
                        break

            if not pool_address:
                chain_pools = CURVE_POOLS.get(ctx.chain, {})
                available = {name: d["coins"] for name, d in chain_pools.items()}
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"No Curve pool found for {from_token.symbol}/{to_token.symbol} on {ctx.chain}. "
                        f"Available pools: {available}. "
                        f'You can specify a pool explicitly via swap_params={{"pool": "0x..."}}.'
                    ),
                    intent_id=intent.intent_id,
                )

            slippage_bps = int(intent.max_slippage * Decimal("10000"))

            logger.info(
                "Compiling Curve SWAP: %s -> %s, pool=%s (%s), amount=%s",
                from_token.symbol,
                to_token.symbol,
                pool_name or pool_address,
                ctx.chain,
                amount_decimal,
            )

            config = CurveConfig(
                chain=ctx.chain,
                wallet_address=ctx.wallet_address,
                default_slippage_bps=slippage_bps,
                rpc_url=ctx.rpc_url,
                gateway_client=ctx.gateway_client,
            )
            adapter = CurveAdapter(config)

            price_ratio: Decimal | None = None
            try:
                price_in = ctx.services.require_token_price(from_token.symbol)
                price_out = ctx.services.require_token_price(to_token.symbol)
                if price_out > 0:
                    price_ratio = price_in / price_out
            except (ValueError, ZeroDivisionError):
                logger.warning(
                    "Could not compute price_ratio for Curve swap %s -> %s; "
                    "CryptoSwap pools will fail, StableSwap pools will proceed safely.",
                    from_token.symbol,
                    to_token.symbol,
                )

            swap_result = adapter.swap(
                pool_address=pool_address,
                token_in=from_token.symbol,
                token_out=to_token.symbol,
                amount_in=amount_decimal,
                slippage_bps=slippage_bps,
                price_ratio=price_ratio,
            )

            if not swap_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=swap_result.error or "Curve swap failed",
                    intent_id=intent.intent_id,
                )

            if swap_result.amount_out_estimate <= 0 or swap_result.token_out_decimals < 0:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Curve quote returned non-positive amount_out_estimate "
                        f"({swap_result.amount_out_estimate}, decimals={swap_result.token_out_decimals}) "
                        f"for {from_token.symbol} -> {to_token.symbol} on pool {pool_name or pool_address}; "
                        f"refusing to build swap with no real slippage floor"
                    ),
                    intent_id=intent.intent_id,
                )

            transactions = swap_result.transactions
            total_gas = sum(tx.gas_estimate for tx in transactions)

            expected_out_human = Decimal(swap_result.amount_out_estimate) / Decimal(10**swap_result.token_out_decimals)
            metadata: dict[str, Any] = {
                "from_token": from_token.to_dict(),
                "to_token": to_token.to_dict(),
                "amount_in": str(amount_decimal),
                "pool_address": pool_address,
                "pool_name": pool_name,
                "protocol": "curve",
                "expected_output_human": str(expected_out_human),
            }

            action_bundle = ActionBundle(
                intent_type=IntentType.SWAP.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata=metadata,
            )

            result.action_bundle = action_bundle
            result.transactions = transactions  # type: ignore[assignment]
            result.total_gas_estimate = total_gas

            logger.info(
                "Compiled Curve SWAP intent: %s -> %s, %d txs, %d gas",
                from_token.symbol,
                to_token.symbol,
                len(transactions),
                total_gas,
            )

        except Exception as e:
            logger.exception("Failed to compile Curve SWAP intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def compile_lp_open(self, ctx: BaseCompilerContext, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Curve Finance."""
        from almanak.framework.connectors.curve.adapter import (
            CURVE_ADDRESSES,
            CURVE_POOLS,
            CurveAdapter,
            CurveConfig,
        )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[Any] = []

        try:
            if ctx.chain not in CURVE_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Curve is not supported on {ctx.chain}. Supported chains: {list(CURVE_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            chain_pools = CURVE_POOLS.get(ctx.chain, {})

            pool_name: str = ""
            pool_address: str = intent.pool
            pool_data: dict[str, Any] | None = None

            if intent.pool in chain_pools:
                pool_name = intent.pool
                pool_data = chain_pools[intent.pool]
                pool_address = pool_data["address"]
            else:
                for name, data in chain_pools.items():
                    if data["address"].lower() == intent.pool.lower():
                        pool_name = name
                        pool_data = data
                        pool_address = data["address"]
                        break

            if pool_data is None:
                available = {name: d["address"] for name, d in chain_pools.items()}
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Unknown Curve pool: {intent.pool} on {ctx.chain}. Available pools: {available}"),
                    intent_id=intent.intent_id,
                )

            n_coins = pool_data["n_coins"]

            amounts: list[Decimal] = [intent.amount0, intent.amount1]
            while len(amounts) < n_coins:
                amounts.append(Decimal("0"))

            slippage_bps = 50

            logger.info(
                "Compiling Curve LP_OPEN: pool=%s (%s), amounts=%s",
                pool_name,
                ctx.chain,
                amounts,
            )

            config = CurveConfig(
                chain=ctx.chain,
                wallet_address=ctx.wallet_address,
                default_slippage_bps=slippage_bps,
                rpc_url=ctx.rpc_url,
                gateway_client=ctx.gateway_client,
            )
            adapter = CurveAdapter(config)

            liq_result = adapter.add_liquidity(
                pool_address=pool_address,
                amounts=amounts,
                slippage_bps=slippage_bps,
            )

            if not liq_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=liq_result.error or "Curve add_liquidity failed",
                    intent_id=intent.intent_id,
                )

            transactions = liq_result.transactions
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_OPEN.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool_address": pool_address,
                    "pool_name": pool_name,
                    "amounts": [str(a) for a in amounts],
                    "n_coins": n_coins,
                    "lp_token": pool_data["lp_token"],
                    "protocol": "curve",
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions  # type: ignore[assignment]
            result.total_gas_estimate = total_gas

            logger.info(
                "Compiled Curve LP_OPEN intent: pool=%s, %d txs, %d gas",
                pool_name,
                len(transactions),
                total_gas,
            )

        except Exception as e:
            logger.exception("Failed to compile Curve LP_OPEN intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def compile_lp_close(self, ctx: BaseCompilerContext, intent: LPCloseIntent) -> CompilationResult:  # noqa: C901
        """Compile LP_CLOSE intent for Curve Finance."""
        from almanak.framework.connectors.curve.adapter import (
            CURVE_ADDRESSES,
            CURVE_POOLS,
            CurveAdapter,
            CurveConfig,
        )

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[Any] = []

        try:
            if ctx.chain not in CURVE_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Curve is not supported on {ctx.chain}. Supported chains: {list(CURVE_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            if not intent.pool:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="intent.pool must be set to the Curve pool address for LP_CLOSE",
                    intent_id=intent.intent_id,
                )

            chain_pools = CURVE_POOLS.get(ctx.chain, {})

            pool_name: str = ""
            pool_address: str = intent.pool
            pool_data: dict[str, Any] | None = None

            if intent.pool in chain_pools:
                pool_name = intent.pool
                pool_data = chain_pools[intent.pool]
                pool_address = pool_data["address"]
            else:
                for name, data in chain_pools.items():
                    if data["address"].lower() == intent.pool.lower():
                        pool_name = name
                        pool_data = data
                        pool_address = data["address"]
                        break

            if pool_data is None:
                available = {name: d["address"] for name, d in chain_pools.items()}
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Unknown Curve pool: {intent.pool} on {ctx.chain}. Available pools: {available}"),
                    intent_id=intent.intent_id,
                )

            lp_token_for_pool = pool_data.get("lp_token", "")
            if not lp_token_for_pool:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Pool config for '{pool_name or pool_address}' is missing 'lp_token' field. "
                        f"Cannot compile Curve LP_CLOSE safely."
                    ),
                    intent_id=intent.intent_id,
                )

            position_id_str = str(intent.position_id).strip()
            if position_id_str.startswith("0x") and len(position_id_str) == 42:
                lp_token_address = position_id_str
                if lp_token_address.lower() != lp_token_for_pool.lower():
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"position_id LP token {lp_token_address} does not match "
                            f"pool '{pool_name}' LP token {lp_token_for_pool}. "
                            f"Refusing to proceed — this would close the wrong position."
                        ),
                        intent_id=intent.intent_id,
                    )
                raw_balance = ctx.services.query_erc20_balance(lp_token_for_pool, ctx.wallet_address)
                if raw_balance is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Failed to query LP token balance for {pool_name or pool_address} "
                            f"({lp_token_for_pool}). Ensure gateway_client or rpc_url is configured."
                        ),
                        intent_id=intent.intent_id,
                    )
                if raw_balance == 0:
                    logger.info("Curve LP_CLOSE: zero LP balance for %s — no_op", pool_name)
                    return CompilationResult(
                        status=CompilationStatus.SUCCESS,
                        action_bundle=ActionBundle(
                            intent_type=IntentType.LP_CLOSE.value,
                            transactions=[],
                            metadata={
                                "no_op": True,
                                "reason": f"zero LP token balance for {pool_name} ({lp_token_for_pool})",
                            },
                        ),
                        intent_id=intent.intent_id,
                    )
                lp_token_info = ctx.services.resolve_token(lp_token_for_pool)
                if not lp_token_info:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Could not resolve decimals for Curve LP token {lp_token_for_pool}. "
                            f"Cannot safely compute withdrawal amount without known decimals."
                        ),
                        intent_id=intent.intent_id,
                    )
                lp_amount = Decimal(raw_balance) / Decimal(10**lp_token_info.decimals)
                logger.info("Queried on-chain LP balance for %s: %s", pool_name, lp_amount)
            else:
                try:
                    lp_amount = Decimal(position_id_str)
                except (InvalidOperation, TypeError, ValueError):
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=(
                            f"Invalid position_id for Curve LP_CLOSE: '{intent.position_id}'. "
                            f"Must be an LP token address (0x...) or LP token amount as decimal string (e.g., '100.5')."
                        ),
                        intent_id=intent.intent_id,
                    )

            slippage_bps = 50

            logger.info(
                "Compiling Curve LP_CLOSE: pool=%s (%s), lp_amount=%s",
                pool_name,
                ctx.chain,
                lp_amount,
            )

            config = CurveConfig(
                chain=ctx.chain,
                wallet_address=ctx.wallet_address,
                default_slippage_bps=slippage_bps,
                rpc_url=ctx.rpc_url,
                gateway_client=ctx.gateway_client,
            )
            adapter = CurveAdapter(config)

            liq_result = adapter.remove_liquidity(
                pool_address=pool_address,
                lp_amount=lp_amount,
                slippage_bps=slippage_bps,
            )

            if not liq_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=liq_result.error or "Curve remove_liquidity failed",
                    intent_id=intent.intent_id,
                )

            transactions = liq_result.transactions
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "pool_address": pool_address,
                    "pool_name": pool_name,
                    "lp_amount": str(lp_amount),
                    "lp_token": pool_data["lp_token"],
                    "protocol": "curve",
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions  # type: ignore[assignment]
            result.total_gas_estimate = total_gas

            logger.info(
                "Compiled Curve LP_CLOSE intent: pool=%s, %d txs, %d gas",
                pool_name,
                len(transactions),
                total_gas,
            )

        except Exception as e:
            logger.exception("Failed to compile Curve LP_CLOSE intent")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def compile_collect_fees(self, ctx: BaseCompilerContext, intent: CollectFeesIntent) -> CompilationResult:
        """Curve fungible LP positions do not expose a separate fee-collect intent."""
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error="Curve does not support LP_COLLECT_FEES compilation.",
            intent_id=intent.intent_id,
        )


__all__ = ["CurveCompiler"]
