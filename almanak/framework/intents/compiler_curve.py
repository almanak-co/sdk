"""Curve Finance compilation helpers extracted from IntentCompiler.

These standalone functions receive the compiler instance as their first
parameter and implement all Curve-related compilation logic (swap, LP open,
LP close).
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from ..models.reproduction_bundle import ActionBundle
from .compiler_models import CompilationResult, CompilationStatus
from .vocabulary import IntentType

if TYPE_CHECKING:
    from .vocabulary import LPCloseIntent, LPOpenIntent, SwapIntent

logger = logging.getLogger("almanak.framework.intents.compiler")


def compile_swap_curve(compiler, intent: SwapIntent) -> CompilationResult:
    """Compile SWAP intent for Curve Finance.

    Curve uses pool-specific AMMs (StableSwap, CryptoSwap, Tricrypto).
    The pool is selected automatically from the registry by matching the
    token pair, or can be overridden via swap_params={"pool": "0x..."}.

    swap_params options:
    - pool (str): Explicit pool address (overrides auto-lookup)
    - slippage_bps (int): Override slippage in basis points

    Args:
        compiler: IntentCompiler instance
        intent: SwapIntent with from_token, to_token, and amount

    Returns:
        CompilationResult with Curve exchange ActionBundle
    """
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
        # Check chain support
        if compiler.chain not in CURVE_ADDRESSES:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Curve is not supported on {compiler.chain}. Supported chains: {list(CURVE_ADDRESSES.keys())}",
                intent_id=intent.intent_id,
            )

        # Resolve tokens
        from_token = compiler._resolve_token(intent.from_token)
        to_token = compiler._resolve_token(intent.to_token)

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

        # Calculate input amount (in token units)
        if intent.amount_usd is not None:
            price = compiler._require_token_price(from_token.symbol)
            amount_decimal = intent.amount_usd / price
        elif intent.amount is not None:
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                    intent_id=intent.intent_id,
                )
            amount_decimal = Decimal(str(intent.amount))
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Either amount_usd or amount must be provided",
                intent_id=intent.intent_id,
            )

        # Resolve pool address: explicit override or auto-lookup by token pair
        swap_params = intent.swap_params if hasattr(intent, "swap_params") and intent.swap_params else {}
        pool_address: str | None = swap_params.get("pool")
        pool_name: str = ""

        if not pool_address:
            chain_pools = CURVE_POOLS.get(compiler.chain, {})
            for name, pool_data in chain_pools.items():
                coins_upper = [c.upper() for c in pool_data["coins"]]
                if from_token.symbol.upper() in coins_upper and to_token.symbol.upper() in coins_upper:
                    pool_address = pool_data["address"]
                    pool_name = name
                    break

        if not pool_address:
            chain_pools = CURVE_POOLS.get(compiler.chain, {})
            available = {name: d["coins"] for name, d in chain_pools.items()}
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"No Curve pool found for {from_token.symbol}/{to_token.symbol} on {compiler.chain}. "
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
            compiler.chain,
            amount_decimal,
        )

        config = CurveConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            default_slippage_bps=slippage_bps,
            rpc_url=compiler._get_chain_rpc_url(),
        )
        adapter = CurveAdapter(config)

        # Compute price ratio for CryptoSwap/Tricrypto slippage protection.
        # price_ratio = price_in / price_out so that:
        # expected_output_tokens = amount_in_tokens * price_ratio
        price_ratio: Decimal | None = None
        try:
            price_in = compiler._require_token_price(from_token.symbol)
            price_out = compiler._require_token_price(to_token.symbol)
            if price_out > 0:
                price_ratio = price_in / price_out
        except (ValueError, ZeroDivisionError):
            # Price unavailable — adapter will reject CryptoSwap swaps (fail closed)
            # and accept StableSwap swaps (price_ratio not needed for 1:1 pairs)
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

        transactions = swap_result.transactions
        total_gas = sum(tx.gas_estimate for tx in transactions)

        action_bundle = ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "from_token": from_token.to_dict(),
                "to_token": to_token.to_dict(),
                "amount_in": str(amount_decimal),
                "pool_address": pool_address,
                "pool_name": pool_name,
                "protocol": "curve",
            },
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


def compile_lp_open_curve(compiler, intent: LPOpenIntent) -> CompilationResult:
    """Compile LP_OPEN intent for Curve Finance.

    Curve LP positions are fungible (not NFT-based). The pool is specified
    via intent.pool (address or name like "3pool"). Both amount0 and amount1
    are used; for 3-coin pools, only these two coins are deposited (third = 0).

    Pool format: "0xPoolAddress" or pool name like "3pool", "frax_usdc"

    Args:
        compiler: IntentCompiler instance
        intent: LPOpenIntent with pool, amount0, amount1

    Returns:
        CompilationResult with Curve add_liquidity ActionBundle
    """
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
        # Check chain support
        if compiler.chain not in CURVE_ADDRESSES:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Curve is not supported on {compiler.chain}. Supported chains: {list(CURVE_ADDRESSES.keys())}",
                intent_id=intent.intent_id,
            )

        chain_pools = CURVE_POOLS.get(compiler.chain, {})

        # Resolve pool: by address or by name
        pool_name: str = ""
        pool_address: str = intent.pool
        pool_data: dict[str, Any] | None = None

        # Check by name first (e.g., "3pool", "frax_usdc")
        if intent.pool in chain_pools:
            pool_name = intent.pool
            pool_data = chain_pools[intent.pool]
            pool_address = pool_data["address"]
        else:
            # Check by address
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
                error=(f"Unknown Curve pool: {intent.pool} on {compiler.chain}. Available pools: {available}"),
                intent_id=intent.intent_id,
            )

        n_coins = pool_data["n_coins"]

        # Build amounts list padded to n_coins (amount0, amount1, then 0s for remaining)
        amounts: list[Decimal] = [intent.amount0, intent.amount1]
        while len(amounts) < n_coins:
            amounts.append(Decimal("0"))

        slippage_bps = 50  # Default 0.5% for LP

        logger.info(
            "Compiling Curve LP_OPEN: pool=%s (%s), amounts=%s",
            pool_name,
            compiler.chain,
            amounts,
        )

        config = CurveConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            default_slippage_bps=slippage_bps,
            rpc_url=compiler._get_chain_rpc_url(),
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


def compile_lp_close_curve(compiler, intent: LPCloseIntent) -> CompilationResult:
    """Compile LP_CLOSE intent for Curve Finance.

    Burns LP tokens in exchange for underlying tokens (proportional removal).
    LP token amount is passed via intent.position_id (as a decimal string).

    intent.pool: Curve pool address or name
    intent.position_id: LP token amount to burn (e.g., "100.5")

    Args:
        compiler: IntentCompiler instance
        intent: LPCloseIntent with pool and position_id (LP amount)

    Returns:
        CompilationResult with Curve remove_liquidity ActionBundle
    """
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
        # Check chain support
        if compiler.chain not in CURVE_ADDRESSES:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Curve is not supported on {compiler.chain}. Supported chains: {list(CURVE_ADDRESSES.keys())}",
                intent_id=intent.intent_id,
            )

        if not intent.pool:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="intent.pool must be set to the Curve pool address for LP_CLOSE",
                intent_id=intent.intent_id,
            )

        chain_pools = CURVE_POOLS.get(compiler.chain, {})

        # Resolve pool: by name or address
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
                error=(f"Unknown Curve pool: {intent.pool} on {compiler.chain}. Available pools: {available}"),
                intent_id=intent.intent_id,
            )

        # Parse LP token amount from position_id.
        # position_id can be:
        #   - An LP token address (0x...) — query on-chain balance and withdraw all
        #   - An LP token amount as decimal string (e.g., "100.5") — legacy, withdraw that amount
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
            # Position ID is an LP token address — withdraw full balance
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
            # Query on-chain LP token balance via shared helper
            raw_balance = compiler._query_erc20_balance(lp_token_for_pool, compiler.wallet_address)
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
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Wallet has zero LP token balance for {pool_name} ({lp_token_for_pool})",
                    intent_id=intent.intent_id,
                )
            lp_token_info = compiler._resolve_token(lp_token_for_pool)
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
            # Legacy: position_id is LP token amount as decimal string
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

        slippage_bps = 50  # Default 0.5%

        logger.info(
            "Compiling Curve LP_CLOSE: pool=%s (%s), lp_amount=%s",
            pool_name,
            compiler.chain,
            lp_amount,
        )

        config = CurveConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            default_slippage_bps=slippage_bps,
            rpc_url=compiler._get_chain_rpc_url(),
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
