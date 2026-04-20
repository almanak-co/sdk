"""Aerodrome/Velodrome compilation helpers extracted from IntentCompiler.

These standalone functions receive the compiler instance as their first
parameter and implement all Aerodrome-related compilation logic (LP open,
LP close, swap, pool address query).
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ..models.reproduction_bundle import ActionBundle
from . import compiler_constants
from .compiler_models import CompilationResult, CompilationStatus
from .vocabulary import IntentType

if TYPE_CHECKING:
    from .vocabulary import LPCloseIntent, LPOpenIntent, SwapIntent

logger = logging.getLogger("almanak.framework.intents.compiler")

LP_POSITION_MANAGERS = compiler_constants.LP_POSITION_MANAGERS


def compile_lp_open_aerodrome(compiler, intent: LPOpenIntent) -> CompilationResult:
    """Compile LP_OPEN intent for Aerodrome Finance (Solidly fork on Base).

    Aerodrome uses a simple xy=k or x^3y+y^3x AMM with:
    - Fungible LP tokens (not NFTs)
    - Two pool types: volatile (0.3% fee) and stable (0.05% fee)
    - Full range liquidity (no concentrated positions)

    Pool format: "TOKEN0/TOKEN1/volatile" or "TOKEN0/TOKEN1/stable"

    Args:
        compiler: IntentCompiler instance
        intent: LPOpenIntent to compile

    Returns:
        CompilationResult with Aerodrome addLiquidity ActionBundle
    """
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[Any] = []
    warnings: list[str] = []

    try:
        # Import Aerodrome adapter (lazy import to avoid circular deps)
        from almanak.framework.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

        # Parse pool info (format: TOKEN0/TOKEN1/pool_type)
        pool_parts = intent.pool.split("/")
        if len(pool_parts) < 2:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Invalid pool format: {intent.pool}. Expected: TOKEN0/TOKEN1/volatile or TOKEN0/TOKEN1/stable",
                intent_id=intent.intent_id,
            )

        token0_symbol = pool_parts[0]
        token1_symbol = pool_parts[1]
        # Default to volatile if not specified
        stable = pool_parts[2].lower() == "stable" if len(pool_parts) > 2 else False

        logger.info(
            f"Compiling Aerodrome LP_OPEN: {token0_symbol}/{token1_symbol}, stable={stable}, amounts={intent.amount0}/{intent.amount1}"
        )

        # Resolve token addresses
        token0_info = compiler._resolve_token(token0_symbol)
        token1_info = compiler._resolve_token(token1_symbol)

        if token0_info is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {token0_symbol}",
                intent_id=intent.intent_id,
            )
        if token1_info is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {token1_symbol}",
                intent_id=intent.intent_id,
            )

        # Validate pool existence (best-effort)
        from .pool_validation import validate_aerodrome_pool

        pool_check = validate_aerodrome_pool(
            compiler.chain, token0_info.address, token1_info.address, stable, compiler._get_chain_rpc_url()
        )
        failed = compiler._validate_pool(pool_check, intent.intent_id)
        if failed is not None:
            return failed

        # Convert amounts to wei
        int(intent.amount0 * Decimal(10**token0_info.decimals))
        int(intent.amount1 * Decimal(10**token1_info.decimals))

        # Get router address
        router_address = LP_POSITION_MANAGERS.get(compiler.chain, {}).get(
            "aerodrome", "0x0000000000000000000000000000000000000000"
        )

        if router_address == "0x0000000000000000000000000000000000000000":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Aerodrome not supported on {compiler.chain}",
                intent_id=intent.intent_id,
            )

        # Create Aerodrome adapter to build all transactions
        # The adapter handles approvals and the addLiquidity call
        config = AerodromeConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            price_provider=compiler.price_oracle,
            rpc_url=compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
        )
        adapter = AerodromeAdapter(config)

        # Build addLiquidity transaction using the adapter
        liquidity_result = adapter.add_liquidity(
            token_a=token0_symbol,
            token_b=token1_symbol,
            amount_a=intent.amount0,
            amount_b=intent.amount1,
            stable=stable,
            recipient=compiler.wallet_address,
        )

        if not liquidity_result.success:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Failed to build addLiquidity TX: {liquidity_result.error}",
                intent_id=intent.intent_id,
            )

        # Use transactions from the adapter result (includes approvals + addLiquidity)
        # The adapter already builds all needed transactions
        for tx in liquidity_result.transactions:
            transactions.append(tx)

        # Build ActionBundle
        total_gas = sum(tx.gas_estimate for tx in transactions)

        action_bundle = ActionBundle(
            intent_type=IntentType.LP_OPEN.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "pool": intent.pool,
                "token0": token0_info.to_dict(),
                "token1": token1_info.to_dict(),
                "stable": stable,
                "amount0": str(intent.amount0),
                "amount1": str(intent.amount1),
                "protocol": "aerodrome",
            },
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas
        result.warnings = warnings

        tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
        tx_summary = f" ({tx_types})" if tx_types else ""
        logger.info(
            f"Compiled Aerodrome LP_OPEN intent: {token0_symbol}/{token1_symbol}, stable={stable}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
        )

    except Exception as e:
        logger.exception(f"Failed to compile Aerodrome LP_OPEN intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


def compile_lp_close_aerodrome(compiler, intent: LPCloseIntent) -> CompilationResult:
    """Compile LP_CLOSE intent for Aerodrome Finance.

    Aerodrome LP close:
    1. Approve LP tokens for router (if needed)
    2. Call removeLiquidity to burn LP and receive both tokens

    Pool format: "TOKEN0/TOKEN1/volatile" or "TOKEN0/TOKEN1/stable"

    Args:
        compiler: IntentCompiler instance
        intent: LPCloseIntent to compile

    Returns:
        CompilationResult with Aerodrome removeLiquidity ActionBundle
    """
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[Any] = []
    warnings: list[str] = []

    try:
        # Import Aerodrome adapter (lazy import to avoid circular deps)
        from almanak.framework.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

        # Parse pool info from position_id (format: TOKEN0/TOKEN1/pool_type)
        pool_parts = intent.position_id.split("/")
        if len(pool_parts) < 2:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Invalid position ID: {intent.position_id}. Expected: TOKEN0/TOKEN1/volatile or TOKEN0/TOKEN1/stable",
                intent_id=intent.intent_id,
            )

        token0_symbol = pool_parts[0]
        token1_symbol = pool_parts[1]
        stable = pool_parts[2].lower() == "stable" if len(pool_parts) > 2 else False

        logger.info(f"Compiling Aerodrome LP_CLOSE: {token0_symbol}/{token1_symbol}, stable={stable}")

        # Resolve token addresses
        token0_info = compiler._resolve_token(token0_symbol)
        token1_info = compiler._resolve_token(token1_symbol)

        if token0_info is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {token0_symbol}",
                intent_id=intent.intent_id,
            )
        if token1_info is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {token1_symbol}",
                intent_id=intent.intent_id,
            )

        # Get router address
        router_address = LP_POSITION_MANAGERS.get(compiler.chain, {}).get(
            "aerodrome", "0x0000000000000000000000000000000000000000"
        )

        if router_address == "0x0000000000000000000000000000000000000000":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Aerodrome not supported on {compiler.chain}",
                intent_id=intent.intent_id,
            )

        # Create Aerodrome adapter
        config = AerodromeConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            price_provider=compiler.price_oracle,
            rpc_url=compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
        )
        adapter = AerodromeAdapter(config)

        # Get LP token address for the pool (gateway-aware for deployed mode)
        pool_address = compiler._get_aerodrome_pool_address(
            token0_info.address,
            token1_info.address,
            stable,
        )

        if not pool_address:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Pool not found for {token0_symbol}/{token1_symbol} (stable={stable})",
                intent_id=intent.intent_id,
            )

        # Query actual LP token balance from on-chain
        # LP token is the pool contract itself (ERC-20)
        lp_balance_wei = compiler._query_erc20_balance(pool_address, compiler.wallet_address)

        # In permission discovery mode, use a synthetic balance so the
        # compiler produces the full approve + removeLiquidity transaction
        # set.  Without this, the zero/None balance causes an early return
        # with empty transactions, and the LP token approve permission is
        # never discovered.
        _cfg = getattr(compiler, "_config", None)
        if _cfg and getattr(_cfg, "permission_discovery", False) and (lp_balance_wei is None or lp_balance_wei == 0):
            lp_balance_wei = 10**18  # 1 LP token (synthetic)
            logger.debug("Permission discovery mode: using synthetic LP balance for %s", pool_address)

        if lp_balance_wei is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Could not query LP balance for pool {pool_address}. Ensure rpc_url is provided to IntentCompiler.",
                intent_id=intent.intent_id,
            )

        if lp_balance_wei == 0:
            warning = (
                f"No LP tokens found in wallet for {token0_symbol}/{token1_symbol} pool "
                f"(pool={pool_address}) - treating LP_CLOSE as no-op"
            )
            warnings.append(warning)
            logger.info(warning)

            result.action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[],
                metadata={
                    "pool": intent.position_id,
                    "pool_address": pool_address,
                    "token0_symbol": token0_symbol,
                    "token1_symbol": token1_symbol,
                    "stable": stable,
                    "protocol": "aerodrome",
                    "collect_fees": intent.collect_fees,
                    "warning": "No LP tokens found; LP_CLOSE no-op",
                },
            )
            result.transactions = []
            result.total_gas_estimate = 0
            result.warnings = warnings
            return result

        # Convert wei to decimal (LP tokens have 18 decimals)
        lp_balance = Decimal(lp_balance_wei) / Decimal(10**18)
        logger.info(f"Found {lp_balance} LP tokens ({lp_balance_wei} wei) for Aerodrome pool")

        # Build removeLiquidity transaction using the adapter
        # Pass pre-resolved pool_address so the adapter doesn't make
        # its own direct RPC call (which fails in deployed mode).
        liquidity_result = adapter.remove_liquidity(
            token_a=token0_symbol,
            token_b=token1_symbol,
            liquidity=lp_balance,
            stable=stable,
            recipient=compiler.wallet_address,
            pool_address=pool_address,
        )

        if not liquidity_result.success:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Failed to build removeLiquidity TX: {liquidity_result.error}",
                intent_id=intent.intent_id,
            )

        # Use transactions from the adapter result (includes approvals + removeLiquidity)
        for tx in liquidity_result.transactions:
            transactions.append(tx)

        # Build ActionBundle
        total_gas = sum(tx.gas_estimate for tx in transactions)

        action_bundle = ActionBundle(
            intent_type=IntentType.LP_CLOSE.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "pool": intent.position_id,
                "token0": token0_info.to_dict(),
                "token1": token1_info.to_dict(),
                "stable": stable,
                "protocol": "aerodrome",
                "collect_fees": intent.collect_fees,
            },
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas
        result.warnings = warnings

        tx_types = " + ".join(str(getattr(tx, "tx_type", "")) for tx in transactions) if transactions else ""
        tx_summary = f" ({tx_types})" if tx_types else ""
        logger.info(
            f"Compiled Aerodrome LP_CLOSE intent: {token0_symbol}/{token1_symbol}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
        )

    except Exception as e:
        logger.exception(f"Failed to compile Aerodrome LP_CLOSE intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


def compile_swap_aerodrome(compiler, intent: SwapIntent) -> CompilationResult:
    """Compile SWAP intent for Aerodrome/Velodrome (Solidly forks).

    On Base (Aerodrome): defaults to Slipstream CL pools; classic via swap_params={"classic": True}.
    On Optimism (Velodrome): defaults to classic routing (no CL/Slipstream contracts).

    swap_params options:
    - tick_spacing (int): CL pool tick spacing, default 100
    - classic (bool): If True, use Classic volatile/stable routing
    - stable (bool): Pool type for Classic routing (default False)

    Args:
        compiler: IntentCompiler instance
        intent: SwapIntent with from_token, to_token, and amount

    Returns:
        CompilationResult with Aerodrome swap ActionBundle
    """
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[Any] = []

    try:
        # Import Aerodrome adapter (lazy import to avoid circular deps)
        from almanak.framework.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

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

        # Calculate input amount
        amount_decimal: Decimal
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
            amount_decimal = intent.amount  # type: ignore[assignment]
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Either amount_usd or amount must be provided",
                intent_id=intent.intent_id,
            )

        # Extract routing params from swap_params
        swap_params = intent.swap_params if hasattr(intent, "swap_params") and intent.swap_params else {}
        tick_spacing = swap_params.get("tick_spacing", 100)
        stable = swap_params.get("stable", False)

        # Check chain support dynamically from contract addresses
        from almanak.core.contracts import AERODROME as AERODROME_ADDRESSES

        if compiler.chain not in AERODROME_ADDRESSES:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Aerodrome/Velodrome is not supported on {compiler.chain}. Supported: {list(AERODROME_ADDRESSES.keys())}",
                intent_id=intent.intent_id,
            )

        # Auto-detect CL (Slipstream) availability from contract addresses.
        # Only Base has cl_router/cl_factory; Optimism (Velodrome) uses classic only.
        chain_addrs = AERODROME_ADDRESSES[compiler.chain]
        has_cl = bool(chain_addrs.get("cl_router") and chain_addrs.get("cl_factory"))
        requested_classic = swap_params.get("classic")
        if requested_classic is False and not has_cl:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"CL (Slipstream) routing is not available on {compiler.chain}; use classic routing instead.",
                intent_id=intent.intent_id,
            )
        use_classic = requested_classic if requested_classic is not None else not has_cl

        routing = "classic" if use_classic else "cl"
        logger.info(
            f"Compiling Aerodrome SWAP ({routing}): {from_token.symbol} -> {to_token.symbol}, amount={amount_decimal}"
        )

        # Validate pool existence
        if use_classic:
            from .pool_validation import validate_aerodrome_pool

            pool_check = validate_aerodrome_pool(
                compiler.chain, from_token.address, to_token.address, stable, compiler._get_chain_rpc_url()
            )
        else:
            from .pool_validation import validate_aerodrome_cl_pool

            pool_check = validate_aerodrome_cl_pool(
                compiler.chain, from_token.address, to_token.address, tick_spacing, compiler._get_chain_rpc_url()
            )
        failed = compiler._validate_pool(pool_check, intent.intent_id)
        if failed is not None:
            return failed

        # Create Aerodrome adapter
        config = AerodromeConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            default_slippage_bps=int(intent.max_slippage * Decimal("10000")),
            price_provider=compiler.price_oracle,
            rpc_url=compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
        )
        adapter = AerodromeAdapter(config)

        # Build swap using adapter
        swap_result = adapter.swap_exact_input(
            token_in=from_token.symbol,
            token_out=to_token.symbol,
            amount_in=amount_decimal,
            stable=stable,
            tick_spacing=tick_spacing,
            use_classic=use_classic,
        )

        if not swap_result.success:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=swap_result.error or "Aerodrome swap failed",
                intent_id=intent.intent_id,
            )

        # Convert adapter transactions to compiler format
        for tx_data in swap_result.transactions:
            transactions.append(tx_data)

        total_gas = sum(tx.gas_estimate for tx in transactions)

        # VIB-3203: Pre-slippage-discount quote in human units for realized slippage
        # computation by ResultEnricher after execution.
        expected_output_human: Decimal | None = None
        try:
            quoted_amount_out = getattr(swap_result.quote, "amount_out", None) if swap_result.quote else None
            if quoted_amount_out:
                expected_output_human = Decimal(str(quoted_amount_out)) / Decimal(10**to_token.decimals)
        except (TypeError, ValueError, AttributeError):
            expected_output_human = None

        metadata: dict[str, Any] = {
            "from_token": from_token.to_dict(),
            "to_token": to_token.to_dict(),
            "amount_in": str(amount_decimal),
            "routing": routing,
            "protocol": "aerodrome",
        }
        if expected_output_human is not None:
            metadata["expected_output_human"] = str(expected_output_human)

        action_bundle = ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata=metadata,
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas

        logger.info(
            f"Compiled Aerodrome SWAP intent ({routing}): {from_token.symbol} -> {to_token.symbol}, {len(transactions)} txs, {total_gas} gas"
        )

    except Exception as e:
        logger.exception(f"Failed to compile Aerodrome SWAP intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


def get_aerodrome_pool_address(compiler, token_a: str, token_b: str, stable: bool) -> str | None:
    """Query Aerodrome pool address, preferring gateway RPC over direct calls.

    In deployed mode the strategy container has no outbound network access,
    so direct Web3 HTTP calls fail with DNS resolution errors.  This method
    routes the factory ``getPool()`` call through the gateway's RPC proxy
    when available, falling back to a direct ``eth_call`` for local dev.

    Args:
        compiler: IntentCompiler instance
        token_a: Token A address
        token_b: Token B address
        stable: Pool type (True=stable, False=volatile)

    Returns:
        Pool contract address, or None if pool not found / query failed.
    """
    from almanak.core.contracts import AERODROME
    from almanak.framework.intents.pool_validation import (
        ZERO_ADDRESS,
        _decode_address,
        _encode_get_pool_aerodrome,
    )

    chain_contracts = AERODROME.get(compiler.chain.lower())
    if chain_contracts is None or "factory" not in chain_contracts:
        logger.warning(f"No Aerodrome factory address for chain '{compiler.chain}'")
        return None

    factory = chain_contracts["factory"]
    calldata = _encode_get_pool_aerodrome(token_a, token_b, stable)

    def _process_raw_result(raw: bytes | None) -> str | None:
        """Decode raw eth_call bytes into a pool address, returning None if invalid."""
        if raw is None:
            return None
        pool_address = _decode_address(raw)
        if pool_address == ZERO_ADDRESS:
            return None
        return pool_address

    # --- Gateway path (deployed mode) ---
    if compiler._gateway_client is not None:
        try:
            hex_result = compiler._gateway_client.eth_call(
                chain=compiler.chain,
                to=factory,
                data=calldata,
            )
            if hex_result and hex_result != "0x":
                raw = bytes.fromhex(hex_result[2:] if hex_result.startswith("0x") else hex_result)
                pool_address = _process_raw_result(raw)
                if pool_address:
                    logger.debug(f"Resolved Aerodrome pool via gateway: {pool_address}")
                    return pool_address
            return None
        except Exception as e:
            logger.warning("Gateway Aerodrome pool query failed, falling back to direct RPC: %s", e)

    # --- Direct RPC fallback (local dev) ---
    rpc_url = compiler._get_chain_rpc_url()
    if rpc_url is None:
        logger.warning("No RPC URL or gateway client — cannot query Aerodrome pool address")
        return None

    from almanak.framework.intents.pool_validation import _eth_call

    rpc_raw = _eth_call(rpc_url, factory, calldata)
    pool_address = _process_raw_result(rpc_raw)
    if pool_address:
        logger.debug(f"Resolved Aerodrome pool via direct RPC: {pool_address}")
    return pool_address
