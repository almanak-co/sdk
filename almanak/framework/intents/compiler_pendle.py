"""Pendle compilation helpers extracted from IntentCompiler.

These standalone functions receive the compiler instance as their first
parameter and implement all Pendle-related compilation logic (swap, LP open,
LP close, redeem).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from ..models.reproduction_bundle import ActionBundle
from . import compiler_constants
from .compiler_models import CompilationResult, CompilationStatus, TokenInfo, TransactionData
from .vocabulary import IntentType

if TYPE_CHECKING:
    from .vocabulary import LPCloseIntent, LPOpenIntent, SwapIntent, WithdrawIntent

logger = logging.getLogger("almanak.framework.intents.compiler")


def compile_pendle_swap(compiler, intent: SwapIntent) -> CompilationResult:
    """Compile SWAP intent for Pendle Protocol (yield tokenization).

    Pendle enables swapping tokens to PT (Principal Tokens) and YT (Yield Tokens).
    PT tokens trade at a discount before maturity and can be redeemed 1:1 for the
    underlying at maturity.

    Args:
        compiler: IntentCompiler instance
        intent: SwapIntent with from_token, to_token, and amount.
                to_token should be a PT token like "PT-wstETH"

    Returns:
        CompilationResult with Pendle swap ActionBundle
    """
    from almanak.framework.connectors.pendle import PendleAdapter, PendleSwapParams
    from almanak.framework.connectors.pendle.sdk import (
        MARKET_BY_PT_TOKEN,
        MARKET_BY_YT_TOKEN,
        MARKET_TOKEN_MINT_SY,
        PT_TOKEN_INFO,
        YT_TOKEN_INFO,
    )

    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[TransactionData] = []

    try:
        # Check chain support
        if compiler.chain not in ("arbitrum", "ethereum", "plasma"):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Pendle is only available on Arbitrum, Ethereum, and Plasma, not {compiler.chain}",
                intent_id=intent.intent_id,
            )

        # Pre-detect PT/YT tokens before resolution
        from_token_name = intent.from_token.upper()
        is_from_pt = from_token_name.startswith("PT-")
        is_from_yt = from_token_name.startswith("YT-")

        # Resolve from token - handle PT/YT tokens specially
        from_token = compiler._resolve_token(intent.from_token)
        if from_token is None and is_from_pt:
            # Try to resolve PT token from Pendle SDK mappings
            pt_info = PT_TOKEN_INFO.get(compiler.chain, {})
            pt_data = pt_info.get(from_token_name) or pt_info.get(intent.from_token)
            if pt_data:
                pt_address, pt_decimals = pt_data
                from_token = TokenInfo(
                    symbol=intent.from_token,
                    address=pt_address,
                    decimals=pt_decimals,
                    is_native=False,
                )
        elif from_token is None and is_from_yt:
            # Try to resolve YT token from Pendle SDK mappings
            yt_info = YT_TOKEN_INFO.get(compiler.chain, {})
            yt_data = yt_info.get(from_token_name) or yt_info.get(intent.from_token)
            if yt_data:
                yt_address, yt_decimals = yt_data
                from_token = TokenInfo(
                    symbol=intent.from_token,
                    address=yt_address,
                    decimals=yt_decimals,
                    is_native=False,
                )

        if from_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown from_token: {intent.from_token}",
                intent_id=intent.intent_id,
            )

        # Calculate input amount
        if intent.amount_usd is not None:
            amount_in = compiler._usd_to_token_amount(intent.amount_usd, from_token)
        elif intent.amount is not None:
            if intent.amount == "all":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
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

        # Pendle adapter accepts either a connected gateway_client (production
        # path) or an RPC URL (local/backtest fallback). Normalize a
        # disconnected gateway_client to None so we fall back cleanly.
        gateway_client = compiler._gateway_client
        if gateway_client is not None and not gateway_client.is_connected:
            gateway_client = None

        rpc_url = None if gateway_client is not None else compiler._get_chain_rpc_url()
        if gateway_client is None and not rpc_url:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Pendle requires either a connected gateway_client or an RPC URL "
                    f"for {compiler.chain}. Configure gateway client or provide rpc_url."
                ),
                intent_id=intent.intent_id,
            )

        # Create Pendle adapter
        adapter = PendleAdapter(
            rpc_url=rpc_url,
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            gateway_client=gateway_client,
        )

        # Determine swap type based on token names
        # PT-*/YT-* prefix means buying/selling PT or YT tokens
        to_token_name = intent.to_token.upper()
        from_token_name = intent.from_token.upper()

        is_buying_pt = to_token_name.startswith("PT-")
        is_selling_pt = from_token_name.startswith("PT-")
        is_buying_yt = to_token_name.startswith("YT-")
        is_selling_yt = from_token_name.startswith("YT-")

        # Guard against invalid PT/YT->PT/YT swaps
        pendle_token_count = sum([is_buying_pt, is_selling_pt, is_buying_yt, is_selling_yt])
        if pendle_token_count > 1:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Pendle swaps do not support direct PT/YT to PT/YT transfers",
                intent_id=intent.intent_id,
            )

        if is_buying_pt:
            swap_type = "token_to_pt"
            pt_markets = MARKET_BY_PT_TOKEN.get(compiler.chain, {})
            market = pt_markets.get(to_token_name) or pt_markets.get(to_token_name.upper())
            if not market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No Pendle market found for {to_token_name} on {compiler.chain}. "
                    f"Available PT tokens: {', '.join(sorted(pt_markets.keys()))}",
                    intent_id=intent.intent_id,
                )
        elif is_selling_pt:
            swap_type = "pt_to_token"
            pt_markets = MARKET_BY_PT_TOKEN.get(compiler.chain, {})
            market = pt_markets.get(from_token_name) or pt_markets.get(from_token_name.upper())
            if not market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No Pendle market found for {from_token_name} on {compiler.chain}. "
                    f"Available PT tokens: {', '.join(sorted(pt_markets.keys()))}",
                    intent_id=intent.intent_id,
                )
        elif is_buying_yt:
            swap_type = "token_to_yt"
            yt_markets = MARKET_BY_YT_TOKEN.get(compiler.chain, {})
            market = yt_markets.get(to_token_name) or yt_markets.get(to_token_name.upper())
            if not market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No Pendle market found for {to_token_name} on {compiler.chain}. "
                    f"Available YT tokens: {', '.join(sorted(yt_markets.keys()))}",
                    intent_id=intent.intent_id,
                )
        elif is_selling_yt:
            swap_type = "yt_to_token"
            yt_markets = MARKET_BY_YT_TOKEN.get(compiler.chain, {})
            market = yt_markets.get(from_token_name) or yt_markets.get(from_token_name.upper())
            if not market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No Pendle market found for {from_token_name} on {compiler.chain}. "
                    f"Available YT tokens: {', '.join(sorted(yt_markets.keys()))}",
                    intent_id=intent.intent_id,
                )
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Pendle swaps require either from_token or to_token to be a PT or YT token "
                "(e.g., PT-wstETH, YT-wstETH)",
                intent_id=intent.intent_id,
            )

        slippage_bps = int(intent.max_slippage * Decimal("10000"))

        # The Pendle SDK methods apply slippage_bps internally on top of min_amount_out.
        # Previously this line also reduced by slippage, causing double-count (VIB-576).
        #
        # For BUY directions (token_to_pt, token_to_yt): PT/YT is cheaper than the
        # underlying, so output >= input. A 1:1 estimate is a safe conservative minimum.
        #
        # For SELL directions (pt_to_token, yt_to_token): PT/YT trades at a DISCOUNT
        # to the underlying (depends on implied yield + time to maturity). Output < input.
        # A 1:1 estimate causes INSUFFICIENT_TOKEN_OUT reverts (VIB-1366).
        #
        # PT holds most of the underlying's value (typically 90-99%), so a 50% haircut
        # is a safe floor even for long-dated maturities.
        # YT represents only the remaining yield and can approach zero near expiry,
        # so we use a 1% floor to avoid reverts on near-maturity YT sells.
        if swap_type == "yt_to_token":
            # YT value decays toward zero near expiry. A fixed 1% floor caused
            # INSUFFICIENT_TOKEN_OUT reverts that TeardownManager slippage
            # escalation could not overcome (VIB-2174).
            #
            # Scale the floor by slippage: at default 200bps use 1% floor,
            # at >=500bps use a minimal floor (1 wei) so the SDK's own
            # slippage_bps reduction is the only protection. This lets
            # TeardownManager escalation actually widen the tolerance.
            if slippage_bps >= 500:
                min_amount_out = 1  # Accept any output; SDK applies slippage_bps on top
                estimation_method = f"minimal floor (high slippage {slippage_bps}bps, YT near-expiry)"
            else:
                min_amount_out = amount_in // 100
                estimation_method = f"1% floor (YT near-expiry safe, slippage {slippage_bps}bps)"
        elif swap_type == "pt_to_token":
            min_amount_out = amount_in // 2
            estimation_method = "50% floor (PT discount safe)"
        else:
            min_amount_out = amount_in
            estimation_method = "1:1 estimate (BUY direction)"

        logger.info(
            f"Pendle slippage params: swap_type={swap_type}, amount_in={amount_in}, "
            f"min_amount_out={min_amount_out}, slippage_bps={slippage_bps}, "
            f"estimation={estimation_method}"
        )

        # Look up the token that mints SY for this market
        # For yield-bearing token markets (like fUSDT0), this is the yield-bearing token
        chain_mint_sy_map = MARKET_TOKEN_MINT_SY.get(compiler.chain, {})
        token_mint_sy = chain_mint_sy_map.get(market.lower())

        # Track original input for pre-flight balance checks (VIB-2533)
        original_from_token = None
        original_amount_in = None

        # ================================================================
        # Pre-swap routing: when tokenIn != tokenMintSy
        # ================================================================
        # When the input token differs from the token that mints SY
        # (e.g., WETH as input but wstETH mints SY), the Pendle router
        # cannot route internally. We insert a Uniswap V3 pre-swap step
        # to convert tokenIn -> tokenMintSy before calling Pendle.
        if token_mint_sy and (is_buying_pt or is_buying_yt) and from_token.address.lower() != token_mint_sy.lower():
            # Resolve tokenMintSy to get its symbol and decimals
            mint_sy_token = compiler._resolve_token(token_mint_sy)
            if mint_sy_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Cannot resolve tokenMintSy address {token_mint_sy} for pre-swap routing on {compiler.chain}. "
                    f"Use the SY-minting token directly as from_token instead.",
                    intent_id=intent.intent_id,
                )

            # Check if a V3-compatible DEX is available on this chain for the pre-swap.
            # Prefer the chain's default protocol (may be a V3 fork like Agni Finance).
            from almanak.framework.connectors.protocol_aliases import display_protocol, is_uniswap_v3_fork

            chain_routers = compiler_constants.PROTOCOL_ROUTERS.get(compiler.chain, {})
            v3_pre_swap_protocol = None
            v3_pre_swap_router = None
            # Prefer compiler.default_protocol if it's a V3 fork on this chain
            if compiler.default_protocol in chain_routers and is_uniswap_v3_fork(compiler.default_protocol):
                v3_pre_swap_protocol = compiler.default_protocol
                v3_pre_swap_router = chain_routers[compiler.default_protocol]
            else:
                for proto_key, router_addr in chain_routers.items():
                    if is_uniswap_v3_fork(proto_key):
                        v3_pre_swap_protocol = proto_key
                        v3_pre_swap_router = router_addr
                        break
            if not v3_pre_swap_router or not v3_pre_swap_protocol:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Pre-swap routing from {from_token.symbol} to {mint_sy_token.symbol} requires "
                    f"a V3-compatible DEX, but none is configured for {compiler.chain}. "
                    f"Use {mint_sy_token.symbol} directly as from_token instead.",
                    intent_id=intent.intent_id,
                )

            # Estimate the pre-swap output using price oracle
            try:
                estimated_mint_sy_output = compiler._calculate_expected_output(amount_in, from_token, mint_sy_token)
            except (ValueError, KeyError, ZeroDivisionError):
                # Fallback: decimal-adjusted 1:1 estimate when price data unavailable.
                # Many Pendle SY mint tokens (aEthPYUSD, sUSDai, USDG) are yield-bearing
                # stablecoin wrappers where 1:1 is a safe conservative estimate.
                # The 2% buffer applied below and Pendle's own slippage protection
                # guard against estimation inaccuracy (VIB-2561).
                from_decimals = from_token.decimals or 6
                to_decimals = mint_sy_token.decimals or 18
                estimated_mint_sy_output = int(
                    Decimal(str(amount_in)) * Decimal(10**to_decimals) / Decimal(10**from_decimals)
                )
                logger.info(
                    f"Pre-swap price fallback: {from_token.symbol} -> {mint_sy_token.symbol}, "
                    f"using 1:1 decimal-adjusted estimate ({amount_in} -> {estimated_mint_sy_output})"
                )

            # Apply 2% safety buffer on the estimated output for the Pendle step.
            # This ensures the Pendle transaction doesn't try to spend more
            # tokenMintSy than the pre-swap actually produces.
            pre_swap_buffer = Decimal("0.98")
            buffered_mint_sy_amount = int(Decimal(str(estimated_mint_sy_output)) * pre_swap_buffer)

            # Handle native ETH: the SwapRouter02 accepts msg.value for native swaps
            actual_from_address = from_token.address
            pre_swap_value = 0
            if from_token.is_native:
                pre_swap_value = amount_in
                weth_address = compiler._get_wrapped_native_address()
                if not weth_address:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Cannot resolve wrapped native token address for {compiler.chain}. "
                        f"Native ETH pre-swap routing requires a configured wrapped native address.",
                        intent_id=intent.intent_id,
                    )
                actual_from_address = weth_address

            # Build approval for V3 DEX router (skip for native token)
            if not from_token.is_native:
                approve_txs = compiler._build_approve_tx(
                    from_token.address,
                    v3_pre_swap_router,
                    amount_in,
                )
                transactions.extend(approve_txs)

            # Build pre-swap calldata via V3-compatible DEX
            from .compiler_adapters import DefaultSwapAdapter

            pre_swap_adapter = DefaultSwapAdapter(
                chain=compiler.chain,
                protocol=v3_pre_swap_protocol,
                pool_selection_mode=compiler._config.swap_pool_selection_mode,
                fixed_fee_tier=compiler._config.fixed_swap_fee_tier,
                rpc_url=compiler._get_chain_rpc_url(),
                rpc_timeout=compiler.rpc_timeout,
            )

            pre_swap_min_out = int(Decimal(str(estimated_mint_sy_output)) * (Decimal("1") - intent.max_slippage))
            # Cap Pendle input to the guaranteed pre-swap minimum.
            # When max_slippage > 2%, the V3 DEX swap may legally return
            # less than the 2%-buffered estimate, so the Pendle step must
            # not try to spend more than the swap guarantees.
            buffered_mint_sy_amount = min(buffered_mint_sy_amount, pre_swap_min_out)

            if buffered_mint_sy_amount <= 0:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Pre-swap routing failed: computed Pendle input amount is {buffered_mint_sy_amount} "
                    f"(max_slippage={intent.max_slippage} too high for pre-swap path). "
                    f"Use {mint_sy_token.symbol} directly as from_token or reduce max_slippage.",
                    intent_id=intent.intent_id,
                )

            deadline = int(datetime.now(UTC).timestamp()) + compiler.default_deadline_seconds

            pre_swap_calldata = pre_swap_adapter.get_swap_calldata(
                from_token=actual_from_address,
                to_token=token_mint_sy,
                amount_in=amount_in,
                min_amount_out=pre_swap_min_out,
                recipient=compiler.wallet_address,
                deadline=deadline,
            )

            pre_swap_tx = TransactionData(
                to=v3_pre_swap_router,
                value=pre_swap_value,
                data="0x" + pre_swap_calldata.hex(),
                gas_estimate=200_000,
                description=f"Pre-swap: {from_token.symbol} -> {mint_sy_token.symbol} via {display_protocol(compiler.chain, v3_pre_swap_protocol)}",
                tx_type="swap",
            )
            transactions.append(pre_swap_tx)

            logger.info(
                f"Pendle pre-swap routing: {from_token.symbol} -> {mint_sy_token.symbol} -> {intent.to_token}, "
                f"estimated output={estimated_mint_sy_output}, using {buffered_mint_sy_amount} "
                f"(capped to min of 2% buffer and {intent.max_slippage:.1%} slippage floor)"
            )

            # Save original input token/amount for pre-flight balance checks (VIB-2533).
            # The orchestrator must verify the wallet holds the *original* input token
            # (e.g., USDC), not the intermediate token produced by the pre-swap (e.g., sUSDe).
            original_from_token = from_token
            original_amount_in = amount_in

            # Override from_token and amount for the Pendle step
            from_token = mint_sy_token
            amount_in = buffered_mint_sy_amount
            token_mint_sy = None  # tokenIn now equals tokenMintSy
            # Don't apply slippage here -- the SDK applies it internally (VIB-576).
            # For sell directions, use discounted estimate (VIB-1366).
            if swap_type == "yt_to_token":
                if slippage_bps >= 500:
                    min_amount_out = 1
                    estimation_method = (
                        f"minimal floor (high slippage {slippage_bps}bps, YT near-expiry, post-pre-swap)"
                    )
                else:
                    min_amount_out = amount_in // 100
                    estimation_method = f"1% floor (YT near-expiry safe, slippage {slippage_bps}bps, post-pre-swap)"
            elif swap_type == "pt_to_token":
                min_amount_out = amount_in // 2
                estimation_method = "50% floor (PT discount safe, post-pre-swap)"
            else:
                min_amount_out = amount_in
                estimation_method = "1:1 estimate (BUY direction, post-pre-swap)"

            logger.info(
                f"Pendle slippage params (post-pre-swap): swap_type={swap_type}, amount_in={amount_in}, "
                f"min_amount_out={min_amount_out}, slippage_bps={slippage_bps}, "
                f"estimation={estimation_method}"
            )

        # Resolve token_out to an address
        # For buying PT/YT, token_out is the PT/YT (use PT_TOKEN_INFO/YT_TOKEN_INFO)
        # For selling PT/YT, token_out is the underlying token (use _resolve_token)
        to_token_name_upper = intent.to_token.upper()
        if to_token_name_upper.startswith("PT-"):
            # Buying PT - resolve PT address
            pt_info = PT_TOKEN_INFO.get(compiler.chain, {})
            pt_data = pt_info.get(to_token_name_upper) or pt_info.get(intent.to_token)
            if pt_data:
                token_out_address = pt_data[0]
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Cannot resolve PT token '{intent.to_token}' - not found in PT_TOKEN_INFO for chain {compiler.chain}",
                    intent_id=intent.intent_id,
                )
        elif to_token_name_upper.startswith("YT-"):
            # Buying YT - resolve YT address
            yt_info = YT_TOKEN_INFO.get(compiler.chain, {})
            yt_data = yt_info.get(to_token_name_upper) or yt_info.get(intent.to_token)
            if yt_data:
                token_out_address = yt_data[0]
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Cannot resolve YT token '{intent.to_token}' - not found in YT_TOKEN_INFO for chain {compiler.chain}",
                    intent_id=intent.intent_id,
                )
        else:
            # Selling PT/YT - resolve underlying token address
            to_token = compiler._resolve_token(intent.to_token)
            if to_token is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Cannot resolve output token '{intent.to_token}' - token not found in registry for chain {compiler.chain}",
                    intent_id=intent.intent_id,
                )
            token_out_address = to_token.address

        # Build swap parameters
        params = PendleSwapParams(
            market=market,
            token_in=from_token.address,
            token_out=token_out_address,
            amount_in=amount_in,
            min_amount_out=min_amount_out,
            receiver=compiler.wallet_address,
            swap_type=swap_type,
            slippage_bps=slippage_bps,
            token_mint_sy=token_mint_sy,
        )

        logger.info(
            f"Compiling Pendle SWAP: {from_token.symbol} -> {intent.to_token}, "
            f"amount={amount_in}, market={market[:10]}..."
        )

        # Build approval transaction if needed
        router_address = adapter.get_router_address()
        if not from_token.is_native:
            approve_txs = compiler._build_approve_tx(
                from_token.address,
                router_address,
                amount_in,
            )
            transactions.extend(approve_txs)

        # Build swap transaction using adapter
        tx_data = adapter.build_swap(params)

        swap_tx = TransactionData(
            to=tx_data.to,
            value=tx_data.value,
            data=tx_data.data,
            gas_estimate=tx_data.gas_estimate,
            description=tx_data.description,
            tx_type="swap",
        )
        transactions.append(swap_tx)

        total_gas = sum(tx.gas_estimate for tx in transactions)

        metadata = {
            "from_token": from_token.to_dict(),
            "to_token": intent.to_token,
            "amount_in": str(amount_in),
            "min_amount_out": str(min_amount_out),
            "slippage": str(intent.max_slippage),
            "protocol": "pendle",
            "market": market,
            "swap_type": swap_type,
        }

        # When a pre-swap was inserted, expose the original input token/amount
        # so the orchestrator's pre-flight balance check validates the token the
        # wallet actually holds (e.g., USDC), not the intermediate token produced
        # by the pre-swap (e.g., sUSDe). See VIB-2533.
        if original_from_token is not None:
            metadata["original_from_token"] = original_from_token.to_dict()
            metadata["original_amount_in"] = str(original_amount_in)

        action_bundle = ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata=metadata,
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas

        logger.info(
            f"Compiled Pendle SWAP intent: {from_token.symbol} -> {intent.to_token}, "
            f"{len(transactions)} txs, {total_gas} gas"
        )

    except Exception:
        logger.exception("Failed to compile Pendle SWAP intent")
        result.status = CompilationStatus.FAILED
        result.error = "Pendle SWAP compilation failed"

    return result


def compile_pendle_lp_open(compiler, intent: LPOpenIntent) -> CompilationResult:
    """Compile LP_OPEN intent for Pendle Protocol (single-token liquidity).

    Adds liquidity to a Pendle market using a single input token.
    The router handles splitting into SY and PT.

    Args:
        compiler: IntentCompiler instance
        intent: LPOpenIntent with pool (market address), token, and amount

    Returns:
        CompilationResult with Pendle LP open ActionBundle
    """
    from almanak.framework.connectors.pendle import PendleAdapter
    from almanak.framework.connectors.pendle.sdk import MARKET_BY_PT_TOKEN

    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[TransactionData] = []

    try:
        if compiler.chain not in ("arbitrum", "ethereum", "plasma"):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Pendle LP not available on {compiler.chain}",
                intent_id=intent.intent_id,
            )

        # Pool format for Pendle: "TOKEN/0xmarket_address" or "TOKEN/PT-name"
        # Parse token symbol and market from pool field
        pool_str = intent.pool or ""
        if "/" in pool_str:
            parts = pool_str.split("/", 1)
            token_symbol = parts[0].strip()
            market_part = parts[1].strip()
        elif pool_str.startswith("0x"):
            # Bare market address -- no token specified
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Pendle LP pool must be 'TOKEN/0xmarket_address' format. Got: {pool_str}",
                intent_id=intent.intent_id,
            )
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Invalid Pendle pool format: {pool_str}. Expected: TOKEN/0xmarket_address",
                intent_id=intent.intent_id,
            )

        token = compiler._resolve_token(token_symbol)
        if token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {token_symbol}",
                intent_id=intent.intent_id,
            )

        # Resolve market address
        market = market_part
        if not market.startswith("0x"):
            pt_markets = MARKET_BY_PT_TOKEN.get(compiler.chain, {})
            found_market = pt_markets.get(market, None)
            if found_market:
                market = found_market
            else:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid Pendle market: {market}. Must be a 0x address or known PT token name.",
                    intent_id=intent.intent_id,
                )

        # Use amount0 as deposit amount (single-sided LP)
        amount_decimal: Decimal = intent.amount0
        if amount_decimal <= 0:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="amount0 must be positive for Pendle LP",
                intent_id=intent.intent_id,
            )
        amount_in = int(amount_decimal * Decimal(10**token.decimals))

        # Default slippage (LPOpenIntent has no max_slippage field)
        slippage_bps = 50
        min_lp_out = 0  # Pendle LP minting: use adapter to estimate proper min

        rpc_url = compiler._get_chain_rpc_url()
        if not rpc_url:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"RPC URL not available for {compiler.chain}",
                intent_id=intent.intent_id,
            )

        adapter = PendleAdapter(
            rpc_url=rpc_url,
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            gateway_client=compiler._gateway_client,
        )

        # Build approval
        router_address = adapter.get_router_address()
        if not token.is_native:
            approve_txs = compiler._build_approve_tx(token.address, router_address, amount_in)
            transactions.extend(approve_txs)

        # Build add liquidity TX
        from almanak.framework.connectors.pendle import PendleLPParams

        lp_params = PendleLPParams(
            market=market,
            token=token.address,
            amount=amount_in,
            min_amount=min_lp_out,
            receiver=compiler.wallet_address,
            operation="add",
            slippage_bps=slippage_bps,
        )
        tx_data = adapter.build_add_liquidity(lp_params)

        lp_tx = TransactionData(
            to=tx_data.to,
            value=tx_data.value,
            data=tx_data.data,
            gas_estimate=tx_data.gas_estimate,
            description=tx_data.description,
            tx_type="lp_open",
        )
        transactions.append(lp_tx)

        total_gas = sum(tx.gas_estimate for tx in transactions)
        action_bundle = ActionBundle(
            intent_type=IntentType.LP_OPEN.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "protocol": "pendle",
                "market": market,
                "token": token.to_dict(),
                "amount_in": str(amount_in),
                "min_lp_out": str(min_lp_out),
                "chain": compiler.chain,
            },
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas

        logger.info(f"Compiled Pendle LP_OPEN: {token.symbol} -> market {market[:10]}..., {len(transactions)} txs")

    except Exception:
        logger.exception("Failed to compile Pendle LP_OPEN intent")
        result.status = CompilationStatus.FAILED
        result.error = "Pendle LP_OPEN compilation failed"

    return result


def compile_pendle_lp_close(compiler, intent: LPCloseIntent) -> CompilationResult:
    """Compile LP_CLOSE intent for Pendle Protocol.

    Removes liquidity from a Pendle market to a single output token.

    Args:
        compiler: IntentCompiler instance
        intent: LPCloseIntent with pool (market address), position_id (LP amount), token

    Returns:
        CompilationResult with Pendle LP close ActionBundle
    """
    from almanak.framework.connectors.pendle import PendleAdapter, PendleLPParams

    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[TransactionData] = []

    try:
        if compiler.chain not in ("arbitrum", "ethereum", "plasma"):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Pendle LP not available on {compiler.chain}",
                intent_id=intent.intent_id,
            )

        # Resolve output token (LPCloseIntent has no dedicated token field)
        out_token_name: str = getattr(intent, "token_a", None) or getattr(intent, "token", None) or ""
        if not out_token_name:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Pendle LP close requires an output token. Specify via intent metadata.",
                intent_id=intent.intent_id,
            )
        out_token = compiler._resolve_token(out_token_name)
        if out_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown output token: {out_token_name}",
                intent_id=intent.intent_id,
            )

        market = intent.pool
        if not market or not market.startswith("0x"):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Invalid Pendle market address: {intent.pool}",
                intent_id=intent.intent_id,
            )

        # LP amount comes from position_id (the LP token amount in wei)
        try:
            lp_amount = int(intent.position_id)
        except (ValueError, TypeError):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Invalid LP amount (position_id): {intent.position_id}. Must be LP token amount in wei.",
                intent_id=intent.intent_id,
            )

        # Default slippage (LPCloseIntent has no max_slippage field)
        slippage_bps = 50
        min_token_out = 0  # Pendle LP removal: use adapter to estimate proper min

        rpc_url = compiler._get_chain_rpc_url()
        if not rpc_url:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"RPC URL not available for {compiler.chain}",
                intent_id=intent.intent_id,
            )

        adapter = PendleAdapter(
            rpc_url=rpc_url,
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            gateway_client=compiler._gateway_client,
        )

        # Build approval for LP token (market address IS the LP token)
        approve_txs = compiler._build_approve_tx(market, adapter.get_router_address(), lp_amount)
        transactions.extend(approve_txs)

        # Build remove liquidity TX
        lp_params = PendleLPParams(
            market=market,
            token=out_token.address,
            amount=lp_amount,
            min_amount=min_token_out,
            receiver=compiler.wallet_address,
            operation="remove",
            slippage_bps=slippage_bps,
        )
        tx_data = adapter.build_remove_liquidity(lp_params)

        remove_tx = TransactionData(
            to=tx_data.to,
            value=tx_data.value,
            data=tx_data.data,
            gas_estimate=tx_data.gas_estimate,
            description=tx_data.description,
            tx_type="lp_close",
        )
        transactions.append(remove_tx)

        total_gas = sum(tx.gas_estimate for tx in transactions)
        action_bundle = ActionBundle(
            intent_type=IntentType.LP_CLOSE.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "protocol": "pendle",
                "market": market,
                "out_token": out_token.to_dict(),
                "lp_amount": str(lp_amount),
                "min_token_out": str(min_token_out),
                "chain": compiler.chain,
            },
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas

        logger.info(f"Compiled Pendle LP_CLOSE: market {market[:10]}..., {len(transactions)} txs")

    except Exception:
        logger.exception("Failed to compile Pendle LP_CLOSE intent")
        result.status = CompilationStatus.FAILED
        result.error = "Pendle LP_CLOSE compilation failed"

    return result


def compile_pendle_redeem(compiler, intent: WithdrawIntent) -> CompilationResult:
    """Compile WITHDRAW intent as Pendle PT+YT redemption.

    Redeems PT+YT to the underlying token via Pendle's redeemPyToToken.

    Args:
        compiler: IntentCompiler instance
        intent: WithdrawIntent with token (underlying), amount, and optionally market_id (YT address)

    Returns:
        CompilationResult with Pendle redeem ActionBundle
    """
    from almanak.framework.connectors.pendle import PendleAdapter, PendleRedeemParams

    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[TransactionData] = []

    try:
        if compiler.chain not in ("arbitrum", "ethereum", "plasma"):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Pendle redeem not available on {compiler.chain}",
                intent_id=intent.intent_id,
            )

        # Resolve output token
        out_token = compiler._resolve_token(intent.token)
        if out_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {intent.token}",
                intent_id=intent.intent_id,
            )

        # YT address comes from market_id field
        yt_address = intent.market_id
        if not yt_address:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="market_id (YT address) is required for Pendle redeem. Set intent.market_id to the YT contract address.",
                intent_id=intent.intent_id,
            )

        # Calculate amount
        if intent.amount == "all":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="amount='all' must be resolved before compilation for Pendle redeem",
                intent_id=intent.intent_id,
            )
        amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
        # PT/YT tokens are always 18 decimals on Pendle
        py_decimals = 18
        py_amount = int(amount_decimal * Decimal(10**py_decimals))

        slippage_bps = 50
        min_token_out = 0  # Pendle redeem: use adapter to estimate proper min

        rpc_url = compiler._get_chain_rpc_url()
        if not rpc_url:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"RPC URL not available for {compiler.chain}",
                intent_id=intent.intent_id,
            )

        adapter = PendleAdapter(
            rpc_url=rpc_url,
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            gateway_client=compiler._gateway_client,
        )

        # Build redeem TX
        redeem_params = PendleRedeemParams(
            yt_address=yt_address,
            py_amount=py_amount,
            token_out=out_token.address,
            min_token_out=min_token_out,
            receiver=compiler.wallet_address,
            slippage_bps=slippage_bps,
        )
        tx_data = adapter.build_redeem(redeem_params)

        redeem_tx = TransactionData(
            to=tx_data.to,
            value=tx_data.value,
            data=tx_data.data,
            gas_estimate=tx_data.gas_estimate,
            description=tx_data.description,
            tx_type="redeem",
        )
        transactions.append(redeem_tx)

        total_gas = sum(tx.gas_estimate for tx in transactions)
        action_bundle = ActionBundle(
            intent_type=IntentType.WITHDRAW.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "protocol": "pendle",
                "yt_address": yt_address,
                "out_token": out_token.to_dict(),
                "py_amount": str(py_amount),
                "min_token_out": str(min_token_out),
                "chain": compiler.chain,
            },
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas

        logger.info(f"Compiled Pendle REDEEM: {out_token.symbol}, {len(transactions)} txs")

    except Exception:
        logger.exception("Failed to compile Pendle REDEEM intent")
        result.status = CompilationStatus.FAILED
        result.error = "Pendle REDEEM compilation failed"

    return result
