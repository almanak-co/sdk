"""Connector-owned compiler for Pendle intents.

These standalone functions receive the compiler instance as their first
parameter and implement all Pendle-related compilation logic (swap, LP open,
LP close, redeem).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.intents import compiler_constants
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo, TransactionData
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.models.reproduction_bundle import ActionBundle

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent, SwapIntent, WithdrawIntent

logger = logging.getLogger("almanak.framework.intents.compiler")


class PendleCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compiler for Pendle swap and LP intents.

    Pendle is its own primitive — PT/YT/SY tokens with a custom AMM — and is
    NOT concentrated liquidity. The pre-swap V3 leg (when paying with a
    non-Pendle base token) gets its ``DefaultSwapAdapter`` via
    ``ctx.services.default_swap_adapter(protocol)`` rather than the
    CL-specific factory fields.
    """

    protocols: ClassVar[frozenset[str]] = frozenset({"pendle"})
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {
            IntentType.SWAP,
            IntentType.LP_OPEN,
            IntentType.LP_CLOSE,
            IntentType.WITHDRAW,
        }
    )
    chains: ClassVar[frozenset[str]] = frozenset({"arbitrum", "ethereum"})
    context_type: ClassVar[type[BaseCompilerContext]] = BaseCompilerContext

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
        if intent_type == IntentType.WITHDRAW:
            return self.compile_withdraw(ctx, intent)
        if intent_type == IntentType.LP_COLLECT_FEES:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                intent_id=getattr(intent, "intent_id", ""),
                error="Pendle does not support LP_COLLECT_FEES compilation.",
            )
        return self._unsupported(intent)

    def compile_swap(self, ctx: BaseCompilerContext, intent: SwapIntent) -> CompilationResult:
        return compile_pendle_swap(_PendleCompileImpl(ctx), intent)

    def compile_lp_open(self, ctx: BaseCompilerContext, intent: LPOpenIntent) -> CompilationResult:
        return compile_pendle_lp_open(_PendleCompileImpl(ctx), intent)

    def compile_lp_close(self, ctx: BaseCompilerContext, intent: LPCloseIntent) -> CompilationResult:
        return compile_pendle_lp_close(_PendleCompileImpl(ctx), intent)

    def compile_withdraw(self, ctx: BaseCompilerContext, intent: WithdrawIntent) -> CompilationResult:
        return compile_pendle_redeem(_PendleCompileImpl(ctx), intent)


class _PendleCompileImpl:
    """Per-call adapter exposing framework services to relocated Pendle functions."""

    def __init__(self, ctx: BaseCompilerContext) -> None:
        self._ctx = ctx
        self.chain = ctx.chain
        self.wallet_address = ctx.wallet_address
        self.rpc_timeout = ctx.rpc_timeout
        self.price_oracle = ctx.price_oracle
        self.default_protocol = ctx.default_protocol
        self.default_deadline_seconds = ctx.default_deadline_seconds
        self._gateway_client = ctx.gateway_client
        self._token_resolver = ctx.token_resolver

    def _get_chain_rpc_url(self) -> str | None:
        return self._ctx.rpc_url

    def _resolve_token(self, token: str) -> TokenInfo | None:
        return self._ctx.services.resolve_token(token)

    def _usd_to_token_amount(self, usd_amount: Decimal, token: TokenInfo) -> int:
        return self._ctx.services.usd_to_token_amount(usd_amount, token)

    def _calculate_expected_output(self, amount_in: int, from_token: TokenInfo, to_token: TokenInfo) -> int:
        return self._ctx.services.calculate_expected_output(amount_in, from_token, to_token)

    def _build_approve_tx(self, token_address: str, spender: str, amount: int) -> list[TransactionData]:
        return self._ctx.services.build_approve_tx(token_address, spender, amount)

    def _get_wrapped_native_address(self) -> str | None:
        return self._ctx.services.get_wrapped_native_address()


def _resolve_pt_from_yt(adapter: Any, yt_address: str) -> str | None:
    """Return the PT address for a given YT contract via the on-chain YT.PT() call.

    Uses the adapter's existing web3 instance so no additional RPC connection is
    opened. Returns None on failure (approval is then skipped and execution will
    revert with a clearer error from the router).
    """
    from web3 import Web3

    try:
        w3 = adapter.sdk.web3
        selector = w3.keccak(text="PT()")[:4]
        result = w3.eth.call(
            {
                "to": Web3.to_checksum_address(yt_address),
                "data": "0x" + selector.hex(),
            }
        )
        # ABI-encoded address: rightmost 20 bytes of the 32-byte return value
        return Web3.to_checksum_address("0x" + result[-20:].hex())
    except Exception as e:
        logger.warning(f"_resolve_pt_from_yt({yt_address}): {e}")
        return None


def _failed(intent_id: str, error: str) -> CompilationResult:
    """Build a FAILED CompilationResult with the given error message."""
    return CompilationResult(status=CompilationStatus.FAILED, error=error, intent_id=intent_id)


def _resolve_pendle_from_token(compiler, intent: SwapIntent) -> TokenInfo | None:
    """Resolve the SWAP from_token, falling back to PT/YT static info dicts.

    Returns ``None`` when the token cannot be resolved by any path.
    """
    from almanak.framework.connectors.pendle.sdk import PT_TOKEN_INFO, YT_TOKEN_INFO

    from_token = compiler._resolve_token(intent.from_token)
    if from_token is not None:
        return from_token

    from_token_name = intent.from_token.upper()
    if from_token_name.startswith("PT-"):
        pt_info = PT_TOKEN_INFO.get(compiler.chain, {})
        pt_data = pt_info.get(from_token_name) or pt_info.get(intent.from_token)
        if pt_data:
            pt_address, pt_decimals = pt_data
            return TokenInfo(symbol=intent.from_token, address=pt_address, decimals=pt_decimals, is_native=False)
    elif from_token_name.startswith("YT-"):
        yt_info = YT_TOKEN_INFO.get(compiler.chain, {})
        yt_data = yt_info.get(from_token_name) or yt_info.get(intent.from_token)
        if yt_data:
            yt_address, yt_decimals = yt_data
            return TokenInfo(symbol=intent.from_token, address=yt_address, decimals=yt_decimals, is_native=False)
    return None


def _compute_pendle_amount_in(intent: SwapIntent, compiler, from_token: TokenInfo) -> int | CompilationResult:
    """Convert intent amount/amount_usd to a wei integer; return FAILED result on error."""
    if intent.amount_usd is not None:
        return compiler._usd_to_token_amount(intent.amount_usd, from_token)
    if intent.amount is not None:
        if intent.amount == "all":
            return _failed(
                intent.intent_id,
                "amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
            )
        amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
        return int(amount_decimal * Decimal(10**from_token.decimals))
    return _failed(intent.intent_id, "Either amount_usd or amount must be provided")


def _resolve_pendle_adapter_inputs(compiler, intent_id: str) -> tuple[Any, str | None] | CompilationResult:
    """Pick gateway_client + rpc_url for the Pendle adapter.

    Pendle adapter accepts either a connected gateway_client (production path) or an RPC URL
    (local/backtest fallback). A disconnected gateway_client is normalized to None.
    """
    gateway_client = compiler._gateway_client
    if gateway_client is not None and not gateway_client.is_connected:
        gateway_client = None

    rpc_url = None if gateway_client is not None else compiler._get_chain_rpc_url()
    if gateway_client is None and not rpc_url:
        return _failed(
            intent_id,
            f"Pendle requires either a connected gateway_client or an RPC URL "
            f"for {compiler.chain}. Configure gateway client or provide rpc_url.",
        )
    return gateway_client, rpc_url


def _classify_pendle_swap_type(intent: SwapIntent) -> tuple[str, str] | CompilationResult:
    """Return (swap_type, side) where side is one of {buying_pt, selling_pt, buying_yt, selling_yt}."""
    to_name = intent.to_token.upper()
    from_name = intent.from_token.upper()

    flags = {
        "buying_pt": to_name.startswith("PT-"),
        "selling_pt": from_name.startswith("PT-"),
        "buying_yt": to_name.startswith("YT-"),
        "selling_yt": from_name.startswith("YT-"),
    }
    if sum(flags.values()) > 1:
        return _failed(intent.intent_id, "Pendle swaps do not support direct PT/YT to PT/YT transfers")

    if flags["buying_pt"]:
        return "token_to_pt", "buying_pt"
    if flags["selling_pt"]:
        return "pt_to_token", "selling_pt"
    if flags["buying_yt"]:
        return "token_to_yt", "buying_yt"
    if flags["selling_yt"]:
        return "yt_to_token", "selling_yt"
    return _failed(
        intent.intent_id,
        "Pendle swaps require either from_token or to_token to be a PT or YT token (e.g., PT-wstETH, YT-wstETH)",
    )


def _resolve_pendle_market(intent: SwapIntent, compiler, side: str) -> str | CompilationResult:
    """Look up the Pendle market address for the given swap side."""
    from almanak.framework.connectors.pendle.sdk import MARKET_BY_PT_TOKEN, MARKET_BY_YT_TOKEN

    is_pt = side in ("buying_pt", "selling_pt")
    is_buy = side in ("buying_pt", "buying_yt")
    token_name = (intent.to_token if is_buy else intent.from_token).upper()
    markets = (MARKET_BY_PT_TOKEN if is_pt else MARKET_BY_YT_TOKEN).get(compiler.chain, {})
    market = markets.get(token_name) or markets.get(token_name.upper())
    if market:
        return market
    label = "PT" if is_pt else "YT"
    return _failed(
        intent.intent_id,
        f"No Pendle market found for {token_name} on {compiler.chain}. "
        f"Available {label} tokens: {', '.join(sorted(markets.keys()))}",
    )


def _compute_pendle_min_out(
    swap_type: str, amount_in: int, slippage_bps: int, post_preswap: bool = False
) -> tuple[int, str]:
    """Compute (min_amount_out, estimation_method) for the Pendle step.

    The Pendle SDK methods apply slippage_bps internally on top of min_amount_out;
    do NOT pre-apply slippage here (VIB-576).

    BUY (token_to_pt/yt): PT/YT is cheaper than the underlying so a 1:1 estimate is a safe minimum.
    SELL (pt_to_token): PT trades at a discount; use a 50% floor (VIB-1366).
    SELL (yt_to_token): YT decays toward zero near expiry. Scale the floor by slippage so
        TeardownManager escalation can actually widen tolerance (VIB-2174):
        - <500bps: 1% floor
        - >=500bps: 1 wei floor (SDK's slippage_bps is the only protection).
    """
    suffix = ", post-pre-swap" if post_preswap else ""
    if swap_type == "yt_to_token":
        if slippage_bps >= 500:
            return 1, f"minimal floor (high slippage {slippage_bps}bps, YT near-expiry{suffix})"
        return amount_in // 100, f"1% floor (YT near-expiry safe, slippage {slippage_bps}bps{suffix})"
    if swap_type == "pt_to_token":
        return amount_in // 2, f"50% floor (PT discount safe{suffix})"
    return amount_in, f"1:1 estimate (BUY direction{suffix})"


def _select_v3_pre_swap_router(compiler) -> tuple[str | None, str | None]:
    """Return (protocol_key, router_address) of a V3-compatible DEX on this chain, or (None, None)."""
    from almanak.framework.connectors.protocol_aliases import is_uniswap_v3_fork

    chain_routers = compiler_constants.PROTOCOL_ROUTERS.get(compiler.chain, {})
    # Prefer compiler.default_protocol if it's a V3 fork on this chain
    if compiler.default_protocol in chain_routers and is_uniswap_v3_fork(compiler.default_protocol):
        return compiler.default_protocol, chain_routers[compiler.default_protocol]
    for proto_key, router_addr in chain_routers.items():
        if is_uniswap_v3_fork(proto_key):
            return proto_key, router_addr
    return None, None


def _estimate_pre_swap_output(compiler, amount_in: int, from_token: TokenInfo, mint_sy_token: TokenInfo) -> int:
    """Estimate tokenIn -> tokenMintSy output, falling back to 1:1 decimal-adjusted (VIB-2561)."""
    try:
        return compiler._calculate_expected_output(amount_in, from_token, mint_sy_token)
    except (ValueError, KeyError, ZeroDivisionError):
        # Many Pendle SY mint tokens (aEthPYUSD, sUSDai, USDG) are yield-bearing stablecoin
        # wrappers where 1:1 is a safe conservative estimate. The 2% buffer applied by
        # callers and Pendle's own slippage protection guard against estimation inaccuracy.
        from_decimals = from_token.decimals or 6
        to_decimals = mint_sy_token.decimals or 18
        estimated = int(Decimal(str(amount_in)) * Decimal(10**to_decimals) / Decimal(10**from_decimals))
        logger.info(
            f"Pre-swap price fallback: {from_token.symbol} -> {mint_sy_token.symbol}, "
            f"using 1:1 decimal-adjusted estimate ({amount_in} -> {estimated})"
        )
        return estimated


def _build_pre_swap_tx(
    compiler,
    intent: SwapIntent,
    from_token: TokenInfo,
    mint_sy_token: TokenInfo,
    token_mint_sy: str,
    amount_in: int,
    estimated_mint_sy_output: int,
    v3_protocol: str,
    v3_router: str,
) -> tuple[list[TransactionData], int, TransactionData] | CompilationResult:
    """Build the V3 pre-swap approval(s) + swap tx and return (approvals, buffered_pendle_input, tx)."""
    from almanak.framework.connectors.protocol_aliases import display_protocol

    # Apply 2% safety buffer on the estimated output for the Pendle step. This ensures
    # the Pendle transaction doesn't try to spend more tokenMintSy than the pre-swap
    # actually produces.
    buffered_mint_sy_amount = int(Decimal(str(estimated_mint_sy_output)) * Decimal("0.98"))

    # Handle native ETH: SwapRouter02 accepts msg.value for native swaps
    actual_from_address = from_token.address
    pre_swap_value = 0
    if from_token.is_native:
        pre_swap_value = amount_in
        weth_address = compiler._get_wrapped_native_address()
        if not weth_address:
            return _failed(
                intent.intent_id,
                f"Cannot resolve wrapped native token address for {compiler.chain}. "
                f"Native ETH pre-swap routing requires a configured wrapped native address.",
            )
        actual_from_address = weth_address

    approvals: list[TransactionData] = []
    if not from_token.is_native:
        approvals.extend(compiler._build_approve_tx(from_token.address, v3_router, amount_in))

    pre_swap_adapter = compiler._ctx.services.default_swap_adapter(v3_protocol)

    pre_swap_min_out = int(Decimal(str(estimated_mint_sy_output)) * (Decimal("1") - intent.max_slippage))
    # Cap Pendle input to the guaranteed pre-swap minimum. When max_slippage > 2%, the V3
    # DEX swap may legally return less than the 2%-buffered estimate, so the Pendle step
    # must not try to spend more than the swap guarantees.
    buffered_mint_sy_amount = min(buffered_mint_sy_amount, pre_swap_min_out)
    if buffered_mint_sy_amount <= 0:
        return _failed(
            intent.intent_id,
            f"Pre-swap routing failed: computed Pendle input amount is {buffered_mint_sy_amount} "
            f"(max_slippage={intent.max_slippage} too high for pre-swap path). "
            f"Use {mint_sy_token.symbol} directly as from_token or reduce max_slippage.",
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
        to=v3_router,
        value=pre_swap_value,
        data="0x" + pre_swap_calldata.hex(),
        gas_estimate=200_000,
        description=f"Pre-swap: {from_token.symbol} -> {mint_sy_token.symbol} via {display_protocol(compiler.chain, v3_protocol)}",
        tx_type="swap",
    )
    logger.info(
        f"Pendle pre-swap routing: {from_token.symbol} -> {mint_sy_token.symbol} -> {intent.to_token}, "
        f"estimated output={estimated_mint_sy_output}, using {buffered_mint_sy_amount} "
        f"(capped to min of 2% buffer and {intent.max_slippage:.1%} slippage floor)"
    )
    return approvals, buffered_mint_sy_amount, pre_swap_tx


def _apply_pendle_pre_swap_routing(
    compiler,
    intent: SwapIntent,
    from_token: TokenInfo,
    amount_in: int,
    token_mint_sy: str,
) -> tuple[list[TransactionData], TokenInfo, int, TokenInfo, int] | CompilationResult:
    """Build the Uniswap V3 pre-swap that converts tokenIn -> tokenMintSy.

    When the input token differs from the token that mints SY (e.g., WETH input but wstETH
    mints SY), the Pendle router cannot route internally — we insert a V3 pre-swap step.

    Returns ``(transactions, new_from_token, new_amount_in, original_from_token, original_amount_in)``.
    """
    mint_sy_token = compiler._resolve_token(token_mint_sy)
    if mint_sy_token is None:
        return _failed(
            intent.intent_id,
            f"Cannot resolve tokenMintSy address {token_mint_sy} for pre-swap routing on {compiler.chain}. "
            f"Use the SY-minting token directly as from_token instead.",
        )

    v3_protocol, v3_router = _select_v3_pre_swap_router(compiler)
    if not v3_router or not v3_protocol:
        return _failed(
            intent.intent_id,
            f"Pre-swap routing from {from_token.symbol} to {mint_sy_token.symbol} requires "
            f"a V3-compatible DEX, but none is configured for {compiler.chain}. "
            f"Use {mint_sy_token.symbol} directly as from_token instead.",
        )

    estimated_output = _estimate_pre_swap_output(compiler, amount_in, from_token, mint_sy_token)

    built = _build_pre_swap_tx(
        compiler, intent, from_token, mint_sy_token, token_mint_sy, amount_in, estimated_output, v3_protocol, v3_router
    )
    if isinstance(built, CompilationResult):
        return built
    approvals, buffered_pendle_input, pre_swap_tx = built

    return [*approvals, pre_swap_tx], mint_sy_token, buffered_pendle_input, from_token, amount_in


def _resolve_pendle_token_out(intent: SwapIntent, compiler) -> tuple[str, int | None] | CompilationResult:
    """Resolve (token_out_address, to_token_decimals) for the swap output.

    For BUY directions the out token is a PT/YT looked up in the static info dicts; for SELL
    directions it is the underlying resolved via ``compiler._resolve_token``.
    """
    from almanak.framework.connectors.pendle.sdk import PT_TOKEN_INFO, YT_TOKEN_INFO

    to_name_upper = intent.to_token.upper()
    if to_name_upper.startswith("PT-"):
        pt_info = PT_TOKEN_INFO.get(compiler.chain, {})
        pt_data = pt_info.get(to_name_upper) or pt_info.get(intent.to_token)
        if not pt_data:
            return _failed(
                intent.intent_id,
                f"Cannot resolve PT token '{intent.to_token}' - not found in PT_TOKEN_INFO for chain {compiler.chain}",
            )
        return pt_data[0], pt_data[1]
    if to_name_upper.startswith("YT-"):
        yt_info = YT_TOKEN_INFO.get(compiler.chain, {})
        yt_data = yt_info.get(to_name_upper) or yt_info.get(intent.to_token)
        if not yt_data:
            return _failed(
                intent.intent_id,
                f"Cannot resolve YT token '{intent.to_token}' - not found in YT_TOKEN_INFO for chain {compiler.chain}",
            )
        return yt_data[0], yt_data[1]
    # Selling PT/YT — out token is the underlying.
    to_token = compiler._resolve_token(intent.to_token)
    if to_token is None:
        return _failed(
            intent.intent_id,
            f"Cannot resolve output token '{intent.to_token}' - token not found in registry for chain {compiler.chain}",
        )
    return to_token.address, to_token.decimals


def _check_pendle_chain_supported(compiler, intent_id: str, label: str) -> CompilationResult | None:
    """Return a FAILED CompilationResult when chain is not Pendle-supported, else None."""
    if compiler.chain in ("arbitrum", "ethereum", "plasma"):
        return None
    return _failed(intent_id, f"{label} on {compiler.chain}")


def _parse_pendle_lp_open_pool(pool_str: str, intent_id: str) -> tuple[str, str] | CompilationResult:
    """Split LPOpen pool field 'TOKEN/0xmarket_or_PT_name' into (token_symbol, market_part)."""
    if "/" in pool_str:
        token_symbol, market_part = (p.strip() for p in pool_str.split("/", 1))
        return token_symbol, market_part
    if pool_str.startswith("0x"):
        return _failed(intent_id, f"Pendle LP pool must be 'TOKEN/0xmarket_address' format. Got: {pool_str}")
    return _failed(intent_id, f"Invalid Pendle pool format: {pool_str}. Expected: TOKEN/0xmarket_address")


def _resolve_pendle_lp_open_market(compiler, market_part: str, intent_id: str) -> str | CompilationResult:
    """Resolve a Pendle LP_OPEN market: 0x address pass-through or PT-name lookup."""
    from almanak.framework.connectors.pendle.sdk import MARKET_BY_PT_TOKEN

    if market_part.startswith("0x"):
        return market_part
    pt_markets = MARKET_BY_PT_TOKEN.get(compiler.chain, {})
    found_market = pt_markets.get(market_part) or pt_markets.get(market_part.upper())
    if found_market:
        return found_market
    return _failed(
        intent_id,
        f"Invalid Pendle market: {market_part}. Must be a 0x address or known PT token name.",
    )


def _compute_pendle_lp_open_amount(intent: LPOpenIntent, token: TokenInfo) -> int | CompilationResult:
    """Convert ``intent.amount0`` (token terms) to a wei integer."""
    amount_decimal: Decimal = intent.amount0
    if amount_decimal <= 0:
        return _failed(intent.intent_id, "amount0 must be positive for Pendle LP")
    return int(amount_decimal * Decimal(10**token.decimals))


def _resolve_pendle_lp_open_inputs(compiler, intent: LPOpenIntent) -> tuple[TokenInfo, str, int] | CompilationResult:
    """Parse LP_OPEN pool, resolve token + market, and compute the wei deposit amount."""
    parsed = _parse_pendle_lp_open_pool(intent.pool or "", intent.intent_id)
    if isinstance(parsed, CompilationResult):
        return parsed
    token_symbol, market_part = parsed

    token = compiler._resolve_token(token_symbol)
    if token is None:
        return _failed(intent.intent_id, f"Unknown token: {token_symbol}")

    market_or_err = _resolve_pendle_lp_open_market(compiler, market_part, intent.intent_id)
    if isinstance(market_or_err, CompilationResult):
        return market_or_err

    amount_or_err = _compute_pendle_lp_open_amount(intent, token)
    if isinstance(amount_or_err, CompilationResult):
        return amount_or_err
    return token, market_or_err, amount_or_err


def _resolve_pendle_lp_close_out_token(compiler, intent: LPCloseIntent) -> TokenInfo | CompilationResult:
    """Resolve LPClose output token from protocol_params (canonical) or legacy attrs."""
    params = getattr(intent, "protocol_params", None) or {}
    out_token_name: str = (
        params.get("token")
        or params.get("token_out")
        or getattr(intent, "token_a", None)
        or getattr(intent, "token", None)
        or ""
    )
    if not out_token_name:
        return _failed(intent.intent_id, "Pendle LP close requires an output token. Specify via intent metadata.")
    out_token = compiler._resolve_token(out_token_name)
    if out_token is None:
        return _failed(intent.intent_id, f"Unknown output token: {out_token_name}")
    return out_token


def _parse_pendle_lp_close_amount(intent: LPCloseIntent) -> int | CompilationResult:
    """Parse LP token amount from ``intent.position_id`` (wei integer)."""
    try:
        return int(intent.position_id)
    except (ValueError, TypeError):
        return _failed(
            intent.intent_id,
            f"Invalid LP amount (position_id): {intent.position_id}. Must be LP token amount in wei.",
        )


def _resolve_pendle_redeem_pt_address(compiler, adapter: Any, yt_address: str) -> str | None:
    """Resolve PT address for a YT — static reverse-lookup with on-chain fallback."""
    from almanak.framework.connectors.pendle.sdk import PT_TOKEN_INFO as _PT_TOKEN_INFO
    from almanak.framework.connectors.pendle.sdk import YT_TOKEN_INFO as _YT_TOKEN_INFO

    yt_addr_lower = yt_address.lower()
    for _pt_name, (_pt_addr, _) in _PT_TOKEN_INFO.get(compiler.chain, {}).items():
        _yt_name = _pt_name.replace("PT-", "YT-", 1)
        _yt_entry = _YT_TOKEN_INFO.get(compiler.chain, {}).get(_yt_name)
        if _yt_entry and _yt_entry[0].lower() == yt_addr_lower:
            logger.debug("compile_pendle_redeem: resolved PT %s via static config for YT %s", _pt_addr, yt_address)
            return _pt_addr
    return _resolve_pt_from_yt(adapter, yt_address)


def _build_pendle_redeem_pt_approval(pt_address: str, router_address: str) -> TransactionData:
    """Build an unconditional infinite-approve TX for the PT token to the Pendle router.

    Unconditional infinite approve — ``_build_approve_tx`` skips txs when the simulated
    allowance already seems sufficient, but Anvil simulates each tx in isolation (without
    prior txs' state changes), which can cause it to see allowance=0 for the redeem and
    flag it as broken. Building the approve calldata directly avoids this ordering
    sensitivity.
    """
    from web3 import Web3

    from almanak.framework.intents.compiler_constants import MAX_UINT256

    approve_data = (
        "0x095ea7b3" + Web3.to_checksum_address(router_address)[2:].lower().zfill(64) + hex(MAX_UINT256)[2:].zfill(64)
    )
    return TransactionData(
        to=pt_address,
        value=0,
        data=approve_data,
        gas_estimate=60_000,
        description=f"Approve PT ({pt_address[:10]}…) for Pendle Router",
        tx_type="approve",
    )


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
    from almanak.framework.connectors.pendle.sdk import MARKET_TOKEN_MINT_SY

    result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
    transactions: list[TransactionData] = []

    try:
        if compiler.chain not in ("arbitrum", "ethereum", "plasma"):
            return _failed(
                intent.intent_id,
                f"Pendle is only available on Arbitrum, Ethereum, and Plasma, not {compiler.chain}",
            )

        from_token = _resolve_pendle_from_token(compiler, intent)
        if from_token is None:
            return _failed(intent.intent_id, f"Unknown from_token: {intent.from_token}")

        amount_in_or_err = _compute_pendle_amount_in(intent, compiler, from_token)
        if isinstance(amount_in_or_err, CompilationResult):
            return amount_in_or_err
        amount_in = amount_in_or_err

        adapter_inputs = _resolve_pendle_adapter_inputs(compiler, intent.intent_id)
        if isinstance(adapter_inputs, CompilationResult):
            return adapter_inputs
        gateway_client, rpc_url = adapter_inputs

        adapter = PendleAdapter(
            rpc_url=rpc_url,
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            gateway_client=gateway_client,
        )

        classified = _classify_pendle_swap_type(intent)
        if isinstance(classified, CompilationResult):
            return classified
        swap_type, side = classified

        market_or_err = _resolve_pendle_market(intent, compiler, side)
        if isinstance(market_or_err, CompilationResult):
            return market_or_err
        market = market_or_err

        slippage_bps = int(intent.max_slippage * Decimal("10000"))
        min_amount_out, estimation_method = _compute_pendle_min_out(swap_type, amount_in, slippage_bps)
        logger.info(
            f"Pendle slippage params: swap_type={swap_type}, amount_in={amount_in}, "
            f"min_amount_out={min_amount_out}, slippage_bps={slippage_bps}, estimation={estimation_method}"
        )

        # Look up the token that mints SY for this market. For yield-bearing token markets
        # (like fUSDT0), this is the yield-bearing token.
        token_mint_sy = MARKET_TOKEN_MINT_SY.get(compiler.chain, {}).get(market.lower())

        # Track original input for pre-flight balance checks (VIB-2533)
        original_from_token: TokenInfo | None = None
        original_amount_in: int | None = None

        # Pre-swap routing: when tokenIn != tokenMintSy, insert a Uniswap V3 hop.
        is_buy_side = side in ("buying_pt", "buying_yt")
        if token_mint_sy and is_buy_side and from_token.address.lower() != token_mint_sy.lower():
            applied = _apply_pendle_pre_swap_routing(compiler, intent, from_token, amount_in, token_mint_sy)
            if isinstance(applied, CompilationResult):
                return applied
            pre_swap_txs, from_token, amount_in, original_from_token, original_amount_in = applied
            transactions.extend(pre_swap_txs)
            token_mint_sy = None  # tokenIn now equals tokenMintSy
            min_amount_out, estimation_method = _compute_pendle_min_out(
                swap_type, amount_in, slippage_bps, post_preswap=True
            )
            logger.info(
                f"Pendle slippage params (post-pre-swap): swap_type={swap_type}, amount_in={amount_in}, "
                f"min_amount_out={min_amount_out}, slippage_bps={slippage_bps}, estimation={estimation_method}"
            )

        token_out = _resolve_pendle_token_out(intent, compiler)
        if isinstance(token_out, CompilationResult):
            return token_out
        token_out_address, to_token_decimals = token_out

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

        router_address = adapter.get_router_address()
        if not from_token.is_native:
            transactions.extend(compiler._build_approve_tx(from_token.address, router_address, amount_in))

        tx_data = adapter.build_swap(params)
        transactions.append(
            TransactionData(
                to=tx_data.to,
                value=tx_data.value,
                data=tx_data.data,
                gas_estimate=tx_data.gas_estimate,
                description=tx_data.description,
                tx_type="swap",
            )
        )

        total_gas = sum(tx.gas_estimate for tx in transactions)

        metadata: dict[str, Any] = {
            "from_token": from_token.to_dict(),
            "to_token": intent.to_token,
            "to_token_address": token_out_address,
            "to_token_decimals": to_token_decimals,
            "amount_in": str(amount_in),
            "min_amount_out": str(min_amount_out),
            "slippage": str(intent.max_slippage),
            "protocol": "pendle",
            "market": market,
            "swap_type": swap_type,
            # VIB-3751: receipt parser needs the wallet address to reconstruct user-facing
            # YT swap amounts from Transfer events (the Pendle Market Swap event reflects
            # an internal flash-mint and is NOT a faithful representation of the user trade).
            "wallet_address": compiler.wallet_address,
        }
        # When a pre-swap was inserted, expose the original input token/amount so the
        # orchestrator's pre-flight balance check validates the token the wallet actually
        # holds (e.g., USDC), not the intermediate token (e.g., sUSDe). See VIB-2533.
        if original_from_token is not None:
            metadata["original_from_token"] = original_from_token.to_dict()
            metadata["original_amount_in"] = str(original_amount_in)

        result.action_bundle = ActionBundle(
            intent_type=IntentType.SWAP.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata=metadata,
        )
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

    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[TransactionData] = []

    try:
        chain_err = _check_pendle_chain_supported(compiler, intent.intent_id, "Pendle LP not available")
        if chain_err is not None:
            return chain_err

        inputs = _resolve_pendle_lp_open_inputs(compiler, intent)
        if isinstance(inputs, CompilationResult):
            return inputs
        token, market, amount_in = inputs

        # Default slippage (LPOpenIntent has no max_slippage field)
        slippage_bps = 50
        min_lp_out = 0  # Pendle LP minting: use adapter to estimate proper min

        adapter_inputs = _resolve_pendle_adapter_inputs(compiler, intent.intent_id)
        if isinstance(adapter_inputs, CompilationResult):
            return adapter_inputs
        gateway_client, rpc_url = adapter_inputs

        adapter = PendleAdapter(
            rpc_url=rpc_url,
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            gateway_client=gateway_client,
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
        chain_err = _check_pendle_chain_supported(compiler, intent.intent_id, "Pendle LP not available")
        if chain_err is not None:
            return chain_err

        out_token_or_err = _resolve_pendle_lp_close_out_token(compiler, intent)
        if isinstance(out_token_or_err, CompilationResult):
            return out_token_or_err
        out_token = out_token_or_err

        market = intent.pool
        if not market or not market.startswith("0x"):
            return _failed(intent.intent_id, f"Invalid Pendle market address: {intent.pool}")

        # LP amount comes from position_id (the LP token amount in wei)
        lp_amount_or_err = _parse_pendle_lp_close_amount(intent)
        if isinstance(lp_amount_or_err, CompilationResult):
            return lp_amount_or_err
        lp_amount = lp_amount_or_err

        # Default slippage (LPCloseIntent has no max_slippage field)
        slippage_bps = 50
        min_token_out = 0  # Pendle LP removal: use adapter to estimate proper min

        adapter_inputs = _resolve_pendle_adapter_inputs(compiler, intent.intent_id)
        if isinstance(adapter_inputs, CompilationResult):
            return adapter_inputs
        gateway_client, rpc_url = adapter_inputs

        adapter = PendleAdapter(
            rpc_url=rpc_url,
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            gateway_client=gateway_client,
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
        chain_err = _check_pendle_chain_supported(compiler, intent.intent_id, "Pendle redeem not available")
        if chain_err is not None:
            return chain_err

        # Resolve output token
        out_token = compiler._resolve_token(intent.token)
        if out_token is None:
            return _failed(intent.intent_id, f"Unknown token: {intent.token}")

        # YT address comes from market_id field
        yt_address = intent.market_id
        if not yt_address:
            return _failed(
                intent.intent_id,
                "market_id (YT address) is required for Pendle redeem. Set intent.market_id to the YT contract address.",
            )

        # Calculate amount
        if intent.amount == "all":
            return _failed(intent.intent_id, "amount='all' must be resolved before compilation for Pendle redeem")
        amount_decimal: Decimal = intent.amount  # type: ignore[assignment]
        # PT/YT tokens are always 18 decimals on Pendle
        py_decimals = 18
        py_amount = int(amount_decimal * Decimal(10**py_decimals))

        slippage_bps = 50
        min_token_out = 0  # Pendle redeem: use adapter to estimate proper min

        adapter_inputs = _resolve_pendle_adapter_inputs(compiler, intent.intent_id)
        if isinstance(adapter_inputs, CompilationResult):
            return adapter_inputs
        gateway_client, rpc_url = adapter_inputs

        adapter = PendleAdapter(
            rpc_url=rpc_url,
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            gateway_client=gateway_client,
        )

        # Approve PT tokens for the Pendle router (required before redeemPyToToken).
        # Primary: static YT_TOKEN_INFO reverse-lookup (fast, reliable);
        # fallback: on-chain YT.PT() call for markets not in config.
        pt_address = _resolve_pendle_redeem_pt_address(compiler, adapter, yt_address)
        if pt_address:
            router_address = adapter.get_router_address()
            transactions.append(_build_pendle_redeem_pt_approval(pt_address, router_address))

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
