"""Connector-owned compiler for Aerodrome/Velodrome intents.

These standalone functions receive the compiler instance as their first
parameter and implement all Aerodrome-related compilation logic (LP open,
LP close, swap, pool address query).
"""

from __future__ import annotations

import logging
from decimal import Decimal
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar

from almanak.connectors._strategy_base.base.cl_math import (
    compute_lp_slippage_mins,
    maybe_recompute_lp_amounts_from_slot0,
)
from almanak.connectors._strategy_base.base.compiler import BaseConcentratedLiquidityCompiler, CLCompilerContext
from almanak.framework.intents import compiler_constants
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.models.reproduction_bundle import ActionBundle

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import CollectFeesIntent, LPCloseIntent, LPOpenIntent, SwapIntent

logger = logging.getLogger("almanak.framework.intents.compiler")

LP_POSITION_MANAGERS = compiler_constants.LP_POSITION_MANAGERS

# Selector for Aerodrome V1 pool `metadata()` view — first 4 bytes of
# keccak256("metadata()"). Returns
# (uint256 dec0, uint256 dec1, uint256 r0, uint256 r1, bool stable, address token0, address token1).
# Used by the LP_CLOSE bare-pool-address path to reverse a pool contract into
# its pair identity, mirroring Uniswap V3's opaque tokenId convention.
_AERODROME_POOL_METADATA_SELECTOR = "0x392f37e9"


class AerodromeCompiler(BaseConcentratedLiquidityCompiler):
    """Compiler for Aerodrome classic and Slipstream routes."""

    protocols: ClassVar[frozenset[str]] = frozenset({"aerodrome", "aerodrome_slipstream"})
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {
            IntentType.SWAP,
            IntentType.LP_OPEN,
            IntentType.LP_CLOSE,
            IntentType.LP_COLLECT_FEES,
        }
    )
    chains: ClassVar[frozenset[str]] = frozenset({"base", "optimism"})

    def compile_swap(self, ctx: CLCompilerContext, intent: SwapIntent) -> CompilationResult:
        return compile_swap_aerodrome(_AerodromeCompileImpl(ctx), intent)

    def compile_lp_open(self, ctx: CLCompilerContext, intent: LPOpenIntent) -> CompilationResult:
        impl = _AerodromeCompileImpl(ctx)
        if ctx.protocol == "aerodrome_slipstream":
            return compile_lp_open_aerodrome_slipstream(impl, intent)
        return compile_lp_open_aerodrome(impl, intent)

    def compile_lp_close(self, ctx: CLCompilerContext, intent: LPCloseIntent) -> CompilationResult:
        # VIB-5346 defense-in-depth: Aerodrome/Velodrome Slipstream position_id is
        # an NFT token-id (and the Slipstream close path VALIDATES it is numeric,
        # so it would otherwise ACCEPT minted-LP wei as a token-id); classic
        # routes key on pool/identity. Reject amount="all" chaining via the shared
        # fail-closed allowlist (the runner gate is the primary control).
        from almanak.framework.strategies.lp_position_tracker import (
            lp_close_amount_chaining_supported,
        )

        protocol = getattr(intent, "protocol", None) or ctx.protocol
        if getattr(intent, "is_chained_amount", False) and not lp_close_amount_chaining_supported(protocol):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"LP_CLOSE amount='all' chaining is not supported for {protocol}: "
                    "position_id is a position identity (NFT token-id), not a fungible amount"
                ),
                intent_id=intent.intent_id,
            )
        impl = _AerodromeCompileImpl(ctx)
        if ctx.protocol == "aerodrome_slipstream":
            return compile_lp_close_aerodrome_slipstream(impl, intent)
        return compile_lp_close_aerodrome(impl, intent)

    def compile_collect_fees(self, ctx: CLCompilerContext, intent: CollectFeesIntent) -> CompilationResult:
        if ctx.protocol == "aerodrome_slipstream":
            return compile_collect_fees_aerodrome_slipstream(_AerodromeCompileImpl(ctx), intent)
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=(
                "Aerodrome/Velodrome classic pools do not support LP_COLLECT_FEES: "
                "fees auto-compound into the LP token. Use LP_CLOSE to harvest, or "
                "use Intent.collect_fees(protocol='aerodrome_slipstream', ...) for Slipstream positions."
            ),
        )


class _AerodromeCompileImpl:
    """Per-call adapter exposing framework services to relocated Aerodrome functions."""

    def __init__(self, ctx: CLCompilerContext) -> None:
        self._ctx = ctx
        self.chain = ctx.chain
        self.wallet_address = ctx.wallet_address
        self.price_oracle = ctx.price_oracle
        self.rpc_timeout = ctx.rpc_timeout
        self.default_lp_slippage = ctx.default_lp_slippage
        self.default_deadline_seconds = ctx.default_deadline_seconds
        self._gateway_client = ctx.gateway_client
        self._token_resolver = ctx.token_resolver
        self._config = SimpleNamespace(
            swap_pool_selection_mode=ctx.swap_pool_selection_mode,
            fixed_swap_fee_tier=ctx.fixed_swap_fee_tier,
            max_price_impact_pct=ctx.max_price_impact_pct,
            allow_placeholder_prices=ctx.allow_placeholder_prices,
            # Runtime placeholder flag (distinct from the allow_placeholder_prices
            # config option): True when the compiler was built without a real
            # price oracle. The price-impact guard skips the IMPACT branch in
            # this mode, matching the uniswap_v3 / camelot / fluid pipelines.
            using_placeholders=ctx.using_placeholders,
            permission_discovery=ctx.permission_discovery,
        )

    def _get_chain_rpc_url(self) -> str | None:
        return self._ctx.rpc_url

    def _resolve_token(self, token: str):
        return self._ctx.services.resolve_token(token)

    def _require_token_price(self, symbol: str) -> Decimal:
        return self._ctx.services.require_token_price(symbol)

    def _usd_to_token_amount(self, usd_amount: Decimal, token: Any) -> int:
        return self._ctx.services.usd_to_token_amount(usd_amount, token)

    def _build_approve_tx(self, token_address: str, spender: str, amount: int):
        return self._ctx.services.build_approve_tx(token_address, spender, amount)

    def _validate_pool(self, result: Any, intent_id: str):
        return self._ctx.services.validate_pool(result, intent_id)

    def _format_amount(self, amount: int, decimals: int) -> str:
        return self._ctx.services.format_amount(amount, decimals)

    def _price_to_tick(self, price: Decimal, *, token0_decimals: int, token1_decimals: int) -> int:
        return self._ctx.services.price_to_tick(
            price,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
        )

    def _get_tick_spacing(self, fee_tier: int) -> int:
        return self._ctx.services.get_tick_spacing(fee_tier)

    def _get_wrapped_native_address(self) -> str | None:
        return self._ctx.services.get_wrapped_native_address()

    def _query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None:
        return self._ctx.services.query_erc20_balance(token_address, wallet_address)

    def _fetch_lp_pool_slot0(self, pool_check: Any) -> Any:
        # Shared V3-family slot0 read, lifted to the CL compiler base so
        # slipstream reuses it without importing the Uniswap V3 connector.
        return BaseConcentratedLiquidityCompiler._fetch_lp_pool_slot0(self._ctx, pool_check)

    def _get_aerodrome_pool_address(self, token_a: str, token_b: str, stable: bool) -> str | None:
        return get_aerodrome_pool_address(self, token_a, token_b, stable)


def _looks_like_evm_address(value: str) -> bool:
    """Return True iff ``value`` is a syntactically valid 0x-prefixed 20-byte address."""
    if not value or not value.startswith("0x") or len(value) != 42:
        return False
    try:
        int(value, 16)
        return True
    except ValueError:
        return False


def _aerodrome_swap_price_impact_guard(
    compiler,
    intent: SwapIntent,
    from_token: Any,
    to_token: Any,
    amount_decimal: Decimal,
    swap_result: Any,
) -> CompilationResult | None:
    """Pre-trade price-impact guard for Aerodrome/Velodrome swaps (ALM-2890).

    Mirrors the guard applied by the uniswap_v3 / camelot / fluid swap compilers
    (``almanak.framework.intents._compiler_helpers.check_price_impact``): compare
    the on-chain quoter amount against an independent oracle estimate and fail
    closed when the realized impact exceeds ``intent.max_price_impact`` (or the
    compiler config default ``max_price_impact_pct``). Blueprint 05 §"Pool
    Selection Policy (UX First, Safety Always)".

    Returns a FAILED ``CompilationResult`` to abort compilation, or ``None`` when
    the swap is within the cap (or the check is legitimately skipped — offline /
    placeholder mode, or no oracle to compare against).
    """
    from almanak.framework.intents._compiler_helpers import (
        PriceImpactDecision,
        check_price_impact,
    )

    cfg = getattr(compiler, "_config", None)
    # Empty != Zero: a configured cap of Decimal("0") is a deliberate "any
    # nonzero impact fails closed" setting and must NOT be coerced to the 5%
    # default; only an unset (None) cap falls back to the default.
    configured_max_impact = getattr(cfg, "max_price_impact_pct", None)
    config_max_impact = Decimal("0.05") if configured_max_impact is None else configured_max_impact
    using_placeholders = bool(getattr(cfg, "using_placeholders", False))
    offline_mode = using_placeholders or bool(getattr(cfg, "permission_discovery", False))

    # Oracle-derived expected output (wei), independent of the pool quote.
    # Degrade to 0 (== "no oracle to compare against") if any price is missing;
    # the guard then skips rather than hard-failing on a data gap.
    oracle_estimate_wei = 0
    try:
        from_price = compiler._require_token_price(from_token.symbol)
        to_price = compiler._require_token_price(to_token.symbol)
        if to_price > 0:
            oracle_out_human = (amount_decimal * from_price) / to_price
            oracle_estimate_wei = int(oracle_out_human * Decimal(10**to_token.decimals))
    except Exception:  # noqa: BLE001 — oracle gap degrades to "no comparison", never a hard error
        oracle_estimate_wei = 0

    # Only a genuine ON-CHAIN quote counts as the quoter amount. The Aerodrome
    # adapter silently falls back to an oracle-derived amount when the on-chain
    # quote is unavailable (RPC/gateway failure, thin pool with no route); that
    # fallback amount is NOT independent of the oracle estimate, so comparing
    # the two would always show ~0 impact and defeat the guard (ALM-2890).
    # Treat an oracle-fallback quote as "quoter missing" so check_price_impact
    # fails closed in live mode (and is relaxed only in offline/placeholder
    # mode, where oracle-only pricing is expected).
    quote = getattr(swap_result, "quote", None)
    quote_is_onchain = bool(getattr(quote, "is_onchain", False)) if quote is not None else False
    quoter_raw = getattr(quote, "amount_out", None) if (quote is not None and quote_is_onchain) else None
    quoter_amount = int(quoter_raw) if quoter_raw is not None else None

    impact = check_price_impact(
        oracle_estimate=oracle_estimate_wei,
        quoter_amount=quoter_amount,
        intent_max_impact=intent.max_price_impact,
        config_max_impact=config_max_impact,
        offline_mode=offline_mode,
        using_placeholders=using_placeholders,
    )
    if impact.decision is PriceImpactDecision.IMPACT_TOO_HIGH:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Price impact too high: quoter amount implies "
                f"{impact.price_impact:.1%} price impact "
                f"(oracle estimate: {oracle_estimate_wei}, quoter: {quoter_amount}). "
                f"Maximum allowed: {impact.effective_max_impact:.2%}. "
                f"Likely cause: pool has insufficient liquidity for "
                f"{intent.from_token}->{intent.to_token} on Aerodrome."
            ),
            intent_id=intent.intent_id,
        )
    if impact.decision is PriceImpactDecision.QUOTER_MISSING_FAIL_CLOSED:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Price impact guard: Aerodrome quoter returned no amount for "
                f"{intent.from_token}->{intent.to_token}. Cannot verify pool liquidity "
                f"or price impact. Refusing to compile a swap backed only by the oracle price."
            ),
            intent_id=intent.intent_id,
        )
    if impact.decision is PriceImpactDecision.SKIPPED_NO_ORACLE:
        # No oracle price to compare against — the swap proceeds with
        # slippage-only protection. Surface it so an operator can see the
        # impact guard was skipped rather than silently passed.
        logger.warning(
            "Aerodrome price-impact guard skipped for %s->%s: no oracle price available "
            "to compare against the quoter; slippage-only protection applies.",
            intent.from_token,
            intent.to_token,
        )
    return None


def _validate_slipstream_tick_bounds(
    intent: LPOpenIntent,
    tick_spacing: int,
) -> tuple[int, int] | CompilationResult:
    """Validate Slipstream tick bounds: integer, ordered, aligned to tick_spacing.

    Returns ``(tick_lower, tick_upper)`` on success or a FAILED
    ``CompilationResult``. Extracted from ``compile_lp_open_aerodrome_slipstream``
    so the main path stays under the mccabe limit; behaviour is byte-identical to
    the previous inline guards.
    """
    if int(intent.range_lower) != intent.range_lower or int(intent.range_upper) != intent.range_upper:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Aerodrome Slipstream tick bounds must be integers, "
                f"got range_lower={intent.range_lower}, range_upper={intent.range_upper}"
            ),
            intent_id=intent.intent_id,
        )
    tick_lower = int(intent.range_lower)
    tick_upper = int(intent.range_upper)
    if tick_lower >= tick_upper:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Aerodrome Slipstream tick_lower ({tick_lower}) must be less than tick_upper ({tick_upper})",
            intent_id=intent.intent_id,
        )
    if tick_lower % tick_spacing != 0 or tick_upper % tick_spacing != 0:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Aerodrome Slipstream tick bounds must be aligned to tick_spacing={tick_spacing}: "
                f"tick_lower={tick_lower} (rem={tick_lower % tick_spacing}), "
                f"tick_upper={tick_upper} (rem={tick_upper % tick_spacing})"
            ),
            intent_id=intent.intent_id,
        )
    return tick_lower, tick_upper


def _slipstream_tick_straddle_failure(
    intent: LPOpenIntent,
    slot0: Any,
    tick_lower: int,
    tick_upper: int,
) -> CompilationResult | None:
    """Reject a Slipstream LP_OPEN whose range does not straddle the current tick (ALM-2891).

    A V3-style position is two-sided only when ``tick_lower <= current_tick <
    tick_upper``; a range entirely on one side mints a silent one-sided /
    out-of-range position (e.g. all-token0 with amount1 stranded). Without this
    check a decimals footgun in price->tick conversion (see
    :func:`almanak.framework.intents.tick_utils.price_to_tick`) produced exactly
    that with no error.

    Returns a FAILED ``CompilationResult`` to abort, or ``None`` when the range
    straddles the current tick, the live tick is unavailable (``slot0 is None``),
    or the caller opted in via ``protocol_params={'allow_out_of_range': True}``
    for a deliberate single-sided / limit-order range.
    """
    protocol_params = getattr(intent, "protocol_params", None) or {}
    if slot0 is None or bool(protocol_params.get("allow_out_of_range", False)):
        return None

    current_tick = slot0[1]
    if tick_lower <= current_tick < tick_upper:
        return None

    # Describe the RANGE relative to the current tick: if the current tick sits
    # below tick_lower the whole range is above it, and vice-versa.
    side = "above" if current_tick < tick_lower else "below"
    return CompilationResult(
        status=CompilationStatus.FAILED,
        error=(
            f"Aerodrome Slipstream tick range [{tick_lower}, {tick_upper}) does not "
            f"straddle the pool's current tick {current_tick} (range is entirely "
            f"{side} it). This mints a one-sided / out-of-range position, leaving one "
            f"token stranded — likely a price->tick decimals error (use "
            f"price_to_tick with explicit decimals0/decimals1). Pass "
            f"protocol_params={{'allow_out_of_range': True}} if a single-sided range "
            f"is intended."
        ),
        intent_id=intent.intent_id,
    )


# crap-allowlist: VIB-4853 — import-path swap only (pool-validation moved into connectors, #2527); function body unchanged, anvil-only coverage. Refactor + coverage backfill tracked in VIB-4139.
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
        from almanak.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

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
        from almanak.connectors.aerodrome.pool_validation import validate_aerodrome_pool

        pool_check = validate_aerodrome_pool(
            compiler.chain,
            token0_info.address,
            token1_info.address,
            stable,
            compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
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


def compile_lp_close_aerodrome(compiler, intent: LPCloseIntent) -> CompilationResult:  # noqa: C901
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
        from almanak.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

        # Parse position_id. Accepts two shapes:
        #  1. Canonical symbolic form: "TOKEN0/TOKEN1/volatile|stable"
        #  2. Bare Aerodrome V1 pool address: "0x..."
        # The second form is what ResultEnricher writes into state after LP_OPEN
        # (the pool address is the authoritative identifier for fungible LP tokens,
        # analogous to Uniswap V3's NFT tokenId). When given an address, the pair
        # identity is recovered on-chain via pool.metadata().
        position_id_raw = intent.position_id or ""
        prebuilt_pool_address: str | None = None

        if _looks_like_evm_address(position_id_raw):
            metadata = get_aerodrome_pool_metadata(compiler, position_id_raw)
            if metadata is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Could not resolve Aerodrome pool metadata for {position_id_raw}. "
                        f"Ensure the address is a live Aerodrome V1 pool on {compiler.chain} "
                        f"and that RPC/gateway access is configured."
                    ),
                    intent_id=intent.intent_id,
                )
            token0_addr, token1_addr, stable = metadata
            token0_info = compiler._resolve_token(token0_addr)
            token1_info = compiler._resolve_token(token1_addr)
            if token0_info is None or token1_info is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Could not resolve tokens for Aerodrome pool {position_id_raw} "
                        f"(token0={token0_addr}, token1={token1_addr})"
                    ),
                    intent_id=intent.intent_id,
                )
            token0_symbol = token0_info.symbol
            token1_symbol = token1_info.symbol
            prebuilt_pool_address = position_id_raw
            logger.info(
                f"Compiling Aerodrome LP_CLOSE (bare pool address): "
                f"{token0_symbol}/{token1_symbol}, stable={stable}, pool={position_id_raw}"
            )
        else:
            pool_parts = position_id_raw.split("/")
            if len(pool_parts) < 2:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Invalid position ID: {intent.position_id}. "
                        f"Expected: TOKEN0/TOKEN1/volatile or TOKEN0/TOKEN1/stable, "
                        f"or a bare Aerodrome pool address (0x...)."
                    ),
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

        # Get LP token address for the pool (gateway-aware for deployed mode).
        # When position_id was a bare pool address, we already have it — skip the
        # factory forward lookup.
        if prebuilt_pool_address is not None:
            pool_address = prebuilt_pool_address
        else:
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
                    "no_op": True,
                    "reason": "No LP tokens found; LP_CLOSE no-op",
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


# crap-allowlist: VIB-4687 — pre-existing complexity (cc=26) relocated from
# compiler_aerodrome.py by the phase-2 connector fold; bodies are byte-identical
# apart from a .pool_validation -> absolute import-path change. Split into
# per-route helpers (Slipstream / Aerodrome Classic / Velodrome Classic) under
# the four-step CRAP refactor protocol.
def compile_swap_aerodrome(compiler, intent: SwapIntent) -> CompilationResult:  # noqa: C901
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
        from almanak.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

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
        from .addresses import AERODROME as AERODROME_ADDRESSES

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
            from almanak.connectors.aerodrome.pool_validation import validate_aerodrome_pool

            pool_check = validate_aerodrome_pool(
                compiler.chain,
                from_token.address,
                to_token.address,
                stable,
                compiler._get_chain_rpc_url(),
                gateway_client=compiler._gateway_client,
            )
        else:
            from almanak.connectors.aerodrome.pool_validation import validate_aerodrome_cl_pool

            pool_check = validate_aerodrome_cl_pool(
                compiler.chain,
                from_token.address,
                to_token.address,
                tick_spacing,
                compiler._get_chain_rpc_url(),
                gateway_client=compiler._gateway_client,
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

        # ALM-2890: pre-trade price-impact guard (fail-closed). Aerodrome
        # previously enforced only slippage on the quoter amount; a thin pool
        # would still compile a swap that moved the price arbitrarily far from
        # the oracle. Mirror the uniswap_v3 / camelot / fluid guard.
        impact_failure = _aerodrome_swap_price_impact_guard(
            compiler, intent, from_token, to_token, amount_decimal, swap_result
        )
        if impact_failure is not None:
            return impact_failure

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


# crap-allowlist: VIB-4853 — import-path swap only (pool-validation moved into connectors, #2527); function body unchanged, anvil-only coverage. Refactor + coverage backfill tracked in VIB-4139.
def compile_lp_open_aerodrome_slipstream(compiler, intent: LPOpenIntent) -> CompilationResult:
    """Compile LP_OPEN intent for Aerodrome Slipstream CL (concentrated liquidity).

    Aerodrome Slipstream uses Uniswap V3-style concentrated liquidity with NFT positions.
    Pool format: "TOKEN0/TOKEN1/200" (tick_spacing as 3rd component, integer)

    The intent's ``range_lower`` and ``range_upper`` are the tick bounds (cast to int).

    Args:
        compiler: IntentCompiler instance
        intent: LPOpenIntent to compile

    Returns:
        CompilationResult with Aerodrome Slipstream mint ActionBundle
    """
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[Any] = []
    warnings: list[str] = []

    try:
        from almanak.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

        # Parse pool: "TOKEN0/TOKEN1/tick_spacing"
        pool_parts = intent.pool.split("/")
        if len(pool_parts) < 3:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Invalid pool format for aerodrome_slipstream: '{intent.pool}'. Expected: TOKEN0/TOKEN1/tick_spacing (e.g. WETH/USDC/200)",
                intent_id=intent.intent_id,
            )

        token0_symbol = pool_parts[0]
        token1_symbol = pool_parts[1]
        try:
            tick_spacing = int(pool_parts[2])
        except ValueError:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Invalid tick_spacing in pool '{intent.pool}': '{pool_parts[2]}' must be an integer",
                intent_id=intent.intent_id,
            )

        # Validate CL support (only Base has cl_nft)
        cl_nft = LP_POSITION_MANAGERS.get(compiler.chain, {}).get("aerodrome_slipstream")
        if not cl_nft:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Aerodrome Slipstream CL not supported on chain '{compiler.chain}'. Only 'base' is supported.",
                intent_id=intent.intent_id,
            )

        logger.info(
            f"Compiling Aerodrome Slipstream LP_OPEN: {token0_symbol}/{token1_symbol}, "
            f"tick_spacing={tick_spacing}, ticks=[{intent.range_lower},{intent.range_upper}], "
            f"amounts={intent.amount0}/{intent.amount1}"
        )

        # Resolve tokens
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

        # Enforce canonical token order (token0 address < token1 address by EVM convention).
        # Slipstream/V3 ticks are defined relative to token0/token1: reversing the order
        # silently inverts the tick direction, placing the position on the wrong side of
        # the price curve.
        if int(token0_info.address, 16) > int(token1_info.address, 16):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Non-canonical pool token order: {token0_symbol} ({token0_info.address}) "
                    f"has a higher address than {token1_symbol} ({token1_info.address}). "
                    f"Slipstream ticks are defined with the lower-address token as token0. "
                    f"Use '{token1_symbol}/{token0_symbol}/{tick_spacing}' instead."
                ),
                intent_id=intent.intent_id,
            )

        # Validate pool existence
        from almanak.connectors.aerodrome.pool_validation import validate_aerodrome_cl_pool

        pool_check = validate_aerodrome_cl_pool(
            compiler.chain,
            token0_info.address,
            token1_info.address,
            tick_spacing,
            compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
        )
        failed = compiler._validate_pool(pool_check, intent.intent_id)
        if failed is not None:
            return failed

        # Validate tick bounds: must be integers, ordered, and aligned to tick_spacing.
        tick_bounds = _validate_slipstream_tick_bounds(intent, tick_spacing)
        if isinstance(tick_bounds, CompilationResult):
            return tick_bounds
        tick_lower, tick_upper = tick_bounds

        # Convert oracle-derived amounts to wei. Token order is canonical here
        # (token0 < token1 enforced above), so amount0 corresponds to token0.
        amount0_desired = int(intent.amount0 * Decimal(10**token0_info.decimals))
        amount1_desired = int(intent.amount1 * Decimal(10**token1_info.decimals))

        # Read the pool's live slot0 once (sqrtPriceX96, current tick). Used for
        # BOTH the straddle assertion (ALM-2891) and the amount recompute below.
        slot0 = compiler._fetch_lp_pool_slot0(pool_check)

        straddle_failure = _slipstream_tick_straddle_failure(intent, slot0, tick_lower, tick_upper)
        if straddle_failure is not None:
            return straddle_failure

        # Align desired amounts to the pool's current sqrtPriceX96 (slot0).
        # Slipstream pools are V3-shaped, so the V3 recompute helper applies
        # directly. Without this, oracle/pool price divergence causes the
        # NonfungiblePositionManager to revert with "Price slippage check"
        # because the actual amounts taken by the pool fall below the mins.
        recomputed_or_fail = maybe_recompute_lp_amounts_from_slot0(
            fetch_slot0=compiler._fetch_lp_pool_slot0,
            pool_check=pool_check,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            amount0_desired=amount0_desired,
            amount1_desired=amount1_desired,
            intent_id=intent.intent_id,
            slot0=slot0,
        )
        if isinstance(recomputed_or_fail, CompilationResult):
            return recomputed_or_fail
        amount0_desired, amount1_desired = recomputed_or_fail

        # LP slippage-based minimums computed from POOL-ALIGNED amounts, not
        # oracle inputs. Matches the V3-family connector compiler path.
        amount0_min, amount1_min = compute_lp_slippage_mins(
            intent=intent,
            amount0_desired=amount0_desired,
            amount1_desired=amount1_desired,
            default_lp_slippage=compiler.default_lp_slippage,
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

        # Build CL mint transactions. Pass corrected wei amounts and pre-computed
        # mins via the wei-overload kwargs so the adapter does NOT re-derive
        # mins from raw (uncorrected) amounts.
        cl_result = adapter.add_cl_liquidity(
            token_a=token0_symbol,
            token_b=token1_symbol,
            tick_spacing=tick_spacing,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            amount_a=intent.amount0,
            amount_b=intent.amount1,
            recipient=compiler.wallet_address,
            amount_a_wei=amount0_desired,
            amount_b_wei=amount1_desired,
            amount_a_min_wei=amount0_min,
            amount_b_min_wei=amount1_min,
        )

        if not cl_result.success:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Failed to build CL mint TX: {cl_result.error}",
                intent_id=intent.intent_id,
            )

        for tx in cl_result.transactions:
            transactions.append(tx)

        total_gas = sum(tx.gas_estimate for tx in transactions)

        action_bundle = ActionBundle(
            intent_type=IntentType.LP_OPEN.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "pool": intent.pool,
                "token0": token0_info.to_dict(),
                "token1": token1_info.to_dict(),
                "tick_spacing": tick_spacing,
                "tick_lower": tick_lower,
                "tick_upper": tick_upper,
                "amount0": str(intent.amount0),
                "amount1": str(intent.amount1),
                # Wei-denominated post-recompute values, matching the V3 metadata
                # shape. Required by orchestrator._preflight_lp_open_requirements
                # which reads amount0_desired/amount1_desired (in wei) to validate
                # wallet balance before submission.
                "amount0_desired": str(amount0_desired),
                "amount1_desired": str(amount1_desired),
                "amount0_min": str(amount0_min),
                "amount1_min": str(amount1_min),
                "protocol": "aerodrome_slipstream",
                "token_id": None,
                "nft_manager": cl_nft,
            },
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas
        result.warnings = warnings

        tx_types = " + ".join(tx.tx_type for tx in transactions) if transactions else ""
        tx_summary = f" ({tx_types})" if tx_types else ""
        logger.info(
            f"Compiled Aerodrome Slipstream LP_OPEN: {token0_symbol}/{token1_symbol}, "
            f"tick_spacing={tick_spacing}, {len(transactions)} txs{tx_summary}, {total_gas} gas"
        )

    except Exception as e:
        logger.exception(f"Failed to compile Aerodrome Slipstream LP_OPEN intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


# crap-allowlist: VIB-4835 — pre-existing complexity (cc=17, cov=40%) relocated by Phase 2 fold from almanak/framework/connectors/aerodrome/compiler.py; function body unchanged by this PR. Refactor + coverage backfill tracked in VIB-4139.
def compile_lp_close_aerodrome_slipstream(compiler, intent: LPCloseIntent) -> CompilationResult:
    """Compile LP_CLOSE intent for Aerodrome Slipstream CL.

    The ``intent.position_id`` is the NFT tokenId as a numeric string (e.g. "12345").

    Args:
        compiler: IntentCompiler instance
        intent: LPCloseIntent to compile

    Returns:
        CompilationResult with Aerodrome Slipstream decreaseLiquidity + collect ActionBundle
    """
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[Any] = []
    warnings: list[str] = []

    try:
        from almanak.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

        position_id_raw = intent.position_id or ""

        # Validate and parse tokenId
        if not position_id_raw:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="position_id is required for aerodrome_slipstream LP_CLOSE (must be NFT tokenId string)",
                intent_id=intent.intent_id,
            )

        try:
            token_id = int(position_id_raw)
        except ValueError:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Invalid position_id '{position_id_raw}': aerodrome_slipstream LP_CLOSE requires a numeric tokenId",
                intent_id=intent.intent_id,
            )

        # Validate CL support
        cl_nft = LP_POSITION_MANAGERS.get(compiler.chain, {}).get("aerodrome_slipstream")
        if not cl_nft:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Aerodrome Slipstream CL not supported on chain '{compiler.chain}'. Only 'base' is supported.",
                intent_id=intent.intent_id,
            )

        logger.info(f"Compiling Aerodrome Slipstream LP_CLOSE: tokenId={token_id}")

        # Handle permission discovery mode: tokenId=0 → synthetic non-zero
        _cfg = getattr(compiler, "_config", None)
        permission_discovery = _cfg and getattr(_cfg, "permission_discovery", False)
        if permission_discovery and token_id == 0:
            # Use a non-zero synthetic tokenId so the adapter can produce real TXs
            token_id = 1
            logger.debug("Permission discovery mode: using synthetic tokenId=1 for Aerodrome Slipstream LP_CLOSE")

        # Create Aerodrome adapter
        config = AerodromeConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            price_provider=compiler.price_oracle,
            rpc_url=compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
        )
        adapter = AerodromeAdapter(config)

        # Build remove liquidity transactions
        cl_result = adapter.remove_cl_liquidity(
            token_id=token_id,
            recipient=compiler.wallet_address,
        )

        if not cl_result.success:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Failed to build CL decreaseLiquidity TX: {cl_result.error}",
                intent_id=intent.intent_id,
            )

        # Handle zero-liquidity case (position already closed)
        if not cl_result.transactions:
            warning = f"CL position tokenId={token_id} has zero liquidity — treating LP_CLOSE as no-op"
            warnings.append(warning)
            logger.info(warning)

            result.action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[],
                metadata={
                    "position_id": intent.position_id,
                    "token_id": token_id,
                    "protocol": "aerodrome_slipstream",
                    "collect_fees": intent.collect_fees,
                    "no_op": True,
                    "reason": "Zero liquidity; LP_CLOSE no-op",
                },
            )
            result.transactions = []
            result.total_gas_estimate = 0
            result.warnings = warnings
            return result

        for tx in cl_result.transactions:
            transactions.append(tx)

        total_gas = sum(tx.gas_estimate for tx in transactions)

        action_bundle = ActionBundle(
            intent_type=IntentType.LP_CLOSE.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "position_id": intent.position_id,
                "token_id": token_id,
                "protocol": "aerodrome_slipstream",
                "collect_fees": intent.collect_fees,
                "nft_manager": cl_nft,
            },
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas
        result.warnings = warnings

        tx_types = " + ".join(str(getattr(tx, "tx_type", "")) for tx in transactions) if transactions else ""
        tx_summary = f" ({tx_types})" if tx_types else ""
        logger.info(
            f"Compiled Aerodrome Slipstream LP_CLOSE: tokenId={token_id}, "
            f"{len(transactions)} txs{tx_summary}, {total_gas} gas"
        )

    except Exception as e:
        logger.exception(f"Failed to compile Aerodrome Slipstream LP_CLOSE intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


def compile_collect_fees_aerodrome_slipstream(compiler, intent: CollectFeesIntent) -> CompilationResult:
    """Compile LP_COLLECT_FEES intent for Aerodrome Slipstream CL.

    Slipstream's NonfungiblePositionManager is V3-shaped: ``collect()`` harvests
    accrued fees + any previously-unlocked principal without burning the position.
    Calling it on a position with zero owed tokens is a no-op on-chain (the
    transaction succeeds but transfers nothing); we still emit it so the runner
    sees a deterministic outcome rather than guessing client-side.

    The NFT ``tokenId`` is required and is read from
    ``intent.protocol_params["position_id"]``.

    Args:
        compiler: IntentCompiler instance
        intent: CollectFeesIntent to compile

    Returns:
        CompilationResult with Aerodrome Slipstream collect ActionBundle
    """
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )

    try:
        from almanak.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

        protocol_params = intent.protocol_params or {}
        position_id_raw = protocol_params.get("position_id")
        if position_id_raw is None or position_id_raw == "":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "Aerodrome Slipstream LP_COLLECT_FEES requires protocol_params={'position_id': '<NFT tokenId>'}"
                ),
                intent_id=intent.intent_id,
            )

        try:
            # Coerce to string first to reject implicit numeric conversions:
            # ``int(1.9)`` silently truncates to ``1`` and ``int(True)`` is
            # ``1`` — both would build a tx for the wrong NFT. Going through
            # ``str(...).strip()`` requires the caller pass a clean integer
            # literal (or an int) and surfaces float / bool inputs as errors.
            token_id = int(str(position_id_raw).strip())
        except (TypeError, ValueError):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Invalid position_id '{position_id_raw}': Aerodrome Slipstream "
                    f"LP_COLLECT_FEES requires a numeric NFT tokenId"
                ),
                intent_id=intent.intent_id,
            )

        cl_nft = LP_POSITION_MANAGERS.get(compiler.chain, {}).get("aerodrome_slipstream")
        if not cl_nft:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Aerodrome Slipstream CL not supported on chain '{compiler.chain}'. "
                    f"Supported chains: {sorted(c for c, m in LP_POSITION_MANAGERS.items() if 'aerodrome_slipstream' in m)}."
                ),
                intent_id=intent.intent_id,
            )

        _cfg = getattr(compiler, "_config", None)
        permission_discovery = bool(_cfg and getattr(_cfg, "permission_discovery", False))
        # Reject non-positive tokenIds at compile time outside permission
        # discovery — ``NonfungiblePositionManager.collect()`` reverts on
        # tokenId 0 / non-existent positions, so failing loudly here saves
        # the strategy a chain round-trip and a confusing on-chain error.
        if token_id < 0 or (token_id == 0 and not permission_discovery):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Invalid position_id '{position_id_raw}': Aerodrome Slipstream "
                    f"LP_COLLECT_FEES requires a positive NFT tokenId"
                ),
                intent_id=intent.intent_id,
            )
        if permission_discovery and token_id == 0:
            token_id = 1
            logger.debug(
                "Permission discovery mode: using synthetic tokenId=1 for Aerodrome Slipstream LP_COLLECT_FEES"
            )

        config = AerodromeConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            price_provider=compiler.price_oracle,
            rpc_url=compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
        )
        adapter = AerodromeAdapter(config)

        collect_result = adapter.collect_cl_fees(
            token_id=token_id,
            recipient=compiler.wallet_address,
        )

        if not collect_result.success:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Failed to build CL collect TX: {collect_result.error}",
                intent_id=intent.intent_id,
            )

        # ``adapter.collect_cl_fees`` returns the connector-local
        # ``aerodrome.adapter.TransactionData``, distinct from the compiler's
        # ``compiler_models.TransactionData``; type as ``Any`` to mirror the
        # pattern in ``compile_lp_close_aerodrome_slipstream`` and avoid
        # spurious mypy errors at the boundary.
        transactions: list[Any] = list(collect_result.transactions)
        total_gas = sum(tx.gas_estimate for tx in transactions)

        # Preserve the caller-supplied position_id verbatim so manifest
        # consumers see what the strategy passed (mirrors LP_CLOSE Slipstream
        # at compile_lp_close_aerodrome_slipstream's metadata). In permission
        # discovery the on-chain ``token_id`` field carries the synthetic
        # substitute; the symbolic ``position_id`` field carries the original.
        action_bundle = ActionBundle(
            intent_type=IntentType.LP_COLLECT_FEES.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "pool": intent.pool,
                "position_id": str(position_id_raw),
                "token_id": token_id,
                "protocol": "aerodrome_slipstream",
                "chain": compiler.chain,
                "nft_manager": cl_nft,
            },
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas

        tx_types = " + ".join(str(getattr(tx, "tx_type", "")) for tx in transactions) if transactions else ""
        tx_summary = f" ({tx_types})" if tx_types else ""
        logger.info(
            f"Compiled Aerodrome Slipstream LP_COLLECT_FEES: tokenId={token_id}, "
            f"{len(transactions)} txs{tx_summary}, {total_gas} gas"
        )

    except Exception as e:
        logger.exception(f"Failed to compile Aerodrome Slipstream LP_COLLECT_FEES intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


# crap-allowlist: VIB-4853 — import-path swap only (pool-validation moved into connectors, #2527); function body unchanged, anvil-only coverage. Refactor + coverage backfill tracked in VIB-4139.
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
    from almanak.connectors._strategy_base.pool_validation_base import (
        ZERO_ADDRESS,
    )
    from almanak.connectors._strategy_base.pool_validation_base import (
        decode_address as _decode_address,
    )
    from almanak.connectors.aerodrome.pool_validation import _encode_get_pool_aerodrome

    from .addresses import AERODROME

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

    from almanak.connectors._strategy_base.pool_validation_base import eth_call as _eth_call

    rpc_raw = _eth_call(rpc_url, factory, calldata)
    pool_address = _process_raw_result(rpc_raw)
    if pool_address:
        logger.debug(f"Resolved Aerodrome pool via direct RPC: {pool_address}")
    return pool_address


# crap-allowlist: VIB-4853 — import-path swap only (pool-validation moved into connectors, #2527); function body unchanged, anvil-only coverage. Refactor + coverage backfill tracked in VIB-4139.
def get_aerodrome_pool_metadata(compiler, pool_address: str) -> tuple[str, str, bool] | None:
    """Query an Aerodrome V1 pool's (token0, token1, stable) via ``metadata()``.

    Reverse of :func:`get_aerodrome_pool_address`: given the pool contract
    address, recover the pair identity. Supports bare-pool-address position
    IDs in LP_CLOSE, which mirrors Uniswap V3's opaque tokenId pattern (the
    pool address is the authoritative on-chain identifier for fungible
    Aerodrome LP tokens).

    Returns:
        Tuple of ``(token0_address, token1_address, stable)`` on success,
        or ``None`` if the pool can't be read (no gateway/RPC access, the
        address isn't an Aerodrome V1 pool, etc).
    """
    from almanak.connectors._strategy_base.pool_validation_base import decode_address as _decode_address

    def _decode(raw: bytes | None) -> tuple[str, str, bool] | None:
        # metadata() returns 7 × 32-byte words:
        #   [0:32]    uint256 dec0
        #   [32:64]   uint256 dec1
        #   [64:96]   uint256 reserve0
        #   [96:128]  uint256 reserve1
        #   [128:160] bool    stable
        #   [160:192] address token0
        #   [192:224] address token1
        if raw is None or len(raw) < 224:
            return None
        stable = int.from_bytes(raw[128:160], "big") != 0
        token0 = _decode_address(raw[160:192])
        token1 = _decode_address(raw[192:224])
        if not token0 or not token1:
            return None
        return token0, token1, stable

    # --- Gateway path (deployed mode) ---
    if compiler._gateway_client is not None:
        try:
            hex_result = compiler._gateway_client.eth_call(
                chain=compiler.chain,
                to=pool_address,
                data=_AERODROME_POOL_METADATA_SELECTOR,
            )
            if hex_result and hex_result != "0x":
                raw = bytes.fromhex(hex_result[2:] if hex_result.startswith("0x") else hex_result)
                decoded = _decode(raw)
                if decoded is not None:
                    logger.debug(f"Resolved Aerodrome pool metadata via gateway for {pool_address}")
                    return decoded
            return None
        except Exception as e:
            logger.warning("Gateway Aerodrome pool metadata query failed, falling back to direct RPC: %s", e)

    # --- Direct RPC fallback (local dev) ---
    rpc_url = compiler._get_chain_rpc_url()
    if rpc_url is None:
        logger.warning("No RPC URL or gateway client — cannot query Aerodrome pool metadata")
        return None

    from almanak.connectors._strategy_base.pool_validation_base import eth_call as _eth_call

    rpc_raw = _eth_call(rpc_url, pool_address, _AERODROME_POOL_METADATA_SELECTOR)
    decoded = _decode(rpc_raw)
    if decoded is not None:
        logger.debug(f"Resolved Aerodrome pool metadata via direct RPC for {pool_address}")
    return decoded
