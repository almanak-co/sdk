"""Connector-owned compiler for Aerodrome/Velodrome intents.

These standalone functions receive the compiler instance as their first
parameter and implement all Aerodrome-related compilation logic (LP open,
LP close, swap, pool address query).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar, cast

from almanak.connectors._strategy_base.base.cl_math import (
    compute_lp_slippage_mins,
    lp_range_excludes_spot_warning,
    maybe_recompute_lp_amounts_from_slot0,
)
from almanak.connectors._strategy_base.base.compiler import (
    BaseConcentratedLiquidityCompiler,
    CLCompilerContext,
    CompilerServicesFacadeMixin,
)
from almanak.connectors._strategy_base.cl_range import PriceBandToTicksError, price_band_to_ticks
from almanak.connectors._strategy_base.slippage import SlippagePrecisionError, slippage_to_bps
from almanak.connectors.aerodrome.addresses import (
    SLIPSTREAM_LP_DEPLOYMENTS,
    SlipstreamDeployment,
    slipstream_deployment_for_factory,
    slipstream_lp_deployments,
)
from almanak.framework.data.tokens import build_swap_token_meta
from almanak.framework.intents import compiler_constants
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.min_out_guard import UnprotectedTradeError
from almanak.framework.intents.vocabulary import IntentType, lp_range_bounds, lp_range_is_ticks
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.framework.teardown.lp_clamp import LpClampUnresolved, bound_close_amount
from almanak.framework.venues import VerifiedVenueBinding

if TYPE_CHECKING:
    from almanak.connectors.aerodrome.adapter import AerodromeAdapter
    from almanak.framework.intents.compiler_models import TokenInfo
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


class _AerodromeCompileImpl(CompilerServicesFacadeMixin):
    """Per-call adapter exposing framework services to relocated Aerodrome functions."""

    def __init__(self, ctx: CLCompilerContext) -> None:
        super().__init__(ctx)
        self.default_lp_slippage = ctx.default_lp_slippage
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

    def _fetch_lp_pool_slot0(self, pool_check: Any) -> Any:
        # Shared V3-family slot0 read, lifted to the CL compiler base so
        # slipstream reuses it without importing the Uniswap V3 connector.
        return BaseConcentratedLiquidityCompiler._fetch_lp_pool_slot0(cast(CLCompilerContext, self._ctx), pool_check)

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


def _looks_like_bare_pool(pool: str | None) -> bool:
    """True when ``pool`` is offered as an exact address rather than a symbolic key."""
    return isinstance(pool, str) and pool.lower().startswith("0x") and "/" not in pool


def _exact_pool_transport(
    compiler: Any, pool_address: str, intent_id: str
) -> tuple[str | None, Any] | CompilationResult:
    """Gateway-boundary gate shared by every exact-address LP lane.

    Strategy-side compilation must cross the gateway channel; a direct RPC url
    is permitted only while compiling inside the gateway process
    (``gateway_internal_preflight``). Returns ``(internal_rpc, gateway_client)``
    or a FAILED result.
    """
    gateway_client = compiler._gateway_client
    gateway_connected = gateway_client is not None and bool(getattr(gateway_client, "is_connected", False))
    internal_preflight = bool(compiler._gateway_internal_preflight)
    if not gateway_connected and not internal_preflight:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Cannot resolve exact pool {pool_address} on {compiler.chain}: "
                "a connected gateway is required for pool identity reads."
            ),
            intent_id=intent_id,
        )
    return (compiler._get_chain_rpc_url() if internal_preflight else None), gateway_client


def _require_resolved_token_price(compiler: Any, token: TokenInfo) -> Decimal:
    """Use exact identity in production while preserving the legacy test seam."""
    exact_lookup = getattr(compiler, "_require_token_price_for", None)
    if callable(exact_lookup):
        return exact_lookup(token)
    return compiler._require_token_price(token.symbol)


def _verify_slipstream_binding(
    *,
    compiler: _AerodromeCompileImpl,
    pool_address: str,
    token0_address: str,
    token1_address: str,
    tick_spacing: int,
    expected_position_manager: str,
    intent_id: str,
    allow_unavailable_for_risk_reduction: bool = False,
) -> VerifiedVenueBinding | CompilationResult | None:
    """Verify one exact Slipstream pool through its manifest-owned provider."""

    factory = compiler._ctx.venue_verification_gateway_factory
    if not callable(factory):
        if allow_unavailable_for_risk_reduction:
            return None
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error="Exact Slipstream venue verification is unavailable on base",
            is_safety_refusal=True,
            intent_id=intent_id,
        )

    from almanak.core.asset_identity import AssetIdentity, AssetNamespace
    from almanak.framework.primitives.types import Primitive
    from almanak.framework.venues import (
        VenueBindingComponent,
        VenueBindingFailure,
        VenueBindingFailureState,
        VenueReferenceNamespace,
        VenueTargetRef,
        VenueTargetRole,
        VenueVerificationRequest,
    )

    request = VenueVerificationRequest(
        chain=compiler.chain,
        protocol="aerodrome_slipstream",
        primitive=Primitive.LP,
        requested_refs=(
            VenueTargetRef(
                role=VenueTargetRole.POOL,
                reference_namespace=VenueReferenceNamespace.EVM_ADDRESS,
                reference=pool_address.lower(),
            ),
        ),
        ordered_assets=(
            AssetIdentity(compiler.chain, AssetNamespace.ERC20, token0_address),
            AssetIdentity(compiler.chain, AssetNamespace.ERC20, token1_address),
        ),
        binding_components=(VenueBindingComponent("tick_spacing", str(tick_spacing)),),
        binding_policy_version=1,
    )
    try:
        from almanak.connectors._strategy_base.venue_verifier_registry import VenueVerifierRegistry

        registry = VenueVerifierRegistry()
        verifier = registry.load_class(request.protocol)()
        gateway = factory()
    except (ConnectionError, ImportError, OSError, RuntimeError, TimeoutError, TypeError, ValueError) as exc:
        if allow_unavailable_for_risk_reduction:
            return None
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Exact Slipstream venue transport is unavailable: {exc}",
            is_safety_refusal=True,
            intent_id=intent_id,
        )
    verified = registry.validate_result(request, verifier.verify_venue(request, gateway))
    if isinstance(verified, VenueBindingFailure):
        if allow_unavailable_for_risk_reduction and verified.state is VenueBindingFailureState.UNAVAILABLE:
            return None
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Exact Slipstream venue refused: {verified.reason_code.value}: {verified.detail}",
            is_safety_refusal=True,
            intent_id=intent_id,
        )
    operational = {ref.role: ref.reference for ref in verified.operational_refs}
    if operational.get(VenueTargetRole.POSITION_MANAGER) != expected_position_manager.lower():
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Exact Slipstream verifier did not authorize the position manager {expected_position_manager.lower()}"
            ),
            is_safety_refusal=True,
            intent_id=intent_id,
        )
    return verified


def _slipstream_venue_metadata(verified: VerifiedVenueBinding | None) -> dict[str, Any]:
    if verified is None:
        return {}
    return {
        "venue_binding_hash": verified.binding.binding_hash,
        "venue_binding": verified.binding.to_preimage_wire(),
        "venue_operational_refs": [ref.to_wire() for ref in verified.operational_refs],
        "venue_verification": verified.evidence.to_wire(),
    }


@dataclass(frozen=True, slots=True)
class _ResolvedSlipstreamPosition:
    """Physical position authority plus any currently certified venue."""

    deployment: SlipstreamDeployment
    verified_venue: VerifiedVenueBinding | None


def _resolve_slipstream_position(
    *,
    compiler: _AerodromeCompileImpl,
    adapter: AerodromeAdapter,
    token_id: int,
    intent_id: str,
    permission_discovery: bool,
    reviewed_deployments: tuple[SlipstreamDeployment, ...],
    expected_pool: str | None = None,
) -> _ResolvedSlipstreamPosition | CompilationResult:
    """Resolve a position's manager generation and paired factory binding.

    Close and standalone collection share this boundary. A measured mismatch
    refuses both; typed verifier unavailability preserves the independently
    executable risk-reduction path without attaching certified metadata.

    ``expected_pool`` is the intent's ``pool`` field. When it is a bare address
    the NFT's factory-reconstructed pool must be that exact address (the V3
    lane's address-bound close contract); a symbolic key is not cross-checked.
    """

    if permission_discovery:
        return _ResolvedSlipstreamPosition(reviewed_deployments[0], None)

    try:
        deployment = adapter.resolve_owned_cl_deployment(token_id)
    except (ConnectionError, OSError, TimeoutError, ValueError) as exc:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Cannot resolve physical Slipstream position authority: {exc}",
            is_safety_refusal=True,
            intent_id=intent_id,
        )
    position = adapter.get_cl_position(token_id, deployment)
    if position is None:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Could not read Slipstream tokenId={token_id} through reviewed manager {deployment.position_manager}"
            ),
            intent_id=intent_id,
        )

    from almanak.connectors.aerodrome.pool_validation import validate_aerodrome_cl_pool

    pool_check = validate_aerodrome_cl_pool(
        compiler.chain,
        position.token0,
        position.token1,
        position.tick_spacing,
        compiler._get_chain_rpc_url(),
        gateway_client=compiler._gateway_client,
        deployment=deployment,
    )
    failed = compiler._validate_pool(pool_check, intent_id)
    if failed is not None:
        return failed
    if not pool_check.pool_address:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error="Slipstream factory confirmed a position pool without returning its address",
            is_safety_refusal=True,
            intent_id=intent_id,
        )
    if _looks_like_bare_pool(expected_pool):
        assert expected_pool is not None
        if not _looks_like_evm_address(expected_pool):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Invalid exact Slipstream pool address: {expected_pool}",
                intent_id=intent_id,
            )
        if pool_check.pool_address.lower() != expected_pool.lower():
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Slipstream position tokenId={token_id} belongs to pool {pool_check.pool_address} "
                    f"({position.token0}/{position.token1} tick_spacing={position.tick_spacing}, "
                    f"{deployment.generation} generation), not exact pool {expected_pool}. "
                    f"Refusing to act on a position from a different pool."
                ),
                intent_id=intent_id,
            )
    verified_venue = _verify_slipstream_binding(
        compiler=compiler,
        pool_address=pool_check.pool_address,
        token0_address=position.token0,
        token1_address=position.token1,
        tick_spacing=position.tick_spacing,
        expected_position_manager=deployment.position_manager,
        intent_id=intent_id,
        allow_unavailable_for_risk_reduction=True,
    )
    if isinstance(verified_venue, CompilationResult):
        return verified_venue
    return _ResolvedSlipstreamPosition(deployment, verified_venue)


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
        from_price = _require_resolved_token_price(compiler, from_token)
        to_price = _require_resolved_token_price(compiler, to_token)
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
    lower: Decimal | None = None,
    upper: Decimal | None = None,
) -> tuple[int, int] | CompilationResult:
    """Validate Slipstream tick bounds: integer, ordered, aligned to tick_spacing.

    Returns ``(tick_lower, tick_upper)`` on success or a FAILED
    ``CompilationResult``. Extracted from ``compile_lp_open_aerodrome_slipstream``
    so the main path stays under the mccabe limit.

    ``lower``/``upper`` are the bounds resolved by the caller via
    :func:`lp_range_bounds` (which prefers ``range_spec`` over the legacy fields,
    so a ``TickBand``-only ``model_construct`` intent whose ``range_lower``/
    ``range_upper`` are absent still validates — VIB-5867). They default to the
    legacy fields for the standalone callers (e.g. the scaffold guard test) that
    pass a fully-populated intent.
    """
    if lower is None:
        lower = intent.range_lower
    if upper is None:
        upper = intent.range_upper
    if int(lower) != lower or int(upper) != upper:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(f"Aerodrome Slipstream tick bounds must be integers, got range_lower={lower}, range_upper={upper}"),
            intent_id=intent.intent_id,
        )
    tick_lower = int(lower)
    tick_upper = int(upper)
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


def _slipstream_price_band_to_ticks(
    intent: LPOpenIntent,
    tick_spacing: int,
    token0_decimals: int,
    token1_decimals: int,
    lower: Decimal,
    upper: Decimal,
) -> tuple[int, int] | CompilationResult:
    """Convert a Slipstream price band to spacing-aligned ticks (VIB-5867 / ALM-2901).

    Delegates every step to the shared ``cl_range`` seam so the decimals-correct
    price->tick math is written once for all concentrated-liquidity connectors.
    Before this, Slipstream was the only CL connector without a price path, which
    forced strategy authors and codegen to hand-roll ``log(price)/log(1.0001)`` —
    a formula that omits the decimals term and is therefore wrong by
    ``|decimals0 - decimals1| * 23027`` ticks (46,054 for USDC(6)/cbBTC(8); ~100x
    off in price). That hand-roll is what produced ALM-2901.

    ``lower``/``upper`` are the price bounds already resolved by the caller via
    :func:`lp_range_bounds` (so a ``PriceBand``-only intent whose legacy
    ``range_lower``/``range_upper`` are absent is handled — VIB-5867).

    ``tokens_swapped=False`` is correct here and not an assumption: the caller
    has already rejected any non-canonical pool ordering, so the user's pair
    orientation always matches the pool's ``token0``/``token1``.

    The straddle invariant is deliberately *not* enforced inside the seam
    (``current_tick=None``): the caller reads ``slot0`` after this and applies
    :func:`_slipstream_tick_straddle_failure` to BOTH the price and tick paths,
    so there stays exactly one straddle guard with one error message.

    Returns:
        ``(tick_lower, tick_upper)`` on success, or a FAILED ``CompilationResult``.
    """
    try:
        tick_range = price_band_to_ticks(
            range_lower=lower,
            range_upper=upper,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
            tokens_swapped=False,
            tick_spacing=tick_spacing,
            current_tick=None,
        )
    except (PriceBandToTicksError, ValueError) as exc:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Aerodrome Slipstream price band [{lower}, {upper}] "
                f"could not be converted to a tick range (tick_spacing={tick_spacing}, "
                f"decimals={token0_decimals}/{token1_decimals}): {exc}"
            ),
            intent_id=intent.intent_id,
        )
    return tick_range.tick_lower, tick_range.tick_upper


def _resolve_slipstream_ticks(
    intent: LPOpenIntent,
    tick_spacing: int,
    token0_decimals: int,
    token1_decimals: int,
) -> tuple[int, int] | CompilationResult:
    """Resolve a Slipstream LP_OPEN range to ticks, whichever form it was stated in.

    The bounds are resolved ONCE here via
    :func:`~almanak.framework.intents.vocabulary.lp_range_bounds` (prefers
    ``range_spec`` over the legacy fields, so a ``range_spec``-only intent whose
    ``range_lower``/``range_upper`` are absent still resolves — VIB-5867), then
    dispatched on the one shared discriminator
    (:func:`~almanak.framework.intents.vocabulary.lp_range_is_ticks`) that the
    backtest extractor also consumes, so a given intent can never be read as
    ticks by one lane and prices by the other:

    - **Price band** (the canonical, decimals-safe UX, same as every other CL
      connector) -> the shared ``cl_range`` seam, which aligns to ``tick_spacing``.
    - **Tick band** (explicit opt-in escape hatch) -> the pre-existing
      ``_validate_slipstream_tick_bounds``: raw ticks are taken literally, so they
      must already be integral, ordered and aligned.
    """
    bounds = lp_range_bounds(intent)
    if bounds is None:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error="Aerodrome Slipstream LP_OPEN has no range: provide a price band (range_lower/range_upper or a PriceBand range_spec).",
            intent_id=intent.intent_id,
        )
    lower, upper = bounds
    if lp_range_is_ticks(intent):
        return _validate_slipstream_tick_bounds(intent, tick_spacing, lower, upper)
    return _slipstream_price_band_to_ticks(intent, tick_spacing, token0_decimals, token1_decimals, lower, upper)


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


def _lp_slippage_bps(intent: Any) -> int | None:
    """Convert an LP intent's optional tolerance to basis points.

    ``max_slippage`` is optional on LP intents and permission discovery compiles
    synthetic intents that carry none, so ``None`` is a legitimate input meaning
    "use the connector default". ``slippage_to_bps`` rejects non-Decimal, so the
    conversion is guarded here rather than at each call site.
    """
    return slippage_to_bps(intent.max_slippage) if intent.max_slippage is not None else None


@dataclass(frozen=True, slots=True)
class _ResolvedAerodromeClassicPool:
    """One admitted Classic (Solidly) LP_OPEN venue, however the intent named it."""

    token0: TokenInfo
    token1: TokenInfo
    stable: bool
    pool_check: Any


def _authenticate_aerodrome_classic_pool(
    compiler: Any,
    pool_address: str,
    token0_address: str,
    token1_address: str,
    stable: bool,
    intent_id: str,
    *,
    rpc_url: str | None,
    gateway_client: Any,
    allow_unavailable: bool = False,
) -> Any | CompilationResult:
    """Require the Classic factory to round-trip ``(token0, token1, stable)`` to ``pool_address``.

    ABI shape alone is spoofable: any contract answering ``metadata()`` would
    otherwise be admitted. Returns the confirmed ``PoolValidationResult`` or a
    FAILED result; never substitutes another pool.

    ``allow_unavailable`` is the risk-reduction asymmetry the Slipstream close
    lane also carries: a factory read that could not be completed (no
    confirmation either way) lets a CLOSE proceed on the pool's own identity
    instead of stranding the position on a transient gateway fault, while a
    measured mismatch or a factory that denies the pool still refuses. New
    positions never use it.
    """
    from almanak.connectors.aerodrome.pool_validation import validate_aerodrome_pool

    pool_check = validate_aerodrome_pool(
        compiler.chain, token0_address, token1_address, stable, rpc_url, gateway_client=gateway_client
    )
    if pool_check.exists is None and allow_unavailable:
        logger.warning(
            "Aerodrome Classic close: factory authentication of %s unavailable (%s); proceeding on the pool's "
            "own identity because the close is risk-reducing",
            pool_address,
            pool_check.warning or pool_check.error or "factory lookup unavailable",
        )
        return pool_check
    if pool_check.exists is not True:
        detail = pool_check.error or pool_check.warning or "factory lookup unavailable"
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Cannot authenticate exact Aerodrome pool {pool_address} against the registered "
                f"factory on {compiler.chain}: {detail}"
            ),
            intent_id=intent_id,
        )
    canonical = pool_check.pool_address or ""
    if canonical.lower() != pool_address.lower():
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Exact Aerodrome pool {pool_address} is not the registered pool for "
                f"{token0_address}/{token1_address} stable={stable} on {compiler.chain}; "
                f"the factory returned {canonical or 'no pool'}. Refusing alternate-pool substitution."
            ),
            intent_id=intent_id,
        )
    return pool_check


def _reverse_and_authenticate_aerodrome_classic_pool(
    compiler: Any,
    pool_address: str,
    intent_id: str,
    *,
    rpc_url: str | None,
    gateway_client: Any,
    allow_unavailable: bool = False,
) -> _ResolvedAerodromeClassicPool | CompilationResult:
    """Reverse a bare Classic pool address into its identity and factory-authenticate it.

    Shared by the exact LP_OPEN lane and the address-form LP_CLOSE lane; the
    caller decides the transport (LP_OPEN applies the gateway-boundary gate,
    LP_CLOSE keeps its historical direct-or-gateway transport).
    """
    metadata = get_aerodrome_pool_metadata(compiler, pool_address)
    if metadata is None:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Could not resolve Aerodrome pool metadata for {pool_address}. "
                f"Ensure the address is a live Aerodrome V1 pool on {compiler.chain} "
                f"and that RPC/gateway access is configured."
            ),
            intent_id=intent_id,
        )
    token0_addr, token1_addr, stable = metadata
    token0_info = compiler._resolve_token(token0_addr)
    token1_info = compiler._resolve_token(token1_addr)
    if token0_info is None or token1_info is None:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Could not resolve tokens for Aerodrome pool {pool_address} "
                f"(token0={token0_addr}, token1={token1_addr})"
            ),
            intent_id=intent_id,
        )
    # The address is authoritative but ABI shape is spoofable: the registered
    # factory must round-trip the recovered tuple to THIS address before any
    # approve / addLiquidity / removeLiquidity is built.
    pool_check = _authenticate_aerodrome_classic_pool(
        compiler,
        pool_address,
        token0_addr,
        token1_addr,
        stable,
        intent_id,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
        allow_unavailable=allow_unavailable,
    )
    if isinstance(pool_check, CompilationResult):
        return pool_check
    return _ResolvedAerodromeClassicPool(token0_info, token1_info, stable, pool_check)


def _resolve_exact_aerodrome_classic_pool(
    compiler: Any,
    pool_address: str,
    intent_id: str,
) -> _ResolvedAerodromeClassicPool | CompilationResult:
    """Resolve and authenticate an exact bare-address Classic (Solidly) pool.

    The address is authoritative: the pool's own ``metadata()`` yields
    ``(token0, token1, stable)`` and the registered factory must round-trip that
    tuple to the same address. Mirrors the Slipstream and Uniswap V3 exact lanes;
    Classic has no block-anchored venue verifier, so factory authentication is
    the admission contract.
    """
    from .addresses import AERODROME

    if not _looks_like_evm_address(pool_address):
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Invalid exact Aerodrome pool address: {pool_address}",
            intent_id=intent_id,
        )
    if "factory" not in AERODROME.get(compiler.chain.lower(), {}):
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Aerodrome not supported on {compiler.chain}",
            intent_id=intent_id,
        )
    transport = _exact_pool_transport(compiler, pool_address, intent_id)
    if isinstance(transport, CompilationResult):
        return transport
    internal_rpc, gateway_client = transport
    return _reverse_and_authenticate_aerodrome_classic_pool(
        compiler, pool_address, intent_id, rpc_url=internal_rpc, gateway_client=gateway_client
    )


def _resolve_symbolic_aerodrome_classic_pool(
    compiler: Any,
    intent: LPOpenIntent,
) -> _ResolvedAerodromeClassicPool | CompilationResult:
    """Resolve a ``TOKEN0/TOKEN1/volatile|stable`` key (best-effort factory probe)."""
    from almanak.connectors.aerodrome.pool_validation import validate_aerodrome_pool

    pool_parts = intent.pool.split("/")
    if len(pool_parts) < 2:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Invalid pool format: {intent.pool}. Expected: TOKEN0/TOKEN1/volatile or TOKEN0/TOKEN1/stable, "
                "or an exact 0x pool address"
            ),
            intent_id=intent.intent_id,
        )

    token0_symbol = pool_parts[0]
    token1_symbol = pool_parts[1]
    # Default to volatile if not specified
    stable = pool_parts[2].lower() == "stable" if len(pool_parts) > 2 else False

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
    return _ResolvedAerodromeClassicPool(token0_info, token1_info, stable, pool_check)


def compile_lp_open_aerodrome(compiler, intent: LPOpenIntent) -> CompilationResult:
    """Compile LP_OPEN intent for Aerodrome Finance (Solidly fork on Base).

    Aerodrome uses a simple xy=k or x^3y+y^3x AMM with:
    - Fungible LP tokens (not NFTs)
    - Two pool types: volatile (0.3% fee) and stable (0.05% fee)
    - Full range liquidity (no concentrated positions)

    Pool format: "TOKEN0/TOKEN1/volatile" or "TOKEN0/TOKEN1/stable", or an
    exact bare ``0x…`` pool address (authenticated against the registered
    factory, see :func:`_resolve_exact_aerodrome_classic_pool`).

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

        # Two admission lanes, one resolved shape (mirrors the Slipstream and
        # Uniswap V3 exact lanes): a bare address is authenticated against the
        # registered factory; a symbolic key is resolved through it.
        if _looks_like_bare_pool(intent.pool):
            resolved = _resolve_exact_aerodrome_classic_pool(compiler, intent.pool, intent.intent_id)
        else:
            resolved = _resolve_symbolic_aerodrome_classic_pool(compiler, intent)
        if isinstance(resolved, CompilationResult):
            return resolved
        token0_info = resolved.token0
        token1_info = resolved.token1
        stable = resolved.stable
        token0_symbol = token0_info.symbol
        token1_symbol = token1_info.symbol

        logger.info(
            f"Compiling Aerodrome LP_OPEN: {token0_symbol}/{token1_symbol}, stable={stable}, amounts={intent.amount0}/{intent.amount1}"
        )

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
        lp_slippage_bps = _lp_slippage_bps(intent)
        config = AerodromeConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            deadline_seconds=compiler.default_deadline_seconds,
            price_provider=compiler.price_oracle,
            rpc_url=compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
        )
        adapter = AerodromeAdapter(config)

        liquidity_result = adapter.add_liquidity(
            token_a=token0_info.address,
            token_b=token1_info.address,
            amount_a=intent.amount0,
            amount_b=intent.amount1,
            stable=stable,
            slippage_bps=lp_slippage_bps,
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


def _clamp_lp_close_to_own_liquidity(
    intent: Any,
    lp_balance: Decimal,
    pool_address: str,
    warnings: list[str],
    result: CompilationResult,
) -> CompilationResult | tuple[Decimal, int]:
    """Bound an Aerodrome LP_CLOSE to THIS deployment's own liquidity (VIB-6162).

    ``lp_balance`` as passed in is the wallet's ENTIRE balance in this pool, and on a
    Solidly fork the pool contract IS the LP token, so handing it to ``removeLiquidity``
    burns any LP the strategy never minted: a user's own position, or a sibling
    deployment's. The teardown lane attaches the ledger-side figure; enforcement lands
    in the connector because only the connector has the live balance, and the framework
    deliberately never reads the chain.

    **An absent bound means full withdrawal.** For ``almanak ax lp-close`` that is
    correct and deliberate: its help says "fully withdraw" and its operator IS the wallet
    owner acting on their own position. Neither runner lane arrives here unbounded: the
    teardown lane refuses before compiling, and the iteration lane
    (``StrategyRunner._step_attach_lp_outstanding``, VIB-6517) attaches either the
    ledger figure or a non-numeric ``"unmeasured: ..."`` sentinel that the validation
    branch below rejects as a safety refusal — so a lifecycle / rebalance / depeg
    ``LP_CLOSE`` is clamped or refused, never a whole-balance burn. That sentinel is a
    deliberate consumer of this branch: keep the non-numeric path a refusal
    (``is_safety_refusal=True``), never a fallback to the full balance.

    Returns:
        * :class:`CompilationResult` — the caller returns it verbatim. Either a refusal,
          or the no-op close for a deployment holding no outstanding LP. **A no-op is not
          a failure**: it means there is nothing of ours left to withdraw, and burning the
          wallet's balance instead is the defect this exists to prevent.
        * ``(lp_balance, lp_balance_wei)`` — the bounded amount to withdraw.

    Extracted from ``compile_lp_close_aerodrome`` for the CRAP gate. Pure move: every
    branch, message and return value is unchanged from the inline form.
    """
    outstanding_raw = (getattr(intent, "protocol_params", None) or {}).get("deployment_outstanding_lp")
    if outstanding_raw is None:
        return lp_balance, int(lp_balance * Decimal(10**18))
    try:
        outstanding = Decimal(str(outstanding_raw))
    except (ArithmeticError, ValueError):
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"VIB-6162: deployment_outstanding_lp={outstanding_raw!r} is not a "
                f"number; refusing to close rather than burning the wallet's whole "
                f"LP balance in pool {pool_address}"
            ),
            intent_id=intent.intent_id,
            is_safety_refusal=True,
        )
    try:
        lp_balance = bound_close_amount(outstanding, lp_balance, pool_key=pool_address)
    except LpClampUnresolved as exc:
        # Refusal, never min(): with outstanding=100, foreign=50 and 60 of the
        # strategy's own LP moved out, min(100, 90) would burn 90 -- including all 50
        # foreign shares, which is this very defect.
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"VIB-6162: refusing LP_CLOSE on pool {pool_address} — {exc}",
            intent_id=intent.intent_id,
            is_safety_refusal=True,
        )
    lp_balance_wei = int(lp_balance * Decimal(10**18))
    logger.info(
        "VIB-6162: clamped Aerodrome LP_CLOSE to this deployment's own %s LP "
        "(%s wei); the rest of the wallet's balance in pool %s is left untouched",
        lp_balance,
        lp_balance_wei,
        pool_address,
    )
    if lp_balance_wei == 0:
        warning = (
            f"VIB-6162: this deployment has no outstanding LP in pool {pool_address} "
            f"— treating LP_CLOSE as a no-op rather than burning the wallet's balance"
        )
        warnings.append(warning)
        logger.info(warning)
        result.action_bundle = ActionBundle(
            intent_type=IntentType.LP_CLOSE.value,
            transactions=[],
            metadata={
                "pool": intent.position_id,
                "pool_address": pool_address,
                "protocol": "aerodrome",
                "collect_fees": intent.collect_fees,
                "no_op": True,
                "reason": "Deployment holds no outstanding LP; LP_CLOSE no-op (VIB-6162)",
            },
        )
        result.transactions = []
        result.total_gas_estimate = 0
        result.warnings = warnings
        return result
    return lp_balance, lp_balance_wei


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
            # A bare ``pool`` on the intent must name the same contract as the
            # bare ``position_id``; the two are one identity for a Classic LP.
            supplied_pool = getattr(intent, "pool", None)
            if _looks_like_bare_pool(supplied_pool) and str(supplied_pool).lower() != position_id_raw.lower():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Aerodrome LP_CLOSE names pool {supplied_pool} but position_id {position_id_raw} "
                        f"is a different pool contract; refusing to close against a mismatched pool."
                    ),
                    intent_id=intent.intent_id,
                )
            resolved_close = _reverse_and_authenticate_aerodrome_classic_pool(
                compiler,
                position_id_raw,
                intent.intent_id,
                rpc_url=compiler._get_chain_rpc_url(),
                gateway_client=compiler._gateway_client,
                allow_unavailable=True,
            )
            if isinstance(resolved_close, CompilationResult):
                return resolved_close
            token0_info, token1_info, stable = resolved_close.token0, resolved_close.token1, resolved_close.stable
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
        lp_slippage_bps = _lp_slippage_bps(intent)
        config = AerodromeConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            deadline_seconds=compiler.default_deadline_seconds,
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

        # VIB-6162 — bound the burn to THIS deployment's own liquidity. Extracted so the
        # parent stays under the CRAP gate; the guard itself is unchanged. The helper has
        # ONE exit shape: a CompilationResult the caller returns verbatim (refusal, or the
        # no-op close), or the bounded amounts to withdraw.
        clamped = _clamp_lp_close_to_own_liquidity(intent, lp_balance, pool_address, warnings, result)
        if isinstance(clamped, CompilationResult):
            return clamped
        lp_balance, lp_balance_wei = clamped

        # Build removeLiquidity transaction using the adapter
        # Pass pre-resolved pool_address so the adapter doesn't make
        # its own direct RPC call (which fails in deployed mode).
        liquidity_result = adapter.remove_liquidity(
            token_a=token0_symbol,
            token_b=token1_symbol,
            liquidity=lp_balance,
            stable=stable,
            slippage_bps=lp_slippage_bps,
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
# Ordered CL tick-spacing candidates probed in the pure-auto routing case
# (VIB-5548 / ALM-2889, design O3). 100 first preserves the historical default;
# 200/50/1/2000 cover the remaining live Slipstream spacings. Probes are
# read-only ``getPool`` eth_calls, memoized per compile cycle, and the loop
# stops at the first existing pool — so at most five reads, usually one.
_CL_CANDIDATE_TICK_SPACINGS: tuple[int, ...] = (100, 200, 50, 1, 2000)


@dataclass(frozen=True)
class _AerodromeRoute:
    """Resolved Aerodrome swap route (VIB-5548).

    Carries the chosen venue (CL vs Classic), the parameters needed to build the
    swap, the on-chain pool-existence probe that selected it (still passed through
    the compiler's ``_validate_pool`` fail-closed gate), and provenance flags for
    bundle metadata / logging.
    """

    use_classic: bool
    tick_spacing: int | None
    stable: bool
    pool_check: Any
    fallback_used: bool
    routing: str  # "cl" | "classic"
    degraded: bool = False
    # Exact pool the caller pinned via ``swap_params={"pool": "0x..."}`` after
    # factory authentication; None for auto/symbolic routing.
    pinned_pool: str | None = None


def _aerodrome_chain_has_cl(chain_addrs: dict[str, str]) -> bool:
    """Hard capability gate: True iff this chain has Slipstream CL contracts.

    Optimism/Velodrome has no CL factory/router, so it is Classic-only.
    """
    return bool(chain_addrs.get("cl_router") and chain_addrs.get("cl_factory"))


def _aerodrome_is_offline(compiler) -> bool:
    """True in placeholder-price or permission-discovery mode.

    In these modes pool-existence probes cannot be trusted (they run against
    unreachable RPC / only need calldata shapes), so auto-routing degrades to the
    legacy default rather than fail-closing on an unverifiable probe.
    """
    cfg = getattr(compiler, "_config", None)
    return bool(getattr(cfg, "using_placeholders", False) or getattr(cfg, "permission_discovery", False))


def _aerodrome_stable_pair(from_symbol: str, to_symbol: str) -> bool:
    """True when BOTH legs are known USD stablecoins (design O4)."""
    from .addresses import AERODROME_STABLE_SYMBOLS

    return from_symbol.upper() in AERODROME_STABLE_SYMBOLS and to_symbol.upper() in AERODROME_STABLE_SYMBOLS


def _aerodrome_cached_probe(compiler, kind: str, from_addr: str, to_addr: str, variant):
    """Memoized (per compile cycle) read-only pool-existence probe.

    ``kind`` is ``"cl"`` (``variant`` = tick spacing) or ``"classic"``
    (``variant`` = ``stable`` bool). Calls are routed through the
    ``pool_validation`` module attribute so test patches are honoured.
    """
    from . import pool_validation

    cache = getattr(compiler, "_aerodrome_pool_probe_cache", None)
    if cache is None:
        cache = {}
        try:
            compiler._aerodrome_pool_probe_cache = cache
        except (AttributeError, TypeError):  # pragma: no cover - defensive (slotted impls)
            cache = None
    key = (kind, from_addr.lower(), to_addr.lower(), variant, compiler.chain)
    if cache is not None and key in cache:
        return cache[key]

    rpc_url = compiler._get_chain_rpc_url()
    gateway_client = compiler._gateway_client
    if kind == "cl":
        result = pool_validation.validate_aerodrome_cl_pool(
            compiler.chain, from_addr, to_addr, variant, rpc_url, gateway_client=gateway_client
        )
    else:
        result = pool_validation.validate_aerodrome_pool(
            compiler.chain, from_addr, to_addr, bool(variant), rpc_url, gateway_client=gateway_client
        )
    if cache is not None:
        cache[key] = result
    return result


def _resolve_aerodrome_classic_route(
    compiler, from_token: Any, to_token: Any, *, stable_req: bool | None, fallback: bool, probed: list[str]
) -> _AerodromeRoute:
    """Resolve a Classic (Solidly) route, probing pool type(s) in order.

    Order (design O4): an explicit ``stable`` honoured as the only type; else
    stable-first when both legs are known stablecoins, else volatile-first. The
    first existing pool wins; an unverifiable probe (``exists is None``) degrades
    to that type (``_validate_pool`` then warns-and-proceeds); if every probed
    type is confirmed absent the last (absent) probe is returned so the caller
    can compose a fail-closed result.
    """
    if stable_req is not None:
        order: tuple[bool, ...] = (bool(stable_req),)
    elif _aerodrome_stable_pair(from_token.symbol, to_token.symbol):
        order = (True, False)
    else:
        order = (False, True)

    last_check = None
    for stable in order:
        pool_check = _aerodrome_cached_probe(compiler, "classic", from_token.address, to_token.address, stable)
        probed.append(f"classic(stable={stable})")
        last_check = pool_check
        if pool_check.exists is True:
            return _AerodromeRoute(True, None, stable, pool_check, fallback, "classic")
        if pool_check.exists is None:
            return _AerodromeRoute(True, None, stable, pool_check, fallback, "classic", degraded=True)
        # exists is False -> try the next pool type
    return _AerodromeRoute(True, None, order[-1], last_check, fallback, "classic")


def _resolve_aerodrome_cl_route(
    compiler, from_token: Any, to_token: Any, *, probed: list[str]
) -> _AerodromeRoute | None:
    """Probe CL (Slipstream) pools across the candidate tick spacings (VIB-5548).

    First *confirmed* pool wins. If a probe is unverifiable (``exists is None`` —
    e.g. a missing factory entry or malformed response), degrade to the legacy
    CL@100 default **carrying that unverifiable probe** so the caller's
    ``_validate_pool`` gate warns-and-proceeds instead of fail-closing on a
    previously-cached absent CL@100 probe. Returns ``None`` only when every
    candidate spacing is *confirmed absent*, leaving the caller to either fall
    back to Classic (auto) or fail closed (explicit ``classic=False``).
    """
    for ts in _CL_CANDIDATE_TICK_SPACINGS:
        pool_check = _aerodrome_cached_probe(compiler, "cl", from_token.address, to_token.address, ts)
        probed.append(f"cl(tick_spacing={ts})")
        if pool_check.exists is True:
            return _AerodromeRoute(False, ts, False, pool_check, False, "cl")
        if pool_check.exists is None:
            # Unverifiable while online (missing factory entry / malformed
            # response). Degrade to the legacy default and warn-and-proceed,
            # carrying THIS probe (exists=None) so _validate_pool does not
            # fail-close on the earlier confirmed-absent CL@100 probe.
            logger.info(
                "Aerodrome routing could not verify CL pool for %s->%s at tick_spacing=%s (reason=%s); "
                "defaulting to CL@100 (warn-and-proceed).",
                from_token.symbol,
                to_token.symbol,
                ts,
                getattr(pool_check, "reason", None),
            )
            return _AerodromeRoute(False, 100, False, pool_check, False, "cl", degraded=True)
        # exists is False -> probe the next candidate tick spacing
    return None


def _pinned_route_conflict(pool: str, detail: str, intent_id: str) -> CompilationResult:
    return CompilationResult(
        status=CompilationStatus.FAILED,
        error=f"swap_params conflict for pinned pool {pool}: {detail}. Drop the conflicting key.",
        intent_id=intent_id,
    )


def _pinned_pair_mismatch(
    pool: str, token0: str, token1: str, from_token: Any, to_token: Any, intent_id: str
) -> CompilationResult:
    return CompilationResult(
        status=CompilationStatus.FAILED,
        error=(
            f"Pinned pool {pool} holds pair {token0}/{token1}, which does not match the swap pair "
            f"{from_token.address}/{to_token.address}. Pin the pool for the pair actually being swapped."
        ),
        intent_id=intent_id,
    )


def _pin_aerodrome_classic_route(
    compiler,
    pool: str,
    classic: tuple[str, str, bool],
    swap_params: dict[str, Any],
    from_token: Any,
    to_token: Any,
    intent_id: str,
) -> _AerodromeRoute | CompilationResult:
    """Pin a swap to one factory-authenticated Classic (Solidly) pool."""
    token0, token1, stable = classic
    if swap_params.get("classic") is False:
        return _pinned_route_conflict(pool, "it is a Classic pool but classic=False was given", intent_id)
    if swap_params.get("tick_spacing") is not None:
        return _pinned_route_conflict(pool, "it is a Classic pool but tick_spacing was given", intent_id)
    stable_req = swap_params.get("stable")
    if stable_req is not None and bool(stable_req) != stable:
        return _pinned_route_conflict(pool, f"the pool is stable={stable} but stable={stable_req} was given", intent_id)
    if {token0.lower(), token1.lower()} != {from_token.address.lower(), to_token.address.lower()}:
        return _pinned_pair_mismatch(pool, token0, token1, from_token, to_token, intent_id)
    pool_check = _authenticate_aerodrome_classic_pool(
        compiler,
        pool,
        token0,
        token1,
        stable,
        intent_id,
        rpc_url=compiler._get_chain_rpc_url(),
        gateway_client=compiler._gateway_client,
    )
    if isinstance(pool_check, CompilationResult):
        return pool_check
    return _AerodromeRoute(True, None, stable, pool_check, False, "classic", pinned_pool=pool)


def _pin_aerodrome_cl_route(
    compiler,
    pool: str,
    binding: Any,
    swap_params: dict[str, Any],
    from_token: Any,
    to_token: Any,
    chain_addrs: dict[str, str],
    intent_id: str,
) -> _AerodromeRoute | CompilationResult:
    """Pin a swap to one Slipstream CL pool reachable by the registered swap router.

    The router derives the pool from ITS factory as ``(pair, tickSpacing)``, so
    the pinned pool must report that same factory; a pool from another
    generation would resolve to a plausible binding yet execute elsewhere.
    """
    from . import pool_validation

    chain = compiler.chain
    if not _aerodrome_chain_has_cl(chain_addrs):
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Pinned pool {pool} is a Slipstream CL pool but CL routing is not available on {chain}.",
            intent_id=intent_id,
        )
    if swap_params.get("classic") is True:
        return _pinned_route_conflict(pool, "it is a Slipstream CL pool but classic=True was given", intent_id)
    if swap_params.get("stable") is not None:
        return _pinned_route_conflict(pool, "it is a Slipstream CL pool but stable was given", intent_id)
    ts_req = swap_params.get("tick_spacing")
    if ts_req is not None and int(ts_req) != binding.tick_spacing:
        return _pinned_route_conflict(
            pool, f"the pool has tick_spacing={binding.tick_spacing} but tick_spacing={ts_req} was given", intent_id
        )
    if {binding.token0, binding.token1} != {from_token.address.lower(), to_token.address.lower()}:
        return _pinned_pair_mismatch(pool, binding.token0, binding.token1, from_token, to_token, intent_id)
    router_factory = chain_addrs["cl_factory"]
    if binding.factory.lower() != router_factory.lower():
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Pinned pool {pool} reports Slipstream factory {binding.factory}, but the registered Slipstream "
                f"swap router on {chain} routes through factory {router_factory}; a swap cannot be pinned to a "
                f"pool the router cannot reach. Use the {router_factory} pool for this pair, or pin by tick_spacing."
            ),
            intent_id=intent_id,
        )
    pool_check = pool_validation.validate_aerodrome_cl_pool(
        chain,
        binding.token0,
        binding.token1,
        binding.tick_spacing,
        compiler._get_chain_rpc_url(),
        gateway_client=compiler._gateway_client,
    )
    if pool_check.exists is not True:
        detail = pool_check.error or pool_check.warning or "factory lookup unavailable"
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Cannot authenticate pinned pool {pool} against the Slipstream factory on {chain}: {detail}",
            intent_id=intent_id,
        )
    canonical = pool_check.pool_address or ""
    if canonical.lower() != pool.lower():
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Pinned pool {pool} is not the Slipstream pool for {binding.token0}/{binding.token1} "
                f"tick_spacing {binding.tick_spacing} on {chain}; the factory returned {canonical or 'no pool'}. "
                f"Refusing alternate-pool substitution."
            ),
            intent_id=intent_id,
        )
    return _AerodromeRoute(False, binding.tick_spacing, False, pool_check, False, "cl", pinned_pool=pool)


def _resolve_aerodrome_pinned_route(
    compiler,
    intent_id: str,
    from_token: Any,
    to_token: Any,
    swap_params: dict[str, Any],
    chain_addrs: dict[str, str],
) -> _AerodromeRoute | CompilationResult:
    """Honour ``swap_params={"pool": "0x..."}`` as an exact, factory-authenticated pin.

    Mirrors the Uniswap V3 pinned-pool resolver: the pool's own identity is read
    on-chain, its pair must match the swap pair, and the registered factory must
    round-trip that identity to the same address. A pin either executes against
    exactly that pool or fails compilation; it is never downgraded to auto
    routing. The family (Classic vs Slipstream CL) is discriminated by which ABI
    the contract answers: Solidly pools expose ``metadata()``, CL pools expose
    ``tickSpacing()``; each family reverts on the other's selector.
    """
    from . import pool_validation

    pool = str(swap_params["pool"])
    if not _looks_like_evm_address(pool):
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Invalid pinned pool address: {pool}",
            intent_id=intent_id,
        )
    classic = get_aerodrome_pool_metadata(compiler, pool)
    if classic is not None:
        return _pin_aerodrome_classic_route(compiler, pool, classic, swap_params, from_token, to_token, intent_id)
    binding = pool_validation.read_slipstream_cl_pool_binding(
        pool, compiler._get_chain_rpc_url(), chain=compiler.chain, gateway_client=compiler._gateway_client
    )
    if binding is None:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Cannot resolve pinned pool {pool} on {compiler.chain}: it answers neither the Aerodrome Classic "
                f"metadata() ABI nor the Slipstream token0()/token1()/tickSpacing()/factory() ABI, or no "
                f"RPC/gateway is available."
            ),
            intent_id=intent_id,
        )
    return _pin_aerodrome_cl_route(compiler, pool, binding, swap_params, from_token, to_token, chain_addrs, intent_id)


def _resolve_aerodrome_route(  # noqa: C901 - explicit, flat routing-priority ladder (design O2/O3)
    compiler, intent: SwapIntent, from_token: Any, to_token: Any, swap_params: dict[str, Any]
) -> _AerodromeRoute | CompilationResult:
    """Resolve the per-pair Aerodrome/Velodrome swap route (VIB-5548 / ALM-2889).

    Routing priority (design §b):

    1. ``chain_has_cl`` — hard capability gate (Velodrome/Optimism is Classic-only).
    2. Explicit ``classic=True`` -> Classic only.
    3. Explicit ``classic=False`` -> CL only; fail closed if absent (never silently
       route to Classic against an explicit choice).
    4. Explicit ``tick_spacing`` -> CL at that exact spacing, probe once, no fallback.
    5. Auto -> probe CL across :data:`_CL_CANDIDATE_TICK_SPACINGS`; first hit wins.
       No CL pool -> probe Classic and use it with ``fallback_used=True``. Neither
       -> fail closed listing what was probed.

    Returns a :class:`_AerodromeRoute` (whose ``pool_check`` the caller still
    feeds through ``_validate_pool``), or a FAILED ``CompilationResult`` for the
    fail-closed cases the single-result gate cannot express.
    """
    from .addresses import AERODROME as AERODROME_ADDRESSES

    chain = compiler.chain
    intent_id = intent.intent_id
    if chain not in AERODROME_ADDRESSES:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Aerodrome/Velodrome is not supported on {chain}. Supported: {list(AERODROME_ADDRESSES.keys())}",
            intent_id=intent_id,
        )

    chain_addrs = AERODROME_ADDRESSES[chain]
    has_cl = _aerodrome_chain_has_cl(chain_addrs)
    classic_req = swap_params.get("classic")
    ts_req = swap_params.get("tick_spacing")
    stable_req = swap_params.get("stable")
    probed: list[str] = []

    # (0) Exact pool pin. Resolved on-chain and factory-authenticated; never
    #     downgraded to the auto ladder. Only offline PERMISSION DISCOVERY
    #     bypasses it (swap permissions are router-scoped, so the manifest is
    #     identical for every pool of the pair) — the same rule as the V3 lane.
    #     Placeholder pricing does not waive the pin: a pin that cannot be
    #     read on-chain fails closed rather than routing elsewhere.
    if swap_params.get("pool") is not None:
        if getattr(getattr(compiler, "_config", None), "permission_discovery", False):
            logger.info("Aerodrome permission discovery: pinned pool %s not resolved", swap_params["pool"])
        else:
            return _resolve_aerodrome_pinned_route(compiler, intent_id, from_token, to_token, swap_params, chain_addrs)

    # (2) Explicit Classic.
    if classic_req is True:
        return _resolve_aerodrome_classic_route(
            compiler, from_token, to_token, stable_req=stable_req, fallback=False, probed=probed
        )

    # (1) CL not available on this chain.
    if not has_cl:
        if classic_req is False or ts_req is not None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"CL (Slipstream) routing is not available on {chain}; use classic routing instead.",
                intent_id=intent_id,
            )
        return _resolve_aerodrome_classic_route(
            compiler, from_token, to_token, stable_req=stable_req, fallback=False, probed=probed
        )

    # --- chain has CL from here ---

    # Offline/placeholder cannot verify pools -> legacy default CL@100 for every
    # CL-eligible path (auto, classic=False, pinned tick_spacing). Permission
    # discovery only needs calldata shapes, so degrade rather than fail-close.
    if _aerodrome_is_offline(compiler):
        ts = ts_req if ts_req is not None else 100
        pool_check = _aerodrome_cached_probe(compiler, "cl", from_token.address, to_token.address, ts)
        logger.info(
            "Aerodrome routing offline/placeholder for %s->%s: defaulting to CL@%s (warn-and-proceed).",
            from_token.symbol,
            to_token.symbol,
            ts,
        )
        return _AerodromeRoute(False, ts, False, pool_check, False, "cl", degraded=True)

    # (3) Explicit CL-only (classic=False): never fall back to Classic. A pinned
    #     tick_spacing probes that exact pool once; otherwise probe CL across the
    #     candidate spacings (first hit wins) and fail closed if none exists.
    if classic_req is False:
        if ts_req is not None:
            pool_check = _aerodrome_cached_probe(compiler, "cl", from_token.address, to_token.address, ts_req)
            return _AerodromeRoute(False, ts_req, False, pool_check, False, "cl")
        cl_route = _resolve_aerodrome_cl_route(compiler, from_token, to_token, probed=probed)
        if cl_route is not None:
            return cl_route
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"No Aerodrome CL pool found for {from_token.symbol}->{to_token.symbol} on {chain}; "
                f"swap_params={{'classic': False}} forbids Classic fallback. Probed: {', '.join(probed)}."
            ),
            intent_id=intent_id,
        )

    # (4) Explicit tick_spacing (classic unset): CL at that spacing, no fallback.
    if ts_req is not None:
        pool_check = _aerodrome_cached_probe(compiler, "cl", from_token.address, to_token.address, ts_req)
        return _AerodromeRoute(False, ts_req, False, pool_check, False, "cl")

    # (5) Auto. Probe CL across the candidate spacings; first hit wins, an
    #     unverifiable probe degrades to legacy CL@100 (warn-and-proceed).
    cl_route = _resolve_aerodrome_cl_route(compiler, from_token, to_token, probed=probed)
    if cl_route is not None:
        return cl_route

    # No CL pool at any candidate spacing -> bounded auto fallback to Classic.
    classic_route = _resolve_aerodrome_classic_route(
        compiler, from_token, to_token, stable_req=stable_req, fallback=True, probed=probed
    )
    if classic_route.pool_check.exists is True:
        logger.info(
            "Aerodrome auto-routing: no CL pool for %s->%s at tick spacings %s; "
            "falling back to Classic (stable=%s). swap_params={'classic': False} forbids this.",
            from_token.symbol,
            to_token.symbol,
            _CL_CANDIDATE_TICK_SPACINGS,
            classic_route.stable,
        )
        return classic_route
    if classic_route.degraded:
        return classic_route
    # Neither a CL pool nor a Classic pool exists -> fail closed.
    return CompilationResult(
        status=CompilationStatus.FAILED,
        error=(
            f"No Aerodrome pool found for {from_token.symbol}->{to_token.symbol} on {chain}. "
            f"Probed: {', '.join(probed)}."
        ),
        intent_id=intent_id,
    )


def compile_swap_aerodrome(compiler, intent: SwapIntent) -> CompilationResult:  # noqa: C901
    """Compile SWAP intent for Aerodrome/Velodrome (Solidly forks).

    Routing is resolved per-pair by :func:`_resolve_aerodrome_route` (VIB-5548):
    on Base (Aerodrome) it auto-routes to a Slipstream CL pool and falls back to
    a Classic pool when no CL pool exists; on Optimism (Velodrome) it is
    Classic-only. The ``swap_params`` escape hatch overrides routing:

    - ``classic`` (bool): force Classic (True) / CL-only, no fallback (False).
    - ``tick_spacing`` (positive int): pin a CL pool's tick spacing, no fallback.
    - ``stable`` (bool): Classic stable vs volatile pool type.

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
            price = _require_resolved_token_price(compiler, from_token)
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

        # Extract routing params from the (now-reachable, VIB-5548) swap_params
        # escape hatch and resolve the per-pair route.
        swap_params: dict[str, Any] = intent.swap_params or {}
        route = _resolve_aerodrome_route(compiler, intent, from_token, to_token, swap_params)
        if isinstance(route, CompilationResult):
            return route

        use_classic = route.use_classic
        stable = route.stable
        # Adapter needs a concrete tick spacing even for Classic routing (unused
        # there); default to 100 when the route did not pin one.
        tick_spacing = route.tick_spacing if route.tick_spacing is not None else 100
        routing = route.routing

        if route.fallback_used:
            logger.info(
                "Aerodrome SWAP %s->%s: routing fallback engaged (CL->%s).",
                from_token.symbol,
                to_token.symbol,
                routing,
            )
        logger.info(
            f"Compiling Aerodrome SWAP ({routing}): {from_token.symbol} -> {to_token.symbol}, amount={amount_decimal}"
        )

        # The resolved pool still passes the fail-closed _validate_pool gate.
        failed = compiler._validate_pool(route.pool_check, intent.intent_id)
        if failed is not None:
            return failed

        # Create Aerodrome adapter
        config = AerodromeConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            default_slippage_bps=slippage_to_bps(intent.max_slippage),
            deadline_seconds=compiler.default_deadline_seconds,
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
            "swap_token_meta": build_swap_token_meta(from_token, to_token, chain=compiler.chain),
            "amount_in": str(amount_decimal),
            "routing": routing,
            "routing_fallback": route.fallback_used,
            "stable": stable,
            "protocol": "aerodrome",
        }
        # tick_spacing is only meaningful for CL routing; omit for Classic.
        if not use_classic:
            metadata["tick_spacing"] = tick_spacing
        if route.pinned_pool is not None:
            metadata["pinned_pool"] = route.pinned_pool
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

    except SlippagePrecisionError as e:
        logger.error("Aerodrome SWAP refused by slippage precision guard: %s", e)
        result.status = CompilationStatus.FAILED
        result.error = str(e)
        result.is_safety_refusal = True
    except Exception as e:
        logger.exception(f"Failed to compile Aerodrome SWAP intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


# crap-allowlist: VIB-4853 — import-path swap only (pool-validation moved into connectors, #2527); function body unchanged, anvil-only coverage. Refactor + coverage backfill tracked in VIB-4139.
@dataclass(frozen=True, slots=True)
class _ResolvedSlipstreamPool:
    """One admitted Slipstream LP_OPEN venue, however the intent named it.

    Both the symbolic ``TOKEN0/TOKEN1/tick_spacing`` lane and the exact
    bare-address lane resolve to this shape before the shared range, straddle,
    amount-recompute, verifier, and mint body runs. ``pool_check`` is always a
    factory-confirmed result carrying the pool address.
    """

    token0: TokenInfo
    token1: TokenInfo
    tick_spacing: int
    deployment: SlipstreamDeployment
    pool_check: Any


def _resolve_symbolic_slipstream_pool(
    compiler: _AerodromeCompileImpl,
    intent: LPOpenIntent,
) -> _ResolvedSlipstreamPool | CompilationResult:
    """Resolve a ``TOKEN0/TOKEN1/tick_spacing`` key through the current reviewed factory."""
    from almanak.connectors.aerodrome.pool_validation import validate_aerodrome_cl_pool

    pool_parts = intent.pool.split("/")
    if len(pool_parts) < 3:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Invalid pool format for aerodrome_slipstream: '{intent.pool}'. "
                "Expected: TOKEN0/TOKEN1/tick_spacing (e.g. WETH/USDC/200) or an exact 0x pool address"
            ),
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

    deployments = slipstream_lp_deployments(compiler.chain)
    if not deployments:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Aerodrome Slipstream CL not supported on chain '{compiler.chain}'. Only 'base' is supported.",
            intent_id=intent.intent_id,
        )
    # New positions always use the newest reviewed factory/NPM pair.
    # Historical pairs remain available only to close positions they own.
    deployment = deployments[0]

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
    pool_check = validate_aerodrome_cl_pool(
        compiler.chain,
        token0_info.address,
        token1_info.address,
        tick_spacing,
        compiler._get_chain_rpc_url(),
        gateway_client=compiler._gateway_client,
        deployment=deployment,
    )
    failed = compiler._validate_pool(pool_check, intent.intent_id)
    if failed is not None:
        return failed

    if not pool_check.pool_address:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error="Slipstream factory confirmed a pool without returning its address",
            is_safety_refusal=True,
            intent_id=intent.intent_id,
        )
    return _ResolvedSlipstreamPool(token0_info, token1_info, tick_spacing, deployment, pool_check)


def _resolve_exact_slipstream_pool(
    compiler: _AerodromeCompileImpl,
    pool_address: str,
    intent_id: str,
) -> _ResolvedSlipstreamPool | CompilationResult:
    """Resolve and authenticate an exact bare-address Slipstream LP pool.

    The address is authoritative (ALM-3462): the compiler reads the pool's
    ``token0``/``token1``/``tickSpacing``/``factory`` through the gateway,
    selects the reviewed factory/NPM generation the pool claims, and requires
    that reviewed factory to return the same address for the tuple. No symbol
    inference, tick-spacing auto-selection, or alternate-pool substitution is
    permitted — the same contract as the Uniswap V3-family exact lane.

    New positions are still admitted only through the current reviewed
    generation (blueprint 05, VIB-6679); an authenticated legacy-generation
    pool is refused with the generation named so the caller can pick the
    current-generation venue for that pair (generation policy is ALM-3451).
    """
    from almanak.connectors.aerodrome.pool_validation import (
        read_slipstream_cl_pool_binding,
        validate_aerodrome_cl_pool,
    )

    if not _looks_like_evm_address(pool_address):
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Invalid exact Slipstream pool address: {pool_address}",
            intent_id=intent_id,
        )

    deployments = slipstream_lp_deployments(compiler.chain)
    if not deployments:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=f"Aerodrome Slipstream CL not supported on chain '{compiler.chain}'. Only 'base' is supported.",
            intent_id=intent_id,
        )

    transport = _exact_pool_transport(compiler, pool_address, intent_id)
    if isinstance(transport, CompilationResult):
        return transport
    internal_rpc, gateway_client = transport

    binding = read_slipstream_cl_pool_binding(
        pool_address,
        internal_rpc,
        chain=compiler.chain,
        gateway_client=gateway_client,
    )
    if binding is None:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Cannot resolve exact Slipstream pool {pool_address} on {compiler.chain}: "
                "token0()/token1()/tickSpacing()/factory() reads failed or returned non-pool values."
            ),
            intent_id=intent_id,
        )

    deployment = slipstream_deployment_for_factory(compiler.chain, binding.factory)
    if deployment is None:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Exact Slipstream pool {pool_address} reports unreviewed factory {binding.factory} "
                f"on {compiler.chain}; only reviewed Slipstream factory generations are admitted."
            ),
            intent_id=intent_id,
        )

    token0_info = compiler._resolve_token(binding.token0)
    token1_info = compiler._resolve_token(binding.token1)
    if token0_info is None or token1_info is None:
        missing = binding.token0 if token0_info is None else binding.token1
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Cannot resolve token metadata for {missing} from exact Slipstream pool {pool_address} "
                f"on {compiler.chain}."
            ),
            intent_id=intent_id,
        )

    # Authenticate the pool's self-reported tuple: the reviewed factory it
    # claims must round-trip that tuple to THIS address. A pool that merely
    # answers the ABI (a fork, a spoof, an unreviewed deployment) fails here.
    pool_check = validate_aerodrome_cl_pool(
        compiler.chain,
        binding.token0,
        binding.token1,
        binding.tick_spacing,
        internal_rpc,
        gateway_client=gateway_client,
        deployment=deployment,
    )
    if pool_check.exists is not True:
        detail = pool_check.error or pool_check.warning or "factory lookup unavailable"
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Cannot authenticate exact Slipstream pool {pool_address} against the reviewed "
                f"{deployment.generation} factory {deployment.factory} on {compiler.chain}: {detail}"
            ),
            intent_id=intent_id,
        )
    canonical = pool_check.pool_address or ""
    if canonical.lower() != pool_address.lower():
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Exact Slipstream pool {pool_address} is not the reviewed {deployment.generation} factory's pool "
                f"for {binding.token0}/{binding.token1} tick_spacing {binding.tick_spacing} on {compiler.chain}; "
                f"the factory returned {canonical or 'no pool'}. Refusing alternate-pool substitution."
            ),
            intent_id=intent_id,
        )

    current = deployments[0]
    if deployment != current:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Exact Slipstream pool {pool_address} belongs to the {deployment.generation} factory generation "
                f"({deployment.factory}); new positions are admitted only through the current reviewed "
                f"factory/position-manager pair ({current.factory}). Name the current-generation pool for "
                f"{token0_info.symbol}/{token1_info.symbol} instead (generation policy: ALM-3451)."
            ),
            intent_id=intent_id,
        )

    return _ResolvedSlipstreamPool(token0_info, token1_info, binding.tick_spacing, deployment, pool_check)


def compile_lp_open_aerodrome_slipstream(compiler, intent: LPOpenIntent) -> CompilationResult:  # noqa: C901 - VIB-6217 safety-refusal handler is +1 branch on a function already AT the cap (15); decomposition is VIB-4139, see below
    """Compile LP_OPEN intent for Aerodrome Slipstream CL (concentrated liquidity).

    Aerodrome Slipstream uses Uniswap V3-style concentrated liquidity with NFT positions.
    Pool format: "TOKEN0/TOKEN1/200" (tick_spacing as 3rd component, integer),
    or an exact bare ``0x…`` pool address. A bare address is an execution
    constraint, not a discovery hint: the pool's own token0/token1/tickSpacing
    are read through the gateway and the reviewed factory must round-trip that
    tuple to the same address (see :func:`_resolve_exact_slipstream_pool`).
    In that form ``amount0``/``amount1`` and the range follow the pool
    contract's canonical token0/token1 orientation.

    The intent's ``range_lower``/``range_upper`` may be stated either way, and the
    form is resolved by the shared discriminator (see
    :func:`_resolve_slipstream_ticks`):

    - a **price band** (``PriceBand`` range_spec, or positive fractional legacy
      bounds) — the canonical UX shared with every other CL connector, converted
      here with decimals-correct math via the ``cl_range`` seam;
    - a **tick band** (``TickBand`` range_spec, or legacy tick-shaped bounds) —
      raw Slipstream ticks, taken literally.

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

        # Two admission lanes, one resolved shape. A bare address is exact and
        # authenticated against the pool's own reviewed factory; a
        # symbolic key is resolved through the current reviewed factory.
        if _looks_like_bare_pool(intent.pool):
            resolved = _resolve_exact_slipstream_pool(compiler, intent.pool, intent.intent_id)
        else:
            resolved = _resolve_symbolic_slipstream_pool(compiler, intent)
        if isinstance(resolved, CompilationResult):
            return resolved
        token0_info = resolved.token0
        token1_info = resolved.token1
        tick_spacing = resolved.tick_spacing
        deployment = resolved.deployment
        pool_check = resolved.pool_check
        token0_symbol = token0_info.symbol
        token1_symbol = token1_info.symbol

        range_form = "ticks" if lp_range_is_ticks(intent) else "prices"
        logger.info(
            f"Compiling Aerodrome Slipstream LP_OPEN: {token0_symbol}/{token1_symbol}, "
            f"tick_spacing={tick_spacing}, range={range_form}=[{intent.range_lower},{intent.range_upper}], "
            f"amounts={intent.amount0}/{intent.amount1}"
        )

        # Resolve the range to ticks. A price band (the canonical UX) is converted
        # via the shared decimals-correct cl_range seam; an explicit TickBand is
        # validated as raw ticks (integer, ordered, aligned to tick_spacing).
        tick_bounds = _resolve_slipstream_ticks(intent, tick_spacing, token0_info.decimals, token1_info.decimals)
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

        # The straddle check above already fails closed on an out-of-range
        # request UNLESS the caller opted in via allow_out_of_range=True --
        # in which case it returns None and suppresses its own explanation.
        # Re-derive that same warning here so an intentional one-sided open
        # still gets a loud, explicit "this earns zero fees until price
        # re-enters" notice instead of silence (VIB-exp19).
        range_warning = lp_range_excludes_spot_warning(
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            slot0=slot0,
            pool_address=pool_check.pool_address,
            protocol="aerodrome_slipstream",
        )
        if range_warning:
            warnings.append(range_warning)

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
        # VIB-6269: same price-band instrument as the V3-family compiler --
        # Slipstream pools are V3-shaped, so leaving this call site on the flat
        # per-leg haircut would keep the identical deterministic-revert defect
        # alive on the one connector that shares this helper.
        amount0_min, amount1_min = compute_lp_slippage_mins(
            intent=intent,
            amount0_desired=amount0_desired,
            amount1_desired=amount1_desired,
            default_lp_slippage=compiler.default_lp_slippage,
            sqrt_price_x96=slot0[0] if slot0 else None,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )

        # Admission is deliberately after read-only range/amount computation,
        # but before the adapter can build approvals or protocol calldata.
        verified_venue = _verify_slipstream_binding(
            compiler=compiler,
            pool_address=pool_check.pool_address,
            token0_address=token0_info.address,
            token1_address=token1_info.address,
            tick_spacing=tick_spacing,
            expected_position_manager=deployment.position_manager,
            intent_id=intent.intent_id,
        )
        if isinstance(verified_venue, CompilationResult):
            return verified_venue

        # Create Aerodrome adapter
        config = AerodromeConfig(
            chain=compiler.chain,
            wallet_address=compiler.wallet_address,
            deadline_seconds=compiler.default_deadline_seconds,
            price_provider=compiler.price_oracle,
            rpc_url=compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
        )
        adapter = AerodromeAdapter(config)

        # Build CL mint transactions. Pass corrected wei amounts and pre-computed
        # mins via the wei-overload kwargs so the adapter does NOT re-derive
        # mins from raw (uncorrected) amounts.
        cl_result = adapter.add_cl_liquidity(
            token_a=token0_info.address,
            token_b=token1_info.address,
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
            deployment=deployment,
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
                "nft_manager": deployment.position_manager,
                "slipstream_deployment": deployment.generation,
                **_slipstream_venue_metadata(verified_venue),
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

    except UnprotectedTradeError as e:
        # A SAFETY REFUSAL, not a fault (VIB-6217). Zero transactions were built
        # and the on-chain position is untouched — the guard did its job. Without
        # is_safety_refusal the runner maps this to an ordinary fault and it counts
        # toward the circuit breaker's consecutive-failure trip, so a
        # correctly-refusing strategy would trip itself off.
        #
        # logger.error, NOT logger.exception: a deliberate refusal that emits a
        # full traceback reads as a crash, and a guard that looks like a crash is
        # a guard someone switches off.
        logger.error(f"Refusing to compile Aerodrome Slipstream LP_OPEN without output protection: {e}")
        result.status = CompilationStatus.FAILED
        result.is_safety_refusal = True
        result.error = str(e)
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

        reviewed_deployments = slipstream_lp_deployments(compiler.chain)
        if not reviewed_deployments:
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
            deadline_seconds=compiler.default_deadline_seconds,
            price_provider=compiler.price_oracle,
            rpc_url=compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
        )
        adapter = AerodromeAdapter(config)

        resolved = _resolve_slipstream_position(
            compiler=compiler,
            adapter=adapter,
            token_id=token_id,
            intent_id=intent.intent_id,
            permission_discovery=bool(permission_discovery),
            reviewed_deployments=reviewed_deployments,
            expected_pool=intent.pool,
        )
        if isinstance(resolved, CompilationResult):
            return resolved
        deployment = resolved.deployment
        verified_venue = resolved.verified_venue

        # Build remove liquidity transactions
        cl_result = adapter.remove_cl_liquidity(
            token_id=token_id,
            recipient=compiler.wallet_address,
            deployment=deployment,
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
                    "nft_manager": deployment.position_manager,
                    "slipstream_deployment": deployment.generation,
                    **_slipstream_venue_metadata(verified_venue),
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
                "nft_manager": deployment.position_manager,
                "slipstream_deployment": deployment.generation,
                **_slipstream_venue_metadata(verified_venue),
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

        reviewed_deployments = slipstream_lp_deployments(compiler.chain)
        if not reviewed_deployments:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Aerodrome Slipstream CL not supported on chain '{compiler.chain}'. "
                    f"Supported chains: {sorted(SLIPSTREAM_LP_DEPLOYMENTS)}."
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
            deadline_seconds=compiler.default_deadline_seconds,
            price_provider=compiler.price_oracle,
            rpc_url=compiler._get_chain_rpc_url(),
            gateway_client=compiler._gateway_client,
        )
        adapter = AerodromeAdapter(config)

        resolved = _resolve_slipstream_position(
            compiler=compiler,
            adapter=adapter,
            token_id=token_id,
            intent_id=intent.intent_id,
            permission_discovery=permission_discovery,
            reviewed_deployments=reviewed_deployments,
            expected_pool=intent.pool,
        )
        if isinstance(resolved, CompilationResult):
            return resolved
        deployment = resolved.deployment
        verified_venue = resolved.verified_venue

        collect_result = adapter.collect_cl_fees(
            token_id=token_id,
            recipient=compiler.wallet_address,
            deployment=deployment,
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
                "nft_manager": deployment.position_manager,
                "slipstream_deployment": deployment.generation,
                **_slipstream_venue_metadata(verified_venue),
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
