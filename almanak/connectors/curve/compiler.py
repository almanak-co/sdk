"""Connector-owned compiler for Curve Finance."""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple

if TYPE_CHECKING:
    from almanak.connectors.curve.adapter import CurveAdapter, LiquidityResult

from almanak.connectors._strategy_base.base.compiler import (
    BaseCompilerContext,
    BaseProtocolCompiler,
    SwapCompilerContext,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType, LPCloseIntent, LPOpenIntent, SwapIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)

# Built-in LP slippage floor used when an intent does not request its own
# tolerance. 50 bps (0.5%) — the historical Curve LP default. Kept as a named
# constant so the fallback is explicit at both the LP_OPEN and LP_CLOSE sites.
_DEFAULT_LP_SLIPPAGE_BPS = 50


def _resolve_lp_slippage_bps(max_slippage: Decimal | None) -> int:
    """Convert an intent's optional ``max_slippage`` to integer basis points.

    Mirrors the SWAP path's ``int(intent.max_slippage * Decimal("10000"))``
    conversion so LP and SWAP read the same field in the same units. When
    ``max_slippage`` is ``None`` (the historical case — LP intents never carried
    a slippage field before audit P0-7), fall back to the built-in
    ``_DEFAULT_LP_SLIPPAGE_BPS`` so existing callers are byte-for-byte unchanged.
    """
    if max_slippage is None:
        return _DEFAULT_LP_SLIPPAGE_BPS
    return int(max_slippage * Decimal("10000"))


def _resolve_oracle_guard_bps(swap_params: dict[str, Any]) -> int | None:
    """Resolve the per-intent oracle/MEV min-out guard threshold (VIB-5439).

    A strategy widens the guard for a large or volatile-pool swap with real price
    impact (``swap_params={"oracle_guard_bps": 300}``) or narrows it for a tight
    stable desk. ``None`` (the common case) lets the adapter apply
    ``DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS``. A boolean / non-positive / non-integer
    override is ignored (falls back to the default) rather than silently disabling
    the guard on a money path.
    """
    raw = swap_params.get("oracle_guard_bps")
    if raw is None:
        return None
    # ``bool`` is a subclass of ``int`` (``int(True) == 1``), so a boolean override
    # would silently become a 1 bps threshold — reject it explicitly.
    if isinstance(raw, bool):
        logger.warning("Ignoring boolean oracle_guard_bps=%r; using connector default.", raw)
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        logger.warning("Ignoring non-integer oracle_guard_bps=%r; using connector default.", raw)
        return None
    if value <= 0:
        logger.warning("Ignoring non-positive oracle_guard_bps=%r; using connector default.", raw)
        return None
    return value


def _normalize_asset_set_token(token: str, ctx: BaseCompilerContext) -> str:
    """Canonicalize a single asset-set token to its uppercase registry symbol.

    Mirrors the SWAP path, which compares ``ctx.services.resolve_token(...).symbol``
    against each pool's ``coins``. Resolution normalizes aliases (e.g. ``"usdc"`` ->
    ``"USDC"``); when the token is not in the registry we fall back to the raw
    uppercased string so pure-symbol asset-sets still match without a registry hit.
    """
    raw = token.strip()
    if not raw:
        return ""
    try:
        resolved = ctx.services.resolve_token(raw)
    except Exception:  # noqa: BLE001 - resolution failure must not block symbol-only match
        resolved = None
    if resolved is not None and getattr(resolved, "symbol", None):
        return str(resolved.symbol).upper()
    return raw.upper()


def _resolve_pool_by_asset_set(
    asset_set: str,
    chain_pools: dict[str, dict[str, Any]],
    ctx: BaseCompilerContext,
) -> tuple[str, dict[str, Any]] | None:
    """Resolve an asset-set string (e.g. ``"USDT/USDC"``) to a registered Curve pool.

    The LP analogue of the SWAP pool resolver. Splits the asset-set on ``/``,
    canonicalizes each token, and selects the pool whose ``coins`` set equals the
    requested asset set exactly.

    VIB-3946 core acceptance — *never silently pick the wrong pool*. If more than
    one registered pool has the identical coin set (Curve routinely ships a legacy
    StableSwap and a StableSwap-NG for the same pair on the same chain), this is
    genuinely ambiguous: liquidity, not alphabetical name order, decides which is
    "right", and the resolver has no liquidity signal. So it raises ``ValueError``
    listing the colliding pools and instructing the author to disambiguate by pool
    name or address — it does NOT auto-pick.

    Returns ``(pool_name, pool_data)`` on a unique exact match, else ``None``.
    Raises ``ValueError`` when the asset set matches more than one pool.
    """
    requested = {_normalize_asset_set_token(t, ctx) for t in asset_set.split("/")}
    requested.discard("")
    if len(requested) < 2:
        # A single token (or empty) is not an asset set; let the caller error out.
        return None

    matches: list[tuple[str, dict[str, Any]]] = []
    for name, data in chain_pools.items():
        coins = data.get("coins")
        if not coins:
            continue
        coins_upper = {str(c).upper() for c in coins}
        if coins_upper == requested:
            matches.append((name, data))

    if not matches:
        return None
    if len(matches) > 1:
        colliding = {name: data.get("address") for name, data in matches}
        raise ValueError(
            f"Ambiguous Curve asset set {asset_set!r}: matches multiple registered pools "
            f"{colliding}. Disambiguate by passing an explicit pool name or address as "
            f"intent.pool — refusing to auto-pick, which could open into the wrong pool."
        )
    return matches[0]


def _metapool_combined_coins(pool_data: dict[str, Any]) -> set[str]:
    """Uppercased COMBINED coin symbols of a metapool (meta coin + base coins).

    Returns the empty set for non-metapools. The combined space is
    ``[coins[0]] + base_pool_coins`` (index 0 is the meta coin; the base-LP
    token ``coins[1]`` is intentionally excluded — it is not a tradeable
    underlying coin).
    """
    if not pool_data.get("is_metapool"):
        return set()
    coins = pool_data.get("coins") or []
    if not coins:
        # A metapool with no coins is a malformed registry entry; treat as having
        # no combined space rather than raising on the index access.
        return set()
    meta_coin = coins[0]
    base = pool_data.get("base_pool_coins") or []
    return {str(meta_coin).upper(), *(str(c).upper() for c in base)}


def _pair_on_metapool_underlying(from_symbol: str, to_symbol: str, pool_data: dict[str, Any]) -> bool:
    """True when BOTH tokens live on the metapool's combined coin space."""
    combined = _metapool_combined_coins(pool_data)
    return from_symbol.upper() in combined and to_symbol.upper() in combined


def _resolve_metapool_by_underlying_pair(
    from_symbol: str,
    to_symbol: str,
    chain_pools: dict[str, dict[str, Any]],
) -> tuple[str, dict[str, Any]] | None:
    """Find the unique metapool whose combined coin space carries the pair.

    Mirrors the SWAP native pool resolver but searches the COMBINED (underlying)
    coin space of metapools. Returns ``(pool_name, pool_data)`` on a unique
    match, else ``None``. Like the asset-set resolver, raises on ambiguity
    rather than auto-picking (VIB-3946 discipline).
    """
    matches: list[tuple[str, dict[str, Any]]] = []
    for name, data in chain_pools.items():
        if _pair_on_metapool_underlying(from_symbol, to_symbol, data):
            matches.append((name, data))
    if not matches:
        return None
    if len(matches) > 1:
        colliding = {name: data.get("address") for name, data in matches}
        raise ValueError(
            f"Ambiguous Curve metapool underlying swap {from_symbol}->{to_symbol}: matches "
            f"{colliding}. Disambiguate via swap_params={{'pool': '0x...'}}."
        )
    return matches[0]


def _metapool_data_for_address(
    pool_address: str,
    chain_pools: dict[str, dict[str, Any]],
) -> tuple[str, dict[str, Any]] | None:
    """Return ``(name, data)`` for a registered metapool by name or address, else ``None``."""
    if pool_address in chain_pools and chain_pools[pool_address].get("is_metapool"):
        return pool_address, chain_pools[pool_address]
    for name, data in chain_pools.items():
        if data.get("is_metapool") and str(data.get("address", "")).lower() == pool_address.lower():
            return name, data
    return None


def _resolve_swap_pool_and_route(
    from_symbol: str,
    to_symbol: str,
    swap_params: dict[str, Any],
    chain_pools: dict[str, dict[str, Any]],
) -> tuple[str | None, str, bool]:
    """Resolve ``(pool_address, pool_name, use_metapool_underlying)`` for a Curve swap.

    Pure pool selection — no ``ctx``, no adapter, no I/O. Mirrors the existing
    module-level resolver helpers (blueprint 05 §6). Resolution order:

    1. Explicit ``swap_params["pool"]`` (address) if given.
    2. NATIVE pool whose ``coins`` carry both tokens.
    3. Metapool UNDERLYING fallback (VIB-5419): a metapool whose COMBINED coin
       space (meta coin + base-pool coins) carries the pair → route through
       ``exchange_underlying`` (``use_metapool_underlying=True``).

    When the pool was given/matched natively but the pair lives only on a
    metapool's combined space, flips to the underlying route.

    Returns ``pool_address=None`` when nothing carries the pair (the caller emits
    the "No Curve pool found" error). Propagates ``ValueError`` on an ambiguous
    metapool underlying match (the caller's ``except`` turns it into a FAILED
    result — identical to the prior inline behaviour).
    """
    pool_address: str | None = swap_params.get("pool")
    pool_name: str = ""
    use_metapool_underlying = False

    if not pool_address:
        for name, pool_data in chain_pools.items():
            coins_upper = [c.upper() for c in pool_data["coins"]]
            if from_symbol.upper() in coins_upper and to_symbol.upper() in coins_upper:
                pool_address = pool_data["address"]
                pool_name = name
                break

    # Metapool underlying fallback (VIB-5419): no NATIVE pool carries the pair,
    # but a metapool's COMBINED coin space (meta coin + base-pool coins) does.
    # Only triggers when the native loop above found nothing, so non-meta
    # behaviour is untouched.
    if not pool_address:
        meta_match = _resolve_metapool_by_underlying_pair(from_symbol, to_symbol, chain_pools)
        if meta_match is not None:
            pool_name, meta_data = meta_match
            pool_address = meta_data["address"]
            use_metapool_underlying = True

    # When the pool was given explicitly (or matched a metapool nickname) and the
    # pair lives on the combined space rather than the native coins, prefer the
    # underlying route.
    if pool_address and not use_metapool_underlying:
        resolved_meta = _metapool_data_for_address(pool_address, chain_pools)
        if resolved_meta is not None:
            meta_name, meta_data = resolved_meta
            native_coins = {c.upper() for c in meta_data["coins"]}
            pair = {from_symbol.upper(), to_symbol.upper()}
            if not pair.issubset(native_coins) and _pair_on_metapool_underlying(from_symbol, to_symbol, meta_data):
                use_metapool_underlying = True
                if not pool_name:
                    pool_name = meta_name

    return pool_address, pool_name, use_metapool_underlying


def _resolve_lp_open_amounts(
    intent: LPOpenIntent,
    pool_data: dict[str, Any],
    pool_name: str,
    pool_address: str,
    chain: str,
) -> tuple[list[Decimal], bool] | str:
    """Decide the LP_OPEN deposit vector and whether it is an underlying (zap) deposit.

    Pure — reads only ``intent`` + static ``pool_data``. Returns
    ``(amounts, is_underlying_deposit)`` on success, or an error STRING when a
    ``coin_amounts`` vector has an invalid length (the caller builds the FAILED
    result, keeping this helper free of ``ctx`` / ``CompilationResult`` — same
    shape as ``_resolve_pool_by_asset_set`` returning ``None``-or-tuple).

    Distinctions (VIB-5419 + VIB-5154):
    - Metapool UNDERLYING deposit: a ``coin_amounts`` vector whose length matches
      the COMBINED space (meta coin + base coins = ``n_coins`` is always the
      native count, combined is ``1 + len(base_pool_coins)``), routed through the
      zap. Native is always exactly ``n_coins``, so the two are unambiguous.
    - Native pool-coin-aligned vector: ``coin_amounts`` length == ``n_coins``.
    - Legacy two-slot: ``amount0`` / ``amount1`` -> indices 0/1, tail zero-filled.
    """
    n_coins = pool_data["n_coins"]
    # Combined (underlying) coin-space size for a metapool: meta coin + base-pool
    # coins. None for non-metapools.
    combined_len: int | None = None
    if pool_data.get("is_metapool"):
        combined_len = 1 + len(pool_data.get("base_pool_coins") or [])

    coin_amounts = getattr(intent, "coin_amounts", None)

    is_underlying_deposit = coin_amounts is not None and combined_len is not None and len(coin_amounts) == combined_len
    if is_underlying_deposit:
        assert coin_amounts is not None  # narrowed by is_underlying_deposit
        return ([Decimal(str(a)) for a in coin_amounts], True)

    if coin_amounts is not None:
        # Pool-coin-aligned full allocation vector (VIB-5154 / ALM-2728).
        # coin_amounts[i] maps directly to pool coin index i, so non-leading
        # coins (index 2+) can be funded without forcing index 0.
        if len(coin_amounts) != n_coins:
            meta_hint = (
                f" (or {combined_len} for an underlying deposit via the zap)" if combined_len is not None else ""
            )
            return (
                f"coin_amounts has {len(coin_amounts)} entries but Curve pool "
                f"'{pool_name or pool_address}' on {chain} has {n_coins} coins{meta_hint}. "
                f"coin_amounts must provide exactly one amount per pool coin, "
                f"indexed as {pool_data.get('coins')}."
            )
        return ([Decimal(str(a)) for a in coin_amounts], False)

    # Legacy two-slot mapping: amount0/amount1 -> indices 0/1, tail zero-filled.
    # Unchanged behaviour for every existing caller.
    amounts = [intent.amount0, intent.amount1]
    while len(amounts) < n_coins:
        amounts.append(Decimal("0"))
    return (amounts, False)


def _is_pool_address(value: str | None) -> bool:
    """True when ``value`` is a 0x-prefixed 20-byte address literal."""
    return value is not None and value.startswith("0x") and len(value) == 42


def _resolve_dynamic_pool(ctx: BaseCompilerContext, pool_address: str) -> tuple[str, dict[str, Any]] | None:
    """Resolve an UNCURATED Curve pool from the on-chain MetaRegistry (VIB-5628).

    Builds a Curve adapter and calls ``get_pool_info``, whose static-miss path
    resolves the pool shape (coins / decimals / lp_token / metapool / gamma-
    discriminated pool_type) from Curve's MetaRegistry via the gateway-first
    seam. Returns ``(pool_name, pool_data)`` — the same ``dict`` shape the static
    ``CURVE_POOLS`` entries carry, so every downstream consumer is unchanged — or
    ``None`` when the address is not a Curve pool / there is no read transport
    (preserving today's "Unknown Curve pool" behaviour for a genuine miss).

    The heavy MetaRegistry reads are memoised per-process in
    ``pool_resolver._METADATA_CACHE``, so a second adapter that later executes the
    trade re-uses the cached resolution rather than re-reading the chain.
    """
    from almanak.connectors.curve.adapter import CurveAdapter, CurveConfig

    config = CurveConfig(
        chain=ctx.chain,
        wallet_address=ctx.wallet_address,
        rpc_url=ctx.rpc_url,
        gateway_client=ctx.gateway_client,
    )
    info = CurveAdapter(config).get_pool_info(pool_address)
    if info is None:
        return None
    return info.name, info.to_dict()


class _ClosePool(NamedTuple):
    """Resolved Curve pool for an LP_CLOSE compile (VIB-5438 decomposition).

    Carries the registry-resolved pool identity through the
    ``compile_lp_close`` helper chain so the dispatcher reads a single typed
    value instead of four loose locals (``name``/``address``/``data``/
    ``lp_token``). ``data`` is the raw ``CURVE_POOLS`` entry; ``lp_token`` is the
    already-validated non-empty LP token address.
    """

    name: str
    address: str
    data: dict[str, Any]
    lp_token: str


class CurveCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compiler for Curve pool-based swaps and fungible LP positions."""

    # Curve compiles swaps, so it needs the swap-pipeline context — notably
    # ``using_placeholders``, which the P0-8 oracle min-out guard (VIB-5439)
    # reads to avoid firing on known-fake placeholder / offline prices.
    context_type: ClassVar[type[BaseCompilerContext]] = SwapCompilerContext
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
        from almanak.connectors.curve.adapter import (
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
            chain_pools = CURVE_POOLS.get(ctx.chain, {})

            # VIB-5628: an explicit ``swap_params["pool"]`` ADDRESS that misses the
            # static registry is NOT a hard error here — ``_resolve_swap_pool_and_route``
            # returns the given address, and ``adapter.swap`` resolves the uncurated
            # pool's shape (coins / decimals / gamma-discriminated pool_type) live
            # from the on-chain MetaRegistry via ``get_pool_info``. Native-coin
            # routing only; metapool-underlying routing for uncurated pools is P1-4.

            # Pool selection + route detection (native vs metapool-underlying).
            # `use_metapool_underlying` is True when the resolved Curve pool is a
            # metapool whose combined (underlying) coin space — not its native
            # 2-coin space — carries the requested pair (e.g. FRAX -> USDC on a
            # FRAX/3CRV metapool). Ambiguity raises ValueError, caught below.
            pool_address, pool_name, use_metapool_underlying = _resolve_swap_pool_and_route(
                from_token.symbol, to_token.symbol, swap_params, chain_pools
            )

            if not pool_address:
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

            # P0-8 min-out guard knobs (VIB-5439): per-intent overrides for the
            # oracle-vs-pool divergence threshold and the unmeasured-oracle policy.
            # ``oracle_prices_real`` gates the guard off placeholder / offline
            # prices: those are known-fake (the compiler logs a PLACEHOLDER PRICES
            # warning), so they must not be trusted as an independent oracle —
            # otherwise the guard would block every real swap in test / discovery
            # mode. price_ratio still feeds the CryptoSwap slippage estimate.
            oracle_guard_bps = _resolve_oracle_guard_bps(swap_params)
            strict_oracle_guard = bool(swap_params.get("strict_oracle_guard", False))
            oracle_prices_real = not getattr(ctx, "using_placeholders", False)

            if use_metapool_underlying:
                # Metapool combined-space swap via exchange_underlying. The
                # combined coins are all USD stables, so price_ratio is not needed
                # for the slippage estimate (the adapter uses the on-chain
                # get_dy_underlying quote or a 1:1 decimal-adjusted estimate) — but
                # it IS the independent oracle reference for the min-out guard, so
                # thread it through to flag a depegged underlying.
                swap_result = adapter.swap_underlying(
                    pool_address=pool_address,
                    token_in=from_token.symbol,
                    token_out=to_token.symbol,
                    amount_in=amount_decimal,
                    slippage_bps=slippage_bps,
                    price_ratio=price_ratio,
                    oracle_guard_bps=oracle_guard_bps,
                    strict_oracle_guard=strict_oracle_guard,
                    oracle_prices_real=oracle_prices_real,
                )
            else:
                swap_result = adapter.swap(
                    pool_address=pool_address,
                    token_in=from_token.symbol,
                    token_out=to_token.symbol,
                    amount_in=amount_decimal,
                    slippage_bps=slippage_bps,
                    price_ratio=price_ratio,
                    oracle_guard_bps=oracle_guard_bps,
                    strict_oracle_guard=strict_oracle_guard,
                    oracle_prices_real=oracle_prices_real,
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
        from almanak.connectors.curve.adapter import (
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

            if pool_data is None and "/" in intent.pool:
                # Asset-set fallback (e.g. "USDT/USDC", "USDT/USDC/DAI") — the LP
                # analogue of the SWAP pool resolver. VIB-3946.
                asset_match = _resolve_pool_by_asset_set(intent.pool, chain_pools, ctx)
                if asset_match is not None:
                    pool_name, pool_data = asset_match
                    pool_address = pool_data["address"]

            # VIB-5628: static + asset-set miss on an ADDRESS -> resolve the
            # UNCURATED pool live from the on-chain MetaRegistry.
            if pool_data is None and _is_pool_address(intent.pool):
                dynamic = _resolve_dynamic_pool(ctx, intent.pool)
                if dynamic is not None:
                    dyn_name, pool_data = dynamic
                    pool_name = pool_name or dyn_name
                    pool_address = pool_data["address"]

            if pool_data is None:
                available = {name: d["address"] for name, d in chain_pools.items()}
                coins_by_pool = {name: d.get("coins") for name, d in chain_pools.items()}
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Unknown Curve pool: {intent.pool} on {ctx.chain}. "
                        f"Available pools: {available}. "
                        f"Pool asset sets (pass as e.g. 'USDT/USDC'): {coins_by_pool}"
                    ),
                    intent_id=intent.intent_id,
                )

            n_coins = pool_data["n_coins"]

            # Resolve the deposit vector + route (native vs metapool-underlying).
            # Returns an error string on an invalid coin_amounts length.
            amounts_or_error = _resolve_lp_open_amounts(intent, pool_data, pool_name, pool_address, ctx.chain)
            if isinstance(amounts_or_error, str):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=amounts_or_error,
                    intent_id=intent.intent_id,
                )
            amounts, is_underlying_deposit = amounts_or_error

            # Honor the intent's requested LP slippage (audit P0-7); fall back to
            # the built-in 50 bps default when the caller didn't set one.
            slippage_bps = _resolve_lp_slippage_bps(intent.max_slippage)
            config = CurveConfig(
                chain=ctx.chain,
                wallet_address=ctx.wallet_address,
                default_slippage_bps=slippage_bps,
                rpc_url=ctx.rpc_url,
                gateway_client=ctx.gateway_client,
            )
            adapter = CurveAdapter(config)

            logger.info(
                "Compiling Curve LP_OPEN%s: pool=%s (%s), amounts=%s",
                " (metapool underlying)" if is_underlying_deposit else "",
                pool_name,
                ctx.chain,
                amounts,
            )
            if is_underlying_deposit:
                liq_result = adapter.add_liquidity_underlying(
                    pool_address=pool_address,
                    underlying_amounts=amounts,
                    slippage_bps=slippage_bps,
                )
            else:
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

    def _resolve_close_pool(self, ctx: BaseCompilerContext, intent: LPCloseIntent) -> _ClosePool | CompilationResult:
        """Resolve the LP_CLOSE target pool from the registry (VIB-5438 decomposition).

        Returns a ``_ClosePool`` on success, or the existing FAILED
        ``CompilationResult`` (unchanged error text) when the chain is
        unsupported, ``intent.pool`` is unset, the pool is unknown, or the
        registry entry is missing an ``lp_token``. Behaviour-preserving extract
        of the original ``compile_lp_close`` resolution block.
        """
        from almanak.connectors.curve.adapter import CURVE_ADDRESSES, CURVE_POOLS

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

        if pool_data is None and "/" in intent.pool:
            # Asset-set fallback (e.g. "USDT/USDC", "USDT/USDC/DAI") — the LP
            # analogue of the SWAP pool resolver. VIB-3946.
            asset_match = _resolve_pool_by_asset_set(intent.pool, chain_pools, ctx)
            if asset_match is not None:
                pool_name, pool_data = asset_match
                pool_address = pool_data["address"]

        # VIB-5628: static + asset-set miss on an ADDRESS -> resolve the UNCURATED
        # pool live from the MetaRegistry so an uncurated LP position can be closed.
        if pool_data is None and _is_pool_address(intent.pool):
            dynamic = _resolve_dynamic_pool(ctx, intent.pool)
            if dynamic is not None:
                dyn_name, pool_data = dynamic
                pool_name = pool_name or dyn_name
                pool_address = pool_data["address"]

        if pool_data is None:
            available = {name: d["address"] for name, d in chain_pools.items()}
            coins_by_pool = {name: d.get("coins") for name, d in chain_pools.items()}
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Unknown Curve pool: {intent.pool} on {ctx.chain}. "
                    f"Available pools: {available}. "
                    f"Pool asset sets (pass as e.g. 'USDT/USDC'): {coins_by_pool}"
                ),
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

        return _ClosePool(name=pool_name, address=pool_address, data=pool_data, lp_token=lp_token_for_pool)

    def _resolve_close_lp_amount(
        self, ctx: BaseCompilerContext, intent: LPCloseIntent, pool: _ClosePool
    ) -> Decimal | CompilationResult:
        """Resolve the LP amount to close (VIB-5438 decomposition).

        Returns the ``Decimal`` LP amount on success, or a ``CompilationResult``
        for BOTH the zero-balance no_op SUCCESS and every FAILED case (LP-token
        mismatch, unreadable balance, unresolved decimals, malformed decimal
        string). Behaviour-preserving extract of the original block; error text
        references ``pool.name``/``pool.address``/``pool.lp_token`` byte-for-byte.
        """
        position_id_str = str(intent.position_id).strip()
        if position_id_str.startswith("0x") and len(position_id_str) == 42:
            lp_token_address = position_id_str
            if lp_token_address.lower() != pool.lp_token.lower():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"position_id LP token {lp_token_address} does not match "
                        f"pool '{pool.name}' LP token {pool.lp_token}. "
                        f"Refusing to proceed — this would close the wrong position."
                    ),
                    intent_id=intent.intent_id,
                )
            raw_balance = ctx.services.query_erc20_balance(pool.lp_token, ctx.wallet_address)
            if raw_balance is None:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Failed to query LP token balance for {pool.name or pool.address} "
                        f"({pool.lp_token}). Ensure gateway_client or rpc_url is configured."
                    ),
                    intent_id=intent.intent_id,
                )
            if raw_balance == 0:
                logger.info("Curve LP_CLOSE: zero LP balance for %s — no_op", pool.name)
                return CompilationResult(
                    status=CompilationStatus.SUCCESS,
                    action_bundle=ActionBundle(
                        intent_type=IntentType.LP_CLOSE.value,
                        transactions=[],
                        metadata={
                            "no_op": True,
                            "reason": f"zero LP token balance for {pool.name} ({pool.lp_token})",
                        },
                    ),
                    intent_id=intent.intent_id,
                )
            lp_token_info = ctx.services.resolve_token(pool.lp_token)
            if not lp_token_info:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        f"Could not resolve decimals for Curve LP token {pool.lp_token}. "
                        f"Cannot safely compute withdrawal amount without known decimals."
                    ),
                    intent_id=intent.intent_id,
                )
            lp_amount = Decimal(raw_balance) / Decimal(10**lp_token_info.decimals)
            logger.info("Queried on-chain LP balance for %s: %s", pool.name, lp_amount)
            return lp_amount

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
        # ``Decimal`` parses "0", "-1", "NaN", "Infinity" without error — reject them
        # before they reach wei conversion / calldata padding (CodeRabbit). A
        # non-positive or non-finite LP amount is never a valid withdrawal size.
        if not lp_amount.is_finite() or lp_amount <= 0:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    f"Invalid position_id for Curve LP_CLOSE: '{intent.position_id}'. "
                    f"LP token amount must be a positive finite decimal."
                ),
                intent_id=intent.intent_id,
            )
        return lp_amount

    def _dispatch_remove_liquidity(
        self,
        adapter: CurveAdapter,
        intent: LPCloseIntent,
        pool_address: str,
        lp_amount: Decimal,
        slippage_bps: int,
    ) -> LiquidityResult:
        """Dispatch the exit-shape to the right adapter call (VIB-5438 decomposition).

        Exit-shape dispatch (audit P0-4). Three mutually-exclusive paths:
          * imbalanced_amounts (VIB-5438): withdraw EXACT per-coin amounts via
            remove_liquidity_imbalance. The adapter sizes a fail-closed
            MAX-BURN ceiling (the inverse of a min-out) from the pool's
            on-chain calc_token_amount(amounts, is_deposit=False) — never an
            unbounded cap. StableSwap-family only.
          * coin_index (VIB-5437): withdraw the whole position into one coin
            via remove_liquidity_one_coin (min-out from calc_withdraw_one_coin).
          * neither (default): proportional remove_liquidity — unchanged.
        The vocabulary validator enforces coin_index/imbalanced_amounts mutual
        exclusivity, so the branch order does not mask a conflicting request.
        """
        if intent.imbalanced_amounts is not None:
            return adapter.remove_liquidity_imbalance(
                pool_address=pool_address,
                amounts=intent.imbalanced_amounts,
                lp_amount=lp_amount,
                slippage_bps=slippage_bps,
            )
        if intent.coin_index is not None:
            return adapter.remove_liquidity_one_coin(
                pool_address=pool_address,
                lp_amount=lp_amount,
                coin_index=intent.coin_index,
                slippage_bps=slippage_bps,
            )
        return adapter.remove_liquidity(
            pool_address=pool_address,
            lp_amount=lp_amount,
            slippage_bps=slippage_bps,
        )

    def compile_lp_close(self, ctx: BaseCompilerContext, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Curve Finance.

        Thin dispatcher (VIB-5438 decomposition): resolves the pool and LP amount
        via ``_resolve_close_pool`` / ``_resolve_close_lp_amount`` (both UNION-RETURN
        a ``CompilationResult`` for their no_op / FAILED cases, so the outer
        ``except`` can never rewrite an early-return's error text), then dispatches
        the exit-shape via ``_dispatch_remove_liquidity`` and builds the bundle.
        """
        from almanak.connectors.curve.adapter import CurveAdapter, CurveConfig

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[Any] = []

        try:
            pool = self._resolve_close_pool(ctx, intent)
            if isinstance(pool, CompilationResult):
                return pool

            lp_amount = self._resolve_close_lp_amount(ctx, intent, pool)
            if isinstance(lp_amount, CompilationResult):
                return lp_amount

            # Honor the intent's requested LP slippage (audit P0-7); fall back to
            # the built-in 50 bps default when the caller didn't set one.
            slippage_bps = _resolve_lp_slippage_bps(intent.max_slippage)

            logger.info(
                "Compiling Curve LP_CLOSE: pool=%s (%s), lp_amount=%s",
                pool.name,
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

            liq_result = self._dispatch_remove_liquidity(adapter, intent, pool.address, lp_amount, slippage_bps)

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
                    "pool_address": pool.address,
                    "pool_name": pool.name,
                    "lp_amount": str(lp_amount),
                    "lp_token": pool.data["lp_token"],
                    "protocol": "curve",
                    "operation": liq_result.operation,
                    "coin_index": intent.coin_index,
                    "imbalanced_amounts": (
                        [str(a) for a in intent.imbalanced_amounts] if intent.imbalanced_amounts is not None else None
                    ),
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions  # type: ignore[assignment]
            result.total_gas_estimate = total_gas

            logger.info(
                "Compiled Curve LP_CLOSE intent: pool=%s, %d txs, %d gas",
                pool.name,
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
