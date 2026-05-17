"""Canonical Uniswap V4 PoolKey seed registry (VIB-4534).

In Uniswap V4, every pool is identified on-chain by a 32-byte ``poolId``
computed as ``keccak256(abi.encode(currency0, currency1, fee, tickSpacing,
hooks))``. The hash is irreversible, so resolving a poolId back to its
structured ``PoolKey`` requires either:

1. An ``Initialize`` event log emitted by the PoolManager at the block the
   pool was created (the historical-scan path in
   :mod:`almanak.gateway.data.v4_pool_key_cache`), OR
2. A pre-computed seed registry of ``(chain, token0, token1, fee, tickSpacing,
   hooks)`` tuples whose hashes are pre-computed off-chain.

Path (1) is the only way to discover an unknown pool, but it fails for any
pool whose ``Initialize`` event lives outside the gateway's bounded backfill
window (Base WETH/USDC fee=3000 sits ~15M blocks behind the configured
500k-block historical floor as of 2026-05-17). This module supplies path (2)
for the common case: a static table of ``CanonicalV4Pair`` rows covering the
WETH/USDC, WETH/USDT, WBTC/WETH, and USDC/USDT pairs at all four V4-canonical
fee tiers, on every chain whose V4 PoolManager is registered in
:data:`almanak.core.contracts.UNISWAP_V4`.

The seed is read in-process only — no outbound RPC or HTTP call is made when
populating it. Pool addresses are resolved via the framework token resolver,
and any pair whose tokens are not present in the resolver for a given chain
is silently SKIPPED (recorded in the seed report). This is the correct
behaviour: a chain that does not carry USDT in the token registry is a chain
for which we do not have a deterministic on-chain USDT address, and inventing
one would inject incorrect pool keys into the cache.

The seed does NOT replace the eth_getLogs-backed historical scan in
:class:`V4PoolKeyCache` — it complements it. Non-canonical pools (custom hook
contracts, exotic pairs, pools whose tokens are not in the resolver) continue
to be discovered via the existing scan path.

Layering:

* :class:`CanonicalV4Pair` — frozen dataclass declaring (chain, token0_symbol,
  token1_symbol, fee). Tick spacing and hooks are derived from the fee tier
  via the same convention used by :class:`UniswapV4SDK.compute_pool_key`.
* :data:`CANONICAL_V4_PAIRS` — the static table.
* :func:`seed_canonical_pool_keys` — orchestration: resolve tokens, build
  :class:`CachedPoolKey`, register into the cache; called once at gateway
  boot from :meth:`MarketService._get_v4_pool_key_cache`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from almanak.core.contracts import UNISWAP_V4
from almanak.framework.connectors.uniswap_v4.hooks import compute_pool_id
from almanak.framework.connectors.uniswap_v4.sdk import (
    NATIVE_CURRENCY,
    TICK_SPACING,
)
from almanak.framework.connectors.uniswap_v4.sdk import (
    PoolKey as FrameworkPoolKey,
)
from almanak.framework.data.tokens import (
    TokenNotFoundError,
    TokenResolutionError,
    get_token_resolver,
)
from almanak.gateway.data.v4_pool_key_cache import (
    NO_HOOKS,
    CachedPoolKey,
    V4CanonicalSeedCollisionError,
)

if TYPE_CHECKING:
    from almanak.gateway.data.v4_pool_key_cache import V4PoolKeyCache

logger = logging.getLogger(__name__)

# Canonical V4 fee tiers per docs.uniswap.org/contracts/v4. Pool creators may
# pick custom tiers, but the seed only covers the four canonical ones — that
# is where ~99% of TVL sits.
_CANONICAL_FEE_TIERS: tuple[int, ...] = (100, 500, 3000, 10000)


class V4CanonicalSeedConfigError(ValueError):
    """A row in :data:`CANONICAL_V4_PAIRS` references a chain or pair that
    cannot be reconciled with the framework's V4 deployment registry. Raised
    at :func:`seed_canonical_pool_keys` invocation time, not at module
    import, so a misconfigured row fails on first gateway boot rather than
    silently registering a key for a chain that has no PoolManager.
    """


@dataclass(frozen=True)
class CanonicalV4Pair:
    """One row of the canonical V4 seed table.

    Tick spacing is intentionally NOT a field — it is derived from
    ``fee`` via :data:`TICK_SPACING` so the seed matches what
    :meth:`UniswapV4SDK.compute_pool_key` produces on the same inputs.
    Setting hooks to anything other than :data:`NO_HOOKS` is unsupported
    here because hooks change the PoolKey hash AND the framework token
    resolver carries no notion of "this pool uses a custom hook" — for
    custom-hook pools, fall back to the eth_getLogs cache path.

    Attributes:
        chain: lowercase chain name; must be present in :data:`UNISWAP_V4`.
        token0_symbol: e.g. ``"WETH"``; resolved via the framework token resolver.
        token1_symbol: e.g. ``"USDC"``; resolved via the framework token resolver.
        fee: one of ``CANONICAL_FEE_TIERS`` (100, 500, 3000, 10000).
    """

    chain: str
    token0_symbol: str
    token1_symbol: str
    fee: int


@dataclass(frozen=True)
class SeedReport:
    """Outcome of one :func:`seed_canonical_pool_keys` invocation.

    Exposed for observability — the gateway logs the registered/skipped
    counts at INFO and the per-chain skip reasons at DEBUG. Tests assert
    against the report fields to prove the registry is doing what the
    trust statement claims.

    Attributes:
        registered: count of pool_ids inserted into the cache (excludes
            duplicates that were already present from a previous call).
        already_present: count of rows whose pool_id was already in the
            cache (idempotent re-registration, NOT a collision).
        skipped: list of ``(pair, reason)`` tuples for rows that could
            not be registered (most commonly: ``TokenNotFoundError`` on
            one of the symbols for the row's chain).
    """

    registered: int
    already_present: int
    skipped: list[tuple[CanonicalV4Pair, str]] = field(default_factory=list)


def _build_pairs() -> tuple[CanonicalV4Pair, ...]:
    """Construct the canonical pair table as the cartesian product of
    (chain, base_pair, fee_tier) plus a couple of chain-specific entries.

    Kept as a function so the registry's shape is auditable in one place
    and the chain/pair coverage is obvious from reading the source.
    """
    # Base pairs that we expect to find on every V4 chain.
    base_pairs: tuple[tuple[str, str], ...] = (
        ("WETH", "USDC"),
        ("WETH", "USDT"),
        ("WBTC", "WETH"),
        ("USDC", "USDT"),
    )
    pairs: list[CanonicalV4Pair] = []
    # Chains supported by V4 per the framework deployment registry. We rely
    # on UNISWAP_V4 as the single source of truth so future chain additions
    # require updating only one place (the contracts registry).
    v4_chains: tuple[str, ...] = tuple(sorted(UNISWAP_V4.keys()))
    for chain in v4_chains:
        for token0, token1 in base_pairs:
            for fee in _CANONICAL_FEE_TIERS:
                pairs.append(
                    CanonicalV4Pair(
                        chain=chain,
                        token0_symbol=token0,
                        token1_symbol=token1,
                        fee=fee,
                    )
                )
    return tuple(pairs)


CANONICAL_V4_PAIRS: tuple[CanonicalV4Pair, ...] = _build_pairs()


def _resolve_pair_addresses(pair: CanonicalV4Pair) -> tuple[str, str]:
    """Resolve the symbol pair to per-chain addresses via the token resolver.

    Returns a tuple of ``(token0_address, token1_address)`` — both lowercased
    and 0x-prefixed. Raises :class:`TokenNotFoundError` (the resolver's own
    exception) if either symbol is missing on the chain; the caller is
    responsible for swallowing the miss and recording it in the skip report.
    """
    resolver = get_token_resolver()
    t0 = resolver.resolve(pair.token0_symbol, pair.chain, skip_gateway=True, log_errors=False)
    t1 = resolver.resolve(pair.token1_symbol, pair.chain, skip_gateway=True, log_errors=False)
    return t0.address.lower(), t1.address.lower()


def _make_pool_key(token0_addr: str, token1_addr: str, fee: int) -> tuple[CachedPoolKey, str]:
    """Build a :class:`CachedPoolKey` and compute its canonical pool_id.

    Returns ``(cached_key, pool_id_hex)``. The pool_id is computed via
    :func:`compute_pool_id` so the seed is byte-identical to what the
    framework's :meth:`UniswapV4SDK.compute_pool_key` would produce.
    """
    tick_spacing = TICK_SPACING.get(fee)
    if tick_spacing is None:
        # Defensive: a non-canonical fee tier should never reach this code
        # path because _CANONICAL_FEE_TIERS is the only generator of fee
        # values. Re-raise as ConfigError so the test-time invariant is
        # visible to operators if the table is ever hand-edited.
        raise V4CanonicalSeedConfigError(f"fee={fee} has no canonical tick spacing in TICK_SPACING")

    # CachedPoolKey enforces currency0 < currency1; sort once here so the
    # call site doesn't need to know about V4's sorting convention.
    if int(token0_addr, 16) > int(token1_addr, 16):
        token0_addr, token1_addr = token1_addr, token0_addr

    cached = CachedPoolKey(
        currency0=token0_addr,
        currency1=token1_addr,
        fee=fee,
        tick_spacing=tick_spacing,
        hooks=NO_HOOKS,
    )
    # Use the framework's helper so the seed pool_id MATCHES what the
    # connector / receipt parser computes for the same inputs — no chance
    # of drift between the seed's hash and the on-chain hash.
    fw_key = FrameworkPoolKey(
        currency0=token0_addr,
        currency1=token1_addr,
        fee=fee,
        tick_spacing=tick_spacing,
        hooks=NATIVE_CURRENCY,  # hooks = zero address; NATIVE_CURRENCY == NO_HOOKS
    )
    pool_id = compute_pool_id(fw_key).lower()
    return cached, pool_id


def seed_canonical_pool_keys(
    cache: V4PoolKeyCache,
    pairs: tuple[CanonicalV4Pair, ...] | None = None,
) -> SeedReport:
    """Populate ``cache`` with the canonical V4 PoolKey table.

    Idempotent: calling twice is a no-op (the cache's internal dict insert
    is "first write wins" within :meth:`V4PoolKeyCache.register_canonical`).

    Args:
        cache: target :class:`V4PoolKeyCache` instance.
        pairs: optional pair table override (tests inject a small fixture
            table; production passes None to use the module-level registry).

    Returns:
        :class:`SeedReport` summarising how many rows were registered,
        already present, or skipped — and why each skip happened.

    Raises:
        V4CanonicalSeedConfigError: a row references a chain not registered
            in :data:`UNISWAP_V4`. Boot fails rather than silently dropping
            the row.
        V4CanonicalSeedCollisionError: two PoolKeys hashed to the same
            pool_id but carry different PoolKey fields. Indicates a
            duplicate-row bug in the seed table.
    """
    table = pairs if pairs is not None else CANONICAL_V4_PAIRS
    report_registered = 0
    report_already = 0
    skipped: list[tuple[CanonicalV4Pair, str]] = []

    for pair in table:
        # 1. Chain must be present in the framework V4 registry. Failing
        # loud here is intentional — a row for a non-V4 chain is a bug,
        # not a config option.
        if pair.chain not in UNISWAP_V4:
            raise V4CanonicalSeedConfigError(
                f"canonical V4 pair references chain={pair.chain!r} which is "
                f"not in UNISWAP_V4 deployment registry "
                f"({sorted(UNISWAP_V4.keys())!r})"
            )

        # 2. Resolve token symbols. A miss here is expected (e.g. USDT not
        # in the static registry on Base) and is logged + skipped, not
        # raised — the rest of the table still populates.
        try:
            t0_addr, t1_addr = _resolve_pair_addresses(pair)
        except (TokenNotFoundError, TokenResolutionError) as exc:
            skipped.append((pair, f"token_not_found: {exc}"))
            logger.debug(
                "V4 canonical seed: skipping %s/%s @ %s fee=%d — %s",
                pair.token0_symbol,
                pair.token1_symbol,
                pair.chain,
                pair.fee,
                exc,
            )
            continue

        # 3. Compute the canonical pool_id and build CachedPoolKey.
        cached_key, pool_id = _make_pool_key(t0_addr, t1_addr, pair.fee)

        # 4. Insert into the cache via the canonical-registration helper
        # which enforces collision detection.
        try:
            outcome = cache.register_canonical(pair.chain, pool_id, cached_key)
        except V4CanonicalSeedCollisionError:
            # Re-raise immediately — collisions are programming errors in
            # the seed table, not recoverable runtime conditions.
            raise

        if outcome == "registered":
            report_registered += 1
        else:
            # "already_present" — the same key was registered earlier in
            # the same loop iteration. Most commonly because two pairs
            # (e.g. USDC/USDT on Ethereum at fee=3000) sort to the same
            # canonical PoolKey when constructed against the resolver's
            # canonical addresses; that's fine, it's the same pool.
            report_already += 1

    logger.info(
        "V4 canonical seed: registered=%d already_present=%d skipped=%d (table_size=%d)",
        report_registered,
        report_already,
        len(skipped),
        len(table),
    )
    return SeedReport(
        registered=report_registered,
        already_present=report_already,
        skipped=skipped,
    )


__all__ = [
    "CANONICAL_V4_PAIRS",
    "CanonicalV4Pair",
    "SeedReport",
    "V4CanonicalSeedCollisionError",
    "V4CanonicalSeedConfigError",
    "seed_canonical_pool_keys",
]
