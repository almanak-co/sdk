"""Curve Finance permission hints for permission discovery.

Curve does NOT use the generic ``synthetic_swap_pair`` mechanism. Its pools
are pair-specific (StableSwap, CryptoSwap, Tricrypto), so a single pair only
resolves to one curated pool per chain — leaving every other registered pool
unauthorised on the Safe (#1903).

Curve owns its discovery vectors via ``build_discovery_vectors`` below —
see :func:`almanak.framework.permissions.hints.get_discovery_vectors_override`
for the dispatcher contract.

Synthetic-discovery participation (VIB-4928 / VIB-6046): ``SWAP``, ``LP_OPEN``
and ``LP_CLOSE``. Every one of them is discovered by *compiling* real intents
against the connector's own ``CURVE_POOLS`` registry, so the manifest can never
drift from the calldata the compiler emits — no hand-typed target or selector
literals live in this module.

LP was originally omitted, which made ``discover_permissions(...,
intent_types=["LP_OPEN","LP_CLOSE"])`` return ``([], [])`` on all five chains
(VIB-6046). That did **not** manifest as "curve LP reverts under Safe" — it
manifested as curve LP being *unconstrained*, because the wildcarded MultiSend
DELEGATECALL grant carried the batch past the Roles allowlist entirely
(VIB-6057). Closing this gap is a prerequisite for turning that enforcement on.

Offline and deterministic (VIB-6046 D5). This was NOT true when LP discovery
first landed, and the way it failed is worth keeping written down.

As originally shipped, a single
``discover_permissions("arbitrum", ["curve"], ["LP_OPEN","LP_CLOSE"])`` issued
**43 live ``eth_call``s to a public RPC**, because ``IntentCompiler``'s
``_get_chain_rpc_url`` falls through to ``*-rpc.publicnode.com`` even when the
compiler is built with ``rpc_url=None`` and no gateway. Those reads decided
which selectors landed on the manifest, so the manifest was a function of
rate-limit weather rather than of ``CURVE_POOLS``. Three consecutive runs
produced **7 / 3 / 7 targets and 6 / 2 / 7 selectors** from identical inputs.

The mechanism was ``_probe_is_ng``, which selects between the StableSwap-NG
dynamic-array ABI and the legacy fixed-size ABI — a **different 4-byte
selector** for ``add_liquidity``, ``remove_liquidity`` and
``remove_liquidity_imbalance``. A clean probe pinned the NG selector; a reverted
or rate-limited (429) probe pinned the legacy one. A manifest generated under
one outcome does not authorise the selector the strategy emits under the other,
so ``execTransactionWithRole`` reverts *unauthorized* — at teardown, with
capital committed.

Three changes close it, and none of them relaxes a slippage guard:

1. ``PermissionHints.offline_discovery=True`` (below) stops the compiler from
   resolving an *implicit* transport during discovery, so no network read can
   influence the result. An explicit ``rpc_url`` is still honoured.
2. ``CurveConfig.permission_discovery`` substitutes positive, deterministic
   synthetic bounds at the **quote seam** — upstream of the fail-closed
   ``min_lp`` / ``min_amounts`` / ``max_burn`` guards, which are UNCHANGED and
   still refuse any non-positive bound. This is sound only because discovery
   compiles calldata to read its ``(target, selector)`` pairs and then throws
   the calldata away; it never signs or submits. The flag must never be set on
   a funds-moving path.
3. ``CurveConfig.force_is_ng`` lets discovery compile each vector under **both**
   ABI variants, so a live ``is_ng`` probe that disagrees with the static
   registry at execution time still finds its selector authorised.

Measured after the fix — 0 network calls, 0 warnings, byte-identical across
repeated runs: arbitrum 7 targets, base 9, ethereum 17, optimism 7, polygon 3.

Same defect class, NOT fixed here: ``gmx_v2``, ``pendle``, ``traderjoe_v2`` and
``uniswap_v4`` hooks also derive their manifests from implicit live reads —
they discover *nothing* with the fallback removed, which is why
``offline_discovery`` is opt-in per connector rather than the default.

Known limitation — uncurated pools. ``CurveCompiler`` can also resolve a pool
that is absent from ``CURVE_POOLS`` via the on-chain MetaRegistry
(``_resolve_dynamic_pool``, VIB-5628). Such a pool address is only knowable at
runtime, so no ahead-of-time manifest can authorise it: a Safe-wallet strategy
must LP into a *registered* pool, or the pool must be added to ``CURVE_POOLS``
first. Synthetic discovery is deliberately scoped to the registered set rather
than widened with a wildcard target — a wildcard would re-introduce exactly the
over-grant VIB-6057 is about.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.permissions.hints import DiscoveryContext, PermissionHints

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent

logger = logging.getLogger(__name__)

PERMISSION_HINTS = PermissionHints(
    synthetic_discovery_intents=frozenset({"SWAP", "LP_OPEN", "LP_CLOSE"}),
    # Curve's compiler produces complete LP calldata with zero network reads
    # (see the module docstring): the discovery compiler substitutes synthetic
    # slippage bounds for the on-chain quotes, and compiles each vector under
    # BOTH ABI variants. Opting in here suppresses the compiler's implicit
    # Anvil/public-RPC fallback, which is what made the manifest a function of
    # rate-limit weather rather than of CURVE_POOLS (VIB-6046 D5).
    offline_discovery=True,
)

# Synthetic deposit/withdrawal sizes. Permission discovery only cares about the
# ``(target, selector)`` pairs the compiler emits, never about the numbers — but
# they must be positive and finite or the intent validators reject them.
_SYNTHETIC_COIN_AMOUNT = Decimal("1")
_SYNTHETIC_LP_AMOUNT = "1"
# A per-coin withdrawal vector for ``remove_liquidity_imbalance``. Small enough
# that the adapter's fail-closed max-burn ceiling stays satisfiable against a
# 1-LP-token position on every registered StableSwap pool.
_SYNTHETIC_IMBALANCED_AMOUNT = Decimal("0.001")


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
    chain: str,
    ctx: DiscoveryContext,
) -> list[AnyIntent] | None:
    """Dispatch synthetic discovery for every intent type curve owns.

    Curve owns SWAP, LP_OPEN and LP_CLOSE discovery because all three are
    per-pool: the framework's chain-default token pair resolves to at most one
    curated pool, leaving every other registered pool unauthorised on the Safe.
    Each builder walks ``CURVE_POOLS[chain]`` so the manifest covers the whole
    registered set.

    Returns ``None`` for any other intent type so the framework default takes
    over.
    """
    if intent_type == "SWAP":
        return _build_swap_vectors(chain)
    if intent_type == "LP_OPEN":
        return _build_lp_open_vectors(chain)
    if intent_type == "LP_CLOSE":
        return _build_lp_close_vectors(chain)
    return None


def _chain_pools(chain: str) -> dict[str, dict[str, Any]]:
    """Return the registered curve pools for ``chain`` (empty dict when none)."""
    from .adapter import CURVE_POOLS

    return CURVE_POOLS.get(chain, {})


def _build_swap_vectors(chain: str) -> list[AnyIntent]:
    """Emit one synthetic ``SwapIntent`` per curated curve pool on ``chain``.

    Curve pools are pair-specific (StableSwap, CryptoSwap, Tricrypto), so a
    single token pair only resolves to one pool. ``CurveCompiler`` walks
    ``CURVE_POOLS[chain]`` to match pool by
    coin pair; emitting one intent per registered pool — using the first
    two coin addresses of each — guarantees every pool's address lands on
    the manifest.

    The price-oracle gate in ``CurveCompiler`` (price_ratio for
    CryptoSwap/Tricrypto pools) does NOT fire during permission discovery
    because ``IntentCompiler`` is created with ``allow_placeholder_prices=True``
    and ``_require_token_price`` returns the placeholder map (USDT=$1,
    WETH=$2000, WBTC=$45000, …) — every pool's coin pair resolves to a
    finite, positive price_ratio.

    No registered pool sets ``use_underlying`` today — polygon's aave-type
    am3pool was removed under VIB-5551 (frozen Aave V2 Polygon reserves made
    it non-executable); polygon's representative is now the frxUSD/USDT
    StableSwap-NG pool. If an aave-type pool is ever re-registered, the
    compiler routes to ``exchange_underlying`` automatically based on the
    pool's flags; no special-casing is needed here.

    """
    from almanak.framework.intents.vocabulary import SwapIntent

    chain_pools = _chain_pools(chain)
    if not chain_pools:
        return []

    intents: list[AnyIntent] = []
    for pool_name, pool_data in chain_pools.items():
        coins = pool_data.get("coin_addresses") or []
        if len(coins) < 2:
            logger.warning(
                "Curve pool %s on %s has fewer than 2 coins; skipping synthetic discovery",
                pool_name,
                chain,
            )
            continue
        intents.append(
            SwapIntent(
                from_token=coins[0],
                to_token=coins[1],
                amount=Decimal("1"),
                protocol="curve",
                chain=chain,
            )
        )
    return intents


def _build_lp_open_vectors(chain: str) -> list[AnyIntent]:
    """Emit synthetic ``LPOpenIntent``s covering every registered pool's deposit path.

    Two shapes, both keyed off the pool's own registry entry so the discovered
    targets/selectors come from the compiler rather than from literals here:

    * **Native deposit** — a full ``coin_amounts`` vector of length ``n_coins``.
      A vector (rather than the legacy ``amount0``/``amount1`` two-slot form) is
      what makes discovery authorise an ``approve`` on *every* pool coin: the
      two-slot form zero-fills the tail, so on a 3- or 4-coin pool the trailing
      coins would never appear on the manifest and a real deposit funding them
      would revert unauthorized at ``execTransactionWithRole``.
    * **Metapool underlying (zap) deposit** — a combined-space vector of length
      ``1 + len(base_pool_coins)``. This routes through the pool's
      ``zap_address``, a target that the native shape never touches.

    Both are compiled offline: ``CurveCompiler.compile_lp_open`` needs no live
    position state, and its price-oracle gate is satisfied by the discovery
    compiler's placeholder prices (see ``_build_swap_vectors``).
    """
    from almanak.framework.intents.vocabulary import LPOpenIntent, PriceBand

    chain_pools = _chain_pools(chain)
    if not chain_pools:
        return []

    # Curve LP is fungible — there is no price range. ``LPOpenIntent`` still
    # requires a well-formed band, and stating it as an explicit ``PriceBand``
    # (rather than a bare whole-number pair) keeps the intent valid on every
    # protocol's validator (VIB-5867).
    band = PriceBand(lower=Decimal("1500"), upper=Decimal("4000"))

    intents: list[AnyIntent] = []
    for pool_name, pool_data in chain_pools.items():
        n_coins = int(pool_data.get("n_coins") or 0)
        if n_coins < 2:
            logger.warning(
                "Curve pool %s on %s declares n_coins=%s; skipping LP_OPEN synthetic discovery",
                pool_name,
                chain,
                pool_data.get("n_coins"),
            )
            continue
        vectors: list[list[Decimal]] = [[_SYNTHETIC_COIN_AMOUNT] * n_coins]
        if pool_data.get("is_metapool"):
            combined_len = 1 + len(pool_data.get("base_pool_coins") or [])
            if combined_len != n_coins:
                vectors.append([_SYNTHETIC_COIN_AMOUNT] * combined_len)
        for coin_amounts in vectors:
            intents.append(
                LPOpenIntent(
                    pool=pool_name,
                    # ``coin_amounts`` is the full allocation vector; the
                    # two-slot ``amount0``/``amount1`` fields are required but
                    # must be zero alongside it (LPOpenIntent validator).
                    coin_amounts=coin_amounts,
                    amount0=Decimal("0"),
                    amount1=Decimal("0"),
                    range_spec=band,
                    range_lower=band.lower,
                    range_upper=band.upper,
                    protocol="curve",
                    chain=chain,
                )
            )
    return intents


def _build_lp_close_vectors(chain: str) -> list[AnyIntent]:
    """Emit synthetic ``LPCloseIntent``s covering every registered pool's exit shapes.

    ``CurveCompiler._dispatch_remove_liquidity`` picks one of three adapter
    calls — and therefore one of three *different selectors on the same pool* —
    from the intent's shape. A manifest that only covers the default
    proportional exit leaves a strategy that closes via ``coin_index`` or
    ``imbalanced_amounts`` reverting unauthorized mid-teardown, which is the
    worst possible moment to discover a permission gap. All three are emitted:

    * proportional ``remove_liquidity`` (default),
    * ``remove_liquidity_one_coin`` (``coin_index``),
    * ``remove_liquidity_imbalance`` (``imbalanced_amounts``) — **StableSwap
      family only**; the compiler rejects it on CryptoSwap/Tricrypto pools, so
      emitting it there would add a guaranteed compilation-failure warning to
      every manifest without adding a selector.

    ``position_id`` is an LP-token *amount* for curve (not an NFT id), so the
    framework's ``synthetic_position_id`` hint does not apply.
    """
    from almanak.framework.intents.vocabulary import LPCloseIntent

    chain_pools = _chain_pools(chain)
    if not chain_pools:
        return []

    intents: list[AnyIntent] = []
    for pool_name, pool_data in chain_pools.items():
        n_coins = int(pool_data.get("n_coins") or 0)
        if n_coins < 2:
            logger.warning(
                "Curve pool %s on %s declares n_coins=%s; skipping LP_CLOSE synthetic discovery",
                pool_name,
                chain,
                pool_data.get("n_coins"),
            )
            continue
        shapes: list[dict[str, Any]] = [{}, {"coin_index": 0}]
        if str(pool_data.get("pool_type") or "").lower() == "stableswap":
            shapes.append({"imbalanced_amounts": [_SYNTHETIC_IMBALANCED_AMOUNT] * n_coins})
        for shape in shapes:
            intents.append(
                LPCloseIntent(
                    pool=pool_name,
                    position_id=_SYNTHETIC_LP_AMOUNT,
                    protocol="curve",
                    chain=chain,
                    **shape,
                )
            )
    return intents
