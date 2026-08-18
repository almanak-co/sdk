"""Curve Finance permission hints for permission discovery.

Curve does NOT use the generic ``synthetic_swap_pair`` mechanism. Its pools
are deployment resources, so a Safe manifest must compile the exact live-bound
pool selected by the deployment rather than a chain-wide SDK catalog.

Curve owns its discovery vectors via ``build_discovery_vectors`` below —
see :func:`almanak.framework.permissions.hints.get_discovery_vectors_override`
for the dispatcher contract.

Synthetic-discovery participation (VIB-4928 / VIB-6046): ``SWAP``, ``LP_OPEN``
and ``LP_CLOSE``. Every one is discovered by *compiling* real intents against
an immutable MetaRegistry-verified binding, so no target or selector literal
lives in this module.

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
rate-limit weather rather than explicit deployment input. Three consecutive runs
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
3. ``CurveConfig.force_is_ng`` lets discovery compile each StableSwap vector
   under **both** ABI variants, so runtime ABI fingerprinting always finds its
   selector authorised.

After one live admission read, calldata discovery is offline and deterministic.

Same defect class, NOT fixed here: ``gmx_v2``, ``pendle``, ``traderjoe_v2`` and
``uniswap_v4`` hooks also derive their manifests from implicit live reads —
they discover *nothing* with the fallback removed, which is why
``offline_discovery`` is opt-in per connector rather than the default.

``CurveCompiler`` resolves exact pools through the on-chain MetaRegistry
(``_resolve_dynamic_pool``, VIB-5628). Permission discovery consumes the
deployment's exact ``pool`` plus
ordered token-address config, resolves it through the same MetaRegistry seam,
and adds connector-owned synthetic vectors for that immutable binding. There is
no global pool registry and no wildcard target. A missing binding or transport,
identity mismatch, or failed bound compile aborts manifest generation instead
of emitting an incomplete role.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.core.intent_types import IntentType
from almanak.framework.permissions.hints import DiscoveryContext, PermissionBindingError, PermissionHints

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent

logger = logging.getLogger(__name__)

PERMISSION_HINTS = PermissionHints(
    synthetic_discovery_intents=frozenset({IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE}),
    # Curve's compiler produces complete LP calldata with zero network reads
    # (see the module docstring): the discovery compiler substitutes synthetic
    # slippage bounds for the on-chain quotes, and compiles each vector under
    # BOTH ABI variants. Opting in here suppresses the compiler's implicit
    # Anvil/public-RPC fallback, which is what made the manifest a function of
    # rate-limit weather rather than explicit bindings (VIB-6046 D5).
    offline_discovery=True,
    # Exact pools opt into live MetaRegistry binding. Calldata compilation after
    # admission remains offline and receives only the frozen identity.
    needs_rpc_discovery=True,
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
    intent_type: IntentType,
    chain: str,
    ctx: DiscoveryContext,
) -> list[AnyIntent] | None:
    """Dispatch synthetic discovery for every intent type curve owns.

    Curve owns SWAP, LP_OPEN and LP_CLOSE discovery because all three are
    per-pool. The deployment must provide exact pool and ordered coin addresses;
    absence is an admission error, never an empty or wildcard manifest.

    Returns ``None`` for any other intent type so the framework default takes
    over.
    """
    if intent_type is IntentType.SWAP:
        return _build_swap_vectors(chain, ctx)
    if intent_type is IntentType.LP_OPEN:
        return _build_lp_open_vectors(chain, ctx)
    if intent_type is IntentType.LP_CLOSE:
        return _build_lp_close_vectors(chain, ctx)
    return None


def _discovery_pools(chain: str, ctx: DiscoveryContext) -> list[tuple[str, dict[str, Any], Any | None]]:
    """Return deployment-bound exact pools or fail admission closed."""
    from .pool_binding import resolve_configured_pool_bindings

    bindings = resolve_configured_pool_bindings(
        chain=chain,
        config=ctx.strategy_config,
        gateway_client=ctx.gateway_client,
        rpc_url=ctx.rpc_url,
    )
    if not bindings:
        raise PermissionBindingError(
            f"Curve permission generation on {chain} requires an exact deployment pool binding. "
            "Declare config.permission_bindings with protocol='curve', resource_type='pool', "
            "address, chain, and ordered coin_addresses; SDK-wide pool catalogs and wildcard targets are unsupported."
        )
    return [(binding.pool_address, binding.pool_data(), binding) for binding in bindings]


def _build_swap_vectors(chain: str, ctx: DiscoveryContext) -> list[AnyIntent]:
    """Emit one synthetic ``SwapIntent`` for every possible input coin.

    The price-oracle gate in ``CurveCompiler`` (price_ratio for
    CryptoSwap/Tricrypto pools) does NOT fire during permission discovery
    because ``IntentCompiler`` is created with ``allow_placeholder_prices=True``
    and ``_require_token_price`` returns the placeholder map (USDT=$1,
    WETH=$2000, WBTC=$45000, …) — every pool's coin pair resolves to a
    finite, positive price_ratio.

    Wrapped-lending pools remain rejected by the MetaRegistry resolver. Exact
    metapool bindings cover native pool calls; underlying zap calls require a
    separately verified zap resource and are not guessed here.

    """
    from almanak.framework.intents.vocabulary import SwapIntent

    pools = _discovery_pools(chain, ctx)
    if not pools:
        return []

    intents: list[AnyIntent] = []
    for pool_name, pool_data, binding in pools:
        coins = pool_data.get("coin_addresses") or []
        if len(coins) < 2:
            logger.warning(
                "Curve pool %s on %s has fewer than 2 coins; skipping synthetic discovery",
                pool_name,
                chain,
            )
            continue
        # ERC-20 approval and native-value authorization are input-dependent.
        # A single coin[0] -> coin[1] vector therefore under-authorizes reverse
        # swaps and every input at index 2+. A directed ring is sufficient and
        # least-privilege: it exercises each input coin exactly once while the
        # pool selector/target is identical for every output choice.
        for index, from_coin in enumerate(coins):
            intents.append(
                SwapIntent(
                    from_token=from_coin,
                    to_token=coins[(index + 1) % len(coins)],
                    amount=Decimal("1"),
                    protocol="curve",
                    chain=chain,
                    swap_params=(
                        {"pool": binding.pool_address, **binding.marker_params()} if binding is not None else None
                    ),
                )
            )
    return intents


def _build_lp_open_vectors(chain: str, ctx: DiscoveryContext) -> list[AnyIntent]:
    """Emit synthetic ``LPOpenIntent``s covering each bound pool's deposit path.

    The shape is keyed off the verified pool binding so the discovered
    targets/selectors come from the compiler rather than from literals here:

    * **Native deposit** — a full ``coin_amounts`` vector of length ``n_coins``.
      A vector (rather than the legacy ``amount0``/``amount1`` two-slot form) is
      what makes discovery authorise an ``approve`` on *every* pool coin: the
      two-slot form zero-fills the tail, so on a 3- or 4-coin pool the trailing
      coins would never appear on the manifest and a real deposit funding them
      would revert unauthorized at ``execTransactionWithRole``.
    It is compiled offline: ``CurveCompiler.compile_lp_open`` needs no live
    position state, and its price-oracle gate is satisfied by the discovery
    compiler's placeholder prices (see ``_build_swap_vectors``).

    Deployment bindings authorize only the exact pool they identify. Generic
    metapool underlying deposits route through a separate zap contract, so they
    are deliberately excluded until that resource has its own independently
    verified deployment binding. Discovery must never guess or inherit a zap
    target from test fixtures.
    """
    from almanak.framework.intents.vocabulary import LPOpenIntent, PriceBand

    pools = _discovery_pools(chain, ctx)
    if not pools:
        return []

    # Curve LP is fungible — there is no price range. ``LPOpenIntent`` still
    # requires a well-formed band, and stating it as an explicit ``PriceBand``
    # (rather than a bare whole-number pair) keeps the intent valid on every
    # protocol's validator (VIB-5867).
    band = PriceBand(lower=Decimal("1500"), upper=Decimal("4000"))

    intents: list[AnyIntent] = []
    for pool_name, pool_data, binding in pools:
        n_coins = int(pool_data.get("n_coins") or 0)
        if n_coins < 2:
            logger.warning(
                "Curve pool %s on %s declares n_coins=%s; skipping LP_OPEN synthetic discovery",
                pool_name,
                chain,
                pool_data.get("n_coins"),
            )
            continue
        for coin_amounts in [[_SYNTHETIC_COIN_AMOUNT] * n_coins]:
            intents.append(
                LPOpenIntent(
                    # The admitted binding's address is authoritative.  A
                    # deployment-local label is presentation only and must not
                    # become the target carried into compiler discovery.
                    pool=binding.pool_address if binding is not None else pool_name,
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
                    protocol_params=binding.marker_params() if binding is not None else None,
                )
            )
    return intents


def _build_lp_close_vectors(chain: str, ctx: DiscoveryContext) -> list[AnyIntent]:
    """Emit synthetic ``LPCloseIntent``s covering each bound pool's exit shapes.

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

    pools = _discovery_pools(chain, ctx)
    if not pools:
        return []

    intents: list[AnyIntent] = []
    for pool_name, pool_data, binding in pools:
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
                    pool=binding.pool_address if binding is not None else pool_name,
                    position_id=_SYNTHETIC_LP_AMOUNT,
                    protocol="curve",
                    chain=chain,
                    protocol_params=binding.marker_params() if binding is not None else None,
                    **shape,
                )
            )
    return intents
