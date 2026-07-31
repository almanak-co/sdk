"""Uniswap V4 permission-discovery hints + synthetic-intent dispatch.

V4 owns its full synthetic-discovery dispatch through
:func:`build_discovery_vectors` below — the connector self-containment
endpoint (VIB-4121 pattern, mirrors ``traderjoe_v2``). This is *required*
rather than cosmetic: the framework-default synthetic builders in
``almanak/framework/permissions/synthetic_intents.py`` cannot construct
V4-compatible intents (VIB-4421):

- **SWAP** — the default ``_build_swap_intents`` gates non-exempt protocols on
  ``PROTOCOL_ROUTERS.get(chain)``. V4 doesn't use ``PROTOCOL_ROUTERS``; it
  routes through the canonical UniversalRouter resolved from
  ``uniswap_v4/addresses.py`` (``UNISWAP_V4[chain]["universal_router"]``), so
  the default returns ``[]`` for V4. This override runs *before* that gate.
- **LP_OPEN** — the default emits ``pool="{token0}/{token1}"`` (no fee tier)
  unless the protocol is in ``SWAP_FEE_TIERS`` or declares ``synthetic_fee_tier``.
  V4's pool key is ``(currency0, currency1, fee, tickSpacing, hooks)``, so its
  LP compiler *requires* the ``TOKEN0/TOKEN1/FEE`` form and rejects the bare
  pair (``Invalid pool format``).
- **LP_CLOSE** — V4's LP_CLOSE compiler needs ``currency0``/``currency1`` (or a
  resolvable ``pool``) in ``protocol_params`` to reconstruct the pool key; the
  default only supplies ``position_id``.
- **LP_COLLECT_FEES** — no longer overridden. V4's collect compiler needs
  ``position_id`` in ``protocol_params``, which the shared builder now supplies
  (VIB-6149). This entry previously documented a defect in that shared builder
  while working around it locally, which is why the same bug silently affected
  every other connector for as long as it did.

The three synthetic shapes still overridden below (SWAP, LP_OPEN, LP_CLOSE —
LP_COLLECT_FEES now falls through to the framework default) were verified to
compile offline via the real
``IntentCompiler`` (``allow_placeholder_prices=True``, ``permission_discovery=True``,
no RPC) and to emit the expected targets: token + Permit2 approvals, the
UniversalRouter (``execute`` ``0x3593564c``) for SWAP, and the PositionManager
(``0xdd46508f``) for the LP trio. See VIB-4421 (https://linear.app/almanak/issue/VIB-4421).

``supports_native_in_swap`` is deliberately left ``False``: it models the
V3-style ``SwapRouter02`` ``msg.value`` auto-wrap that flips ``send_allowed`` on
the router target. V4's native-ETH path runs through the UniversalRouter's
settle/take flow (a distinct mechanism) and no V4 intent test exercises a
native-in swap — every V4 swap test uses an ERC-20 pair. Authorising
value-bearing V4 UR calls is separate, independently-validated work.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.core.intent_types import IntentType
from almanak.framework.permissions.hints import DiscoveryContext, PermissionHints

from .addresses import UNISWAP_V4

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent

# Canonical synthetic LP fee tier (0.30% → tickSpacing 60). V4 LP discovery
# needs a concrete (fee, tickSpacing) so the pool-key encode path runs; 3000/60
# is the deepest-liquidity WETH/USDC tier and matches the ``WETH/USDC/3000``
# pool string the real V4 LP intent tests use, so the generated manifest
# authorises the exact PositionManager selectors those tests exercise.
_SYNTHETIC_FEE_TIER = 3000
_SYNTHETIC_TICK_SPACING = 60


PERMISSION_HINTS = PermissionHints(
    # Synthetic-discovery participation (VIB-4928 derivation; wired by VIB-4421).
    # SWAP + LP via the override below. LP_COLLECT_FEES is NOT listed here — it
    # stays gated by ``supports_standalone_fee_collection`` (the V4 compiler's
    # ``_compile_collect_fees_uniswap_v4`` supports standalone collection).
    synthetic_discovery_intents=frozenset({IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE}),
    supports_standalone_fee_collection=True,
)


def build_discovery_vectors(
    protocol: str,
    intent_type: IntentType,
    chain: str,
    ctx: DiscoveryContext,
) -> list[AnyIntent] | None:
    """Emit V4-shaped synthetic intents covering every selector the manifest needs.

    Returns ``None`` for any ``(intent_type, chain)`` V4 does not deploy on, so
    the framework default cleanly short-circuits to ``[]`` — matching the prior
    ``no_zodiac`` behaviour for unsupported chains. ``chain`` is gated on
    ``UNISWAP_V4`` membership (the same registry the adapter resolves its
    contracts from), so a chain without V4 contracts emits nothing.
    """
    # ``chain`` here is the SDK canonical chain name: the whole discovery
    # pipeline (``_get_token_pair``, ``UNISWAP_V4``, the V4 compiler) is keyed
    # on canonical names, so BSC arrives as ``"bsc"`` — never ``"bnb"`` (a
    # ChainRegistry alias; ``supported_chains`` declarations canonicalize at
    # the registry boundary too). Returning ``None`` for a non-SDK
    # string is the framework-consistent behaviour (every connector's token
    # resolution is bsc-keyed too); we do NOT alias bnb→bsc here because
    # ``ctx.usdc``/``ctx.weth`` would still be the wrong (fallback) tokens for
    # a ``"bnb"`` caller, yielding a broken partial manifest instead of a
    # clean empty one.
    if chain not in UNISWAP_V4:
        return None

    from almanak.framework.intents.vocabulary import (
        LPCloseIntent,
        LPOpenIntent,
        SwapIntent,
    )

    if intent_type is IntentType.SWAP:
        return [
            SwapIntent(
                from_token=ctx.usdc,
                to_token=ctx.weth,
                amount=Decimal("1"),
                protocol=protocol,
                chain=chain,
            )
        ]

    if intent_type is IntentType.LP_OPEN:
        return [
            LPOpenIntent(
                pool=f"{ctx.usdc}/{ctx.weth}/{_SYNTHETIC_FEE_TIER}",
                amount0=Decimal("100"),
                amount1=Decimal("0.05"),
                range_lower=Decimal("1500"),
                range_upper=Decimal("4000"),
                protocol=protocol,
                chain=chain,
                # Synthetic discovery is offline (no RPC / placeholder prices),
                # so V4's on-chain ``sqrtPrice`` read is unavailable and the LP
                # compiler falls back to an estimated price. Without this flag
                # the estimated-price guard (VIB-2180) rejects the default 0.5%
                # slippage on any chain where no placeholder price exists for
                # the pair (optimism, avalanche) — dropping the second token's
                # approval from the manifest and breaking the real LP_OPEN test
                # under Zodiac there. We only need the (target, selector) set,
                # not price accuracy, so opt into the estimate explicitly to
                # make LP_OPEN discovery deterministic across all V4 chains.
                protocol_params={"allow_estimated_price": True},
            )
        ]

    if intent_type is IntentType.LP_CLOSE:
        # V4 LP_CLOSE reconstructs the pool key from ``protocol_params`` — the
        # default builder only supplies ``position_id`` (insufficient for V4).
        return [
            LPCloseIntent(
                position_id="1",
                protocol=protocol,
                chain=chain,
                protocol_params={
                    "currency0": ctx.usdc,
                    "currency1": ctx.weth,
                    "fee": _SYNTHETIC_FEE_TIER,
                    "tick_spacing": _SYNTHETIC_TICK_SPACING,
                },
            )
        ]

    # LP_COLLECT_FEES deliberately falls through to the framework default
    # (VIB-6149). This branch used to build its own CollectFeesIntent purely to
    # supply ``position_id`` in ``protocol_params`` — a local workaround for a
    # defect in the SHARED builder, which never supplied it and therefore could
    # not compile for ANY protocol. That is now fixed at the source in
    # ``_build_lp_collect_fees_intents``, so the workaround is redundant.
    #
    # Verified before deleting rather than assumed: the emitted manifest is
    # byte-identical (targets, selectors and send_allowed) with and without this
    # branch on all seven chains V4 declares.
    #
    # The override also passed a fee tier in the pool string that the default
    # omits. That is inert — but NOT because the pool string is unused here, and
    # stating it that way would be wrong in a load-bearing place. ``pool`` IS
    # consulted: ``compile_collect_fees`` falls back to
    # ``_resolve_pool_currencies(...)`` to derive ``currency0``/``currency1``
    # whenever ``protocol_params`` omits them, which is exactly what the
    # framework default does. The tier is inert only because that helper reads
    # ``pool.split("/")[:2]`` and never looks at a third segment.
    #
    # So the load-bearing precondition for this deletion is narrower than "pool
    # is irrelevant": it is that the first two segments still resolve to the
    # right currencies. Make ``_resolve_pool_currencies`` strict about segment
    # count, or make collection depend on fee / tick-spacing / hooks, and this
    # branch has to come back.
    #
    # Second precondition, latent today: collect now runs the framework default,
    # which gates on ``LP_POSITION_MANAGERS``, whereas the SWAP / LP_OPEN /
    # LP_CLOSE branches above return non-``None`` and so never reach that gate.
    # Collect is therefore the only V4 LP verb subject to it. The two registries
    # list the same seven chains today (verified), so this is inert — but adding
    # a V4 chain to ``addresses.py`` WITHOUT adding it to ``LP_POSITION_MANAGERS``
    # would silently drop collect on that chain while the other verbs keep working.
    return None
