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
- **LP_COLLECT_FEES** — V4's collect compiler needs ``position_id`` in
  ``protocol_params``; the default ``CollectFeesIntent`` only supplies ``pool``.

All four synthetic shapes below were verified to compile offline via the real
``IntentCompiler`` (``allow_placeholder_prices=True``, ``permission_discovery=True``,
no RPC) and to emit the expected targets: token + Permit2 approvals, the
UniversalRouter (``execute`` ``0x3593564c``) for SWAP, and the PositionManager
(``0xdd46508f``) for the LP trio. See ``docs/internal/VIB-4421-v4-zodiac-matrix.md``.

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
    synthetic_discovery_intents=frozenset({"SWAP", "LP_OPEN", "LP_CLOSE"}),
    supports_standalone_fee_collection=True,
)


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
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
    # ``chain`` here is the SDK chain name, NOT the user-facing venue alias: the
    # whole discovery pipeline (``_get_token_pair``, ``UNISWAP_V4``, the V4
    # compiler) is keyed on SDK names, so BSC arrives as ``"bsc"`` — never
    # ``"bnb"`` (the KNOWN_VENUES alias used only in ``strategy_chains``).
    # Returning ``None`` for a non-SDK string is the framework-consistent
    # behaviour (every connector's token resolution is bsc-keyed too); we do NOT
    # alias bnb→bsc here because ``ctx.usdc``/``ctx.weth`` would still be the
    # wrong (fallback) tokens for a ``"bnb"`` caller, yielding a broken partial
    # manifest instead of a clean empty one.
    if chain not in UNISWAP_V4:
        return None

    from almanak.framework.intents.vocabulary import (
        CollectFeesIntent,
        LPCloseIntent,
        LPOpenIntent,
        SwapIntent,
    )

    if intent_type == "SWAP":
        return [
            SwapIntent(
                from_token=ctx.usdc,
                to_token=ctx.weth,
                amount=Decimal("1"),
                protocol=protocol,
                chain=chain,
            )
        ]

    if intent_type == "LP_OPEN":
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

    if intent_type == "LP_CLOSE":
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

    if intent_type == "LP_COLLECT_FEES":
        # V4 standalone fee collection needs ``position_id`` in protocol_params;
        # the default ``CollectFeesIntent`` only supplies ``pool``.
        return [
            CollectFeesIntent(
                pool=f"{ctx.usdc}/{ctx.weth}/{_SYNTHETIC_FEE_TIER}",
                protocol=protocol,
                chain=chain,
                # str to match the LP_CLOSE synthetic above (the V4 collect
                # compiler coerces via ``int(...)``; both forms compile).
                protocol_params={"position_id": "1"},
            )
        ]

    return None
