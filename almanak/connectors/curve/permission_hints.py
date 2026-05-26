"""Curve Finance permission hints for permission discovery.

Curve does NOT use the generic ``synthetic_swap_pair`` mechanism. Its pools
are pair-specific (StableSwap, CryptoSwap, Tricrypto), so a single pair only
resolves to one curated pool per chain — leaving every other registered pool
unauthorised on the Safe (#1903).

Curve owns its discovery vectors via ``build_discovery_vectors`` below —
see :func:`almanak.framework.permissions.hints.get_discovery_vectors_override`
for the dispatcher contract.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.framework.permissions.hints import DiscoveryContext, PermissionHints

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent

logger = logging.getLogger(__name__)

PERMISSION_HINTS = PermissionHints()


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
    chain: str,
    ctx: DiscoveryContext,
) -> list[AnyIntent] | None:
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

    For polygon's am3pool which sets ``use_underlying=True``, the compiler
    routes to ``exchange_underlying`` automatically based on the pool's
    pool_type; no special-casing is needed here.

    Returns ``None`` for any ``intent_type`` other than ``SWAP`` so the
    framework default takes over for non-SWAP intents (curve only owns
    SWAP discovery today).
    """
    if intent_type != "SWAP":
        return None

    from almanak.framework.intents.vocabulary import SwapIntent

    from .adapter import CURVE_POOLS

    chain_pools = CURVE_POOLS.get(chain, {})
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
