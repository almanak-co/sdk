"""PancakeSwap Perps permission hints for permission discovery.

PancakeSwap Perps is broker_id=2 on the Aster Diamond — same on-chain
contract surface, same selectors. On BSC the Diamond accepts both ERC-20
and native BNB as margin collateral via two distinct selectors:

- ``openMarketTrade(...)``    = ``0x703085c7`` — ERC-20 collateral path
- ``openMarketTradeBNB(...)`` = ``0xb7aeae66`` — native BNB collateral path

A single ERC-20 synthetic only authorises the ERC-20 selector; without a
companion native-collateral synthetic the manifest blocks every native-margin
open at ``execTransactionWithRole``. The connector owns its discovery
vectors via ``build_discovery_vectors`` below so the BSC native-collateral
synthetic is declared next to the connector that produces the selectors.

PERP_CLOSE compiles ``closeTrade(bytes32)`` (selector ``0x5177fd3b``) and
requires ``intent.position_id`` to be a 0x-prefixed bytes32 tradeHash. A
placeholder hash satisfies the compiler's shape validation; the manifest
target is the Diamond address, not the trade-specific hash.

Mirrors ``connectors/aster_perps/permission_hints.py``. Duplication is
intentional: each connector pins its own BSC invariant, so if either
protocol ever diverges (different chain, different collateral surface), the
two overrides can evolve independently. See VIB-4121 for the connector
self-containment rationale.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.framework.permissions.hints import DiscoveryContext, PermissionHints

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent

PERMISSION_HINTS = PermissionHints()


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
    chain: str,
    ctx: DiscoveryContext,
) -> list[AnyIntent] | None:
    """Emit synthetic perp intents covering every selector the manifest needs.

    On BSC, returns BOTH the ERC-20 and native-BNB collateral synthetics for
    PERP_OPEN so the manifest authorises ``openMarketTrade`` *and*
    ``openMarketTradeBNB``. For PERP_CLOSE on BSC, returns a single intent
    with a placeholder bytes32 ``position_id`` — the Aster Diamond compile
    path requires ``intent.position_id`` to be shape-valid for
    ``closeTrade(bytes32)`` to land on the manifest at all.

    Returns ``None`` for non-BSC chains (the broker-shim only operates on
    BSC) and for non-perp intent types so the framework default takes over.
    """
    from almanak.framework.intents.vocabulary import PerpCloseIntent, PerpOpenIntent

    if chain != "bsc":
        return None

    if intent_type == "PERP_OPEN":
        return [
            # ERC-20 (USDC) collateral path -> ``openMarketTrade`` (0x703085c7)
            PerpOpenIntent(
                market="ETH/USD",
                collateral_token=ctx.usdc,
                collateral_amount=Decimal("100"),
                size_usd=Decimal("500"),
                is_long=True,
                leverage=Decimal("5"),
                protocol=protocol,
                chain=chain,
            ),
            # Native BNB collateral path -> ``openMarketTradeBNB`` (0xb7aeae66)
            PerpOpenIntent(
                market="ETH/USD",
                collateral_token="BNB",
                collateral_amount=Decimal("0.5"),
                size_usd=Decimal("500"),
                is_long=True,
                leverage=Decimal("5"),
                protocol=protocol,
                chain=chain,
            ),
        ]

    if intent_type == "PERP_CLOSE":
        # Placeholder bytes32 tradeHash satisfies the compiler's shape check
        # for ``closeTrade(bytes32)`` (selector ``0x5177fd3b``). The compiler
        # validates shape, not on-chain existence.
        placeholder_trade_hash = "0x" + "00" * 32
        return [
            PerpCloseIntent(
                market="ETH/USD",
                collateral_token=ctx.usdc,
                is_long=True,
                size_usd=None,  # closeTrade(bytes32) is always full-close
                protocol=protocol,
                chain=chain,
                position_id=placeholder_trade_hash,
            )
        ]

    return None
