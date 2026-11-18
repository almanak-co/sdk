"""HyperCore perpetual markets as the venue's `meta` names them.

A market target is a coin of the first perp dex. Its asset index, the number every
order carries, is its position in that dex's universe: it is read from `meta` when
the target resolves and kept with the resolved evidence, never written by an
author. HIP-3 dexes are not admitted.
"""

from dataclasses import dataclass

from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_shapes import MarginTier, PerpAsset, PerpAssetContext, PerpMeta


@dataclass(frozen=True)
class PerpMarket:
    asset: int
    meta: PerpAsset
    # The tiers of the market's margin table when `meta` lists it. Tables the market
    # names but `meta` does not list are left unknown rather than inferred.
    margin_tiers: tuple[MarginTier, ...] | None
    context: PerpAssetContext


def perp_markets(answer: tuple[PerpMeta, tuple[PerpAssetContext, ...]]) -> tuple[PerpMarket, ...]:
    """Every market of a `metaAndAssetCtxs` answer, in asset index order."""
    meta, contexts = answer
    tables = {identifier: table.marginTiers for identifier, table in meta.marginTables}
    names = [asset.name for asset in meta.universe]
    if len(set(names)) != len(names):
        raise DataError("inconsistent", "HyperCore meta names one perp market twice")
    return tuple(
        PerpMarket(index, asset, tables.get(asset.marginTableId), context)
        for index, (asset, context) in enumerate(zip(meta.universe, contexts, strict=True))
    )


def find_market(markets: tuple[PerpMarket, ...], coin: str) -> PerpMarket:
    """The market `meta` names `coin`, or the one it names so apart from letter case."""
    for market in markets:
        if market.meta.name == coin:
            return market
    folded = [market for market in markets if market.meta.name.casefold() == coin.casefold()]
    if len(folded) == 1:
        return folded[0]
    if folded:
        named = ", ".join(market.meta.name for market in folded)
        raise DataError("ambiguous", f"HyperCore perp market {coin!r} could be any of {named}")
    raise DataError("not_found", f"no HyperCore perp market is named {coin!r}")
