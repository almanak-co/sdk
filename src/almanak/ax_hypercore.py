"""HyperCore observations for `almanak ax hypercore-markets` and `hypercore-account`.

HyperCore is a venue of `eip155:999`. Its state comes from Hyperliquid's `/info`
API as the API serves it: there is no block to pin, so every report names the
query and the time its answer was received instead. Prices, funding and volume
are the venue's own figures, reported as it writes them.
"""

from decimal import Decimal

from almanak.connectors.hyperliquid.assets import spot_asset
from almanak.connectors.hyperliquid.markets import PerpMarket, find_market, perp_markets
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import (
    ClearinghouseState,
    ExtraAgents,
    InfoRequest,
    InfoResult,
    Meta,
    MetaAndAssetCtxs,
    OpenOrders,
    SpotClearinghouseState,
    SpotMeta,
    UserAbstraction,
    UserRole,
    typed,
)
from almanak.gateway.hypercore_shapes import SpotBalance, SpotToken
from almanak.models import address

VENUE = {"chain": "eip155:999", "venue": "hypercore", "dex": ""}
# Hyperliquid: under these modes the spot state holds every balance and hold, and the
# per-dex perp state is not meaningful.
UNIFIED_MODES = ("unifiedAccount", "portfolioMargin")


def markets(info, query: str | None, limit: int, offset: int, include_delisted: bool) -> dict:
    result = info.fetch(InfoRequest(info=MetaAndAssetCtxs()))
    listed = perp_markets(typed(result))
    if query is not None:
        page, matched = [find_market(listed, query)], 1
    else:
        candidates = [m for m in listed if include_delisted or not m.meta.isDelisted]
        # Most traded first, by the venue's own 24-hour notional volume.
        candidates.sort(key=lambda m: Decimal(m.context.dayNtlVlm), reverse=True)
        page, matched = candidates[offset : offset + limit], len(candidates)
    return {
        "product": "hypercore_perp_markets",
        "venue": VENUE,
        "source": source(result),
        "matched": matched,
        "markets": [market_report(market) for market in page],
    }


def market_report(market: PerpMarket) -> dict:
    tiers = market.margin_tiers
    return {
        "target": {"dex": "", "coin": market.meta.name},
        "asset": market.asset,
        "meta": market.meta.model_dump(mode="json", exclude_none=True),
        "margin_tiers": None if tiers is None else [t.model_dump(mode="json") for t in tiers],
        "context": market.context.model_dump(mode="json"),
    }


def accounts(info, values: list[str]) -> dict:
    users = [address(value) for value in values]
    spot_meta = info.fetch(InfoRequest(info=SpotMeta()))
    meta = info.fetch(InfoRequest(info=Meta()))
    tokens = {token.index: token for token in typed(spot_meta).tokens}
    indices = {asset.name: index for index, asset in enumerate(typed(meta).universe)}
    return {
        "product": "hypercore_account",
        "venue": VENUE,
        "sources": [source(spot_meta), source(meta)],
        "accounts": [account_report(info, user, tokens, indices) for user in users],
    }


def account_report(info, user: str, tokens: dict[int, SpotToken], indices: dict[str, int]):
    queries = (
        UserRole(user=user),
        UserAbstraction(user=user),
        ClearinghouseState(user=user),
        SpotClearinghouseState(user=user),
        OpenOrders(user=user),
        ExtraAgents(user=user),
    )
    role, abstraction, perp, spot, orders, agents = (
        info.fetch(InfoRequest(info=query)) for query in queries
    )
    mode = typed(abstraction)
    state = typed(perp)
    return {
        "account": user,
        # "missing" means HyperCore holds no account for the address yet.
        "role": role.result,
        "abstraction": mode,
        "perp": {
            "state": "not_meaningful" if mode in UNIFIED_MODES else "as_reported",
            "margin_summary": state.marginSummary.model_dump(mode="json"),
            "cross_margin_summary": state.crossMarginSummary.model_dump(mode="json"),
            "cross_maintenance_margin_used": state.crossMaintenanceMarginUsed,
            "withdrawable": state.withdrawable,
            "positions": [
                {
                    "asset": indices.get(entry.position.coin),
                    "position": entry.position.model_dump(mode="json"),
                }
                for entry in state.assetPositions
            ],
            "time": state.time,
        },
        "spot": [balance_report(balance, tokens) for balance in typed(spot).balances],
        # Every open order on the first dex, spot included, whoever placed it.
        "open_orders": orders.result,
        # The agents the account approved, each with its expiry in ms (null for none).
        "agents": agents.result,
        "sources": [source(result) for result in (role, abstraction, perp, spot, orders, agents)],
    }


def balance_report(balance: SpotBalance, tokens: dict[int, SpotToken]) -> dict:
    token = tokens.get(balance.token)
    if token is None:
        raise DataError(
            "inconsistent", f"HyperCore spot balance names token {balance.token} spotMeta lacks"
        )
    asset = spot_asset(token)
    return {
        # Null when HyperCore links the token to no HyperEVM asset: it has no identity.
        "asset": None if asset is None else str(asset),
        "token": {
            "index": token.index,
            "token_id": token.tokenId,
            "name": token.name,
            "wei_decimals": token.weiDecimals,
        },
        # Decimal amounts in the token's HyperCore units, as the venue writes them.
        "total": balance.total,
        "hold": balance.hold,
        "entry_notional": balance.entryNtl,
    }


def source(result: InfoResult) -> dict:
    return {"query": result.request.info.type, "received_at": result.received_at}
