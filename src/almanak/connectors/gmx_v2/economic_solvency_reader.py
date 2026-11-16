"""Capture the deployed MarketUtils token-balance obligations at each opportunity."""

from almanak.connectors.gmx_v2.economic_solvency import MarketSolvency, TokenSolvency
from almanak.connectors.gmx_v2.oracle_reader import stored


def market_token_uint(reader, market, block, asset, name):
    return stored(
        reader,
        block,
        market.contracts.data_store,
        "Uint",
        name,
        ("address", "address"),
        (market.share_asset.reference, asset.reference),
    )


def token_solvency(reader, market, block, asset):
    minimum = sum(
        market_token_uint(reader, market, block, asset, name)
        for name in (
            "POOL_AMOUNT",
            "SWAP_IMPACT_POOL_AMOUNT",
            "CLAIMABLE_COLLATERAL_AMOUNT",
            "CLAIMABLE_FEE_AMOUNT",
            "CLAIMABLE_UI_FEE_AMOUNT",
            "AFFILIATE_REWARD",
        )
    )
    collateral = sum(
        stored(
            reader,
            block,
            market.contracts.data_store,
            "Uint",
            "COLLATERAL_SUM",
            ("address", "address", "bool"),
            (market.share_asset.reference, asset.reference, is_long),
        )
        for is_long in (True, False)
    )
    return TokenSolvency(
        asset=asset,
        balance=reader.call(
            asset.reference,
            "balanceOf(address)",
            block,
            inputs=("address",),
            args=(market.share_asset.reference,),
            outputs=("uint256",),
        )[0],
        minimum_balance=minimum,
        collateral_amount=collateral,
        claimable_funding_amount=market_token_uint(
            reader, market, block, asset, "CLAIMABLE_FUNDING_AMOUNT"
        ),
    )


def market_solvency(reader, market, block):
    return MarketSolvency(
        market=market,
        block=block,
        long=token_solvency(reader, market, block, market.long_asset),
        short=token_solvency(reader, market, block, market.short_asset),
    )
