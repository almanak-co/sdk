"""SDK integration wrappers for gateway-backed data sources.

This package provides clean Python APIs for accessing third-party data sources
through the gateway. API keys are kept secure in the gateway.

Usage in strategies:
    from almanak.framework.integrations import binance, coingecko, thegraph

    # Binance market data
    ticker = binance.get_ticker("BTCUSDT")
    klines = binance.get_klines("ETHUSDT", interval="1h", limit=100)

    # CoinGecko prices
    price = coingecko.get_price("ethereum", vs_currencies=["usd", "eur"])
    markets = coingecko.get_markets(vs_currency="usd")

    # TheGraph queries
    result = thegraph.query(
        subgraph_id="uniswap-v3-arbitrum",
        query="{ pools(first: 10) { id } }",
    )
"""

from almanak.framework.integrations import binance, coingecko, thegraph

__all__ = [
    "binance",
    "coingecko",
    "thegraph",
]
