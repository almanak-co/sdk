from almanak.integrations._base import ImportRef, Integration

INTEGRATION = Integration(
    name="coingecko",
    gateway_price_source=ImportRef(
        module="almanak.integrations.coingecko.gateway.factory",
        attribute="CoinGeckoPriceSourceFactory",
        order=40,
    ),
    gateway_api_client=ImportRef(
        module="almanak.integrations.coingecko.gateway.factory",
        attribute="CoinGeckoClientFactory",
    ),
)
