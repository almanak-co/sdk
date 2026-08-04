from almanak.integrations._base import ImportRef, Integration

INTEGRATION = Integration(
    name="dexscreener",
    gateway_price_source=ImportRef(
        module="almanak.integrations.dexscreener.gateway.factory",
        attribute="DexScreenerPriceSourceFactory",
        order=30,
    ),
)
