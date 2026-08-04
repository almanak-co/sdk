from almanak.integrations._base import ImportRef, Integration

INTEGRATION = Integration(
    name="hypercore",
    gateway_price_source=ImportRef(
        module="almanak.integrations.hypercore.gateway.factory",
        attribute="HypercorePriceSourceFactory",
        order=10,
    ),
    price_source_exclusive_group="venue_oracle",
)
