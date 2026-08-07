from almanak.integrations._base import ImportRef, Integration

INTEGRATION = Integration(
    name="gmx",
    gateway_price_source=ImportRef(
        module="almanak.integrations.gmx.gateway.factory",
        attribute="GmxTickerPriceSourceFactory",
        order=10,
    ),
    # Deliberately NO exclusive group: hypercore's "venue_oracle" group blocks
    # Binance (price_source_blocked_by_groups), which is correct on HyperEVM
    # where the venue oracle replaces CEX pricing, and would be a silent
    # regression on Arbitrum/Avalanche where this source only ADDS the
    # synthetic-index symbols the existing stack cannot price at all.
)
