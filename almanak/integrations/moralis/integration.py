from almanak.integrations._base import ImportRef, Integration

INTEGRATION = Integration(
    name="moralis",
    gateway_portfolio_provider=ImportRef(
        module="almanak.integrations.moralis.gateway.factory",
        attribute="MoralisPortfolioProviderFactory",
    ),
)
