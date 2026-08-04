from almanak.integrations._base import ImportRef, Integration

INTEGRATION = Integration(
    name="okx",
    gateway_portfolio_provider=ImportRef(
        module="almanak.integrations.okx.gateway.factory",
        attribute="OkxPortfolioProviderFactory",
    ),
)
