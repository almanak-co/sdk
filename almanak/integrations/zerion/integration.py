from almanak.integrations._base import ImportRef, Integration

INTEGRATION = Integration(
    name="zerion",
    gateway_api_client=ImportRef(
        module="almanak.integrations.zerion.gateway.factory",
        attribute="ZerionClientFactory",
    ),
    gateway_portfolio_provider=ImportRef(
        module="almanak.integrations.zerion.gateway.factory",
        attribute="ZerionPortfolioProviderFactory",
    ),
)
