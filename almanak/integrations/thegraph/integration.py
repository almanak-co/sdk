"""The Graph integration manifest."""

from almanak.integrations._base import ImportRef, Integration

INTEGRATION = Integration(
    name="thegraph",
    gateway_api_client=ImportRef(
        module="almanak.integrations.thegraph.gateway.factory",
        attribute="TheGraphClientFactory",
    ),
)
