"""Chainlink integration manifest."""

from almanak.integrations._base import ImportRef, Integration

INTEGRATION = Integration(
    name="chainlink",
    gateway_price_source=ImportRef(
        module="almanak.integrations.chainlink.gateway.factory",
        attribute="ChainlinkPriceSourceFactory",
        order=10,
    ),
    price_source_blocked_by_groups=frozenset({"venue_oracle"}),
    gateway_oracle_reader=ImportRef(
        module="almanak.integrations.chainlink.gateway.factory",
        attribute="ChainlinkOracleReaderFactory",
    ),
)
