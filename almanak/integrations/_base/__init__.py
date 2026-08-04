"""Provider-neutral integration descriptors and capabilities."""

from .capabilities import (
    GatewayApiClientFactory,
    GatewayOraclePricePage,
    GatewayOraclePricePoint,
    GatewayOracleReader,
    GatewayOracleReaderFactory,
    GatewayPortfolioProviderFactory,
    GatewayPriceSourceFactory,
    OracleDataUnavailable,
    PriceSourceScope,
)
from .descriptor import ImportRef, Integration
from .registry import INTEGRATION_REGISTRY, IntegrationRegistry

__all__ = [
    "INTEGRATION_REGISTRY",
    "GatewayPriceSourceFactory",
    "GatewayOraclePricePage",
    "GatewayOraclePricePoint",
    "GatewayOracleReader",
    "GatewayOracleReaderFactory",
    "GatewayApiClientFactory",
    "GatewayPortfolioProviderFactory",
    "OracleDataUnavailable",
    "ImportRef",
    "Integration",
    "IntegrationRegistry",
    "PriceSourceScope",
]
