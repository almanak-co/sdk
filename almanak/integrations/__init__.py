"""Provider integrations for market data and external APIs.

The package root is intentionally import-light.  Provider metadata lives in
each provider package while gateway-only network implementations live below
that provider's ``gateway`` package.
"""

from ._base import (
    INTEGRATION_REGISTRY,
    GatewayPriceSourceFactory,
    ImportRef,
    Integration,
    IntegrationRegistry,
    PriceSourceScope,
)

__all__ = [
    "INTEGRATION_REGISTRY",
    "GatewayPriceSourceFactory",
    "ImportRef",
    "Integration",
    "IntegrationRegistry",
    "PriceSourceScope",
]
