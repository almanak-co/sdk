"""Price data providers.

This package contains the gateway-backed price oracle.
Direct price providers have been moved to the gateway.

Available Providers:
    - GatewayPriceOracle: Price data through the gateway sidecar
"""

from .gateway_oracle import GatewayPriceOracle

__all__ = [
    "GatewayPriceOracle",
]
