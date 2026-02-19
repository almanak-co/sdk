"""Web3 utilities and gateway-backed providers.

This package provides web3.py providers that route through the gateway:
- GatewayWeb3Provider: Routes JSON-RPC calls through gateway for secure access
"""

from almanak.framework.web3.gateway_provider import (
    GatewayWeb3Provider,
    get_gateway_web3,
)

__all__ = [
    "GatewayWeb3Provider",
    "get_gateway_web3",
]
