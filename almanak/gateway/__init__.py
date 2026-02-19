"""Almanak Gateway - Secure bridge between strategy containers and platform services.

The gateway serves two purposes:
1. FastAPI server for external HTTP API access (docs, monitoring)
2. gRPC server for internal strategy-gateway communication (secure, efficient)

Strategy containers connect only to the gRPC interface and have no direct
access to platform secrets or external services.
"""

from almanak.gateway.core.settings import GatewaySettings, get_settings

__all__ = ["GatewaySettings", "get_settings"]
