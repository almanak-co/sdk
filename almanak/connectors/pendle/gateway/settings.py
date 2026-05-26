"""Pendle gateway-side configuration fragment (VIB-4812).

Composed into ``GatewaySettings`` via multi-inheritance — see the
:mod:`almanak.connectors.polymarket.gateway.settings` docstring for the
rationale. The env-var surface is preserved byte-identically:

    ALMANAK_GATEWAY_PENDLE_API_KEY
    ALMANAK_GATEWAY_PENDLE_API_CACHE_TTL

Strategy-side code MUST NOT import this module.
"""

from __future__ import annotations

from pydantic import BaseModel


class PendleGatewaySettings(BaseModel):
    """Pendle gateway-side fields. Inherited by ``GatewaySettings``."""

    # Pendle API credential. Optional — used by the market-lookup
    # resolver to enrich PT / YT / LP token metadata.
    pendle_api_key: str | None = None

    # Cache TTL (seconds) for the Pendle market-lookup HTTP cache.
    pendle_api_cache_ttl: float = 15.0


__all__ = ["PendleGatewaySettings"]
