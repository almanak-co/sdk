"""Enso gateway-side configuration fragment (VIB-4812).

Composed into ``GatewaySettings`` via multi-inheritance — see the
:mod:`almanak.connectors.polymarket.gateway.settings` docstring for the
rationale. The env-var surface is preserved byte-identically:

    ALMANAK_GATEWAY_ENSO_API_KEY

The bare-name fallback (``ENSO_API_KEY``) continues to live at the
service boundary in ``almanak/config/env.py``.

Strategy-side code MUST NOT import this module.
"""

from __future__ import annotations

from pydantic import BaseModel


class EnsoGatewaySettings(BaseModel):
    """Enso gateway-side fields. Inherited by ``GatewaySettings``."""

    # Enso routing API credential. Optional — when absent, the Enso
    # servicer returns UNAVAILABLE and strategies fall back to direct
    # protocol-specific connectors.
    enso_api_key: str | None = None


__all__ = ["EnsoGatewaySettings"]
