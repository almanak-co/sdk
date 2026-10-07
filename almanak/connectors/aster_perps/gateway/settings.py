"""Aster Pro gateway-side configuration fragment.

Composed into ``GatewaySettings`` through the connector manifest's
``gateway_settings`` declaration, so the env var is
``ALMANAK_GATEWAY_ASTER_PERPS_BASE_URL`` (and
``ALMANAK_GATEWAY_ASTER_PERPS_WITHDRAW_IP_WHITELIST`` to enable withdrawals). The trading identity is the gateway's
own EOA (``private_key``); no Aster-specific secret exists because the agent key
is generated in memory and approved by that EOA.

Strategy-side code MUST NOT import this module.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator

from almanak.connectors.aster_perps.gateway.api_client import DEFAULT_BASE_URL


class AsterPerpsGatewaySettings(BaseModel):
    """Aster Pro gateway-side fields."""

    aster_perps_base_url: str = DEFAULT_BASE_URL
    # Space-separated public egress IP(s) of this gateway. Aster binds a
    # withdraw-capable agent to an IP whitelist; withdrawals are refused when unset.
    aster_perps_withdraw_ip_whitelist: str | None = None

    @field_validator("aster_perps_base_url")
    @classmethod
    def _require_https(cls, value: str) -> str:
        if not value.startswith("https://"):
            raise ValueError(f"aster_perps_base_url must be an https URL (got {value!r})")
        return value.rstrip("/")


__all__ = ["AsterPerpsGatewaySettings"]
