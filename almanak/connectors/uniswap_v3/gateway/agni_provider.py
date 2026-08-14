"""Gateway-side connector binding for Agni Finance (VIB-4853 / W1).

Agni Finance is a Uniswap V3 fork on Mantle. There is no separate
``almanak/connectors/agni_finance/`` folder — Agni reuses the
Uniswap V3 connector's adapter / receipt parser, and the addresses sit
alongside Uniswap V3's in ``uniswap_v3/addresses.py``. This minimal
scaffold lets Agni be registered as its own protocol with the gateway
registry so non-connector callers can resolve its addresses through
:class:`GatewayAddressCapability` without importing the dict by name.

Contributes:

* ``GatewayAddressCapability`` — Agni's Mantle addresses, surfaced under
  ``ProtocolName("agni_finance")``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDexPoolStateCapability,
    GatewayDexTwapCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import AGNI_FINANCE


class AgniFinanceGatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayDexTwapCapability,
    GatewayDexPoolStateCapability,
):
    """Gateway-side connector for Agni Finance (Uniswap V3 fork on Mantle)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("agni_finance")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Agni Finance contract addresses for ``chain`` (or empty)."""
        return AGNI_FINANCE.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Agni Finance addresses are registered."""
        return frozenset(AGNI_FINANCE.keys())

    def dex_name(self) -> str:
        return "agni_finance"

    def twap_supported_chains(self) -> frozenset[str]:
        return frozenset(AGNI_FINANCE.keys())

    async def fetch_twap(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        secs_ago_start: int,
        secs_ago_end: int,
        as_of_block: int | None = None,
    ) -> Any:
        from almanak.connectors._base.v3_gateway_twap import fetch_v3_twap_observation

        return await fetch_v3_twap_observation(
            servicer,
            chain=chain,
            pool_address=pool_address,
            secs_ago_start=secs_ago_start,
            secs_ago_end=secs_ago_end,
            as_of_block=as_of_block,
            protocol="agni_finance",
        )

    async def fetch_twap_series(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
        window_secs: int,
    ) -> Any:
        from almanak.connectors._base.v3_gateway_twap import fetch_v3_twap_series

        return await fetch_v3_twap_series(
            servicer,
            chain=chain,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
            window_secs=window_secs,
            protocol="agni_finance",
        )

    def pool_state_supported_chains(self) -> frozenset[str]:
        return frozenset(AGNI_FINANCE.keys())

    async def fetch_pool_state_series(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
    ) -> Any:
        from almanak.connectors._base.v3_gateway_twap import fetch_v3_pool_state_series
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        deployment = AGNI_FINANCE.get(chain.strip().lower())
        factory_address = deployment.get("factory") if deployment is not None else None
        if not isinstance(factory_address, str) or not factory_address.strip():
            raise RateHistoryUnavailable(
                "agni_finance",
                f"no authenticated Agni Finance factory configured for chain {chain!r}",
            )
        return await fetch_v3_pool_state_series(
            servicer,
            chain=chain,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
            protocol="agni_finance",
            factory_address=factory_address,
        )


__all__ = ["AgniFinanceGatewayConnector"]
