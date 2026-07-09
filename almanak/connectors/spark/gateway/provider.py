"""Gateway-side connector binding for Spark.

Spark is an Aave V3 fork: it exposes the identical
``PoolDataProvider.getReserveData(address asset)`` ABI against its own
``pool_data_provider`` contract (see ``almanak/connectors/spark/lending_read.py``,
which reuses the shared ``AAVE_FORK_*`` read specs the same way). This module
contributes the gateway-side rate surface:

* ``GatewayLendingRateHistoryCapability`` — live supply / borrow APY +
  utilisation via the fork-shared ``getReserveData`` pipeline in
  :mod:`almanak.connectors._base.aave_fork_gateway_rates`. Without this
  capability a rates-driven Spark strategy could not call
  ``MarketSnapshot.lending_rate("spark", ...)`` at all. Egress happens
  through the ``RateHistoryService`` servicer's
  shared aiohttp session — the strategy container never makes this call.

Spark ships no curated per-chain token table (unlike ``AAVE_V3_TOKENS``);
asset symbols resolve through the global ``TokenResolver`` fallback inside
the shared pipeline. Spark reserves are keyed by the canonical underlying
token address, so the resolver's answer is the right ``getReserveData``
argument.
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from almanak.connectors._base.aave_fork_gateway_rates import fetch_aave_fork_lending_current
from almanak.connectors._base.gateway_capabilities import GatewayLendingRateHistoryCapability
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import SPARK

logger = logging.getLogger(__name__)


class SparkGatewayConnector(
    GatewayConnector,
    GatewayLendingRateHistoryCapability,
):
    """Gateway-side connector for Spark."""

    protocol: ClassVar[ProtocolName] = ProtocolName("spark")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    # ---------------------------------------------------------------------
    # GatewayLendingRateHistoryCapability
    # ---------------------------------------------------------------------

    def lending_supported_chains(self) -> frozenset[str]:
        """Chains where Spark lending rates are queryable.

        Equal to the chains the connector ships addresses for — anywhere
        we have a ``pool_data_provider`` address we can do the on-chain
        ``getReserveData`` call.
        """
        return frozenset(SPARK.keys())

    async def fetch_lending_current(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
    ) -> Any:
        """Fetch live Spark supply / borrow / utilisation via on-chain
        ``eth_call`` to ``PoolDataProvider.getReserveData(asset)``.

        Delegates to the fork-shared pipeline (identical Aave V3 ABI);
        only the ``pool_data_provider`` address differs from Aave's.
        """
        return await fetch_aave_fork_lending_current(
            servicer,
            protocol="spark",
            display_name="Spark",
            contracts_by_chain=SPARK,
            tokens_by_chain={},
            chain=chain,
            asset_symbol=asset_symbol,
            side=side,
        )

    async def fetch_lending_history(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
        start_ts: int,
        end_ts: int,
    ) -> Any:
        """Historical lending series.

        Spark's historical lane follows the Aave V3 fan-out (W7 step 3,
        lending cluster) — until that ships, raise so the dispatcher
        surfaces a clean ``success=False`` envelope rather than
        fabricating data.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "spark",
            "lending-history fan-out lands with the W7 step 3 lending cluster (same lane as aave_v3)",
        )


__all__ = ["SparkGatewayConnector"]
