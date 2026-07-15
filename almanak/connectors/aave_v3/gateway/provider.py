"""Gateway-side connector binding for Aave v3.

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Aave v3 receipt-token (aToken / vToken) lookup without
hand-wiring an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 (VIB-4810) — the capability is declared but ``token_service``
continues to call ``get_aave_lookup`` directly. Phase 4 collapses the
explicit per-protocol accessor methods on ``TokenService`` into a loop
over ``GATEWAY_REGISTRY.capability_providers(GatewayMarketLookupCapability)``.

Phase 3 (VIB-4811) adds:

* ``GatewayDefillamaSlugCapability`` — DefiLlama project slug
  (``"aave-v3"``).
* ``GatewaySubgraphCapability`` — TheGraph subgraph URLs (Ethereum,
  Arbitrum, Optimism, Polygon). Moved verbatim from
  ``almanak.gateway.integrations.thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
* ``GatewayPriceIdCapability`` — Aave governance token CoinGecko slug
  (``AAVE`` → ``aave``). Moved verbatim from
  ``almanak.gateway.data.price.coingecko``'s per-chain token-id tables.

W1 (VIB-4853) adds:

* ``GatewayAddressCapability`` — per-chain Pool + PoolDataProvider +
  AaveOracle addresses, moved verbatim from
  ``almanak.core.contracts``. Non-connector callers (teardown
  discovery, valuation, rate monitor, ContractRegistry, CLI support
  matrix) resolve Aave addresses through this capability instead of
  importing the dict by name.

W7 (VIB-4859) adds:

* ``GatewayLendingRateHistoryCapability`` — live + historical supply /
  borrow APY + utilisation. The live path migrated the
  ``_fetch_aave_v3_rate_onchain`` body that used to live strategy-side
  in ``framework/data/rates/monitor.py`` (and opened its own
  ``httpx.AsyncClient``); the egress now happens through the
  ``RateHistoryService`` servicer's shared HTTP session, which is the
  correct layer for outbound network traffic. The fork-shared
  ``getReserveData`` pipeline now lives in
  :mod:`almanak.connectors._base.aave_fork_gateway_rates` so Aave V3
  forks (Spark) reuse it instead of copy-pasting the rate math.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, ClassVar

from almanak.connectors._base.aave_fork_gateway_rates import fetch_aave_fork_lending_current
from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDefillamaSlugCapability,
    GatewayLendingRateHistoryCapability,
    GatewayMarketLookupCapability,
    GatewayPriceIdCapability,
    GatewaySubgraphCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import AAVE_V3, AAVE_V3_TOKENS
from .market_lookup import get_aave_lookup

logger = logging.getLogger(__name__)

# Aave v3 subgraph URLs. Moved verbatim from
# ``thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
_AAVE_V3_SUBGRAPHS: dict[str, str] = {
    "aave-v3-ethereum": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3",
    "aave-v3-arbitrum": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum",
    "aave-v3-optimism": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-optimism",
    "aave-v3-polygon": "https://api.thegraph.com/subgraphs/name/aave/protocol-v3-polygon",
}


class AaveV3GatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayLendingRateHistoryCapability,
    GatewayMarketLookupCapability,
    GatewayDefillamaSlugCapability,
    GatewaySubgraphCapability,
    GatewayPriceIdCapability,
):
    """Gateway-side connector for Aave v3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aave_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Aave v3 contract addresses for ``chain`` (or empty)."""
        return AAVE_V3.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Aave v3 addresses are registered."""
        return frozenset(AAVE_V3.keys())

    # The CLI support matrix consumes connector-level matrix data through
    # ``ConnectorManifest.matrix_entries`` on the strategy side
    # (see ``almanak/connectors/aave_v3/__init__.py``); declaring a
    # parallel gateway capability would duplicate the source of truth.

    def market_lookup(self):
        """Return the awaitable Aave market-lookup singleton factory.

        The underlying ``get_aave_lookup`` is a coroutine factory that
        returns a lazily-loaded singleton with disk-cache + retry
        plumbing (see ``ProtocolTokenLookup``). Phase 4 will swap this
        for an ``async`` capability contract; for Phase 1+2 the provider
        method just returns the callable so the capability registration
        is visible without coupling to the lookup's async lifecycle.
        """
        return get_aave_lookup

    def defillama_slug(self) -> str | None:
        """DefiLlama project slug for Aave v3."""
        return "aave-v3"

    def defillama_slug_aliases(self) -> dict[str, str]:
        return {}

    def subgraph_endpoints(self) -> dict[str, str]:
        """TheGraph subgraph URLs for Aave v3 (one per supported chain)."""
        return dict(_AAVE_V3_SUBGRAPHS)

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Aave governance token."""
        return {"AAVE": "aave"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """Aave token addresses are resolved via ``TokenResolver`` on EVM chains."""
        return {}

    # ---------------------------------------------------------------------
    # GatewayLendingRateHistoryCapability (VIB-4859 / W7)
    # ---------------------------------------------------------------------

    def lending_supported_chains(self) -> frozenset[str]:
        """Chains where Aave v3 lending rates are queryable.

        Equal to the chains the connector ships addresses for — anywhere
        we have a ``PoolDataProvider`` address we can do the on-chain
        ``getReserveData`` call.
        """
        return frozenset(AAVE_V3.keys())

    async def fetch_lending_current(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
        market_id: str | None = None,  # noqa: ARG002 — whole-account venue: see below
    ) -> Any:
        """Fetch live Aave v3 supply / borrow / utilisation via on-chain
        ``eth_call`` to ``AaveProtocolDataProvider.getReserveData(asset)``.

        Delegates to the fork-shared pipeline in
        :mod:`almanak.connectors._base.aave_fork_gateway_rates` (the ABI is
        identical across the Aave V3 fork family). ``servicer`` is the
        gateway-side ``RateHistoryServiceServicer`` — the shared body reads
        its shared aiohttp session + settings.

        ``market_id`` is accepted-and-ignored (VIB-5729): Aave V3 is a
        whole-account venue with one pool per chain, so a reserve's rate is fully
        identified by ``asset_symbol``. Ignoring it is safe BECAUSE the returned
        point leaves ``market_id`` unset, so a market-scoped caller sees no echo
        and falls closed to unmeasured rather than trusting this rate.
        """
        return await fetch_aave_fork_lending_current(
            servicer,
            protocol="aave_v3",
            display_name="Aave",
            contracts_by_chain=AAVE_V3,
            tokens_by_chain=AAVE_V3_TOKENS,
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

        Migration of ``framework/data/rates/history.py``'s TheGraph crawl
        + ``backtesting/pnl/providers/lending/aave_v3_apy.py`` arrives in
        Step 3 (lending cluster) of the W7 plan. For Step 2 (this PR),
        the historical lane raises ``RateHistoryUnavailable`` so the
        dispatcher surfaces a clean ``success=False`` envelope rather
        than fabricating data.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "aave_v3",
            "lending-history fan-out lands in W7 step 3 (lending cluster); see plan PR #2473 §5.3",
        )


__all__ = ["AaveV3GatewayConnector"]
