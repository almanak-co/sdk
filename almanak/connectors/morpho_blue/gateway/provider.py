"""Gateway-side connector binding for Morpho Blue (VIB-4853 / W1).

Minimal Phase-3-scaffold-style binding so Morpho Blue can publish its
on-chain contract addresses through :class:`GatewayAddressCapability`
without forcing every consumer to import the connector by name. The
strategy-side connector code (adapter, compiler, flash-loan provider,
SDK) still lives under ``almanak/connectors/morpho_blue/``; this module
contributes the gateway-side address surface only.

Contributes:

* ``GatewayAddressCapability`` — per-chain Morpho Blue Morpho + Bundler
  addresses (per-chain because Arbitrum / Polygon / Monad each deployed
  at a distinct address that differs from the universal vanity address).
  Moved verbatim from the entries previously held in
  ``almanak.core.contracts``.

W7 (VIB-4859) adds:

* ``GatewayLendingRateHistoryCapability`` — Morpho Blue has no on-chain
  live rate query in the pre-W7 strategy code (the
  ``_fetch_morpho_rate`` body in ``framework/data/rates/monitor.py``
  returned hard-coded placeholder constants). The capability is declared
  so the registry dispatcher routes Morpho lending requests through this
  connector; the live path raises ``RateHistoryUnavailable`` until a real
  on-chain integration ships (tracked in VIB-5040). The framework client
  surfaces the failure envelope and falls back to its placeholder layer
  for parity with pre-W7 behaviour.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayLendingRateHistoryCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import MORPHO_BLUE


class MorphoBlueGatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayLendingRateHistoryCapability,
):
    """Gateway-side connector for Morpho Blue."""

    protocol: ClassVar[ProtocolName] = ProtocolName("morpho_blue")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Morpho Blue contract addresses for ``chain`` (or empty)."""
        return MORPHO_BLUE.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Morpho Blue addresses are registered."""
        return frozenset(MORPHO_BLUE.keys())

    # The CLI support matrix consumes Morpho Blue's matrix surface via
    # ``ConnectorManifest.matrix_entries`` on the strategy side
    # (see ``almanak/connectors/morpho_blue/__init__.py``).

    # ---------------------------------------------------------------------
    # GatewayLendingRateHistoryCapability (VIB-4859 / W7)
    # ---------------------------------------------------------------------

    def lending_supported_chains(self) -> frozenset[str]:
        """Chains where Morpho Blue lending rates are surfaceable.

        Currently equal to the address registry — when an on-chain live
        rate fetcher lands (VIB-5040), it will use the
        ``MorphoBlue`` market-state contract on these chains.
        """
        return frozenset(MORPHO_BLUE.keys())

    async def fetch_lending_current(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
    ) -> Any:
        """Live Morpho Blue rate.

        Pre-W7 ``framework/data/rates/monitor.py:_fetch_morpho_rate`` returned
        hardcoded placeholder constants — there is no on-chain rate fetch
        implemented yet. The framework client side preserves the placeholder
        layer for parity; this capability raises so the dispatcher returns
        a typed ``success=False`` envelope rather than fabricating data.
        Tracked for real on-chain wiring in VIB-5040.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "morpho_blue",
            "Morpho Blue on-chain live rate not implemented; framework client falls back to placeholder. Tracked in VIB-5040.",
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

        Morpho Blue historical APY series is sourced from the dedicated
        ``MorphoBlueAPYProvider`` (see ``framework/backtesting/pnl/providers/
        lending/morpho_apy.py``) which continues to consume TheGraph subgraph
        data through the shared ``SubgraphClient``. Surface lands in W7 step 4
        once the consumer rewrite wires through the gRPC service.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "morpho_blue",
            "lending-history surface lands once the framework consumer rewrite ships (W7 step 4)",
        )


__all__ = ["MorphoBlueGatewayConnector"]
