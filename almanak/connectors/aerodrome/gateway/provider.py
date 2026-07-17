"""Gateway-side connector binding for Aerodrome.

Phase 3 (VIB-4811) introduces capability-keyed dispatch at the gateway
boundary. Aerodrome contributes:

* ``GatewayPoolHistoryCapability`` — pool history is supported on Base
  only. Previously this was the single ``("base", "aerodrome")`` entry
  in ``almanak.gateway.services.pool_history_service.SUPPORTED_POOL_PAIRS``.
* ``GatewayDefillamaSlugCapability`` — DefiLlama project slug
  (``"aerodrome-v1"`` plus the ``"aerodrome_slipstream"`` alias).
* ``GatewayPriceIdCapability`` — AERO CoinGecko slug
  (``aerodrome-finance``).

W7-followup (VIB-4870) adds:

* ``GatewayDexVolumeCapability`` — daily trading-volume history. The
  configured subgraph is the Aerodrome **Slipstream** (concentrated-liquidity)
  deployment, which uses the Uniswap-V3-style ``poolDayDatas`` / ``pool`` /
  ``volumeUSD`` schema (NOT the Solidly ``pairDayDatas`` shape). Egress runs on
  the ``RateHistoryService`` servicer's shared HTTP session.
* ``GatewayDexTwapCapability`` — single-observation TWAP via the pool's
  ``observe()`` oracle. Aerodrome **V2** (Solidly volatile/stable pairs) has
  no ``observe()``, but **Slipstream** (the concentrated-liquidity product) is
  a Uniswap-V3 fork that does, so this capability is scoped to Slipstream and
  reuses the shared V3 ``observe()`` helper.

This connector's ``dex_name()`` is the legacy ``"aerodrome"`` key; it also
declares ``dex_aliases()`` so RateHistoryService routes the canonical
``"aerodrome_slipstream"`` slug (used by strategies / the executor) here too.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDefillamaSlugCapability,
    GatewayDexTwapCapability,
    GatewayDexVolumeCapability,
    GatewayPoolHistoryCapability,
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._base.v3_gateway_twap import fetch_v3_twap_observation

from ..addresses import AERODROME

# W7-followup / VIB-4870 — Aerodrome daily-volume subgraph IDs.
# Migrated verbatim from
# ``framework/backtesting/pnl/providers/dex/aerodrome_volume.py``.
# Base-native; the configured subgraph is the Slipstream (CL) deployment →
# Uniswap-V3-style ``poolDayDatas`` schema (not Solidly ``pairDayDatas``).
_AERODROME_VOLUME_SUBGRAPH_IDS: dict[str, str] = {
    "base": "GENunSHWLBXm59mBSgPzQ8metBEp9YDfdqwFr91Av1UM",
}


class AerodromeGatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayPoolHistoryCapability,
    GatewayDefillamaSlugCapability,
    GatewayPriceIdCapability,
    GatewayDexVolumeCapability,
    GatewayDexTwapCapability,
):
    """Gateway-side connector for Aerodrome."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aerodrome")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Aerodrome contract addresses for ``chain`` (or empty)."""
        return AERODROME.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Aerodrome addresses are registered."""
        return frozenset(AERODROME.keys())

    def pool_history_supported_chains(self) -> frozenset[str]:
        """Aerodrome lives on Base only."""
        return frozenset({"base"})

    def defillama_slug(self) -> str | None:
        """DefiLlama project slug for Aerodrome classic (Solidly vAMM/sAMM).

        The yields catalog tracks classic Aerodrome pools under project
        ``"aerodrome-v1"``. The previously declared ``"aerodrome-v2"`` exists
        in no DefiLlama namespace (yields projects are ``aerodrome-v1`` /
        ``aerodrome-slipstream``; the protocols API 404s on it), so every
        project-filtered pool match was an always-miss.
        """
        return "aerodrome-v1"

    def defillama_slug_aliases(self) -> dict[str, str]:
        """Aerodrome's Slipstream concentrated-liquidity product rides the
        same connector but DefiLlama tracks it as a separate project.

        Returning the alias here keeps the dispatcher single-pass: the
        pool-analytics matcher resolves ``protocol="aerodrome_slipstream"``
        to ``"aerodrome-slipstream"`` without a second dispatch table.
        """
        return {"aerodrome_slipstream": "aerodrome-slipstream"}

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Aerodrome governance token."""
        return {"AERO": "aerodrome-finance"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """AERO is an EVM-only token resolved via ``TokenResolver``."""
        return {}

    # ---------------------------------------------------------------------
    # GatewayDexVolumeCapability (VIB-4870 / W7-followup)
    # ---------------------------------------------------------------------

    def dex_name(self) -> str:
        """DEX identifier — matches the legacy ``"aerodrome"`` provider key."""
        return "aerodrome"

    def volume_supported_chains(self) -> frozenset[str]:
        """Chains with a registered Aerodrome volume subgraph (Base only)."""
        return frozenset(_AERODROME_VOLUME_SUBGRAPH_IDS)

    async def fetch_volume_history(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
    ) -> Any:
        """Daily trading-volume history via the Aerodrome Slipstream subgraph.

        The configured subgraph (``_AERODROME_VOLUME_SUBGRAPH_IDS``) is the
        Slipstream concentrated-liquidity deployment, which exposes the
        Uniswap-V3-style ``poolDayDatas`` entity filtered by ``pool`` with the
        ``volumeUSD`` field — the same shape uniswap_v3 / pancakeswap_v3 use.
        (The older Solidly ``pairDayDatas`` / ``pairAddress`` / ``dailyVolumeUSD``
        spec did not exist on this subgraph and failed with a GraphQL schema
        error for every pool.)
        """
        from almanak.gateway.services._dex_volume_subgraph import (
            DexVolumeSubgraphSpec,
            fetch_dex_volume_history,
        )

        return await fetch_dex_volume_history(
            servicer,
            DexVolumeSubgraphSpec(
                dex_name="aerodrome",
                subgraph_ids=dict(_AERODROME_VOLUME_SUBGRAPH_IDS),
                entity="poolDayDatas",
                id_field="pool",
                volume_field="volumeUSD",
                source="aerodrome_subgraph",
            ),
            chain=chain,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
        )

    def dex_aliases(self) -> tuple[str, ...]:
        """Canonical slugs (besides ``dex_name()``) this connector answers to.

        The Slipstream concentrated-liquidity product is addressed as
        ``aerodrome_slipstream`` by strategies and the executor, while this
        connector's ``dex_name()`` is the legacy ``aerodrome`` key. Declaring
        the alias lets ``RateHistoryService`` route TWAP / volume requests for
        either name to this connector.
        """
        return ("aerodrome_slipstream",)

    # ---------------------------------------------------------------------
    # GatewayDexTwapCapability (Slipstream only)
    #
    # Aerodrome **Slipstream** pools are a Uniswap-V3 fork and expose the
    # standard ``observe()`` oracle, so TWAP rides the shared V3 helper.
    # Aerodrome **V2** (Solidly pairs) has no ``observe()`` — but those are
    # not concentrated-liquidity pools and are not resolved through this path.
    # ---------------------------------------------------------------------

    def twap_supported_chains(self) -> frozenset[str]:
        """Aerodrome Slipstream is deployed on Base only."""
        return frozenset({"base"})

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
        """Single TWAP observation via the pool's ``observe(secondsAgos)`` oracle.

        Slipstream shares the Uniswap-V3 ``observe()`` ABI, so this delegates to
        the shared V3 helper; ``protocol`` is used only for error attribution.
        """
        return await fetch_v3_twap_observation(
            servicer,
            chain=chain,
            pool_address=pool_address,
            secs_ago_start=secs_ago_start,
            secs_ago_end=secs_ago_end,
            as_of_block=as_of_block,
            protocol="aerodrome_slipstream",
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
    ) -> Any:
        """TWAP series — not yet implemented (mirrors the Uniswap V3 lane).

        The block-by-block bisect for ``interval_secs`` sampling lands with the
        W7 step-3 DEX TWAP cluster; until then the series lane raises.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "aerodrome_slipstream",
            "DEX TWAP series fan-out lands in W7 step 3 (DEX cluster)",
        )


__all__ = ["AerodromeGatewayConnector"]
