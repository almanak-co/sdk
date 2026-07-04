"""Strategy-side agent-read provider for Compound V3 (VIB-4951).

Compound V3 is comet-based, not reserve-based: each market (comet) has ONE
borrowable base asset plus a set of collateral assets with per-asset
collateral factors. The reserve-discovery plan maps that explicitly onto the
tool's reserve-row vocabulary (the VIB-4951 acceptance-criterion-4 mapping):

* base asset row — ``borrowing_enabled=True``, ``usage_as_collateral_enabled=False``
  (a comet's base asset cannot be posted as collateral), risk factors ``None``
  (not applicable), ``extra={"market", "role": "base"}``.
* collateral row — ``borrowing_enabled=False`` (collateral cannot be borrowed),
  ``usage_as_collateral_enabled=True``, ``ltv_bps`` from the LIVE
  ``getAssetInfoByAddress`` ``borrowCollateralFactor`` and
  ``liquidation_threshold_bps`` from ``liquidateCollateralFactor`` (both
  uint64 1e18-scaled -> bps). Live reads, not the static table factors, so a
  governance change can never serve stale risk params.

Markets are enumerated from the DISTINCT comet addresses in
``COMPOUND_V3_COMET_ADDRESSES`` (the universe the adapter/compiler accepts);
``COMPOUND_V3_MARKETS`` supplies optional metadata. The per-collateral factors are
live ``eth_call``s executed by the framework. ``is_frozen`` is ``None``
(unmeasured — comet pause states are not read here; Empty != Zero).
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.agent_read_registry import (
    AgentReadCapability,
    AgentReadConnector,
)
from almanak.connectors._strategy_base.lending_reserve_read import (
    LendingReserveDiscoveryPlan,
    ReserveCall,
    ReserveConfigRow,
    ReserveEntry,
)

_CF_WORD_BORROW = 4  # AssetInfo.borrowCollateralFactor (uint64, 1e18-scaled)
_CF_WORD_LIQUIDATE = 5  # AssetInfo.liquidateCollateralFactor (uint64, 1e18-scaled)


def _decode_asset_info_factors(raw_hex: str) -> ReserveConfigRow:
    """Decode getAssetInfoByAddress -> collateral-row risk factors (1e18 -> bps)."""
    raw = raw_hex[2:] if raw_hex[:2].lower() == "0x" else raw_hex
    if len(raw) < 8 * 64:
        raise ValueError("getAssetInfoByAddress blob shorter than the 8-word AssetInfo struct")
    borrow_cf = int(raw[_CF_WORD_BORROW * 64 : (_CF_WORD_BORROW + 1) * 64], 16)
    liquidate_cf = int(raw[_CF_WORD_LIQUIDATE * 64 : (_CF_WORD_LIQUIDATE + 1) * 64], 16)
    return ReserveConfigRow(
        borrowing_enabled=False,
        usage_as_collateral_enabled=True,
        is_active=True,
        is_frozen=None,
        ltv_bps=borrow_cf // 10**14,
        liquidation_threshold_bps=liquidate_cf // 10**14,
    )


class CompoundV3AgentReadConnector(AgentReadConnector, AgentReadCapability):
    """Agent-read descriptors for Compound V3 comet markets."""

    protocol: ClassVar[ProtocolName] = ProtocolName("compound_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def agent_read_keys(self) -> frozenset[str]:
        return frozenset({"lending_reserves"})

    def factory_address(self, chain: str) -> str | None:
        return None

    def position_manager_address(self, chain: str) -> str | None:
        return None

    def get_pool_selector(self) -> str:
        return "0x1698ee82"  # not a CL DEX; descriptor totality only

    def lending_pool_address(self, chain: str) -> str | None:
        return None  # comet-per-market; no single Pool analogue

    def lending_reserve_discovery_plan(self, chain: str) -> LendingReserveDiscoveryPlan | None:
        from almanak.connectors._strategy_base.lending_read_base import (
            build_compound_asset_info_calldata,
        )
        from almanak.connectors.compound_v3.addresses import (
            COMPOUND_V3_COMET_ADDRESSES,
            COMPOUND_V3_MARKETS,
        )

        markets = COMPOUND_V3_MARKETS.get(chain, {})
        comets = COMPOUND_V3_COMET_ADDRESSES.get(chain, {})
        if not comets:
            return None

        # Enumerate DISTINCT comets from the address table — the actual market
        # universe the adapter/compiler accepts — never from the curated
        # metadata table alone (codex review, PR #3197: metadata-only
        # enumeration omitted valid comets and duplicated aliased ids that
        # point at one comet, e.g. polygon usdc_e/usdc_bridged).
        by_comet: dict[str, list[str]] = {}
        for market_id, comet in comets.items():
            by_comet.setdefault(comet.lower(), []).append(market_id)

        entries: list[ReserveEntry] = []
        for comet_key, market_ids in by_comet.items():
            # Prefer the id the metadata table catalogues (aliases carry none).
            market_id = next((m for m in market_ids if m in markets), market_ids[0])
            market = markets.get(market_id)
            if market:
                base_symbol = market["base_token"]
                base_address = market["base_token_address"].lower()
                base_extra = {"market": market_id, "role": "base", "comet": comet_key}
            else:
                # Comet ids are base-token tickers by table convention
                # (usdc / weth / wsteth / usds / usdbc ...). Surface the
                # borrowable market rather than silently omitting it; the
                # base-token address is not catalogued statically.
                base_symbol = market_id.upper()
                base_address = ""
                base_extra = {"market": market_id, "role": "base", "comet": comet_key, "metadata": "uncatalogued"}
            entries.append(
                ReserveEntry(
                    symbol=base_symbol,
                    address=base_address,
                    static_config=ReserveConfigRow(
                        borrowing_enabled=True,
                        usage_as_collateral_enabled=False,
                        is_active=True,
                        is_frozen=None,
                        ltv_bps=None,
                        liquidation_threshold_bps=None,
                        extra=base_extra,
                    ),
                )
            )
            for symbol, collateral in (market or {}).get("collaterals", {}).items():
                address = collateral["address"]
                entries.append(
                    ReserveEntry(
                        symbol=symbol,
                        address=address.lower(),
                        config_call=ReserveCall(
                            to=comet_key,
                            data=build_compound_asset_info_calldata(address),
                            id=f"compound_asset_info:{market_id}:{symbol}",
                        ),
                    )
                )

        if not entries:
            return None
        return LendingReserveDiscoveryPlan(
            protocol="compound_v3",
            provider_address="",
            static_entries=tuple(entries),
            decode_config=_decode_asset_info_factors,
        )


__all__ = ["CompoundV3AgentReadConnector"]
