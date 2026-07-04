"""Strategy-side agent-read provider for Morpho Blue (VIB-4951).

Morpho Blue is market-keyed, not reserve-keyed: each immutable market is a
(collateral token, loan token, oracle, IRM, LLTV) tuple. The reserve-discovery
plan lists one row per catalogued market, mapped onto the tool's reserve-row
vocabulary:

* ``symbol`` — the market name (``"wstETH/USDC"``: collateral/loan);
  ``address`` — the collateral token address.
* ``borrowing_enabled=True`` (the loan side is borrowable by construction),
  ``usage_as_collateral_enabled=True`` (the collateral side is collateral by
  construction), ``is_active=True`` / ``is_frozen=False`` — Morpho markets
  are immutable and permissionless; there is no pause/freeze mechanism.
* ``ltv_bps`` and ``liquidation_threshold_bps`` — both the market LLTV
  (1e18-scaled -> bps). Morpho has a single liquidation LTV, no separate
  max-borrow LTV; ``extra`` carries the raw ``market_id`` + token detail so
  callers can disambiguate.

Fully static — the plan performs zero RPC (the catalogue and LLTV are
compile-time constants of the market id). ``provider_address`` is the Morpho
singleton for the chain.
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
    ReserveConfigRow,
    ReserveEntry,
)


class MorphoBlueAgentReadConnector(AgentReadConnector, AgentReadCapability):
    """Agent-read descriptors for Morpho Blue markets."""

    protocol: ClassVar[ProtocolName] = ProtocolName("morpho_blue")
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
        return None  # market-keyed singleton; getUserAccountData does not apply

    def lending_reserve_discovery_plan(self, chain: str) -> LendingReserveDiscoveryPlan | None:
        from almanak.connectors._strategy_base.address_registry import AddressRegistry
        from almanak.connectors.morpho_blue.addresses import MORPHO_MARKETS

        markets = MORPHO_MARKETS.get(chain)
        singleton = AddressRegistry.resolve_contract_address("morpho_blue", chain, "morpho")
        if not markets or not singleton:
            return None

        entries: list[ReserveEntry] = []
        for market_id, market in markets.items():
            lltv_bps = int(market["lltv"]) // 10**14
            entries.append(
                ReserveEntry(
                    symbol=market.get("name") or f"{market['collateral_token']}/{market['loan_token']}",
                    address=market["collateral_token_address"].lower(),
                    static_config=ReserveConfigRow(
                        borrowing_enabled=True,
                        usage_as_collateral_enabled=True,
                        is_active=True,
                        is_frozen=False,
                        ltv_bps=lltv_bps,
                        liquidation_threshold_bps=lltv_bps,
                        extra={
                            "market_id": market_id,
                            "loan_token": market["loan_token"],
                            "collateral_token": market["collateral_token"],
                        },
                    ),
                )
            )

        return LendingReserveDiscoveryPlan(
            protocol="morpho_blue",
            provider_address=singleton,
            static_entries=tuple(entries),
        )


__all__ = ["MorphoBlueAgentReadConnector"]
