"""Strategy-side agent-read providers for Uniswap V3 + its Agni fork (VIB-4860 / W8).

Publishes the on-chain *read descriptors* the agent-tool pool-state and
LP-position read handlers need:

* ``factory_address(chain)`` — the V3 factory for ``factory.getPool()``.
* ``position_manager_address(chain)`` — the NonfungiblePositionManager for
  ``positions(uint256)``.
* ``get_pool_selector()`` — ``0x1698ee82`` (the uint24-fee ``getPool``
  selector the v3 family uses; Aerodrome Slipstream's int24 variant lives
  in the aerodrome provider).

Byte-equivalence (VIB-4860)
===========================

These values MUST match what ``executor.py`` resolved inline before W8:

* ``factory_address`` ← ``UNISWAP_V3[chain]["factory"]`` (canonical
  ``uniswap_v3``) / ``AGNI_FINANCE[chain]["factory"]`` (``agni_finance``).
* ``position_manager_address`` ← ``POSITION_MANAGER_ADDRESSES[chain]``.
  **Note:** the pre-W8 LP-position read handler resolved the NPM for
  *every* non-Slipstream protocol (Uniswap V3 *and* its Agni alias) from
  ``POSITION_MANAGER_ADDRESSES`` — not from ``AGNI_FINANCE[..]["position_manager"]``.
  Both happen to be the same Mantle address, but we replicate the exact
  table the handler used so the behaviour is byte-identical.

Agni Finance is a Uniswap V3 fork whose address tables live inside this
connector (``addresses.AGNI_FINANCE``); it has no folder of its own, so the
Uniswap V3 connector publishes both the ``uniswap_v3`` and ``agni_finance``
canonical protocol names. ``normalize_protocol("mantle", "uniswap_v3")``
maps to ``agni_finance`` before reaching the registry.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.agent_read_registry import (
    AgentReadCapability,
    AgentReadConnector,
)

# The uint24-fee getPool selector shared by the Uniswap V3 family (and its
# Agni / PancakeSwap / SushiSwap forks). Slipstream's int24 variant lives in
# the aerodrome provider.
_GET_POOL_SELECTOR_V3 = "0x1698ee82"


class UniswapV3AgentReadConnector(AgentReadConnector, AgentReadCapability):
    """Agent-read descriptors for canonical ``uniswap_v3`` pools/positions."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def agent_read_keys(self) -> frozenset[str]:
        return frozenset({"pool_state", "lp_position"})

    def factory_address(self, chain: str) -> str | None:
        from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3

        # A chain explicitly mapped to ``None`` (documented as disabled) must
        # not raise on ``.get("factory")`` — mirror the aerodrome/sushiswap
        # isinstance guard.
        chain_contracts = UNISWAP_V3.get(chain)
        return chain_contracts.get("factory") if isinstance(chain_contracts, dict) else None

    def position_manager_address(self, chain: str) -> str | None:
        from almanak.connectors.uniswap_v3.receipt_parser import (
            POSITION_MANAGER_ADDRESSES,
        )

        return POSITION_MANAGER_ADDRESSES.get(chain)

    def get_pool_selector(self) -> str:
        return _GET_POOL_SELECTOR_V3

    def lending_pool_address(self, chain: str) -> str | None:
        return None


class AgniFinanceAgentReadConnector(AgentReadConnector, AgentReadCapability):
    """Agent-read descriptors for the ``agni_finance`` Uniswap V3 fork (Mantle)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("agni_finance")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def agent_read_keys(self) -> frozenset[str]:
        return frozenset({"pool_state", "lp_position"})

    def factory_address(self, chain: str) -> str | None:
        from almanak.connectors.uniswap_v3.addresses import AGNI_FINANCE

        # Defensive: a chain explicitly mapped to ``None`` must not raise on
        # ``.get("factory")``.
        chain_contracts = AGNI_FINANCE.get(chain)
        return chain_contracts.get("factory") if isinstance(chain_contracts, dict) else None

    def position_manager_address(self, chain: str) -> str | None:
        # The pre-W8 LP-position read handler used POSITION_MANAGER_ADDRESSES
        # for every non-Slipstream protocol (incl. the Agni alias). Preserve it.
        from almanak.connectors.uniswap_v3.receipt_parser import (
            POSITION_MANAGER_ADDRESSES,
        )

        return POSITION_MANAGER_ADDRESSES.get(chain)

    def get_pool_selector(self) -> str:
        return _GET_POOL_SELECTOR_V3

    def lending_pool_address(self, chain: str) -> str | None:
        return None


__all__ = ["AgniFinanceAgentReadConnector", "UniswapV3AgentReadConnector"]
