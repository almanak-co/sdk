"""Strategy-side connector contract-address registry (W1 / VIB-4853).

This is the **strategy-side** sibling of the gateway-side
:class:`almanak.connectors._base.gateway_capabilities.GatewayAddressCapability`.
W1 moved every protocol's on-chain contract-address table out of the
deleted central ``almanak.core.contracts`` module into the owning
connector's ``addresses.py``. The gateway sidecar reads those tables
through ``GatewayAddressCapability`` on each connector's
``GatewayConnector``.

Strategy-container code (anything under ``almanak/framework/**``) cannot
import the gateway-side capability registry — the import boundary in
``tests/static/test_strategy_import_boundary.py`` forbids it, and the
gateway boundary forbids the network egress the gateway-side providers
sometimes perform. Before this module, the strategy-side consumers that
need a protocol's address table (pool-existence validation, on-chain LP
discovery, teardown post-conditions) each re-imported the connector
``addresses`` modules by name and hand-rolled their own
``{protocol: address_table}`` dispatch dict. That is exactly the
cross-cutting connector knowledge VIB-4851 set out to retire: adding a new
V3-fork connector meant editing three framework files.

This registry is the strategy-side seam. It owns the single
protocol-identifier → ``addresses`` module mapping (``_BUILTIN_LOADERS``)
and lazily imports only the connector module that owns a requested
protocol, so a broken sibling connector cannot poison an unrelated
lookup. Consumers ask :func:`addresses_for` / :func:`address_supported_chains`
/ :func:`resolve_contract_address` instead of importing connectors
directly — the dispatch table lives here (a canonical foundation home the
coupling scanner allowlists), not scattered across the framework.

The protocol-identifier vocabulary intentionally mirrors the gateway-side
contract: a single connector may publish several identifiers (the Uniswap
V3 connector owns both ``uniswap_v3`` and its ``agni_finance`` fork, which
share the same connector code but ship distinct per-chain address tables).

Gateway-boundary note: this module is strategy-side. It imports nothing
from ``almanak/connectors/_base/gateway_*`` and performs no network
egress — every ``addresses.py`` it loads is a pure-Python dict literal.
"""

from __future__ import annotations

import importlib
from enum import StrEnum
from typing import ClassVar

__all__ = [
    "AbiFamily",
    "AddressRegistry",
    "address_supported_chains",
    "addresses_for",
    "resolve_contract_address",
]


class AbiFamily(StrEnum):
    """Shared on-chain ABI family a group of connectors exposes.

    Several strategy-side consumers (pool-existence validation, on-chain LP
    discovery, teardown post-conditions) speak one canonical ABI to a whole
    *family* of connectors — every Uniswap V3 fork exposes the same
    ``factory.getPool(...)`` and the same NonfungiblePositionManager
    ``positions(tokenId)`` interface, for instance. The family membership is
    connector knowledge, so it lives on this registry (a canonical
    foundation home) rather than as a hardcoded protocol set inside each
    framework consumer. Consumers ask :meth:`AddressRegistry.protocols_with_abi`
    for the members and iterate — they never name a protocol themselves.

    Members are intentionally *not* chain or protocol identifiers (so the
    coupling scanner doesn't conflate them): they name the ABI shape.
    """

    #: Uniswap-V3-style factory exposing ``getPool(address,address,uint24)``.
    V3_FACTORY = "v3_factory"
    #: Canonical Uniswap V3 NonfungiblePositionManager
    #: (``balanceOf`` / ``tokenOfOwnerByIndex`` / ``positions(tokenId)``).
    V3_NPM = "v3_npm"


class AddressRegistry:
    """Protocol-identifier → connector contract-address-table registry.

    ``_BUILTIN_LOADERS`` maps each protocol identifier to the
    ``(module_path, attribute)`` pair naming the connector ``addresses.py``
    dict that owns that protocol's per-chain contract addresses. Multiple
    identifiers can point at the same connector module (``uniswap_v3`` and
    ``agni_finance`` both live in ``uniswap_v3.addresses`` under distinct
    attributes). The connector module is imported lazily on first lookup
    of a protocol it owns, and the resolved table is cached for the
    process lifetime.

    The cached table is the connector module's own dict (not a copy), so
    its identity is stable across calls — consumers must treat the returned
    mapping as read-only.
    """

    _BUILTIN_LOADERS: ClassVar[dict[str, tuple[str, str]]] = {
        # Concentrated-liquidity / V3-fork DEXes (canonical NPM ABI).
        "uniswap_v3": ("almanak.connectors.uniswap_v3.addresses", "UNISWAP_V3"),
        "agni_finance": ("almanak.connectors.uniswap_v3.addresses", "AGNI_FINANCE"),
        "pancakeswap_v3": ("almanak.connectors.pancakeswap_v3.addresses", "PANCAKESWAP_V3"),
        "sushiswap_v3": ("almanak.connectors.sushiswap_v3.addresses", "SUSHISWAP_V3"),
        "uniswap_v4": ("almanak.connectors.uniswap_v4.addresses", "UNISWAP_V4"),
        "camelot": ("almanak.connectors.camelot.addresses", "CAMELOT"),
        # Solidly-fork DEXes.
        "aerodrome": ("almanak.connectors.aerodrome.addresses", "AERODROME"),
        # Liquidity Book DEX.
        "traderjoe_v2": ("almanak.connectors.traderjoe_v2.addresses", "TRADERJOE_V2"),
        # Weighted / stable AMM.
        "balancer_v2": ("almanak.connectors.balancer_v2.addresses", "BALANCER_V2"),
        # Yield / fixed-rate.
        "pendle": ("almanak.connectors.pendle.addresses", "PENDLE"),
        # Lending — pooled.
        "aave_v3": ("almanak.connectors.aave_v3.addresses", "AAVE_V3"),
        "radiant_v2": ("almanak.connectors.radiant_v2.addresses", "RADIANT_V2"),
        "spark": ("almanak.connectors.spark.addresses", "SPARK"),
        "fluid": ("almanak.connectors.fluid.addresses", "FLUID"),
        # Lending — isolated markets.
        "morpho_blue": ("almanak.connectors.morpho_blue.addresses", "MORPHO_BLUE"),
        # Perpetuals.
        "gmx_v2": ("almanak.connectors.gmx_v2.addresses", "GMX_V2"),
        "aster_perps": ("almanak.connectors.aster_perps.addresses", "ASTER_PERPS"),
        "pancakeswap_perps": ("almanak.connectors.aster_perps.addresses", "PANCAKESWAP_PERPS"),
    }

    # ABI family -> the protocol identifiers whose connectors expose that
    # canonical interface. Membership is ordered deterministically so
    # consumers that fan out over the family produce stable output.
    #
    # The V3 forks (Uniswap V3 and bytecode-compatible forks) all expose the
    # same ``factory.getPool(...)`` *and* the same NonfungiblePositionManager
    # interface, so they appear in both families. ``camelot`` and
    # ``uniswap_v4`` are deliberately absent: Camelot V3 uses Algebra's
    # ``getPool(address,address)`` (no fee arg) and V4 has a singleton
    # PoolManager rather than per-pool NPM NFTs — neither matches the
    # canonical V3 ABI these families denote.
    _ABI_FAMILIES: ClassVar[dict[AbiFamily, tuple[str, ...]]] = {
        AbiFamily.V3_FACTORY: (
            "uniswap_v3",
            "agni_finance",
            "pancakeswap_v3",
            "sushiswap_v3",
        ),
        AbiFamily.V3_NPM: (
            "uniswap_v3",
            "agni_finance",
            "pancakeswap_v3",
            "sushiswap_v3",
        ),
    }

    # protocol identifier -> the connector module's own per-chain table.
    _cache: ClassVar[dict[str, dict[str, dict[str, str]]]] = {}

    @classmethod
    def _load_table(cls, protocol: str) -> dict[str, dict[str, str]] | None:
        """Resolve and cache one protocol's per-chain address table.

        Imports ONLY the connector module that owns ``protocol`` (per
        ``_BUILTIN_LOADERS``) — a broken sibling connector cannot block
        this lookup. Returns ``None`` when the protocol is unknown.
        """
        cached = cls._cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._BUILTIN_LOADERS.get(protocol)
        if entry is None:
            return None
        module_path, attribute = entry
        module = importlib.import_module(module_path)
        table = getattr(module, attribute, None)
        if not isinstance(table, dict):
            raise TypeError(
                f"Registry maps {protocol!r} to {module_path}.{attribute}, "
                f"but that attribute is {type(table).__name__}, not a dict."
            )
        cls._cache[protocol] = table
        return table

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector-owned address table."""
        return protocol.lower() in cls._BUILTIN_LOADERS

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return every protocol identifier with a connector-owned table."""
        return tuple(sorted(cls._BUILTIN_LOADERS))

    @classmethod
    def protocols_with_abi(cls, family: AbiFamily) -> tuple[str, ...]:
        """Return the protocol identifiers whose connectors expose ``family``.

        Lets a strategy-side consumer fan out over every connector that
        speaks a shared ABI (e.g. every Uniswap V3 fork) without naming a
        single protocol in framework code — the membership lives here, in
        the canonical foundation home. Membership order is deterministic so
        the consumer's output is stable across runs.
        """
        return cls._ABI_FAMILIES.get(family, ())

    @classmethod
    def has_abi(cls, protocol: str, family: AbiFamily) -> bool:
        """Return True when ``protocol``'s connector exposes ``family``."""
        return protocol.lower() in cls._ABI_FAMILIES.get(family, ())

    @classmethod
    def addresses_for(cls, protocol: str, chain: str) -> dict[str, str]:
        """Return the ``{contract_kind: address}`` mapping for ``protocol`` on ``chain``.

        Mirrors the gateway-side
        :meth:`GatewayAddressCapability.addresses_for` contract: the
        contract-kind vocabulary (``swap_router`` / ``position_manager`` /
        ``pool`` / ``factory`` / ``nft`` / …) is connector-private and may
        grow over time — callers MUST NOT assume any specific key is
        present. Returns an empty dict when the protocol is unknown or the
        chain is unsupported.

        The returned dict is the connector module's own per-chain entry
        (stable identity); callers must treat it as read-only.
        """
        table = cls._load_table(protocol.lower())
        if table is None:
            return {}
        return table.get(chain.lower()) or table.get(chain) or {}

    @classmethod
    def address_supported_chains(cls, protocol: str) -> frozenset[str]:
        """Return the chains ``protocol`` publishes a non-empty address table for.

        Mirrors the gateway-side
        :meth:`GatewayAddressCapability.address_supported_chains`. Returns
        an empty frozenset for an unknown protocol or one that ships no
        addresses.
        """
        table = cls._load_table(protocol.lower())
        if table is None:
            return frozenset()
        return frozenset(chain for chain, contracts in table.items() if contracts)

    @classmethod
    def resolve_contract_address(
        cls,
        protocol: str,
        chain: str,
        kinds: tuple[str, ...] | str,
    ) -> str | None:
        """Return the first non-empty address among ``kinds`` for ``(protocol, chain)``.

        ``kinds`` is the connector's contract-kind vocabulary tried in
        order — e.g. ``("position_manager", "nft")`` to resolve a
        NonfungiblePositionManager that PancakeSwap V3 records under
        ``nft`` and the other V3 forks record under ``position_manager``.
        Returns ``None`` when the protocol / chain is unsupported or none
        of ``kinds`` is present, so callers can fail-closed on ``None``.
        """
        contracts = cls.addresses_for(protocol, chain)
        if not contracts:
            return None
        key_order = (kinds,) if isinstance(kinds, str) else kinds
        for key in key_order:
            address = contracts.get(key)
            if address:
                return address
        return None

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the resolved-table cache so the next call re-imports.

        Production code should never call this — it exists for narrow test
        setups that intentionally re-trigger a connector import.
        """
        cls._cache.clear()


def addresses_for(protocol: str, chain: str) -> dict[str, str]:
    """Module-level convenience wrapper for :meth:`AddressRegistry.addresses_for`."""
    return AddressRegistry.addresses_for(protocol, chain)


def address_supported_chains(protocol: str) -> frozenset[str]:
    """Module-level convenience wrapper for :meth:`AddressRegistry.address_supported_chains`."""
    return AddressRegistry.address_supported_chains(protocol)


def resolve_contract_address(
    protocol: str,
    chain: str,
    kinds: tuple[str, ...] | str,
) -> str | None:
    """Module-level convenience wrapper for :meth:`AddressRegistry.resolve_contract_address`."""
    return AddressRegistry.resolve_contract_address(protocol, chain, kinds)
