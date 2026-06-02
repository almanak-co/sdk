"""Lazy registry for connector-owned chain coverage.

Each connector that runs on one or more chains ships a
``supported_chains.py`` module exporting a module-level
``SUPPORTED_CHAINS_BY_PROTOCOL`` dict keyed by every protocol identifier the
connector backs, mapping to a ``frozenset[str]`` of chains it runs on. This
registry imports those modules on demand and exposes a single aggregated
``protocol -> {chains}`` view.

**Direction (the whole point of this module).** The data "which chains does
protocol X run on" is *connector* knowledge and lives in the connector's own
folder — never in a chain file. Adding a connector means dropping one folder
with its own ``supported_chains.py``; adding a chain to an existing connector
is a one-line edit in that connector's module. The legacy hand-maintained
``protocol -> {chains}`` matrix in
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS` is now a
*derived* back-compat view built by
:func:`supported_protocols_matrix` iterating this registry, so it cannot
drift from the connectors.

This registry deliberately mirrors
:class:`almanak.connectors._strategy_base.capabilities_registry.CapabilitiesRegistry`:
``_BUILTIN_LOADERS`` maps each protocol identifier to the connector module
that owns it; multiple identifiers may resolve to the same module
(``uniswap_v3`` and the fork ``agni_finance`` both resolve to
``uniswap_v3.supported_chains``). Per-protocol lookups import ONLY the owning
module — a broken sibling cannot poison an unrelated lookup. Bulk consumers
load every module once and cache the merged dict.

**Import boundary.** This module is strategy-side
(``almanak/connectors/_strategy_base``) and pulls in nothing gateway-side.
It is the seam that lets ``almanak.framework.execution.config`` — which runs
in the strategy container — derive the matrix WITHOUT importing
``almanak/connectors/_base/gateway_capabilities.py`` (enforced by
``tests/static/test_strategy_import_boundary.py``).
"""

from __future__ import annotations

import importlib
from typing import ClassVar


class SupportedChainsRegistry:
    """Protocol-name to connector chain-set registry.

    ``_BUILTIN_LOADERS`` maps each protocol identifier to the connector
    module that owns it. Multiple identifiers can point at the same module
    (``uniswap_v3`` and its fork ``agni_finance`` both resolve to
    ``uniswap_v3.supported_chains``). The ``supported_chains`` module itself
    is responsible for declaring every key it owns via
    ``SUPPORTED_CHAINS_BY_PROTOCOL`` -- the registry does not infer key lists
    from the protocol→module mapping, keeping the fork-ownership contract
    explicit at the connector level.
    """

    _BUILTIN_LOADERS: ClassVar[dict[str, str]] = {
        # Lending — pooled / forks
        "aave_v3": "almanak.connectors.aave_v3.supported_chains",
        "spark": "almanak.connectors.spark.supported_chains",
        "benqi": "almanak.connectors.benqi.supported_chains",
        "euler_v2": "almanak.connectors.euler_v2.supported_chains",
        "silo_v2": "almanak.connectors.silo_v2.supported_chains",
        # Concentrated-liquidity / swap / aggregator
        "uniswap_v3": "almanak.connectors.uniswap_v3.supported_chains",
        # Agni Finance is a Uniswap V3 fork with no own folder; the
        # uniswap_v3 connector module declares it.
        "agni_finance": "almanak.connectors.uniswap_v3.supported_chains",
        "sushiswap_v3": "almanak.connectors.sushiswap_v3.supported_chains",
        "pancakeswap_v3": "almanak.connectors.pancakeswap_v3.supported_chains",
        "traderjoe_v2": "almanak.connectors.traderjoe_v2.supported_chains",
        "enso": "almanak.connectors.enso.supported_chains",
        # Perps
        "gmx_v2": "almanak.connectors.gmx_v2.supported_chains",
        "hyperliquid": "almanak.connectors.hyperliquid.supported_chains",
        # Staking / synthetic
        "lido": "almanak.connectors.lido.supported_chains",
        "ethena": "almanak.connectors.ethena.supported_chains",
        "gimo": "almanak.connectors.gimo.supported_chains",
    }

    # Aggregated ``protocol -> frozenset[chains]`` view. Populated
    # incrementally by ``get`` and fully by ``all_supported_chains``. Once an
    # entry is added it is never replaced or removed within a process.
    _aggregated: ClassVar[dict[str, frozenset[str]]] = {}
    # True once every module in ``_BUILTIN_LOADERS`` has been loaded.
    _all_loaded: ClassVar[bool] = False

    @classmethod
    def _load_module_chains(cls, module_path: str) -> dict[str, frozenset[str]]:
        """Import a connector ``supported_chains`` module and return its dict."""
        module = importlib.import_module(module_path)
        decl = getattr(module, "SUPPORTED_CHAINS_BY_PROTOCOL", None)
        if not isinstance(decl, dict):
            raise TypeError(f"{module_path}.SUPPORTED_CHAINS_BY_PROTOCOL must be a dict, got {type(decl).__name__}")
        return decl

    @classmethod
    def _load_protocol(cls, key: str) -> frozenset[str] | None:
        """Resolve a single protocol's chain set and cache it.

        Imports ONLY the connector module that owns ``key`` (per the
        ``_BUILTIN_LOADERS`` mapping) — a broken sibling connector cannot
        block this lookup. Returns ``None`` when the protocol is unknown.
        """
        if key in cls._aggregated:
            return cls._aggregated[key]
        module_path = cls._BUILTIN_LOADERS.get(key)
        if module_path is None:
            return None
        module_chains = cls._load_module_chains(module_path)
        if key not in module_chains:
            raise KeyError(
                f"Registry maps '{key}' to '{module_path}' but that module's "
                f"SUPPORTED_CHAINS_BY_PROTOCOL has no '{key}' key. Available "
                f"keys in {module_path}.SUPPORTED_CHAINS_BY_PROTOCOL: "
                f"{sorted(module_chains)}"
            )
        chains = module_chains[key]
        if not isinstance(chains, frozenset):
            raise TypeError(
                f"{module_path}.SUPPORTED_CHAINS_BY_PROTOCOL['{key}'] must be a frozenset, got {type(chains).__name__}"
            )
        cls._aggregated[key] = chains
        return chains

    @classmethod
    def all_supported_chains(cls) -> dict[str, frozenset[str]]:
        """Return the aggregated ``protocol -> frozenset[chains]`` mapping.

        Imports every connector ``supported_chains`` module on first call and
        merges them into the shared ``_aggregated`` dict. Identity is stable —
        the same dict instance is returned on every call.
        """
        if cls._all_loaded:
            return cls._aggregated
        for key in cls._BUILTIN_LOADERS:
            cls._load_protocol(key)
        cls._all_loaded = True
        return cls._aggregated

    @classmethod
    def get(cls, protocol: str) -> frozenset[str] | None:
        """Return the chain set for ``protocol`` (lower-cased), or ``None``.

        Imports ONLY the connector module that owns ``protocol`` — unrelated
        connectors with broken imports cannot break this lookup.
        """
        return cls._load_protocol(protocol.lower())

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector-owned chain entry."""
        return protocol.lower() in cls._BUILTIN_LOADERS

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return all protocol names with a connector-owned chain entry."""
        return tuple(cls._BUILTIN_LOADERS)

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: clear the aggregated cache so the next call rebuilds.

        Clears the ``_aggregated`` dict **in place** so any external reference
        to it stays live and refills on the next access. Production code
        should never call this.
        """
        cls._aggregated.clear()
        cls._all_loaded = False


def supported_protocols_matrix() -> dict[str, set[str]]:
    """Derive the legacy ``protocol -> {chains}`` matrix from the registry.

    Mutable ``set`` values (a fresh copy per call) preserve the exact shape of
    the historical hand-maintained
    :data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS` literal so
    its consumers (``config.runtime`` validation,
    ``MultiChainRuntimeConfig._validate_protocols``) keep working unchanged.
    The values are copies so a mutating consumer cannot corrupt the registry's
    cached frozensets.
    """
    return {protocol: set(chains) for protocol, chains in SupportedChainsRegistry.all_supported_chains().items()}


def supported_chains_for(protocol: str) -> frozenset[str]:
    """Module-level convenience wrapper.

    Returns an empty frozenset when the protocol is unknown so consumers can
    branch on membership without a ``None`` check.
    """
    return SupportedChainsRegistry.get(protocol) or frozenset()


__all__ = [
    "SupportedChainsRegistry",
    "supported_chains_for",
    "supported_protocols_matrix",
]
