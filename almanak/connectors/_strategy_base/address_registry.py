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
``{protocol: address_table}`` dispatch dict. Adding a new connector should not
require editing this framework-facing registry.

This registry is the strategy-side seam. It composes connector-published
``AddressTableSpec`` manifests and lazily imports only the address-table module
that owns a requested protocol, so a broken sibling address module cannot
poison an unrelated lookup. Consumers ask :func:`addresses_for` /
:func:`address_supported_chains` / :func:`resolve_contract_address` instead of
importing connectors directly.

The protocol-identifier vocabulary intentionally mirrors the gateway-side
contract: a single connector may publish several identifiers (the Uniswap
V3 connector owns both ``uniswap_v3`` and its ``agni_finance`` fork, which
share the same connector code but ship distinct per-chain address tables).

Gateway-boundary note: this module is strategy-side. It imports nothing
from ``almanak/connectors/_base/gateway_*`` and performs no network
egress — every ``addresses.py`` it loads is a pure-Python dict literal.
"""

from __future__ import annotations

import logging
from typing import ClassVar

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.address_table import AbiFamily, AddressTableSpec

__all__ = [
    "AbiFamily",
    "AddressRegistry",
    "address_supported_chains",
    "addresses_for",
    "resolve_contract_address",
]

logger = logging.getLogger(__name__)


class AddressRegistry:
    """Protocol-identifier → connector contract-address-table registry.

    Connector manifests publish lightweight ``AddressTableSpec`` values that
    map each protocol identifier to the connector ``addresses.py`` dict that
    owns that protocol's per-chain contract addresses. Multiple identifiers can
    point at the same connector module (``uniswap_v3`` and ``agni_finance`` both
    live in ``uniswap_v3.addresses`` under distinct attributes). The address
    table module is imported lazily on first lookup of a protocol it owns, and
    the resolved table is cached for the process lifetime.

    The cached table is the connector module's own dict (not a copy), so
    its identity is stable across calls — consumers must treat the returned
    mapping as read-only.
    """

    # protocol identifier -> the connector module's own per-chain table.
    _cache: ClassVar[dict[str, dict[str, dict[str, str]]]] = {}
    _spec_cache: ClassVar[dict[str, AddressTableSpec] | None] = None

    @classmethod
    def _address_table_specs(cls) -> dict[str, AddressTableSpec]:
        """Return protocol -> connector-published address-table spec."""
        if cls._spec_cache is not None:
            return cls._spec_cache

        specs: dict[str, AddressTableSpec] = {}
        for connector_manifest in CONNECTOR_REGISTRY.with_address_tables():
            if connector_manifest.address_tables is None:
                continue
            for spec in connector_manifest.address_tables:
                owner = specs.get(spec.protocol)
                if owner is not None:
                    raise ValueError(
                        f"AddressTableSpec protocol {spec.protocol!r} is declared twice "
                        f"({owner.module}.{owner.attribute} and {spec.module}.{spec.attribute})"
                    )
                specs[spec.protocol] = spec

        cls._spec_cache = specs
        return specs

    @classmethod
    def _load_table(cls, protocol: str) -> dict[str, dict[str, str]] | None:
        """Resolve and cache one protocol's per-chain address table.

        Imports ONLY the connector module that owns ``protocol`` (per its
        ``AddressTableSpec``) — a broken sibling connector cannot block this
        lookup. Returns ``None`` when the protocol is unknown.
        """
        cached = cls._cache.get(protocol)
        if cached is not None:
            return cached
        spec = cls._address_table_specs().get(protocol)
        if spec is None:
            return None
        try:
            table = spec.load_table()
        except Exception:
            logger.exception(
                "Failed to load address table for protocol %r from %s.%s",
                protocol,
                spec.module,
                spec.attribute,
            )
            return None
        cls._cache[protocol] = table
        return table

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector-owned address table."""
        return protocol.lower() in cls._address_table_specs()

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return every protocol identifier with a connector-owned table."""
        return tuple(sorted(cls._address_table_specs()))

    @classmethod
    def protocols_with_abi(cls, family: AbiFamily) -> tuple[str, ...]:
        """Return the protocol identifiers whose connectors expose ``family``.

        Lets a strategy-side consumer fan out over every connector that
        speaks a shared ABI (e.g. every Uniswap V3 fork) without naming a
        single protocol in framework code. The membership lives in each
        connector's address-table manifest; this registry composes it into a
        deterministic order so the consumer's output is stable across runs.
        """
        specs = [spec for spec in cls._address_table_specs().values() if family in spec.abi_families]
        return tuple(
            spec.protocol
            for spec in sorted(
                specs,
                key=lambda spec: (spec.abi_family_order is None, spec.abi_family_order or 0, spec.protocol),
            )
        )

    @classmethod
    def has_abi(cls, protocol: str, family: AbiFamily) -> bool:
        """Return True when ``protocol``'s connector exposes ``family``."""
        spec = cls._address_table_specs().get(protocol.lower())
        return spec is not None and family in spec.abi_families

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

        This is a set-membership view; iteration order is unspecified. When the
        order the connector declares its chains in matters (the intent
        compiler's per-chain address tables key on it for byte-equivalence),
        use :meth:`address_chains_ordered` instead.
        """
        table = cls._load_table(protocol.lower())
        if table is None:
            return frozenset()
        return frozenset(chain for chain, contracts in table.items() if contracts)

    @classmethod
    def address_chains_ordered(cls, protocol: str) -> tuple[str, ...]:
        """Return ``protocol``'s non-empty chains in connector-declaration order.

        Same membership as :meth:`address_supported_chains` but preserves the
        connector ``addresses.py`` table's insertion order. The intent
        compiler (VIB-4928 PR-3a) iterates this as the inner loop when
        materialising ``PROTOCOL_ROUTERS`` / ``LP_POSITION_MANAGERS`` / … so the
        derived tables' outer chain-key order stays byte-equivalent to the
        pre-inversion hand-rolled builders (which iterated ``table.items()``
        directly). Returns an empty tuple for an unknown protocol.
        """
        table = cls._load_table(protocol.lower())
        if table is None:
            return ()
        return tuple(chain for chain, contracts in table.items() if contracts)

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
        """Test helper: drop resolved specs/tables so the next call re-imports.

        Production code should never call this — it exists for narrow test
        setups that intentionally re-trigger a connector import.
        """
        cls._cache.clear()
        cls._spec_cache = None


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
