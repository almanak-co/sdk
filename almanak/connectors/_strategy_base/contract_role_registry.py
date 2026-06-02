"""Connector contract-role registry (VIB-4928 PR-3a).

Sibling of :class:`~almanak.connectors._strategy_base.address_registry.AddressRegistry`
and the other ``_strategy_base`` registries (flash-loan, lending-read,
gas-estimate). It owns the **semantic-role → contract-kind** mapping that the
intent compiler's per-chain address tables fan out over.

Why a sibling, not bolted onto ``AddressRegistry``
--------------------------------------------------
``AddressRegistry`` answers *"what address does connector X record under kind
``k`` on chain ``c``?"* — its vocabulary (``swap_router`` / ``position_manager``
/ ``nft`` / ``cl_nft`` / ``dex_factory`` / ``pool`` / ``vault`` / …) is
**connector-private** and may differ between connectors for the same semantic
slot (PancakeSwap V3 records its NFT position manager under ``nft`` while the
other V3 forks use ``position_manager``; TraderJoe V2 serves both its router
and its LP position-manager slot from a single ``router`` address). This
registry adds the missing layer: a **stable semantic role vocabulary**
(:class:`ContractRole`) that the framework's six address tables key on, mapped
per-connector to the ordered list of connector-private kinds that satisfy that
role. Folding both concerns into ``AddressRegistry`` would conflate the
connector's private kind names with the framework's public role slots — the
exact coupling VIB-4851 set out to retire — so the role layer is its own
registry, populated independently by the boot file.

Population
----------
This module stays protocol-clean — it imports **no concrete connector**. The
connectors are registered by the boot file
``almanak/connectors/_strategy_contract_role_registry.py`` (mirrors
``_strategy_flash_loan_registry.py``), which lives one level up so
``_strategy_base/`` never imports a concrete connector.

**Registration order is load-bearing.** The intent compiler iterates
:meth:`ContractRoleRegistry.protocols_with_role` (registration order, filtered
to the role) as the *outer* loop when materialising each address table, and the
per-protocol key order within every chain's sub-dict is exactly that iteration
order. Keep the boot-file registration order stable unless intentionally
changing that surface (the full-dict equivalence pins in
``tests/unit/intents/test_contract_role_registry_equivalence.py`` enforce it).

Gateway-boundary note: this module is strategy-side and performs no network
egress. It holds only role→kind metadata (plain tuples / strings).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

__all__ = [
    "CONTRACT_ROLE_REGISTRY",
    "ContractRole",
    "ContractRoleRegistry",
    "ContractRoleSpec",
]


class ContractRole(StrEnum):
    """Semantic contract slot the framework's address tables key on.

    Roles are deliberately distinct from each connector's private contract
    ``kinds`` (see :class:`AddressRegistry`): a role is the *purpose* an
    address serves at the compile-time lookup boundary (``"the router I send a
    swap to"``), while a kind is the connector's internal label for the address
    in its ``addresses.py`` table. The per-connector
    ``contract_roles.CONTRACT_ROLES`` map translates one to the other.

    The seven members below back the six PR-3a address tables (``ROUTER`` →
    ``PROTOCOL_ROUTERS``; ``LP_POSITION_MANAGER`` / ``CL_POSITION_MANAGER`` →
    ``LP_POSITION_MANAGERS``; ``QUOTER`` → ``SWAP_QUOTER_ADDRESSES``;
    ``LENDING_POOL`` → ``LENDING_POOL_ADDRESSES``; ``LENDING_DATA_PROVIDER`` →
    ``LENDING_POOL_DATA_PROVIDERS``; ``FLASH_LOAN_VAULT`` →
    ``BALANCER_VAULT_ADDRESSES``).
    """

    #: Swap router the DefaultSwapAdapter / connector compiler sends swaps to.
    ROUTER = "router"
    #: NonfungiblePositionManager (or LB router serving the LP slot) for the
    #: connector's fungible / V3-style concentrated-liquidity positions.
    LP_POSITION_MANAGER = "lp_position_manager"
    #: Concentrated-liquidity (Slipstream) NFT position manager — distinct from
    #: ``LP_POSITION_MANAGER`` because a single connector (Aerodrome) publishes
    #: both a fungible-LP router and a separate CL NFT manager.
    CL_POSITION_MANAGER = "cl_position_manager"
    #: Quoter used for AUTO fee-tier selection.
    QUOTER = "quoter"
    #: Lending pool (Aave-V3-family ``Pool`` / Spark ``Pool``).
    LENDING_POOL = "lending_pool"
    #: Lending protocol-data provider (Aave V3 ``PoolDataProvider``).
    LENDING_DATA_PROVIDER = "lending_data_provider"
    #: Flash-loan vault (Balancer V2 ``Vault``).
    FLASH_LOAN_VAULT = "flash_loan_vault"


@dataclass(frozen=True)
class ContractRoleSpec:
    """One protocol slug's contract-role declaration.

    A connector's ``contract_roles.CONTRACT_ROLES`` is an ordered tuple of
    these — one per slug the connector owns (most connectors own a single
    slug; the Uniswap V3 connector owns ``uniswap_v3`` + ``agni_finance``, and
    Aerodrome owns ``aerodrome`` + the ``aerodrome_slipstream`` pseudo-slug).
    The boot file
    (``almanak.connectors._strategy_contract_role_registry``) feeds each spec
    straight into :meth:`ContractRoleRegistry.register`.

    Attributes:
        protocol: The framework-facing protocol slug.
        roles: Map of :class:`ContractRole` → ordered connector-private
            contract kinds satisfying that role (tried in order by
            ``AddressRegistry.resolve_contract_address``).
        address_protocol: ``AddressRegistry._BUILTIN_LOADERS`` key whose
            per-chain table backs this slug. ``None`` → same as ``protocol``;
            set it only for a pseudo-slug riding on another connector's table
            (``aerodrome_slipstream`` → ``"aerodrome"``).
    """

    protocol: str
    roles: Mapping[ContractRole, tuple[str, ...]]
    address_protocol: str | None = None


class ContractRoleRegistry:
    """Protocol-identifier → {role → ordered connector-kinds} dispatch.

    Populated once at boot by
    ``almanak.connectors._strategy_contract_role_registry``. The intent
    compiler asks :meth:`protocols_with_role` for the protocols that satisfy a
    role (registration order), :meth:`address_protocol` for the
    ``AddressRegistry`` table that backs a protocol slug, and :meth:`kinds_for`
    for the ordered connector kinds to resolve — never naming a connector.
    """

    #: protocol slug -> {role -> ordered tuple of connector-private kinds}.
    #: Insertion-ordered (registration order) so :meth:`protocols_with_role`
    #: yields a stable, byte-equivalent sequence.
    _roles: ClassVar[dict[str, dict[ContractRole, tuple[str, ...]]]] = {}

    #: protocol slug -> the ``AddressRegistry._BUILTIN_LOADERS`` key whose
    #: per-chain table backs the slug. Only differs from the slug itself for
    #: pseudo-protocols that ride on another connector's table
    #: (``aerodrome_slipstream`` → ``aerodrome``).
    _aliases: ClassVar[dict[str, str]] = {}

    @classmethod
    def register(
        cls,
        *,
        protocol: str,
        roles: Mapping[ContractRole, tuple[str, ...]],
        address_protocol: str | None = None,
    ) -> None:
        """Register (or replace) one protocol slug's role → kinds map.

        Args:
            protocol: The protocol slug the framework address tables key on
                (e.g. ``"uniswap_v3"``, ``"agni_finance"``,
                ``"aerodrome_slipstream"``).
            roles: Map of :class:`ContractRole` → the ordered connector-private
                contract kinds that satisfy that role. Each kinds tuple is
                passed straight to ``AddressRegistry.resolve_contract_address``
                (tried in order; first non-empty wins). Iteration order of
                ``roles`` is otherwise irrelevant — only ``protocol``
                registration order matters for the derived tables.
            address_protocol: The ``AddressRegistry._BUILTIN_LOADERS`` key
                whose per-chain table backs ``protocol``. Defaults to
                ``protocol``; set it only for a pseudo-protocol that resolves
                its addresses from another connector's table
                (``aerodrome_slipstream`` → ``"aerodrome"``).
        """
        # Insertion-ordered: re-registering an existing slug keeps its original
        # position (dict assignment to an existing key does not reorder), which
        # preserves byte-equivalence if a connector is re-imported under xdist.
        cls._roles[protocol] = {role: tuple(kinds) for role, kinds in roles.items()}
        # Re-registering without an alias must clear any stale alias from a
        # prior registration — otherwise ``address_protocol`` keeps resolving to
        # the old table key, breaking the "register (or replace)" contract.
        if address_protocol is not None and address_protocol != protocol:
            cls._aliases[protocol] = address_protocol
        else:
            cls._aliases.pop(protocol, None)

    @classmethod
    def protocols_with_role(cls, role: ContractRole) -> tuple[str, ...]:
        """Return the protocol slugs that declare ``role``, in registration order.

        Mirrors :meth:`AddressRegistry.protocols_with_abi` — lets the compiler
        fan out over every connector that fills a semantic slot without naming
        one. Registration order is deterministic so the derived address tables
        (and the per-protocol key order within each chain) stay byte-stable.
        """
        return tuple(protocol for protocol, roles in cls._roles.items() if role in roles)

    @classmethod
    def kinds_for(cls, protocol: str, role: ContractRole) -> tuple[str, ...] | None:
        """Return the ordered connector kinds for ``(protocol, role)``.

        ``None`` when the protocol is unregistered or does not declare
        ``role`` — callers fan out via :meth:`protocols_with_role` first, so a
        ``None`` here is a registration bug, not an expected miss.
        """
        roles = cls._roles.get(protocol)
        if roles is None:
            return None
        return roles.get(role)

    @classmethod
    def address_protocol(cls, protocol: str) -> str:
        """Return the ``AddressRegistry`` table key backing ``protocol``.

        Resolves the pseudo-protocol alias (``aerodrome_slipstream`` →
        ``"aerodrome"``); returns ``protocol`` unchanged for a slug that owns
        its own table.
        """
        return cls._aliases.get(protocol, protocol)

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Whether ``protocol`` has any role registration."""
        return protocol in cls._roles

    @classmethod
    def registered_protocols(cls) -> tuple[str, ...]:
        """Every registered protocol slug, in registration order."""
        return tuple(cls._roles)

    @classmethod
    def reset(cls) -> None:
        """Test helper: drop all registrations.

        Production code never calls this — it exists for narrow test setups
        that intentionally rebuild the registry. (The boot file is import-once;
        a re-import is a no-op without this.)
        """
        cls._roles.clear()
        cls._aliases.clear()


#: The single in-process registry. Concrete connectors are registered into it
#: by ``almanak.connectors._strategy_contract_role_registry`` (the boot file).
CONTRACT_ROLE_REGISTRY = ContractRoleRegistry
