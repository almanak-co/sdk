"""Strategy-side protocol-family membership registry (VIB-4928 PR-3b).

Sibling of
:class:`~almanak.connectors._strategy_base.contract_role_registry.ContractRoleRegistry`,
scoped to named ABI/behaviour *family-membership* sets the framework surfaces as
a single ``protocol in <family>`` test. Two families exist today:

* :attr:`ProtocolFamily.AAVE_V3` -> ``AAVE_COMPATIBLE_PROTOCOLS`` (lending
  connectors sharing the Aave V3 ``supply`` / ``borrow`` / ``repay`` /
  ``withdraw`` Pool ABI).
* :attr:`ProtocolFamily.UNIV3_LP_GROUPING` -> ``UNIV3_LP_GROUPING_PROTOCOLS``
  (DEX connectors using the Uniswap-V3-shape ``univ3_lp@v1``
  NFT-position-manager-keyed grouping policy).

Why a sibling registry keyed by a family enum
---------------------------------------------
Both symbols are the identical shape — a frozenset of slugs for one membership
test — so one narrow registry keyed by :class:`ProtocolFamily` serves both
without five one-off registries, and without conflating with the swap-router
classification or contract-address concerns (each its own sibling registry).
Membership is union-semantics: two connectors declaring the same slug in the
same family is harmless (set union), so there is no collision gate here (unlike
the swap fee-tier values).

Gateway-boundary note: this module is strategy-side and performs no network
egress. It holds only membership metadata (frozensets of protocol slugs).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

__all__ = [
    "PROTOCOL_FAMILY_REGISTRY",
    "ProtocolFamily",
    "ProtocolFamilyRegistry",
    "ProtocolFamilySpec",
]


class ProtocolFamily(StrEnum):
    """A named ABI/behaviour family the framework tests membership against."""

    #: Lending connectors sharing the Aave V3 Pool ABI ->
    #: ``AAVE_COMPATIBLE_PROTOCOLS``.
    AAVE_V3 = "aave_v3"
    #: DEX connectors using the ``univ3_lp@v1`` grouping policy ->
    #: ``UNIV3_LP_GROUPING_PROTOCOLS``.
    UNIV3_LP_GROUPING = "univ3_lp_grouping"


@dataclass(frozen=True)
class ProtocolFamilySpec:
    """One connector's family-membership contribution.

    A connector's ``protocol_family.PROTOCOL_FAMILY`` is one of these. The boot
    file (``almanak.connectors._strategy_protocol_family_registry``) feeds each
    spec straight into :meth:`ProtocolFamilyRegistry.register`.

    Attributes:
        families: Map of :class:`ProtocolFamily` -> the protocol slugs this
            connector contributes to that family.
    """

    families: Mapping[ProtocolFamily, frozenset[str]]


class ProtocolFamilyRegistry:
    """Family -> union of contributed protocol slugs.

    Populated once at boot by
    ``almanak.connectors._strategy_protocol_family_registry``. The intent
    compiler / migration backfill ask :meth:`members` for a family's frozenset
    — never naming a connector.
    """

    #: family -> accumulated slug set (set-union across registrations).
    _families: ClassVar[dict[ProtocolFamily, set[str]]] = {}

    @classmethod
    def register(cls, spec: ProtocolFamilySpec) -> None:
        """Add a connector's family contributions (set-union; idempotent).

        Re-registering an identical contribution is a no-op (set union), so a
        connector re-imported under pytest-xdist does not widen or duplicate.
        """
        for family, slugs in spec.families.items():
            cls._families.setdefault(family, set()).update(slugs)

    @classmethod
    def members(cls, family: ProtocolFamily) -> frozenset[str]:
        """Return the membership frozenset for ``family`` (empty if none)."""
        return frozenset(cls._families.get(family, set()))

    @classmethod
    def families(cls) -> tuple[ProtocolFamily, ...]:
        """Every family with at least one registered member."""
        return tuple(cls._families)

    @classmethod
    def reset(cls) -> None:
        """Test helper: drop all registrations (production never calls this)."""
        cls._families.clear()


#: The single in-process registry. Concrete connectors are registered into it
#: by ``almanak.connectors._strategy_protocol_family_registry`` (boot file).
PROTOCOL_FAMILY_REGISTRY = ProtocolFamilyRegistry
