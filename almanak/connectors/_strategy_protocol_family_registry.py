"""Strategy-side protocol-family registration site (VIB-4928 PR-3b).

Sibling of :mod:`almanak.connectors._strategy_contract_role_registry`, scoped to
named protocol-family membership (``AAVE_COMPATIBLE_PROTOCOLS`` /
``UNIV3_LP_GROUPING_PROTOCOLS``). Adding a connector to a family is one
``protocol_family.py`` data module plus a ``CONNECTOR.protocol_family`` import
reference in the connector's own manifest.

Registration order is irrelevant (set-union membership). Lives one level up
from ``_strategy_base/`` because it owns strategy-side registry bootstrap;
``_strategy_base/`` stays protocol-clean (no concrete connector imports). This
file imports only connector manifests. Protocol-family specs are loaded from
connector-owned lazy import references.

The completeness invariant: every connector shipping a ``protocol_family``
module MUST publish it from its manifest. This is enforced by
``tests/unit/connectors/test_protocol_family_registry_completeness.py``.

Gateway-boundary note: this module is strategy-side and performs no network
egress. The connector ``protocol_family`` modules it loads are pure data.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY, ConnectorDiscoveryError
from almanak.connectors._strategy_base.protocol_family_registry import (
    PROTOCOL_FAMILY_REGISTRY,
    ProtocolFamily,
    ProtocolFamilyRegistry,
    ProtocolFamilySpec,
)

__all__ = [
    "PROTOCOL_FAMILY_REGISTRY",
    "ProtocolFamily",
    "ProtocolFamilyRegistry",
    "ProtocolFamilySpec",
]


def _register_all() -> None:
    """Register every connector-owned protocol-family membership spec."""
    for connector_manifest in CONNECTOR_REGISTRY.with_protocol_family():
        if connector_manifest.protocol_family is None:
            continue
        spec = connector_manifest.protocol_family.load()
        if not isinstance(spec, ProtocolFamilySpec):
            import_ref = connector_manifest.protocol_family
            raise ConnectorDiscoveryError(
                f"{import_ref.module}.{import_ref.attribute} must be a ProtocolFamilySpec, "
                f"got {type(spec).__qualname__}"
            )
        PROTOCOL_FAMILY_REGISTRY.register(spec)


_register_all()
