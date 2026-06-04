"""Strategy-side gas-estimate connector registration site (VIB-4858 / W6).

Sibling of :mod:`almanak.connectors._strategy_receipt_registry`, scoped
to the gas-estimate concern.

Lives one level up from ``_strategy_base/`` because it owns gas-estimate
registry bootstrap, and ``_strategy_base/`` must stay protocol-clean
(no concrete connector imports). It imports only connector descriptors;
provider classes are loaded from connector-owned lazy import references.

Connectors that publish ``almanak/connectors/<protocol>/connector.py`` with
a ``CONNECTOR.gas_estimate_connector`` import reference are registered from
that connector object::

    CONNECTOR = Connector(
        name="<protocol>",
        kind=ProtocolKind.<KIND>,
        gas_estimate_connector=ImportRef(
            module="almanak.connectors.<protocol>.gas_estimate_provider",
            attribute="<Protocol>GasEstimateConnector",
        ),
    )

The completeness invariant — every connector whose actions appeared in
the central ``DEFAULT_GAS_ESTIMATES`` table MUST publish a descriptor
reference — is enforced by
``tests/unit/connectors/test_gas_estimate_registry_completeness.py`` and
the byte-equivalence pin in
``tests/unit/intents/test_w6_gas_estimate_byte_equivalence.py``.

Why a strategy-side registry (vs. reading from ``GATEWAY_REGISTRY``)
====================================================================

Intent compilation runs inside the strategy container — the framework's
``Compiler`` builds ``TransactionData`` entries with their gas estimates
before the gateway ever sees them. Strategy-side modules are forbidden
from importing the gateway-side registry
(``almanak.connectors._gateway_registry``) per
``tests/static/test_strategy_import_boundary.py``, so the gas-estimate
dispatch cannot consume ``GATEWAY_REGISTRY``. This file is the
strategy-side mirror.

This file is allow-listed in the strategy-side import boundary scan
(``_STRATEGY_SCAN_SKIP_PARTS`` in
``tests/static/test_strategy_import_boundary.py``) the same way
``_strategy_receipt_registry.py`` is allow-listed: it is the boot-time
discovery entry point. It no longer knows connector names.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.gas_estimate_registry import (
    STRATEGY_GAS_ESTIMATE_REGISTRY,
)

__all__ = ["STRATEGY_GAS_ESTIMATE_REGISTRY"]


def _register_discovered_gas_estimates() -> None:
    """Register gas-estimate connectors published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_gas_estimate():
        if connector_manifest.gas_estimate_connector is None:
            continue
        connector = connector_manifest.gas_estimate_connector.instantiate()
        STRATEGY_GAS_ESTIMATE_REGISTRY.register(connector)


def _register_all() -> None:
    """Register every strategy-side gas-estimate connector.

    Descriptor-backed connectors are discovered here. Import targets are
    stored as strings on each connector descriptor so loading this module
    does not transitively import every provider module's class until the
    registry bootstraps.
    """
    _register_discovered_gas_estimates()


_register_all()
