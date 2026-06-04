"""Strategy-side receipt-parser connector registration site (VIB-4854 / W2).

Sibling of :mod:`almanak.connectors._gateway_registry`, scoped to the
receipt-parser concern.

Lives one level up from ``_strategy_base/`` because it owns receipt-parser
registry bootstrap, and ``_strategy_base/`` must stay protocol-clean
(no concrete connector imports). It imports only connector descriptors;
provider classes are loaded from connector-owned lazy import references.

Connectors that publish ``almanak/connectors/<protocol>/connector.py`` with
a ``CONNECTOR.receipt_parser_connector`` import reference are registered from
that connector object::

    CONNECTOR = Connector(
        name="<protocol>",
        kind=ProtocolKind.<KIND>,
        receipt_parser_connector=ImportRef(
            module="almanak.connectors.<protocol>.receipt_parser_provider",
            attribute="<Protocol>ReceiptParserConnector",
        ),
    )

The completeness invariant: every connector that ships a
``receipt_parser.py`` file MUST register through its descriptor. This is enforced statically
by ``tests/unit/core/test_receipt_parser_registry_completeness.py``.

Why a strategy-side registry (vs. reading from ``GATEWAY_REGISTRY``)
====================================================================

Receipt parsing runs inside the strategy container. The framework's
``ResultEnricher`` and migration backfill construct parser instances
at runtime and feed them already-fetched transaction receipts.
Strategy-side modules are forbidden from importing the gateway-side
registry (``almanak.connectors._gateway_registry``) per
``tests/static/test_strategy_import_boundary.py``, so the receipt
parser dispatch cannot consume ``GATEWAY_REGISTRY``. This file is the
strategy-side mirror.

This file is still allow-listed in the strategy-side import boundary scan
(``_STRATEGY_SCAN_SKIP_PARTS`` in
``tests/static/test_strategy_import_boundary.py``) as the boot-time registry
entry point, but it no longer knows connector names.
"""

from __future__ import annotations

from almanak.connectors._connector import (
    CONNECTOR_REGISTRY,
)
from almanak.connectors._strategy_base.receipt_parser_registry import (
    STRATEGY_RECEIPT_PARSER_REGISTRY,
)

__all__ = ["STRATEGY_RECEIPT_PARSER_REGISTRY"]


def _register_discovered_receipt_parsers() -> None:
    """Register receipt-parser connectors published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_receipt_parser():
        if connector_manifest.receipt_parser_connector is None:
            continue
        connector = connector_manifest.receipt_parser_connector.instantiate()
        STRATEGY_RECEIPT_PARSER_REGISTRY.register(connector)


def _register_all() -> None:
    """Register every strategy-side receipt-parser connector.

    Descriptor-backed connectors are discovered here. Import targets are
    stored as strings on each connector descriptor so loading this module does
    not transitively import every parser module's class until the registry
    resolves a key.
    """
    _register_discovered_receipt_parsers()


_register_all()
