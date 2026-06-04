"""Strategy-side flash-loan provider registration site (VIB-4837).

Sibling of :mod:`almanak.connectors._strategy_receipt_registry`, scoped to the
flash-loan-provider concern.

Lives one level up from ``_strategy_base/`` because it owns strategy-side
registry bootstrap; ``_strategy_base/`` must stay protocol-clean (no concrete
connector imports). Adding a new connector that provides flash loans means
``CONNECTOR.flash_loan_provider`` / ``CONNECTOR.flash_loan_builder`` import
references in the connector's own manifest.

The completeness invariant: every connector that ships a
``flash_loan_provider.py`` MUST publish it from its manifest. This is enforced
statically by
``tests/unit/connectors/test_flash_loan_registry_completeness.py``.

Registration order (aave, balancer, morpho) is load-bearing: it fixes the
selector's candidate order and the compiler's ``"Supported providers: ..."``
error string. The manifest ``flash_loan_provider.order`` values preserve this
order.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from almanak.connectors._connector import CONNECTOR_REGISTRY, Connector, ConnectorDiscoveryError, ImportRef
from almanak.connectors._strategy_base.flash_loan_base import FlashLoanProvider
from almanak.connectors._strategy_base.flash_loan_registry import (
    FLASH_LOAN_PROVIDER_REGISTRY,
    FlashLoanProviderRegistration,
)

__all__ = ["FLASH_LOAN_PROVIDER_REGISTRY"]


def _ordered_connectors(connectors: list[Connector]) -> list[Connector]:
    """Return flash-loan connector manifests in explicit order."""

    def order_key(connector: Connector) -> tuple[bool, int]:
        flash_loan_provider = connector.flash_loan_provider
        if flash_loan_provider is None:
            raise ConnectorDiscoveryError(
                f"{connector.name} was passed to flash-loan ordering without CONNECTOR.flash_loan_provider"
            )
        return (flash_loan_provider.order is None, flash_loan_provider.order or 0)

    return sorted(connectors, key=order_key)


def _load_provider(import_ref: ImportRef) -> Callable[[], FlashLoanProvider]:
    """Load and validate one connector-owned flash-loan provider class."""
    provider_cls = import_ref.load()
    if not isinstance(provider_cls, type) or not issubclass(provider_cls, FlashLoanProvider):
        got_name = provider_cls.__qualname__ if isinstance(provider_cls, type) else type(provider_cls).__qualname__
        raise ConnectorDiscoveryError(
            f"{import_ref.module}.{import_ref.attribute} must be a FlashLoanProvider subclass, got {got_name}"
        )
    return provider_cls


def _load_builder(import_ref: ImportRef) -> Callable[..., dict[str, Any]]:
    """Load and validate one connector-owned flash-loan build callable."""
    builder = import_ref.load()
    if not callable(builder):
        raise ConnectorDiscoveryError(
            f"{import_ref.module}.{import_ref.attribute} must be callable, got {type(builder).__qualname__}"
        )
    return builder


def _register_all() -> None:
    """Register every strategy-side flash-loan provider.

    Descriptor-backed connectors are discovered here. Import targets are stored
    as strings on each connector descriptor so loading this module does not
    transitively import every flash-loan provider until registry bootstrap.
    """
    connector_manifests = list(CONNECTOR_REGISTRY.with_flash_loan())
    for connector_manifest in _ordered_connectors(connector_manifests):
        if (
            connector_manifest.flash_loan_provider_name is None
            or connector_manifest.flash_loan_provider is None
            or connector_manifest.flash_loan_builder is None
        ):
            continue
        FLASH_LOAN_PROVIDER_REGISTRY.register(
            FlashLoanProviderRegistration(
                name=connector_manifest.flash_loan_provider_name,
                make_provider=_load_provider(connector_manifest.flash_loan_provider),
                build=_load_builder(connector_manifest.flash_loan_builder),
                synthetic_discovery=connector_manifest.flash_loan_synthetic_discovery,
            )
        )


_register_all()
