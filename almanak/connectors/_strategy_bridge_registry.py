"""Strategy-side bridge provider registration site (VIB-4837).

Sibling of :mod:`almanak.connectors._strategy_receipt_registry` and
:mod:`almanak.connectors._strategy_flash_loan_registry`, scoped to the
bridge-provider concern.

Lives one level up from ``_strategy_base/`` because it owns strategy-side
registry bootstrap; ``_strategy_base/`` must stay protocol-clean (no concrete
connector imports). Adding a new bridge connector means one
``CONNECTOR.bridge_adapter`` import reference in the connector's own manifest.

Registration order (across, stargate) is load-bearing: it fixes the selector's
candidate order and therefore the selection for a given input. The manifest
``bridge_adapter.order`` values preserve this order.

The factory is the adapter **class** itself: bridge adapters accept
``token_resolver=`` (plus optional configuration), so
``BRIDGE_PROVIDER_REGISTRY.build_all(token_resolver)`` calls the class directly
to mint a fresh adapter per selection. No
``BridgeProviderRegistration`` dataclass is needed (unlike the flash-loan
registry) because a bridge contributes exactly one artifact: the adapter.

No completeness guard: bridge adapters are manifest-opt-in. Every connector
ships an ``adapter.py``, and ``IntentType.BRIDGE`` would falsely flag ``lifi``
because it is a swap aggregator that also declares bridge intent support. A
pure bridge connector that omits ``CONNECTOR.bridge_adapter`` is therefore
silently absent from selection; that residual risk is accepted instead of a
brittle guard that imports every connector or false-positives on aggregators.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY, ConnectorDiscoveryError, ImportRef
from almanak.connectors._strategy_base.bridge_base import BridgeAdapter
from almanak.connectors._strategy_base.bridge_registry import BRIDGE_PROVIDER_REGISTRY, BridgeAdapterFactory

__all__ = ["BRIDGE_PROVIDER_REGISTRY"]


def _ordered_refs(refs: list[tuple[str, ImportRef]]) -> list[tuple[str, ImportRef]]:
    """Return named import refs in explicit order, with unordered refs last."""
    return sorted(refs, key=lambda item: (item[1].order is None, item[1].order or 0))


def _load_factory(import_ref: ImportRef) -> BridgeAdapterFactory:
    """Load and validate one connector-owned bridge-adapter factory."""
    factory = import_ref.load()
    if not isinstance(factory, type) or not issubclass(factory, BridgeAdapter):
        got_name = factory.__qualname__ if isinstance(factory, type) else type(factory).__qualname__
        raise ConnectorDiscoveryError(
            f"{import_ref.module}.{import_ref.attribute} must be a BridgeAdapter subclass, got {got_name}"
        )
    return factory


def _register_all() -> None:
    """Register every strategy-side bridge provider.

    Descriptor-backed connectors are discovered here. Import targets are stored
    as strings on each connector descriptor so loading this module does not
    transitively import every bridge adapter until registry bootstrap.
    """
    refs: list[tuple[str, ImportRef]] = []
    for connector_manifest in CONNECTOR_REGISTRY.with_bridge_adapter():
        if connector_manifest.bridge_adapter is None:
            continue
        refs.append((connector_manifest.name, connector_manifest.bridge_adapter))

    for name, import_ref in _ordered_refs(refs):
        BRIDGE_PROVIDER_REGISTRY.register(name=name, factory=_load_factory(import_ref))


_register_all()
