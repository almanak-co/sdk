"""Strategy-side swap-classification registration site (VIB-4928 PR-3b).

Sibling of :mod:`almanak.connectors._strategy_contract_role_registry`, scoped to
the swap-router-ABI + fee-tier classification concern. The intent compiler's
five swap-classification symbols (``SWAP_FEE_TIERS``, ``DEFAULT_SWAP_FEE_TIER``,
``SWAP_ROUTER_V1_PROTOCOLS``, ``SWAP_ROUTER_V1_CHAIN_OVERRIDES``,
``SWAP_ROUTER_ALGEBRA_PROTOCOLS``) fan out over this registry instead of
hand-importing each DEX connector's ``swap_classification`` module. Adding a
V3-fork or Algebra DEX means one ``swap_classification.py`` data module plus a
``CONNECTOR.swap_classification`` import reference in the connector's own
manifest, with no edits across ``compiler_constants.py``.

Lives one level up from ``_strategy_base/`` because it owns strategy-side
registry bootstrap; ``_strategy_base/`` stays protocol-clean (no concrete
connector imports). This file imports only connector manifests.

Registration order is NOT load-bearing for the derived symbols. Every consumer
does ``protocol in X`` / ``X.get(protocol)``, never an ordered iteration (unlike
the contract-role address tables). The order below mirrors the pre-PR-3b
``_swap_constants_sources`` order only for diagnostic determinism.

The completeness invariant: every connector shipping a ``swap_classification``
module MUST publish it from its manifest. This is enforced by
``tests/unit/connectors/test_swap_classification_registry_completeness.py``.

Gateway-boundary note: this module is strategy-side and performs no network
egress. The connector ``swap_classification`` modules it loads are pure data.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY, ConnectorDiscoveryError, ImportRef
from almanak.connectors._strategy_base.swap_classification_registry import (
    SWAP_CLASSIFICATION_REGISTRY,
    SwapClassificationRegistry,
    SwapClassificationSpec,
)

__all__ = [
    "SWAP_CLASSIFICATION_REGISTRY",
    "SwapClassificationRegistry",
    "SwapClassificationSpec",
]


def _ordered_refs(refs: list[ImportRef]) -> list[ImportRef]:
    """Return import refs in explicit order, with unordered refs last."""
    return sorted(refs, key=lambda ref: (ref.order is None, ref.order or 0))


def _load_specs(import_ref: ImportRef) -> tuple[SwapClassificationSpec, ...]:
    """Load and validate one connector-owned swap-classification spec tuple."""
    specs = import_ref.load()
    if not isinstance(specs, tuple):
        raise ConnectorDiscoveryError(
            f"{import_ref.module}.{import_ref.attribute} must be a tuple[SwapClassificationSpec, ...], "
            f"got {type(specs).__qualname__}"
        )
    bad_specs = [spec for spec in specs if not isinstance(spec, SwapClassificationSpec)]
    if bad_specs:
        raise ConnectorDiscoveryError(
            f"{import_ref.module}.{import_ref.attribute} must contain only SwapClassificationSpec values, "
            f"got {[type(spec).__qualname__ for spec in bad_specs]!r}"
        )
    return specs


def _register_all() -> None:
    """Register every connector-owned swap classification.

    Descriptor-backed connectors are discovered here. Import targets are stored
    as strings on each connector descriptor so loading this module does not
    transitively import every connector's data module until registry bootstrap.
    """
    refs: list[ImportRef] = []
    for connector_manifest in CONNECTOR_REGISTRY.with_swap_classification():
        if connector_manifest.swap_classification is None:
            continue
        refs.append(connector_manifest.swap_classification)

    for import_ref in _ordered_refs(refs):
        specs = _load_specs(import_ref)
        for spec in specs:
            SWAP_CLASSIFICATION_REGISTRY.register(spec)


_register_all()
