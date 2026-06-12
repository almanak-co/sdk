"""Strategy-side yield-poke registration site.

Iterates the connector registry and registers every connector that declares
``CONNECTOR.yield_poke`` into ``YIELD_POKE_REGISTRY``. The registration runs
eagerly at import time so that ``YieldPoker._derived_chain_protocol_map()``
receives a fully-populated registry on first call.

This module is imported lazily by ``yield_poker._derived_chain_protocol_map()``
— it must NOT be imported at the top-level of any framework module that is
imported at process start, to keep the connector-registry walk off the hot path.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.yield_poke_registry import YIELD_POKE_REGISTRY

__all__ = ["YIELD_POKE_REGISTRY"]


def _register_discovered_yield_pokes() -> None:
    """Register yield-poke functions published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_yield_poke():
        if connector_manifest.yield_poke is None:
            continue
        # Use .load() to get the callable reference without calling it;
        # .instantiate() would call the poke function immediately.
        poke_fn = connector_manifest.yield_poke.poke.load()
        if not callable(poke_fn):
            from almanak.connectors._connector_descriptor import ConnectorDiscoveryError

            raise ConnectorDiscoveryError(
                f"{connector_manifest.name}.yield_poke.poke must resolve to a callable, "
                f"got {type(poke_fn).__qualname__}"
            )
        for chain in connector_manifest.yield_poke.chains:
            YIELD_POKE_REGISTRY.register(chain, connector_manifest.name, poke_fn)


def _register_all() -> None:
    """Register every descriptor-backed yield-poke function."""
    _register_discovered_yield_pokes()


_register_all()
