"""Strategy-side pool reader registration site."""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors._strategy_base.pool_reader_registry import POOL_READER_REGISTRY

__all__ = ["POOL_READER_REGISTRY"]


def _iter_specs(value: object) -> tuple[PoolReaderSpec, ...]:
    if isinstance(value, PoolReaderSpec):
        return (value,)
    if isinstance(value, tuple) and all(isinstance(item, PoolReaderSpec) for item in value):
        return value
    raise TypeError(f"pool_reader ImportRef must load PoolReaderSpec or tuple[PoolReaderSpec, ...], got {value!r}")


def _register_discovered_pool_readers() -> None:
    """Register pool reader specs published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_pool_reader():
        pool_reader_ref = connector_manifest.pool_reader
        assert pool_reader_ref is not None
        for spec in _iter_specs(pool_reader_ref.load()):
            POOL_READER_REGISTRY.register(spec)


def _register_all() -> None:
    """Register every descriptor-backed pool reader spec."""
    _register_discovered_pool_readers()


_register_all()
