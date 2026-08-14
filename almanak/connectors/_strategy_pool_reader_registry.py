"""Strategy-side pool reader registration site."""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors._strategy_base.pool_reader_registry import POOL_READER_REGISTRY
from almanak.connectors._strategy_pool_data_registry import POOL_DATA_REGISTRY

__all__ = ["POOL_READER_REGISTRY"]


def _iter_specs(value: object) -> tuple[PoolReaderSpec, ...]:
    if isinstance(value, PoolReaderSpec):
        return (value,)
    if isinstance(value, tuple) and all(isinstance(item, PoolReaderSpec) for item in value):
        return value
    raise TypeError(f"pool_reader ImportRef must load PoolReaderSpec or tuple[PoolReaderSpec, ...], got {value!r}")


def _register_discovered_pool_readers() -> None:
    """Register legacy pool reader specs plus pool-data live-price projections."""
    for pool_data_spec in POOL_DATA_REGISTRY.all():
        if pool_data_spec.price_reader is not None:
            POOL_READER_REGISTRY.register(pool_data_spec.price_reader)
    for connector_manifest in CONNECTOR_REGISTRY.with_pool_reader():
        pool_reader_ref = connector_manifest.pool_reader
        assert pool_reader_ref is not None
        for spec in _iter_specs(pool_reader_ref.load()):
            POOL_READER_REGISTRY.register(spec)


def _register_all() -> None:
    """Register every descriptor-backed pool reader spec."""
    _register_discovered_pool_readers()


_register_all()
