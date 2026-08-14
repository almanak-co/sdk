"""Discover connector-owned pool-data specs from connector manifests."""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.pool_data import PoolDataSpec
from almanak.connectors._strategy_base.pool_data_registry import POOL_DATA_REGISTRY

__all__ = ["POOL_DATA_REGISTRY"]


def _iter_specs(value: object) -> tuple[PoolDataSpec, ...]:
    if isinstance(value, PoolDataSpec):
        return (value,)
    if isinstance(value, tuple) and value and all(isinstance(item, PoolDataSpec) for item in value):
        return value
    raise TypeError(f"pool_data ImportRef must load PoolDataSpec or non-empty tuple[PoolDataSpec, ...], got {value!r}")


def _register_all() -> None:
    for connector in CONNECTOR_REGISTRY.with_pool_data():
        assert connector.pool_data is not None
        for spec in _iter_specs(connector.pool_data.load()):
            POOL_DATA_REGISTRY.register(spec)


_register_all()
