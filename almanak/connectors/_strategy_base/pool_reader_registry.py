"""Strategy-side registry for connector-owned CL pool reader specs."""

from __future__ import annotations

from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec

__all__ = ["POOL_READER_REGISTRY", "PoolReaderRegistry", "PoolReaderRegistryError"]


class PoolReaderRegistryError(Exception):
    """Registry contract violation."""


class PoolReaderRegistry:
    """In-process registry of protocol key to pool reader spec."""

    def __init__(self) -> None:
        self._specs: dict[str, PoolReaderSpec] = {}

    def register(self, spec: PoolReaderSpec) -> None:
        """Register a connector-owned pool reader spec."""
        if not isinstance(spec, PoolReaderSpec):
            raise PoolReaderRegistryError(
                f"register() expects a PoolReaderSpec instance, got {type(spec).__qualname__}"
            )
        for key in spec.keys:
            key_lower = key.lower()
            existing = self._specs.get(key_lower)
            if existing is not None:
                if existing is spec or existing.protocol.lower() == spec.protocol.lower():
                    continue
                raise PoolReaderRegistryError(
                    f"pool reader key {key!r} already registered by {existing.protocol!r}; "
                    f"refusing to overwrite with {spec.protocol!r}"
                )
            self._specs[key_lower] = spec

    def lookup(self, protocol: str) -> PoolReaderSpec | None:
        """Return the pool reader spec for ``protocol`` if one exists."""
        return self._specs.get(protocol.lower())

    def require(self, protocol: str) -> PoolReaderSpec:
        """Return the pool reader spec for ``protocol`` or raise."""
        spec = self.lookup(protocol)
        if spec is None:
            raise PoolReaderRegistryError(f"protocol {protocol!r} does not publish a pool reader spec")
        return spec

    def all(self) -> tuple[PoolReaderSpec, ...]:
        """Return unique specs in registration order."""
        specs: list[PoolReaderSpec] = []
        seen: set[str] = set()
        for spec in self._specs.values():
            protocol_key = spec.protocol.lower()
            if protocol_key in seen:
                continue
            specs.append(spec)
            seen.add(protocol_key)
        return tuple(specs)

    def clear(self) -> None:
        """Test helper: clear all registrations."""
        self._specs.clear()


POOL_READER_REGISTRY: PoolReaderRegistry = PoolReaderRegistry()
