"""Registry for connector-owned protocol-neutral pool-data declarations."""

from __future__ import annotations

from .pool_data import PoolDataFacet, PoolDataSource, PoolDataSpec

__all__ = ["POOL_DATA_REGISTRY", "PoolDataRegistry", "PoolDataRegistryError"]


class PoolDataRegistryError(Exception):
    """Pool-data declaration collision or lookup failure."""


class PoolDataRegistry:
    """Case-insensitive registry keyed by canonical protocol and aliases."""

    def __init__(self) -> None:
        self._specs: dict[str, PoolDataSpec] = {}

    def register(self, spec: PoolDataSpec) -> None:
        if not isinstance(spec, PoolDataSpec):
            raise PoolDataRegistryError(f"register() expects PoolDataSpec, got {type(spec).__qualname__}")
        for key in spec.keys:
            existing = self._specs.get(key)
            if existing is not None and existing is not spec:
                raise PoolDataRegistryError(
                    f"pool-data key {key!r} is already registered by {existing.protocol!r}; "
                    f"refusing to overwrite with {spec.protocol!r}"
                )
            self._specs[key] = spec

    def lookup(self, protocol: str) -> PoolDataSpec | None:
        return self._specs.get(protocol.strip().lower().replace("-", "_"))

    def require(self, protocol: str) -> PoolDataSpec:
        spec = self.lookup(protocol)
        if spec is None:
            raise PoolDataRegistryError(f"protocol {protocol!r} does not publish pool-data capabilities")
        return spec

    def supports(self, protocol: str, facet: PoolDataFacet) -> bool:
        spec = self.lookup(protocol)
        return spec is not None and spec.supports(facet)

    def source_for(self, protocol: str, facet: PoolDataFacet) -> PoolDataSource | None:
        """Return the executable lane bound to ``facet``, if any."""
        spec = self.lookup(protocol)
        return spec.source_for(facet) if spec is not None else None

    def supports_from(self, protocol: str, facet: PoolDataFacet, source: PoolDataSource) -> bool:
        """Whether ``protocol`` binds ``facet`` to the requested lane."""
        return self.source_for(protocol, facet) is source

    def unsupported_reason(self, protocol: str, facet: PoolDataFacet) -> str | None:
        spec = self.lookup(protocol)
        if spec is None:
            return "connector does not publish pool-data capabilities"
        return spec.unsupported_reason(facet)

    def all(self) -> tuple[PoolDataSpec, ...]:
        result: list[PoolDataSpec] = []
        seen: set[int] = set()
        for spec in self._specs.values():
            if id(spec) in seen:
                continue
            seen.add(id(spec))
            result.append(spec)
        return tuple(result)

    def clear(self) -> None:
        self._specs.clear()


POOL_DATA_REGISTRY = PoolDataRegistry()
