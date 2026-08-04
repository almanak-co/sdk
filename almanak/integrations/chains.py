"""Integration-facing projections of chain-owned provider identifiers."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from typing import Any, TypeVar, overload

from almanak.core.chains._helpers import external_chain_id_for

from ._base import INTEGRATION_REGISTRY

_DefaultT = TypeVar("_DefaultT")


class _LazyRegistryMapping[KeyT](dict[KeyT, str]):
    """Import-safe ``dict`` compatibility surface materialized on first use."""

    def __init__(self, loader: Callable[[], dict[KeyT, str]]) -> None:
        super().__init__()
        self._loader = loader
        self._loaded = False

    def _materialize(self) -> None:
        if not self._loaded:
            super().update(self._loader())
            self._loaded = True

    def __getitem__(self, key: KeyT) -> str:
        self._materialize()
        return super().__getitem__(key)

    def __iter__(self) -> Iterator[KeyT]:
        self._materialize()
        return super().__iter__()

    def __len__(self) -> int:
        self._materialize()
        return super().__len__()

    def __contains__(self, key: object) -> bool:
        self._materialize()
        return super().__contains__(key)

    @overload
    def get(self, key: KeyT, default: None = None, /) -> str | None: ...

    @overload
    def get(self, key: KeyT, default: str, /) -> str: ...

    @overload
    def get(self, key: KeyT, default: _DefaultT, /) -> str | _DefaultT: ...

    def get(self, key: KeyT, default: _DefaultT | None = None, /) -> str | _DefaultT | None:
        self._materialize()
        return super().get(key, default)

    def keys(self) -> Any:
        self._materialize()
        return super().keys()

    def items(self) -> Any:
        self._materialize()
        return super().items()

    def values(self) -> Any:
        self._materialize()
        return super().values()

    def copy(self) -> dict[KeyT, str]:
        self._materialize()
        return super().copy()

    def __repr__(self) -> str:
        self._materialize()
        return super().__repr__()

    def __eq__(self, other: object) -> bool:
        self._materialize()
        return super().__eq__(other)


def integration_chain_id(chain: str, integration: str) -> str | None:
    """Resolve a chain-owned provider identifier through its integration."""
    if not chain or not integration:
        return None
    try:
        INTEGRATION_REGISTRY.get(integration)
        return external_chain_id_for(chain, integration)
    except KeyError:
        return None


def integration_chain_map(integration: str) -> dict[str, str]:
    """Return a copy of one chain-owned provider-id projection."""
    if not integration:
        return {}
    try:
        return INTEGRATION_REGISTRY.chain_id_map(integration)
    except KeyError:
        return {}


def integration_market_symbol(integration: str, base: str, quote: str) -> str | None:
    """Resolve a provider-native market symbol from a canonical pair."""
    if not integration or not base or not quote:
        return None
    try:
        symbols = INTEGRATION_REGISTRY.market_symbol_map(integration)
    except KeyError:
        return None
    return symbols.get((base.strip().upper(), quote.strip().upper()))


def integration_market_symbol_map() -> dict[tuple[str, str, str], str]:
    """Materialize all manifest-owned CEX market symbols for compatibility."""
    return {
        (integration.name, base, quote): symbol
        for integration in INTEGRATION_REGISTRY.all()
        for (base, quote), symbol in (integration.market_symbols or {}).items()
    }


def lazy_integration_market_symbol_map() -> Mapping[tuple[str, str, str], str]:
    """Return a mapping that defers provider discovery until first access."""
    return _LazyRegistryMapping(integration_market_symbol_map)


def lazy_integration_asset_id_map(integration: str) -> Mapping[str, str]:
    """Return a provider asset-id mapping that discovers lazily."""
    return _LazyRegistryMapping(lambda: INTEGRATION_REGISTRY.asset_id_map(integration))


__all__ = [
    "integration_chain_id",
    "integration_chain_map",
    "integration_market_symbol",
    "integration_market_symbol_map",
    "lazy_integration_asset_id_map",
    "lazy_integration_market_symbol_map",
]
