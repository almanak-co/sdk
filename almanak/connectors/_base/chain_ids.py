"""Read-only EVM chain-ID views derived from connector support metadata."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Protocol

from almanak.core.chains import ChainRegistry

__all__ = ["chain_ids_from_registered_ids", "chain_ids_from_supported_chains", "chain_names_by_id"]


class _SupportedChainsView(Protocol):
    """Minimal cross-boundary view of ``SupportedChainsSpec``."""

    def all_chains(self) -> tuple[str, ...]: ...


def chain_ids_from_supported_chains(supported_chains: _SupportedChainsView | None) -> Mapping[str, int]:
    """Project connector support metadata to an immutable EVM chain-ID map.

    ``SupportedChainsSpec`` remains strategy-owned metadata, so this helper
    depends only on its tiny public view.  Gateway and strategy compatibility
    surfaces can therefore share the projection without duplicating chain IDs.
    """
    if supported_chains is None:
        raise ValueError("connector must declare supported_chains before deriving chain IDs")

    chain_ids: dict[str, int] = {}
    ids_seen: dict[int, str] = {}
    for chain in supported_chains.all_chains():
        descriptor = ChainRegistry.try_resolve(chain)
        if descriptor is None:
            raise ValueError(f"supported chain {chain!r} is not registered")
        if descriptor.chain_id == 0:
            raise ValueError(f"supported chain {chain!r} does not have an EVM chain ID")
        previous = ids_seen.get(descriptor.chain_id)
        if previous is not None:
            raise ValueError(
                f"supported chains {previous!r} and {descriptor.name!r} share chain ID {descriptor.chain_id}"
            )
        chain_ids[descriptor.name] = descriptor.chain_id
        ids_seen[descriptor.chain_id] = descriptor.name
    return MappingProxyType(chain_ids)


def chain_ids_from_registered_ids(chain_ids: Iterable[int]) -> Mapping[str, int]:
    """Project a protocol deployment's numeric IDs through ChainRegistry.

    Some integration APIs expose more deployed chains than the connector
    advertises as end-to-end strategy support. Their deployment/address table
    owns that broader protocol surface; the registry still owns every numeric
    ID and silently excludes deployments outside the SDK chain inventory.
    """
    registered: dict[str, int] = {}
    for chain_id in chain_ids:
        descriptor = ChainRegistry.try_resolve_id(chain_id)
        if descriptor is not None:
            registered[descriptor.name] = descriptor.chain_id
    return MappingProxyType(registered)


def chain_names_by_id(chain_ids: Mapping[str, int]) -> Mapping[int, str]:
    """Return an immutable inverse of a canonical chain-name map."""
    names = {chain_id: chain for chain, chain_id in chain_ids.items()}
    if len(names) != len(chain_ids):
        raise ValueError("cannot invert chain-ID map with duplicate IDs")
    return MappingProxyType(names)
