"""Connector-owned pool reader specification types."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from almanak.connectors._strategy_base.v3_pool_abi import V3_GET_POOL_SELECTOR

KnownPoolKey = tuple[str, str, int]
KnownPoolsByChain = Mapping[str, Mapping[KnownPoolKey, str]]


@dataclass(frozen=True)
class PoolReaderSpec:
    """Static inputs needed to build a generic CL pool reader for a protocol."""

    protocol: str
    factory_addresses: Mapping[str, str]
    known_pools: KnownPoolsByChain = field(default_factory=dict)
    get_pool_selector: str = V3_GET_POOL_SELECTOR
    aliases: tuple[str, ...] = field(default_factory=tuple)

    @property
    def keys(self) -> tuple[str, ...]:
        """Return canonical protocol plus any lookup aliases."""
        return (self.protocol, *self.aliases)


__all__ = ["KnownPoolKey", "KnownPoolsByChain", "PoolReaderSpec"]
