"""Connector-owned pool reader specification types."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from almanak.connectors._strategy_base.v3_pool_abi import V3_GET_POOL_SELECTOR

KnownPoolKey = tuple[str, str, int]
KnownPoolsByChain = Mapping[str, Mapping[KnownPoolKey, str]]

# Canonical Uniswap fee tiers — the ``candidate_pool_keys`` fallback for any
# v3-family protocol whose spec (or connector) does not declare its own set.
DEFAULT_CANDIDATE_POOL_KEYS: tuple[int, ...] = (100, 500, 3000, 10000)


@dataclass(frozen=True)
class PoolReaderSpec:
    """Static inputs needed to build a generic pool reader for a protocol."""

    protocol: str
    factory_addresses: Mapping[str, str]
    known_pools: KnownPoolsByChain = field(default_factory=dict)
    get_pool_selector: str = V3_GET_POOL_SELECTOR
    aliases: tuple[str, ...] = field(default_factory=tuple)
    # ``factory.getPool()`` third-arg candidates swept by best-pool resolution
    # (VIB-4924 C1): fee tiers for the uint24 v3 family, tick spacings for the
    # int24 Slipstream family. Default = the canonical Uniswap fee tiers.
    candidate_pool_keys: tuple[int, ...] = DEFAULT_CANDIDATE_POOL_KEYS
    # Read-shape discriminator: which framework reader implementation can read
    # this protocol's pools. ``"v3_slot0"`` is the Uniswap-V3 slot0() family
    # (all v3 forks + Slipstream); protocols with a different on-chain shape
    # (e.g. Curve's get_dy/coins ABI) declare their own kind and the framework
    # maps each kind to a reader class. A spec whose kind the framework does
    # not know fails loudly at registry construction — it is a manifest bug,
    # never a silent mis-read.
    reader_kind: str = "v3_slot0"

    @property
    def keys(self) -> tuple[str, ...]:
        """Return canonical protocol plus any lookup aliases."""
        return (self.protocol, *self.aliases)


__all__ = ["DEFAULT_CANDIDATE_POOL_KEYS", "KnownPoolKey", "KnownPoolsByChain", "PoolReaderSpec"]
