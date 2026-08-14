"""Connector-owned pool reader specification types."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum

from almanak.connectors._connector_descriptor import ImportRef
from almanak.connectors._strategy_base.v3_pool_abi import V3_GET_POOL_SELECTOR

KnownPoolKey = tuple[str, str, int]
KnownPoolsByChain = Mapping[str, Mapping[KnownPoolKey, str]]

# Canonical Uniswap fee tiers — the ``candidate_pool_keys`` fallback for any
# v3-family protocol whose spec (or connector) does not declare its own set.
DEFAULT_CANDIDATE_POOL_KEYS: tuple[int, ...] = (100, 500, 3000, 10000)
_DEFAULT_V3_READER = ImportRef(
    module="almanak.framework.data.pools.reader",
    attribute="UniswapV3PoolPriceReader",
)


class PoolDiscriminatorKind(Enum):
    """Meaning of the integer used to distinguish pools for one asset pair.

    The value is an economic fee tier for canonical V3 and vanilla V4 pools,
    a signed tick spacing for Slipstream, and absent for families such as
    Curve.  Declaring the meaning prevents compatibility fields named
    ``fee_tier`` from being compared with a different on-chain quantity.
    """

    FEE_TIER = "fee_tier"
    TICK_SPACING = "tick_spacing"
    NONE = "none"


@dataclass(frozen=True)
class PoolReaderSpec:
    """Static inputs needed to build a live pool-price reader.

    ``reader`` is an explicit class binding.  The framework must never infer a
    reader from a family-name string: the connector owns which implementation
    can decode its on-chain shape.  Its Uniswap V3 default is retained only for
    source compatibility with external legacy specs; connector manifests in
    this repository bind their reader explicitly.
    """

    protocol: str
    factory_addresses: Mapping[str, str]
    known_pools: KnownPoolsByChain = field(default_factory=dict)
    get_pool_selector: str = V3_GET_POOL_SELECTOR
    aliases: tuple[str, ...] = field(default_factory=tuple)
    # ``factory.getPool()`` third-arg candidates swept by best-pool resolution
    # (VIB-4924 C1): fee tiers for the uint24 v3 family, tick spacings for the
    # int24 Slipstream family. Default = the canonical Uniswap fee tiers.
    candidate_pool_keys: tuple[int, ...] = DEFAULT_CANDIDATE_POOL_KEYS
    # Deprecated constructor compatibility. This field remains in its original
    # positional slot so external V3 specs keep constructing; executable
    # dispatch uses ``reader`` exclusively.
    reader_kind: str = "v3_slot0"
    reader: ImportRef = field(default_factory=lambda: _DEFAULT_V3_READER)
    discriminator_kind: PoolDiscriminatorKind = PoolDiscriminatorKind.FEE_TIER

    def __post_init__(self) -> None:
        if not isinstance(self.reader, ImportRef):
            raise TypeError("PoolReaderSpec.reader must be an ImportRef")
        if not isinstance(self.reader_kind, str) or not self.reader_kind.strip():
            raise TypeError("PoolReaderSpec.reader_kind must be a non-empty compatibility string")
        if not isinstance(self.discriminator_kind, PoolDiscriminatorKind):
            raise TypeError("PoolReaderSpec.discriminator_kind must be a PoolDiscriminatorKind")
        if self.reader == _DEFAULT_V3_READER and self.reader_kind != "v3_slot0":
            raise ValueError(
                f"legacy reader_kind={self.reader_kind!r} has no implicit class binding; "
                "pass reader=ImportRef(...) explicitly"
            )

    @property
    def keys(self) -> tuple[str, ...]:
        """Return canonical protocol plus any lookup aliases."""
        return (self.protocol, *self.aliases)


__all__ = [
    "DEFAULT_CANDIDATE_POOL_KEYS",
    "KnownPoolKey",
    "KnownPoolsByChain",
    "PoolDiscriminatorKind",
    "PoolReaderSpec",
]
