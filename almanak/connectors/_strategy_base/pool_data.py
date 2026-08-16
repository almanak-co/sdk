"""Protocol-neutral pool identity, capabilities, and reader contracts.

Pool protocols do not share one ABI or even one identifier shape.  A V3 pool
is an EVM contract, a V4 pool is a ``bytes32`` PoolId, Balancer keys state by a
Vault pool id, and Solana pool accounts are base58 public keys.  This module
therefore models the common contract (identity, assets, observations, and
provenance) without prescribing a two-token/V3 state layout.

Connector manifests publish :class:`PoolDataSpec` values.  A facet is either
bound to an executable framework/gateway lane or carries an explicit
unsupported reason; absence is never treated as support.  This lets a
connector expose live pricing, archive state, or native TWAP independently
without pretending that every pool family shares one reader implementation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from types import MappingProxyType

from almanak.connectors._base.types import ProtocolName
from almanak.core.chains import ChainDescriptor, ChainRegistry

from .pool_reader import PoolReaderSpec

_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
_EVM_POOL_ID_RE = re.compile(r"^0x[0-9a-f]{64}$")
_SOLANA_ACCOUNT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


class PoolReferenceKind(Enum):
    """Wire-independent shape of a protocol's canonical pool identifier."""

    EVM_CONTRACT = "evm_contract"
    EVM_POOL_ID = "evm_pool_id"
    SOLANA_ACCOUNT = "solana_account"


class PoolDataFacet(Enum):
    """Independently implementable pool-data operations."""

    METADATA = "metadata"
    BALANCES = "balances"
    SPOT_PRICE = "spot_price"
    LIQUIDITY = "liquidity"
    HISTORICAL_STATE = "historical_state"
    TWAP = "twap"
    TICK_LIQUIDITY = "tick_liquidity"


class PoolDataSource(Enum):
    """Executable framework/gateway lane bound to a supported facet."""

    LIVE_PRICE_READER = "live_price_reader"
    GATEWAY_POOL_STATE = "gateway_pool_state"
    GATEWAY_TWAP = "gateway_twap"


_ALLOWED_FACETS_BY_SOURCE: Mapping[PoolDataSource, frozenset[PoolDataFacet]] = MappingProxyType(
    {
        PoolDataSource.LIVE_PRICE_READER: frozenset(
            {
                PoolDataFacet.SPOT_PRICE,
                PoolDataFacet.LIQUIDITY,
                PoolDataFacet.TICK_LIQUIDITY,
            }
        ),
        PoolDataSource.GATEWAY_POOL_STATE: frozenset(
            {
                PoolDataFacet.METADATA,
                PoolDataFacet.BALANCES,
                PoolDataFacet.SPOT_PRICE,
                PoolDataFacet.LIQUIDITY,
                PoolDataFacet.HISTORICAL_STATE,
                PoolDataFacet.TICK_LIQUIDITY,
            }
        ),
        PoolDataSource.GATEWAY_TWAP: frozenset({PoolDataFacet.TWAP}),
    }
)


def _normalize_pool_data_identity(protocol: str, aliases: tuple[str, ...]) -> tuple[str, tuple[str, ...]]:
    normalized_protocol = protocol.strip().lower().replace("-", "_")
    normalized_aliases = tuple(alias.strip().lower().replace("-", "_") for alias in aliases)
    if not normalized_protocol or any(not alias for alias in normalized_aliases):
        raise ValueError("PoolDataSpec protocol and aliases must be non-empty")
    keys = (normalized_protocol, *normalized_aliases)
    if len(set(keys)) != len(keys):
        raise ValueError("PoolDataSpec protocol and aliases must be unique")
    return normalized_protocol, normalized_aliases


def _validate_facet_classification(
    bindings: Mapping[PoolDataFacet, PoolDataSource],
    unsupported: Mapping[PoolDataFacet, str],
) -> None:
    if any(not isinstance(facet, PoolDataFacet) for facet in bindings.keys() | unsupported.keys()):
        raise TypeError("PoolDataSpec facets must be PoolDataFacet values")
    if any(not isinstance(source, PoolDataSource) for source in bindings.values()):
        raise TypeError("PoolDataSpec bindings must use PoolDataSource values")
    overlap = bindings.keys() & unsupported.keys()
    if overlap:
        raise ValueError(
            f"PoolDataSpec facets cannot be both supported and unsupported: {sorted(f.value for f in overlap)}"
        )
    missing = set(PoolDataFacet) - bindings.keys() - unsupported.keys()
    if missing:
        raise ValueError(f"PoolDataSpec must classify every facet; missing: {sorted(f.value for f in missing)}")
    if any(not isinstance(reason, str) or not reason.strip() for reason in unsupported.values()):
        raise ValueError("PoolDataSpec unsupported facets require non-empty reasons")


def _validate_price_reader(
    protocol: str,
    aliases: tuple[str, ...],
    bindings: Mapping[PoolDataFacet, PoolDataSource],
    price_reader: PoolReaderSpec | None,
) -> None:
    live_facets = {facet for facet, source in bindings.items() if source is PoolDataSource.LIVE_PRICE_READER}
    if live_facets and price_reader is None:
        raise ValueError("PoolDataSpec LIVE_PRICE_READER bindings require price_reader")
    if price_reader is None:
        return
    if price_reader.protocol != protocol or price_reader.aliases != aliases:
        raise ValueError("PoolDataSpec price_reader identity must match protocol and aliases")
    if PoolDataFacet.SPOT_PRICE not in live_facets:
        raise ValueError("PoolDataSpec with price_reader must bind SPOT_PRICE to LIVE_PRICE_READER")


def _validate_source_bindings(bindings: Mapping[PoolDataFacet, PoolDataSource]) -> None:
    invalid_bindings = {
        facet: source for facet, source in bindings.items() if facet not in _ALLOWED_FACETS_BY_SOURCE[source]
    }
    if invalid_bindings:
        raise ValueError(f"PoolDataSpec has invalid facet/source bindings: {invalid_bindings}")


@dataclass(frozen=True, slots=True)
class PoolRef:
    """Canonical pool identity: chain, protocol, identifier kind, and value."""

    chain: ChainDescriptor
    protocol: ProtocolName
    kind: PoolReferenceKind
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.chain, ChainDescriptor):
            raise TypeError("PoolRef.chain must be a ChainDescriptor")
        protocol = str(self.protocol).strip().lower().replace("-", "_")
        if not protocol:
            raise ValueError("PoolRef.protocol is required")
        value = self.value.strip()
        normalized = value.lower() if self.kind is not PoolReferenceKind.SOLANA_ACCOUNT else value
        validators = {
            PoolReferenceKind.EVM_CONTRACT: _EVM_ADDRESS_RE,
            PoolReferenceKind.EVM_POOL_ID: _EVM_POOL_ID_RE,
            PoolReferenceKind.SOLANA_ACCOUNT: _SOLANA_ACCOUNT_RE,
        }
        if validators[self.kind].fullmatch(normalized) is None:
            raise ValueError(f"invalid {self.kind.value} pool reference: {self.value!r}")
        object.__setattr__(self, "protocol", ProtocolName(protocol))
        object.__setattr__(self, "value", normalized)

    @classmethod
    def parse(
        cls,
        *,
        chain: str | ChainDescriptor,
        protocol: str | ProtocolName,
        kind: PoolReferenceKind,
        value: str,
    ) -> PoolRef:
        """Resolve a public chain/protocol input into a typed pool reference."""
        return cls(
            chain=ChainRegistry.get(chain),
            protocol=ProtocolName(str(protocol)),
            kind=kind,
            value=value,
        )

    @property
    def key(self) -> tuple[str, str, str, str]:
        return self.chain.name, str(self.protocol), self.kind.value, self.value


@dataclass(frozen=True, slots=True)
class PoolAsset:
    """One ordered asset in a pool; no binary-pair assumption is made."""

    identifier: str
    decimals: int
    index: int
    symbol: str | None = None

    def __post_init__(self) -> None:
        identifier = self.identifier.strip()
        if not identifier:
            raise ValueError("PoolAsset.identifier is required")
        if not 0 <= self.decimals <= 255:
            raise ValueError("PoolAsset.decimals must be in [0, 255]")
        if self.index < 0:
            raise ValueError("PoolAsset.index must be non-negative")
        if self.symbol is not None and not self.symbol.strip():
            raise ValueError("PoolAsset.symbol must be non-empty when provided")
        object.__setattr__(self, "identifier", identifier)


@dataclass(frozen=True, slots=True)
class PoolMetadata:
    """Immutable connector-authenticated identity for an arbitrary pool."""

    ref: PoolRef
    assets: tuple[PoolAsset, ...]
    provenance: str
    fee_rate: Decimal | None = None
    owner: str | None = None
    attributes: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.assets:
            raise ValueError("PoolMetadata.assets must not be empty")
        indexes = tuple(asset.index for asset in self.assets)
        if indexes != tuple(range(len(self.assets))):
            raise ValueError("PoolMetadata.assets must use contiguous canonical indexes")
        if len({asset.identifier for asset in self.assets}) != len(self.assets):
            raise ValueError("PoolMetadata.assets contains duplicate identifiers")
        if not self.provenance.strip():
            raise ValueError("PoolMetadata.provenance is required")
        if self.fee_rate is not None and not Decimal("0") <= self.fee_rate < Decimal("1"):
            raise ValueError("PoolMetadata.fee_rate must be in [0, 1) when measured")
        normalized_attributes = {str(key).strip(): str(value).strip() for key, value in self.attributes.items()}
        if any(not key for key in normalized_attributes):
            raise ValueError("PoolMetadata.attributes keys must be non-empty")
        object.__setattr__(self, "provenance", self.provenance.strip())
        object.__setattr__(self, "attributes", MappingProxyType(normalized_attributes))


@dataclass(frozen=True, slots=True)
class PoolStateObservation:
    """Block-anchored state with N-asset balances and connector extensions."""

    metadata: PoolMetadata
    timestamp: int
    block_number: int
    balances_raw: tuple[int | None, ...]
    spot_price: Decimal | None = None
    state: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.timestamp <= 0 or self.block_number <= 0:
            raise ValueError("PoolStateObservation requires positive timestamp and block number")
        if len(self.balances_raw) != len(self.metadata.assets):
            raise ValueError("PoolStateObservation balances must align with metadata assets")
        if any(balance is not None and balance < 0 for balance in self.balances_raw):
            raise ValueError("PoolStateObservation balances must be non-negative when measured")
        if self.spot_price is not None and self.spot_price <= 0:
            raise ValueError("PoolStateObservation.spot_price must be positive when measured")
        normalized_state = {str(key).strip(): str(value).strip() for key, value in self.state.items()}
        if any(not key for key in normalized_state):
            raise ValueError("PoolStateObservation.state keys must be non-empty")
        object.__setattr__(self, "state", MappingProxyType(normalized_state))


@dataclass(frozen=True, slots=True)
class PoolPriceObservation:
    """Oriented price observation for two assets within a possibly N-asset pool."""

    ref: PoolRef
    base_asset: str
    quote_asset: str
    price: Decimal
    timestamp: int
    block_number: int
    provenance: str

    def __post_init__(self) -> None:
        if not self.base_asset.strip() or not self.quote_asset.strip():
            raise ValueError("PoolPriceObservation requires base and quote assets")
        if self.base_asset == self.quote_asset:
            raise ValueError("PoolPriceObservation base and quote assets must differ")
        if self.price <= 0 or self.timestamp <= 0 or self.block_number <= 0:
            raise ValueError("PoolPriceObservation price, timestamp, and block must be positive")
        if not self.provenance.strip():
            raise ValueError("PoolPriceObservation.provenance is required")


@dataclass(frozen=True, slots=True)
class PoolDataSpec:
    """Connector manifest declaration for one pool protocol surface."""

    protocol: str
    reference_kind: PoolReferenceKind
    bindings: Mapping[PoolDataFacet, PoolDataSource]
    unsupported: Mapping[PoolDataFacet, str]
    aliases: tuple[str, ...] = ()
    price_reader: PoolReaderSpec | None = None

    def __post_init__(self) -> None:
        protocol, aliases = _normalize_pool_data_identity(self.protocol, self.aliases)
        if not isinstance(self.reference_kind, PoolReferenceKind):
            raise TypeError("PoolDataSpec.reference_kind must be PoolReferenceKind")
        bindings = dict(self.bindings)
        unsupported = dict(self.unsupported)
        _validate_facet_classification(bindings, unsupported)
        _validate_price_reader(protocol, aliases, bindings, self.price_reader)
        _validate_source_bindings(bindings)
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "aliases", aliases)
        object.__setattr__(self, "bindings", MappingProxyType(bindings))
        object.__setattr__(
            self,
            "unsupported",
            MappingProxyType({facet: reason.strip() for facet, reason in unsupported.items()}),
        )

    @property
    def keys(self) -> tuple[str, ...]:
        return self.protocol, *self.aliases

    @property
    def supported(self) -> frozenset[PoolDataFacet]:
        return frozenset(self.bindings)

    def supports(self, facet: PoolDataFacet) -> bool:
        return facet in self.bindings

    def source_for(self, facet: PoolDataFacet) -> PoolDataSource | None:
        return self.bindings.get(facet)

    def unsupported_reason(self, facet: PoolDataFacet) -> str | None:
        return self.unsupported.get(facet)


def unsupported_pool_data_spec(
    *,
    protocol: str,
    reference_kind: PoolReferenceKind,
    reason: str,
    aliases: tuple[str, ...] = (),
) -> PoolDataSpec:
    """Declare an inventoried pool surface whose generic readers are not wired.

    This is deliberately a declaration, not a fallback implementation.  It
    lets preflight report a connector-owned reason for every requested facet
    and gives the inventory test a closed migration target.
    """
    if not reason.strip():
        raise ValueError("unsupported pool-data declaration requires a reason")
    return PoolDataSpec(
        protocol=protocol,
        aliases=aliases,
        reference_kind=reference_kind,
        bindings={},
        unsupported=dict.fromkeys(PoolDataFacet, reason),
    )


__all__ = [
    "PoolAsset",
    "PoolDataFacet",
    "PoolDataSource",
    "PoolDataSpec",
    "PoolMetadata",
    "PoolPriceObservation",
    "PoolRef",
    "PoolReferenceKind",
    "PoolStateObservation",
    "unsupported_pool_data_spec",
]
