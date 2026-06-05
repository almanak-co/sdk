"""Strategy-side protocol metadata registry.

Protocol connectors may publish static metadata that framework data layers need
without the framework importing a concrete connector module. The current primary
consumer is Pendle's PT/YT token and market metadata; the shapes are kept
generic so other protocols with synthetic assets can opt in without another
central registry.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, TypeVar, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "PROTOCOL_METADATA_REGISTRY",
    "MarketMintMetadata",
    "ProtocolMarketMetadata",
    "ProtocolMetadataCapability",
    "ProtocolMetadataConnector",
    "ProtocolMetadataRegistry",
    "ProtocolMetadataRegistryError",
    "ProtocolTokenMetadata",
]


@dataclass(frozen=True)
class ProtocolTokenMetadata:
    """Connector-owned synthetic token metadata."""

    protocol: str
    chain: str
    symbol: str
    address: str
    decimals: int
    family: str


@dataclass(frozen=True)
class ProtocolMarketMetadata:
    """Connector-owned market address keyed by a protocol token."""

    protocol: str
    chain: str
    token_symbol: str
    market_address: str
    family: str


@dataclass(frozen=True)
class MarketMintMetadata:
    """Connector-owned market -> mint token metadata."""

    protocol: str
    chain: str
    market_address: str
    mint_token_address: str


class ProtocolMetadataRegistryError(Exception):
    """Registry contract violation."""


@runtime_checkable
class ProtocolMetadataCapability(Protocol):
    """Connector publishes static protocol metadata.

    Methods are pure and metadata-only: no RPC, no signing, no gateway channel.
    """

    def synthetic_tokens(self) -> tuple[ProtocolTokenMetadata, ...]: ...

    def market_tokens(self) -> tuple[ProtocolMarketMetadata, ...]: ...

    def market_mint_tokens(self) -> tuple[MarketMintMetadata, ...]: ...


class ProtocolMetadataConnector:
    """Base class for strategy-side protocol metadata connector instances."""

    protocol: ClassVar[ProtocolName]
    kind: ClassVar[ProtocolKind]


T = TypeVar("T")


class ProtocolMetadataRegistry:
    """In-process registry of strategy-side protocol metadata connectors."""

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, ProtocolMetadataConnector] = {}

    def register(self, connector: ProtocolMetadataConnector) -> None:
        """Register a connector instance. Same-type re-registration is a no-op."""
        if not isinstance(connector, ProtocolMetadataConnector):
            raise ProtocolMetadataRegistryError(
                "register() expects a ProtocolMetadataConnector instance, got "
                f"{type(connector).__qualname__!s} ({connector!r}); did you "
                "forget to instantiate the class?"
            )
        if not isinstance(connector, ProtocolMetadataCapability):
            raise ProtocolMetadataRegistryError(
                "register() expects a connector implementing ProtocolMetadataCapability "
                f"in addition to ProtocolMetadataConnector; {type(connector).__qualname__!s} "
                "is missing the required methods."
            )
        proto = connector.protocol
        existing = self._connectors.get(proto)
        if existing is not None:
            if type(existing) is type(connector):
                return
            raise ProtocolMetadataRegistryError(
                f"protocol {proto!r} already registered by "
                f"{type(existing).__qualname__}; refusing to overwrite with "
                f"{type(connector).__qualname__}"
            )
        self._connectors[proto] = connector

    def lookup(self, protocol: str) -> ProtocolMetadataCapability | None:
        """Return the metadata capability for ``protocol``, or ``None``."""
        connector = self._connectors.get(ProtocolName(protocol))
        if connector is None:
            return None
        if not isinstance(connector, ProtocolMetadataCapability):
            return None
        return connector

    def all(self) -> tuple[ProtocolMetadataConnector, ...]:
        """Return every registered connector in registration order."""
        return tuple(self._connectors.values())

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``."""
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def synthetic_tokens(self) -> tuple[ProtocolTokenMetadata, ...]:
        """Return synthetic-token metadata from every registered connector."""
        tokens: list[ProtocolTokenMetadata] = []
        for connector in self._connectors.values():
            if not isinstance(connector, ProtocolMetadataCapability):
                continue
            tokens.extend(connector.synthetic_tokens())
        return tuple(tokens)

    def clear(self) -> None:
        """Test helper: clear registrations."""
        self._connectors.clear()


PROTOCOL_METADATA_REGISTRY: ProtocolMetadataRegistry = ProtocolMetadataRegistry()
