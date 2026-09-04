"""Strategy-side swap quote registry.

Connectors that compile swap intents can publish a protocol-owned quoter
through this registry. The quote contract is deliberately narrow: exact-input
only, base-unit amounts in and out, and no USD price dependency.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "SLIPPAGE_REFERENCE_STABLE_PARITY",
    "SLIPPAGE_REFERENCE_UNSUPPORTED",
    "SLIPPAGE_REFERENCE_V3_SPOT",
    "SWAP_QUOTE_REGISTRY",
    "SwapQuoteCapability",
    "SwapQuoteConnector",
    "SwapQuoteRegistry",
    "SwapQuoteRegistryError",
    "SwapQuoteRequest",
    "SwapQuoteResult",
    "SwapQuoteUnavailable",
]

SLIPPAGE_REFERENCE_V3_SPOT = "v3_slot0_spot"
SLIPPAGE_REFERENCE_STABLE_PARITY = "stable_parity"
SLIPPAGE_REFERENCE_UNSUPPORTED = "unsupported"


class SwapQuoteRegistryError(Exception):
    """Registry contract violation."""


class SwapQuoteUnavailable(Exception):
    """Connector could not fetch an executable quote for the request."""


_BINDING_HASH = re.compile(r"^[0-9a-f]{64}$")


def _validate_binding_hash(value: str | None) -> None:
    if value is not None and (type(value) is not str or _BINDING_HASH.fullmatch(value) is None):
        raise ValueError("venue_binding_hash must be a lowercase 64-character SHA-256 hex digest")


@dataclass(frozen=True, kw_only=True)
class SwapQuoteRequest:
    """Exact-input swap quote request.

    Amounts are token base units. ``token_in`` and ``token_out`` should be
    executable token addresses after strategy-side token resolution.
    """

    chain: str
    protocol: str
    token_in: str
    token_out: str
    amount_in: int
    token_in_symbol: str = ""
    token_out_symbol: str = ""
    token_in_decimals: int | None = None
    token_out_decimals: int | None = None
    fee_tier: int | None = None
    pool_address: str | None = None
    venue_binding_hash: str | None = None
    extra: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if self.amount_in <= 0:
            raise ValueError(f"SwapQuoteRequest.amount_in must be positive, got {self.amount_in}")
        _validate_binding_hash(self.venue_binding_hash)
        object.__setattr__(self, "extra", MappingProxyType(dict(self.extra)))


@dataclass(frozen=True, kw_only=True)
class SwapQuoteResult:
    """Exact-input quote result in token base units.

    Providers used for slippage estimation declare ``slippage_reference`` in
    metadata. Supported values are ``v3_slot0_spot`` for an exact V3-family
    pool, ``stable_parity`` for a connector-vouched pegged pair, and
    ``unsupported`` when no safe pre-trade reference is available.
    """

    amount_out: int
    source: str
    gas_estimate: int | None = None
    venue_binding_hash: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if self.amount_out <= 0:
            raise ValueError(f"SwapQuoteResult.amount_out must be positive, got {self.amount_out}")
        _validate_binding_hash(self.venue_binding_hash)
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@runtime_checkable
class SwapQuoteCapability(Protocol):
    """Connector can quote an exact-input swap against executable pool state."""

    def quote_swap(self, ctx: Any, request: SwapQuoteRequest) -> SwapQuoteResult: ...


class SwapQuoteConnector:
    """Base class for strategy-side swap quote connectors."""

    protocol: ClassVar[ProtocolName]
    protocol_aliases: ClassVar[tuple[ProtocolName, ...]] = ()
    kind: ClassVar[ProtocolKind]


T = TypeVar("T")


class SwapQuoteRegistry:
    """In-process registry of connector-owned swap quote providers."""

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, SwapQuoteConnector] = {}

    def register(self, connector: SwapQuoteConnector) -> None:
        """Register a connector instance. Same-type re-registration is a no-op."""
        if not isinstance(connector, SwapQuoteConnector):
            raise SwapQuoteRegistryError(
                "register() expects a SwapQuoteConnector instance, got "
                f"{type(connector).__qualname__!s} ({connector!r}); did you "
                "forget to instantiate the class?"
            )
        if not isinstance(connector, SwapQuoteCapability):
            raise SwapQuoteRegistryError(
                "register() expects a connector implementing SwapQuoteCapability "
                f"in addition to SwapQuoteConnector; {type(connector).__qualname__!s} "
                "is missing quote_swap()."
            )
        protocols = (connector.protocol, *connector.protocol_aliases)
        for proto in protocols:
            existing = self._connectors.get(proto)
            if existing is not None and type(existing) is not type(connector):
                raise SwapQuoteRegistryError(
                    f"protocol {proto!r} already registered by "
                    f"{type(existing).__qualname__}; refusing to overwrite with "
                    f"{type(connector).__qualname__}"
                )
        registered = next((self._connectors[proto] for proto in protocols if proto in self._connectors), connector)
        for proto in protocols:
            self._connectors[proto] = registered

    def get(self, protocol: str) -> SwapQuoteConnector | None:
        """Return the provider for ``protocol`` if registered."""
        return self._connectors.get(ProtocolName(protocol))

    def quote_swap(self, ctx: Any, request: SwapQuoteRequest) -> SwapQuoteResult | None:
        """Quote through the registered provider for ``request.protocol``."""
        connector = self.get(request.protocol)
        if connector is None:
            return None
        if not isinstance(connector, SwapQuoteCapability):
            return None
        return connector.quote_swap(ctx, request)

    def all(self) -> tuple[SwapQuoteConnector, ...]:
        """Return every registered connector in registration order."""
        connectors: list[SwapQuoteConnector] = []
        for connector in self._connectors.values():
            if not any(existing is connector for existing in connectors):
                connectors.append(connector)
        return tuple(connectors)

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``."""
        return tuple(c for c in self.all() if isinstance(c, capability))

    def clear(self) -> None:
        """Test helper: clear registrations."""
        self._connectors.clear()


SWAP_QUOTE_REGISTRY: SwapQuoteRegistry = SwapQuoteRegistry()
