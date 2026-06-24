"""Strategy-side principal-token market reader registry.

Protocols with principal-token style markets can publish an on-chain read
capability without framework code importing the concrete connector module. The
current primary user is Pendle PT valuation and PT-collateral health data.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "PRINCIPAL_TOKEN_MARKET_READ_REGISTRY",
    "PrincipalTokenMarketReadCapability",
    "PrincipalTokenMarketReadConnector",
    "PrincipalTokenMarketReadRegistry",
    "PrincipalTokenMarketReadRegistryError",
    "PrincipalTokenMarketReader",
]


class PrincipalTokenMarketReadRegistryError(Exception):
    """Registry contract violation."""


@runtime_checkable
class PrincipalTokenMarketReader(Protocol):
    """Read-only market data needed for principal-token valuation."""

    def get_pt_to_asset_rate(self, market_address: str) -> Decimal: ...

    def get_pt_to_sy_rate(self, market_address: str) -> Decimal:
        """PT→SY exchange rate (the discounted market mark, VIB-5407).

        Canonical money-path rate for open-PT mark-to-market: ``PT/USD =
        get_pt_to_sy_rate × underlying/USD`` (the underlying being the SY mint
        token the SY wraps ~1:1). Distinct from :meth:`get_pt_to_asset_rate`,
        which is denominated in the SY accounting asset and over-marks the PT
        toward par when that asset differs from the priced underlying.
        """
        ...

    def get_implied_apy(self, market_address: str) -> Decimal: ...

    def is_market_expired(self, market_address: str) -> bool: ...

    def get_market_expiry_ts(self, market_address: str) -> int | None:
        """Authoritative on-chain ``expiry()`` unix timestamp, or None on failure.

        Single source of truth for PT maturity (VIB-5384): the gateway stamps this
        as the response ``maturity_ts`` and derives ``days_to_maturity`` from the
        same read. None means the expiry could not be read (Empty≠Zero — never a
        fabricated 0).
        """
        ...

    def get_days_to_maturity(self, market_address: str) -> int | None: ...

    def get_market_tokens(self, market_address: str) -> dict[str, str]: ...

    def estimate_pt_output(self, market_address: str, amount_in: int) -> int: ...


@runtime_checkable
class PrincipalTokenMarketReadCapability(Protocol):
    """Connector builds read-only principal-token market readers."""

    def build_reader(
        self,
        *,
        chain: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
        cache_ttl_seconds: float = 30.0,
    ) -> PrincipalTokenMarketReader: ...


class PrincipalTokenMarketReadConnector:
    """Base class for principal-token market reader connector instances."""

    protocol: ClassVar[ProtocolName]
    kind: ClassVar[ProtocolKind]


T = TypeVar("T")


class PrincipalTokenMarketReadRegistry:
    """In-process registry of principal-token market reader connectors."""

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, PrincipalTokenMarketReadConnector] = {}

    def register(self, connector: PrincipalTokenMarketReadConnector) -> None:
        """Register a connector instance. Same-type re-registration is a no-op."""
        if not isinstance(connector, PrincipalTokenMarketReadConnector):
            raise PrincipalTokenMarketReadRegistryError(
                "register() expects a PrincipalTokenMarketReadConnector instance, got "
                f"{type(connector).__qualname__!s} ({connector!r}); did you "
                "forget to instantiate the class?"
            )
        if not isinstance(connector, PrincipalTokenMarketReadCapability):
            raise PrincipalTokenMarketReadRegistryError(
                "register() expects a connector implementing PrincipalTokenMarketReadCapability "
                f"in addition to PrincipalTokenMarketReadConnector; {type(connector).__qualname__!s} "
                "is missing build_reader()."
            )
        proto = connector.protocol
        existing = self._connectors.get(proto)
        if existing is not None:
            if type(existing) is type(connector):
                return
            raise PrincipalTokenMarketReadRegistryError(
                f"protocol {proto!r} already registered by "
                f"{type(existing).__qualname__}; refusing to overwrite with "
                f"{type(connector).__qualname__}"
            )
        self._connectors[proto] = connector

    def lookup(self, protocol: str) -> PrincipalTokenMarketReadCapability | None:
        """Return the read capability for ``protocol``, or ``None``."""
        connector = self._connectors.get(ProtocolName(protocol))
        if connector is None:
            return None
        if not isinstance(connector, PrincipalTokenMarketReadCapability):
            return None
        return connector

    def build_reader(
        self,
        protocol: str,
        *,
        chain: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
        cache_ttl_seconds: float = 30.0,
    ) -> PrincipalTokenMarketReader:
        """Build a protocol-owned principal-token market reader."""
        capability = self.lookup(protocol)
        if capability is None:
            raise PrincipalTokenMarketReadRegistryError(
                f"protocol {protocol!r} does not publish a principal-token market reader"
            )
        return capability.build_reader(
            chain=chain,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            cache_ttl_seconds=cache_ttl_seconds,
        )

    def default(self) -> PrincipalTokenMarketReadCapability:
        """Return the sole registered reader capability.

        This is intentionally strict: a framework path may use it only while
        one principal-token market reader exists. Once a second protocol opts
        in, callers must route by caller-provided protocol rather than by a
        central framework literal; broad fallback handlers must not treat the
        multi-reader error as a measured zero.
        """
        connectors = tuple(
            connector
            for connector in self._connectors.values()
            if isinstance(connector, PrincipalTokenMarketReadCapability)
        )
        if not connectors:
            raise PrincipalTokenMarketReadRegistryError("no principal-token market reader is registered")
        if len(connectors) > 1:
            raise PrincipalTokenMarketReadRegistryError(
                "multiple principal-token market readers are registered; caller must provide a protocol"
            )
        return connectors[0]

    def build_default_reader(
        self,
        *,
        chain: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
        cache_ttl_seconds: float = 30.0,
    ) -> PrincipalTokenMarketReader:
        """Build the sole registered principal-token market reader."""
        return self.default().build_reader(
            chain=chain,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            cache_ttl_seconds=cache_ttl_seconds,
        )

    def all(self) -> tuple[PrincipalTokenMarketReadConnector, ...]:
        """Return every registered connector in registration order."""
        return tuple(self._connectors.values())

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``."""
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def clear(self) -> None:
        """Test helper: clear registrations."""
        self._connectors.clear()


PRINCIPAL_TOKEN_MARKET_READ_REGISTRY: PrincipalTokenMarketReadRegistry = PrincipalTokenMarketReadRegistry()
