"""Gateway capabilities published by integration manifests."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable


class OracleDataUnavailable(RuntimeError):
    """A provider-exact oracle read produced no measured observation."""


class PriceSourceScope(StrEnum):
    """Lifecycle scope for a gateway price source."""

    SHARED = "shared"
    CHAIN = "chain"


@runtime_checkable
class GatewayPriceSourceFactory(Protocol):
    """Factory used by ``MarketService`` without concrete provider imports.

    ``supports`` must be pure and deterministic. ``build`` runs only inside the
    gateway and may return ``None`` when deployment configuration intentionally
    disables the provider.
    """

    @property
    def name(self) -> str: ...

    @property
    def scope(self) -> PriceSourceScope: ...

    @property
    def order(self) -> int: ...

    def supports(self, chain: str | None) -> bool: ...

    def build(self, *, chain: str | None, settings: Any) -> Any | None: ...


class GatewayOraclePricePoint(Protocol):
    """Provider-exact oracle observation consumed by the gateway dispatcher."""

    timestamp: int
    price: Decimal
    observation_id: int | str


class GatewayOraclePricePage(Protocol):
    """Bounded oracle-history page with an explicit completeness signal."""

    points: Sequence[GatewayOraclePricePoint]
    truncated: bool
    recommended_split_ts: int


@runtime_checkable
class GatewayOracleReader(Protocol):
    """Current and historical provider-exact oracle reader contract."""

    async def get_latest(self, *, token: str) -> GatewayOraclePricePoint: ...

    async def get_history_page(
        self,
        *,
        token: str,
        start_ts: int,
        end_ts: int,
        max_points: int,
    ) -> GatewayOraclePricePage: ...

    async def close(self) -> None: ...


@runtime_checkable
class GatewayOracleReaderFactory(Protocol):
    """Factory for provider-exact current and historical oracle reads."""

    @property
    def name(self) -> str: ...

    def supports(self, chain: str, token: str) -> bool: ...

    def build(self, *, chain: str, settings: Any) -> GatewayOracleReader: ...


@runtime_checkable
class GatewayApiClientFactory(Protocol):
    """Factory for a provider's gateway-only API client."""

    @property
    def name(self) -> str: ...

    def build(self, *, settings: Any) -> Any: ...


@runtime_checkable
class GatewayPortfolioProviderFactory(Protocol):
    """Factory for a normalized wallet-portfolio provider."""

    @property
    def name(self) -> str: ...

    @property
    def requires_api_key(self) -> bool: ...

    def build(self, *, api_key: str | None, cache_ttl: int, settings: Any | None = None) -> Any: ...
