"""Strategy-side base types for connector-owned prediction-market (CLOB) reads (VIB-4989).

The framework's prediction surface (``MarketSnapshot.prediction(...)``, the
prediction monitor) must not hardcode *how* a venue's CLOB market data is fetched
— that is **connector knowledge**. Historically a Polymarket-specific provider
lived in the framework (``framework/data/prediction_provider.py``) and was
imported directly by ``cli/run.py`` / ``strategy_runner.py``, leaking the
connector into the framework (VIB-4989, part of the VIB-4851 self-containment
epic).

This module owns the venue-neutral half of the read seam, mirroring the on-chain
read seams (``PerpsReadSpec`` / ``LendingReadSpec``). The architectural wrinkle:
a prediction market is an **off-chain CLOB**, not an on-chain primitive. There is
no ``eth_call`` plan to materialise/decode — market data and the order book come
from a stateful gRPC round-trip to a connector-owned ``PolymarketService``. So
the seam publishes a **provider factory** (``build_provider``), not a calldata
plan, and the framework consumes the returned provider through the venue-neutral
:class:`PredictionProvider` Protocol.

* :class:`PredictionReadSpec` — the descriptor a connector publishes (as a
  module-level ``PREDICTION_READ_SPEC``): how to build its provider, and the
  chains its prediction reads support.
* :class:`PredictionProvider` — the duck-typed read surface the framework calls.
  The concrete result types (``PredictionMarket`` / ``PredictionPosition`` / …)
  stay in the framework as the venue-neutral interface; the connector imports
  them back (connector→framework is allowed) and returns them.

Gateway-boundary note: this module is ``_strategy_base`` (the broker tier). It
performs no network egress and does not import ``almanak.framework`` at runtime —
the framework result types are referenced only in annotations (kept as strings by
``from __future__ import annotations``) under ``TYPE_CHECKING``, avoiding a
connectors→framework import cycle. The connector's ``build_provider`` wraps a
gateway-routed client; the gRPC round-trip itself stays behind the gateway.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from decimal import Decimal
    from typing import Literal

    # Annotation-only: a runtime import would pull ``almanak.framework.data`` into
    # ``_strategy_base`` (a connectors→framework cycle). The Protocol only names
    # these types in string annotations; nothing here touches them at runtime.
    from almanak.framework.data.prediction_provider import (
        PredictionMarket,
        PredictionOrder,
        PredictionPosition,
        PriceHistory,
    )

__all__ = ["PredictionProvider", "PredictionReadSpec"]


@runtime_checkable
class PredictionProvider(Protocol):
    """Venue-neutral read surface for a prediction-market (CLOB) data provider.

    The contract a connector's ``build_provider`` must return and the framework's
    ``MarketSnapshot`` / prediction monitor consume. ``@runtime_checkable`` so a
    test can structurally assert a relocated provider satisfies the surface (only
    method *names* are checked at runtime — signatures are documentary). The
    surface is the subset the framework consumers call; a connector provider may
    expose more. Return types live in the framework (venue-neutral); the connector
    imports and returns them.
    """

    def get_market(self, market_id_or_slug: str) -> PredictionMarket: ...

    def get_market_by_token_id(self, token_id: str) -> PredictionMarket | None: ...

    def get_price(self, market_id_or_slug: str, outcome: Literal["YES", "NO"]) -> Decimal: ...

    def get_positions(
        self,
        wallet: str | None = ...,
        market_id: str | None = ...,
        outcome: Literal["YES", "NO"] | None = ...,
    ) -> list[PredictionPosition]: ...

    def get_open_orders(self, market_id: str | None = ...) -> list[PredictionOrder]: ...

    def get_price_history(self, market_id_or_slug: str, *args: object, **kwargs: object) -> PriceHistory: ...

    def clear_cache(self) -> None: ...


@dataclass(frozen=True)
class PredictionReadSpec:
    """Connector-published descriptor: how to build a prediction-read provider.

    Attributes:
        build_provider: ``(*, gateway_client, wallet) -> PredictionProvider``. A
            factory the registry calls to construct the connector's provider,
            wired to the gateway client. Keyword-only at the registry boundary.
        chains: The chains this connector's prediction reads support (e.g.
            ``frozenset({"polygon"})``). Drives ``supports_chain`` discovery so the
            framework never hardcodes a chain for a venue.
    """

    build_provider: Callable[..., PredictionProvider]
    chains: frozenset[str]

    def __post_init__(self) -> None:
        # Validate ``build_provider`` is callable so an invalid spec is caught at
        # registration/import, not late when the runner first builds the provider
        # (mirrors ``GatewayStubSpec``'s ``stub_factory`` check).
        if not callable(self.build_provider):
            raise TypeError(f"build_provider must be callable, got {type(self.build_provider).__name__}.")
        # Reject a bare str for ``chains``: a str is iterable, so it would silently
        # register each *character* as a chain — the same footgun the accounting
        # seam guards against. Coerce any other iterable of non-empty strings to a
        # frozenset (the dataclass is frozen, so set via ``object.__setattr__``).
        chains = self.chains
        if isinstance(chains, str | bytes):
            raise TypeError(
                "chains must be a frozenset[str], not a bare "
                f"{type(chains).__name__} (a bare string is iterated character-by-character); "
                "pass e.g. frozenset({'polygon'})."
            )
        coerced = frozenset(chains)
        for member in coerced:
            if not isinstance(member, str) or not member:
                raise TypeError(f"chains members must be non-empty strings, got {member!r}.")
        object.__setattr__(self, "chains", coerced)
