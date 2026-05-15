"""Factory for the standard OHLCV stack — single composition path.

Single source of truth for how the framework composes its OHLCV stack:

- **Indicator calculators** (RSI, MACD, ...) consume :class:`RoutingOHLCVProvider`,
  typically wrapped in :class:`DedupingOHLCVProvider` for per-iteration coalescing.
- **Live ``MarketSnapshot.ohlcv()``** consumes the underlying sync
  :class:`OHLCVRouter` directly so the snapshot can return ``DataEnvelope`` with
  provenance metadata without an async hop.
- **Dashboard** (``DashboardAPIClient.get_ohlcv()``) consumes
  :class:`RoutingOHLCVProvider` and unwraps to plain dicts.

All three surfaces compose providers the same way, so adding a new provider
(CoinGecko OHLCV, DeFi Llama OHLCV, ...) lights up everywhere by editing this
file alone.

This factory previously lived at :mod:`almanak.framework.cli.run`. It was
relocated to its dependencies' home (VIB-4347) because its placement in
``cli/`` made dashboard / paper-trade consumers reach into CLI launcher
internals to compose the OHLCV stack. The :func:`create_routing_ohlcv_provider`
symbol is still importable from ``almanak.framework.cli.run`` as a one-line
back-compat re-export.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from almanak.framework.data.ohlcv.gateway_data_adapter import (
    GatewayOHLCVDataProvider,
    GeckoTerminalGatewayDataProvider,
)
from almanak.framework.data.ohlcv.gateway_provider import (
    GatewayGeckoTerminalOHLCVProvider,
    GatewayOHLCVProvider,
)
from almanak.framework.data.ohlcv.ohlcv_router import OHLCVRouter
from almanak.framework.data.ohlcv.routing_provider import RoutingOHLCVProvider


@dataclass(frozen=True)
class OHLCVStack:
    """A composed OHLCV stack exposing both the sync router and the async provider.

    Why both surfaces?

    - :attr:`router` is the sync :class:`OHLCVRouter` that returns
      :class:`DataEnvelope` with full provenance. ``MarketSnapshot.ohlcv()`` is
      sync and binds the router via ``ohlcv_router=`` on the snapshot.
    - :attr:`provider` is the async :class:`RoutingOHLCVProvider` that implements
      the ``OHLCVProvider`` protocol expected by indicator calculators (and
      consumed by ``DashboardAPIClient.get_ohlcv`` via ``asyncio.run``). It
      shares the same :class:`OHLCVRouter` instance, so disk cache and TTL
      hits are coherent across surfaces.

    Attributes:
        router: The sync :class:`OHLCVRouter` with providers registered.
        provider: The async :class:`RoutingOHLCVProvider` wrapping ``router``.
    """

    router: OHLCVRouter
    provider: RoutingOHLCVProvider


def create_ohlcv_stack(
    gateway_client: Any,
    chain: str,
    pool_address: str | None = None,
) -> OHLCVStack:
    """Build the standard OHLCV stack (router + gateway-backed providers).

    Wires the two gateway-backed providers (``geckoterminal`` and ``binance``)
    into a single :class:`OHLCVRouter` and returns an :class:`OHLCVStack` that
    exposes both the sync router (for ``MarketSnapshot.ohlcv()``) and the async
    routing provider (for indicators + dashboard).

    Args:
        gateway_client: Connected ``GatewayClient`` instance.
        chain: Chain name (e.g. ``"base"``, ``"arbitrum"``) — bound to every
            request through the routing provider so the protocol's chainless
            signature still routes correctly.
        pool_address: Optional pool address for DEX-pool lookups. ``None`` is
            valid for CEX-only lookups (Binance is symbol-only).

    Returns:
        :class:`OHLCVStack` with ``router`` and ``provider`` populated.

    Note:
        ``OHLCVRouter._PROVIDER_CHAINS["defi_primary"]`` lists a ``"defillama"``
        middle tier between ``"geckoterminal"`` and ``"binance"``, but no
        gateway-backed DeFi Llama OHLCV provider exists yet. Until one is wired,
        GeckoTerminal blips fall straight through to Binance. Tracked on
        VIB-3448 / gateway roadmap.
    """
    gateway_provider = GatewayOHLCVProvider(gateway_client=gateway_client)
    binance_adapter = GatewayOHLCVDataProvider(gateway_provider)

    gecko_provider = GatewayGeckoTerminalOHLCVProvider(gateway_client=gateway_client, chain=chain)
    gecko_adapter = GeckoTerminalGatewayDataProvider(gecko_provider)

    router = OHLCVRouter(default_chain=chain)
    router.register_provider(gecko_adapter)
    router.register_provider(binance_adapter)

    routing_provider = RoutingOHLCVProvider(
        router=router,
        chain=chain,
        pool_address=str(pool_address) if pool_address else None,
        closeable_providers=[],
    )

    return OHLCVStack(router=router, provider=routing_provider)


def create_routing_ohlcv_provider(
    gateway_client: Any,
    chain: str,
    pool_address: str | None = None,
) -> RoutingOHLCVProvider:
    """Build the standard OHLCV routing provider (convenience wrapper).

    Equivalent to ``create_ohlcv_stack(...).provider``. Preserved as the public
    entry-point that pre-existing call sites (live runner indicator wiring,
    paper-trade engine, ad-hoc tools) used before the :class:`OHLCVStack`
    dataclass was introduced. New call sites that also need the underlying
    sync router (notably ``MarketSnapshot.ohlcv()`` wiring) should call
    :func:`create_ohlcv_stack` instead.

    Args:
        gateway_client: Connected ``GatewayClient`` instance.
        chain: Chain name (e.g. ``"base"``, ``"arbitrum"``).
        pool_address: Optional pool address for DEX-pool lookups.

    Returns:
        :class:`RoutingOHLCVProvider` wrapping a freshly built
        :class:`OHLCVRouter` with both gateway-backed providers registered.
    """
    return create_ohlcv_stack(
        gateway_client=gateway_client,
        chain=chain,
        pool_address=pool_address,
    ).provider


__all__ = [
    "OHLCVStack",
    "create_ohlcv_stack",
    "create_routing_ohlcv_provider",
]
