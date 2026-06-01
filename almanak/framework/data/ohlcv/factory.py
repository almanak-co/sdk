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
    CoinGeckoGatewayDataProvider,
    GatewayOHLCVDataProvider,
    GeckoTerminalGatewayDataProvider,
)
from almanak.framework.data.ohlcv.gateway_provider import (
    GatewayCoinGeckoOHLCVProvider,
    GatewayGeckoTerminalOHLCVProvider,
    GatewayOHLCVProvider,
)
from almanak.framework.data.ohlcv.ohlcv_router import (
    OHLCVRouter,
    provider_names_in_chains,
)
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
        Three providers are wired: ``geckoterminal`` (DEX-native),
        ``binance`` (CEX primary), and ``coingecko`` (CEX fallback, VIB-4847).
        CoinGecko gives the ``cex_primary`` chain a real fallback so a stale /
        rebranded Binance ticker (rejected by the ALM-2697 staleness guard) no
        longer dead-ends in a permanent ``DATA_ERROR``.

        The ``defillama`` tier named in older chain configs was **removed** from
        ``_PROVIDER_CHAINS`` (VIB-4847) because no gateway-backed DeFi Llama
        OHLCV provider exists yet (tracked on VIB-3448). The
        provider-chain ↔ registry invariant (:func:`assert_provider_chains_registered`)
        forbids dangling names, so DeFi Llama must be re-added to both the chain
        AND the registry in the same change when it ships.

    Raises:
        ValueError: If a provider named in ``_PROVIDER_CHAINS`` is not
            registered (provider-chain ↔ registry invariant, VIB-4847).
    """
    gateway_provider = GatewayOHLCVProvider(gateway_client=gateway_client)
    binance_adapter = GatewayOHLCVDataProvider(gateway_provider)

    gecko_provider = GatewayGeckoTerminalOHLCVProvider(gateway_client=gateway_client, chain=chain)
    gecko_adapter = GeckoTerminalGatewayDataProvider(gecko_provider)

    coingecko_provider = GatewayCoinGeckoOHLCVProvider(gateway_client=gateway_client)
    coingecko_adapter = CoinGeckoGatewayDataProvider(coingecko_provider)

    router = OHLCVRouter(default_chain=chain)
    router.register_provider(gecko_adapter)
    router.register_provider(binance_adapter)
    router.register_provider(coingecko_adapter)

    # Provider-chain ↔ registry invariant (VIB-4847): fail loud at build time
    # if any advertised provider name was never registered. This is the durable
    # fix — it catches the next phantom-tier regression before a strategy hits a
    # silent failover dead-end in production.
    assert_provider_chains_registered(router)

    routing_provider = RoutingOHLCVProvider(
        router=router,
        chain=chain,
        pool_address=str(pool_address) if pool_address else None,
        closeable_providers=[],
    )

    return OHLCVStack(router=router, provider=routing_provider)


def assert_provider_chains_registered(router: OHLCVRouter) -> None:
    """Assert every provider named in ``_PROVIDER_CHAINS`` is registered.

    The root cause of VIB-4847 was silent drift between the *advertised*
    failover chain (``_PROVIDER_CHAINS``) and the *registered* providers: a
    name listed in the chain but never constructed makes the router walk past
    it on every miss, degrading the chain to fewer providers than advertised.

    This guard makes that drift loud. It runs at factory build time (so a
    misconfigured deployment fails on boot, not mid-strategy) and is also
    exercised directly by unit tests.

    Args:
        router: A composed :class:`OHLCVRouter` with providers registered.

    Raises:
        ValueError: If any name in ``_PROVIDER_CHAINS`` has no registered
            provider. The message lists the missing names so the fix is
            obvious: either register the provider, or remove it from the chain.
    """
    advertised = provider_names_in_chains()
    registered = set(router._providers.keys())
    missing = advertised - registered
    if missing:
        raise ValueError(
            "OHLCV provider-chain ↔ registry invariant violated (VIB-4847): "
            f"provider(s) {sorted(missing)} are referenced in _PROVIDER_CHAINS "
            f"but not registered in the factory (registered: {sorted(registered)}). "
            "Either register the provider or remove its name from _PROVIDER_CHAINS."
        )


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
    "assert_provider_chains_registered",
    "create_ohlcv_stack",
    "create_routing_ohlcv_provider",
]
