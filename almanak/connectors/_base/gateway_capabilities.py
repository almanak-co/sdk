"""Gateway-side capability Protocols — gateway-side only.

Each ``Gateway*Capability`` is a ``@runtime_checkable`` Protocol that a
``GatewayConnector`` subclass declares it implements simply by defining
the matching method. The registry groups connectors by capability so the
gateway can dispatch capability-keyed calls without knowing about
specific protocols.

Phase 0 ships only the three capabilities Phase 2 needs immediately:

* ``GatewayServicerCapability`` — connector ships its own gRPC servicer
  (Phase 2 callers: ``enso_service``, ``polymarket_service``).
* ``GatewayMarketLookupCapability`` — connector provides a token / market
  metadata lookup (Phase 2 callers: aave / compound / fluid / morpho /
  pendle / jupiter / beefy / yearn lookups).
* ``GatewayPoolKeySeedCapability`` — connector pre-seeds the gateway's
  pool-key cache at boot (Phase 2 caller: ``uniswap_v4``).

Phase 3 adds further capabilities (funding rates, DEX quotes, DefiLlama
slug, subgraph URLs, CoinGecko IDs, pool history support, …) as their
gateway-side dispatchers migrate.

Strategy-side code MUST NOT import this module.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class GatewayServicerCapability(Protocol):
    """Connector ships its own gRPC servicer.

    The gateway boot loop calls ``register_servicers`` after constructing
    the connector. The connector is responsible for invoking
    ``add_<ServiceName>ServiceServicer_to_server`` on ``server``.

    ``settings`` is the gateway ``GatewaySettings`` object — the
    connector reads its own configuration keys from it.

    Replaces hand-wired ``server.py`` registrations for protocols whose
    gateway-side code lives in the connector (e.g. Polymarket, Enso).
    """

    def register_servicers(self, server: Any, settings: Any) -> None: ...


@runtime_checkable
class GatewayMarketLookupCapability(Protocol):
    """Connector provides a token / market metadata lookup service.

    Returns a singleton lookup instance (typically a subclass of
    ``ProtocolTokenLookup`` in ``almanak/gateway/services/_protocol_lookup.py``).
    The gateway caches the returned object; ``market_lookup`` is called
    once at boot per connector.
    """

    def market_lookup(self) -> Any: ...


@runtime_checkable
class GatewayPoolKeySeedCapability(Protocol):
    """Connector pre-seeds the gateway's pool-key cache at boot.

    Used by Uniswap V4 to register canonical PoolKeys (WETH/USDC,
    WBTC/WETH, …) whose Initialize event is too old to be discovered by
    the runtime log-scan window. The connector receives the cache
    instance and calls ``cache.register(...)`` for each canonical pool.
    """

    def seed_pool_keys(self, cache: Any) -> None: ...
