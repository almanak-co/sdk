"""Gateway-side capability Protocols — gateway-side only.

Each ``Gateway*Capability`` is a ``@runtime_checkable`` Protocol that a
``GatewayConnector`` subclass declares it implements simply by defining
the matching method. The registry groups connectors by capability so the
gateway can dispatch capability-keyed calls without knowing about
specific protocols.

Phase 0 ships the three capabilities Phase 2 needs immediately:

* ``GatewayServicerCapability`` — connector ships its own gRPC servicer
  (Phase 2 callers: ``enso_service``, ``polymarket_service``).
* ``GatewayMarketLookupCapability`` — connector provides a token / market
  metadata lookup (Phase 2 callers: aave / compound / fluid / morpho /
  pendle / jupiter / beefy / yearn lookups).
* ``GatewayPoolKeySeedCapability`` — connector pre-seeds the gateway's
  pool-key cache at boot (Phase 2 caller: ``uniswap_v4``).

Phase 3 (VIB-4811) replaces the gateway's hardcoded protocol-keyed
dispatch tables with registry queries by adding:

* ``GatewayPoolHistoryCapability`` — connector publishes the chains on
  which its pool history is queryable.
* ``GatewayDefillamaSlugCapability`` — connector reports its DefiLlama
  project slug.
* ``GatewayFundingRateCapability`` — perp connector publishes per-market
  default rates + a funding-payment helper.
* ``GatewaySubgraphCapability`` — connector publishes alias → subgraph URL
  pairs for ``TheGraphIntegration``.
* ``GatewayPriceIdCapability`` — connector publishes CoinGecko +
  Dexscreener IDs for its protocol token(s).
* ``GatewayDexQuoteCapability`` — DEX connector publishes a quote
  function + supported chains.

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

    After ``register_servicers`` is called, ``servicer`` exposes the
    concrete servicer instance the connector wired into the server, so
    the gateway shutdown loop can call its ``close()`` (and any other
    resource-finalising method) without each connector having to wire
    a separate teardown hook. Before ``register_servicers`` runs (or
    when the connector legitimately registers nothing), ``servicer``
    is ``None``.

    Replaces hand-wired ``server.py`` registrations for protocols whose
    gateway-side code lives in the connector (e.g. Polymarket, Enso).
    """

    @property
    def servicer(self) -> Any | None: ...

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


@runtime_checkable
class GatewayFundingRateCapability(Protocol):
    """Perp connector publishes its venue ID + default funding rates.

    Replaces the hardcoded ``DEFAULT_RATES`` table and the venue-string
    ``if venue == "hyperliquid" ... elif venue == "gmx_v2" ...`` dispatch
    in ``almanak.gateway.services.funding_rate_service`` (VIB-4811 /
    Phase 3).

    The proto ``FundingRateRequest.venue`` field is preserved — strategy
    callers still address perp venues by string identifier. The change
    is that the gateway looks up the matching capability provider from
    the registry instead of branching on ``if venue == "...":``.

    Contract:

    * ``venue() -> str`` — the venue identifier (e.g. ``"hyperliquid"``,
      ``"gmx_v2"``); matches the ``FundingRateRequest.venue`` string a
      strategy submits.
    * ``default_funding_rate(market) -> Decimal`` — fallback hourly
      funding rate when the live fetch fails. Returning a non-positive
      Decimal is legal (the historical default is ``Decimal("0.00001")``
      for unknown markets).
    * ``fetch_funding_rate(servicer, market, chain) -> Awaitable`` —
      venue-specific live fetch. Receives the servicer (its HTTP
      session, web3 cache, settings) so connector code stays free of
      gateway plumbing. Returns the ``FundingRateData`` dataclass that
      the servicer translates to the proto envelope.

    The ``fetch_funding_rate`` callable is typed as ``Any`` because the
    ``FundingRateData`` return type lives in a gateway-side module and
    importing it here would couple ``_base/`` to the gateway internals
    (breaks the foundation's leaf-of-the-import-graph invariant).
    """

    def venue(self) -> str: ...

    def default_funding_rate(self, market: str) -> Any: ...

    async def fetch_funding_rate(
        self,
        servicer: Any,
        market: str,
        chain: str,
    ) -> Any: ...


@runtime_checkable
class GatewayDefillamaSlugCapability(Protocol):
    """Connector publishes its DefiLlama project slug.

    Replaces the hardcoded ``_PROTOCOL_TO_LLAMA`` dispatch dict in
    ``almanak.gateway.services.pool_analytics_service`` (VIB-4811 /
    Phase 3).

    Returns the canonical DefiLlama slug for the connector's protocol
    (e.g. Aave v3 returns ``"aave-v3"``, Aerodrome returns
    ``"aerodrome-v2"``). The dispatcher keys the result by
    ``GatewayConnector.protocol``.

    ``defillama_slug_aliases`` lets a connector publish additional
    ``protocol_key -> slug`` entries for variants that ride the same
    connector — e.g. Aerodrome exposes ``aerodrome_slipstream`` as a
    distinct slug while sharing the underlying integration. Returns an
    empty mapping when no aliases apply.

    The connector's protocol name is read by the dispatcher from the
    base ``GatewayConnector.protocol`` ClassVar; this Protocol only
    contributes the slug + optional aliases.
    """

    def defillama_slug(self) -> str | None: ...

    def defillama_slug_aliases(self) -> dict[str, str]: ...


@runtime_checkable
class GatewayPoolHistoryCapability(Protocol):
    """Connector publishes the chains on which its pool history is queryable.

    Replaces the hardcoded
    ``almanak.gateway.services.pool_history_service.POOL_PROTOCOL_ALLOWLIST``
    + ``SUPPORTED_POOL_PAIRS`` dispatch tables (VIB-4811 / Phase 3).

    The pool history servicer (POOL-3 / VIB-4751) validates incoming
    requests against ``(chain, protocol)``. With the capability in place
    the validator iterates ``GATEWAY_REGISTRY.capability_providers(
    GatewayPoolHistoryCapability)`` and unions every connector's
    ``pool_history_supported_chains()`` into the live allowlist.

    Returning an empty ``frozenset`` is a legal "I declare the capability
    but presently support no chain" state — useful when a connector's
    subgraph coverage is being staged in.

    The connector's protocol name is read by the dispatcher from the
    base ``GatewayConnector.protocol`` ClassVar; this Protocol only
    contributes the chain list.
    """

    def pool_history_supported_chains(self) -> frozenset[str]: ...


@runtime_checkable
class GatewaySubgraphCapability(Protocol):
    """Connector publishes its TheGraph subgraph URLs.

    Replaces the hardcoded ``DEFAULT_ALLOWED_SUBGRAPHS`` dict in
    ``almanak.gateway.integrations.thegraph`` (VIB-4811 / Phase 3).

    The TheGraph integration builds its allowlisted subgraph dict at
    construction time by iterating ``GATEWAY_REGISTRY.capability_providers(
    GatewaySubgraphCapability)`` and merging each connector's
    ``subgraph_endpoints()`` mapping into the live dict.

    ``subgraph_endpoints`` returns a mapping from subgraph alias (the
    public key, e.g. ``"uniswap-v3-arbitrum"``) to the GraphQL endpoint
    URL. The alias scheme historically encodes ``<protocol>-<chain>`` so
    callers may pass ``"uniswap-v3-arbitrum"`` directly; this Protocol
    keeps that surface unchanged.

    Returning an empty mapping is legal — useful while the connector
    stages in subgraph coverage incrementally.
    """

    def subgraph_endpoints(self) -> dict[str, str]: ...


@runtime_checkable
class GatewayDexQuoteCapability(Protocol):
    """DEX connector publishes a quote function + supported-chain list.

    Replaces the hardcoded ``Dex`` enum + ``DEX_CHAINS`` dispatch table
    + ``if dex == "uniswap_v3" ... elif dex == "curve" ... elif dex ==
    "enso" ...`` chain in ``almanak.gateway.data.price.multi_dex``
    (VIB-4811 / Phase 3).

    The multi-DEX price service builds its lookup tables at startup by
    iterating ``GATEWAY_REGISTRY.capability_providers(GatewayDexQuoteCapability)``
    and indexing the providers by ``dex_name()``. ``get_quote`` then
    dispatches by registry lookup, not by a string-keyed if/elif.

    Contract:

    * ``dex_name() -> str`` — the DEX identifier (e.g. ``"uniswap_v3"``,
      ``"curve"``, ``"enso"``). Matches the string callers pass to
      ``MultiDexPriceService.get_quote(dex=...)``.
    * ``supported_chains() -> frozenset[str]`` — chains where this DEX
      is queryable. ``MultiDexPriceService`` derives its per-chain
      ``DEX_CHAINS`` dispatch table by unioning this across every
      provider.
    * ``async quote(service, token_in, token_out, amount_in) -> DexQuote``
      — delegated quote computation. Receives the calling service so
      DEX-specific simulation helpers (price-impact / slippage curves,
      mock hooks, default-price fallback) stay on the service where
      they share state with siblings; the capability layer only owns
      dispatch.

    The returned ``DexQuote`` is the gateway-internal dataclass
    declared in ``almanak.gateway.data.price.multi_dex``; importing it
    here would couple ``_base/`` to the gateway internals (breaks the
    foundation's leaf-of-the-import-graph invariant), so ``quote``
    returns ``Any``.
    """

    def dex_name(self) -> str: ...

    def supported_chains(self) -> frozenset[str]: ...

    async def quote(
        self,
        service: Any,
        token_in: str,
        token_out: str,
        amount_in: Any,
    ) -> Any: ...


@runtime_checkable
class GatewayPriceIdCapability(Protocol):
    """Connector publishes its protocol token's price-source identifiers.

    Replaces protocol-token entries in the hardcoded per-chain
    ``*_TOKEN_IDS`` dispatch tables in
    ``almanak.gateway.data.price.coingecko`` and the Solana entry in
    ``_KNOWN_TOKEN_ADDRESSES`` in
    ``almanak.gateway.data.price.dexscreener`` (VIB-4811 / Phase 3).

    The price sources build their lookup tables at startup by iterating
    ``GATEWAY_REGISTRY.capability_providers(GatewayPriceIdCapability)``
    and merging each connector's contribution into the live dict.

    Contract:

    * ``coingecko_ids() -> dict[str, str]`` — token symbol (uppercase)
      → CoinGecko slug. Returns an empty mapping when the connector's
      protocol token isn't priced via CoinGecko. Multiple connectors
      may legitimately publish the same symbol (e.g. CG-canonical
      cross-chain tokens), provided they agree on the slug — disagreeing
      slugs raise a loud assembly-time error.
    * ``dexscreener_ids() -> dict[str, dict[str, str]]`` — DexScreener
      platform slug (e.g. ``"solana"``) → {token symbol → on-chain
      address}. Solana-native protocol tokens (Jupiter JUP, Raydium
      RAY, Orca ORCA, …) historically lived in DexScreener's
      ``_KNOWN_TOKEN_ADDRESSES["solana"]`` dict; this Protocol moves
      them onto the owning connector. Returns an empty mapping when
      the connector's protocol token isn't addressable on DexScreener
      (e.g. an EVM-only protocol whose token is resolved via
      ``TokenResolver``).

    The connector's protocol name is read by the dispatcher from the
    base ``GatewayConnector.protocol`` ClassVar; this Protocol only
    contributes the symbol → identifier mappings.
    """

    def coingecko_ids(self) -> dict[str, str]: ...

    def dexscreener_ids(self) -> dict[str, dict[str, str]]: ...
