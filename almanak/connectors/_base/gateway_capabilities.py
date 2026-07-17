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
* ``GatewayPoolKeyCacheCapability`` — connector builds the gateway's
  pool-key cache, including any pre-seeding (Phase 2 caller:
  ``uniswap_v4``). Supersedes the original ``GatewayPoolKeySeedCapability``
  (VIB-4810): folding "construct" + "seed" into one call lets
  ``MarketService`` hold the cache instance behind a structural Protocol
  (``PoolKeyCacheProtocol``) instead of importing a connector-specific
  cache class. The corresponding lookup-failure exception
  (``PoolKeyCacheError``) lives here so the gateway's ``except`` branch
  no longer imports a connector-specific error type either.

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
* ``GatewaySolanaRouteRefreshCapability`` — Solana route connector refreshes
  stale serialized transactions immediately before gateway-side signing.

Strategy-side code MUST NOT import this module.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
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
class GatewaySolanaRouteRefreshCapability(Protocol):
    """Connector refreshes stale Solana route transactions at execution time."""

    def refresh_solana_route(self, request: Any) -> Any: ...


class PoolKeyCacheError(Exception):
    """Refresh-time failure that prevented a pool-key cache from
    answering a lookup.

    Surfaced through caches produced by ``GatewayPoolKeyCacheCapability``.
    The gateway's ``LookupV4PoolKey`` servicer catches this base type and
    translates ``code`` to a gRPC status:

    * ``"failed_precondition"`` — cache has no upstream configured for
      the requested chain (e.g. no contract address resolved, no RPC URL,
      RPC URL resolver raised). Maps to ``FAILED_PRECONDITION``.
    * ``"unavailable"`` — upstream was reachable but the call itself
      failed (``eth_blockNumber`` / ``eth_getLogs`` raised). Maps to
      ``UNAVAILABLE``.

    The two codes carry distinct operational signals: ``failed_precondition``
    is a deployment / config issue (operator action), ``unavailable`` is a
    transient upstream problem (retry / circuit-breaker territory).
    Without the distinction operators chasing a missing-pool counter
    cannot tell whether their gateway is misconfigured or the pool
    genuinely doesn't exist on-chain.
    """

    def __init__(self, message: str, *, code: str) -> None:
        if code not in ("failed_precondition", "unavailable"):
            raise ValueError(f"PoolKeyCacheError.code must be one of failed_precondition/unavailable, got {code!r}")
        self.code = code
        super().__init__(message)


@runtime_checkable
class PoolKeyCacheProtocol(Protocol):
    """Cache interface the gateway invokes for ``LookupV4PoolKey``.

    Concrete implementations carry additional connector-specific state
    (backfill cursors, in-memory derivation indexes, per-chain RPC
    clients) but only ``lookup`` is called at the gateway boundary.

    Contract:

    * Returns ``None`` for "scanned and the pool is not in the cache" —
      the gateway translates this to ``NOT_FOUND``.
    * Raises :class:`PoolKeyCacheError` for refresh-time failures the
      cache could not paper over (no upstream configured, RPC raised,
      …) — the gateway translates these per the error's ``code``.

    The return type is ``Any`` because the cached object's field shape
    is connector-specific (the V4 cache returns a ``CachedPoolKey`` with
    V4 PoolManager struct fields); coupling ``_base/`` to a concrete
    cache module would break the foundation's leaf-of-the-import-graph
    invariant. The gateway reads the fields it needs by name.
    """

    async def lookup(self, chain: str, pool_id: bytes) -> Any | None: ...


@runtime_checkable
class GatewayPoolKeyCacheCapability(Protocol):
    """Connector builds the gateway's pool-key cache.

    ``MarketService`` holds at most one cache instance per process
    lifetime, constructed lazily on first ``LookupV4PoolKey`` request via
    ``GATEWAY_REGISTRY.capability_providers(GatewayPoolKeyCacheCapability)``.
    The connector's ``build_cache`` is responsible for any seeding the
    cache needs (e.g. registering canonical pools whose ``Initialize``
    event is older than the runtime log-scan window) — by the time the
    method returns, the cache must be ready to answer ``lookup`` calls.

    Folding construction + seeding into one method (vs. the original
    VIB-4810 split into ``GatewayPoolKeySeedCapability``) lets the
    gateway hold the cache instance behind ``PoolKeyCacheProtocol``
    instead of importing the connector's cache class to instantiate it.
    """

    def build_cache(self, *, network: str) -> PoolKeyCacheProtocol: ...


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


@dataclass(frozen=True)
class OraclePriceQuery:
    """A resolved venue-native oracle-price read request.

    Returned by :class:`GatewayOraclePriceCapability.resolve_oracle_query` so a
    gateway price source can perform the eth_call WITHOUT importing any concrete
    connector module (gateway↔connector isolation, VIB-4121). The connector owns
    the venue-specific knowledge — which precompile / contract to read, how to
    encode the query, and how to decode + scale the raw return — and hands the
    gateway a protocol-agnostic (address, calldata) pair plus the decode routine
    via :meth:`GatewayOraclePriceCapability.decode_oracle_price`.

    The gateway source owns ALL the RPC plumbing (aiohttp session, RPC URL via
    ChainRegistry, bounded timeout) and ALL the Empty≠Zero miss semantics (a
    ``None`` decode, empty read, or non-positive price is a MISS, never a
    fabricated zero). The capability never performs network egress.

    Attributes:
        symbol: Canonical symbol the query resolves (e.g. ``"BTC"``), for logging.
        to_address: The precompile / contract address the gateway ``eth_call``s.
        calldata: Hex-encoded calldata (``0x``-prefixed). For a precompile this is
            raw ABI args with no selector; for a contract it includes the selector.
        context: Opaque connector-owned handle threaded back into
            :meth:`GatewayOraclePriceCapability.decode_oracle_price` so the decode
            can access the scale metadata (e.g. ``szDecimals``) without the gateway
            interpreting it. Typed ``Any`` to keep ``_base/`` a leaf of the import
            graph.
    """

    symbol: str
    to_address: str
    calldata: str
    context: Any


@runtime_checkable
class GatewayOraclePriceCapability(Protocol):
    """Perp connector publishes a venue-native oracle price read.

    Lets a gateway price source (e.g.
    ``almanak.gateway.data.price.hyperevm.HypercoreOraclePriceSource``) read a
    venue's canonical oracle price for a perp symbol WITHOUT importing the
    concrete connector's ``addresses`` / ``markets`` / ``sdk`` modules — the
    gateway↔connector isolation ratchet forbids that direct import (VIB-4121).

    The gateway source resolves the provider from the registry via
    ``GATEWAY_REGISTRY.capability_providers(GatewayOraclePriceCapability)`` and
    dispatches by ``oracle_price_chain()``. The provider (which lives UNDER the
    connector package) owns the venue-specific bits — precompile address, query
    encoding, decode + fixed-point scale, and symbol→asset resolution — while the
    gateway source keeps the RPC plumbing and Empty≠Zero miss semantics.

    Contract:

    * ``oracle_price_chain() -> str`` — the chain string this oracle serves
      (e.g. ``"hyperevm"``). The gateway source uses it to index providers.
    * ``resolve_oracle_query(symbol) -> OraclePriceQuery | None`` — resolve a
      symbol to the ``(to_address, calldata, context)`` the gateway needs to make
      the read. Returns ``None`` when the symbol is not a resolvable market on
      this venue (the gateway maps ``None`` to a MISS so its aggregator falls
      through to other spot sources). MUST NOT perform network egress —
      resolution is from connector-owned static metadata only.
    * ``decode_oracle_price(query, raw_hex) -> Decimal | None`` — decode + scale
      the raw eth_call return into a human price. Returns ``None`` for an empty /
      undecodable / non-positive read (Empty≠Zero — the gateway maps ``None`` to
      a MISS, never a fabricated zero). MUST NOT raise on a malformed payload:
      swallow decode errors and return ``None`` so the gateway source's aggregator
      never crashes on a bad on-chain payload.

    The ``Decimal`` return + ``OraclePriceQuery`` argument are the only shared
    types; the connector's own precompile constants and encode/decode helpers
    stay private to the connector package.
    """

    def oracle_price_chain(self) -> str: ...

    def resolve_oracle_query(self, symbol: str) -> OraclePriceQuery | None: ...

    def decode_oracle_price(self, query: OraclePriceQuery, raw_hex: str) -> Any | None: ...


@runtime_checkable
class GatewayPerpFillsCapability(Protocol):
    """Perp connector publishes per-fill economics + funding deltas (VIB-5595).

    The CoreWriter-style async-settlement venues (Hyperliquid first) settle
    orders **off the EVM** on their own matching engine, so the submit receipt
    carries NO fill price, fee, realized PnL, or funding. That data lives on the
    venue's Info API (``api.hyperliquid.xyz/info`` ``userFills`` / ``userFunding``)
    — egress that MUST live in the gateway sidecar, never the strategy container
    (AGENTS.md §"Gateway boundary"). This capability is the gateway-side reader:
    the ``PerpFillService`` servicer dispatches ``GetUserFills`` / ``GetUserFunding``
    to the connector whose :meth:`fills_venue` matches the request venue.

    Sibling of :class:`GatewayFundingRateCapability` (live rate) and
    :class:`GatewayFundingHistoryCapability` (historical rate); this one is the
    per-account **executed-fill** lane the accounting path correlates by the
    deterministic ``cloid`` a CoreWriter order carries.

    Contract:

    * ``fills_venue() -> str`` — venue identifier matching
      :meth:`GatewayFundingRateCapability.venue` so all perp capabilities on the
      same connector agree on identity (e.g. ``"hyperliquid"``).
    * ``fetch_user_fills(servicer, *, wallet_address, coin, start_ts) -> Awaitable``
      — per-fill economics for ``wallet_address`` (optionally filtered to
      ``coin`` and to fills at/after ``start_ts`` epoch-ms). Returns the
      gateway-side ``PerpFillData`` list the servicer maps to the proto
      envelope. Each fill carries ``fee`` / ``closed_pnl`` / ``px`` / ``sz`` /
      ``dir`` / ``oid`` / ``cloid`` / ``time`` — Empty≠Zero: a value the venue
      did not report is unmeasured (empty string on the wire), never a
      fabricated ``0``.
    * ``fetch_user_funding(servicer, *, wallet_address, coin, start_ts) -> Awaitable``
      — per-settlement funding deltas (``usdc`` signed amount, negative = paid).

    Both fetch methods receive ``servicer`` so the venue REST client + shared
    aiohttp session stay on the gateway service; the connector body holds only
    venue-specific request/response encoding. The return types are ``Any``
    because the ``PerpFillData`` / ``PerpFundingData`` dataclasses live gateway-
    side (coupling ``_base/`` to gateway internals would break the foundation's
    leaf-of-the-import-graph invariant).

    "No silent zeros" (matching ``RateHistoryService``): a "no data" path returns
    an empty list (a measured empty book), NEVER a fabricated fill; the framework
    reader treats an empty list as "no fills for this window", distinct from an
    RPC failure which the servicer surfaces as ``success=false``.
    """

    def fills_venue(self) -> str: ...

    async def fetch_user_fills(
        self,
        servicer: Any,
        *,
        wallet_address: str,
        coin: str = "",
        start_ts: int = 0,
    ) -> Any: ...

    async def fetch_user_funding(
        self,
        servicer: Any,
        *,
        wallet_address: str,
        coin: str = "",
        start_ts: int = 0,
    ) -> Any: ...


@runtime_checkable
class GatewayDefillamaSlugCapability(Protocol):
    """Connector publishes its DefiLlama project slug.

    Replaces the hardcoded ``_PROTOCOL_TO_LLAMA`` dispatch dict in
    ``almanak.gateway.services.pool_analytics_service`` (VIB-4811 /
    Phase 3).

    Returns the canonical DefiLlama slug for the connector's protocol
    (e.g. Aave v3 returns ``"aave-v3"``, Aerodrome returns
    ``"aerodrome-v1"``). The dispatcher keys the result by
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
class GatewayAddressCapability(Protocol):
    """Connector owns its on-chain contract addresses per chain.

    Replaces the centralised
    ``almanak.core.contracts`` registry (W1 / VIB-4853 / epic VIB-4851).
    Each connector folder now publishes its own ``addresses.py`` module
    holding the protocol → contract-kind → address mapping; the
    capability is the gateway-side adapter that lets non-connector
    callers ask "what's Aave V3's pool address on Arbitrum?" without
    importing ``almanak.connectors.aave_v3`` by name.

    Contract:

    * ``addresses_for(chain) -> Mapping[contract_kind, address]`` —
      return the ``{contract_kind: address}`` mapping for ``chain``,
      keyed by the connector's own internal contract-kind vocabulary
      (e.g. ``"swap_router"``, ``"position_manager"``, ``"pool"``,
      ``"morpho"``). Return an empty mapping when the connector
      doesn't support the chain. Callers must NOT assume any specific
      key is present — the kind vocabulary is per-connector and may
      grow over time.
    * ``address_supported_chains() -> frozenset[str]`` — the chains
      for which ``addresses_for`` returns a non-empty mapping.
      Provided so callers can enumerate without speculatively asking
      for every registered chain. Returning an empty frozenset is
      legal (and means the connector currently ships no addresses —
      e.g. a Solana-native connector that resolves accounts at runtime).
      The name is namespaced (vs. plain ``supported_chains``) because
      several connectors already implement ``GatewayDexQuoteCapability``
      whose ``supported_chains`` carries different semantics (chains
      where the DEX is queryable for quotes); collapsing the two would
      silently bind the wrong list to one of the capabilities.

    The connector's protocol name is read by the dispatcher from the
    base ``GatewayConnector.protocol`` ClassVar; this Protocol only
    contributes the per-chain address tables.

    The strategy-side ``ContractRegistry`` reads through this
    capability via
    ``GATEWAY_REGISTRY.capability_providers(GatewayAddressCapability)``
    rather than importing each connector's dict by name — the
    cross-cutting knowledge it used to require disappears.
    """

    def addresses_for(self, chain: str) -> Mapping[str, str]: ...

    def address_supported_chains(self) -> frozenset[str]: ...


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


# ``SupportedAction`` / ``SupportedActionsCapability`` lived here during an
# earlier W4 iteration that planned to consume matrix data from the gateway
# registry. The strategy-side import boundary
# (``tests/static/test_strategy_import_boundary.py``) forbids the matrix CLI
# (under ``almanak/framework/cli/``) from reading anything in this module, so
# the gateway-side capability was unreachable from the only consumer that
# needed it. Matrix data now lives strategy-side on
# :class:`almanak.connectors._strategy_base.registry.MatrixEntry` and
# ``ConnectorManifest.matrix_entries``; see
# ``almanak/framework/cli/support_matrix.py`` for the consumer.


# =============================================================================
# W7 / VIB-4859 — RateHistoryCapability cluster
# =============================================================================
#
# Three sibling capabilities + one complementary capability collapse the
# per-protocol ``if protocol == "aave_v3" elif "compound_v3" elif ...``
# dispatch in ``framework/data/rates/{monitor,history}.py`` and
# ``framework/backtesting/pnl/providers/{lending_apy,twap,dex/*}.py`` and
# move the HTTP / Web3 egress out of the strategy container into the
# gateway sidecar (where egress is the correct layer per
# ``AGENTS.md`` §"Gateway boundary").
#
# Plan PR #2473 §3 picked three sibling Protocols over one
# kind-discriminated Protocol because the three data shapes don't share
# an addressing scheme:
#
# * lending is keyed by ``(chain, asset_symbol, side, window)``
# * perp funding is keyed by ``(venue, market, window)``
# * DEX TWAP is keyed by ``(chain, pool_address, window)``
#
# Forcing all three through one Protocol with a ``kind`` enum hides
# not-applicable parameters and pushes runtime validation into each
# connector. Three sibling Protocols + three matching gRPC RPCs let
# every connector declare only what it actually serves; a DEX connector
# never sees lending-shaped requests and a lending connector never
# sees DEX-TWAP-shaped requests.
#
# ``GatewayDexVolumeCapability`` is the complementary fourth capability
# that lives alongside ``GatewayDexTwapCapability`` because both fan
# out across the same DEX connectors. Pulling it in together avoids a
# second round of touching every DEX connector to add volume support
# later (per the decision recorded on VIB-4859 2026-05-27).
#
# "No silent zeros" rule (matching ``PoolHistoryService`` / VIB-4727):
# any "no data" path raises (gateway side) or surfaces as
# ``success=false`` on the proto envelope (wire), which the framework
# reader translates to :class:`DataSourceUnavailable`. Never substitute
# ``Decimal("0")`` for "unmeasured", never substitute a default value
# for "upstream returned empty".


@runtime_checkable
class GatewayLendingRateHistoryCapability(Protocol):
    """Lending connector publishes APY / utilisation history + live rate.

    Replaces the hardcoded
    ``if protocol == "aave_v3" elif "compound_v3" elif "morpho_blue":``
    dispatch in:

    * ``almanak/framework/data/rates/monitor.py`` (live, on-chain
      ``eth_call`` against ``AaveProtocolDataProvider`` / Comet).
    * ``almanak/framework/data/rates/history.py`` (TheGraph subgraph
      crawl, with DefiLlama fallback).
    * ``almanak/framework/backtesting/pnl/providers/lending_apy.py`` (and
      its sibling sub-package ``pnl/providers/lending/``).

    Live + historical live on the same Protocol because every connector
    that knows the historical data source for an ``(asset, chain)`` also
    knows the live one. Splitting them per Alt C in the plan PR would
    force two registrations for one underlying knowledge.

    Contract:

    * ``lending_supported_chains() -> frozenset[str]`` — the chains the
      connector serves. Empty set is legal (connector registered for a
      sibling capability while lending coverage is staged in).
    * ``fetch_lending_current(*, servicer, chain, asset_symbol, side,
      market_id=None) -> LendingRatePoint`` — single-point live rate.
      ``servicer`` carries the gateway-side HTTP session, web3 cache,
      settings; the connector body stays free of gateway plumbing.
      ``market_id`` scopes the read on isolated-market venues — see the
      per-argument contract below.
    * ``fetch_lending_history(*, servicer, chain, asset_symbol, side,
      start_ts, end_ts) -> list[LendingRatePoint]`` — time-series
      history, ascending timestamps, NEVER fake-success with an empty
      list (raise from the connector when the window has no upstream
      data; the dispatcher converts to a ``success=false`` envelope).

    ``side`` is the literal ``"supply"`` or ``"borrow"`` string a
    strategy submits via ``RateMonitor.get_lending_rate`` — same
    vocabulary the existing framework consumers already use.

    ``market_id`` (VIB-5729) scopes ``fetch_lending_current`` to ONE market on
    isolated-market lenders, where ``asset_symbol`` alone is ambiguous (several
    Morpho Blue markets can lend the same loan token at different rates).
    Contract for implementers:

    * ``None`` / empty — unscoped. Keep the venue's existing behaviour.
    * Non-empty and the venue is market-scoped — read THAT market only, and set
      ``LendingRatePoint.market_id`` to the market actually read. Raise
      ``RateHistoryUnavailable`` for an unknown market; never silently widen to
      another market.
    * Non-empty but the venue is NOT market-scoped (the Aave family) — ignore it
      and leave ``LendingRatePoint.market_id`` unset. The caller sees no echo and
      fails closed to unmeasured, which is the honest outcome.

    Implementations MUST accept the keyword even when they ignore it: the
    dispatcher passes it uniformly, so omitting it from the signature is a
    ``TypeError`` at runtime, not a graceful degradation.

    The ``LendingRatePoint`` return type is the dataclass declared in
    ``almanak.gateway.services._rate_history_models`` (gateway-side, so
    it can carry validator + serializer plumbing without polluting
    ``_base/``). The connector receives it via the ``servicer`` argument
    rather than importing it directly to keep ``_base/`` clean of
    gateway internals.

    The connector's protocol name is read by the dispatcher from the
    base ``GatewayConnector.protocol`` ClassVar; this Protocol only
    contributes the chain list + the two fetch methods.
    """

    def lending_supported_chains(self) -> frozenset[str]: ...

    async def fetch_lending_current(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
        market_id: str | None = None,
    ) -> Any: ...

    async def fetch_lending_history(
        self,
        servicer: Any,
        *,
        chain: str,
        asset_symbol: str,
        side: str,
        start_ts: int,
        end_ts: int,
    ) -> Any: ...


@runtime_checkable
class GatewayFundingHistoryCapability(Protocol):
    """Perp connector publishes historical funding-rate series.

    Sibling of the existing :class:`GatewayFundingRateCapability` (which
    publishes the *live* rate). Replaces the hardcoded
    ``if venue in ("hyperliquid", "gmx_v2"):`` dispatch in
    ``almanak/framework/data/rates/history.py`` and the duplicated
    egress in ``almanak/framework/backtesting/pnl/providers/perp/``.

    Contract:

    * ``funding_venue() -> str`` — the venue identifier matching
      :meth:`GatewayFundingRateCapability.venue` so both capabilities on
      the same connector agree on identity.
    * ``funding_supported_markets() -> frozenset[str]`` — markets the
      connector serves for the historical lane (e.g. ``"ETH-USD"``,
      ``"BTC-USD"``). Empty set is legal.
    * ``fetch_funding_history(*, servicer, market, chain, start_ts,
      end_ts) -> list[FundingRatePoint]`` — ascending timestamps,
      never fake-success with empty (raise from the connector when the
      upstream window is empty).

    Cross-venue fallback (GMX V2 historical funding is served by
    Hyperliquid since GMX has no historical API) is handled by the
    dispatcher in ``RateHistoryService``, not by the capability — the
    GMX connector declares an empty market set for its own historical
    endpoint and the dispatcher fans out to siblings on
    ``DataSourceUnavailable``.
    """

    def funding_venue(self) -> str: ...

    def funding_supported_markets(self) -> frozenset[str]: ...

    async def fetch_funding_history(
        self,
        servicer: Any,
        *,
        market: str,
        chain: str,
        start_ts: int,
        end_ts: int,
    ) -> Any: ...


@runtime_checkable
class GatewayOrderStatusCapability(Protocol):
    """Perp connector publishes fill-vs-submission order-status lookup (VIB-5597).

    For venues that settle **asynchronously** (an EVM submit tx does not carry
    the fill), a strategy cannot trust submission success — it must confirm the
    fill against the venue. Hyperliquid CoreWriter is the canonical case: the
    order settles off-EVM on HyperCore and is addressable by a deterministic
    client-order-id (``cloid``) via the HyperCore Info API (``orderStatus``).

    This capability puts that API egress on the **gateway side** (where third-
    party HTTP correctly lives — gateway-boundary rule); strategy/framework code
    reads through the gateway. The connector's pure request-builder + response-
    parser live in the connector module (e.g.
    ``almanak.connectors.hyperliquid.fill_reconciliation``); this method runs
    that parser internally and returns an already-parsed, neutral gateway-side
    verdict — so the gateway holds NO connector import (symmetric with
    :meth:`GatewayPerpFillsCapability.fetch_user_fills` returning a typed
    ``PerpFillResult``; blueprint 22 §connector self-containment).

    Contract:

    * ``order_status_venue() -> str`` — venue identifier, matching
      :meth:`GatewayFundingRateCapability.venue` for the same connector.
    * ``fetch_order_status(servicer, *, wallet_address, cloid, chain)`` — returns
      a gateway-side ``OrderStatusData`` (``almanak.gateway.services.perp_fill_service``):
      the connector fetches the raw HyperCore ``orderStatus`` payload, runs its
      own pure parser, and returns the neutral verdict (``status`` +
      Empty ≠ Zero ``filled_size`` / ``avg_fill_price`` strings). Raises on
      transport failure so the caller fail-closes to ``UNMEASURED`` (Empty ≠ Zero
      — never assume filled). Typed ``Any`` here to keep ``_base/`` free of the
      gateway import, exactly like ``fetch_user_fills``.
    """

    def order_status_venue(self) -> str: ...

    async def fetch_order_status(
        self,
        servicer: Any,
        *,
        wallet_address: str,
        cloid: int,
        chain: str,
    ) -> Any: ...


@runtime_checkable
class GatewayDexTwapCapability(Protocol):
    """DEX connector publishes TWAP price + TWAP series (natively-supported only).

    Replaces the hardcoded pool-table dispatch in
    ``almanak/framework/backtesting/pnl/providers/twap.py``. Today twap.py
    only supports Uniswap V3 ``observe()``; the W7 fan-out adds the V3-style
    DEX forks (PancakeSwap V3, SushiSwap V3, Aerodrome Slipstream) which
    expose the same ``observe()`` ABI.

    **Natively-supported only.** Connectors whose underlying AMM has no
    TWAP primitive (Curve StableSwap, Balancer V2 weighted, TraderJoe V2
    Liquidity Book) MUST NOT implement this capability — fabricating TWAP
    via event-log reconstruction is out of scope for W7 (VIB-4859 §2.2).

    Contract:

    * ``dex_name() -> str`` — the DEX identifier (e.g. ``"uniswap_v3"``,
      ``"sushiswap_v3"``). This is the routing key ``RateHistoryService``
      uses to dispatch ``GetDexTwap`` / ``GetDexTwapSeries`` by
      ``request.dex``; it MUST match the string callers pass and the
      ``GatewayDexQuoteCapability.dex_name()`` for the same DEX. Declared
      here (not just on the quote capability) so the registry's structural
      Protocol check enforces it — otherwise a TWAP provider missing the
      method would slip through registration and ``AttributeError`` at
      dispatch-table build time.
    * ``twap_supported_chains() -> frozenset[str]`` — chains where the
      DEX exposes TWAP. Empty set is legal during fan-out staging.
    * ``fetch_twap(*, servicer, chain, pool_address, secs_ago_start,
      secs_ago_end, as_of_block) -> DexTwapPoint`` — single TWAP
      observation over the requested window.
    * ``fetch_twap_series(*, servicer, chain, pool_address, start_ts,
      end_ts, interval_secs) -> list[DexTwapPoint]`` — TWAP samples at
      ``interval_secs`` spacing. Connectors may down-sample upstream and
      return at the requested resolution.

    The connector receives ``servicer`` so the DEX-specific archive-RPC
    cache and web3 helpers stay on the service and the capability body
    holds only protocol-specific encoding (function selectors, return
    decoding, sqrtPriceX96 → price math).
    """

    def dex_name(self) -> str: ...

    def twap_supported_chains(self) -> frozenset[str]: ...

    async def fetch_twap(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        secs_ago_start: int,
        secs_ago_end: int,
        as_of_block: int | None = None,
    ) -> Any: ...

    async def fetch_twap_series(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
    ) -> Any: ...


@runtime_checkable
class GatewayDexLwapCapability(Protocol):
    """DEX connector publishes liquidity-weighted spot price (LWAP) over pools.

    The L3 follow-up to ``GatewayDexTwapCapability`` (VIB-4948 / ALM-2770).
    Where ``GetDexTwap`` is a single-pool time-weighted oracle read,
    ``GetDexLwap`` is a multi-pool liquidity-weighted *spot* read:
    ``LWAP = Σ(price_i · liquidity_i) / Σ(liquidity_i)`` over the supplied,
    already-resolved pools. Pool resolution stays framework-side (the caller
    passes pool addresses); the connector body only reads slot0 + in-range
    liquidity per pool and the service owns the web3 helpers.

    **V3-style only.** Pools must expose the Uniswap-V3 ``slot0()`` +
    ``liquidity()`` ABI (Uniswap V3 + its forks PancakeSwap V3, SushiSwap V3,
    Aerodrome Slipstream). The read is uniform across these because the
    framework resolves the per-DEX pool addresses; the connector just decodes
    ``sqrtPriceX96`` → price and weights by liquidity.

    Contract:

    * ``dex_name() -> str`` — the DEX identifier and the routing key
      ``RateHistoryService`` uses to dispatch ``GetDexLwap`` by
      ``request.dex``; MUST match the connector's other DEX capability names.
    * ``lwap_supported_chains() -> frozenset[str]`` — chains where the DEX
      exposes V3-style pools the connector can read.
    * ``fetch_lwap(*, servicer, chain, pool_addresses, min_liquidity,
      as_of_block, base_token, quote_token) -> DexLwapPoint`` — the
      liquidity-weighted price across the readable pools. When ``base_token`` /
      ``quote_token`` addresses are supplied, pools not containing exactly that
      pair are dropped (so one stale/foreign-pair pool address cannot poison the
      aggregate). Never fake-success with an empty / zero price.

    The connector receives ``servicer`` so the per-chain web3 cache and
    archive-RPC helpers stay on the service and the capability body holds only
    protocol-specific selector + sqrtPriceX96 → price math.
    """

    def dex_name(self) -> str: ...

    def lwap_supported_chains(self) -> frozenset[str]: ...

    async def fetch_lwap(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_addresses: list[str],
        min_liquidity: str = "",
        as_of_block: int | None = None,
        base_token: str = "",
        quote_token: str = "",
    ) -> Any: ...


@runtime_checkable
class GatewayDexVolumeCapability(Protocol):
    """DEX connector publishes historical trading-volume series.

    Replaces the per-DEX duplicate egress in
    ``almanak/framework/backtesting/pnl/providers/dex/`` — eight files
    (one per DEX) that each open their own aiohttp session and hit
    their own subgraph for ``volume_24h_usd``.

    Pulled into the W7 wave (alongside ``GatewayDexTwapCapability``) so
    every DEX connector is touched exactly once for read-side data
    capabilities. The shape matches ``GatewayDexTwapCapability``'s
    ``fetch_*_series`` so DEX connectors stay symmetric.

    Contract:

    * ``dex_name() -> str`` — the DEX identifier (e.g. ``"uniswap_v3"``).
      The routing key ``RateHistoryService`` uses to dispatch
      ``GetDexVolumeHistory`` by ``request.dex``; MUST match the
      ``GatewayDexQuoteCapability.dex_name()`` for the same DEX. Declared
      here so the registry's structural Protocol check enforces it (a
      volume provider missing the method would otherwise slip through
      registration and ``AttributeError`` at dispatch-table build time).
    * ``volume_supported_chains() -> frozenset[str]`` — chains where the
      DEX exposes trading-volume history (almost always = the connector's
      subgraph coverage).
    * ``fetch_volume_history(*, servicer, chain, pool_address, start_ts,
      end_ts, interval_secs) -> list[DexVolumePoint]`` — ascending
      timestamps, never fake-success with empty.

    A DEX MAY implement only TWAP, only volume, or both. Connectors that
    implement neither (e.g. an EVM connector whose backtest providers
    haven't been migrated yet) simply do not declare either capability.
    """

    def dex_name(self) -> str: ...

    def volume_supported_chains(self) -> frozenset[str]: ...

    async def fetch_volume_history(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
    ) -> Any: ...


@dataclass(frozen=True)
class PrincipalTokenMarketRef:
    """Resolved on-chain identity of a principal-token (PT/YT/LP) symbol.

    Returned by :class:`GatewayPrincipalTokenPriceCapability` so the gateway
    can compose a PT/YT-USD price (VIB-5310) WITHOUT the gateway holding any
    protocol-specific symbol→market knowledge. The connector owns the static
    metadata; this struct is the protocol-agnostic hand-off.

    Attributes:
        protocol: Owning connector's protocol name (e.g. ``"pendle"``). The
            gateway uses it to build the protocol's on-chain market reader via
            ``PRINCIPAL_TOKEN_MARKET_READ_REGISTRY``.
        market_address: The PT/YT market contract address (RouterStatic key for
            ``getPtToAssetRate`` / ``expiry``).
        underlying_token: The token whose USD price denominates the composition
            (``pt_usd = underlying_price × pt_to_asset_rate``). Address or
            symbol — priced through the existing ``MarketService`` aggregator.
            MUST be non-empty: a ref with no priceable underlying is unmeasured,
            so the connector returns ``None`` from ``resolve_*`` instead.
        family: ``"PT"``, ``"YT"`` or ``"LP"``. The gateway returns
            ``UNMEASURED`` for ``"YT"`` in M1 (held-YT valuation is VIB-5322/M3).
        maturity_ts: Unix seconds of contract maturity, or 0 when not statically
            known (the gateway reads days-to-maturity on-chain regardless).
    """

    protocol: str
    market_address: str
    underlying_token: str
    family: str
    maturity_ts: int = 0


@runtime_checkable
class GatewayPrincipalTokenPriceCapability(Protocol):
    """Connector resolves a PT/YT symbol to its market + underlying for pricing.

    The single source of the gateway's PT/YT-USD price authority (VIB-5310,
    epic VIB-5299, M1). The gateway is the *price* authority — it sources both
    composition legs (``pt_to_asset_rate`` via the connector's on-chain market
    reader, underlying/USD via the existing price aggregator), composes, and
    stamps confidence — but it holds NO protocol-specific symbol→market table.
    That knowledge stays connector-owned behind this capability, so a second
    principal-token protocol plugs in by implementing it (capability dispatch,
    never a protocol-name literal in ``market_service``).

    Contract:

    * ``principal_token_price_chains() -> frozenset[str]`` — chains on which
      ``resolve_principal_token_ref`` can return a ref. Empty is legal.
    * ``resolve_principal_token_ref(*, symbol, chain, maturity_ts=0)
      -> PrincipalTokenMarketRef | None`` — resolve a canonical PT/YT symbol
      (case-insensitive; the *symbol* is the identity + join key, design-spine
      §3) to its market + underlying. Return ``None`` when the symbol is
      unknown OR has no priceable underlying — the gateway maps ``None`` to
      ``UNMEASURED`` (Empty≠Zero: expected-no-data, never a fabricated price).
      MUST NOT perform network egress: resolution is from connector-owned
      static metadata only.
    """

    def principal_token_price_chains(self) -> frozenset[str]: ...

    def resolve_principal_token_ref(
        self,
        *,
        symbol: str,
        chain: str,
        maturity_ts: int = 0,
    ) -> PrincipalTokenMarketRef | None: ...

    def build_principal_token_market_reader(self, *, chain: str, rpc_client: Any) -> Any | None:
        """Build the connector's on-chain market reader (``get_pt_to_sy_rate``,
        ``get_market_expiry_ts``) for ``chain``, or ``None`` if unsupported.

        Returned here — on the same capability as resolution — so the gateway
        never reaches into a strategy-side connector registry to construct it
        (gateway/connector isolation, VIB-4121).

        ``rpc_client`` is a gateway-supplied eth_call transport injected into the
        reader's gateway-mode seam (VIB-5348). It must duck-type the reader's
        ``gateway_client``: expose ``rpc.Call(request, timeout=...)`` returning an
        object with ``.success`` / ``.result`` / ``.error``, backed by the
        gateway's own audited async egress (e.g.
        :class:`~almanak.gateway.services.pt_rpc_adapter.GatewayPtRpcClient`).
        The connector forwards it straight into the reader so the read happens
        inside the egress layer with NO connector-side ``Web3(HTTPProvider(...))``
        — the gateway never instantiates a raw provider on the hosted perimeter.
        """
        ...
