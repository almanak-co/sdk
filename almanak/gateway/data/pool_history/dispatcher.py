"""PoolHistoryDispatcher (POOL-5 / VIB-4753) — provider fallback orchestration.

The dispatcher owns:

* The shared ``aiohttp.ClientSession`` (lazy; gateway SSL context) consumed by
  the REST providers, and the ``GatewayGraphQLClient`` for The Graph.
* Registry-driven URL + slug resolvers (decision #1: subgraph URLs come from
  ``GatewaySubgraphCapability`` and DefiLlama slugs from
  ``GatewayDefillamaSlugCapability`` — NOT a hardcoded ``_SUBGRAPH_URLS`` dict).
* The per-provider ``_TokenBucket`` rate limiters + the TheGraph monthly-budget
  breaker.
* The pure ``eligible_providers(resolution)`` table (exposed for D2.M3.b) and
  ``is_supported(chain, protocol)``.
* The ``dispatch()`` fallback loop with the 3-state provider taxonomy.

Finality (decision #5) is isolated in ``_compute_finality`` — POOL-6 (VIB-4754)
fills it with real per-provider cutoffs, selecting the raw-cache band for each
provider's response (``provisional`` when the newest row is within the
provider's cutoff, else ``finalized``).

Truncation (decision #8) is owned by the SERVICER, not the dispatcher: the
servicer clamps the window to the soft cap BEFORE calling ``dispatch`` (which
sees only the already-clamped ``[start_ts, end_ts)``), and classifies
``CAP_EXCEEDED`` / ``PROVIDER_PAGE_CAP`` / ``PROVIDER_RETENTION`` on the result
(``_history_common.classify_truncation``). The dispatcher returns the full
ascending provider window untruncated.

The dispatcher mutates a caller-supplied ``counters`` callback bundle so the
servicer's ``health()`` reflects ``provider_fallback`` + per-provider
``requests``/``errors`` + ``the_graph_monthly_queries`` (decision #7:
increment structurally; do NOT add new health() keys).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import aiohttp

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.utils.ssl_context import build_ssl_context

from ._base import (
    PoolHistoryProvider,
    _MonthlyBudgetTracker,
    _NotAttempted,
    _ObservableTokenBucket,
    _ProviderError,
)
from ._graphql import GatewayGraphQLClient
from .defillama import DefiLlamaPoolHistoryProvider
from .geckoterminal import GeckoTerminalPoolHistoryProvider
from .thegraph import TheGraphPoolHistoryProvider

logger = logging.getLogger(__name__)


# =============================================================================
# Rate-limit module constants (decision #2: module constants, not settings)
# =============================================================================

#: The Graph: single shared bucket. Spike R3 — quota is per-account, not
#: per-subgraph, so one bucket gates all subgraph traffic. 2 req/s.
_THEGRAPH_RATE_PER_S = 2
#: DefiLlama public tier: 10 req/s per IP.
_DEFILLAMA_RATE_PER_S = 10
#: CoinGecko Onchain fallback bucket: keep the legacy 30 req/min throttle.
_GECKOTERMINAL_RATE_PER_MIN = 30

#: Per-provider finality cutoff defaults (seconds), POOL-6 (VIB-4754). DefiLlama
#: revises daily data >24h after the fact (PoolX.md §D4), so its default is 72h
#: vs 24h for The Graph / CoinGecko Onchain. These are baked into the dispatcher so a
#: direct ``PoolHistoryDispatcher(...)`` (e.g. a test) still classifies DefiLlama
#: with the 72h contract even when ``finality_cutoffs`` is omitted; the servicer
#: passes the settings-derived map which overrides these.
_DEFAULT_FINALITY_CUTOFFS: dict[str, int] = {
    "the_graph": 86400,
    "defillama": 259200,
    "geckoterminal": 86400,
}
#: Ultimate fallback for an unknown provider id absent from the merged map.
_DEFAULT_FINALITY_CUTOFF_SECONDS = 86400


# =============================================================================
# Counter bundle — the servicer wires these into its health() store
# =============================================================================


@dataclass
class _DispatchCounters:
    """Callbacks the dispatcher invokes to keep ``health()`` live.

    Kept as a struct of callables (rather than a direct dict mutation) so the
    servicer owns its ``_metrics`` shape and the dispatcher stays decoupled
    from the locked health() schema (decision #7: populate existing keys only).

    ``on_provider_throttle_wait`` (POOL-8 / VIB-4756) is invoked once per
    bucket refusal — for BOTH the TheGraph primary path (refusal becomes
    ``_ProviderError``) AND the DefiLlama / CoinGecko Onchain fallback path
    (refusal becomes ``_NotAttempted`` silent skip) — with the THEORETICAL
    ms-until-next-token computed at provider-bucket construction. Default
    is a no-op so callers that don't care about throttle accounting don't
    need to construct the callback.
    """

    on_provider_request: Callable[[str], None] = field(default=lambda _name: None)
    on_provider_error: Callable[[str], None] = field(default=lambda _name: None)
    on_provider_fallback: Callable[[], None] = field(default=lambda: None)
    on_provider_throttle_wait: Callable[[str, int], None] = field(default=lambda _name, _ms: None)


# =============================================================================
# Dispatch result
# =============================================================================


@dataclass
class _DispatchOutcome:
    """Internal result of a ``dispatch()`` call.

    ``snapshots`` is non-empty + ``source`` set on success; on exhaustion
    ``snapshots`` is empty, ``source == ""`` and ``error`` is the joined
    provider error string.
    """

    success: bool
    source: str
    snapshots: list[gateway_pb2.PoolSnapshot]
    error: str


# =============================================================================
# Dispatcher
# =============================================================================


class PoolHistoryDispatcher:
    """Routes a pool-history request through eligible providers in order."""

    def __init__(
        self,
        *,
        thegraph_api_key: str | None,
        thegraph_monthly_budget_max: int,
        is_supported_fn: Callable[[str, str], bool],
        finality_cutoffs: dict[str, int] | None = None,
        coingecko_api_key: str | None = None,
        clock: Callable[[], float] = time.time,
        counters: _DispatchCounters | None = None,
    ) -> None:
        self._counters = counters or _DispatchCounters()
        self._is_supported_fn = is_supported_fn
        # Per-provider finality cutoff (seconds), POOL-6 (VIB-4754): selects the
        # raw-cache band for each provider's response. The provider-specific
        # defaults are merged UNDER any caller-supplied overrides, so an omitted
        # ``finality_cutoffs`` still honours DefiLlama's 72h contract (not a flat
        # 24h). ``clock`` is injectable so tests can pin "now" for the finality
        # boundary without monkeypatching ``time.time``; production passes
        # ``time.time``.
        self._finality_cutoffs = {**_DEFAULT_FINALITY_CUTOFFS, **(finality_cutoffs or {})}
        self._clock = clock

        self._http_session: aiohttp.ClientSession | None = None
        self._graphql = GatewayGraphQLClient(api_key=thegraph_api_key)

        # Per-provider bucket-refusal accounting (POOL-8 / VIB-4756). The
        # theoretical ms-until-next-token is computed ONCE at construction
        # from the bucket's rate + period using the formula contract from
        # the UAT card §D2.M2.b.3 (``round(period * 1000.0 / rate)``); each
        # refusal bumps the per-provider counter by that constant. A test
        # that monkey-patches the module-level ``_THEGRAPH_RATE_PER_S`` to 1
        # constructs a fresh dispatcher, so the cached integer matches the
        # clamped rate — no test-time aliasing risk.
        #
        # The ``or 0`` guards a degenerate ``rate == 0`` config / monkey-
        # patch from raising ``ZeroDivisionError`` during construction
        # (Gemini-flagged 2026-05-28). A zero rate semantically means
        # "bucket is permanently throttled," for which "0 ms theoretical
        # wait" is correct (every refusal contributes 0 to the counter
        # because the operator has already disabled the provider — the
        # throttle-wait signal is meaningless in that mode).
        thegraph_throttle_ms = round(1.0 * 1000.0 / _THEGRAPH_RATE_PER_S) if _THEGRAPH_RATE_PER_S else 0
        defillama_throttle_ms = round(1.0 * 1000.0 / _DEFILLAMA_RATE_PER_S) if _DEFILLAMA_RATE_PER_S else 0
        geckoterminal_throttle_ms = (
            round(60.0 * 1000.0 / _GECKOTERMINAL_RATE_PER_MIN) if _GECKOTERMINAL_RATE_PER_MIN else 0
        )

        def _bump_throttle(provider_name: str, ms: int) -> Callable[[], None]:
            # Bind the provider name + ms at the closure site so a single
            # callable carries everything the counter needs; the dispatcher
            # has zero ongoing knowledge of throttle accounting beyond
            # constructing the buckets.
            return lambda: self._counters.on_provider_throttle_wait(provider_name, ms)

        self._thegraph_bucket = _ObservableTokenBucket(
            rate=_THEGRAPH_RATE_PER_S,
            period=1.0,
            on_refusal=_bump_throttle("the_graph", thegraph_throttle_ms),
        )
        self._defillama_bucket = _ObservableTokenBucket(
            rate=_DEFILLAMA_RATE_PER_S,
            period=1.0,
            on_refusal=_bump_throttle("defillama", defillama_throttle_ms),
        )
        self._geckoterminal_bucket = _ObservableTokenBucket(
            rate=_GECKOTERMINAL_RATE_PER_MIN,
            period=60.0,
            on_refusal=_bump_throttle("geckoterminal", geckoterminal_throttle_ms),
        )
        self._budget = _MonthlyBudgetTracker(budget_max=thegraph_monthly_budget_max)

        self._thegraph = TheGraphPoolHistoryProvider(
            client=self._graphql,
            url_resolver=_resolve_subgraph_url,
            rate_limiter=self._thegraph_bucket,
            budget=self._budget,
        )
        self._defillama = DefiLlamaPoolHistoryProvider(
            session_getter=self._get_http_session,
            slug_resolver=_resolve_defillama_slug,
            rate_limiter=self._defillama_bucket,
        )
        self._geckoterminal = GeckoTerminalPoolHistoryProvider(
            session_getter=self._get_http_session,
            rate_limiter=self._geckoterminal_bucket,
            api_key=coingecko_api_key,
        )
        self._providers: dict[str, PoolHistoryProvider] = {
            self._thegraph.name: self._thegraph,
            self._defillama.name: self._defillama,
            self._geckoterminal.name: self._geckoterminal,
        }

    # -- HTTP session (shared by REST providers) --------------------------

    async def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15.0),
                connector=connector,
                headers={"Accept": "application/json"},
            )
        return self._http_session

    async def close(self) -> None:
        await self._graphql.close()
        if self._http_session is not None and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    # -- Budget read surface (servicer health() reads these) --------------

    @property
    def the_graph_monthly_queries(self) -> int:
        return self._budget.queries

    @property
    def the_graph_monthly_budget_max(self) -> int:
        return self._budget.budget_max

    # -- Eligibility table (pure; exposed for D2.M3.b) --------------------

    def eligible_providers(self, resolution: int) -> tuple[str, ...]:
        """Return the ordered eligible provider ids for ``resolution``.

        DefiLlama is daily-only, so it is excluded from the 1h / 4h chains
        (decision: never relabel daily data as sub-daily). ``UNSPECIFIED``
        raises ``ValueError`` — the validator should have rejected it first.
        """
        if resolution in (gateway_pb2.Resolution.RESOLUTION_1H, gateway_pb2.Resolution.RESOLUTION_4H):
            return ("the_graph", "geckoterminal")
        if resolution == gateway_pb2.Resolution.RESOLUTION_1D:
            return ("the_graph", "defillama", "geckoterminal")
        raise ValueError(f"unsupported resolution for dispatch: {resolution}")

    def is_supported(self, chain: str, protocol: str) -> bool:
        """Delegate to the registry-derived (chain, protocol) support table."""
        return self._is_supported_fn(chain, protocol)

    # -- Finality seam (decision #5 — POOL-6 fills with per-provider cutoffs)

    def _compute_finality(
        self,
        *,
        provider: str,
        snapshots: list[gateway_pb2.PoolSnapshot],
        now_seconds: int,
    ) -> tuple[str, bool]:
        """Return ``(finality_band, finalized_only)`` for a successful response.

        POOL-6 (VIB-4754): a response is ``finalized_only`` iff its newest row
        is older than the serving provider's configured finality cutoff
        (DefiLlama's is longer because it revises daily data >24h after the
        fact — PoolX.md §D4). A provisional response is written to the cache
        under the short-TTL ``provisional`` band so a later revision is
        re-fetched (or re-promoted once it ages past the cutoff). Isolated in
        this ONE helper so the cache band + the public response agree on the
        finality rule (this computes the RAW-cache band on the full provider
        response; the servicer recomputes for the public response after any
        page-cap slice, where the newest row may differ).

        ``_history_cache`` / ``_history_common`` are imported lazily here
        (rather than at module top) to break the import cycle: importing them
        eagerly would load ``almanak.gateway.services.__init__`` ->
        ``pool_history_service`` -> this package while it is still initializing.
        """
        from almanak.gateway.services._history_cache import FINALITY_FINALIZED, FINALITY_PROVISIONAL
        from almanak.gateway.services._history_common import compute_finalized_only

        cutoff = self._finality_cutoffs.get(provider, _DEFAULT_FINALITY_CUTOFF_SECONDS)
        newest_ts = max((int(s.timestamp) for s in snapshots), default=0)
        finalized_only = compute_finalized_only(newest_ts=newest_ts, now_seconds=now_seconds, cutoff_seconds=cutoff)
        return (FINALITY_FINALIZED if finalized_only else FINALITY_PROVISIONAL, finalized_only)

    # -- Fallback loop ----------------------------------------------------

    async def dispatch(
        self,
        *,
        chain: str,
        pool_address: str,
        protocol: str,
        start_ts: int,
        end_ts: int,
        resolution: int,
        on_provider_success: Callable[[str, list[gateway_pb2.PoolSnapshot], str], Awaitable[None]] | None = None,
    ) -> _DispatchOutcome:
        """Try each eligible provider in order; return the first success.

        3-state taxonomy per provider:
          * ``_NotAttempted`` -> local skip; continue (no error counter bump).
          * raised ``_ProviderError`` -> bump errors + fallback; append; continue.
          * ``None`` -> reached upstream, not found; append "not found" +
            fallback; continue.
          * non-empty list -> success: bump requests, run the raw-cache write
            callback, return.

        ``on_provider_success(provider, snapshots, finality_band)`` lets the
        servicer write the raw cache (8-tuple key incl. provider — D2.M4
        partition) before the public-cache ``get_or_fetch`` settles.

        A stray ``Exception`` from any provider is defensively converted to a
        ``_ProviderError`` so it can never abort the chain or surface as a
        gRPC ``UNKNOWN``.
        """
        # Defense-in-depth (audit Important #6): the gRPC servicer runs the
        # full validator before reaching dispatch, but ``dispatch()`` is a
        # reusable surface and ``pool_address`` is interpolated into provider
        # egress URLs (CoinGecko Onchain path / DefiLlama matcher). Never let an
        # unvalidated address reach a provider. Lazy import breaks the
        # services/__init__ -> pool_history_service -> this-package cycle
        # (same rationale as ``_compute_finality``).
        from almanak.gateway.services._history_common import validate_pool_address_syntax

        if not validate_pool_address_syntax(pool_address, chain):
            msg = f"invalid pool_address for chain {chain!r}: {pool_address!r}"
            logger.warning("PoolHistory dispatch rejected unvalidated address: %s", msg)
            return _DispatchOutcome(success=False, source="", snapshots=[], error=msg)

        errors: list[str] = []
        eligible = self.eligible_providers(resolution)
        for provider_name in eligible:
            provider = self._providers[provider_name]
            try:
                result = await provider.fetch(
                    chain=chain,
                    pool_address=pool_address,
                    protocol=protocol,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    resolution=resolution,
                )
            except _ProviderError as exc:
                self._counters.on_provider_error(provider_name)
                self._counters.on_provider_fallback()
                errors.append(f"{provider_name}: {exc}")
                logger.debug("PoolHistory provider %s error for %s/%s: %s", provider_name, chain, pool_address, exc)
                continue
            except Exception as exc:  # noqa: BLE001 - defensive: never let a bare exception abort the chain
                self._counters.on_provider_error(provider_name)
                self._counters.on_provider_fallback()
                errors.append(f"{provider_name}: unexpected error: {exc}")
                logger.warning(
                    "PoolHistory provider %s raised unexpected %s for %s/%s: %s",
                    provider_name,
                    type(exc).__name__,
                    chain,
                    pool_address,
                    exc,
                )
                continue

            if isinstance(result, _NotAttempted):
                # Local skip — not a failure, not a miss. Do not bump errors;
                # do not append to the user-facing error string.
                logger.debug("PoolHistory provider %s not attempted for %s/%s", provider_name, chain, pool_address)
                continue

            if result is None:
                # Reached upstream, queried, genuinely not found.
                errors.append(f"{provider_name}: not found")
                self._counters.on_provider_fallback()
                continue

            # Success — non-empty list (providers never return [] as success).
            self._counters.on_provider_request(provider_name)
            now_seconds = int(self._clock())
            finality_band, _ = self._compute_finality(provider=provider_name, snapshots=result, now_seconds=now_seconds)
            if on_provider_success is not None:
                await on_provider_success(provider_name, result, finality_band)
            return _DispatchOutcome(success=True, source=provider_name, snapshots=result, error="")

        joined = "; ".join(errors) or "all providers exhausted"
        logger.warning(
            "All providers failed for %s/%s (protocol=%s, resolution=%s): %s",
            chain,
            pool_address,
            protocol or "unspecified",
            resolution,
            joined,
        )
        return _DispatchOutcome(success=False, source="", snapshots=[], error=joined)


# =============================================================================
# Registry-driven resolvers (decision #1)
# =============================================================================


def _resolve_subgraph_url(protocol: str, chain: str) -> str | None:
    """Resolve the TheGraph subgraph URL for ``(protocol, chain)`` from the registry.

    The registry keys subgraph endpoints by the public alias
    ``<protocol-with-hyphens>-<chain>`` (e.g. ``uniswap_v3`` -> alias
    ``uniswap-v3-arbitrum``). Returns ``None`` when no endpoint is registered
    (e.g. Aerodrome publishes no ``GatewaySubgraphCapability`` -> falls
    through to the legacy ``geckoterminal`` provider key).
    """
    table = _subgraph_endpoint_table()
    alias = f"{protocol.replace('_', '-').lower()}-{chain.lower()}"
    return table.get(alias)


def _resolve_defillama_slug(protocol: str) -> str | None:
    """Resolve the DefiLlama project slug for ``protocol`` from the registry."""
    return _defillama_slug_table().get(protocol.lower())


def _subgraph_endpoint_table() -> dict[str, str]:
    """Union every connector's ``subgraph_endpoints()`` (lazy, cycle-safe).

    Imports are local: building eagerly at module import races against
    ``_gateway_registry`` registration (same rationale as the analytics
    service's ``_build_protocol_to_llama``).
    """
    from almanak.connectors._base.gateway_capabilities import GatewaySubgraphCapability
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    table: dict[str, str] = {}
    for connector in GATEWAY_REGISTRY.capability_providers(GatewaySubgraphCapability):  # type: ignore[type-abstract]
        for alias, url in connector.subgraph_endpoints().items():
            table[alias.lower()] = url
    return table


def _defillama_slug_table() -> dict[str, str]:
    """Union every connector's DefiLlama slug + aliases (lazy, cycle-safe)."""
    from almanak.connectors._base.gateway_capabilities import GatewayDefillamaSlugCapability
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    table: dict[str, str] = {}
    for connector in GATEWAY_REGISTRY.capability_providers(GatewayDefillamaSlugCapability):  # type: ignore[type-abstract]
        slug = connector.defillama_slug()
        if slug is not None:
            table[str(connector.protocol).lower()] = slug  # type: ignore[attr-defined]
        for alias_key, alias_slug in connector.defillama_slug_aliases().items():
            table[alias_key.lower()] = alias_slug
    return table


__all__ = [
    "PoolHistoryDispatcher",
    "_DispatchCounters",
    "_DispatchOutcome",
    "_resolve_defillama_slug",
    "_resolve_subgraph_url",
]
