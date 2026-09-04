"""PoolHistoryService skeleton + validator + cache (VIB-4728 POOL-2/3/4).

Server-side handler for the gateway-backed PoolHistoryReader migration.
Three tickets ship in this file:

* **POOL-2 (VIB-4750)** — Servicer skeleton + kill-switch + ``health()``
  schema lock. The servicer is registered on the gRPC server from day 1
  so the hosted-auth interceptor and telemetry surface engage in the
  same boot path as every peer service.

* **POOL-3 (VIB-4751)** — Request validator + pool-specific protocol
  allowlist + ``(chain, protocol)`` compatibility table. The validator
  rejects malformed requests with ``INVALID_ARGUMENT`` BEFORE any
  provider is consulted (and is independent of the kill-switch).

* **POOL-4 (VIB-4752)** — Two-tier cache instances are constructed on
  the servicer; ``health()`` reads live cache stats. The cache is not
  yet INVOKED from the handler — POOL-5 wires the dispatcher to call
  ``get_or_fetch`` on the public cache and to write raw-cache entries
  per provider.

Handler decision order:

  1. Kill-switch off  -> ``UNAVAILABLE`` (registered-but-disabled).
     The validator does NOT run when disabled; a strategy hitting the
     "not yet enabled" message gets a uniform error regardless of
     how malformed their request was.
  2. Validator fails  -> ``INVALID_ARGUMENT`` (validator-level fast path
     before any provider work, per UAT card D3.F6 + D3.F8).
  3. Validator passes -> ``UNIMPLEMENTED`` (POOL-5 / VIB-4753 replaces
     this branch with the real dispatcher + providers).

``health()`` exposes a counter-NAMES schema locked here. POOL-8
(VIB-4756) populates VALUES; new metric keys MUST go through that
ticket so the ``health()`` shape is a stable observability contract.
The keyset mirrors the umbrella UAT card
(``docs/internal/uat-cards/VIB-4728.md``) D2.M2 / D3.F11 / D2.M4
telemetry assertions.

Soft-cap behavior (POOL-6 / VIB-4754) — when ``end_ts - start_ts``
exceeds the configured soft cap, the handler returns
``success=True`` with ``truncation_reason=CAP_EXCEEDED`` and
``next_start_ts > 0`` so the caller re-chunks. The validator does
NOT reject for soft-cap; cap behavior is a handler-side decision
locked in the UAT card §"Soft-cap vs hard-cap behavior".

No HTTP / gRPC / GraphQL egress occurs in this module — that all
lands in POOL-5. Mid-flight termination is safe (no writes; mirrors
VIB-4727 PoolAnalyticsService scope rationale).
"""

from __future__ import annotations

import logging
import time
from typing import cast

import grpc

from almanak.core.finality import CacheFinality
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.data.pool_history import (
    PoolHistoryDispatcher,
    _DispatchCounters,
    _ProviderError,
)
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.services._history_cache import (
    FINALITY_FINALIZED,
    FINALITY_PROVISIONAL,
    HistoryCache,
    PoolHistoryPublicKey,
    PoolHistoryRawKey,
    extract_provider_from_raw_key,
    load_max_bytes_from_settings,
    load_max_entries_from_settings,
    make_public_key,
    make_raw_key,
)
from almanak.gateway.services._history_common import (
    END_TS_FUTURE_TOLERANCE_SECONDS,
    ValidationFailure,
    classify_truncation,
    compute_finalized_only,
    get_soft_cap_seconds,
    invalid_argument,
    normalize_pool_address,
    resolution_to_seconds,
    validate_pool_address_syntax,
)

logger = logging.getLogger(__name__)


def _derive_pool_history_tables() -> tuple[frozenset[str], frozenset[tuple[str, str]]]:
    """Compute ``(POOL_PROTOCOL_ALLOWLIST, SUPPORTED_POOL_PAIRS)`` from the registry.

    Iterates every ``GatewayPoolHistoryCapability`` provider once and
    unions their declared chains. The result is a snapshot — the
    module-level ``POOL_PROTOCOL_ALLOWLIST`` / ``SUPPORTED_POOL_PAIRS``
    constants build themselves lazily on first access (see
    ``_LazyAllowlistProxy`` / ``_LazyPairsProxy``) because eager build
    at module import races against ``_gateway_registry`` registration
    — when an entry point lands on ``_gateway_registry`` first and the
    aave_v3 / uniswap_v3 / etc. provider modules transitively pull in
    ``gateway.services.__init__`` (and through it this module) before
    registration finishes, the snapshot would be empty.

    Imports are local to make the deferred build cycle-safe.
    """
    from almanak.connectors._base.gateway_capabilities import (
        GatewayPoolHistoryCapability,
    )
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    allowlist: set[str] = set()
    pairs: set[tuple[str, str]] = set()
    # mypy: ``capability_providers`` is a generic runtime helper —
    # passing a ``@runtime_checkable`` Protocol class trips
    # ``type-abstract``. The Protocol is intentional: the registry
    # filters by isinstance check, not by abstract-class instantiation.
    for connector in GATEWAY_REGISTRY.capability_providers(GatewayPoolHistoryCapability):  # type: ignore[type-abstract]
        # ``connector.protocol`` is declared on ``GatewayConnector``;
        # mypy narrows to the Protocol type which doesn't list it.
        protocol = str(connector.protocol).lower()  # type: ignore[attr-defined]
        allowlist.add(protocol)
        for chain in connector.pool_history_supported_chains():
            # Registry declarations and request values share one lowercase form.
            pairs.add((chain.lower(), protocol))
    return frozenset(allowlist), frozenset(pairs)


class _LazyFrozenset(frozenset):
    """Marker base for the proxy classes below — exists only so callers
    that do ``isinstance(x, frozenset)`` keep returning True.
    """


class _LazyAllowlistProxy(_LazyFrozenset):
    __slots__ = ()  # frozenset is __slots__-friendly; state lives module-level

    def __new__(cls) -> _LazyAllowlistProxy:
        return super().__new__(cls)

    @staticmethod
    def _materialize() -> frozenset[str]:
        global _ALLOWLIST_CACHE, _PAIRS_CACHE
        if _ALLOWLIST_CACHE is None:
            _ALLOWLIST_CACHE, _PAIRS_CACHE = _derive_pool_history_tables()
        return _ALLOWLIST_CACHE

    def __contains__(self, value: object) -> bool:
        return value in _LazyAllowlistProxy._materialize()

    def __iter__(self):
        return iter(_LazyAllowlistProxy._materialize())

    def __len__(self) -> int:
        return len(_LazyAllowlistProxy._materialize())

    def __eq__(self, other: object) -> bool:
        return _LazyAllowlistProxy._materialize() == other

    def __ne__(self, other: object) -> bool:
        return _LazyAllowlistProxy._materialize() != other

    def __hash__(self) -> int:
        return hash(_LazyAllowlistProxy._materialize())

    def __repr__(self) -> str:
        return repr(_LazyAllowlistProxy._materialize())


class _LazyPairsProxy(_LazyFrozenset):
    __slots__ = ()

    def __new__(cls) -> _LazyPairsProxy:
        return super().__new__(cls)

    @staticmethod
    def _materialize() -> frozenset[tuple[str, str]]:
        global _ALLOWLIST_CACHE, _PAIRS_CACHE
        if _PAIRS_CACHE is None:
            _ALLOWLIST_CACHE, _PAIRS_CACHE = _derive_pool_history_tables()
        return _PAIRS_CACHE

    def __contains__(self, value: object) -> bool:
        return value in _LazyPairsProxy._materialize()

    def __iter__(self):
        return iter(_LazyPairsProxy._materialize())

    def __len__(self) -> int:
        return len(_LazyPairsProxy._materialize())

    def __eq__(self, other: object) -> bool:
        return _LazyPairsProxy._materialize() == other

    def __ne__(self, other: object) -> bool:
        return _LazyPairsProxy._materialize() != other

    def __hash__(self) -> int:
        return hash(_LazyPairsProxy._materialize())

    def __repr__(self) -> str:
        return repr(_LazyPairsProxy._materialize())


_ALLOWLIST_CACHE: frozenset[str] | None = None
_PAIRS_CACHE: frozenset[tuple[str, str]] | None = None

POOL_PROTOCOL_ALLOWLIST: frozenset[str] = _LazyAllowlistProxy()
SUPPORTED_POOL_PAIRS: frozenset[tuple[str, str]] = _LazyPairsProxy()


def is_supported_pool_pair(chain: str, protocol: str) -> bool:
    """Return True when the ``(chain, protocol)`` pair has data coverage.

    Used by the validator (POOL-3) to reject e.g. ``aerodrome@ethereum``
    and by the dispatcher (POOL-5) to compute eligible providers. Tests
    in ``test_pool_history_dispatcher.py`` (POOL-5) will lock this
    function's table contents.
    """
    return (chain, protocol) in SUPPORTED_POOL_PAIRS


# Short-circuit on the first failure, in this order:
#
#   resolution -> chain -> protocol -> (chain, protocol) pair
#   -> pool_address (empty / normalize) -> pool_address syntax
#   -> start_ts -> end_ts -> start_ts < end_ts -> future-time tolerance
#
# Tests lock the first failure's code and message, so this order is part
# of the validation contract.


def _validate_pool_history_request(
    request: gateway_pb2.PoolHistoryRequest,
    now_seconds: int,
) -> ValidationFailure | None:
    """Validate a ``PoolHistoryRequest``. Returns None on success.

    ``now_seconds`` is a parameter so tests can pin the future-time
    tolerance check without monkeypatching ``time.time()``. Production
    code passes ``int(time.time())``.
    """
    if request.resolution == gateway_pb2.Resolution.RESOLUTION_UNSPECIFIED:
        return invalid_argument("resolution is required (RESOLUTION_UNSPECIFIED rejected)")
    if request.resolution not in (
        gateway_pb2.Resolution.RESOLUTION_1H,
        gateway_pb2.Resolution.RESOLUTION_4H,
        gateway_pb2.Resolution.RESOLUTION_1D,
    ):
        return invalid_argument(f"unsupported resolution enum value: {request.resolution}")

    chain_raw = request.chain
    chain = chain_raw.strip().lower()
    if not chain:
        return invalid_argument("chain is required")

    protocol_raw = request.protocol
    protocol = protocol_raw.strip().lower()
    if not protocol:
        return invalid_argument("protocol is required")
    if protocol not in POOL_PROTOCOL_ALLOWLIST:
        return invalid_argument(f"unsupported protocol: {protocol_raw!r} (allowed: {sorted(POOL_PROTOCOL_ALLOWLIST)})")
    if not is_supported_pool_pair(chain, protocol):
        return invalid_argument(f"unsupported (chain, protocol) pair: ({chain!r}, {protocol!r})")

    address_raw = request.pool_address
    if not address_raw.strip():
        return invalid_argument("pool_address is required")
    pool_address = normalize_pool_address(address_raw, chain)
    if not validate_pool_address_syntax(pool_address, chain):
        return invalid_argument(f"invalid pool_address for chain {chain!r}: {pool_address!r}")

    if request.start_ts <= 0:
        return invalid_argument("start_ts must be > 0 (unix seconds)")
    if request.end_ts <= 0:
        return invalid_argument("end_ts must be > 0 (unix seconds)")
    if request.start_ts >= request.end_ts:
        return invalid_argument(f"start_ts must be < end_ts (got start_ts={request.start_ts}, end_ts={request.end_ts})")
    if request.end_ts > now_seconds + END_TS_FUTURE_TOLERANCE_SECONDS:
        return invalid_argument(
            f"end_ts is too far in the future (end_ts={request.end_ts}, "
            f"now={now_seconds}, tolerance={END_TS_FUTURE_TOLERANCE_SECONDS}s)"
        )

    return None


_PER_RPC_COUNTER_NAMES: tuple[str, ...] = (
    "requests_total",
    "cache_hits",
    "cache_misses",
    "provider_fallback",
    # The scalar counts all truncated responses; it must equal the sum of
    # the per-reason buckets.
    "truncated",
    "inflight_dedup_hits",
    "cache_evictions_by_entries",
    "cache_evictions_by_bytes",
    "cache_bytes_resident",
)

# truncation_reason counter is split by ``TruncationReason`` enum value.
# Initialized with the four enum names from gateway.proto so test code can
# rely on the keyset being stable (rather than emerging only after a truncation
# actually fires).
_TRUNCATION_REASON_NAMES: tuple[str, ...] = (
    "TRUNCATION_REASON_UNSPECIFIED",
    "CAP_EXCEEDED",
    "PROVIDER_PAGE_CAP",
    "PROVIDER_RETENTION",
)

_PER_PROVIDER_COUNTER_NAMES: tuple[str, ...] = (
    "requests",
    "errors",
    "bucket_throttle_waits_ms",
)

# The dispatcher's budget tracker is authoritative; health() only reads it.
_BUDGET_COUNTER_NAMES: tuple[str, ...] = (
    "the_graph_monthly_queries",
    "the_graph_monthly_budget_max",
)

# Unknown providers use conservative defaults that cannot finalize provisional
# data early or truncate a response to zero rows.
_FALLBACK_FINALITY_CUTOFF_SECONDS = 86400
_FALLBACK_PAGE_CAP_ROWS = 100000


def _zero_health_snapshot() -> dict[str, dict[str, int | dict[str, int]]]:
    """Return a fresh zero-valued ``health()`` payload with the locked schema.

    Public helper used by the skeleton AND the upcoming POOL-8 fill-in.
    Splitting it out makes the schema-lock testable as data, not as
    runtime side-effect.
    """
    return {
        "per_rpc": dict.fromkeys(_PER_RPC_COUNTER_NAMES, 0)
        | {"truncated_by_reason": dict.fromkeys(_TRUNCATION_REASON_NAMES, 0)}
        | {"errors_by_grpc_code": {}}
        | {"raw_cache_entries_by_provider": {}},
        "per_provider": {},  # populated by POOL-5 as providers land
        "budget": dict.fromkeys(_BUDGET_COUNTER_NAMES, 0),
    }


def _pool_history_response_size(msg: gateway_pb2.PoolHistoryResponse) -> int:
    """Byte-size estimator for the cache. Uses protobuf's
    ``ByteSize()`` which counts the wire-format size without
    allocating the serialized bytes (cheaper than
    ``len(SerializeToString())`` per put-call)."""
    return msg.ByteSize()


class PoolHistoryServiceServicer(gateway_pb2_grpc.PoolHistoryServiceServicer):
    """Pool history gRPC servicer.

    Lifecycle by ticket:

    * POOL-2 (VIB-4750) — skeleton + kill-switch + ``health()`` schema lock.
    * POOL-3 (VIB-4751) — validator + protocol allowlist + (chain, protocol)
      compatibility table.
    * **POOL-4 (VIB-4752)** — two-tier cache instances are now constructed
      on the servicer; ``health()`` reads live cache stats. The cache is
      not yet INVOKED from the handler (no providers populate it) — POOL-5
      wires the dispatcher to call ``get_or_fetch``.
    * POOL-5 (VIB-4753) — providers + dispatcher.
    * POOL-6 (VIB-4754) — truncation + finality re-promotion semantics.
    * POOL-8 (VIB-4756) — per-RPC + per-provider counter values.
    """

    def __init__(self, settings: GatewaySettings) -> None:
        self._settings = settings
        self._enabled = bool(settings.pool_history_enabled)
        # Cache-owned and service-owned counters remain separate until health()
        # merges them into its stable schema.
        self._metrics: dict[str, dict[str, int | dict[str, int]]] = _zero_health_snapshot()

        # Two-tier cache:
        #   public: 7-tuple key, no provider, ``get_or_fetch`` lives here.
        #   raw   : 8-tuple key, includes provider; per-provider partition
        #           tracking via ``extract_provider_from_raw_key``.
        max_entries = load_max_entries_from_settings(settings)
        max_bytes = load_max_bytes_from_settings(settings)
        # The public cache uses a wall-clock TTL so its TTL and
        # the finality cutoff share one timeline: a provisional entry's 60s TTL
        # and the "row aged past the cutoff" test advance together, which is what
        # makes finality re-promotion deterministic. The
        # ``repromoter`` flips a provisional entry to finalized in place once its
        # newest row ages past the serving provider's cutoff (see
        # ``_repromote_public_entry``). The raw cache does NOT re-promote.
        self._public_cache: HistoryCache[PoolHistoryPublicKey, gateway_pb2.PoolHistoryResponse] = HistoryCache(
            max_entries=max_entries,
            max_bytes=max_bytes,
            size_estimator=_pool_history_response_size,
            clock=time.time,
            repromoter=self._repromote_public_entry,
            name="pool_history_public",
        )
        self._raw_cache: HistoryCache[PoolHistoryRawKey, gateway_pb2.PoolHistoryResponse] = HistoryCache(
            max_entries=max_entries,
            max_bytes=max_bytes,
            size_estimator=_pool_history_response_size,
            partition_extractor=extract_provider_from_raw_key,
            name="pool_history_raw",
        )

        # Keep one dispatcher for the service lifetime so providers share their
        # HTTP session. Provider knobs use the reported source name; DefiLlama's
        # cutoff is longer because it can revise daily data after 24 hours.
        self._finality_cutoffs: dict[str, int] = {
            "the_graph": settings.pool_history_finality_cutoff_seconds_the_graph,
            "defillama": settings.pool_history_finality_cutoff_seconds_defillama,
            "coingecko_onchain": settings.pool_history_finality_cutoff_seconds_coingecko_onchain,
        }
        self._page_cap_rows: dict[str, int] = {
            "the_graph": settings.pool_history_page_cap_rows_the_graph,
            "defillama": settings.pool_history_page_cap_rows_defillama,
            "coingecko_onchain": settings.pool_history_page_cap_rows_coingecko_onchain,
        }

        self._dispatcher = PoolHistoryDispatcher(
            thegraph_api_key=settings.thegraph_api_key,
            thegraph_monthly_budget_max=settings.pool_history_thegraph_monthly_budget_max,
            is_supported_fn=is_supported_pool_pair,
            finality_cutoffs=self._finality_cutoffs,
            coingecko_api_key=settings.coingecko_api_key,
            counters=_DispatchCounters(
                on_provider_request=self._bump_provider_request,
                on_provider_error=self._bump_provider_error,
                on_provider_fallback=self._bump_provider_fallback,
                on_provider_throttle_wait=self._bump_provider_throttle_wait,
            ),
        )

        logger.debug(
            "Initialized PoolHistoryService (enabled=%s, max_entries=%d, max_bytes=%d, budget_max=%d)",
            self._enabled,
            max_entries,
            max_bytes,
            settings.pool_history_thegraph_monthly_budget_max,
        )

    async def close(self) -> None:
        """Release the dispatcher's HTTP / GraphQL sessions on gateway shutdown.

        Wired into ``GatewayServer.stop()``'s gateway-owned-servicer close loop
        (audit Important #3): the dispatcher lazily opens an aiohttp session +
        a GraphQL client on first use, which would otherwise leak at shutdown.
        Idempotent and safe even if no provider was ever invoked.
        """
        await self._dispatcher.close()

    def _provider_counters(self, provider: str) -> dict[str, int]:
        """Lazily create + return the per-provider counter dict.

        ``per_provider`` starts empty (POOL-2 schema lock) and grows the first
        time a provider is touched, with the locked per-provider keyset
        (requests / errors / bucket_throttle_waits_ms).
        """
        per_provider = self._metrics["per_provider"]
        bucket = per_provider.get(provider)
        if not isinstance(bucket, dict):
            bucket = dict.fromkeys(_PER_PROVIDER_COUNTER_NAMES, 0)
            per_provider[provider] = bucket
        return bucket

    def _bump_provider_request(self, provider: str) -> None:
        self._provider_counters(provider)["requests"] += 1

    def _bump_provider_error(self, provider: str) -> None:
        self._provider_counters(provider)["errors"] += 1

    def _bump_provider_fallback(self) -> None:
        per_rpc = self._metrics["per_rpc"]
        # ``provider_fallback`` is a scalar int counter (the per_rpc dict also
        # holds nested-dict counters, hence the union value type); cast for mypy.
        per_rpc["provider_fallback"] = cast(int, per_rpc.get("provider_fallback", 0)) + 1

    def _bump_provider_throttle_wait(self, provider: str, ms: int) -> None:
        """Accumulate per-provider ``bucket_throttle_waits_ms`` (POOL-8 / VIB-4756).

        Fires once per ``_ObservableTokenBucket`` refusal — both the
        ``_ProviderError``-raising primary path and the ``_NotAttempted``
        fallback path. ``ms`` is the THEORETICAL wait-to-next-token computed
        once at dispatcher construction (UAT card §D2.M2.b.3 formula
        ``round(period * 1000.0 / rate)`` — 1 req/s ⇒ 1000, 30/60s ⇒ 2000).
        Monotonic: a successful call NEVER resets the accumulator (the
        anti-reset test in D2.M2.b.3 pins this).
        """
        bucket = self._provider_counters(provider)
        bucket["bucket_throttle_waits_ms"] += ms

    def _bump_errors_by_grpc_code(self, code: grpc.StatusCode) -> None:
        """Record a non-OK gRPC return on ``per_rpc.errors_by_grpc_code`` (POOL-8).

        Keyed by the gRPC status code NAME (e.g. ``"UNAVAILABLE"``,
        ``"INVALID_ARGUMENT"``) — not the integer value — so the dict is
        ops-readable without an enum lookup. Kill-switch UNAVAILABLE and
        exhausted-providers UNAVAILABLE share the same key (UAT card
        §D2.M2.b.2 exact-equality assertion).
        """
        errors = cast(
            dict[str, int],
            self._metrics["per_rpc"]["errors_by_grpc_code"],
        )
        errors[code.name] = errors.get(code.name, 0) + 1

    def _bump_truncated(self, reason: gateway_pb2.TruncationReason.ValueType) -> None:
        """Bump ``truncated`` (scalar) AND ``truncated_by_reason[NAME]`` per response (POOL-8).

        Pre-condition: ``reason != TRUNCATION_REASON_UNSPECIFIED``. UNSPECIFIED is
        the non-event sentinel; bumping the dict on UNSPECIFIED would break the
        UAT card §D2.M2.b.1 full-dict equality. Caller MUST guard.

        ``TruncationReason.Name`` raises ``ValueError`` on an integer outside
        the locked enum (e.g. a future client introduces a new reason). The
        wrap below routes unknown values to an ``UNKNOWN_<int>`` bucket so the
        counter still moves forward — silently dropping the bump would mask a
        legitimate truncation event from ops (Gemini-flagged 2026-05-28).
        """
        per_rpc = self._metrics["per_rpc"]
        per_rpc["truncated"] = cast(int, per_rpc.get("truncated", 0)) + 1
        by_reason = cast(dict[str, int], per_rpc["truncated_by_reason"])
        reason_name = self._safe_truncation_name(reason)
        by_reason[reason_name] = by_reason.get(reason_name, 0) + 1

    def health(self) -> dict[str, dict[str, int | dict[str, int]]]:
        """Per-RPC + per-provider + budget counter snapshot.

        Schema is LOCKED in POOL-2; later tickets only update VALUES.
        Adding a new key requires bumping POOL-8 acceptance and the
        umbrella UAT card. Counter NAMES are listed in module-level
        constants above so they're trivially diff-able.

        Sources of truth merged into one snapshot:

        * Cache instances own ``cache_hits``, ``cache_misses``,
          ``cache_evictions_by_*``, ``cache_bytes_resident``,
          ``inflight_dedup_hits`` — read via ``stats()``.
        * ``raw_cache_entries_by_provider`` comes from the raw cache's
          ``entries_by_partition``.
        * ``_metrics`` owns the rest (``requests_total``, fallback,
          truncation_reason, errors_by_grpc_code, per_provider, budget).
        """
        public_stats = self._public_cache.stats()
        raw_stats = self._raw_cache.stats()
        # Cache metrics aggregate both tiers; raw currently has no in-flight
        # deduplication, but including it keeps the aggregation symmetric.
        cache_hits = public_stats["cache_hits"] + raw_stats["cache_hits"]
        cache_misses = public_stats["cache_misses"] + raw_stats["cache_misses"]
        evictions_by_entries = public_stats["cache_evictions_by_entries"] + raw_stats["cache_evictions_by_entries"]
        evictions_by_bytes = public_stats["cache_evictions_by_bytes"] + raw_stats["cache_evictions_by_bytes"]
        cache_bytes_resident = public_stats["bytes_resident"] + raw_stats["bytes_resident"]
        inflight_dedup_hits = public_stats["inflight_dedup_hits"] + raw_stats["inflight_dedup_hits"]

        # Copy nested metric dictionaries so callers cannot mutate live state.
        rpc_metrics = {k: (dict(v) if isinstance(v, dict) else v) for k, v in self._metrics["per_rpc"].items()}
        rpc_metrics["cache_hits"] = cache_hits
        rpc_metrics["cache_misses"] = cache_misses
        rpc_metrics["cache_evictions_by_entries"] = evictions_by_entries
        rpc_metrics["cache_evictions_by_bytes"] = evictions_by_bytes
        rpc_metrics["cache_bytes_resident"] = cache_bytes_resident
        rpc_metrics["inflight_dedup_hits"] = inflight_dedup_hits
        rpc_metrics["raw_cache_entries_by_provider"] = self._raw_cache.entries_by_partition

        # Preserve the all-zero fresh-service snapshot until the first TheGraph
        # query, then expose both the live count and configured maximum.
        budget = dict(self._metrics["budget"])
        dispatcher = getattr(self, "_dispatcher", None)
        if dispatcher is not None and dispatcher.the_graph_monthly_queries > 0:
            budget["the_graph_monthly_queries"] = dispatcher.the_graph_monthly_queries
            budget["the_graph_monthly_budget_max"] = dispatcher.the_graph_monthly_budget_max

        return {
            "per_rpc": rpc_metrics,
            "per_provider": {
                provider: dict(counters)  # type: ignore[arg-type]
                for provider, counters in self._metrics["per_provider"].items()
            },
            "budget": budget,
        }

    #: Bound untrusted structured-log input before validation emits the entry record.
    _MAX_LOGGED_POOL_ADDRESS = 80

    @staticmethod
    def _safe_truncation_name(reason: gateway_pb2.TruncationReason.ValueType) -> str:
        """``TruncationReason.Name`` with a graceful fallback on unknown ints.

        ``proto.EnumName(...)`` raises ``ValueError`` for an integer value the
        local proto descriptor doesn't know about. That can happen on
        proto-version skew between a newer gateway and an older client (or
        vice versa). Routing the unknown int to ``UNKNOWN_<int>`` keeps the
        log emission alive (Gemini-flagged 2026-05-28).
        """
        try:
            return gateway_pb2.TruncationReason.Name(reason)
        except ValueError:
            return f"UNKNOWN_{int(reason)}"

    @classmethod
    def _request_log_extra(
        cls,
        *,
        chain: str,
        protocol: str,
        resolution: int,
        pool_address: str,
        start_ts: int,
        end_ts: int,
    ) -> dict[str, object]:
        return {
            "chain": chain,
            "protocol": protocol,
            # Resolution is intentionally logged as the integer enum value.
            "resolution": resolution,
            "pool_address": pool_address[: cls._MAX_LOGGED_POOL_ADDRESS],
            "start_ts": start_ts,
            "end_ts": end_ts,
        }

    @classmethod
    def _exit_log_extra(
        cls,
        *,
        chain: str,
        protocol: str,
        resolution: int,
        pool_address: str,
        source: str,
        snapshots_count: int,
        truncation_reason: gateway_pb2.TruncationReason.ValueType,
        finality_band: str,
        latency_ms: int,
        grpc_code: grpc.StatusCode,
        error: str,
    ) -> dict[str, object]:
        return {
            "chain": chain,
            "protocol": protocol,
            "resolution": resolution,
            "pool_address": pool_address[: cls._MAX_LOGGED_POOL_ADDRESS],
            "source": source,
            "snapshots_count": snapshots_count,
            "truncation_reason": cls._safe_truncation_name(truncation_reason),
            "finality_band": finality_band,
            "latency_ms": latency_ms,
            "grpc_code": grpc_code.name,
            "error": error,
        }

    async def GetPoolHistory(
        self,
        request: gateway_pb2.PoolHistoryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PoolHistoryResponse:
        """Pool history handler.

        Decision order:
          1. Kill-switch off  -> UNAVAILABLE (registered-but-disabled).
          2. Validator fails  -> INVALID_ARGUMENT (POOL-3 / VIB-4751).
          3. Validator passes -> dispatch through providers (POOL-5 / VIB-4753).

        Order matters: the kill-switch precedes the validator so a
        disabled deployment returns a single uniform message regardless
        of input. Once enabled, the validator runs BEFORE any provider
        work (UAT card D3.F6 / D3.F8 fast-path).

        Telemetry (POOL-8 / VIB-4756): every entry emits one INFO log
        (`pool_history.request`). Every exit emits exactly one log —
        INFO `pool_history.response` on success, WARNING
        `pool_history.error` on any non-OK gRPC code — and bumps
        ``errors_by_grpc_code[<status-name>]`` for the non-OK case.
        Counter bumps for ``truncated`` / ``truncated_by_reason`` happen
        after the cache resolves (so a cache HIT carrying a truncated
        envelope counts the same as a fresh truncated fetch — the user
        perceives one truncation regardless of cache state).
        """
        started_monotonic = time.monotonic()
        # Normalize before validation so telemetry and cache keys use canonical
        # fields. The fallback keeps unexpected normalization errors on the
        # validator's clean INVALID_ARGUMENT path.
        chain = request.chain.strip().lower()
        protocol = request.protocol.strip().lower()
        try:
            pool_address = normalize_pool_address(request.pool_address, chain)
        except Exception:  # noqa: BLE001 — defensive: never crash the handler in pre-validation
            pool_address = request.pool_address
        start_ts = int(request.start_ts)
        end_ts = int(request.end_ts)
        resolution = int(request.resolution)

        logger.info(
            "pool_history.request",
            extra=self._request_log_extra(
                chain=chain,
                protocol=protocol,
                resolution=resolution,
                pool_address=pool_address,
                start_ts=start_ts,
                end_ts=end_ts,
            ),
        )

        def _emit_error_exit(code: grpc.StatusCode, message: str) -> None:
            self._bump_errors_by_grpc_code(code)
            logger.warning(
                "pool_history.error",
                extra=self._exit_log_extra(
                    chain=chain,
                    protocol=protocol,
                    resolution=resolution,
                    pool_address=pool_address,
                    source="",
                    snapshots_count=0,
                    truncation_reason=gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED,
                    finality_band="",
                    latency_ms=int((time.monotonic() - started_monotonic) * 1000),
                    grpc_code=code,
                    error=message,
                ),
            )

        if not self._enabled:
            details = (
                "PoolHistoryService not yet enabled - see VIB-4728. Set "
                "ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true after POOL-5 wires "
                "providers."
            )
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(details)
            _emit_error_exit(grpc.StatusCode.UNAVAILABLE, details)
            return gateway_pb2.PoolHistoryResponse(success=False, error=details)

        now_seconds = int(time.time())
        failure = _validate_pool_history_request(request, now_seconds=now_seconds)
        if failure is not None:
            code, message = failure
            context.set_code(code)
            context.set_details(message)
            _emit_error_exit(code, message)
            return gateway_pb2.PoolHistoryResponse(success=False, error=message)

        # Soft caps serve the oldest cap-sized half-open slice and return a
        # forward cursor; they truncate rather than reject the request.
        soft_cap_seconds = get_soft_cap_seconds(self._settings, resolution)
        clamped = (end_ts - start_ts) > soft_cap_seconds
        eff_end_ts = start_ts + soft_cap_seconds if clamped else end_ts
        resolution_seconds = resolution_to_seconds(resolution)

        # A fixed public-key band lets provisional entries re-promote in place.
        # Key the public envelope by the original window because exact-cap and
        # over-cap requests carry different truncation metadata. Raw payloads
        # use the effective window so identical upstream reads still deduplicate.
        public_key = make_public_key(
            chain=chain,
            pool_address=pool_address,
            protocol=protocol,
            start_ts=start_ts,
            end_ts=end_ts,
            resolution=resolution,
            finality_band=FINALITY_FINALIZED,
        )

        self._metrics["per_rpc"]["requests_total"] = cast(int, self._metrics["per_rpc"].get("requests_total", 0)) + 1

        async def _fetch() -> tuple[gateway_pb2.PoolHistoryResponse, CacheFinality]:
            # Keep successful raw payloads partitioned by provider and write
            # them before the public cache settles so fallback results coexist.
            async def _on_success(
                provider: str,
                snapshots: list[gateway_pb2.PoolSnapshot],
                band: CacheFinality,
            ) -> None:
                raw_key = make_raw_key(
                    chain=chain,
                    pool_address=pool_address,
                    protocol=protocol,
                    start_ts=start_ts,
                    end_ts=eff_end_ts,
                    resolution=resolution,
                    finality_band=band,
                    provider=provider,
                )
                # Raw cache stores the full provider payload; truncation belongs
                # only to the public envelope.
                raw_response = self._build_success_response(
                    provider=provider,
                    snapshots=snapshots,
                    reason=gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED,
                    next_start_ts=0,
                    finalized_only=band == FINALITY_FINALIZED,
                )
                self._raw_cache.put(raw_key, raw_response, band)

            outcome = await self._dispatcher.dispatch(
                chain=chain,
                pool_address=pool_address,
                protocol=protocol,
                start_ts=start_ts,
                end_ts=eff_end_ts,
                resolution=resolution,
                on_provider_success=_on_success,
            )
            if not outcome.success:
                # Raising prevents negative caching and clears the in-flight
                # slot, so later calls retry providers.
                raise _ProviderError(outcome.error)

            # The serving provider selects the row ceiling and finality cutoff.
            truncation = classify_truncation(
                snapshots=outcome.snapshots,
                eff_start_ts=start_ts,
                eff_end_ts=eff_end_ts,
                clamped=clamped,
                resolution_seconds=resolution_seconds,
                page_cap_rows=self._page_cap_rows.get(outcome.source, _FALLBACK_PAGE_CAP_ROWS),
            )
            # Recompute time after provider I/O so raw and public entries cannot
            # disagree when a row crosses the finality cutoff during the fetch.
            served_now_seconds = int(time.time())
            cutoff = self._finality_cutoffs.get(outcome.source, _FALLBACK_FINALITY_CUTOFF_SECONDS)
            newest_ts = int(truncation.kept[-1].timestamp) if truncation.kept else 0
            finalized_only = compute_finalized_only(
                newest_ts=newest_ts, now_seconds=served_now_seconds, cutoff_seconds=cutoff
            )
            band = FINALITY_FINALIZED if finalized_only else FINALITY_PROVISIONAL
            response = self._build_success_response(
                provider=outcome.source,
                snapshots=truncation.kept,
                reason=truncation.reason,
                next_start_ts=truncation.next_start_ts,
                finalized_only=finalized_only,
            )
            return (response, band)

        try:
            response = await self._public_cache.get_or_fetch(public_key, _fetch)
        except _ProviderError as exc:
            # Provider exhaustion is UNAVAILABLE with an explicit failure
            # envelope, never an apparently successful empty result.
            message = str(exc)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(message)
            _emit_error_exit(grpc.StatusCode.UNAVAILABLE, message)
            return gateway_pb2.PoolHistoryResponse(
                snapshots=[],
                truncation_reason=gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED,
                next_start_ts=0,
                source="",
                finalized_only=False,
                success=False,
                error=message,
            )

        # Count truncated responses after cache resolution so cache hits and
        # fresh fetches have identical per-response telemetry semantics.
        if response.truncation_reason != gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED:
            self._bump_truncated(response.truncation_reason)

        logger.info(
            "pool_history.response",
            extra=self._exit_log_extra(
                chain=chain,
                protocol=protocol,
                resolution=resolution,
                pool_address=pool_address,
                source=response.source,
                snapshots_count=len(response.snapshots),
                truncation_reason=response.truncation_reason,
                finality_band=(CacheFinality.FINALIZED if response.finalized_only else CacheFinality.PROVISIONAL).value,
                latency_ms=int((time.monotonic() - started_monotonic) * 1000),
                grpc_code=grpc.StatusCode.OK,
                error="",
            ),
        )
        return response

    def _build_success_response(
        self,
        *,
        provider: str,
        snapshots: list[gateway_pb2.PoolSnapshot],
        reason: gateway_pb2.TruncationReason.ValueType,
        next_start_ts: int,
        finalized_only: bool,
    ) -> gateway_pb2.PoolHistoryResponse:
        """Build a populated success envelope.

        POOL-6 (VIB-4754): the caller supplies the classified
        ``truncation_reason`` / ``next_start_ts`` (from ``classify_truncation``)
        and ``finalized_only`` (from ``compute_finalized_only``). The raw-cache
        write passes UNSPECIFIED / 0 (raw payload is never truncated — D6); the
        public response passes the real classification.
        """
        return gateway_pb2.PoolHistoryResponse(
            snapshots=snapshots,
            truncation_reason=reason,
            next_start_ts=next_start_ts,
            source=provider,
            finalized_only=finalized_only,
            success=True,
            error="",
        )

    def _repromote_public_entry(self, value: gateway_pb2.PoolHistoryResponse) -> CacheFinality | None:
        """Finality re-promotion hook for the public cache (POOL-6 / VIB-4754).

        Called by ``HistoryCache`` (under its lock) when a provisional entry's
        TTL has expired. If the cached response's newest row has aged past the
        serving provider's finality cutoff it is now finalized + durable: mutate
        ``finalized_only`` True in place and return ``FINALITY_FINALIZED`` so the
        cache flips the band + extends the TTL on the SAME key (D3.F9 — stable
        key, only the band/TTL change). Return None to leave it provisional (let
        it expire and re-fetch). Pure + fast — safe to run under the cache lock.
        """
        cutoff = self._finality_cutoffs.get(value.source, _FALLBACK_FINALITY_CUTOFF_SECONDS)
        newest_ts = max((int(s.timestamp) for s in value.snapshots), default=0)
        if compute_finalized_only(newest_ts=newest_ts, now_seconds=int(time.time()), cutoff_seconds=cutoff):
            value.finalized_only = True
            return FINALITY_FINALIZED
        return None
