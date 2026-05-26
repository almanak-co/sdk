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

import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.services._history_cache import (
    HistoryCache,
    PoolHistoryPublicKey,
    PoolHistoryRawKey,
    extract_provider_from_raw_key,
    load_max_bytes_from_settings,
    load_max_entries_from_settings,
)
from almanak.gateway.services._history_common import (
    END_TS_FUTURE_TOLERANCE_SECONDS,
    ValidationFailure,
    invalid_argument,
    normalize_pool_address,
    validate_pool_address_syntax,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Pool-specific allowlists (POOL-3 / VIB-4751) — registry-driven (VIB-4811).
# =============================================================================
# Phase 3 (VIB-4811) replaces the hardcoded ``POOL_PROTOCOL_ALLOWLIST`` +
# ``SUPPORTED_POOL_PAIRS`` tables with a derivation from
# ``GATEWAY_REGISTRY.capability_providers(GatewayPoolHistoryCapability)``.
# Each connector publishes its own supported chain set; the validator
# unions them at module-import time.
#
# Behaviour is byte-identical to the historical hardcoded sets: Uniswap
# V3 contributes ``{ethereum, arbitrum, base, optimism, polygon}`` and
# Aerodrome contributes ``{base}`` — exactly the previous six
# ``(chain, protocol)`` pairs. New protocols are added by registering a
# ``GatewayPoolHistoryCapability`` provider in
# ``almanak.connectors._gateway_registry`` (and NOT by editing this file).
#
# These tables live HERE (not in ``_history_common.py``) because they
# remain pool-specific. ``RateHistoryService`` (VIB-4747) will have its
# own allowlist / dispatch built from a sibling capability.


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
            # Normalize to lowercase — the validator (POOL-3) already
            # lowercases incoming request fields, and we must match
            # there. (Gemini code-review.)
            pairs.add((chain.lower(), protocol))
    return frozenset(allowlist), frozenset(pairs)


# ``frozenset`` is final and can't be subclassed cleanly, so the lazy
# proxy is a ``set`` subclass that materializes its contents on first
# access. Tests that compare with ``== frozenset(...)`` still match
# (set equality is contents-based) and ``in`` checks work normally.
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


# =============================================================================
# Request validator (POOL-3 / VIB-4751)
# =============================================================================
#
# Short-circuit on the first failure, in this order:
#
#   resolution -> chain -> protocol -> (chain, protocol) pair
#   -> pool_address (empty / normalize) -> pool_address syntax
#   -> start_ts -> end_ts -> start_ts < end_ts -> future-time tolerance
#
# This order matches VIB-4727's "first failure wins" pattern. Tests in
# ``test_history_validation.py`` lock both the codes AND the error
# messages so a regression that swaps two checks is caught.


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


# =============================================================================
# health() schema — counter NAMES locked here.
# =============================================================================
# Per-RPC observability surface (UAT card VIB-4728 D2.M4 / D2.M2 / D3.F7 /
# D3.F11). POOL-8 fills VALUES; the NAMES below are the stable
# observability contract.
_PER_RPC_COUNTER_NAMES: tuple[str, ...] = (
    "requests_total",
    "cache_hits",
    "cache_misses",
    "provider_fallback",
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

# Provider-bound counters. The provider set is OPEN — POOL-5 adds
# ``the_graph`` / ``defillama`` / ``geckoterminal`` when it lands. POOL-2
# initializes the keyset empty so health() returns a stable shape even
# pre-POOL-5.
_PER_PROVIDER_COUNTER_NAMES: tuple[str, ...] = (
    "requests",
    "errors",
    "bucket_throttle_waits_ms",
)

# Budget-tracker counters. Source-of-truth for `the_graph_monthly_queries`
# lives in whatever store the POOL-5 prerequisite spike chose (per
# PoolX.md §6.1); health() is the READ surface only.
_BUDGET_COUNTER_NAMES: tuple[str, ...] = (
    "the_graph_monthly_queries",
    "the_graph_monthly_budget_max",
)


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
        # ``_metrics`` is the live counter store for fields the cache
        # doesn't own (truncation_reason split, errors_by_grpc_code,
        # provider_fallback, budget). The cache owns hit/miss/eviction
        # counters; ``health()`` merges both sources into the locked
        # schema.
        self._metrics: dict[str, dict[str, int | dict[str, int]]] = _zero_health_snapshot()

        # Two-tier cache (POOL-4 / PoolX.md §D6):
        #   public: 7-tuple key, no provider, ``get_or_fetch`` lives here.
        #   raw   : 8-tuple key, includes provider; per-provider partition
        #           tracking via ``extract_provider_from_raw_key``.
        max_entries = load_max_entries_from_settings(settings)
        max_bytes = load_max_bytes_from_settings(settings)
        self._public_cache: HistoryCache[PoolHistoryPublicKey, gateway_pb2.PoolHistoryResponse] = HistoryCache(
            max_entries=max_entries,
            max_bytes=max_bytes,
            size_estimator=_pool_history_response_size,
            name="pool_history_public",
        )
        self._raw_cache: HistoryCache[PoolHistoryRawKey, gateway_pb2.PoolHistoryResponse] = HistoryCache(
            max_entries=max_entries,
            max_bytes=max_bytes,
            size_estimator=_pool_history_response_size,
            partition_extractor=extract_provider_from_raw_key,
            name="pool_history_raw",
        )

        logger.debug(
            "Initialized PoolHistoryService (enabled=%s, max_entries=%d, max_bytes=%d)",
            self._enabled,
            max_entries,
            max_bytes,
        )

    # -- Health -----------------------------------------------------------

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
        # Cache hit/miss counters aggregate across BOTH tiers (a public
        # hit avoids an upstream call regardless of which tier served it).
        cache_hits = public_stats["cache_hits"] + raw_stats["cache_hits"]
        cache_misses = public_stats["cache_misses"] + raw_stats["cache_misses"]
        evictions_by_entries = public_stats["cache_evictions_by_entries"] + raw_stats["cache_evictions_by_entries"]
        evictions_by_bytes = public_stats["cache_evictions_by_bytes"] + raw_stats["cache_evictions_by_bytes"]
        # ``cache_bytes_resident`` is also the sum (both tiers consume
        # gateway memory).
        cache_bytes_resident = public_stats["bytes_resident"] + raw_stats["bytes_resident"]
        # ``inflight_dedup_hits`` only lives on the public cache (raw cache
        # has no ``get_or_fetch`` surface); summing for forward-compat.
        inflight_dedup_hits = public_stats["inflight_dedup_hits"] + raw_stats["inflight_dedup_hits"]

        # Defensive copy: callers should not be able to mutate the live
        # metrics store (the analytics service test harness has done this
        # historically and surfaced flakes). Generic shallow-deep copy via
        # dict comprehension — when POOL-8 adds new nested-dict counters
        # they're defensively copied automatically.
        rpc_metrics = {k: (dict(v) if isinstance(v, dict) else v) for k, v in self._metrics["per_rpc"].items()}
        # Overlay live cache values (these supersede the zero-initialised
        # per_rpc placeholders from ``_zero_health_snapshot``).
        rpc_metrics["cache_hits"] = cache_hits
        rpc_metrics["cache_misses"] = cache_misses
        rpc_metrics["cache_evictions_by_entries"] = evictions_by_entries
        rpc_metrics["cache_evictions_by_bytes"] = evictions_by_bytes
        rpc_metrics["cache_bytes_resident"] = cache_bytes_resident
        rpc_metrics["inflight_dedup_hits"] = inflight_dedup_hits
        # The raw-cache partition counts are live; POOL-5's dispatcher
        # writes to the raw cache and updates this map on each put.
        rpc_metrics["raw_cache_entries_by_provider"] = self._raw_cache.entries_by_partition

        return {
            "per_rpc": rpc_metrics,
            "per_provider": {
                provider: dict(counters)  # type: ignore[arg-type]
                for provider, counters in self._metrics["per_provider"].items()
            },
            "budget": dict(self._metrics["budget"]),
        }

    # -- gRPC entry point -------------------------------------------------

    async def GetPoolHistory(
        self,
        request: gateway_pb2.PoolHistoryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PoolHistoryResponse:
        """Pool history handler.

        Decision order:
          1. Kill-switch off  -> UNAVAILABLE (registered-but-disabled).
          2. Validator fails  -> INVALID_ARGUMENT (POOL-3 / VIB-4751).
          3. Validator passes -> UNIMPLEMENTED (POOL-5 wires providers).

        Order matters: the kill-switch precedes the validator so a
        disabled deployment returns a single uniform message regardless
        of input. Once enabled, the validator runs BEFORE any provider
        work (UAT card D3.F6 / D3.F8 fast-path).
        """
        if not self._enabled:
            details = (
                "PoolHistoryService not yet enabled - see VIB-4728. Set "
                "ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true after POOL-5 wires "
                "providers."
            )
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(details)
            return gateway_pb2.PoolHistoryResponse(success=False, error=details)

        failure = _validate_pool_history_request(request, now_seconds=int(time.time()))
        if failure is not None:
            code, message = failure
            context.set_code(code)
            context.set_details(message)
            return gateway_pb2.PoolHistoryResponse(success=False, error=message)

        # Validator passed; providers not yet wired: POOL-3 -> POOL-5 window.
        details = (
            "GetPoolHistory not yet implemented - POOL-5 (VIB-4753) lands "
            "providers. Kill-switch is enabled; check deployment if you "
            "expected providers to be wired."
        )
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details(details)
        return gateway_pb2.PoolHistoryResponse(success=False, error=details)
