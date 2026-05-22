"""PoolHistoryService skeleton + validator (VIB-4728 / POOL-2 + POOL-3).

Server-side handler for the gateway-backed PoolHistoryReader migration.
Two tickets ship in this file:

* **POOL-2 (VIB-4750)** — Servicer skeleton + kill-switch + ``health()``
  schema lock. The servicer is registered on the gRPC server from day 1
  so the hosted-auth interceptor and telemetry surface engage in the
  same boot path as every peer service.

* **POOL-3 (VIB-4751)** — Request validator + pool-specific protocol
  allowlist + ``(chain, protocol)`` compatibility table. The validator
  rejects malformed requests with ``INVALID_ARGUMENT`` BEFORE any
  provider is consulted (and is independent of the kill-switch).

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
from almanak.gateway.services._history_common import (
    END_TS_FUTURE_TOLERANCE_SECONDS,
    ValidationFailure,
    invalid_argument,
    normalize_pool_address,
    validate_pool_address_syntax,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Pool-specific allowlists (POOL-3 / VIB-4751)
# =============================================================================
# These tables live HERE (not in ``_history_common.py``) because they are
# pool-specific. ``RateHistoryService`` (VIB-4747) will have its own
# allowlist + (chain, protocol) table in ``rate_history_service.py``. The
# split keeps the shared module strictly chain-/protocol-agnostic.

#: Protocols VIB-4728 supports for pool history. Aerodrome lives on Base
#: only; Uniswap V3 lives on five chains (see ``SUPPORTED_POOL_PAIRS``).
#: Unknown protocols (typos, path-traversal injections like
#: ``"../etc/passwd"``) are rejected with ``INVALID_ARGUMENT``.
POOL_PROTOCOL_ALLOWLIST: frozenset[str] = frozenset({"uniswap_v3", "aerodrome"})

#: ``(chain, protocol)`` pairs that have a registered subgraph URL or
#: provider equivalent. The dispatcher in POOL-5 reads the SAME table to
#: decide ``eligible_providers`` — keeping one source-of-truth here means
#: a validator pass guarantees the dispatcher can route. Aerodrome lives
#: only on Base (per ``almanak/framework/data/pools/history.py:86-93``);
#: Uniswap V3 lives on Ethereum / Arbitrum / Base / Optimism / Polygon.
SUPPORTED_POOL_PAIRS: frozenset[tuple[str, str]] = frozenset(
    {
        ("ethereum", "uniswap_v3"),
        ("arbitrum", "uniswap_v3"),
        ("base", "uniswap_v3"),
        ("optimism", "uniswap_v3"),
        ("polygon", "uniswap_v3"),
        ("base", "aerodrome"),
    }
)


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


class PoolHistoryServiceServicer(gateway_pb2_grpc.PoolHistoryServiceServicer):
    """Pool history gRPC servicer.

    Skeleton in POOL-2; providers wired in POOL-5; caching in POOL-4;
    truncation / finality in POOL-6; telemetry in POOL-8. The skeleton
    only honours the kill-switch and exposes the locked ``health()`` shape.
    """

    def __init__(self, settings: GatewaySettings) -> None:
        self._settings = settings
        self._enabled = bool(settings.pool_history_enabled)
        # ``_metrics`` is the live counter store; POOL-8 will increment it.
        # Initialized to the zero-snapshot shape so health() can return a
        # stable schema from day 1.
        self._metrics: dict[str, dict[str, int | dict[str, int]]] = _zero_health_snapshot()
        logger.debug(
            "Initialized PoolHistoryService (enabled=%s); kill-switch via ALMANAK_GATEWAY_POOL_HISTORY_ENABLED",
            self._enabled,
        )

    # -- Health -----------------------------------------------------------

    def health(self) -> dict[str, dict[str, int | dict[str, int]]]:
        """Per-RPC + per-provider + budget counter snapshot.

        Schema is LOCKED in POOL-2; POOL-8 only updates VALUES. Adding a
        new key requires bumping POOL-8 acceptance and the umbrella UAT
        card. Counter NAMES are listed in module-level constants above
        so they're trivially diff-able.
        """
        # Defensive copy: callers should not be able to mutate the live
        # metrics store (the analytics service test harness has done this
        # historically and surfaced flakes). Generic shallow-deep copy via
        # dict comprehension — when POOL-8 adds new nested-dict counters
        # they're defensively copied automatically.
        rpc_metrics = {k: (dict(v) if isinstance(v, dict) else v) for k, v in self._metrics["per_rpc"].items()}
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
