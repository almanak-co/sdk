"""PoolHistoryService skeleton (VIB-4728 / POOL-2 / VIB-4750).

Server-side handler placeholder for the gateway-backed PoolHistoryReader
migration. This ticket lands the registered-but-default-disabled skeleton:

* The servicer is constructed and registered on the gRPC server from day 1
  so the hosted-auth interceptor, telemetry surface, and proto wire are
  exercised in the same boot path as every peer service.

* The ``GetPoolHistory`` handler does NOT yet talk to providers. Behaviour:

    - ``ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=false`` (default): every call
      returns ``UNAVAILABLE`` with a clear message pointing at the umbrella
      epic (``VIB-4728``). Avoids the registered-but-half-built footgun
      Codex flagged in the POOL-X Round-1 critique.
    - ``ALMANAK_GATEWAY_POOL_HISTORY_ENABLED=true`` AND providers not yet
      wired (POOL-2 → POOL-5 window): returns ``UNIMPLEMENTED`` per the
      gRPC contract. POOL-5 (VIB-4753) lands the actual provider dispatch
      and replaces this branch.

* ``health()`` exposes a counter-NAMES schema locked here. POOL-8 (VIB-4756)
  populates VALUES; new metric keys MUST go through that ticket so the
  ``health()`` shape is a stable observability contract. The keyset
  mirrors the umbrella UAT card (``docs/internal/uat-cards/VIB-4728.md``)
  D2.M2 / D3.F11 / D2.M4 telemetry assertions.

No HTTP / gRPC / GraphQL egress occurs in this module — that all lands in
POOL-5. Mid-flight termination is safe (no writes; mirrors VIB-4727
PoolAnalyticsService scope rationale).
"""

from __future__ import annotations

import logging

import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc

logger = logging.getLogger(__name__)


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
        request: gateway_pb2.PoolHistoryRequest,  # noqa: ARG002 - skeleton
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PoolHistoryResponse:
        """POOL-2 skeleton handler.

        - Kill-switch off  -> UNAVAILABLE (registered-but-disabled)
        - Kill-switch on   -> UNIMPLEMENTED (providers not yet wired)

        POOL-5 (VIB-4753) replaces the UNIMPLEMENTED branch with the real
        validation / dispatcher / provider pipeline.
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

        # Enabled but providers not yet wired: POOL-2 -> POOL-5 window.
        details = (
            "GetPoolHistory not yet implemented - POOL-5 (VIB-4753) lands "
            "providers. Kill-switch is enabled; check deployment if you "
            "expected providers to be wired."
        )
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details(details)
        return gateway_pb2.PoolHistoryResponse(success=False, error=details)
