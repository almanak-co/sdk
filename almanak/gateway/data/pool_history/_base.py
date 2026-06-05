"""Shared primitives for the pool-history providers + dispatcher (POOL-5).

Leaf module (no intra-package imports) so the three provider modules and the
dispatcher can all depend on it without an import cycle. Holds:

* The 3-state provider taxonomy sentinels (``_NotAttempted`` / ``_NOT_ATTEMPTED``)
  and the provider error (``_ProviderError``). **Parallel copies** of the
  ``pool_analytics_service`` types — the two services are intentionally
  decoupled (UAT card decision #3); do NOT import across them.
* The thread-safe ``_TokenBucket`` (parallel copy of the analytics bucket).
* ``_safe_decimal_str`` — Empty != Zero decimal coercion (NOT ``_safe_decimal``
  which collapses to ``Decimal("0")`` — decision #9 must-fix port).
* Chain-name maps + the ``is_solana_family`` helper for DefiLlama +
  CoinGecko Onchain, re-exported from the shared ``_history_common`` home so the
  analytics service and the pool-history providers agree on chain spelling.
* The provider interface result type alias + the ``PoolHistoryProvider``
  Protocol.

No HTTP egress happens here — this is pure data + sentinels.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, runtime_checkable

from almanak.gateway.data._history_common import (
    _CHAIN_TO_GT_NETWORK,
    _CHAIN_TO_LLAMA_DISPLAY,
    is_solana_family,
)
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


# Chain-name maps + Solana-family helper are re-exported from the shared
# ``almanak/gateway/data/_history_common`` home so the analytics service and
# the pool-history providers agree on chain spelling without duplicating the
# literals (coupling-ratchet canonical home — blueprint 22).


# =============================================================================
# 3-state provider taxonomy
# =============================================================================


class _NotAttempted:
    """Sentinel: a provider was skipped LOCALLY (not reached over the wire).

    Returned for: rate-limit bucket empty, monthly-budget breaker tripped,
    ineligible resolution (DefiLlama at sub-daily), or no subgraph URL
    registered for the (protocol, chain). Distinct from ``None`` (reached
    the upstream, queried, genuinely not found) and from a raised
    ``_ProviderError`` (transport / 429 / parse / GraphQL error). The
    dispatcher does NOT bump ``errors`` for a ``_NotAttempted`` (UAT card
    decision: a local skip is not an upstream failure).
    """


#: Module singleton — providers return THIS instance, callers narrow with
#: ``isinstance(result, _NotAttempted)``.
_NOT_ATTEMPTED = _NotAttempted()


class _ProviderError(Exception):
    """Raised inside a provider on a transport / rate-limit / parse / GraphQL failure.

    The dispatcher catches this, bumps the per-provider ``errors`` counter +
    ``provider_fallback``, appends the message to the error string, and falls
    through to the next eligible provider. Parallel copy of the
    ``pool_analytics_service`` type (decision #3).
    """


# =============================================================================
# Token bucket (parallel copy of pool_analytics_service._TokenBucket)
# =============================================================================


class _TokenBucket:
    """Thread-safe token bucket. Empty bucket -> ``acquire()`` returns False.

    The dispatcher converts an empty primary (TheGraph) bucket into a
    ``_ProviderError`` (decision #6: a throttled primary must fall through
    and be observable); for the fallback providers an empty bucket is a
    ``_NotAttempted`` local skip.
    """

    # ``__slots__`` here is load-bearing: the ``_ObservableTokenBucket``
    # subclass below ALSO declares ``__slots__`` to keep its footprint
    # identical to the parent. That subclass-only contract only holds if
    # the parent has slots too — otherwise the child instances inherit a
    # ``__dict__`` from the parent AND carry the new slot, growing the
    # footprint instead of shrinking it (pr-auditor 2026-05-28).
    __slots__ = ("_rate", "_period", "_tokens", "_last_refill", "_lock")

    def __init__(self, rate: int, period: float = 1.0) -> None:
        self._rate = rate
        self._period = period
        self._tokens = float(rate)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(float(self._rate), self._tokens + elapsed * (self._rate / self._period))
            self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


class _ObservableTokenBucket(_TokenBucket):
    """``_TokenBucket`` variant that emits a callback on every refusal (POOL-8 / VIB-4756).

    The dispatcher wraps each provider's bucket in this subclass so the
    ``bucket_throttle_waits_ms`` per-provider counter can accumulate the
    THEORETICAL ms-until-next-token on each bucket-empty event — independent
    of whether the empty path raises ``_ProviderError`` (TheGraph primary)
    or returns ``_NotAttempted`` (DefiLlama / CoinGecko Onchain fallbacks). The
    callback runs UNDER the bucket lock so the refusal observation can't be
    reordered against a concurrent successful acquire.

    The formula contract from UAT card §D2.M2.b.3 is: theoretical ms =
    ``round(period * 1000.0 / rate)``. The dispatcher pre-computes this
    integer at construction and passes it via the bound callback — so a
    1 req/s bucket bumps the counter by exactly ``1000``, a 2 req/s bucket
    by ``500``, a 30/60s bucket by ``2000``. No wall-clock measurement;
    no proportional-to-deficit scaling.
    """

    __slots__ = ("_on_refusal",)

    def __init__(
        self,
        rate: int,
        period: float = 1.0,
        *,
        on_refusal: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(rate=rate, period=period)
        # Stored on the instance (not the class) so an InstanceA refusal
        # callback can't leak into InstanceB. ``__slots__`` keeps the
        # observable variant's footprint identical to the parent's.
        self._on_refusal = on_refusal

    def acquire(self) -> bool:
        # Re-run the parent's acquire (which holds the lock) so the
        # refusal callback fires INSIDE the same critical section that
        # produced the False result. A second thread cannot observe the
        # bucket state between the False return and the counter bump.
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(float(self._rate), self._tokens + elapsed * (self._rate / self._period))
            self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            if self._on_refusal is not None:
                # Callback runs UNDER the bucket's lock. The downstream counter
                # mutation in ``PoolHistoryServiceServicer._bump_provider_throttle_wait``
                # is a single ``dict.__setitem__`` on ``self._metrics``, which the
                # servicer accesses lock-free because every caller reaches this
                # path through ``async def GetPoolHistory`` — i.e. the asyncio
                # event-loop thread is the single mutator. There is NO separate
                # servicer-side lock; the safety property is "asyncio single
                # mutator," not "two locks acquired in a specific order." If a
                # sync gRPC method ever lands on this service, the counter
                # mutation will need its own lock and the comment below must
                # be revisited (pr-auditor 2026-05-28).
                #
                # Defensive ``try/except`` so observability NEVER changes
                # business semantics — if the counter hook raises (rare, but
                # possible under e.g. memory pressure), we still return False
                # so the caller sees a normal bucket miss and routes to its
                # fallback. A propagating exception here would surface as an
                # unexpected provider failure instead of a routine refusal.
                # CodeRabbit-flagged 2026-05-28; broad-except is intentional.
                try:
                    self._on_refusal()
                except Exception:  # noqa: BLE001 - observability must not change dispatch
                    logger.exception("ObservableTokenBucket refusal callback raised; swallowing")
            return False


# =============================================================================
# Monthly-budget breaker for The Graph
# =============================================================================


class _MonthlyBudgetTracker:
    """In-memory monotonic monthly-query counter for The Graph (decision #7).

    The Graph bills per query against a monthly plan quota. This breaker
    increments ``queries`` on each TheGraph query attempt; once
    ``queries >= budget_max`` the TheGraph provider returns ``_NotAttempted``
    and the fallback chain proceeds (D3.F11 trip). The counter resets when
    the calendar month rolls over.

    Gateway-volatile by design: the count lives in process memory, NOT a DB
    (the deployed Postgres schema is owned by ``metrics-database``; the spike
    chose in-memory monotonic with month reset). A gateway restart resets the
    count — acceptable because each gateway serves one strategy (1:1 topology)
    and the bound is a coarse cost guard, not an exact ledger.

    Thread-safe: a ``threading.Lock`` guards the counter so a worker-thread
    caller can't race the asyncio path.
    """

    def __init__(self, *, budget_max: int, clock: Any = None) -> None:
        self._budget_max = int(budget_max)
        self._clock = clock or time.time
        self._lock = threading.Lock()
        self._queries = 0
        self._month_key = self._current_month_key()

    def _current_month_key(self) -> tuple[int, int]:
        # Gmtime so the month boundary is deterministic (UTC), independent of
        # the gateway host's local timezone.
        t = time.gmtime(self._clock())
        return (t.tm_year, t.tm_mon)

    def _roll_if_new_month_locked(self) -> None:
        current = self._current_month_key()
        if current != self._month_key:
            self._month_key = current
            self._queries = 0

    @property
    def budget_max(self) -> int:
        return self._budget_max

    @property
    def queries(self) -> int:
        with self._lock:
            self._roll_if_new_month_locked()
            return self._queries

    def is_tripped(self) -> bool:
        """True when the monthly query count has reached the configured max."""
        with self._lock:
            self._roll_if_new_month_locked()
            return self._queries >= self._budget_max

    def record_query(self) -> None:
        """Increment the monthly counter (called once per TheGraph query attempt)."""
        with self._lock:
            self._roll_if_new_month_locked()
            self._queries += 1


# =============================================================================
# Decimal helper — Empty != Zero (decision #9 must-fix: NOT _safe_decimal)
# =============================================================================


def _safe_decimal_str(value: Any) -> str:
    """Convert ``value`` to a decimal-as-string, or ``""`` when unmeasured.

    ``None`` -> ``""`` (unmeasured, per AGENTS.md "Empty != Zero").
    Numeric ``0`` / ``"0"`` -> ``"0"`` (measured zero, preserved).
    Anything that doesn't parse cleanly -> ``""`` plus a debug log; NEVER
    silently substitutes zero.

    This is the corrected port: the framework reader's ``_safe_decimal``
    collapses unmeasured / unparseable values to ``Decimal("0")``, which
    violates Empty != Zero and would mark unmeasured fee data as a measured
    zero. Do NOT use that one.
    """
    if value is None:
        return ""
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        logger.debug("pool_history: dropped unparseable decimal %r", value)
        return ""


# =============================================================================
# Provider interface
# =============================================================================

#: A provider ``fetch`` returns one of:
#:   * non-empty ``list[PoolSnapshot]``  -> success
#:   * ``None``                          -> reached upstream, genuinely not found
#:   * ``_NotAttempted``                 -> local skip (rate / ineligible / no URL)
#: …or RAISES ``_ProviderError`` for transport / parse failures. A provider
#: MUST NEVER return ``[]`` as success (empty upstream -> ``None``).
ProviderResult = list[gateway_pb2.PoolSnapshot] | _NotAttempted | None


@runtime_checkable
class PoolHistoryProvider(Protocol):
    """Structural interface every pool-history provider implements."""

    #: Stable provider id used for ``source`` + raw-cache partition +
    #: per-provider health counters. One of: ``the_graph`` / ``defillama`` /
    #: ``geckoterminal``.
    name: str

    async def fetch(
        self,
        *,
        chain: str,
        pool_address: str,
        protocol: str,
        start_ts: int,
        end_ts: int,
        resolution: int,
    ) -> ProviderResult: ...


def build_unmeasured_fields(
    *,
    tvl: str,
    volume_24h: str,
    fee_revenue_24h: str,
    token0_reserve: str,
    token1_reserve: str,
) -> list[str]:
    """Return the names of the snapshot fields that are ``""`` (unmeasured).

    Per-row Empty != Zero metadata (inherited audit row #11 / UAT card
    D1.S1): a field that came back ``""`` from ``_safe_decimal_str`` is
    unmeasured and its name is repeated in ``unmeasured_fields``; the
    framework boundary maps each name to a Python ``None`` field.
    """
    candidates = {
        "tvl": tvl,
        "volume_24h": volume_24h,
        "fee_revenue_24h": fee_revenue_24h,
        "token0_reserve": token0_reserve,
        "token1_reserve": token1_reserve,
    }
    return [name for name, value in candidates.items() if value == ""]


__all__ = [
    "_CHAIN_TO_GT_NETWORK",
    "_CHAIN_TO_LLAMA_DISPLAY",
    "is_solana_family",
    "_MonthlyBudgetTracker",
    "_NOT_ATTEMPTED",
    "_NotAttempted",
    "_ObservableTokenBucket",
    "_ProviderError",
    "_TokenBucket",
    "_safe_decimal_str",
    "ProviderResult",
    "PoolHistoryProvider",
    "build_unmeasured_fields",
]
