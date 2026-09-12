"""Failure-kind classifier (VIB-3803).

Classifies an exception into a stable :class:`FailureKind` so the circuit
breaker can apply different tolerance thresholds for **data-class** vs
**action-class** failures.

Why this matters
----------------
The 29 Apr 2026 production incident tripped a strategy's circuit breaker
after 3 consecutive transient ``coingecko_onchain`` OHLCV failures. The breaker
treated all consecutive failures alike — so 3 transient *data* outages
looked identical to 3 *execution-revert* failures. Risk reduction needs
fresh data; killing the strategy at iteration 3 of a transient data outage
when it has open positions is exactly the wrong response.

Classification source-of-truth
------------------------------
Prefer the typed exception contract from VIB-3800
(:class:`almanak.framework.data.interfaces.DataSourceRateLimited` /
:class:`DataSourceTimeout` / :class:`DataSourceUnavailable`) — gateway
services map upstream failures to these types via
``set_error_from_upstream``, so a failure that came from gateway egress
arrives at the runner already typed.

Falls back to walking the ``__cause__`` chain so a generic ``Exception``
wrapping a typed cause is still classified correctly. Returns
:attr:`FailureKind.UNKNOWN` for anything we can't confidently classify —
the breaker treats UNKNOWN as action-class (low tolerance) by default.
"""

from __future__ import annotations

from enum import Enum
from typing import Any


class FailureKind(Enum):
    """Stable classification of an iteration-level failure.

    Used by the circuit breaker to decide whether to apply the standard
    fail-fast threshold or the elevated data-class threshold (VIB-3803).
    """

    DATA_UNAVAILABLE = "data_unavailable"
    DATA_RATE_LIMITED = "data_rate_limited"
    DATA_TIMEOUT = "data_timeout"
    EXECUTION_REVERTED = "execution_reverted"
    STATE_CORRUPT = "state_corrupt"
    # VIB-5746: a pre-execution SAFETY-GUARD refusal — the strategy proposed an
    # action (typically a swap) that a compile-time guard refused BEFORE any
    # transaction was built or sent (e.g. price impact above the configured max,
    # or the on-chain quoter returned no amount so liquidity could not be
    # verified). ZERO transactions reach the chain and the on-chain position is
    # untouched: the guard did exactly its job. This is a safety SUCCESS, not an
    # execution failure, so it must NOT count toward the circuit breaker's
    # consecutive-failure trip thresholds (a refusal streak backs off the loop
    # cadence instead — see the runner's refusal-backoff). Distinct from
    # ``UNKNOWN``/action-class precisely so a correct refusal can never trip an
    # emergency stop the way three real reverts would.
    GUARD_REFUSED = "guard_refused"
    # An accepted broadcast whose outcome is UNRESOLVED. Submission
    # crossed the boundary and returned transaction identity, but no complete
    # receipt set ever became observable, so the durable barrier refuses replay.
    # The outcome is unknown, not failed: a null receipt is not proof of
    # nonexecution and the transaction may still mine. Charging it to the
    # action-class counter emergency-stops a deployment whose funds are intact
    # and whose only correct next move is to keep observing — the breaker cannot
    # protect against a transaction that is already out of our hands. Distinct
    # from ``GUARD_REFUSED``: a refusal sent zero transactions, a hold sent one.
    RECONCILIATION_HOLD = "reconciliation_hold"
    UNKNOWN = "unknown"

    @property
    def is_data_class(self) -> bool:
        """True iff this failure kind is a data-fetch failure.

        Data-class failures get an elevated consecutive-failure threshold
        when the strategy has open exposure, because risk-reduction action
        requires fresh data — fail-fast on transient data outages crash-loops
        a strategy that's holding correct positions.
        """
        return self in _DATA_KINDS

    @property
    def is_guard_refusal(self) -> bool:
        """True iff this is a pre-execution safety-guard refusal (VIB-5746).

        A guard refusal is neither data-class nor action-class for trip
        purposes: no transaction was sent, so it is not an execution failure,
        and it is not a data outage either. The circuit breaker treats it as
        NEUTRAL to its trip counters and the runner backs the loop cadence off
        instead of hot-looping the refused action.
        """
        return self is FailureKind.GUARD_REFUSED

    @property
    def is_execution_hold(self) -> bool:
        """True iff execution is blocked awaiting operator reconciliation.

        The replay barrier, not the breaker, is what keeps the money safe here;
        this classification never touches that barrier.
        """
        return self is FailureKind.RECONCILIATION_HOLD


_DATA_KINDS = frozenset(
    {
        FailureKind.DATA_UNAVAILABLE,
        FailureKind.DATA_RATE_LIMITED,
        FailureKind.DATA_TIMEOUT,
    }
)


def classify_failure(exc: Any) -> FailureKind:
    """Classify an exception into a :class:`FailureKind`.

    Decision order:

    1. Direct typed signal (VIB-3800 ``DataSource*`` exceptions).
    2. Typed gRPC trailer — unwrap via ``data_source_error_from_grpc`` so a
       raw ``grpc.RpcError`` returned from the gateway is still classified.
    3. ``__cause__`` walk — handles the common pattern of a generic exception
       wrapping a typed cause via ``raise X from typed_exc``.
    4. :attr:`FailureKind.UNKNOWN` (caller policy applies — breaker treats
       this as action-class).

    The function is defensive against ``None`` and self-referential causes;
    it never raises and it terminates on cycles.
    """
    if exc is None:
        return FailureKind.UNKNOWN

    # Late imports keep this module importable from anywhere in the runner
    # without forcing the data-layer dependency tree onto modules that
    # don't need it.
    from almanak.framework.data.interfaces import (
        AllDataSourcesFailed,
        DataSourceRateLimited,
        DataSourceTimeout,
        DataSourceUnavailable,
        data_source_error_from_grpc,
    )

    if isinstance(exc, DataSourceRateLimited):
        return FailureKind.DATA_RATE_LIMITED
    if isinstance(exc, DataSourceTimeout):
        return FailureKind.DATA_TIMEOUT
    if isinstance(exc, DataSourceUnavailable | AllDataSourcesFailed):
        return FailureKind.DATA_UNAVAILABLE

    # VIB-5153 / ALM-2814: a transient ``MarketSnapshotError`` (the typed error
    # raised by the strategy-facing snapshot, e.g. ``ILExposureUnavailableError``
    # on a protocol whose IL exposure isn't yet available) is a data-class
    # failure — same family as the ``DataSource*`` exceptions above, just
    # surfaced through the snapshot rather than a raw provider. Without this it
    # falls through to ``UNKNOWN`` (action-class) and a single unavailable-data
    # cycle counts toward the fast-fail breaker, killing the deployment. Only
    # transient/recoverable warnings qualify; hard ``error``/``critical`` or
    # non-retryable snapshot errors (misconfiguration, e.g.
    # ``ChainNotConfiguredError``) keep the conservative action-class default.
    snapshot_kind = _classify_market_snapshot_error(exc)
    if snapshot_kind != FailureKind.UNKNOWN:
        return snapshot_kind

    # Try to unwrap a typed gRPC trailer. ``data_source_error_from_grpc``
    # tolerates non-gRPC inputs (returns None silently).
    try:
        typed = data_source_error_from_grpc(exc)
    except Exception:
        typed = None
    if typed is not None and typed is not exc:
        return classify_failure(typed)

    # Walk __cause__, capped to avoid a malformed chain spinning forever.
    # Each cause is classified via the FULL pipeline (typed → gRPC trailer
    # unwrap → direct check) — not just _classify_direct — so that a
    # nested ``raise X from grpc.RpcError(typed)`` is still classified
    # correctly. We dispatch to the per-step internals to avoid infinite
    # recursion (a cause's cause is walked here, not via classify_failure).
    seen: set[int] = {id(exc)}
    cause = getattr(exc, "__cause__", None)
    depth = 0
    while cause is not None and id(cause) not in seen and depth < 8:
        seen.add(id(cause))
        kind = _classify_direct(cause)
        if kind != FailureKind.UNKNOWN:
            return kind
        # Try the typed-gRPC-trailer unwrap on the cause too — this is the
        # common production shape where ``raise RuntimeError("…") from
        # rpc_error`` hides a typed `RetryInfo` payload one level down.
        try:
            cause_typed = data_source_error_from_grpc(cause)
        except Exception:
            cause_typed = None
        if cause_typed is not None and id(cause_typed) not in seen:
            kind = _classify_direct(cause_typed)
            if kind != FailureKind.UNKNOWN:
                return kind
        cause = getattr(cause, "__cause__", None)
        depth += 1

    return FailureKind.UNKNOWN


def _classify_direct(exc: Any) -> FailureKind:
    """Classify without recursion or cause-walking — used inside the loop."""
    from almanak.framework.data.interfaces import (
        AllDataSourcesFailed,
        DataSourceRateLimited,
        DataSourceTimeout,
        DataSourceUnavailable,
    )

    if isinstance(exc, DataSourceRateLimited):
        return FailureKind.DATA_RATE_LIMITED
    if isinstance(exc, DataSourceTimeout):
        return FailureKind.DATA_TIMEOUT
    if isinstance(exc, DataSourceUnavailable | AllDataSourcesFailed):
        return FailureKind.DATA_UNAVAILABLE
    return _classify_market_snapshot_error(exc)


# Snapshot-error severities that we are willing to treat as a tolerant
# data-class failure. ``error``/``critical`` snapshot errors keep the
# conservative action-class (UNKNOWN) default — they may signal something worse
# than a transient outage — matching the narrow VIB-5153 / ALM-2814 contract.
_TRANSIENT_SNAPSHOT_SEVERITIES = frozenset({"info", "warning"})


def _classify_market_snapshot_error(exc: Any) -> FailureKind:
    """Classify a :class:`MarketSnapshotError` into a data-class kind.

    A snapshot error is data-class only when it is **retryable** and its
    ``severity`` is ``info``/``warning`` (the snapshot's own marker for a
    transient/recoverable data gap, e.g. ``ILExposureUnavailableError`` on a
    protocol whose IL exposure isn't available yet). Anything else — a
    non-snapshot exception, a non-retryable snapshot error, or an
    ``error``/``critical`` severity — returns ``UNKNOWN`` so action-class
    fast-fail semantics are unchanged.
    """
    # Late import: keeps this module importable without the market package and
    # avoids a circular import at runner import time.
    from almanak.framework.market.errors import MarketSnapshotError

    if not isinstance(exc, MarketSnapshotError):
        return FailureKind.UNKNOWN
    # Normalise case defensively: every in-repo subclass uses lowercase
    # severities, but a future subclass declaring ``severity="Warning"`` must
    # not silently fall back to action-class.
    severity = str(getattr(exc, "severity", "error")).lower()
    retryable = bool(getattr(exc, "retryable", False))
    if retryable and severity in _TRANSIENT_SNAPSHOT_SEVERITIES:
        return FailureKind.DATA_UNAVAILABLE
    return FailureKind.UNKNOWN


def kind_for_status(status: Any, error_message: str | None = None) -> FailureKind:
    """Map an ``IterationStatus`` (+ its error message) to a :class:`FailureKind`.

    The runner records *returned* (non-raised) iteration failures on the circuit
    breaker from ``_run_loop_helpers.handle_iteration_failure`` — a path that,
    unlike the ``decide()`` exception handler, has no live exception to classify.
    Without this mapping a ``DATA_ERROR`` (market data unavailable while the
    strategy held) is recorded as the conservative ``UNKNOWN`` and counts against
    the *action-class* fast-fail threshold (3), re-introducing the exact VIB-3803
    failure mode: a transient / quiet-pool data outage trips the breaker at 3
    instead of the elevated data-class threshold.

    A ``DATA_ERROR`` is data-class **only** when it is transient/quiet-pool. A
    *permanent* data failure (e.g. an unknown/unsupported token — a strategy
    misconfiguration that will never recover) must fail fast like an action
    error rather than idle for the full 30-iteration data-class budget. The
    runner stamps the verdict into the error string as
    ``classification=permanent`` on the HOLD-escalation path, so that token is
    the signal here.

    An unresolved submission is recognised here rather than at each producer
    because the barrier error is the only evidence that survives to this seam:
    the multi-leg, bridge, and legacy-marker lanes arrive with
    ``execution_result=None`` and no typed carrier left to read. It needs BOTH
    halves of :func:`is_unresolved_submission` — the token alone also marks
    outcomes that did resolve.

    Every other status/error pair keeps the ``UNKNOWN`` default so action-class
    semantics are unchanged.
    """
    # Late import to avoid a circular dependency at module import time.
    from .runner_models import IterationStatus

    if status == IterationStatus.DATA_ERROR:
        if error_message is not None and "classification=permanent" in error_message:
            return FailureKind.UNKNOWN
        return FailureKind.DATA_UNAVAILABLE
    if is_unresolved_submission(status, error_message):
        return FailureKind.RECONCILIATION_HOLD
    return FailureKind.UNKNOWN


def is_unresolved_submission(status: Any, error_message: str | None) -> bool:
    """Whether a failure reports a broadcast whose outcome is still unknown.

    Both halves are load-bearing. ``BROADCAST_RECONCILIATION_REQUIRED`` marks
    the replay barrier, not the outcome: a single-chain transaction that LANDED
    with its accounting unsealed carries the same token and resumes as
    ``ACCOUNTING_FAILED``. That submission resolved — the money moved and the
    books are wrong — so it is a real accounting failure and must keep
    action-class semantics. Only an execution status leaves execution itself in
    flight — no conclusive receipt for this submission, or a bridge it started
    that has not delivered — and only then does the token say the outcome is
    unresolved. An accounting status means execution is already over.

    Lanes stamp the token at the start of the message or after a ``[chain]``
    prefix, so the text half is a containment test on a machine token that no
    free-text error produces.
    """
    # Late import to avoid a circular dependency at module import time.
    from .runner_models import IterationStatus

    if status not in (IterationStatus.EXECUTION_FAILED, IterationStatus.EXECUTION_PENDING):
        return False
    if not isinstance(error_message, str) or not error_message:
        return False
    # Late import: the execution package pulls in the connector tree, which
    # must not be forced onto every runner module that classifies a failure.
    from almanak.framework.execution.reconciliation import RECONCILIATION_REQUIRED_PREFIX

    return RECONCILIATION_REQUIRED_PREFIX in error_message


__all__ = ["FailureKind", "classify_failure", "is_unresolved_submission", "kind_for_status"]
