"""Tests for VIB-3803: FailureKind taxonomy + exposure-aware breaker.

Covers:
- ``FailureKind.is_data_class`` membership.
- ``classify_failure`` recognises VIB-3800 typed exceptions and walks the
  ``__cause__`` chain.
- ``CircuitBreaker.record_exposure`` caches the value.
- ``_effective_data_threshold`` returns the elevated threshold for
  open / stale / unknown exposure, and the standard threshold for
  fresh-known-closed exposure.
- ``record_failure(kind=DATA_*)`` uses the elevated threshold when
  exposure is open; preserves action-class threshold for non-data failures.
- Mixed sequences: data + action accumulate in their own counters; success
  resets both.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import grpc

from almanak.framework.data.interfaces import (
    AllDataSourcesFailed,
    DataSourceRateLimited,
    DataSourceTimeout,
    DataSourceUnavailable,
)
from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
)
from almanak.framework.runner.failure_kind import FailureKind, classify_failure, kind_for_status
from almanak.framework.runner.runner_models import IterationStatus

# ---------------------------------------------------------------------------
# FailureKind
# ---------------------------------------------------------------------------


class TestFailureKindMembership:
    def test_data_kinds(self) -> None:
        assert FailureKind.DATA_UNAVAILABLE.is_data_class
        assert FailureKind.DATA_RATE_LIMITED.is_data_class
        assert FailureKind.DATA_TIMEOUT.is_data_class

    def test_action_kinds(self) -> None:
        assert not FailureKind.EXECUTION_REVERTED.is_data_class
        assert not FailureKind.STATE_CORRUPT.is_data_class
        assert not FailureKind.UNKNOWN.is_data_class


class TestClassifyFailure:
    def test_none_is_unknown(self) -> None:
        assert classify_failure(None) == FailureKind.UNKNOWN

    def test_rate_limited(self) -> None:
        exc = DataSourceRateLimited("upstream", retry_after=1.0)
        assert classify_failure(exc) == FailureKind.DATA_RATE_LIMITED

    def test_timeout(self) -> None:
        exc = DataSourceTimeout("upstream", timeout_seconds=10.0)
        assert classify_failure(exc) == FailureKind.DATA_TIMEOUT

    def test_unavailable(self) -> None:
        exc = DataSourceUnavailable("upstream", reason="down")
        assert classify_failure(exc) == FailureKind.DATA_UNAVAILABLE

    def test_all_sources_failed(self) -> None:
        exc = AllDataSourcesFailed(errors={"a": "x"})
        assert classify_failure(exc) == FailureKind.DATA_UNAVAILABLE

    def test_unknown_for_unrelated(self) -> None:
        assert classify_failure(ValueError("bad input")) == FailureKind.UNKNOWN

    def test_walks_cause_chain(self) -> None:
        # `raise X from typed_exc` is the dominant pattern in the code base.
        try:
            try:
                raise DataSourceRateLimited("upstream", retry_after=1.0)
            except DataSourceRateLimited as e:
                raise RuntimeError("wrapped") from e
        except RuntimeError as wrapper:
            assert classify_failure(wrapper) == FailureKind.DATA_RATE_LIMITED

    def test_unwraps_typed_grpc_trailer(self) -> None:
        # If the gateway packs a typed trailer, classify_failure should
        # pick it up via data_source_error_from_grpc.
        from almanak.framework.grpc.error_details import pack_status_details

        _, _, trailing = pack_status_details(
            code=grpc.StatusCode.RESOURCE_EXHAUSTED,
            message="rate limited",
            retry_delay_seconds=2.0,
            reason="UPSTREAM_RATE_LIMITED",
            upstream="binance",
        )

        class _FakeRpcError(Exception):
            def trailing_metadata(self):
                return trailing

        assert classify_failure(_FakeRpcError("rpc")) == FailureKind.DATA_RATE_LIMITED

    def test_unwraps_typed_grpc_trailer_in_cause_chain(self) -> None:
        # Regression for CodeRabbit finding: the cause walk only used
        # _classify_direct, so a generic RuntimeError wrapping a typed
        # grpc.RpcError would classify as UNKNOWN. The walk must also try
        # the gRPC trailer unwrap on each cause.
        from almanak.framework.grpc.error_details import pack_status_details

        _, _, trailing = pack_status_details(
            code=grpc.StatusCode.UNAVAILABLE,
            message="upstream down",
            retry_delay_seconds=None,
            reason="UPSTREAM_UNAVAILABLE",
            upstream="geckoterminal",
        )

        class _FakeRpcError(Exception):
            def trailing_metadata(self):
                return trailing

        try:
            try:
                raise _FakeRpcError("rpc")
            except _FakeRpcError as e:
                raise RuntimeError("decide() failed") from e
        except RuntimeError as wrapper:
            assert classify_failure(wrapper) == FailureKind.DATA_UNAVAILABLE


class TestClassifyMarketSnapshotErrorVIB5153:
    """VIB-5153 / ALM-2814: transient ``MarketSnapshotError``s are data-class.

    The source bug: ``il_exposure`` on Aerodrome Slipstream raised
    ``ILExposureUnavailableError`` (a ``MarketSnapshotError``, severity=warning,
    retryable=True). It propagated out of ``decide()`` and classify_failure
    returned UNKNOWN → action-class fast-fail (3) → a single unavailable-data
    cycle tripped the breaker and STOPPED the deployment. These tests pin the
    new tolerant classification.
    """

    def test_il_exposure_unavailable_is_data_class(self) -> None:
        from almanak.framework.market.errors import ILExposureUnavailableError

        kind = classify_failure(ILExposureUnavailableError("pos-1", "Slipstream IL unavailable"))
        assert kind == FailureKind.DATA_UNAVAILABLE
        assert kind.is_data_class

    def test_warning_retryable_snapshot_errors_are_data_class(self) -> None:
        # Sweep the transient (severity=warning/info, retryable) snapshot errors.
        from almanak.framework.market.errors import (
            LiquidityDepthUnavailableError,
            RollingSharpeUnavailableError,
            StaleDataError,
        )

        assert classify_failure(StaleDataError("arbitrum", "too old")) == FailureKind.DATA_UNAVAILABLE
        assert classify_failure(LiquidityDepthUnavailableError("0xpool", "thin")) == FailureKind.DATA_UNAVAILABLE
        assert classify_failure(RollingSharpeUnavailableError("short series")) == FailureKind.DATA_UNAVAILABLE

    def test_error_severity_snapshot_errors_stay_action_class(self) -> None:
        # severity="error" (PriceUnavailableError) is NOT auto-tolerated: it may
        # signal something worse than a transient outage, so it keeps the
        # conservative action-class default (narrow VIB-5153 contract).
        from almanak.framework.market.errors import PriceUnavailableError

        assert classify_failure(PriceUnavailableError("ETH", "no source")) == FailureKind.UNKNOWN

    def test_critical_nonretryable_snapshot_error_stays_action_class(self) -> None:
        # A misconfiguration (chain not configured) is critical + non-retryable —
        # it should fast-fail, not idle for the full data-class budget.
        from almanak.framework.market.errors import ChainNotConfiguredError

        assert classify_failure(ChainNotConfiguredError("arbitrum", "unknown chain")) == FailureKind.UNKNOWN

    def test_snapshot_error_in_cause_chain_is_data_class(self) -> None:
        # The dominant production shape: a generic exception wrapping the typed
        # snapshot error via ``raise X from snapshot_err``.
        from almanak.framework.market.errors import ILExposureUnavailableError

        try:
            try:
                raise ILExposureUnavailableError("pos-1", "unavailable")
            except ILExposureUnavailableError as e:
                raise RuntimeError("decide() error") from e
        except RuntimeError as wrapper:
            assert classify_failure(wrapper) == FailureKind.DATA_UNAVAILABLE


# ---------------------------------------------------------------------------
# CircuitBreaker.record_exposure
# ---------------------------------------------------------------------------


class TestRecordExposure:
    def test_records_open_exposure(self) -> None:
        breaker = CircuitBreaker("test")
        breaker.record_exposure(True)
        status = breaker.get_status()
        assert status["last_known_exposure_open"] is True
        assert status["last_exposure_at"] is not None

    def test_records_closed_exposure(self) -> None:
        breaker = CircuitBreaker("test")
        breaker.record_exposure(False)
        status = breaker.get_status()
        assert status["last_known_exposure_open"] is False


# ---------------------------------------------------------------------------
# _effective_data_threshold
# ---------------------------------------------------------------------------


class TestEffectiveDataThreshold:
    def test_no_exposure_recorded_returns_elevated(self) -> None:
        # Safe default: never been recorded ⇒ assume open ⇒ high tolerance.
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
        )
        breaker = CircuitBreaker("test", cfg)
        assert breaker._effective_data_threshold() == 30

    def test_known_open_returns_elevated(self) -> None:
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
        )
        breaker = CircuitBreaker("test", cfg)
        breaker.record_exposure(True)
        assert breaker._effective_data_threshold() == 30

    def test_known_closed_returns_standard(self) -> None:
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
        )
        breaker = CircuitBreaker("test", cfg)
        breaker.record_exposure(False)
        assert breaker._effective_data_threshold() == 3

    def test_stale_exposure_returns_elevated(self) -> None:
        # Stale: last recording older than freshness ⇒ assume open.
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
            exposure_freshness_seconds=300,
        )
        breaker = CircuitBreaker("test", cfg)
        breaker.record_exposure(False)
        # Manually rewind the cached time stamp.
        breaker._last_exposure_at = datetime.now(UTC) - timedelta(seconds=600)
        assert breaker._effective_data_threshold() == 30


# ---------------------------------------------------------------------------
# record_failure with FailureKind
# ---------------------------------------------------------------------------


class TestRecordFailureKind:
    def test_action_class_trips_at_action_threshold(self) -> None:
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
        )
        breaker = CircuitBreaker("test", cfg)

        for _ in range(3):
            breaker.record_failure("execution failed", kind=FailureKind.EXECUTION_REVERTED)

        assert breaker.state == CircuitBreakerState.OPEN

    def test_data_class_with_open_exposure_trips_at_elevated(self) -> None:
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
        )
        breaker = CircuitBreaker("test", cfg)
        breaker.record_exposure(True)

        # 29 data failures — no trip yet.
        for _ in range(29):
            breaker.record_failure("upstream blip", kind=FailureKind.DATA_UNAVAILABLE)
        assert breaker.state == CircuitBreakerState.CLOSED

        # 30th trips.
        breaker.record_failure("upstream blip", kind=FailureKind.DATA_UNAVAILABLE)
        assert breaker.state == CircuitBreakerState.OPEN

    def test_data_class_with_closed_exposure_uses_standard_threshold(self) -> None:
        # No open exposure → no risk to manage → fast-fail is fine.
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
        )
        breaker = CircuitBreaker("test", cfg)
        breaker.record_exposure(False)

        for _ in range(3):
            breaker.record_failure("upstream blip", kind=FailureKind.DATA_UNAVAILABLE)
        assert breaker.state == CircuitBreakerState.OPEN

    def test_data_class_with_unknown_exposure_uses_elevated(self) -> None:
        # Safe default: exposure never recorded → assume open → 30.
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
        )
        breaker = CircuitBreaker("test", cfg)

        for _ in range(3):
            breaker.record_failure("upstream blip", kind=FailureKind.DATA_UNAVAILABLE)
        # 3 data failures with unknown exposure must NOT trip.
        assert breaker.state == CircuitBreakerState.CLOSED

    def test_data_class_with_stale_exposure_uses_elevated(self) -> None:
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
            exposure_freshness_seconds=300,
        )
        breaker = CircuitBreaker("test", cfg)
        breaker.record_exposure(False)
        breaker._last_exposure_at = datetime.now(UTC) - timedelta(seconds=600)

        for _ in range(3):
            breaker.record_failure("upstream blip", kind=FailureKind.DATA_UNAVAILABLE)
        # Stale "no exposure" must NOT cause fast-fail — safe default kicks in.
        assert breaker.state == CircuitBreakerState.CLOSED

    def test_unknown_kind_treated_as_action_class(self) -> None:
        # Default kind=None / UNKNOWN preserves existing behavior.
        cfg = CircuitBreakerConfig(max_consecutive_failures=3)
        breaker = CircuitBreaker("test", cfg)

        for _ in range(3):
            breaker.record_failure("something")
        assert breaker.state == CircuitBreakerState.OPEN

    def test_mixed_kinds_use_independent_counters(self) -> None:
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
        )
        breaker = CircuitBreaker("test", cfg)
        breaker.record_exposure(True)

        # 2 action + 5 data — no trip yet. Action counter at 2, data at 5.
        breaker.record_failure("a", kind=FailureKind.EXECUTION_REVERTED)
        breaker.record_failure("a", kind=FailureKind.EXECUTION_REVERTED)
        for _ in range(5):
            breaker.record_failure("d", kind=FailureKind.DATA_UNAVAILABLE)
        assert breaker.state == CircuitBreakerState.CLOSED

        # One more action ⇒ action counter = 3 ⇒ trips.
        breaker.record_failure("a", kind=FailureKind.EXECUTION_REVERTED)
        assert breaker.state == CircuitBreakerState.OPEN

    def test_success_resets_both_counters(self) -> None:
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=3,
            data_class_max_consecutive_failures=30,
        )
        breaker = CircuitBreaker("test", cfg)
        breaker.record_exposure(True)

        # 2 action + 5 data
        breaker.record_failure("a", kind=FailureKind.EXECUTION_REVERTED)
        breaker.record_failure("a", kind=FailureKind.EXECUTION_REVERTED)
        for _ in range(5):
            breaker.record_failure("d", kind=FailureKind.DATA_UNAVAILABLE)
        assert breaker.get_status()["consecutive_action_failures"] == 2
        assert breaker.get_status()["consecutive_data_failures"] == 5

        # Success clears both.
        breaker.record_success()
        assert breaker.get_status()["consecutive_action_failures"] == 0
        assert breaker.get_status()["consecutive_data_failures"] == 0
        assert breaker.get_status()["consecutive_failures"] == 0

    def test_legacy_consecutive_failures_total_preserved(self) -> None:
        # Sum of (action + data) — used by tests that assert on the legacy
        # attribute directly (e.g. test_runner_safety_wiring.py).
        cfg = CircuitBreakerConfig(max_consecutive_failures=10)
        breaker = CircuitBreaker("test", cfg)

        breaker.record_failure("a", kind=FailureKind.EXECUTION_REVERTED)
        breaker.record_failure("d", kind=FailureKind.DATA_UNAVAILABLE)
        assert breaker._consecutive_failures == 2

    def test_status_payload_includes_new_fields(self) -> None:
        breaker = CircuitBreaker("test")
        status = breaker.get_status()
        assert "consecutive_action_failures" in status
        assert "consecutive_data_failures" in status
        assert "effective_data_threshold" in status
        assert "last_known_exposure_open" in status
        assert "last_exposure_at" in status

    def test_reset_clears_exposure_cache(self) -> None:
        breaker = CircuitBreaker("test")
        breaker.record_exposure(True)
        breaker.reset()
        assert breaker.get_status()["last_known_exposure_open"] is None
        assert breaker.get_status()["last_exposure_at"] is None

    def test_pause_then_resume_resets_split_counters(self) -> None:
        # Regression for CodeRabbit finding: _close() (used on resume) must
        # reset the per-kind split counters or the breaker comes back CLOSED
        # with stale counts and trips at threshold-1 instead of threshold.
        cfg = CircuitBreakerConfig(max_consecutive_failures=3, data_class_max_consecutive_failures=10)
        breaker = CircuitBreaker("test", cfg)

        breaker.record_failure("a", kind=FailureKind.EXECUTION_REVERTED)
        breaker.record_failure("b", kind=FailureKind.EXECUTION_REVERTED)
        # Two action failures recorded but not enough to trip.

        breaker.pause("ops", "alice")
        breaker.resume("alice")

        status = breaker.get_status()
        assert status["consecutive_action_failures"] == 0
        assert status["consecutive_data_failures"] == 0
        assert status["consecutive_failures"] == 0


# ---------------------------------------------------------------------------
# Regression scenario: 29 Apr Aerodrome incident
# ---------------------------------------------------------------------------


class TestAprilIncidentRegressionGuard:
    """Concretely re-runs the 29 Apr 2026 Aerodrome circuit-breaker incident:

    - Strategy holds open LP positions.
    - 3 consecutive transient ``DataSourceUnavailable`` failures
      (GeckoTerminal OHLCV).

    Pre-VIB-3803 behavior: breaker tripped at iteration 3, K8s crash-looped
    the pod.

    Post-VIB-3803 behavior: breaker stays closed (data-class threshold = 30
    when exposure is open).
    """

    def test_three_transient_data_failures_with_open_exposure_does_not_trip(
        self,
    ) -> None:
        breaker = CircuitBreaker("aerodrome_lp")
        breaker.record_exposure(True)  # Open LP positions

        for i in range(3):
            exc = DataSourceUnavailable(
                source="gateway_geckoterminal",
                reason="upstream temporarily unavailable",
            )
            breaker.record_failure(f"decide() error: {exc}", kind=classify_failure(exc))
            assert breaker.state == CircuitBreakerState.CLOSED, (
                f"Iteration {i + 1}: breaker tripped early — VIB-3803 regression!"
            )

        # And the counter reflects 3 data failures, 0 action failures.
        status = breaker.get_status()
        assert status["consecutive_data_failures"] == 3
        assert status["consecutive_action_failures"] == 0

    def test_one_recovery_resets_both_counters(self) -> None:
        breaker = CircuitBreaker("aerodrome_lp")
        breaker.record_exposure(True)

        # 5 data blips, then a successful execution.
        for _ in range(5):
            breaker.record_failure("blip", kind=FailureKind.DATA_UNAVAILABLE)
        breaker.record_success()

        status = breaker.get_status()
        assert status["consecutive_data_failures"] == 0
        assert status["consecutive_action_failures"] == 0


# ---------------------------------------------------------------------------
# kind_for_status: returned-result failures get classified (VIB-3803 parity
# for the no-live-exception path in handle_iteration_failure)
# ---------------------------------------------------------------------------


class TestKindForStatus:
    def test_data_error_maps_to_data_class(self) -> None:
        kind = kind_for_status(IterationStatus.DATA_ERROR)
        assert kind == FailureKind.DATA_UNAVAILABLE
        assert kind.is_data_class

    def test_transient_data_error_is_data_class(self) -> None:
        kind = kind_for_status(
            IterationStatus.DATA_ERROR,
            "Critical market-data failures while strategy returned HOLD (classification=transient): ...",
        )
        assert kind == FailureKind.DATA_UNAVAILABLE
        assert kind.is_data_class

    def test_permanent_data_error_is_action_class(self) -> None:
        # A permanent data failure (unknown token = misconfiguration) must fail
        # fast like an action error, not idle for the 30-iteration data budget.
        kind = kind_for_status(
            IterationStatus.DATA_ERROR,
            "Critical market-data failures while strategy returned HOLD (classification=permanent): "
            "rsi(...): Unknown token for Binance: NVDAON",
        )
        assert kind == FailureKind.UNKNOWN
        assert not kind.is_data_class

    def test_data_error_without_message_defaults_to_data_class(self) -> None:
        assert kind_for_status(IterationStatus.DATA_ERROR, None) == FailureKind.DATA_UNAVAILABLE

    def test_other_statuses_default_to_unknown_action_class(self) -> None:
        for status in (
            IterationStatus.STRATEGY_ERROR,
            IterationStatus.STRATEGY_TIMEOUT,
            IterationStatus.SUCCESS,
            IterationStatus.HOLD,
        ):
            assert kind_for_status(status) == FailureKind.UNKNOWN
            assert not kind_for_status(status).is_data_class


# ---------------------------------------------------------------------------
# tripped_on_data_class_only — lets the runner keep the process alive on a pure
# market-data trip (don't turn a transient/quiet-pool data gap into a dead pod)
# ---------------------------------------------------------------------------


class TestTrippedOnDataClassOnly:
    def test_data_only_trip_sets_flag(self) -> None:
        # Closed exposure → the data-class threshold collapses to the standard 3
        # (this is exactly the NVDAON no-position case): the breaker still trips,
        # but the trip must be marked data-only so the runner won't exit.
        cfg = CircuitBreakerConfig(max_consecutive_failures=3, data_class_max_consecutive_failures=30)
        breaker = CircuitBreaker("nvdaon", cfg)
        breaker.record_exposure(False)

        for _ in range(3):
            breaker.record_failure(
                "All providers failed for NVDAON/USD ... returned stale OHLCV ... provider miss",
                kind=FailureKind.DATA_UNAVAILABLE,
            )

        assert breaker.state == CircuitBreakerState.OPEN
        assert breaker.tripped_on_data_class_only is True

    def test_action_trip_does_not_set_flag(self) -> None:
        cfg = CircuitBreakerConfig(max_consecutive_failures=3)
        breaker = CircuitBreaker("test", cfg)
        for _ in range(3):
            breaker.record_failure("revert", kind=FailureKind.EXECUTION_REVERTED)
        assert breaker.state == CircuitBreakerState.OPEN
        assert breaker.tripped_on_data_class_only is False

    def test_unknown_kind_trip_is_not_data_only(self) -> None:
        # UNKNOWN is action-class; a trip driven by it is not a data outage.
        cfg = CircuitBreakerConfig(max_consecutive_failures=3)
        breaker = CircuitBreaker("test", cfg)
        for _ in range(3):
            breaker.record_failure("mystery")
        assert breaker.state == CircuitBreakerState.OPEN
        assert breaker.tripped_on_data_class_only is False

    def test_mixed_trip_via_action_is_not_data_only(self) -> None:
        cfg = CircuitBreakerConfig(max_consecutive_failures=3, data_class_max_consecutive_failures=30)
        breaker = CircuitBreaker("test", cfg)
        breaker.record_exposure(True)
        for _ in range(5):  # data failures accumulate but don't trip (elevated)
            breaker.record_failure("blip", kind=FailureKind.DATA_UNAVAILABLE)
        for _ in range(3):  # action failures trip at 3
            breaker.record_failure("revert", kind=FailureKind.EXECUTION_REVERTED)
        assert breaker.state == CircuitBreakerState.OPEN
        assert breaker.tripped_on_data_class_only is False

    def test_cumulative_loss_trip_is_not_data_only(self) -> None:
        cfg = CircuitBreakerConfig(
            max_consecutive_failures=100,
            max_cumulative_loss_usd=Decimal("10"),
        )
        breaker = CircuitBreaker("test", cfg)
        breaker.record_failure("loss", loss_usd=Decimal("11"), kind=FailureKind.DATA_UNAVAILABLE)
        assert breaker.state == CircuitBreakerState.OPEN  # tripped on loss, not consecutive
        assert breaker.tripped_on_data_class_only is False

    def test_flag_resets_on_close(self) -> None:
        cfg = CircuitBreakerConfig(max_consecutive_failures=3, data_class_max_consecutive_failures=30)
        breaker = CircuitBreaker("test", cfg)
        breaker.record_exposure(False)
        for _ in range(3):
            breaker.record_failure("stale", kind=FailureKind.DATA_UNAVAILABLE)
        assert breaker.tripped_on_data_class_only is True

        breaker.pause("ops", "alice")
        breaker.resume("alice")  # _close() path
        assert breaker.tripped_on_data_class_only is False
