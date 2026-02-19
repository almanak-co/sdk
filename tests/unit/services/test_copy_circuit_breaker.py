"""Tests for CopyCircuitBreaker sliding-window quality gate."""

from decimal import Decimal

from almanak.framework.services.copy_circuit_breaker import (
    CopyCircuitBreaker,
    CopyCircuitBreakerConfig,
)
from almanak.framework.services.copy_trading_models import CopyExecutionRecord


def _ok_record(lag_ms: int = 100, deviation_bps: int = 10) -> CopyExecutionRecord:
    return CopyExecutionRecord(
        event_id="evt-ok",
        status="executed",
        status_code="ok",
        timestamp=1,
        leader_follower_lag_ms=lag_ms,
        price_deviation_bps=deviation_bps,
    )


def _failed_record() -> CopyExecutionRecord:
    return CopyExecutionRecord(
        event_id="evt-fail",
        status="failed",
        status_code="revert",
        timestamp=1,
        leader_follower_lag_ms=100,
        price_deviation_bps=10,
    )


class TestCanExecute:
    def test_initially_allowed(self) -> None:
        cb = CopyCircuitBreaker()
        allowed, reason = cb.can_execute()
        assert allowed is True
        assert reason is None

    def test_not_allowed_after_trip(self) -> None:
        cb = CopyCircuitBreaker()
        cb._trip("test_reason")
        allowed, reason = cb.can_execute()
        assert allowed is False
        assert reason == "test_reason"


class TestRevertRateBreach:
    def test_breaker_trips_on_high_revert_rate(self) -> None:
        cfg = CopyCircuitBreakerConfig(window_size=10, min_samples=5, max_revert_rate=Decimal("0.3"))
        cb = CopyCircuitBreaker(config=cfg)

        # 3 ok + 2 fail = 40% revert rate > 30% threshold
        for _ in range(3):
            cb.record(_ok_record())
        for _ in range(2):
            allowed, reason = cb.record(_failed_record())

        assert allowed is False
        assert reason == "copy_revert_rate_breach"

    def test_breaker_stays_ok_below_threshold(self) -> None:
        cfg = CopyCircuitBreakerConfig(window_size=10, min_samples=5, max_revert_rate=Decimal("0.5"))
        cb = CopyCircuitBreaker(config=cfg)

        for _ in range(4):
            cb.record(_ok_record())
        allowed, reason = cb.record(_failed_record())

        assert allowed is True
        assert reason is None


class TestLagBreach:
    def test_breaker_trips_on_high_avg_lag(self) -> None:
        cfg = CopyCircuitBreakerConfig(window_size=10, min_samples=3, max_avg_lag_ms=500)
        cb = CopyCircuitBreaker(config=cfg)

        for _ in range(3):
            allowed, reason = cb.record(_ok_record(lag_ms=600))

        assert allowed is False
        assert reason == "copy_lag_breach"

    def test_breaker_ok_with_low_lag(self) -> None:
        cfg = CopyCircuitBreakerConfig(window_size=10, min_samples=3, max_avg_lag_ms=500)
        cb = CopyCircuitBreaker(config=cfg)

        for _ in range(3):
            allowed, reason = cb.record(_ok_record(lag_ms=100))

        assert allowed is True


class TestPriceDeviationBreach:
    def test_breaker_trips_on_high_deviation(self) -> None:
        cfg = CopyCircuitBreakerConfig(window_size=10, min_samples=3, max_price_deviation_bps=100)
        cb = CopyCircuitBreaker(config=cfg)

        for _ in range(2):
            cb.record(_ok_record(deviation_bps=50))
        allowed, reason = cb.record(_ok_record(deviation_bps=150))

        assert allowed is False
        assert reason == "copy_price_deviation_breach"

    def test_breaker_ok_within_deviation(self) -> None:
        cfg = CopyCircuitBreakerConfig(window_size=10, min_samples=3, max_price_deviation_bps=200)
        cb = CopyCircuitBreaker(config=cfg)

        for _ in range(3):
            allowed, reason = cb.record(_ok_record(deviation_bps=100))

        assert allowed is True


class TestReset:
    def test_reset_clears_tripped_state(self) -> None:
        cb = CopyCircuitBreaker()
        cb._trip("test")
        assert cb.can_execute()[0] is False

        cb.reset()
        assert cb.can_execute()[0] is True

    def test_reset_clears_history(self) -> None:
        cfg = CopyCircuitBreakerConfig(window_size=10, min_samples=3, max_revert_rate=Decimal("0.3"))
        cb = CopyCircuitBreaker(config=cfg)

        # Add failures but not enough samples to trip
        for _ in range(2):
            cb.record(_failed_record())

        cb.reset()

        # After reset, even more failures should not trip because history is cleared
        cb.record(_failed_record())
        cb.record(_failed_record())
        allowed, _ = cb.can_execute()
        assert allowed is True  # only 2 samples < min_samples=3


class TestMinSamples:
    def test_no_evaluation_below_min_samples(self) -> None:
        cfg = CopyCircuitBreakerConfig(window_size=10, min_samples=5, max_revert_rate=Decimal("0.01"))
        cb = CopyCircuitBreaker(config=cfg)

        # All failures but only 4 samples -- should NOT trip
        for _ in range(4):
            allowed, reason = cb.record(_failed_record())

        assert allowed is True

    def test_evaluates_at_min_samples(self) -> None:
        cfg = CopyCircuitBreakerConfig(window_size=10, min_samples=5, max_revert_rate=Decimal("0.01"))
        cb = CopyCircuitBreaker(config=cfg)

        for _ in range(5):
            allowed, reason = cb.record(_failed_record())

        assert allowed is False
        assert reason == "copy_revert_rate_breach"


class TestWindowSliding:
    def test_old_records_evicted(self) -> None:
        cfg = CopyCircuitBreakerConfig(window_size=5, min_samples=3, max_revert_rate=Decimal("0.5"))
        cb = CopyCircuitBreaker(config=cfg)

        # 3 failures -> trips
        for _ in range(3):
            cb.record(_failed_record())

        # Reset and add 5 ok records to push failures out of window
        cb.reset()
        for _ in range(5):
            cb.record(_ok_record())

        allowed, _ = cb.can_execute()
        assert allowed is True


class TestFromCopyConfig:
    def test_builds_from_v2_config(self) -> None:
        from almanak.framework.services.copy_trading_models import CopyTradingConfigV2

        config = CopyTradingConfigV2.from_config({"risk": {"max_price_deviation_bps": 300}})
        cb = CopyCircuitBreaker.from_copy_config(config)
        assert cb.config.max_price_deviation_bps == 300


class TestStateProperty:
    def test_state_returns_copy(self) -> None:
        cb = CopyCircuitBreaker()
        cb._trip("test_trip")

        state = cb.state
        assert state.tripped is True
        assert state.reason_code == "test_trip"
        assert state.tripped_at is not None
