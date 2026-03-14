"""Tests for StrategyRunner safety wiring: CircuitBreaker, decide() timeout, StuckDetector.

Covers VIB-1252, VIB-1253, VIB-1255.
"""

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.interfaces import BalanceResult, PriceResult
from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
)
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.intents.vocabulary import HoldIntent
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.state.state_manager import StateData

# =============================================================================
# Mock Classes (shared with existing tests)
# =============================================================================


@dataclass
class MockMarketSnapshot:
    chain: str = "arbitrum"
    wallet_address: str = "0x1234567890123456789012345678901234567890"


class MockStrategy:
    def __init__(
        self,
        strategy_id: str = "test_strategy",
        chain: str = "arbitrum",
        wallet_address: str = "0x1234567890123456789012345678901234567890",
        decide_returns: Any | None = None,
        decide_raises: Exception | None = None,
        decide_delay: float = 0.0,
    ) -> None:
        self._strategy_id = strategy_id
        self._chain = chain
        self._wallet_address = wallet_address
        self._decide_returns = decide_returns
        self._decide_raises = decide_raises
        self._decide_delay = decide_delay
        self.decide_call_count = 0

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    @property
    def chain(self) -> str:
        return self._chain

    @property
    def wallet_address(self) -> str:
        return self._wallet_address

    def decide(self, market: Any) -> Any | None:
        self.decide_call_count += 1
        if self._decide_delay > 0:
            time.sleep(self._decide_delay)
        if self._decide_raises:
            raise self._decide_raises
        return self._decide_returns

    def create_market_snapshot(self) -> MockMarketSnapshot:
        return MockMarketSnapshot(chain=self._chain, wallet_address=self._wallet_address)


class MockPriceOracle:
    async def get_aggregated_price(self, token: str, quote: str = "USD") -> PriceResult:
        return PriceResult(price=Decimal("2000"), source="mock", timestamp=datetime.now(UTC), confidence=1.0)

    def get_source_health(self, source_name: str) -> dict[str, Any] | None:
        return {"status": "healthy"}


class MockBalanceProvider:
    async def get_balance(self, token: str) -> BalanceResult:
        return BalanceResult(balance=Decimal("10000"), token=token, address="0x" + "0" * 40, decimals=6, raw_balance=0)

    async def get_native_balance(self) -> BalanceResult:
        return await self.get_balance("ETH")

    def invalidate_cache(self, token: str | None = None) -> None:
        pass


class MockExecutionOrchestrator:
    def __init__(self, success: bool = True, error: str | None = None) -> None:
        self._success = success
        self._error = error
        self.execute_called = False

    async def execute(self, action_bundle: ActionBundle, context: Any | None = None) -> ExecutionResult:
        self.execute_called = True
        result = MagicMock(spec=ExecutionResult)
        result.success = self._success
        result.error = self._error
        result.phase = ExecutionPhase.COMPLETE if self._success else ExecutionPhase.VALIDATION
        result.transaction_results = []
        result.total_gas_used = 100000
        result.total_gas_cost_wei = 1000000000000
        result.to_dict = MagicMock(return_value={"success": self._success, "error": self._error})
        return result


class MockStateManager:
    def __init__(self) -> None:
        self._states: dict[str, StateData] = {}
        self.initialized = False

    async def initialize(self) -> None:
        self.initialized = True

    async def close(self) -> None:
        pass

    async def load_state(self, strategy_id: str) -> StateData:
        if strategy_id not in self._states:
            self._states[strategy_id] = StateData(strategy_id=strategy_id, version=1, state={})
        return self._states[strategy_id]

    async def save_state(self, state: StateData, expected_version: int | None = None) -> StateData:
        state.version += 1
        self._states[state.strategy_id] = state
        return state


class MockAlertManager:
    def __init__(self) -> None:
        self.alerts_sent: list[Any] = []

    async def send_alert(self, card: Any, metric_values: Any = None) -> Any:
        self.alerts_sent.append(card)
        result = MagicMock()
        result.success = True
        return result

    def send_alert_sync(self, card: Any, metric_values: Any = None) -> Any:
        self.alerts_sent.append(card)
        result = MagicMock()
        result.success = True
        return result


# =============================================================================
# Fixtures
# =============================================================================


def _make_runner(
    circuit_breaker: CircuitBreaker | None = None,
    config: RunnerConfig | None = None,
    orchestrator: MockExecutionOrchestrator | None = None,
    alert_manager: MockAlertManager | None = None,
) -> StrategyRunner:
    return StrategyRunner(
        price_oracle=MockPriceOracle(),
        balance_provider=MockBalanceProvider(),
        execution_orchestrator=orchestrator or MockExecutionOrchestrator(),
        state_manager=MockStateManager(),
        alert_manager=alert_manager or MockAlertManager(),
        config=config or RunnerConfig(),
        circuit_breaker=circuit_breaker,
    )


# =============================================================================
# VIB-1252: CircuitBreaker Wiring
# =============================================================================


class TestCircuitBreakerWiring:
    """Tests that CircuitBreaker is correctly wired into StrategyRunner."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_blocks_execution_when_open(self):
        """When circuit breaker is OPEN, run_iteration should return CIRCUIT_BREAKER_OPEN."""
        breaker = CircuitBreaker("test_strategy", CircuitBreakerConfig(max_consecutive_failures=1))
        # Trip the breaker
        breaker.record_failure("forced failure")

        runner = _make_runner(circuit_breaker=breaker)
        strategy = MockStrategy(decide_returns=HoldIntent(reason="should not be called"))

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.CIRCUIT_BREAKER_OPEN
        assert strategy.decide_call_count == 0  # decide() should NOT be called
        assert "Circuit breaker open" in (result.error or "")

    @pytest.mark.asyncio
    async def test_circuit_breaker_allows_when_closed(self):
        """When circuit breaker is CLOSED, execution proceeds normally."""
        breaker = CircuitBreaker("test_strategy")
        runner = _make_runner(circuit_breaker=breaker)
        strategy = MockStrategy(decide_returns=HoldIntent(reason="test hold"))

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.HOLD
        assert strategy.decide_call_count == 1

    @pytest.mark.asyncio
    async def test_circuit_breaker_hold_is_neutral(self):
        """HOLD results should NOT reset breaker (neutral — doesn't prove execution works)."""
        breaker = CircuitBreaker("test_strategy", CircuitBreakerConfig(max_consecutive_failures=3))
        breaker.record_failure("fail 1")
        breaker.record_failure("fail 2")
        assert breaker._consecutive_failures == 2

        runner = _make_runner(circuit_breaker=breaker)
        strategy = MockStrategy(decide_returns=HoldIntent(reason="test"))

        # Run via run_loop so post-iteration recording fires
        await runner.run_loop(strategy, max_iterations=1)

        # HOLD is neutral — failures should NOT be cleared
        assert breaker._consecutive_failures == 2

    @pytest.mark.asyncio
    async def test_circuit_breaker_trips_after_consecutive_failures(self):
        """After max_consecutive_failures, breaker should trip and block subsequent iterations."""
        breaker = CircuitBreaker("test_strategy", CircuitBreakerConfig(max_consecutive_failures=2))
        runner = _make_runner(circuit_breaker=breaker)
        strategy = MockStrategy(decide_raises=ValueError("boom"))

        # First failure
        r1 = await runner.run_iteration(strategy)
        assert r1.status == IterationStatus.STRATEGY_ERROR
        # Circuit breaker records failure via decide() error path
        assert breaker._consecutive_failures == 1

        # Second failure trips the breaker
        r2 = await runner.run_iteration(strategy)
        assert r2.status == IterationStatus.STRATEGY_ERROR
        assert breaker._consecutive_failures == 2
        assert breaker.state == CircuitBreakerState.OPEN

        # Third attempt should be blocked
        r3 = await runner.run_iteration(strategy)
        assert r3.status == IterationStatus.CIRCUIT_BREAKER_OPEN
        assert strategy.decide_call_count == 2  # decide not called on 3rd attempt

    @pytest.mark.asyncio
    async def test_no_circuit_breaker_still_works(self):
        """Runner should work fine without a circuit breaker (backward compat)."""
        runner = _make_runner(circuit_breaker=None)
        strategy = MockStrategy(decide_returns=HoldIntent(reason="no breaker"))

        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.HOLD

    @pytest.mark.asyncio
    async def test_circuit_breaker_paused_blocks_execution(self):
        """When circuit breaker is manually PAUSED, execution should be blocked."""
        breaker = CircuitBreaker("test_strategy")
        breaker.pause(reason="maintenance", operator="test@example.com")

        runner = _make_runner(circuit_breaker=breaker)
        strategy = MockStrategy(decide_returns=HoldIntent(reason="should not run"))

        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.CIRCUIT_BREAKER_OPEN
        assert strategy.decide_call_count == 0

    @pytest.mark.asyncio
    async def test_circuit_breaker_allows_teardown_when_open(self):
        """An OPEN circuit breaker must NOT block teardown — operators must always be able to close positions."""
        breaker = CircuitBreaker("test_strategy", CircuitBreakerConfig(max_consecutive_failures=1))
        breaker.record_failure("forced failure")
        assert breaker.state == CircuitBreakerState.OPEN

        runner = _make_runner(circuit_breaker=breaker)
        strategy = MockStrategy(decide_returns=HoldIntent(reason="should not be called"))

        # Mock _check_teardown_requested to simulate a pending teardown
        from unittest.mock import patch

        with patch.object(runner, "_check_teardown_requested", return_value="GRACEFUL"):
            result = await runner.run_iteration(strategy)

        # Should NOT return CIRCUIT_BREAKER_OPEN — teardown bypasses the breaker
        assert result.status != IterationStatus.CIRCUIT_BREAKER_OPEN
        # decide() should not be called — the teardown path intercepts before decide()
        assert strategy.decide_call_count == 0


# =============================================================================
# VIB-1253: decide() Timeout
# =============================================================================


class TestDecideTimeout:
    """Tests for hard timeout around strategy.decide()."""

    @pytest.mark.asyncio
    async def test_decide_timeout_fires(self):
        """A slow decide() should be killed after the configured timeout."""
        config = RunnerConfig(decide_timeout_seconds=0.5)
        runner = _make_runner(config=config)
        # Strategy that sleeps for 5 seconds (well beyond 0.5s timeout)
        strategy = MockStrategy(decide_delay=5.0)

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.STRATEGY_TIMEOUT
        assert "timed out" in (result.error or "")

    @pytest.mark.asyncio
    async def test_decide_timeout_feeds_circuit_breaker(self):
        """A timeout should be recorded as a failure in the circuit breaker."""
        breaker = CircuitBreaker("test_strategy", CircuitBreakerConfig(max_consecutive_failures=2))
        config = RunnerConfig(decide_timeout_seconds=0.5)
        runner = _make_runner(circuit_breaker=breaker, config=config)
        strategy = MockStrategy(decide_delay=5.0)

        await runner.run_iteration(strategy)

        assert breaker._consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_fast_decide_not_affected_by_timeout(self):
        """A fast decide() should complete normally even with a short timeout."""
        config = RunnerConfig(decide_timeout_seconds=10.0)
        runner = _make_runner(config=config)
        strategy = MockStrategy(decide_returns=HoldIntent(reason="fast"))

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.HOLD
        assert strategy.decide_call_count == 1

    @pytest.mark.asyncio
    async def test_decide_timeout_zero_disables(self):
        """Setting timeout to 0 should disable the timeout."""
        config = RunnerConfig(decide_timeout_seconds=0)
        runner = _make_runner(config=config)
        strategy = MockStrategy(decide_returns=HoldIntent(reason="no timeout"))

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.HOLD

    @pytest.mark.asyncio
    async def test_decide_error_feeds_circuit_breaker(self):
        """A decide() exception should be recorded as a failure in the circuit breaker."""
        breaker = CircuitBreaker("test_strategy", CircuitBreakerConfig(max_consecutive_failures=5))
        runner = _make_runner(circuit_breaker=breaker)
        strategy = MockStrategy(decide_raises=RuntimeError("crash"))

        await runner.run_iteration(strategy)

        assert breaker._consecutive_failures == 1


# =============================================================================
# VIB-1255: StuckDetector and OperatorCard Wiring
# =============================================================================


class TestStuckDetectorWiring:
    """Tests that StuckDetector and OperatorCardGenerator are wired into the runner."""

    @pytest.mark.asyncio
    async def test_stuck_detection_runs_on_failure(self):
        """Stuck detection should be triggered when consecutive failures occur."""
        alert_manager = MockAlertManager()
        runner = _make_runner(alert_manager=alert_manager)
        strategy = MockStrategy(decide_raises=ValueError("stuck test"))

        # First failure — sets _failure_state_entered_at
        await runner.run_iteration(strategy)

        # Simulate time passing so stuck detector triggers (>10min default)
        runner._failure_state_entered_at = datetime.now(UTC) - timedelta(minutes=15)

        # Second failure — should trigger stuck detection
        # Use run_loop with max_iterations=1 to trigger the post-iteration stuck detection
        await runner.run_loop(strategy, max_iterations=1)

        # Verify stuck detection was lazy-initialized
        assert runner._stuck_detector is not None
        assert runner._operator_card_generator is not None

    @pytest.mark.asyncio
    async def test_stuck_detection_sends_operator_card_to_alert_manager(self):
        """When stuck is detected, an OperatorCard should be sent via AlertManager."""
        alert_manager = MockAlertManager()
        runner = _make_runner(alert_manager=alert_manager)

        # Simulate pre-existing failure state that is old enough to be "stuck"
        runner._failure_state_entered_at = datetime.now(UTC) - timedelta(minutes=15)

        strategy = MockStrategy(decide_raises=ValueError("stuck"))

        # Run a single loop iteration to trigger post-iteration stuck detection
        await runner.run_loop(strategy, max_iterations=1)

        # Should have received at least one OperatorCard alert
        assert len(alert_manager.alerts_sent) >= 1

    @pytest.mark.asyncio
    async def test_stuck_detection_does_not_fire_on_success(self):
        """Stuck detection should not fire on successful iterations."""
        alert_manager = MockAlertManager()
        runner = _make_runner(alert_manager=alert_manager)
        strategy = MockStrategy(decide_returns=HoldIntent(reason="all good"))

        await runner.run_loop(strategy, max_iterations=1)

        # No stuck alerts should have been sent (HOLD is a success)
        assert len(alert_manager.alerts_sent) == 0

    @pytest.mark.asyncio
    async def test_failure_state_timestamp_tracks_first_failure(self):
        """_failure_state_entered_at should be set on first failure and persist across subsequent failures."""
        runner = _make_runner()
        strategy = MockStrategy(decide_raises=ValueError("fail"))

        assert runner._failure_state_entered_at is None

        await runner.run_loop(strategy, max_iterations=1)
        first_failure_at = runner._failure_state_entered_at
        assert first_failure_at is not None

        # Second failure should NOT overwrite the timestamp
        await runner.run_loop(strategy, max_iterations=1)
        assert runner._failure_state_entered_at == first_failure_at

    @pytest.mark.asyncio
    async def test_failure_state_resets_on_success(self):
        """_failure_state_entered_at should reset when a successful iteration occurs."""
        runner = _make_runner()

        # Simulate an existing failure timestamp
        runner._failure_state_entered_at = datetime.now(UTC) - timedelta(minutes=5)

        strategy = MockStrategy(decide_returns=HoldIntent(reason="recovered"))

        await runner.run_loop(strategy, max_iterations=1)

        # Should have been cleared on success
        assert runner._failure_state_entered_at is None

    @pytest.mark.asyncio
    async def test_stuck_detection_is_non_fatal(self):
        """Even if stuck detection crashes, the runner should continue."""
        runner = _make_runner()
        strategy = MockStrategy(decide_raises=ValueError("fail"))

        # Inject a broken stuck detector that raises
        runner._stuck_detector = MagicMock()
        runner._stuck_detector.detect_stuck.side_effect = RuntimeError("detector crash")
        runner._failure_state_entered_at = datetime.now(UTC) - timedelta(minutes=15)

        # Should not raise - stuck detection is non-fatal
        await runner.run_loop(strategy, max_iterations=1)


# =============================================================================
# Integration: All three features together
# =============================================================================


class TestSafetyIntegration:
    """Integration tests combining circuit breaker, timeout, and stuck detection."""

    @pytest.mark.asyncio
    async def test_timeout_trips_breaker_then_blocks(self):
        """Repeated timeouts should trip the circuit breaker."""
        breaker = CircuitBreaker("test_strategy", CircuitBreakerConfig(max_consecutive_failures=2))
        config = RunnerConfig(decide_timeout_seconds=0.3)
        runner = _make_runner(circuit_breaker=breaker, config=config)
        strategy = MockStrategy(decide_delay=5.0)

        # Two timeouts should trip the breaker
        r1 = await runner.run_iteration(strategy)
        assert r1.status == IterationStatus.STRATEGY_TIMEOUT

        r2 = await runner.run_iteration(strategy)
        assert r2.status == IterationStatus.STRATEGY_TIMEOUT

        # Breaker should now be open
        assert breaker.state == CircuitBreakerState.OPEN

        # Third attempt should be blocked by circuit breaker
        r3 = await runner.run_iteration(strategy)
        assert r3.status == IterationStatus.CIRCUIT_BREAKER_OPEN

    @pytest.mark.asyncio
    async def test_iteration_status_new_values_serialize(self):
        """New IterationStatus values should serialize properly."""
        assert IterationStatus.CIRCUIT_BREAKER_OPEN.value == "CIRCUIT_BREAKER_OPEN"
        assert IterationStatus.STRATEGY_TIMEOUT.value == "STRATEGY_TIMEOUT"

        # Verify they appear in IterationResult.to_dict()
        result = IterationResult(
            status=IterationStatus.CIRCUIT_BREAKER_OPEN,
            error="breaker open",
            strategy_id="test",
        )
        d = result.to_dict()
        assert d["status"] == "CIRCUIT_BREAKER_OPEN"
