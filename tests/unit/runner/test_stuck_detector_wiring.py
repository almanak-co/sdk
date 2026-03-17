"""Tests for StuckDetector + OperatorCardGenerator wiring into StrategyRunner (VIB-1255).

Verifies that:
- Runner accepts optional stuck_detector and operator_card_generator
- _alert_consecutive_errors uses OperatorCardGenerator when available
- _alert_consecutive_errors uses StuckDetector for classification when available
- _handle_execution_error uses OperatorCardGenerator when available
- Fallback to basic cards when no generator is configured
- _first_error_at tracking for stuck_since calculation
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.models import StuckReason
from almanak.framework.models.operator_card import EventType, Severity
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.services.operator_card_generator import (
    OperatorCardGenerator,
)
from almanak.framework.services.stuck_detector import (
    StuckDetectionResult,
    StuckDetector,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_strategy(decide_side_effect=None):
    """Create a mock strategy that fails."""
    from almanak.framework.intents.vocabulary import HoldIntent

    strategy = MagicMock()
    strategy.strategy_id = "test_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.generate_teardown_intents.side_effect = NotImplementedError

    if decide_side_effect is not None:
        strategy.decide.side_effect = decide_side_effect
    else:
        strategy.decide.side_effect = RuntimeError("strategy bug")

    return strategy


def _make_runner(
    alert_manager=None,
    stuck_detector=None,
    operator_card_generator=None,
    max_consecutive_errors=2,
):
    """Create a StrategyRunner with optional components."""
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=alert_manager is not None,
        dry_run=False,
        max_consecutive_errors=max_consecutive_errors,
    )
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=alert_manager,
        config=config,
        stuck_detector=stuck_detector,
        operator_card_generator=operator_card_generator,
    )
    return runner


# Patch paths for skipping internal runner checks
_PAUSE_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._is_strategy_paused"
_TEARDOWN_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._check_teardown_requested"


# =============================================================================
# Tests: Init
# =============================================================================


class TestStuckDetectorInit:
    def test_runner_accepts_stuck_detector(self):
        detector = StuckDetector(emit_events=False)
        runner = _make_runner(stuck_detector=detector)
        assert runner._stuck_detector is detector

    def test_runner_accepts_operator_card_generator(self):
        gen = OperatorCardGenerator()
        runner = _make_runner(operator_card_generator=gen)
        assert runner._operator_card_generator is gen

    def test_runner_works_without_either(self):
        runner = _make_runner()
        assert runner._stuck_detector is None
        assert runner._operator_card_generator is None


# =============================================================================
# Tests: _first_error_at tracking
# =============================================================================


class TestFirstErrorTracking:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_first_error_at_set_on_first_failure(self, _mock_pause, _mock_teardown):
        runner = _make_runner()
        strategy = _make_strategy()

        assert runner._first_error_at is None
        await runner.run_loop(strategy, max_iterations=1)
        assert runner._first_error_at is not None
        assert isinstance(runner._first_error_at, datetime)

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_first_error_at_not_reset_on_subsequent_failures(self, _mock_pause, _mock_teardown):
        runner = _make_runner()
        strategy = _make_strategy()

        await runner.run_loop(strategy, max_iterations=1)
        first_ts = runner._first_error_at

        await runner.run_loop(strategy, max_iterations=1)
        assert runner._first_error_at == first_ts  # Same timestamp

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_first_error_at_cleared_on_success(self, _mock_pause, _mock_teardown):
        from almanak.framework.intents.vocabulary import HoldIntent

        runner = _make_runner()
        strategy = _make_strategy()

        # First: fail
        await runner.run_loop(strategy, max_iterations=1)
        assert runner._first_error_at is not None

        # Then: succeed
        strategy.decide.side_effect = None
        strategy.decide.return_value = HoldIntent(reason="ok")
        await runner.run_loop(strategy, max_iterations=1)
        assert runner._first_error_at is None


# =============================================================================
# Tests: _alert_consecutive_errors with OperatorCardGenerator
# =============================================================================


class TestAlertWithOperatorCardGenerator:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_uses_generator_when_available(self, _mock_pause, _mock_teardown):
        """OperatorCardGenerator.generate_card() should be called instead of hardcoded card."""
        alert_mgr = MagicMock()
        alert_mgr.send_alert = AsyncMock()
        gen = MagicMock(spec=OperatorCardGenerator)
        gen.generate_card.return_value = MagicMock()

        runner = _make_runner(
            alert_manager=alert_mgr,
            operator_card_generator=gen,
            max_consecutive_errors=2,
        )
        strategy = _make_strategy()

        # Run enough iterations to trigger consecutive error alert
        await runner.run_loop(strategy, max_iterations=2)

        gen.generate_card.assert_called()
        alert_mgr.send_alert.assert_called()

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_fallback_to_basic_card_without_generator(self, _mock_pause, _mock_teardown):
        """Without OperatorCardGenerator, basic card with StuckReason.UNKNOWN should be sent."""
        alert_mgr = MagicMock()
        alert_mgr.send_alert = AsyncMock()

        runner = _make_runner(
            alert_manager=alert_mgr,
            max_consecutive_errors=2,
        )
        strategy = _make_strategy()

        await runner.run_loop(strategy, max_iterations=2)

        alert_mgr.send_alert.assert_called()
        card = alert_mgr.send_alert.call_args[0][0]
        assert card.reason == StuckReason.UNKNOWN
        assert card.severity == Severity.MEDIUM

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_generator_receives_error_context(self, _mock_pause, _mock_teardown):
        """OperatorCardGenerator should receive ErrorContext with error details."""
        alert_mgr = MagicMock()
        alert_mgr.send_alert = AsyncMock()
        gen = MagicMock(spec=OperatorCardGenerator)
        gen.generate_card.return_value = MagicMock()

        runner = _make_runner(
            alert_manager=alert_mgr,
            operator_card_generator=gen,
            max_consecutive_errors=2,
        )
        strategy = _make_strategy(decide_side_effect=RuntimeError("slippage exceeded"))

        await runner.run_loop(strategy, max_iterations=2)

        call_kwargs = gen.generate_card.call_args[1]
        assert "slippage exceeded" in call_kwargs["error_context"].error_message

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_generator_receives_stuck_since(self, _mock_pause, _mock_teardown):
        """StrategyState.stuck_since should be set from _first_error_at."""
        alert_mgr = MagicMock()
        alert_mgr.send_alert = AsyncMock()
        gen = MagicMock(spec=OperatorCardGenerator)
        gen.generate_card.return_value = MagicMock()

        runner = _make_runner(
            alert_manager=alert_mgr,
            operator_card_generator=gen,
            max_consecutive_errors=2,
        )
        strategy = _make_strategy()

        await runner.run_loop(strategy, max_iterations=2)

        call_kwargs = gen.generate_card.call_args[1]
        assert call_kwargs["strategy_state"].stuck_since is not None


# =============================================================================
# Tests: _alert_consecutive_errors with StuckDetector
# =============================================================================


class TestAlertWithStuckDetector:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_stuck_detector_called_when_available(self, _mock_pause, _mock_teardown):
        """StuckDetector.detect_stuck() should be called for classification."""
        alert_mgr = MagicMock()
        alert_mgr.send_alert = AsyncMock()

        detector = MagicMock(spec=StuckDetector)
        detector.detect_stuck.return_value = StuckDetectionResult(
            is_stuck=True,
            reason=StuckReason.RPC_FAILURE,
            time_in_state_seconds=120,
        )

        gen = MagicMock(spec=OperatorCardGenerator)
        gen.generate_card.return_value = MagicMock()

        runner = _make_runner(
            alert_manager=alert_mgr,
            stuck_detector=detector,
            operator_card_generator=gen,
            max_consecutive_errors=2,
        )
        strategy = _make_strategy()

        await runner.run_loop(strategy, max_iterations=2)

        detector.detect_stuck.assert_called()
        # Event type should be STUCK when detector finds stuck reason
        call_kwargs = gen.generate_card.call_args[1]
        assert call_kwargs["event_type"] == EventType.STUCK

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_detector_snapshot_includes_chain(self, _mock_pause, _mock_teardown):
        """StrategySnapshot passed to detector should include strategy chain."""
        alert_mgr = MagicMock()
        alert_mgr.send_alert = AsyncMock()

        detector = MagicMock(spec=StuckDetector)
        detector.detect_stuck.return_value = StuckDetectionResult(is_stuck=False)

        gen = MagicMock(spec=OperatorCardGenerator)
        gen.generate_card.return_value = MagicMock()

        runner = _make_runner(
            alert_manager=alert_mgr,
            stuck_detector=detector,
            operator_card_generator=gen,
            max_consecutive_errors=2,
        )
        strategy = _make_strategy()

        await runner.run_loop(strategy, max_iterations=2)

        snapshot = detector.detect_stuck.call_args[0][0]
        assert snapshot.chain == "arbitrum"
        assert snapshot.strategy_id == "test_strategy"

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_warning_event_when_detector_not_stuck(self, _mock_pause, _mock_teardown):
        """When detector says not stuck, event_type should be WARNING not STUCK."""
        alert_mgr = MagicMock()
        alert_mgr.send_alert = AsyncMock()

        detector = MagicMock(spec=StuckDetector)
        detector.detect_stuck.return_value = StuckDetectionResult(is_stuck=False)

        gen = MagicMock(spec=OperatorCardGenerator)
        gen.generate_card.return_value = MagicMock()

        runner = _make_runner(
            alert_manager=alert_mgr,
            stuck_detector=detector,
            operator_card_generator=gen,
            max_consecutive_errors=2,
        )
        strategy = _make_strategy()

        await runner.run_loop(strategy, max_iterations=2)

        call_kwargs = gen.generate_card.call_args[1]
        assert call_kwargs["event_type"] == EventType.WARNING

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_circuit_breaker_state_passed_to_detector(self, _mock_pause, _mock_teardown):
        """Snapshot should reflect circuit breaker triggered state when breaker trips during run."""
        from almanak.framework.execution.circuit_breaker import CircuitBreaker, CircuitBreakerConfig

        alert_mgr = MagicMock()
        alert_mgr.send_alert = AsyncMock()

        # Breaker with low threshold so it trips after 2 failures
        breaker = CircuitBreaker(
            strategy_id="test_strategy",
            config=CircuitBreakerConfig(
                max_consecutive_failures=2,
                max_cumulative_loss_usd=Decimal("1000"),
                cooldown_seconds=2,
            ),
        )

        detector = MagicMock(spec=StuckDetector)
        detector.detect_stuck.return_value = StuckDetectionResult(
            is_stuck=True, reason=StuckReason.CIRCUIT_BREAKER
        )

        gen = MagicMock(spec=OperatorCardGenerator)
        gen.generate_card.return_value = MagicMock()

        config = RunnerConfig(
            default_interval_seconds=1,
            enable_state_persistence=False,
            enable_alerting=True,
            dry_run=False,
            max_consecutive_errors=2,
        )
        runner = StrategyRunner(
            price_oracle=MagicMock(),
            balance_provider=MagicMock(),
            execution_orchestrator=MagicMock(),
            state_manager=MagicMock(),
            alert_manager=alert_mgr,
            config=config,
            circuit_breaker=breaker,
            stuck_detector=detector,
            operator_card_generator=gen,
        )
        strategy = _make_strategy()

        # Run 2 iterations - both fail, breaker trips, alert fires
        await runner.run_loop(strategy, max_iterations=2)

        # Breaker should be open after 2 failures
        assert breaker.state.value == "open"
        snapshot = detector.detect_stuck.call_args[0][0]
        assert snapshot.circuit_breaker_triggered is True


# =============================================================================
# Tests: _handle_execution_error with OperatorCardGenerator
# =============================================================================


class TestExecutionErrorWithGenerator:
    @pytest.mark.asyncio
    async def test_uses_generator_for_execution_errors(self):
        """_handle_execution_error should use OperatorCardGenerator when available."""
        alert_mgr = MagicMock()
        alert_mgr.send_alert = AsyncMock()
        gen = MagicMock(spec=OperatorCardGenerator)
        gen.generate_card.return_value = MagicMock()

        runner = _make_runner(
            alert_manager=alert_mgr,
            operator_card_generator=gen,
        )
        # Enable alerting manually since _make_runner sets it based on alert_manager
        runner.config = RunnerConfig(enable_alerting=True)

        mock_result = MagicMock()
        mock_result.error = "Transaction reverted"
        mock_result.total_gas_used = 21000
        mock_result.phase = None

        strategy = MagicMock()
        strategy.strategy_id = "test_strategy"

        await runner._handle_execution_error(strategy, mock_result)

        gen.generate_card.assert_called_once()
        call_kwargs = gen.generate_card.call_args[1]
        assert call_kwargs["event_type"] == EventType.ERROR
        assert call_kwargs["error_context"].error_message == "Transaction reverted"

    @pytest.mark.asyncio
    async def test_fallback_for_execution_errors_without_generator(self):
        """Without generator, basic card with TRANSACTION_REVERTED should be sent."""
        alert_mgr = MagicMock()
        alert_mgr.send_alert = AsyncMock()

        runner = _make_runner(alert_manager=alert_mgr)
        runner.config = RunnerConfig(enable_alerting=True)

        mock_result = MagicMock()
        mock_result.error = "Transaction reverted"
        mock_result.total_gas_used = 21000
        mock_result.phase = None

        strategy = MagicMock()
        strategy.strategy_id = "test_strategy"

        await runner._handle_execution_error(strategy, mock_result)

        alert_mgr.send_alert.assert_called_once()
        card = alert_mgr.send_alert.call_args[0][0]
        assert card.reason == StuckReason.TRANSACTION_REVERTED


# =============================================================================
# Tests: No alerting when disabled
# =============================================================================


class TestNoAlertingWhenDisabled:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_no_alert_without_alert_manager(self, _mock_pause, _mock_teardown):
        """No alert should be sent if alert_manager is None."""
        gen = MagicMock(spec=OperatorCardGenerator)
        detector = MagicMock(spec=StuckDetector)

        runner = _make_runner(
            stuck_detector=detector,
            operator_card_generator=gen,
            max_consecutive_errors=2,
        )
        strategy = _make_strategy()

        await runner.run_loop(strategy, max_iterations=2)

        gen.generate_card.assert_not_called()
        detector.detect_stuck.assert_not_called()
