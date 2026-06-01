"""Wiring tests: a market-data outage must not kill the strategy.

Covers the three fixes that, together, stop a transient / quiet-pool DEX data
gap from permanently killing a managed agent (the NVDAON/USD incident):

1. ``handle_iteration_failure`` records a ``DATA_ERROR`` result on the breaker
   as *data-class* (elevated threshold), not the UNKNOWN/action-class default.
2. ``_maybe_trigger_emergency`` does NOT exit the process on a trip driven
   solely by data-class failures — the deployment idles and auto-recovers.
3. ``_step_extract_intents`` does not escalate a HOLD to ``DATA_ERROR`` when the
   pool is quiet-but-priceable (``is_quiet_pool_hold()``).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
)
from almanak.framework.intents.vocabulary import HoldIntent
from almanak.framework.runner._run_loop_helpers import handle_iteration_failure
from almanak.framework.runner.failure_kind import FailureKind
from almanak.framework.runner.runner_models import IterationResult, IterationStatus
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner

_PAUSE_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._is_strategy_paused"
_TEARDOWN_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._check_teardown_requested"


def _make_strategy(decide_return=None):
    strategy = MagicMock()
    strategy.deployment_id = "test_strategy"
    strategy.chain = "ethereum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    snap = strategy.create_market_snapshot.return_value
    snap.has_critical_data_failures.return_value = False
    strategy.decide.return_value = decide_return if decide_return is not None else HoldIntent(reason="hold")
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    return strategy


def _make_runner(circuit_breaker=None, emergency_manager=None):
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
        circuit_breaker=circuit_breaker,
        emergency_manager=emergency_manager,
    )


def _make_breaker(max_failures=3, data_max=30):
    config = CircuitBreakerConfig(
        max_consecutive_failures=max_failures,
        data_class_max_consecutive_failures=data_max,
        max_cumulative_loss_usd=Decimal("1000"),
        cooldown_seconds=3600,
    )
    return CircuitBreaker(deployment_id="test_strategy", config=config)


# ---------------------------------------------------------------------------
# Fix 1: DATA_ERROR is recorded on the breaker as data-class
# ---------------------------------------------------------------------------


class TestDataErrorRecordedAsDataClass:
    @pytest.mark.asyncio
    async def test_data_error_increments_data_counter(self) -> None:
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        result = IterationResult(status=IterationStatus.DATA_ERROR, error="stale OHLCV", deployment_id="test_strategy")

        await handle_iteration_failure(runner, strategy, "test_strategy", result)

        status = breaker.get_status()
        assert status["consecutive_data_failures"] == 1
        assert status["consecutive_action_failures"] == 0

    @pytest.mark.asyncio
    async def test_non_data_status_increments_action_counter(self) -> None:
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        result = IterationResult(
            status=IterationStatus.ACCOUNTING_FAILED, error="ledger write failed", deployment_id="test_strategy"
        )

        await handle_iteration_failure(runner, strategy, "test_strategy", result)

        status = breaker.get_status()
        assert status["consecutive_action_failures"] == 1
        assert status["consecutive_data_failures"] == 0

    @pytest.mark.asyncio
    async def test_three_data_errors_with_open_exposure_do_not_trip(self) -> None:
        # The NVDAON regression: 3 consecutive DATA_ERRORs while holding a
        # position must NOT trip the breaker (data-class threshold = 30).
        breaker = _make_breaker()
        breaker.record_exposure(True)
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()

        for _ in range(3):
            result = IterationResult(
                status=IterationStatus.DATA_ERROR, error="stale OHLCV", deployment_id="test_strategy"
            )
            await handle_iteration_failure(runner, strategy, "test_strategy", result)

        assert breaker.state == CircuitBreakerState.CLOSED
        assert breaker.get_status()["consecutive_data_failures"] == 3


# ---------------------------------------------------------------------------
# Fix 2: a data-only trip does not exit a managed-deployment process
# ---------------------------------------------------------------------------


class TestNoProcessExitOnDataOnlyTrip:
    @pytest.mark.asyncio
    async def test_data_only_trip_keeps_process_alive(self) -> None:
        breaker = _make_breaker()
        breaker.record_exposure(False)  # no position → data threshold collapses to 3
        em = MagicMock()
        em.emergency_stop_async = AsyncMock()
        runner = _make_runner(circuit_breaker=breaker, emergency_manager=em)
        strategy = _make_strategy()

        for _ in range(3):
            breaker.record_failure("stale OHLCV", kind=FailureKind.DATA_UNAVAILABLE)
        assert breaker.state == CircuitBreakerState.OPEN
        assert breaker.tripped_on_data_class_only is True

        last = IterationResult(status=IterationStatus.DATA_ERROR, error="stale OHLCV", deployment_id="test_strategy")
        with (
            patch.object(runner, "_is_managed_deployment", return_value=True),
            patch.object(runner, "request_shutdown") as mock_shutdown,
            patch.object(runner, "_lifecycle_write_state") as mock_lifecycle,
        ):
            await runner._maybe_trigger_emergency(strategy, last)

        em.emergency_stop_async.assert_awaited_once()
        mock_shutdown.assert_not_called()  # process stays alive to auto-recover
        mock_lifecycle.assert_not_called()  # no terminal ERROR write

    @pytest.mark.asyncio
    async def test_action_trip_exits_managed_process(self) -> None:
        breaker = _make_breaker()
        em = MagicMock()
        em.emergency_stop_async = AsyncMock()
        runner = _make_runner(circuit_breaker=breaker, emergency_manager=em)
        strategy = _make_strategy()

        for _ in range(3):
            breaker.record_failure("tx reverted", kind=FailureKind.EXECUTION_REVERTED)
        assert breaker.state == CircuitBreakerState.OPEN
        assert breaker.tripped_on_data_class_only is False

        last = IterationResult(
            status=IterationStatus.STRATEGY_ERROR, error="tx reverted", deployment_id="test_strategy"
        )
        with (
            patch.object(runner, "_is_managed_deployment", return_value=True),
            patch.object(runner, "request_shutdown") as mock_shutdown,
            patch.object(runner, "_lifecycle_write_state"),
        ):
            await runner._maybe_trigger_emergency(strategy, last)

        mock_shutdown.assert_called_once()  # execution faults still exit


# ---------------------------------------------------------------------------
# Fix 3: a quiet-but-live pool HOLD is not escalated to DATA_ERROR
# ---------------------------------------------------------------------------


class TestQuietPoolHoldNotEscalated:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_quiet_pool_hold_stays_hold(self, _pause, _teardown) -> None:
        runner = _make_runner()
        strategy = _make_strategy(decide_return=HoldIntent(reason="cannot compute RSI"))
        snap = strategy.create_market_snapshot.return_value
        snap.has_critical_data_failures.return_value = True
        snap.is_quiet_pool_hold.return_value = True  # quiet but priceable

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.HOLD
        assert result.success

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_dead_pool_hold_escalates_to_data_error(self, _pause, _teardown) -> None:
        runner = _make_runner()
        strategy = _make_strategy(decide_return=HoldIntent(reason="cannot compute RSI"))
        snap = strategy.create_market_snapshot.return_value
        snap.has_critical_data_failures.return_value = True
        snap.is_quiet_pool_hold.return_value = False  # not priceable → genuinely dark
        snap.classify_critical_data_failures.return_value = "permanent"
        snap.summarize_critical_data_failures.return_value = "rsi(...): all providers failed"

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.DATA_ERROR
        assert not result.success


# ---------------------------------------------------------------------------
# Error-streak alignment: a tolerated data outage must not flip the deployment
# to ERROR / alert at the generic max_consecutive_errors threshold
# ---------------------------------------------------------------------------


def _error_writes(mock_lifecycle) -> list:
    return [c for c in mock_lifecycle.call_args_list if "ERROR" in c.args]


class TestDataOutageErrorStreak:
    @pytest.mark.asyncio
    async def test_tolerated_data_outage_does_not_mark_error(self) -> None:
        # Transient DATA_ERROR, breaker tolerating (unknown exposure ⇒ 30): the
        # generic error streak must NOT alert / write ERROR at max_consecutive_errors.
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()

        with (
            patch.object(runner, "_alert_consecutive_errors", new_callable=AsyncMock) as mock_alert,
            patch.object(runner, "_lifecycle_write_state") as mock_lifecycle,
        ):
            for _ in range(5):  # well past max_consecutive_errors (3)
                result = IterationResult(
                    status=IterationStatus.DATA_ERROR,
                    error="Critical market-data failures while strategy returned HOLD "
                    "(classification=transient): rsi(...): stale",
                    deployment_id="test_strategy",
                )
                await handle_iteration_failure(runner, strategy, "test_strategy", result)

        assert breaker.state == CircuitBreakerState.CLOSED  # 5 < 30, still tolerating
        mock_alert.assert_not_called()
        assert _error_writes(mock_lifecycle) == []

    @pytest.mark.asyncio
    async def test_permanent_data_outage_still_marks_error(self) -> None:
        # Permanent DATA_ERROR (unknown token) is action-class ⇒ fast-fail: the
        # streak alert + ERROR lifecycle write fire as before.
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()

        with (
            patch.object(runner, "_alert_consecutive_errors", new_callable=AsyncMock) as mock_alert,
            patch.object(runner, "_lifecycle_write_state") as mock_lifecycle,
        ):
            for _ in range(3):
                result = IterationResult(
                    status=IterationStatus.DATA_ERROR,
                    error="Critical market-data failures while strategy returned HOLD "
                    "(classification=permanent): rsi(...): Unknown token for Binance: NVDAON",
                    deployment_id="test_strategy",
                )
                await handle_iteration_failure(runner, strategy, "test_strategy", result)

        assert breaker.state == CircuitBreakerState.OPEN  # action-class trips at 3
        mock_alert.assert_called()
        assert _error_writes(mock_lifecycle)
