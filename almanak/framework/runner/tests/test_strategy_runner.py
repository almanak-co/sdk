"""Tests for StrategyRunner.

This module tests the StrategyRunner class including:
- Single iteration execution (run_iteration)
- Continuous loop execution (run_loop)
- Graceful shutdown handling
- Error handling and alerting
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.interfaces import BalanceResult, PriceResult
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.intents.vocabulary import HoldIntent, Intent, SwapIntent
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.state.state_manager import StateData

# =============================================================================
# Mock Classes
# =============================================================================


@dataclass
class MockMarketSnapshot:
    """Mock market snapshot for testing."""

    chain: str = "arbitrum"
    wallet_address: str = "0x1234567890123456789012345678901234567890"
    eth_price: Decimal = Decimal("2000")
    usdc_balance: Decimal = Decimal("10000")


class MockStrategy:
    """Mock strategy for testing."""

    def __init__(
        self,
        strategy_id: str = "test_strategy",
        chain: str = "arbitrum",
        wallet_address: str = "0x1234567890123456789012345678901234567890",
        decide_returns: Any | None = None,
        decide_raises: Exception | None = None,
    ) -> None:
        self._strategy_id = strategy_id
        self._chain = chain
        self._wallet_address = wallet_address
        self._decide_returns = decide_returns
        self._decide_raises = decide_raises
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
        if self._decide_raises:
            raise self._decide_raises
        return self._decide_returns

    def create_market_snapshot(self) -> MockMarketSnapshot:
        return MockMarketSnapshot(
            chain=self._chain,
            wallet_address=self._wallet_address,
        )


class MockPriceOracle:
    """Mock price oracle for testing."""

    async def get_aggregated_price(self, token: str, quote: str = "USD") -> PriceResult:
        prices = {
            "ETH": Decimal("2000"),
            "WETH": Decimal("2000"),
            "USDC": Decimal("1"),
        }
        return PriceResult(
            price=prices.get(token, Decimal("1")),
            source="mock",
            timestamp=datetime.now(UTC),
            confidence=1.0,
        )

    def get_source_health(self, source_name: str) -> dict[str, Any] | None:
        return {"status": "healthy"}


class MockBalanceProvider:
    """Mock balance provider for testing."""

    def __init__(self) -> None:
        self.invalidate_called = False

    async def get_balance(self, token: str) -> BalanceResult:
        balances = {
            "ETH": Decimal("10"),
            "WETH": Decimal("10"),
            "USDC": Decimal("10000"),
        }
        return BalanceResult(
            balance=balances.get(token, Decimal("0")),
            token=token,
            address="0x" + "0" * 40,
            decimals=18 if token in ("ETH", "WETH") else 6,
            raw_balance=int(balances.get(token, Decimal("0")) * 10**18),
        )

    async def get_native_balance(self) -> BalanceResult:
        return await self.get_balance("ETH")

    def invalidate_cache(self, token: str | None = None) -> None:
        self.invalidate_called = True


class MockExecutionOrchestrator:
    """Mock execution orchestrator for testing."""

    def __init__(
        self,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        self._success = success
        self._error = error
        self.execute_called = False
        self.last_bundle: ActionBundle | None = None

    async def execute(
        self,
        action_bundle: ActionBundle,
        context: Any | None = None,
    ) -> ExecutionResult:
        self.execute_called = True
        self.last_bundle = action_bundle

        # Create a mock ExecutionResult-like object
        result = MagicMock(spec=ExecutionResult)
        result.success = self._success
        result.error = self._error
        result.phase = ExecutionPhase.COMPLETE if self._success else ExecutionPhase.VALIDATION
        result.transaction_results = []
        result.total_gas_used = 100000
        result.total_gas_cost_wei = 1000000000000
        result.to_dict = MagicMock(
            return_value={
                "success": self._success,
                "error": self._error,
                "phase": result.phase.value,
                "transaction_results": [],
                "total_gas_used": 100000,
            }
        )

        return result


class MockStateManager:
    """Mock state manager for testing."""

    def __init__(self) -> None:
        self._states: dict[str, StateData] = {}
        self.initialized = False
        self.closed = False

    async def initialize(self) -> None:
        self.initialized = True

    async def close(self) -> None:
        self.closed = True

    async def load_state(self, strategy_id: str) -> StateData:
        if strategy_id not in self._states:
            self._states[strategy_id] = StateData(
                strategy_id=strategy_id,
                version=1,
                state={},
            )
        return self._states[strategy_id]

    async def save_state(
        self,
        state: StateData,
        expected_version: int | None = None,
    ) -> StateData:
        state.version += 1
        self._states[state.strategy_id] = state
        return state

    async def delete_state(self, strategy_id: str) -> bool:
        if strategy_id in self._states:
            del self._states[strategy_id]
            return True
        return False


class MockAlertManager:
    """Mock alert manager for testing."""

    def __init__(self) -> None:
        self.alerts_sent: list[Any] = []

    async def send_alert(self, card: Any) -> Any:
        self.alerts_sent.append(card)
        result = MagicMock()
        result.success = True
        return result


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def price_oracle() -> MockPriceOracle:
    return MockPriceOracle()


@pytest.fixture
def balance_provider() -> MockBalanceProvider:
    return MockBalanceProvider()


@pytest.fixture
def execution_orchestrator() -> MockExecutionOrchestrator:
    return MockExecutionOrchestrator()


@pytest.fixture
def state_manager() -> MockStateManager:
    return MockStateManager()


@pytest.fixture
def alert_manager() -> MockAlertManager:
    return MockAlertManager()


@pytest.fixture
def runner(
    price_oracle: MockPriceOracle,
    balance_provider: MockBalanceProvider,
    execution_orchestrator: MockExecutionOrchestrator,
    state_manager: MockStateManager,
    alert_manager: MockAlertManager,
) -> StrategyRunner:
    return StrategyRunner(
        price_oracle=price_oracle,
        balance_provider=balance_provider,
        execution_orchestrator=execution_orchestrator,
        state_manager=state_manager,
        alert_manager=alert_manager,
    )


@pytest.fixture
def dry_run_runner(
    price_oracle: MockPriceOracle,
    balance_provider: MockBalanceProvider,
    execution_orchestrator: MockExecutionOrchestrator,
    state_manager: MockStateManager,
) -> StrategyRunner:
    return StrategyRunner(
        price_oracle=price_oracle,
        balance_provider=balance_provider,
        execution_orchestrator=execution_orchestrator,
        state_manager=state_manager,
        config=RunnerConfig(dry_run=True),
    )


# =============================================================================
# Tests: run_iteration
# =============================================================================


class TestRunIteration:
    """Tests for the run_iteration method."""

    @pytest.mark.asyncio
    async def test_hold_intent_returns_hold_status(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Test that HOLD intent results in HOLD status."""
        strategy = MockStrategy(decide_returns=Intent.hold(reason="RSI neutral"))

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.HOLD
        assert result.success is True
        assert isinstance(result.intent, HoldIntent)
        assert result.intent.reason == "RSI neutral"

    @pytest.mark.asyncio
    async def test_none_intent_returns_hold_status(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Test that None intent (no action) results in HOLD status."""
        strategy = MockStrategy(decide_returns=None)

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.HOLD
        assert result.success is True
        assert result.intent is None

    @pytest.mark.asyncio
    async def test_swap_intent_executes_successfully(
        self,
        runner: StrategyRunner,
        execution_orchestrator: MockExecutionOrchestrator,
    ) -> None:
        """Test that SWAP intent compiles and executes."""
        strategy = MockStrategy(
            decide_returns=Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("1000"),
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.SUCCESS
        assert result.success is True
        assert isinstance(result.intent, SwapIntent)
        assert execution_orchestrator.execute_called is True

    @pytest.mark.asyncio
    async def test_strategy_error_returns_error_status(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Test that strategy exceptions result in STRATEGY_ERROR status."""
        strategy = MockStrategy(decide_raises=ValueError("Strategy failed"))

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert result.success is False
        assert "Strategy decision failed" in result.error

    @pytest.mark.asyncio
    async def test_execution_failure_returns_execution_failed(
        self,
        price_oracle: MockPriceOracle,
        balance_provider: MockBalanceProvider,
        state_manager: MockStateManager,
        alert_manager: MockAlertManager,
    ) -> None:
        """Test that execution failure returns EXECUTION_FAILED status."""
        orchestrator = MockExecutionOrchestrator(
            success=False,
            error="Transaction reverted",
        )
        runner = StrategyRunner(
            price_oracle=price_oracle,
            balance_provider=balance_provider,
            execution_orchestrator=orchestrator,
            state_manager=state_manager,
            alert_manager=alert_manager,
        )
        strategy = MockStrategy(
            decide_returns=Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("1000"),
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.EXECUTION_FAILED
        assert result.success is False
        assert "Transaction reverted" in result.error

    @pytest.mark.asyncio
    async def test_dry_run_skips_execution(
        self,
        dry_run_runner: StrategyRunner,
        execution_orchestrator: MockExecutionOrchestrator,
    ) -> None:
        """Test that dry run mode skips actual execution."""
        strategy = MockStrategy(
            decide_returns=Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("1000"),
            )
        )

        result = await dry_run_runner.run_iteration(strategy)

        assert result.status == IterationStatus.DRY_RUN
        assert result.success is True
        # Orchestrator should NOT be called in dry run mode
        assert execution_orchestrator.execute_called is False

    @pytest.mark.asyncio
    async def test_dry_run_intent_sequence_amount_all_does_not_fail(
        self,
        dry_run_runner: StrategyRunner,
        execution_orchestrator: MockExecutionOrchestrator,
    ) -> None:
        """Test that dry-run mode doesn't fail on IntentSequence with amount='all'.

        In dry-run mode, no execution happens so previous step output is unavailable.
        The runner should gracefully skip amount='all' resolution instead of failing
        with 'no previous step amount available'.
        """
        from almanak.framework.intents.vocabulary import IntentSequence, SupplyIntent

        sequence = IntentSequence(
            intents=[
                Intent.swap(
                    from_token="USDC",
                    to_token="WETH",
                    amount=Decimal("1000"),
                ),
                SupplyIntent(
                    protocol="aave_v3",
                    token="WETH",
                    amount="all",
                ),
            ]
        )
        strategy = MockStrategy(decide_returns=sequence)

        result = await dry_run_runner.run_iteration(strategy)

        # Should succeed in dry-run (not fail with COMPILATION_FAILED)
        assert result.success is True
        assert result.status == IterationStatus.DRY_RUN
        # Orchestrator should NOT be called in dry run mode
        assert execution_orchestrator.execute_called is False

    @pytest.mark.asyncio
    async def test_non_dry_run_intent_sequence_amount_all_fails_without_previous(
        self,
        price_oracle: MockPriceOracle,
        balance_provider: MockBalanceProvider,
        state_manager: MockStateManager,
    ) -> None:
        """Test that normal mode still fails on amount='all' without previous output.

        This regression test ensures the fix for dry-run doesn't accidentally
        relax validation in normal execution mode.
        """
        from almanak.framework.intents.vocabulary import IntentSequence, SupplyIntent

        # Create orchestrator that returns success but with no swap_amounts
        orch = MockExecutionOrchestrator(success=True)

        # Patch execute to return a result without swap_amounts
        async def execute_no_swap_amounts(action_bundle, context=None):
            orch.execute_called = True
            result = MagicMock(spec=ExecutionResult)
            result.success = True
            result.error = None
            result.phase = ExecutionPhase.COMPLETE
            result.transaction_results = []
            result.total_gas_used = 100000
            result.total_gas_cost_wei = 1000000000000
            result.swap_amounts = None  # Explicitly no swap amounts
            result.to_dict = MagicMock(return_value={"success": True})
            return result

        orch.execute = execute_no_swap_amounts

        runner_no_swap = StrategyRunner(
            price_oracle=price_oracle,
            balance_provider=balance_provider,
            execution_orchestrator=orch,
            state_manager=state_manager,
        )

        sequence = IntentSequence(
            intents=[
                Intent.swap(
                    from_token="USDC",
                    to_token="WETH",
                    amount=Decimal("1000"),
                ),
                SupplyIntent(
                    protocol="aave_v3",
                    token="WETH",
                    amount="all",
                ),
            ]
        )
        strategy = MockStrategy(decide_returns=sequence)

        result = await runner_no_swap.run_iteration(strategy)

        # The first intent succeeds but no swap_amounts available,
        # so the second intent with amount='all' should fail
        assert result.status == IterationStatus.COMPILATION_FAILED
        assert "amount='all'" in result.error

    @pytest.mark.asyncio
    async def test_balance_cache_invalidated_after_execution(
        self,
        runner: StrategyRunner,
        balance_provider: MockBalanceProvider,
    ) -> None:
        """Test that balance cache is invalidated after successful execution."""
        strategy = MockStrategy(
            decide_returns=Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("1000"),
            )
        )

        await runner.run_iteration(strategy)

        assert balance_provider.invalidate_called is True

    @pytest.mark.asyncio
    async def test_metrics_updated_on_success(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Test that metrics are updated after successful iteration."""
        strategy = MockStrategy(decide_returns=Intent.hold())

        await runner.run_iteration(strategy)

        metrics = runner.get_metrics()
        assert metrics["total_iterations"] == 1
        assert metrics["successful_iterations"] == 1
        assert metrics["consecutive_errors"] == 0

    @pytest.mark.asyncio
    async def test_consecutive_errors_tracked(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Test that consecutive errors are tracked."""
        strategy = MockStrategy(decide_raises=ValueError("Error"))

        # Run multiple failing iterations
        await runner.run_iteration(strategy)
        await runner.run_iteration(strategy)
        await runner.run_iteration(strategy)

        metrics = runner.get_metrics()
        assert metrics["consecutive_errors"] == 3

    @pytest.mark.asyncio
    async def test_consecutive_errors_reset_on_success(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Test that consecutive errors reset after success."""
        failing_strategy = MockStrategy(decide_raises=ValueError("Error"))
        successful_strategy = MockStrategy(decide_returns=Intent.hold())

        await runner.run_iteration(failing_strategy)
        await runner.run_iteration(failing_strategy)
        assert runner.get_metrics()["consecutive_errors"] == 2

        await runner.run_iteration(successful_strategy)
        assert runner.get_metrics()["consecutive_errors"] == 0


# =============================================================================
# Tests: run_loop
# =============================================================================


class TestRunLoop:
    """Tests for the run_loop method."""

    @pytest.mark.asyncio
    async def test_loop_respects_shutdown_request(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Test that run_loop exits when shutdown is requested."""
        strategy = MockStrategy(decide_returns=Intent.hold())

        # Request shutdown after short delay
        async def request_shutdown() -> None:
            await asyncio.sleep(0.05)
            runner.request_shutdown()

        loop_task = asyncio.create_task(runner.run_loop(strategy, interval_seconds=0.01))
        shutdown_task = asyncio.create_task(request_shutdown())

        # Wait for both tasks
        await asyncio.gather(loop_task, shutdown_task)

        # Loop should have exited
        assert runner._shutdown_requested is True

    @pytest.mark.asyncio
    async def test_loop_initializes_state_manager(
        self,
        runner: StrategyRunner,
        state_manager: MockStateManager,
    ) -> None:
        """Test that run_loop initializes state manager."""
        strategy = MockStrategy(decide_returns=Intent.hold())

        # Use a callback to request shutdown after first iteration
        def callback(result: IterationResult) -> None:
            runner.request_shutdown()

        await runner.run_loop(
            strategy,
            interval_seconds=0.01,
            iteration_callback=callback,
        )

        assert state_manager.initialized is True
        assert state_manager.closed is True

    @pytest.mark.asyncio
    async def test_loop_calls_iteration_callback(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Test that run_loop calls iteration callback."""
        strategy = MockStrategy(decide_returns=Intent.hold())
        callback_results: list[IterationResult] = []

        def callback(result: IterationResult) -> None:
            callback_results.append(result)
            runner.request_shutdown()  # Stop after first iteration

        await runner.run_loop(
            strategy,
            interval_seconds=0.01,
            iteration_callback=callback,
        )

        assert len(callback_results) == 1
        assert callback_results[0].status == IterationStatus.HOLD


# =============================================================================
# Tests: Graceful Shutdown
# =============================================================================


class TestGracefulShutdown:
    """Tests for graceful shutdown handling."""

    def test_request_shutdown_sets_flag(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Test that request_shutdown sets the flag."""
        assert runner._shutdown_requested is False

        runner.request_shutdown()

        assert runner._shutdown_requested is True

    def test_get_metrics_reflects_shutdown_state(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Test that get_metrics includes shutdown state."""
        assert runner.get_metrics()["shutdown_requested"] is False

        runner.request_shutdown()

        assert runner.get_metrics()["shutdown_requested"] is True


# =============================================================================
# Tests: Alerting
# =============================================================================


class TestAlerting:
    """Tests for alerting functionality."""

    @pytest.mark.asyncio
    async def test_execution_error_triggers_alert(
        self,
        price_oracle: MockPriceOracle,
        balance_provider: MockBalanceProvider,
        state_manager: MockStateManager,
        alert_manager: MockAlertManager,
    ) -> None:
        """Test that execution failures trigger alerts."""
        orchestrator = MockExecutionOrchestrator(
            success=False,
            error="Transaction reverted",
        )
        runner = StrategyRunner(
            price_oracle=price_oracle,
            balance_provider=balance_provider,
            execution_orchestrator=orchestrator,
            state_manager=state_manager,
            alert_manager=alert_manager,
            config=RunnerConfig(enable_alerting=True),
        )
        strategy = MockStrategy(
            decide_returns=Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("1000"),
            )
        )

        await runner.run_iteration(strategy)

        assert len(alert_manager.alerts_sent) == 1

    @pytest.mark.asyncio
    async def test_consecutive_errors_trigger_alert(
        self,
        price_oracle: MockPriceOracle,
        balance_provider: MockBalanceProvider,
        execution_orchestrator: MockExecutionOrchestrator,
        state_manager: MockStateManager,
        alert_manager: MockAlertManager,
    ) -> None:
        """Test that consecutive errors trigger alert at threshold."""
        runner = StrategyRunner(
            price_oracle=price_oracle,
            balance_provider=balance_provider,
            execution_orchestrator=execution_orchestrator,
            state_manager=state_manager,
            alert_manager=alert_manager,
            config=RunnerConfig(
                max_consecutive_errors=3,
                enable_alerting=True,
            ),
        )
        strategy = MockStrategy(decide_raises=ValueError("Error"))

        # Use iteration counter to shutdown after hitting threshold
        iteration_count = 0

        def callback(result: IterationResult) -> None:
            nonlocal iteration_count
            iteration_count += 1
            if iteration_count >= 3:
                runner.request_shutdown()

        await runner.run_loop(
            strategy,
            interval_seconds=0.001,
            iteration_callback=callback,
        )

        # Should have alert for consecutive errors
        assert len(alert_manager.alerts_sent) > 0

    @pytest.mark.asyncio
    async def test_alerting_disabled_no_alerts(
        self,
        price_oracle: MockPriceOracle,
        balance_provider: MockBalanceProvider,
        state_manager: MockStateManager,
        alert_manager: MockAlertManager,
    ) -> None:
        """Test that disabled alerting prevents alerts."""
        orchestrator = MockExecutionOrchestrator(
            success=False,
            error="Transaction reverted",
        )
        runner = StrategyRunner(
            price_oracle=price_oracle,
            balance_provider=balance_provider,
            execution_orchestrator=orchestrator,
            state_manager=state_manager,
            alert_manager=alert_manager,
            config=RunnerConfig(enable_alerting=False),
        )
        strategy = MockStrategy(
            decide_returns=Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("1000"),
            )
        )

        await runner.run_iteration(strategy)

        assert len(alert_manager.alerts_sent) == 0


# =============================================================================
# Tests: Configuration
# =============================================================================


class TestRunnerConfig:
    """Tests for RunnerConfig."""

    def test_default_config_values(self) -> None:
        """Test default configuration values."""
        config = RunnerConfig()

        assert config.default_interval_seconds == 60
        assert config.max_consecutive_errors == 3
        assert config.enable_state_persistence is True
        assert config.enable_alerting is True
        assert config.dry_run is False

    def test_custom_config_values(self) -> None:
        """Test custom configuration values."""
        config = RunnerConfig(
            default_interval_seconds=30,
            max_consecutive_errors=5,
            enable_state_persistence=False,
            enable_alerting=False,
            dry_run=True,
        )

        assert config.default_interval_seconds == 30
        assert config.max_consecutive_errors == 5
        assert config.enable_state_persistence is False
        assert config.enable_alerting is False
        assert config.dry_run is True
