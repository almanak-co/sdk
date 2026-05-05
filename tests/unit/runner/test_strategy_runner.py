"""Tests for StrategyRunner.

This module tests the StrategyRunner class including:
- Single iteration execution (run_iteration)
- Continuous loop execution (run_loop)
- Graceful shutdown handling
- Error handling and alerting
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

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
        # Avoid the auto-MagicMock attribute that trips the slippage circuit
        # breaker when the runner compares it against an int.
        result.swap_amounts = None
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


def _make_orchestrator_without_swap_amounts() -> "MockExecutionOrchestrator":
    """Build a MockExecutionOrchestrator whose result has swap_amounts=None.

    The default MockExecutionOrchestrator leaves ``swap_amounts`` as an
    auto-mocked attribute on MagicMock, which then trips the slippage
    circuit-breaker comparison when the runner multiplies a MagicMock by
    an int. Tests that care about post-execution branches (e.g. reconciliation
    enforcement) should prefer this helper so the slippage check is skipped.
    """
    orch = MockExecutionOrchestrator(success=True)

    async def _execute(action_bundle, context=None):
        orch.execute_called = True
        orch.last_bundle = action_bundle
        result = MagicMock(spec=ExecutionResult)
        result.success = True
        result.error = None
        result.phase = ExecutionPhase.COMPLETE
        result.transaction_results = []
        result.total_gas_used = 100000
        result.total_gas_cost_wei = 1000000000000
        result.swap_amounts = None
        result.to_dict = MagicMock(return_value={"success": True})
        return result

    orch.execute = _execute
    return orch


class MockStateManager:
    """Mock state manager for testing.

    Provides in-memory stubs for the accounting-persistence surface
    (VIB-3157): ``save_ledger_entry``, ``save_portfolio_snapshot``,
    ``save_portfolio_metrics``. Without these, live-mode writes now
    raise :class:`AccountingPersistenceError` by contract.
    """

    def __init__(self) -> None:
        self._states: dict[str, StateData] = {}
        self._ledger_entries: list[Any] = []
        self._snapshots: list[Any] = []
        self._metrics: dict[str, Any] = {}
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

    async def save_ledger_entry(self, entry: Any) -> None:
        self._ledger_entries.append(entry)

    async def save_portfolio_snapshot(self, snapshot: Any) -> int:
        self._snapshots.append(snapshot)
        return len(self._snapshots)

    async def save_portfolio_metrics(self, metrics: Any) -> bool:
        self._metrics[getattr(metrics, "strategy_id", "")] = metrics
        return True

    async def get_portfolio_metrics(self, strategy_id: str) -> Any:
        return self._metrics.get(strategy_id)

    def get_accounting_events_sync(
        self,
        deployment_id: str,
        position_key: str | None = None,
    ) -> list[dict]:
        return []

    async def save_outbox_entry(self, *args: Any, **kwargs: Any) -> None:
        return None


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

        # VIB-3158 fail-closed would flag the 1st intent's missing swap_amounts
        # as a reconciliation incident before the 2nd intent gets a chance to
        # fail compilation. This test is specifically about IntentSequence
        # `amount='all'` validation, so bypass reconciliation here.
        async def skip_reconcile(strategy, intent, execution_result, pre_snapshot=None):
            return None

        runner_no_swap._reconcile_post_execution_balances = skip_reconcile  # type: ignore[method-assign]

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
    async def test_reconciliation_incident_returns_failed_status(
        self,
        price_oracle: MockPriceOracle,
        balance_provider: MockBalanceProvider,
        state_manager: MockStateManager,
        alert_manager: MockAlertManager,
    ) -> None:
        """VIB-3158: reconciliation incident flips iteration from SUCCESS to RECONCILIATION_FAILED.

        When pre/post balance deltas fall outside the intent's expected range, the
        runner MUST NOT commit the iteration as clean. Instead it returns
        RECONCILIATION_FAILED so the downstream failure handler (circuit breaker,
        consecutive-errors alert) engages instead of the success path.

        Enforcement is now gated behind ``RunnerConfig.reconciliation_enforcement``
        (default False = observation mode while VIB-3348 block-anchored reads are in
        flight). This test exercises the enforcement path, so it opts in explicitly.
        """
        orch = _make_orchestrator_without_swap_amounts()

        runner = StrategyRunner(
            price_oracle=price_oracle,
            balance_provider=balance_provider,
            execution_orchestrator=orch,
            state_manager=state_manager,
            alert_manager=alert_manager,
            config=RunnerConfig(reconciliation_enforcement=True),
        )

        fake_recon = {
            "tokens_checked": ["USDC", "ETH"],
            "pre_balances": {"USDC": "10000", "ETH": "10"},
            "post_balances": {"USDC": "9000", "ETH": "10"},
            "actual_deltas": {"USDC": "-1000", "ETH": "0"},
            "expected_ranges": {
                "USDC": {"min": "-1010", "max": "-990"},
                "ETH": {"min": "0.49", "max": "0.51"},
            },
            "mismatches": [
                {"token": "ETH", "actual": "0", "expected_min": "0.49", "expected_max": "0.51"},
            ],
            "warnings": [],
            "incident": True,
            "enforced": True,
        }

        async def fake_reconcile(strategy, intent, execution_result, pre_snapshot=None):
            return fake_recon

        runner._reconcile_post_execution_balances = fake_reconcile  # type: ignore[method-assign]

        strategy = MockStrategy(
            decide_returns=Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("1000"),
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.RECONCILIATION_FAILED
        assert result.success is False
        assert result.balance_reconciliation is fake_recon
        assert result.error is not None
        assert "ETH" in result.error
        assert orch.execute_called is True

        # Enforcement must not increment successful iterations — the downstream
        # run_loop failure handler (record_failure on the circuit breaker +
        # consecutive-errors alerting) keys off result.success being False,
        # which we assert above. Here we just lock in that run_iteration itself
        # did NOT treat this as a success path.
        metrics = runner.get_metrics()
        assert metrics["successful_iterations"] == 0

    @pytest.mark.asyncio
    async def test_reconciliation_incident_observation_mode_does_not_halt(
        self,
        price_oracle: MockPriceOracle,
        balance_provider: MockBalanceProvider,
        state_manager: MockStateManager,
        alert_manager: MockAlertManager,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Default observation mode: incident is WARNING-logged and attached to the
        IterationResult but DOES NOT halt the iteration.

        Guards the VIB-3348 stable-release contract: while block-anchored balance
        reads are in flight, the dual-layer cache produces false-positive incidents
        on confirmed-on-chain swaps. Halting on those would kill strategies on a
        plumbing race, not a real accounting breach. Observation mode preserves
        full observability (log + ``balance_reconciliation`` on the result) without
        triggering the circuit-breaker path.
        """
        orch = _make_orchestrator_without_swap_amounts()

        # Observation mode = RunnerConfig default (reconciliation_enforcement=False).
        runner = StrategyRunner(
            price_oracle=price_oracle,
            balance_provider=balance_provider,
            execution_orchestrator=orch,
            state_manager=state_manager,
            alert_manager=alert_manager,
        )

        fake_recon = {
            "tokens_checked": ["USDC", "ETH"],
            "pre_balances": {"USDC": "10000", "ETH": "10"},
            "post_balances": {"USDC": "9000", "ETH": "10"},
            "actual_deltas": {"USDC": "-1000", "ETH": "0"},
            "expected_ranges": {
                "USDC": {"min": "-1010", "max": "-990"},
                "ETH": {"min": "0.49", "max": "0.51"},
            },
            "mismatches": [
                {"token": "ETH", "actual": "0", "expected_min": "0.49", "expected_max": "0.51"},
            ],
            "warnings": [],
            "incident": True,
            "enforced": False,
        }

        async def fake_reconcile(strategy, intent, execution_result, pre_snapshot=None):
            return fake_recon

        runner._reconcile_post_execution_balances = fake_reconcile  # type: ignore[method-assign]

        # Spy to prove the enforcement handler is bypassed in observation mode.
        # Locks the contract at the call-site boundary, not just the outcome.
        runner._single_chain_handle_recon_incident = AsyncMock()  # type: ignore[method-assign]

        strategy = MockStrategy(
            decide_returns=Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("1000"),
            )
        )

        with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
            result = await runner.run_iteration(strategy)

        # Iteration stays SUCCESS — no halt on plumbing-race false positives.
        assert result.status == IterationStatus.SUCCESS
        assert result.success is True
        # Recon data still flows to dashboards/metrics via the IterationResult.
        assert result.balance_reconciliation is fake_recon
        # Enforcement handler must be bypassed entirely in observation mode.
        runner._single_chain_handle_recon_incident.assert_not_called()
        # Circuit breaker / consecutive-errors counters stay clean.
        metrics = runner.get_metrics()
        assert metrics["successful_iterations"] == 1
        # Operator visibility: incident is logged at WARNING so it surfaces in
        # ops dashboards and log-based alerting.
        assert any(
            "Reconciliation incident detected (observation mode" in rec.message and rec.levelname == "WARNING"
            for rec in caplog.records
        ), "observation-mode incident must be WARNING-logged for ops visibility"

    @pytest.mark.asyncio
    async def test_reconciliation_clean_keeps_success_status(
        self,
        price_oracle: MockPriceOracle,
        balance_provider: MockBalanceProvider,
        state_manager: MockStateManager,
    ) -> None:
        """Clean reconciliation (incident=False) must not alter the SUCCESS path."""
        orch = _make_orchestrator_without_swap_amounts()

        runner = StrategyRunner(
            price_oracle=price_oracle,
            balance_provider=balance_provider,
            execution_orchestrator=orch,
            state_manager=state_manager,
        )

        fake_recon = {
            "tokens_checked": ["USDC", "ETH"],
            "pre_balances": {"USDC": "10000", "ETH": "10"},
            "post_balances": {"USDC": "9000", "ETH": "10.5"},
            "actual_deltas": {"USDC": "-1000", "ETH": "0.5"},
            "expected_ranges": {},
            "mismatches": [],
            "warnings": [],
            "incident": False,
            "enforced": True,
        }

        async def fake_reconcile(strategy, intent, execution_result, pre_snapshot=None):
            return fake_recon

        runner._reconcile_post_execution_balances = fake_reconcile  # type: ignore[method-assign]

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
        assert result.balance_reconciliation is fake_recon

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
        """Consecutive errors are tracked across failing iterations.

        Post fix #1771, ``_consecutive_errors`` is owned by
        ``handle_iteration_failure`` (invoked from ``run_loop``), not by
        ``_create_error_result`` inside ``run_iteration``. Calling
        ``run_iteration`` in isolation therefore does NOT bump the
        streak counter -- the counter only ticks when the iteration
        result flows back through ``run_loop``. Drive the loop here so
        the test exercises the real ownership boundary.
        """
        strategy = MockStrategy(decide_raises=ValueError("Error"))

        # Run exactly three failing iterations through run_loop so the
        # post-iteration failure handler owns the increments.
        await runner.run_loop(strategy, interval_seconds=0, max_iterations=3)

        metrics = runner.get_metrics()
        assert metrics["consecutive_errors"] == 3
        # Each failed iteration also ticks _total_iterations exactly once
        # (via _create_error_result, which now keeps that responsibility).
        assert metrics["total_iterations"] == 3

    @pytest.mark.asyncio
    async def test_consecutive_errors_reset_on_success(
        self,
        runner: StrategyRunner,
    ) -> None:
        """Consecutive errors reset after a successful iteration.

        See note on ``test_consecutive_errors_tracked``: post fix #1771,
        ``run_iteration`` in isolation no longer increments
        ``_consecutive_errors``. Seed the streak directly and then run
        one successful iteration through ``run_iteration`` to observe
        the success-reset contract owned by ``_record_success``.
        """
        successful_strategy = MockStrategy(decide_returns=Intent.hold())

        # Seed a streak that the next successful iteration should clear.
        runner._consecutive_errors = 2
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
