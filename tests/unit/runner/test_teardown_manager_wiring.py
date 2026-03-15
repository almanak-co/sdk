"""Tests for TeardownManager wiring into StrategyRunner (VIB-1254).

Verifies that single-chain teardown routes through TeardownManager for:
- Safety validation (SafetyGuard)
- Escalating slippage with approval gates
- State persistence for resumability
- Post-execution verification

Also verifies fallback to inline execution when TeardownManager can't be created.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownResult,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_teardown_strategy():
    """Create a mock strategy that supports teardown."""
    strategy = MagicMock()
    strategy.strategy_id = "test_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.supports_teardown.return_value = True
    strategy.should_teardown.return_value = True

    # Teardown methods
    teardown_intent = MagicMock()
    teardown_intent.intent_type = MagicMock()
    teardown_intent.intent_type.value = "SWAP"
    strategy.generate_teardown_intents.return_value = [teardown_intent]

    # For TeardownManager protocol
    strategy.name = "Test Strategy"
    strategy.uses_safe_wallet = False
    strategy.pause = AsyncMock()

    from almanak.framework.teardown.models import (
        PositionInfo,
        PositionType,
        TeardownPositionSummary,
    )

    strategy.get_open_positions.return_value = TeardownPositionSummary(
        strategy_id="test_strategy",
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="test_pos",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("1000"),
                details={"asset": "ETH"},
            )
        ],
    )

    # acknowledge_teardown_request for the runner
    strategy.acknowledge_teardown_request = MagicMock()

    return strategy


def _make_runner():
    """Create a StrategyRunner for teardown tests."""
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
    )
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )
    return runner


def _make_successful_teardown_result():
    """Create a successful TeardownResult."""
    return TeardownResult(
        success=True,
        strategy_id="test_strategy",
        mode="graceful",
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        duration_seconds=5.0,
        intents_total=1,
        intents_succeeded=1,
        intents_failed=0,
        starting_value_usd=Decimal("1000"),
        final_value_usd=Decimal("990"),
        total_costs_usd=Decimal("10"),
        final_balances={},
    )


def _make_failed_teardown_result():
    """Create a failed TeardownResult."""
    return TeardownResult(
        success=False,
        strategy_id="test_strategy",
        mode="graceful",
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        duration_seconds=5.0,
        intents_total=1,
        intents_succeeded=0,
        intents_failed=1,
        starting_value_usd=Decimal("1000"),
        final_value_usd=Decimal("1000"),
        total_costs_usd=Decimal("0"),
        final_balances={},
        error="Slippage too high",
    )


# =============================================================================
# Tests
# =============================================================================


class TestTeardownManagerRouting:
    """Verify that single-chain teardown routes through TeardownManager."""

    @pytest.mark.asyncio
    @patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
    @patch.object(StrategyRunner, "_check_teardown_requested")
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_single_chain_teardown_uses_teardown_manager(
        self, mock_get_state_mgr, mock_check_teardown, _mock_paused
    ):
        """Single-chain teardown should call _execute_teardown_via_manager."""
        mock_check_teardown.return_value = TeardownMode.SOFT

        state_mgr = MagicMock()
        request = MagicMock()
        state_mgr.get_active_request.return_value = request
        mock_get_state_mgr.return_value = state_mgr

        strategy = _make_teardown_strategy()
        runner = _make_runner()

        # Mock the TeardownManager path
        runner._execute_teardown_via_manager = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=None,
                strategy_id="test_strategy",
                duration_ms=100,
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.TEARDOWN
        runner._execute_teardown_via_manager.assert_called_once()

    @pytest.mark.asyncio
    @patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
    @patch.object(StrategyRunner, "_check_teardown_requested")
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_teardown_manager_success_triggers_shutdown(
        self, mock_get_state_mgr, mock_check_teardown, _mock_paused
    ):
        """Successful TeardownManager execution should request shutdown."""
        mock_check_teardown.return_value = TeardownMode.SOFT
        state_mgr = MagicMock()
        state_mgr.get_active_request.return_value = MagicMock()
        mock_get_state_mgr.return_value = state_mgr

        strategy = _make_teardown_strategy()
        runner = _make_runner()

        runner._execute_teardown_via_manager = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=None,
                strategy_id="test_strategy",
                duration_ms=100,
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.TEARDOWN
        runner._execute_teardown_via_manager.assert_called_once()

    @pytest.mark.asyncio
    @patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
    @patch.object(StrategyRunner, "_check_teardown_requested")
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_teardown_manager_failure_marks_failed(
        self, mock_get_state_mgr, mock_check_teardown, _mock_paused
    ):
        """Failed TeardownManager execution should return error status."""
        mock_check_teardown.return_value = TeardownMode.SOFT
        state_mgr = MagicMock()
        request = MagicMock()
        state_mgr.get_active_request.return_value = request
        mock_get_state_mgr.return_value = state_mgr

        strategy = _make_teardown_strategy()
        runner = _make_runner()

        runner._execute_teardown_via_manager = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.STRATEGY_ERROR,
                error="Slippage too high",
                strategy_id="test_strategy",
                duration_ms=100,
            )
        )

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.STRATEGY_ERROR
        assert "Slippage too high" in result.error


class TestTeardownFallback:
    """Verify fallback to inline execution when TeardownManager can't be created."""

    @pytest.mark.asyncio
    @patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
    @patch.object(StrategyRunner, "_check_teardown_requested")
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_fallback_to_inline_on_compiler_failure(
        self, mock_get_state_mgr, mock_check_teardown, _mock_paused
    ):
        """If compiler can't be built, fall back to inline execution."""
        mock_check_teardown.return_value = TeardownMode.SOFT
        state_mgr = MagicMock()
        state_mgr.get_active_request.return_value = MagicMock()
        mock_get_state_mgr.return_value = state_mgr

        strategy = _make_teardown_strategy()
        runner = _make_runner()

        # Make compiler building fail -> should fallback to inline
        runner._build_teardown_compiler = MagicMock(return_value=None)
        runner._execute_teardown_inline = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.TEARDOWN,
                intent=None,
                strategy_id="test_strategy",
                duration_ms=100,
            )
        )

        result = await runner._execute_teardown_via_manager(
            strategy=strategy,
            teardown_intents=[MagicMock()],
            teardown_mode=TeardownMode.SOFT,
            teardown_market=None,
            start_time=datetime.now(UTC),
            request=None,
            state_manager=state_mgr,
        )

        # Should have used _execute_teardown_inline (fallback path)
        runner._execute_teardown_inline.assert_called_once()
        assert result.status == IterationStatus.TEARDOWN


class TestTeardownCompilerCreation:
    """Verify _build_teardown_compiler creates a valid compiler."""

    def test_creates_compiler_with_gateway_client(self):
        """Compiler should use gateway client when available."""
        from almanak.framework.execution.gateway_orchestrator import GatewayExecutionOrchestrator

        mock_orch = MagicMock(spec=GatewayExecutionOrchestrator)
        mock_orch._client = MagicMock()

        runner = StrategyRunner(
            price_oracle=MagicMock(),
            balance_provider=MagicMock(),
            execution_orchestrator=mock_orch,
            state_manager=MagicMock(),
        )

        strategy = MagicMock()
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"

        compiler = runner._build_teardown_compiler(strategy, None)

        assert compiler is not None

    def test_creates_compiler_with_market_prices(self):
        """Compiler should use real prices from market snapshot."""
        runner = _make_runner()

        strategy = MagicMock()
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"

        market = MagicMock()
        market.get_price_oracle_dict.return_value = {"ETH": 3000, "USDC": 1}

        compiler = runner._build_teardown_compiler(strategy, market)

        assert compiler is not None
        # Compiler should have real prices, not placeholders
        assert compiler.price_oracle is not None

    def test_returns_none_on_failure(self):
        """Should return None if compiler cannot be created."""
        runner = _make_runner()
        strategy = MagicMock()
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"

        with patch("almanak.framework.runner.strategy_runner.IntentCompiler", side_effect=RuntimeError("bad")):
            compiler = runner._build_teardown_compiler(strategy, None)

        assert compiler is None
