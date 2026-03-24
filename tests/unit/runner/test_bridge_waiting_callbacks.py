"""Tests for on_intent_executed/save_state callbacks in _execute_with_bridge_waiting.

Validates VIB-1818: the bridge-waiting execution path must call on_intent_executed()
and save_state() after each step, matching the single-chain path behavior.
"""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import (
    RunnerConfig,
    StrategyRunner,
)


def _make_runner() -> StrategyRunner:
    """Create a minimal StrategyRunner with mocked dependencies."""
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=MagicMock(),
        config=RunnerConfig(
            default_interval_seconds=0,
            enable_state_persistence=False,
        ),
    )
    return runner


def _make_strategy() -> MagicMock:
    """Create a mock strategy with required attributes."""
    strategy = MagicMock()
    strategy.strategy_id = "test-bridge-strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x" + "a1" * 20
    strategy.on_intent_executed = MagicMock()
    strategy.save_state = MagicMock()
    return strategy


def _make_intent(intent_type: str = "SWAP", chain: str = "arbitrum") -> MagicMock:
    """Create a mock intent."""
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value=intent_type)
    intent.chain = chain
    intent.serialize = MagicMock(return_value={"type": intent_type, "chain": chain})
    return intent


def _make_orchestrator(success: bool = True) -> MagicMock:
    """Create a mock multi-chain orchestrator."""
    orch = MagicMock()
    orch.wallet_address = "0x" + "a1" * 20
    orch.primary_chain = "arbitrum"
    orch._config = None

    result = MagicMock()
    result.success = success
    result.error = None if success else "execution failed"
    result.tx_result = SimpleNamespace(actual_amount_received=Decimal("100"))
    orch.execute = AsyncMock(return_value=result)
    return orch


class TestBridgeWaitingCallbacksOnSuccess:
    """Test that on_intent_executed and save_state are called on success."""

    @pytest.mark.asyncio
    async def test_on_intent_executed_called_on_success(self):
        runner = _make_runner()
        strategy = _make_strategy()
        orchestrator = _make_orchestrator(success=True)
        intent = _make_intent()

        with (
            patch.object(runner, "_load_execution_progress", new_callable=AsyncMock, return_value=None),
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_clear_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_get_gateway_client", return_value=None),
            patch.object(runner, "_record_success"),
            patch.object(runner, "_calculate_duration_ms", return_value=100),
            patch("almanak.framework.runner.strategy_runner.is_cross_chain_intent", return_value=False),
            patch("almanak.framework.runner.strategy_runner.Intent") as mock_intent_cls,
        ):
            mock_intent_cls.has_chained_amount.return_value = False
            mock_intent_cls.get_amount_field.return_value = Decimal("100")

            result = await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent],
                orchestrator=orchestrator,
                start_time=datetime.now(UTC),
            )

        # on_intent_executed must be called with success=True
        strategy.on_intent_executed.assert_called_once()
        call_args = strategy.on_intent_executed.call_args
        assert call_args[0][0] is intent  # first arg is the intent
        # success may be positional or keyword
        success_val = call_args[1].get("success", call_args[0][1] if len(call_args[0]) > 1 else None)
        assert success_val is True

        # save_state must be called
        strategy.save_state.assert_called_once()

    @pytest.mark.asyncio
    async def test_callbacks_called_per_step(self):
        """With 2 intents, callbacks should be called twice."""
        runner = _make_runner()
        strategy = _make_strategy()
        orchestrator = _make_orchestrator(success=True)
        intent1 = _make_intent()
        intent2 = _make_intent()

        with (
            patch.object(runner, "_load_execution_progress", new_callable=AsyncMock, return_value=None),
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_clear_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_get_gateway_client", return_value=None),
            patch.object(runner, "_record_success"),
            patch.object(runner, "_calculate_duration_ms", return_value=100),
            patch("almanak.framework.runner.strategy_runner.is_cross_chain_intent", return_value=False),
            patch("almanak.framework.runner.strategy_runner.Intent") as mock_intent_cls,
        ):
            mock_intent_cls.has_chained_amount.return_value = False
            mock_intent_cls.get_amount_field.return_value = Decimal("100")

            result = await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent1, intent2],
                orchestrator=orchestrator,
                start_time=datetime.now(UTC),
            )

        assert strategy.on_intent_executed.call_count == 2
        assert strategy.save_state.call_count == 2


class TestBridgeWaitingCallbacksOnFailure:
    """Test that on_intent_executed is called on failure."""

    @pytest.mark.asyncio
    async def test_on_intent_executed_called_on_execution_failure(self):
        """When orchestrator.execute returns success=False, callback fires with success=False."""
        runner = _make_runner()
        strategy = _make_strategy()
        orchestrator = _make_orchestrator(success=False)
        intent = _make_intent()

        with (
            patch.object(runner, "_load_execution_progress", new_callable=AsyncMock, return_value=None),
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_get_gateway_client", return_value=None),
            patch.object(runner, "_calculate_duration_ms", return_value=100),
            patch("almanak.framework.runner.strategy_runner.is_cross_chain_intent", return_value=False),
            patch("almanak.framework.runner.strategy_runner.Intent") as mock_intent_cls,
            patch("almanak.framework.runner.strategy_runner.diagnose_revert", new_callable=AsyncMock),
        ):
            mock_intent_cls.has_chained_amount.return_value = False

            result = await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent],
                orchestrator=orchestrator,
                start_time=datetime.now(UTC),
            )

        # on_intent_executed must be called with success=False
        strategy.on_intent_executed.assert_called_once()
        call_args = strategy.on_intent_executed.call_args
        success_val = call_args[1].get("success", call_args[0][1] if len(call_args[0]) > 1 else None)
        assert success_val is False

        # save_state should NOT be called on failure
        strategy.save_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_intent_executed_called_on_exception(self):
        """When orchestrator.execute raises, callback fires with success=False."""
        runner = _make_runner()
        strategy = _make_strategy()
        orchestrator = _make_orchestrator()
        orchestrator.execute = AsyncMock(side_effect=RuntimeError("RPC timeout"))
        intent = _make_intent()

        with (
            patch.object(runner, "_load_execution_progress", new_callable=AsyncMock, return_value=None),
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_get_gateway_client", return_value=None),
            patch.object(runner, "_calculate_duration_ms", return_value=100),
            patch("almanak.framework.runner.strategy_runner.is_cross_chain_intent", return_value=False),
            patch("almanak.framework.runner.strategy_runner.Intent") as mock_intent_cls,
            patch("almanak.framework.runner.strategy_runner.diagnose_revert", new_callable=AsyncMock),
        ):
            mock_intent_cls.has_chained_amount.return_value = False

            result = await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent],
                orchestrator=orchestrator,
                start_time=datetime.now(UTC),
            )

        # on_intent_executed must be called with success=False
        strategy.on_intent_executed.assert_called_once()
        call_args = strategy.on_intent_executed.call_args
        success_val = call_args[1].get("success", call_args[0][1] if len(call_args[0]) > 1 else None)
        assert success_val is False


class TestBridgeWaitingCallbackErrorHandling:
    """Test that callback errors don't crash the execution path."""

    @pytest.mark.asyncio
    async def test_on_intent_executed_error_is_swallowed(self):
        """If on_intent_executed raises, execution still succeeds."""
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.on_intent_executed.side_effect = RuntimeError("callback bug")
        orchestrator = _make_orchestrator(success=True)
        intent = _make_intent()

        with (
            patch.object(runner, "_load_execution_progress", new_callable=AsyncMock, return_value=None),
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_clear_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_get_gateway_client", return_value=None),
            patch.object(runner, "_record_success"),
            patch.object(runner, "_calculate_duration_ms", return_value=100),
            patch("almanak.framework.runner.strategy_runner.is_cross_chain_intent", return_value=False),
            patch("almanak.framework.runner.strategy_runner.Intent") as mock_intent_cls,
        ):
            mock_intent_cls.has_chained_amount.return_value = False
            mock_intent_cls.get_amount_field.return_value = Decimal("100")

            # Should not raise despite callback error
            result = await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent],
                orchestrator=orchestrator,
                start_time=datetime.now(UTC),
            )

        # save_state still called even if on_intent_executed errored
        strategy.save_state.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_state_error_is_swallowed(self):
        """If save_state raises, execution still succeeds."""
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.save_state.side_effect = RuntimeError("state bug")
        orchestrator = _make_orchestrator(success=True)
        intent = _make_intent()

        with (
            patch.object(runner, "_load_execution_progress", new_callable=AsyncMock, return_value=None),
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_clear_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_get_gateway_client", return_value=None),
            patch.object(runner, "_record_success"),
            patch.object(runner, "_calculate_duration_ms", return_value=100),
            patch("almanak.framework.runner.strategy_runner.is_cross_chain_intent", return_value=False),
            patch("almanak.framework.runner.strategy_runner.Intent") as mock_intent_cls,
        ):
            mock_intent_cls.has_chained_amount.return_value = False
            mock_intent_cls.get_amount_field.return_value = Decimal("100")

            # Should not raise despite save_state error
            result = await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent],
                orchestrator=orchestrator,
                start_time=datetime.now(UTC),
            )

        strategy.on_intent_executed.assert_called_once()


class TestBridgeWaitingCrossChainFailureCallbacks:
    """Test that bridge-stage failures (cross-chain) also fire on_intent_executed."""

    @pytest.mark.asyncio
    async def test_no_tx_hash_fires_callback(self):
        """When execution succeeds but returns no tx_hash, callback fires with success=False.

        This covers the source TX verification path where is_cross_chain=True
        but result.tx_result has no tx_hash, so bridge tracking can't proceed.
        """
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent()
        # Set cross-chain attributes on the intent
        intent.destination_chain = "optimism"
        intent.to_token = "USDC"

        # Orchestrator succeeds, but tx_result has no tx_hash
        orchestrator = _make_orchestrator(success=True)
        result_obj = orchestrator.execute.return_value
        result_obj.tx_result = SimpleNamespace(actual_amount_received=Decimal("100"), tx_hash=None)

        with (
            patch.object(runner, "_load_execution_progress", new_callable=AsyncMock, return_value=None),
            patch.object(runner, "_save_execution_progress", new_callable=AsyncMock),
            patch.object(runner, "_get_gateway_client", return_value=None),
            patch.object(runner, "_calculate_duration_ms", return_value=100),
            patch("almanak.framework.runner.strategy_runner.is_cross_chain_intent", return_value=True),
            patch("almanak.framework.runner.strategy_runner.Intent") as mock_intent_cls,
            patch("almanak.framework.runner.strategy_runner.diagnose_revert", new_callable=AsyncMock),
        ):
            mock_intent_cls.has_chained_amount.return_value = False
            mock_intent_cls.get_amount_field.return_value = Decimal("100")

            await runner._execute_with_bridge_waiting(
                strategy=strategy,
                intents=[intent],
                orchestrator=orchestrator,
                start_time=datetime.now(UTC),
            )

        # on_intent_executed must be called with success=False via the finalization block
        strategy.on_intent_executed.assert_called_once()
        call_args = strategy.on_intent_executed.call_args
        success_val = call_args[1].get("success", call_args[0][1] if len(call_args[0]) > 1 else None)
        assert success_val is False

        # save_state should NOT be called on failure (matching _execute_single_chain parity)
        strategy.save_state.assert_not_called()
