"""Tests for teardown flow in StrategyRunner.

Validates that:
- _check_teardown_requested returns TeardownMode or None (pure check, no side effects)
- Teardown path creates market snapshot before generating intents
- No temp compiler is injected into the strategy during teardown
- Lifecycle state transitions (mark_started, mark_completed, mark_failed) fire correctly
- Backward compatibility: strategies with old signature still work
- Multi-chain teardown receives market parameter
"""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch, call

import pytest

from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    StrategyRunner,
)
from almanak.framework.teardown.models import TeardownMode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_runner(**overrides) -> StrategyRunner:
    """Build a StrategyRunner with minimal mocks."""
    defaults = dict(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=None,
    )
    defaults.update(overrides)
    return StrategyRunner(**defaults)


def _make_strategy(
    *,
    strategy_id: str = "test_strat",
    chain: str = "arbitrum",
    wallet_address: str = "0x1234",
    should_teardown: bool = False,
    supports_teardown: bool = True,
    teardown_intents: list | None = None,
    market_snapshot: object | None = None,
) -> MagicMock:
    """Build a mock strategy with configurable teardown behaviour."""
    strategy = MagicMock()
    strategy.strategy_id = strategy_id
    strategy.chain = chain
    strategy.wallet_address = wallet_address
    strategy.should_teardown.return_value = should_teardown
    strategy.supports_teardown.return_value = supports_teardown

    if teardown_intents is not None:
        strategy.generate_teardown_intents.return_value = teardown_intents

    if market_snapshot is not None:
        strategy.create_market_snapshot.return_value = market_snapshot
    else:
        strategy.create_market_snapshot.return_value = MagicMock(
            get_price_oracle_dict=MagicMock(return_value={}),
        )

    return strategy


def _make_intent(intent_type: str = "SWAP", chain: str = "arbitrum") -> MagicMock:
    """Build a mock intent."""
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value=intent_type)
    intent.chain = chain
    return intent


# ---------------------------------------------------------------------------
# _check_teardown_requested
# ---------------------------------------------------------------------------


class TestCheckTeardownRequested:
    """Tests for _check_teardown_requested (pure check, no side effects)."""

    def test_returns_none_when_no_should_teardown(self):
        runner = _make_runner()
        strategy = MagicMock(spec=[])  # no should_teardown attr
        strategy.strategy_id = "strat"
        assert runner._check_teardown_requested(strategy) is None

    def test_returns_none_when_should_teardown_false(self):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=False)
        assert runner._check_teardown_requested(strategy) is None

    def test_returns_none_when_supports_teardown_false(self):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, supports_teardown=False)
        assert runner._check_teardown_requested(strategy) is None

    def test_returns_none_when_no_generate_teardown_intents(self):
        runner = _make_runner()
        strategy = MagicMock(spec=["strategy_id", "should_teardown", "supports_teardown"])
        strategy.strategy_id = "strat"
        strategy.should_teardown.return_value = True
        strategy.supports_teardown.return_value = True
        assert runner._check_teardown_requested(strategy) is None

    @patch("almanak.framework.teardown.get_teardown_state_manager")
    def test_returns_mode_from_active_request(self, mock_get_manager):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[])

        # Setup teardown state manager mock
        mock_manager = MagicMock()
        mock_request = MagicMock()
        mock_request.mode = TeardownMode.HARD
        mock_manager.get_active_request.return_value = mock_request
        mock_get_manager.return_value = mock_manager

        mode = runner._check_teardown_requested(strategy)
        assert mode == TeardownMode.HARD

    @patch("almanak.framework.teardown.get_teardown_state_manager")
    def test_returns_soft_when_no_active_request(self, mock_get_manager):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[])

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        mode = runner._check_teardown_requested(strategy)
        assert mode == TeardownMode.SOFT

    def test_acknowledges_teardown_request(self):
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[])

        with patch("almanak.framework.teardown.get_teardown_state_manager") as mock_get:
            mock_manager = MagicMock()
            mock_manager.get_active_request.return_value = None
            mock_get.return_value = mock_manager

            runner._check_teardown_requested(strategy)
            strategy.acknowledge_teardown_request.assert_called_once()

    def test_no_compiler_set_on_strategy(self):
        """Verify _check_teardown_requested does NOT inject a temp compiler."""
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[])
        # Ensure _compiler starts as None
        strategy._compiler = None

        with patch("almanak.framework.teardown.get_teardown_state_manager") as mock_get:
            mock_manager = MagicMock()
            mock_manager.get_active_request.return_value = None
            mock_get.return_value = mock_manager

            runner._check_teardown_requested(strategy)

        # _compiler should NOT have been set by the check
        assert strategy._compiler is None


# ---------------------------------------------------------------------------
# Teardown in run_iteration
# ---------------------------------------------------------------------------


class TestTeardownInRunIteration:
    """Tests for the teardown path inside run_iteration."""

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_market_created_before_intents(self, mock_get_manager):
        """Market snapshot is created BEFORE generate_teardown_intents is called."""
        runner = _make_runner()
        market_mock = MagicMock(get_price_oracle_dict=MagicMock(return_value={"ETH": Decimal("3000")}))
        intent = _make_intent()
        strategy = _make_strategy(
            should_teardown=True,
            teardown_intents=[intent],
            market_snapshot=market_mock,
        )

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        # Make _execute_single_chain return a success to avoid errors
        runner._execute_single_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                strategy_id="test_strat",
                duration_ms=100,
            )
        )

        await runner.run_iteration(strategy)

        # Verify ordering: create_market_snapshot called before generate_teardown_intents
        strategy.create_market_snapshot.assert_called_once()
        strategy.generate_teardown_intents.assert_called_once_with(
            TeardownMode.SOFT, market=market_mock
        )

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_empty_intents_marks_completed(self, mock_get_manager):
        """Empty teardown intents should mark teardown as completed."""
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[])

        mock_manager = MagicMock()
        mock_request = MagicMock()
        mock_request.mode = TeardownMode.SOFT
        mock_manager.get_active_request.return_value = mock_request
        mock_get_manager.return_value = mock_manager

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.TEARDOWN
        mock_manager.mark_completed.assert_called_once_with(
            "test_strat", result={"reason": "no_positions"}
        )

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_failure_marks_failed(self, mock_get_manager):
        """Exception in generate_teardown_intents should mark teardown as failed."""
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True)
        strategy.generate_teardown_intents.side_effect = RuntimeError("boom")

        mock_manager = MagicMock()
        mock_request = MagicMock()
        mock_request.mode = TeardownMode.SOFT
        mock_manager.get_active_request.return_value = mock_request
        mock_get_manager.return_value = mock_manager

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.STRATEGY_ERROR
        mock_manager.mark_failed.assert_called_once_with("test_strat", error="boom")

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_not_implemented_marks_failed(self, mock_get_manager):
        """NotImplementedError in generate_teardown_intents should mark failed."""
        runner = _make_runner()
        strategy = _make_strategy(should_teardown=True)
        strategy.generate_teardown_intents.side_effect = NotImplementedError("not done")

        mock_manager = MagicMock()
        mock_request = MagicMock()
        mock_request.mode = TeardownMode.SOFT
        mock_manager.get_active_request.return_value = mock_request
        mock_get_manager.return_value = mock_manager

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.STRATEGY_ERROR
        mock_manager.mark_failed.assert_called_once()

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_market_failure_continues_without_market(self, mock_get_manager):
        """If create_market_snapshot fails, teardown still proceeds with market=None."""
        runner = _make_runner()
        intent = _make_intent()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[intent])
        strategy.create_market_snapshot.side_effect = RuntimeError("no market")

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        runner._execute_single_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                strategy_id="test_strat",
                duration_ms=100,
            )
        )

        await runner.run_iteration(strategy)

        # Should have been called with market=None since snapshot failed
        strategy.generate_teardown_intents.assert_called_once_with(
            TeardownMode.SOFT, market=None
        )

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_no_temp_compiler_injection(self, mock_get_manager):
        """Verify no _compiler is set on the strategy during teardown."""
        runner = _make_runner()
        intent = _make_intent()
        strategy = _make_strategy(should_teardown=True, teardown_intents=[intent])
        strategy._compiler = None

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        # Track if _compiler was set during generate_teardown_intents
        compiler_during_call = []

        original_gen = strategy.generate_teardown_intents.return_value

        def capture_compiler(*args, **kwargs):
            compiler_during_call.append(strategy._compiler)
            return original_gen

        strategy.generate_teardown_intents.side_effect = capture_compiler

        runner._execute_single_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                strategy_id="test_strat",
                duration_ms=100,
            )
        )

        await runner.run_iteration(strategy)

        # _compiler should still be None (no injection happened)
        assert compiler_during_call[0] is None

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_backward_compat_old_signature(self, mock_get_manager):
        """Strategies with old signature def generate_teardown_intents(self, mode) still work.

        The runner calls generate_teardown_intents(mode, market=market).
        A strategy with the old signature (no market param) would raise TypeError.
        The runner catches this and falls back to calling without market.
        """
        runner = _make_runner()
        intent = _make_intent()
        strategy = _make_strategy(should_teardown=True)

        # Replace the mock with a real callable that rejects `market` kwarg
        def old_style_generate(mode):
            """Old-style signature: no market parameter."""
            return [intent]

        strategy.generate_teardown_intents = old_style_generate

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        runner._execute_single_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                strategy_id="test_strat",
                duration_ms=100,
            )
        )

        # Should NOT raise TypeError -- runner falls back to old signature
        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.TEARDOWN

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_multichain_teardown_receives_market(self, mock_get_manager):
        """Multi-chain teardown path passes market to _execute_multi_chain."""
        from almanak.framework.execution.multichain import MultiChainOrchestrator

        multi_orch = MagicMock(spec=MultiChainOrchestrator)
        runner = _make_runner(execution_orchestrator=multi_orch)

        market_mock = MagicMock(get_price_oracle_dict=MagicMock(return_value={}))
        intent = _make_intent()
        strategy = _make_strategy(
            should_teardown=True,
            teardown_intents=[intent],
            market_snapshot=market_mock,
        )

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        runner._execute_multi_chain = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS,
                intent=intent,
                strategy_id="test_strat",
                duration_ms=100,
            )
        )

        await runner.run_iteration(strategy)

        runner._execute_multi_chain.assert_called_once()
        call_kwargs = runner._execute_multi_chain.call_args[1]
        assert call_kwargs["market"] is market_mock

    @pytest.mark.asyncio
    @patch("almanak.framework.teardown.get_teardown_state_manager")
    async def test_multichain_teardown_threads_prices(self, mock_get_manager):
        """Multi-chain teardown extracts price_map from market and passes to orchestrator."""
        from almanak.framework.execution.multichain import MultiChainOrchestrator

        multi_orch = MagicMock(spec=MultiChainOrchestrator)
        multi_orch.primary_chain = "arbitrum"
        multi_orch.execute_sequence = AsyncMock(
            return_value=MagicMock(
                success=True,
                successful_count=1,
                chains_used={"arbitrum"},
                total_execution_time_ms=100,
                errors_by_chain={},
            )
        )
        runner = _make_runner(execution_orchestrator=multi_orch)

        prices = {"ETH": Decimal("3400"), "USDC": Decimal("1")}
        market_mock = MagicMock(get_price_oracle_dict=MagicMock(return_value=prices))
        intent = _make_intent()
        # Mark intent as same-chain (no destination_chain) to hit execute_sequence path
        intent.destination_chain = None
        strategy = _make_strategy(
            should_teardown=True,
            teardown_intents=[intent],
            market_snapshot=market_mock,
        )

        mock_manager = MagicMock()
        mock_manager.get_active_request.return_value = None
        mock_get_manager.return_value = mock_manager

        # Patch is_cross_chain_intent to return False
        with patch("almanak.framework.runner.strategy_runner.is_cross_chain_intent", return_value=False):
            await runner.run_iteration(strategy)

        # Verify execute_sequence was called with price_map and price_oracle
        multi_orch.execute_sequence.assert_called_once()
        call_kwargs = multi_orch.execute_sequence.call_args[1]
        assert call_kwargs["price_map"] == {"ETH": "3400", "USDC": "1"}
        assert call_kwargs["price_oracle"] == prices
