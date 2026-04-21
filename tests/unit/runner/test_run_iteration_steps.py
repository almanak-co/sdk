"""Tests for the ``_step_*`` helpers that ``run_iteration`` dispatches to.

Phase 3b extracted ``StrategyRunner.run_iteration`` into a driver plus a
sequence of step helpers. These tests exercise each helper directly so
regressions in the early-exit contract (return an ``IterationResult`` or
``None``) surface at unit level rather than inside the full iteration
flow.

The runner itself is constructed with the same mock pattern as
``test_circuit_breaker_wiring.py`` and ``test_stuck_detector_wiring.py``.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
)
from almanak.framework.intents.vocabulary import HoldIntent, SwapIntent
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_runner(
    *,
    circuit_breaker: CircuitBreaker | None = None,
    dry_run: bool = False,
    state_manager: MagicMock | None = None,
    balance_provider: MagicMock | None = None,
) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=dry_run,
    )
    if state_manager is None:
        state_manager = MagicMock()
    if balance_provider is None:
        balance_provider = MagicMock()
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=balance_provider,
        execution_orchestrator=MagicMock(),
        state_manager=state_manager,
        config=config,
        circuit_breaker=circuit_breaker,
    )


def _make_strategy(strategy_id: str = "test-strategy") -> MagicMock:
    strategy = MagicMock()
    strategy.strategy_id = strategy_id
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.decide.return_value = HoldIntent(reason="unit test hold")
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    # Ensure these optional hooks are absent so the hook dispatcher is a no-op
    del strategy._wallet_activity_provider
    return strategy


def _make_state(strategy: MagicMock) -> RunIterationState:
    return RunIterationState(
        strategy=strategy,
        strategy_id=strategy.strategy_id,
        start_time=datetime.now(UTC),
    )


def _tripped_breaker() -> CircuitBreaker:
    breaker = CircuitBreaker(
        strategy_id="test-strategy",
        config=CircuitBreakerConfig(
            max_consecutive_failures=3,
            max_cumulative_loss_usd=Decimal("1000"),
            cooldown_seconds=2,
        ),
    )
    breaker.record_failure("fail 1")
    breaker.record_failure("fail 2")
    breaker.record_failure("fail 3")
    assert breaker.state == CircuitBreakerState.OPEN
    return breaker


# =============================================================================
# _step_pause_gate
# =============================================================================


class TestStepPauseGate:
    @pytest.mark.asyncio
    async def test_returns_none_when_not_paused(self) -> None:
        state_manager = MagicMock()
        state_manager.load_state = AsyncMock(return_value=None)
        runner = _make_runner(state_manager=state_manager)
        strategy = _make_strategy()

        result = await runner._step_pause_gate(_make_state(strategy))
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_hold_when_paused(self) -> None:
        state_manager = MagicMock()
        state_manager.load_state = AsyncMock(
            return_value=SimpleNamespace(
                state={"is_paused": True, "pause_reason": "Operator holiday"}
            )
        )
        runner = _make_runner(state_manager=state_manager)
        strategy = _make_strategy()

        result = await runner._step_pause_gate(_make_state(strategy))
        assert result is not None
        assert result.status == IterationStatus.HOLD
        assert isinstance(result.intent, HoldIntent)
        assert "Operator holiday" in result.intent.reason


# =============================================================================
# _step_teardown_and_cb_gate
# =============================================================================


class TestStepTeardownAndCbGate:
    @pytest.mark.asyncio
    async def test_passes_through_when_breaker_closed_and_no_teardown(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()

        with patch.object(runner, "_check_teardown_requested", return_value=None):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        assert result is None

    @pytest.mark.asyncio
    async def test_open_breaker_returns_circuit_breaker_open(self) -> None:
        breaker = _tripped_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()

        with patch.object(runner, "_check_teardown_requested", return_value=None):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        assert result is not None
        assert result.status == IterationStatus.CIRCUIT_BREAKER_OPEN
        assert not result.success

    @pytest.mark.asyncio
    async def test_teardown_mode_skips_breaker_check(self) -> None:
        """When teardown is requested, the early CB gate must NOT block."""
        breaker = _tripped_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()

        sentinel_result = object()

        with (
            patch.object(runner, "_check_teardown_requested", return_value="SOFT"),
            patch.object(
                runner,
                "_execute_teardown",
                new=AsyncMock(return_value=sentinel_result),
            ) as mock_teardown,
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        assert result is sentinel_result
        mock_teardown.assert_awaited_once()


# =============================================================================
# _step_build_snapshot
# =============================================================================


class TestStepBuildSnapshot:
    @pytest.mark.asyncio
    async def test_success_stores_market_and_returns_none(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        market_sentinel = MagicMock()
        strategy.create_market_snapshot.return_value = market_sentinel
        state = _make_state(strategy)

        with patch.object(runner, "_pre_warm_prices", new=AsyncMock()) as mock_prewarm:
            result = await runner._step_build_snapshot(state)

        assert result is None
        assert state.market is market_sentinel
        mock_prewarm.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_snapshot_failure_returns_data_error(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.create_market_snapshot.side_effect = RuntimeError("no market data")

        result = await runner._step_build_snapshot(_make_state(strategy))

        assert result is not None
        assert result.status == IterationStatus.DATA_ERROR
        assert not result.success
        assert "Market snapshot failed" in result.error


# =============================================================================
# _step_decide
# =============================================================================


class TestStepDecide:
    @pytest.mark.asyncio
    async def test_success_stores_decide_result(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        expected = HoldIntent(reason="unit decide")
        strategy.decide.return_value = expected
        state = _make_state(strategy)
        state.market = MagicMock()

        result = await runner._step_decide(state)
        assert result is None
        assert state.decide_result is expected

    @pytest.mark.asyncio
    async def test_decide_raises_returns_strategy_error(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.decide.side_effect = RuntimeError("strategy bug")
        state = _make_state(strategy)
        state.market = MagicMock()

        result = await runner._step_decide(state)
        assert result is not None
        assert result.status == IterationStatus.STRATEGY_ERROR
        assert "strategy bug" in result.error

    @pytest.mark.asyncio
    async def test_decide_timeout_returns_strategy_timeout(self) -> None:
        """When ``strategy.decide()`` exceeds ``decide_timeout_seconds``, the
        helper must return an ``IterationResult`` with ``STRATEGY_TIMEOUT``,
        record a circuit-breaker failure (if a breaker is wired), and leave
        ``_decide_in_progress`` set so the next iteration catches the orphan.
        """
        runner = _make_runner()
        # Force the helper to use a tiny timeout so ``asyncio.wait_for`` trips
        # quickly without actually sleeping the worker for the default 30s.
        runner.config.decide_timeout_seconds = 0.01

        strategy = _make_strategy()

        def slow_decide(market):  # noqa: ARG001 -- signature matches decide()
            # Must be blocking (not await) because decide() runs in asyncio.to_thread.
            time.sleep(0.5)
            return HoldIntent(reason="unreachable")

        strategy.decide.side_effect = slow_decide
        state = _make_state(strategy)
        state.market = MagicMock()

        result = await runner._step_decide(state)

        assert result is not None
        assert result.status == IterationStatus.STRATEGY_TIMEOUT
        assert "timed out" in result.error.lower()
        # Guard remains set so the orphan thread's next call is caught as overlap.
        assert runner._decide_in_progress is True
        assert runner._decide_timed_out_at is not None

    @pytest.mark.asyncio
    async def test_decide_overlap_early_exits_when_previous_still_running(self) -> None:
        """If ``_decide_in_progress`` is already True (previous call timed out
        and the worker thread is still running), the helper must early-exit
        with ``STRATEGY_TIMEOUT`` instead of invoking ``strategy.decide``.
        """
        runner = _make_runner()
        # Simulate a previous timed-out call that has not yet returned.
        # _decide_timed_out_at is left at None so the 2x-timeout recovery path
        # does not reset the guard; this pins the overlap early-exit branch.
        runner._decide_in_progress = True
        runner._decide_timed_out_at = None

        strategy = _make_strategy()
        state = _make_state(strategy)
        state.market = MagicMock()

        result = await runner._step_decide(state)

        assert result is not None
        assert result.status == IterationStatus.STRATEGY_TIMEOUT
        assert "still running" in result.error
        # Critical: the new decide must NOT be invoked while the old one is live.
        strategy.decide.assert_not_called()


# =============================================================================
# _step_extract_intents
# =============================================================================


class TestStepExtractIntents:
    def test_hold_intent_returns_early_exit(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.decide_result = HoldIntent(reason="no action")

        result = runner._step_extract_intents(state)
        assert result is not None
        assert result.status == IterationStatus.HOLD
        assert result.intent.reason == "no action"
        # HOLD intent is still recorded on the state for downstream logging
        assert len(state.intents) == 1

    def test_none_decide_result_returns_hold(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.decide_result = None

        result = runner._step_extract_intents(state)
        assert result is not None
        assert result.status == IterationStatus.HOLD
        assert state.intents == []

    def test_swap_intent_populates_state_and_returns_none(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        swap = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        state.decide_result = swap

        result = runner._step_extract_intents(state)
        assert result is None
        assert state.intents == [swap]


# =============================================================================
# _step_circuit_breaker_pre_execute
# =============================================================================


class TestStepCircuitBreakerPreExecute:
    def test_no_breaker_returns_none(self) -> None:
        runner = _make_runner(circuit_breaker=None)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = [SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))]

        result = runner._step_circuit_breaker_pre_execute(state)
        assert result is None

    def test_closed_breaker_returns_none(self) -> None:
        breaker = CircuitBreaker(
            strategy_id="test-strategy",
            config=CircuitBreakerConfig(max_consecutive_failures=3),
        )
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = [SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))]

        assert runner._step_circuit_breaker_pre_execute(state) is None

    def test_open_breaker_returns_circuit_breaker_open(self) -> None:
        breaker = _tripped_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        state = _make_state(strategy)
        swap = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        state.intents = [swap]

        result = runner._step_circuit_breaker_pre_execute(state)
        assert result is not None
        assert result.status == IterationStatus.CIRCUIT_BREAKER_OPEN
        assert result.intent is swap
        assert "Circuit breaker open" in result.error


# =============================================================================
# _step_snapshot_pre_balances
# =============================================================================


class TestStepSnapshotPreBalances:
    @pytest.mark.asyncio
    async def test_populates_pre_balances_from_balance_provider(self) -> None:
        balance_provider = MagicMock()

        async def _get_balance(token: str) -> SimpleNamespace:
            return SimpleNamespace(balance=Decimal("42") if token == "USDC" else Decimal("1"))

        balance_provider.get_balance = _get_balance
        runner = _make_runner(balance_provider=balance_provider)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = [SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))]

        await runner._step_snapshot_pre_balances(state)

        assert "USDC" in state.pre_balances
        assert "ETH" in state.pre_balances
        assert state.pre_balances["USDC"] == Decimal("42")
        assert set(state.intent_tokens) == {"USDC", "ETH"}

    @pytest.mark.asyncio
    async def test_balance_provider_failure_is_swallowed(self) -> None:
        balance_provider = MagicMock()
        balance_provider.get_balance = AsyncMock(side_effect=RuntimeError("rpc down"))
        runner = _make_runner(balance_provider=balance_provider)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = [SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))]

        # Should not raise
        await runner._step_snapshot_pre_balances(state)
        assert state.pre_balances == {}
        # intent_tokens is still populated (we just couldn't snapshot balances)
        assert set(state.intent_tokens) == {"USDC", "ETH"}


# =============================================================================
# _step_execute dispatcher
# =============================================================================


class TestStepExecuteDispatch:
    @pytest.mark.asyncio
    async def test_single_chain_routes_to_run_single_chain_intents(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = [SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))]

        sentinel = object()
        with patch.object(
            runner,
            "_run_single_chain_intents",
            new=AsyncMock(return_value=sentinel),
        ) as mock_single:
            result = await runner._step_execute(state)

        assert result is sentinel
        mock_single.assert_awaited_once_with(state)

    @pytest.mark.asyncio
    async def test_multi_chain_routes_to_execute_multi_chain(self) -> None:
        runner = _make_runner()
        runner._is_multi_chain = True  # simulate multi-chain orchestrator
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = [SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))]
        state.market = MagicMock()

        sentinel = object()
        with patch.object(
            runner,
            "_execute_multi_chain",
            new=AsyncMock(return_value=sentinel),
        ) as mock_multi:
            result = await runner._step_execute(state)

        assert result is sentinel
        mock_multi.assert_awaited_once()
        kwargs = mock_multi.await_args.kwargs
        assert kwargs["strategy"] is strategy
        assert kwargs["intents"] == state.intents
        assert kwargs["start_time"] is state.start_time
        assert kwargs["market"] is state.market


# =============================================================================
# _resolve_chained_amount_for_intent
# =============================================================================


class TestResolveChainedAmount:
    def test_non_chained_intent_is_passed_through(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))

        resolved, early, should_continue = runner._resolve_chained_amount_for_intent(
            intent=intent,
            idx=0,
            intents=[intent],
            is_multi_intent=False,
            previous_amount_received=None,
            market=MagicMock(),
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert resolved is intent
        assert early is None
        assert should_continue is False

    def test_multi_intent_with_previous_amount_resolves(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        first = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        second = SwapIntent(from_token="ETH", to_token="DAI", amount="all")

        resolved, early, should_continue = runner._resolve_chained_amount_for_intent(
            intent=second,
            idx=1,
            intents=[first, second],
            is_multi_intent=True,
            previous_amount_received=Decimal("0.25"),
            market=MagicMock(),
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert early is None
        assert should_continue is False
        # The resolved intent has a concrete amount bound in
        assert resolved is not second

    def test_multi_intent_missing_previous_amount_dry_run_continues(self) -> None:
        runner = _make_runner(dry_run=True)
        strategy = _make_strategy()
        first = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        second = SwapIntent(from_token="ETH", to_token="DAI", amount="all")

        resolved, early, should_continue = runner._resolve_chained_amount_for_intent(
            intent=second,
            idx=1,
            intents=[first, second],
            is_multi_intent=True,
            previous_amount_received=None,
            market=MagicMock(),
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert resolved is second
        assert early is not None
        assert early.status == IterationStatus.DRY_RUN
        assert should_continue is True

    def test_multi_intent_missing_previous_amount_live_mode_breaks(self) -> None:
        runner = _make_runner(dry_run=False)
        strategy = _make_strategy()
        first = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        second = SwapIntent(from_token="ETH", to_token="DAI", amount="all")

        _, early, should_continue = runner._resolve_chained_amount_for_intent(
            intent=second,
            idx=1,
            intents=[first, second],
            is_multi_intent=True,
            previous_amount_received=None,
            market=MagicMock(),
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert early is not None
        assert early.status == IterationStatus.COMPILATION_FAILED
        assert should_continue is False  # break

    def test_single_intent_amount_all_resolves_from_wallet(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")

        market = MagicMock()
        market.balance.return_value = SimpleNamespace(balance=Decimal("1234.5"))

        resolved, early, should_continue = runner._resolve_chained_amount_for_intent(
            intent=intent,
            idx=0,
            intents=[intent],
            is_multi_intent=False,
            previous_amount_received=None,
            market=market,
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert early is None
        assert should_continue is False
        assert resolved is not intent  # rewritten

    def test_single_intent_amount_all_zero_balance_fails(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")

        market = MagicMock()
        market.balance.return_value = SimpleNamespace(balance=Decimal("0"))

        _, early, should_continue = runner._resolve_chained_amount_for_intent(
            intent=intent,
            idx=0,
            intents=[intent],
            is_multi_intent=False,
            previous_amount_received=None,
            market=market,
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert early is not None
        assert early.status == IterationStatus.COMPILATION_FAILED
        assert "balance is 0" in early.error
        assert should_continue is False


# =============================================================================
# Full iteration smoke test (driver + all steps)
# =============================================================================


class TestRunIterationDriverSmoke:
    """Ensure the Phase 3b driver still returns the expected outcomes."""

    _PAUSE_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._is_strategy_paused"
    _TEARDOWN_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._check_teardown_requested"

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_hold_intent_short_circuits_to_hold(
        self, mock_pause: AsyncMock, mock_teardown: MagicMock
    ) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.decide.return_value = HoldIntent(reason="Market quiet")

        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.HOLD
        assert result.intent is not None
        assert result.intent.reason == "Market quiet"

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_strategy_exception_returns_strategy_error(
        self, mock_pause: AsyncMock, mock_teardown: MagicMock
    ) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.decide.side_effect = RuntimeError("boom")

        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.STRATEGY_ERROR
        assert "boom" in result.error
