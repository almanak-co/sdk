"""Extended tests for Phase 3b ``_step_*`` and helper coverage.

Complements ``test_run_iteration_steps.py`` with additional branch and
edge-case coverage for the helpers ``run_iteration`` dispatches to:
``_step_pause_gate``, ``_step_teardown_and_cb_gate``, ``_step_periodic_hooks``,
``_step_log_intents``, ``_step_snapshot_pre_balances``,
``_step_circuit_breaker_pre_execute``, ``_step_execute``,
``_resolve_chained_amount_for_intent`` / ``_resolve_chained_amount_from_wallet``,
and ``_run_single_chain_intents``.
"""

from __future__ import annotations

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
from almanak.framework.intents.vocabulary import (
    HoldIntent,
    IntentSequence,
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
)
from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)

# =============================================================================
# Helpers
# =============================================================================


def _strategy_runner_info_records(caplog: pytest.LogCaptureFixture) -> list:
    """Return INFO records emitted by the strategy_runner logger.

    In CI environments where a handler has been attached both at the module
    logger and at root, ``caplog`` captures the same ``LogRecord`` twice
    (same ``name``, same ``message``, same identity). Dedupe by ``id`` so
    the helper reflects how many times ``logger.info`` was actually called.
    """
    seen: set[int] = set()
    unique = []
    for record in caplog.records:
        if (
            record.levelname == "INFO"
            and record.name == "almanak.framework.runner.strategy_runner"
            and id(record) not in seen
        ):
            seen.add(id(record))
            unique.append(record)
    return unique


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


def _make_strategy(deployment_id: str = "test-strategy") -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = deployment_id
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.create_market_snapshot.return_value.has_critical_data_failures.return_value = False
    strategy.decide.return_value = HoldIntent(reason="unit test hold")
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    del strategy._wallet_activity_provider
    return strategy


def _make_state(strategy: MagicMock) -> RunIterationState:
    return RunIterationState(
        strategy=strategy,
        deployment_id=strategy.deployment_id,
        start_time=datetime.now(UTC),
    )


def _tripped_breaker() -> CircuitBreaker:
    breaker = CircuitBreaker(
        deployment_id="test-strategy",
        config=CircuitBreakerConfig(
            max_consecutive_failures=2,
            max_cumulative_loss_usd=Decimal("1000"),
            cooldown_seconds=2,
        ),
    )
    breaker.record_failure("fail 1")
    breaker.record_failure("fail 2")
    assert breaker.state == CircuitBreakerState.OPEN
    return breaker


# =============================================================================
# _step_pause_gate - extended
# =============================================================================


class TestStepPauseGateExtended:
    @pytest.mark.asyncio
    async def test_paused_without_reason_still_returns_hold(self) -> None:
        state_manager = MagicMock()
        state_manager.load_state = AsyncMock(
            return_value=SimpleNamespace(state={"is_paused": True})  # no reason
        )
        runner = _make_runner(state_manager=state_manager)
        strategy = _make_strategy()

        result = await runner._step_pause_gate(_make_state(strategy))
        assert result is not None
        assert result.status == IterationStatus.HOLD
        assert isinstance(result.intent, HoldIntent)
        # Fallback reason
        assert "Paused by operator" in result.intent.reason

    @pytest.mark.asyncio
    async def test_paused_logs_only_once_per_strategy(self, caplog: pytest.LogCaptureFixture) -> None:
        state_manager = MagicMock()
        state_manager.load_state = AsyncMock(
            return_value=SimpleNamespace(state={"is_paused": True, "pause_reason": "holiday"})
        )
        runner = _make_runner(state_manager=state_manager)
        strategy = _make_strategy()

        with caplog.at_level("INFO", logger="almanak.framework.runner.strategy_runner"):
            # First pause call adds to _logged_paused_deployment_ids and emits an INFO log.
            await runner._step_pause_gate(_make_state(strategy))
            assert strategy.deployment_id in runner._logged_paused_deployment_ids

            # Second call: already logged, still returns HOLD but must NOT re-log.
            result2 = await runner._step_pause_gate(_make_state(strategy))

        assert result2 is not None
        assert result2.status == IterationStatus.HOLD
        # Still in the set (it gets cleared only when strategy resumes)
        assert strategy.deployment_id in runner._logged_paused_deployment_ids

        # Duplicate-log regression guard: exactly one INFO record mentioning this
        # strategy across both _step_pause_gate calls. The helper emits
        # "[PAUSED] <deployment_id> is paused by operator (<reason>)" only on the
        # first call because of the _logged_paused_deployment_ids membership check.
        paused_records = [
            r
            for r in caplog.records
            if r.levelname == "INFO"
            and strategy.deployment_id in r.message
            and "paused by operator" in r.message
        ]
        assert len(paused_records) == 1, (
            f"expected exactly 1 paused INFO log, got {len(paused_records)}: "
            f"{[r.message for r in paused_records]}"
        )

    @pytest.mark.asyncio
    async def test_unpaused_clears_logged_marker(self) -> None:
        """Clearing the is_paused flag discards the strategy from the logged set."""
        state_manager = MagicMock()
        state_manager.load_state = AsyncMock(return_value=None)
        runner = _make_runner(state_manager=state_manager)
        strategy = _make_strategy()
        # Pre-seed as if strategy was previously paused
        runner._logged_paused_deployment_ids.add(strategy.deployment_id)

        result = await runner._step_pause_gate(_make_state(strategy))
        assert result is None
        assert strategy.deployment_id not in runner._logged_paused_deployment_ids

    @pytest.mark.asyncio
    async def test_paused_records_success_metric(self) -> None:
        """Paused iterations bump the success counter so loop health stays green."""
        state_manager = MagicMock()
        state_manager.load_state = AsyncMock(
            return_value=SimpleNamespace(state={"is_paused": True, "pause_reason": "reason"})
        )
        runner = _make_runner(state_manager=state_manager)
        strategy = _make_strategy()

        pre_total = runner._total_iterations
        pre_success = runner._successful_iterations
        await runner._step_pause_gate(_make_state(strategy))
        assert runner._total_iterations == pre_total + 1
        assert runner._successful_iterations == pre_success + 1


# =============================================================================
# _step_teardown_and_cb_gate - extended
# =============================================================================


class TestStepTeardownAndCbGateExtended:
    @pytest.mark.asyncio
    async def test_no_breaker_and_no_teardown_passes_through(self) -> None:
        runner = _make_runner(circuit_breaker=None)
        strategy = _make_strategy()

        with patch.object(runner, "_check_teardown_requested", return_value=None):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))
        assert result is None

    @pytest.mark.asyncio
    async def test_teardown_mode_stored_on_state(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        teardown_sentinel = MagicMock()

        with (
            patch.object(runner, "_check_teardown_requested", return_value="SOFT"),
            patch.object(
                runner, "_execute_teardown", new=AsyncMock(return_value=teardown_sentinel)
            ) as mock_exec_teardown,
        ):
            result = await runner._step_teardown_and_cb_gate(state)

        assert state.teardown_mode == "SOFT"
        # Verify teardown was actually dispatched and its result propagated --
        # not just that state was mutated (per CodeRabbit PR #1656 feedback).
        mock_exec_teardown.assert_awaited_once()
        assert result is teardown_sentinel

    @pytest.mark.asyncio
    async def test_multi_chain_stuck_resume_returns_result(self) -> None:
        runner = _make_runner()
        runner._is_multi_chain = True
        strategy = _make_strategy()

        resume_sentinel = MagicMock()
        with (
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(
                runner,
                "_check_and_resume_stuck_execution",
                new=AsyncMock(return_value=resume_sentinel),
            ),
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        assert result is resume_sentinel

    @pytest.mark.asyncio
    async def test_multi_chain_no_stuck_resume_proceeds(self) -> None:
        runner = _make_runner()
        runner._is_multi_chain = True
        strategy = _make_strategy()

        with (
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_check_and_resume_stuck_execution", new=AsyncMock(return_value=None)),
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        assert result is None


# =============================================================================
# _step_periodic_hooks - extended
# =============================================================================


class TestStepPeriodicHooksExtended:
    @pytest.mark.asyncio
    async def test_no_activity_provider_and_no_vault_is_noop(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        # Attach a hook that must NOT fire when no provider is configured
        strategy.on_copy_activity_polled = MagicMock()
        # Sanity: no activity provider configured (see _make_strategy)
        assert getattr(strategy, "_wallet_activity_provider", None) is None
        # Sanity: no vault lifecycle configured on the runner
        assert runner._vault_lifecycle is None
        state = _make_state(strategy)

        # Should complete without raising, without any hook side effects
        result = await runner._step_periodic_hooks(state)

        # Helper is side-effect-only and must return None
        assert result is None
        # No hooks should have been invoked on a fully no-op iteration
        strategy.on_copy_activity_polled.assert_not_called()
        # State is not mutated by this helper
        assert state.market is None
        assert state.intents == []
        # State manager must not have been touched (no persistence work done)
        runner.state_manager.save_state.assert_not_called()
        runner.state_manager.load_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_activity_provider_exception_swallowed(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        provider = MagicMock()
        provider.poll_and_process.side_effect = RuntimeError("rpc down")
        strategy._wallet_activity_provider = provider

        # Should not raise; error is logged and iteration continues
        await runner._step_periodic_hooks(_make_state(strategy))
        provider.poll_and_process.assert_called_once()

    @pytest.mark.asyncio
    async def test_activity_provider_invokes_optional_hook(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        provider = MagicMock()
        strategy._wallet_activity_provider = provider
        strategy.on_copy_activity_polled = MagicMock()

        await runner._step_periodic_hooks(_make_state(strategy))

        provider.poll_and_process.assert_called_once()
        strategy.on_copy_activity_polled.assert_called_once_with(provider)


# =============================================================================
# _step_build_snapshot - extended
# =============================================================================


class TestStepBuildSnapshotExtended:
    @pytest.mark.asyncio
    async def test_dry_run_mode_injects_simulated_balances(self) -> None:
        runner = _make_runner(dry_run=True)
        strategy = _make_strategy()
        market = MagicMock()
        strategy.create_market_snapshot.return_value = market

        with (
            patch.object(runner, "_inject_simulated_balances") as mock_inject,
            patch.object(runner, "_pre_warm_prices", new=AsyncMock()) as mock_prewarm,
        ):
            result = await runner._step_build_snapshot(_make_state(strategy))

        assert result is None
        mock_inject.assert_called_once_with(market, strategy)
        mock_prewarm.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_live_mode_skips_simulated_balance_injection(self) -> None:
        runner = _make_runner(dry_run=False)
        strategy = _make_strategy()

        with (
            patch.object(runner, "_inject_simulated_balances") as mock_inject,
            patch.object(runner, "_pre_warm_prices", new=AsyncMock()),
        ):
            await runner._step_build_snapshot(_make_state(strategy))
        mock_inject.assert_not_called()


# =============================================================================
# _step_extract_intents - extended
# =============================================================================


class TestStepExtractIntentsExtended:
    def test_intent_sequence_flattens_into_list(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        seq = IntentSequence(
            [
                SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1")),
                SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1")),
            ]
        )
        state.decide_result = seq

        result = runner._step_extract_intents(state)
        assert result is None
        assert len(state.intents) == 2

    def test_list_with_nested_intent_sequence_flattens(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        seq = IntentSequence([SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))])
        raw_intent = SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1"))
        state.decide_result = [seq, raw_intent]

        result = runner._step_extract_intents(state)
        assert result is None
        assert len(state.intents) == 2

    def test_none_values_filtered_out(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state.decide_result = [intent, None, None]

        result = runner._step_extract_intents(state)
        assert result is None
        assert state.intents == [intent]

    def test_all_none_collapses_to_hold(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.decide_result = [None, None]

        result = runner._step_extract_intents(state)
        assert result is not None
        assert result.status == IterationStatus.HOLD
        # Without a hold intent, the fallback message is used
        assert state.intents == []

    def test_hold_with_critical_market_data_failures_escalates_to_data_error(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        hold = HoldIntent(reason="warming up baseline")
        state.decide_result = [hold]

        market = MagicMock()
        market.has_critical_data_failures.return_value = True
        market.classify_critical_data_failures.return_value = "permanent"
        market.summarize_critical_data_failures.return_value = (
            "price(USD/USD@bsc): unknown token USD; balance(BTC): unknown token BTC"
        )
        state.market = market

        result = runner._step_extract_intents(state)
        assert result is not None
        assert result.status == IterationStatus.DATA_ERROR
        assert result.intent == hold
        assert result.error is not None
        assert "classification=permanent" in result.error
        assert "Critical market-data failures" in result.error
        assert "unknown token USD" in result.error

    def test_no_action_with_critical_market_data_failures_escalates_to_data_error(self) -> None:
        """Empty decide result (no HoldIntent) with failures also escalates."""
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.decide_result = []  # No intents at all

        market = MagicMock()
        market.has_critical_data_failures.return_value = True
        market.classify_critical_data_failures.return_value = "transient"
        market.summarize_critical_data_failures.return_value = "price(ETH/USD@arbitrum): timeout"
        state.market = market

        result = runner._step_extract_intents(state)
        assert result is not None
        assert result.status == IterationStatus.DATA_ERROR
        assert result.intent is None
        assert result.error is not None
        assert "classification=transient" in result.error
        assert "Critical market-data failures" in result.error


# =============================================================================
# _step_log_intents
# =============================================================================


class TestStepLogIntents:
    def test_single_intent_logs_without_raising(self, caplog: pytest.LogCaptureFixture) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state.intents = [intent]

        with caplog.at_level("INFO", logger="almanak.framework.runner.strategy_runner"):
            # Drop any records captured before this block (e.g. the
            # StrategyRunner.__init__ "initialized" log emitted by _make_runner)
            # so the assertion counts only records emitted by the helper under test.
            caplog.clear()
            result = runner._step_log_intents(state)

        # Helper is side-effect-only and returns None
        assert result is None
        # Single-intent path emits exactly one INFO line mentioning the deployment id.
        info_records = _strategy_runner_info_records(caplog)
        assert len(info_records) == 1
        assert strategy.deployment_id in info_records[0].message
        assert "intent:" in info_records[0].message
        # state.intents must be untouched by a logging helper
        assert state.intents == [intent]

    def test_multi_intent_logs_each_step(self, caplog: pytest.LogCaptureFixture) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        intents = [
            SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1")),
            SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1")),
        ]
        state.intents = list(intents)

        with caplog.at_level("INFO", logger="almanak.framework.runner.strategy_runner"):
            # Drop any records captured before this block (see single-intent test).
            caplog.clear()
            result = runner._step_log_intents(state)

        assert result is None
        info_records = _strategy_runner_info_records(caplog)
        # One header line plus one per-step line — regression guard against
        # a refactor that accidentally skips the per-step loop body.
        assert len(info_records) == 1 + len(intents)
        assert f"intent sequence ({len(intents)} steps)" in info_records[0].message
        # Per-step lines are numbered 1..N
        assert info_records[1].message.strip().startswith("1.")
        assert info_records[2].message.strip().startswith("2.")
        # state.intents preserved by the logging helper
        assert state.intents == intents

    def test_empty_intents_does_not_raise(self, caplog: pytest.LogCaptureFixture) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = []

        with caplog.at_level("INFO", logger="almanak.framework.runner.strategy_runner"):
            # Drop any records captured before this block (see single-intent test).
            caplog.clear()
            result = runner._step_log_intents(state)

        # With no intents, the helper takes the else-branch and emits the
        # sequence-header log line with a "0 steps" count but no per-step lines.
        assert result is None
        info_records = _strategy_runner_info_records(caplog)
        assert len(info_records) == 1
        assert "intent sequence (0 steps)" in info_records[0].message
        assert state.intents == []


# =============================================================================
# _step_circuit_breaker_pre_execute - extended
# =============================================================================


class TestStepCircuitBreakerPreExecuteExtended:
    def test_open_breaker_without_any_intents_returns_none_intent_in_result(self) -> None:
        """When state.intents is empty, IterationResult.intent is None."""
        breaker = _tripped_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = []  # unusual but defensive coverage

        result = runner._step_circuit_breaker_pre_execute(state)
        assert result is not None
        assert result.status == IterationStatus.CIRCUIT_BREAKER_OPEN
        assert result.intent is None

    def test_open_breaker_picks_first_intent_for_result(self) -> None:
        breaker = _tripped_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        state = _make_state(strategy)
        first = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        second = SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1"))
        state.intents = [first, second]

        result = runner._step_circuit_breaker_pre_execute(state)
        assert result is not None
        assert result.intent is first

    def test_error_message_contains_reason_from_breaker(self) -> None:
        breaker = _tripped_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = [SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))]

        result = runner._step_circuit_breaker_pre_execute(state)
        assert result is not None
        # Reason must mention the failure count threshold
        assert "Circuit breaker open" in (result.error or "")


# =============================================================================
# _step_snapshot_pre_balances - extended
# =============================================================================


class TestStepSnapshotPreBalancesExtended:
    @pytest.mark.asyncio
    async def test_empty_intents_results_in_empty_maps(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = []

        await runner._step_snapshot_pre_balances(state)

        assert state.pre_balances == {}
        assert state.intent_tokens == []

    @pytest.mark.asyncio
    async def test_duplicate_tokens_across_intents_deduped(self) -> None:
        balance_provider = MagicMock()

        async def _get_balance(token: str) -> SimpleNamespace:
            return SimpleNamespace(balance=Decimal("10"))

        balance_provider.get_balance = _get_balance
        runner = _make_runner(balance_provider=balance_provider)
        strategy = _make_strategy()
        state = _make_state(strategy)
        # Two intents share USDC; dedup must keep a single entry
        state.intents = [
            SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1")),
            SwapIntent(from_token="USDC", to_token="DAI", amount=Decimal("1")),
        ]

        await runner._step_snapshot_pre_balances(state)

        assert set(state.intent_tokens) == {"USDC", "ETH", "DAI"}
        # No duplicates
        assert len(state.intent_tokens) == 3

    @pytest.mark.asyncio
    async def test_single_token_missing_balance_is_skipped(self) -> None:
        """Errors fetching individual token balances do not break the snapshot."""
        call_count = {"n": 0}

        async def _maybe_raise(token: str) -> SimpleNamespace:
            call_count["n"] += 1
            if token == "ETH":
                raise RuntimeError("ETH fetch failed")
            return SimpleNamespace(balance=Decimal("10"))

        balance_provider = MagicMock()
        balance_provider.get_balance = _maybe_raise
        runner = _make_runner(balance_provider=balance_provider)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = [SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))]

        await runner._step_snapshot_pre_balances(state)

        # USDC present; ETH missing because of the error
        assert "USDC" in state.pre_balances
        assert "ETH" not in state.pre_balances
        # intent_tokens still tracks both so deltas skip ETH later
        assert set(state.intent_tokens) == {"USDC", "ETH"}


# =============================================================================
# _step_execute dispatcher - extended
# =============================================================================


class TestStepExecuteDispatchExtended:
    @pytest.mark.asyncio
    async def test_single_chain_path_passes_state_reference(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = [SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))]

        with patch.object(runner, "_run_single_chain_intents", new=AsyncMock(return_value="result")) as mock:
            await runner._step_execute(state)

        # Called with the exact state object (not a copy)
        mock.assert_awaited_once_with(state)

    @pytest.mark.asyncio
    async def test_multi_chain_path_forwards_start_time_and_market(self) -> None:
        runner = _make_runner()
        runner._is_multi_chain = True
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.intents = [SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))]
        state.market = MagicMock()

        with patch.object(runner, "_execute_multi_chain", new=AsyncMock(return_value="r")) as mock:
            await runner._step_execute(state)

        mock.assert_awaited_once()
        kwargs = mock.await_args.kwargs
        assert kwargs["start_time"] is state.start_time
        assert kwargs["market"] is state.market


# =============================================================================
# _resolve_chained_amount_for_intent - extended
# =============================================================================


class TestResolveChainedAmountExtended:
    def test_first_intent_of_multi_intent_with_no_previous_falls_to_wallet(self) -> None:
        """idx==0 bypasses the 'no previous output' branch and goes to wallet-balance path."""
        runner = _make_runner()
        strategy = _make_strategy()
        first = SwapIntent(from_token="USDC", to_token="ETH", amount="all")
        second = SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1"))

        market = MagicMock()
        market.balance.return_value = SimpleNamespace(balance=Decimal("100"))

        resolved, early, should_continue = runner._resolve_chained_amount_for_intent(
            intent=first,
            idx=0,
            intents=[first, second],
            is_multi_intent=True,
            previous_amount_received=None,
            market=market,
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert early is None
        assert should_continue is False
        # Wallet-balance resolution was used (intent rewritten)
        assert resolved is not first

    def test_lp_close_intent_does_not_resolve_from_wallet(self) -> None:
        """Protocol-position intents (LP_CLOSE, WITHDRAW, REPAY...) pass through to compiler."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = LPCloseIntent(position_id="1234")

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
        # LP_CLOSE does not use amount="all" chaining; pass through unchanged
        assert resolved is intent
        assert early is None
        assert should_continue is False


class TestResolveChainedAmountFromWalletExtended:
    def test_wallet_funded_intent_with_no_market_fails(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")

        resolved, early, should_continue = runner._resolve_chained_amount_from_wallet(
            intent=intent,
            market=None,  # no market context
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert early is not None
        assert early.status == IterationStatus.COMPILATION_FAILED
        assert "no market context available" in (early.error or "")
        assert should_continue is False

    def test_lp_open_intent_without_wallet_token_field_passes_through(self) -> None:
        """LP_OPEN is wallet-funded but exposes no single from_token/token -> pass through."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = LPOpenIntent(
            pool="USDC/ETH",
            amount0=Decimal("100"),
            amount1=Decimal("1"),
            range_lower=Decimal("1000"),
            range_upper=Decimal("2000"),
            protocol="uniswap_v3",
        )
        market = MagicMock()

        resolved, early, should_continue = runner._resolve_chained_amount_from_wallet(
            intent=intent,
            market=market,
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert early is None
        assert should_continue is False
        # No token field found -> passed through unchanged
        assert resolved is intent

    def test_market_balance_exception_returns_compilation_failed(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")
        market = MagicMock()
        market.balance.side_effect = RuntimeError("gateway down")

        resolved, early, should_continue = runner._resolve_chained_amount_from_wallet(
            intent=intent,
            market=market,
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert early is not None
        assert early.status == IterationStatus.COMPILATION_FAILED
        assert "gateway down" in (early.error or "")
        assert should_continue is False

    def test_market_balance_returns_bare_decimal_not_wrapper(self) -> None:
        """market.balance may return a bare Decimal (no .balance attr)."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")
        market = MagicMock()
        market.balance.return_value = Decimal("123.45")  # bare decimal

        resolved, early, should_continue = runner._resolve_chained_amount_from_wallet(
            intent=intent,
            market=market,
            strategy=strategy,
            start_time=datetime.now(UTC),
        )
        assert early is None
        assert should_continue is False
        assert resolved is not intent  # rewritten


# =============================================================================
# _run_single_chain_intents - extended
# =============================================================================


class TestRunSingleChainIntents:
    @pytest.mark.asyncio
    async def test_single_intent_delegates_to_execute_single_chain(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state.intents = [intent]

        expected = IterationResult(
            status=IterationStatus.SUCCESS,
            intent=intent,
            deployment_id=strategy.deployment_id,
            duration_ms=1,
        )

        with patch.object(runner, "_execute_single_chain", new=AsyncMock(return_value=expected)) as mock_exec:
            result = await runner._run_single_chain_intents(state)

        assert result is expected
        mock_exec.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_multi_intent_stops_on_first_failure(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        intent1 = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        intent2 = SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1"))
        state.intents = [intent1, intent2]

        fail_result = IterationResult(
            status=IterationStatus.EXECUTION_FAILED,
            intent=intent1,
            deployment_id=strategy.deployment_id,
            duration_ms=5,
            error="first failed",
        )

        exec_calls = []

        async def _exec(**kwargs):
            exec_calls.append(kwargs["intent"])
            return fail_result

        with patch.object(runner, "_execute_single_chain", new=AsyncMock(side_effect=_exec)):
            result = await runner._run_single_chain_intents(state)

        assert result.status == IterationStatus.EXECUTION_FAILED
        # Second intent was never executed
        assert len(exec_calls) == 1

    @pytest.mark.asyncio
    async def test_multi_intent_success_invalidates_balance_cache(self) -> None:
        balance_provider = MagicMock()
        balance_provider.invalidate_cache = MagicMock()

        async def _get_balance(token: str) -> SimpleNamespace:
            return SimpleNamespace(balance=Decimal("10"))

        balance_provider.get_balance = _get_balance
        runner = _make_runner(balance_provider=balance_provider)
        strategy = _make_strategy()
        state = _make_state(strategy)
        # Real multi-intent sequence so we exercise the multi-intent branches
        # of _run_single_chain_intents (not the single-intent path).
        intent1 = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        intent2 = SwapIntent(from_token="ETH", to_token="DAI", amount=Decimal("1"))
        state.intents = [intent1, intent2]
        state.pre_balances = {"USDC": Decimal("100"), "ETH": Decimal("0"), "DAI": Decimal("0")}
        state.intent_tokens = ["USDC", "ETH", "DAI"]

        success1 = IterationResult(
            status=IterationStatus.SUCCESS,
            intent=intent1,
            deployment_id=strategy.deployment_id,
            duration_ms=1,
            execution_result=SimpleNamespace(swap_amounts=None),
        )
        success2 = IterationResult(
            status=IterationStatus.SUCCESS,
            intent=intent2,
            deployment_id=strategy.deployment_id,
            duration_ms=1,
            execution_result=SimpleNamespace(swap_amounts=None),
        )

        exec_mock = AsyncMock(side_effect=[success1, success2])
        with patch.object(runner, "_execute_single_chain", new=exec_mock):
            result = await runner._run_single_chain_intents(state)

        # Both intents were executed sequentially
        assert exec_mock.await_count == 2
        # Final result is the second intent's success result
        assert result is success2
        # Balance cache invalidated once after the full multi-intent sequence
        # so post-delta reads are fresh against the post-execution chain state.
        assert balance_provider.invalidate_cache.call_count == 1
