"""Tests for the ``_execute_single_chain`` step helpers extracted in Phase 3c.

Phase 3c split ``StrategyRunner._execute_single_chain`` (CC=118) into a thin
driver plus per-phase step helpers:

* ``_init_single_chain_state`` (runtime-handle setup)
* ``_single_chain_state_machine_loop`` (state-machine drive)
* ``_single_chain_execute_step`` (per-bundle execution with dry-run short-circuit)
* ``_single_chain_pre_retry_confirmed`` (post-timeout retry short-circuit)
* ``_single_chain_slippage_guard`` (realized-slippage circuit-breaker)
* ``_single_chain_handle_recon_incident`` (reconciliation-failure finalizer)
* ``_single_chain_handle_success`` / ``_single_chain_handle_failure``
* static helper ``_build_single_chain_price_oracle``

These tests exercise the small, deterministic pieces of each helper so
regressions in the early-exit / mutation contract surface at unit level.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
)
from almanak.framework.intents.vocabulary import HoldIntent, SwapIntent
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    SingleChainExecutionState,
    StrategyRunner,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_runner(
    *,
    dry_run: bool = False,
    state_manager: MagicMock | None = None,
    balance_provider: MagicMock | None = None,
    execution_orchestrator: MagicMock | None = None,
) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=dry_run,
        max_retries=2,
    )
    if state_manager is None:
        state_manager = MagicMock()
    if balance_provider is None:
        balance_provider = MagicMock()
        balance_provider.invalidate_cache = MagicMock()
    if execution_orchestrator is None:
        execution_orchestrator = MagicMock()
        # Ensure ``getattr(orch, "tx_risk_config", None)`` returns None so tests
        # exercising the slippage path use intent.max_slippage (not a Mock).
        execution_orchestrator.tx_risk_config = None
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=balance_provider,
        execution_orchestrator=execution_orchestrator,
        state_manager=state_manager,
        config=config,
    )


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.strategy_id = "test-strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    return strategy


def _make_state(strategy: MagicMock, *, intent=None) -> SingleChainExecutionState:
    if intent is None:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
    return SingleChainExecutionState(
        strategy=strategy,
        intent=intent,
        start_time=datetime.now(UTC),
        strategy_id=strategy.strategy_id,
    )


# =============================================================================
# _build_single_chain_price_oracle (static helper)
# =============================================================================


class TestBuildSingleChainPriceOracle:
    def test_no_market_returns_none(self) -> None:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        assert StrategyRunner._build_single_chain_price_oracle(None, intent) is None

    def test_market_without_price_oracle_dict_returns_none(self) -> None:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = object()  # no get_price_oracle_dict attr
        assert StrategyRunner._build_single_chain_price_oracle(market, intent) is None

    def test_populated_oracle_returned(self) -> None:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = MagicMock()
        # First call returns populated dict containing both intent tokens
        market.get_price_oracle_dict.return_value = {"USDC": Decimal("1"), "ETH": Decimal("2000")}
        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        assert result == {"USDC": Decimal("1"), "ETH": Decimal("2000")}

    def test_empty_oracle_after_prefetch_returns_none(self) -> None:
        """Regression: an empty oracle dict is coerced to None so the compiler uses placeholders."""
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {}
        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        assert result is None

    def test_prefetches_missing_tokens(self) -> None:
        """Tokens missing from the oracle trigger ``market.price(token)`` pre-fetch calls."""
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = MagicMock()
        # First call returns oracle missing ETH; second call (post pre-fetch) adds ETH
        market.get_price_oracle_dict.side_effect = [
            {"USDC": Decimal("1")},
            {"USDC": Decimal("1"), "ETH": Decimal("2000")},
        ]
        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        assert result == {"USDC": Decimal("1"), "ETH": Decimal("2000")}
        # ETH must have been pre-fetched; USDC was already present
        called = {call.args[0] for call in market.price.call_args_list}
        assert "ETH" in called


# =============================================================================
# _single_chain_pre_retry_confirmed
# =============================================================================


class TestSingleChainPreRetryConfirmed:
    @pytest.mark.asyncio
    async def test_no_prior_timeout_returns_false(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 0  # first attempt -- not a retry

        single_chain_orch = MagicMock()
        assert (
            await runner._single_chain_pre_retry_confirmed(state, single_chain_orch)
            is False
        )

    @pytest.mark.asyncio
    async def test_non_timeout_error_returns_false(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 1
        state.last_execution_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.SIGNING,
            transaction_results=[
                TransactionResult(tx_hash="0xabc", success=False, gas_used=0, gas_cost_wei=0)
            ],
            error="reverted",
        )

        single_chain_orch = MagicMock()
        assert (
            await runner._single_chain_pre_retry_confirmed(state, single_chain_orch)
            is False
        )

    @pytest.mark.asyncio
    async def test_all_prior_confirmed_short_circuits_to_success(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 1
        state.state_machine.set_receipt = MagicMock()
        state.last_execution_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.SUBMISSION,
            transaction_results=[
                TransactionResult(tx_hash="0xdead", success=False, gas_used=0, gas_cost_wei=0)
            ],
            error="timeout waiting for receipt",
        )

        submitted_receipt = SimpleNamespace(
            tx_hash="0xdead", success=True, gas_used=21000, gas_cost_wei=100, logs=[]
        )
        single_chain_orch = MagicMock()
        single_chain_orch.submitter = MagicMock()
        single_chain_orch.submitter.get_receipt = AsyncMock(return_value=submitted_receipt)

        assert (
            await runner._single_chain_pre_retry_confirmed(state, single_chain_orch)
            is True
        )
        # Success ExecutionResult synthesised
        assert state.last_execution_result.success is True
        assert state.last_execution_result.total_gas_used == 21000
        state.state_machine.set_receipt.assert_called_once()

    @pytest.mark.asyncio
    async def test_prior_reverted_tx_does_not_short_circuit(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 1
        state.state_machine.set_receipt = MagicMock()
        original_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.SUBMISSION,
            transaction_results=[
                TransactionResult(tx_hash="0xdead", success=False, gas_used=0, gas_cost_wei=0)
            ],
            error="timeout waiting for receipt",
        )
        state.last_execution_result = original_result

        reverted_receipt = SimpleNamespace(
            tx_hash="0xdead", success=False, gas_used=21000, gas_cost_wei=100, logs=[]
        )
        single_chain_orch = MagicMock()
        single_chain_orch.submitter = MagicMock()
        single_chain_orch.submitter.get_receipt = AsyncMock(return_value=reverted_receipt)

        # Reverted TX -> not all confirmed -> do not short-circuit
        assert (
            await runner._single_chain_pre_retry_confirmed(state, single_chain_orch)
            is False
        )
        # Does not overwrite last_execution_result
        assert state.last_execution_result is original_result
        state.state_machine.set_receipt.assert_not_called()


# =============================================================================
# _single_chain_execute_step (dry-run only -- full path is integration-tested)
# =============================================================================


class TestSingleChainExecuteStepDryRun:
    @pytest.mark.asyncio
    async def test_dry_run_returns_dry_run_iteration_result(self) -> None:
        """In dry-run mode, the step should short-circuit before calling the orchestrator."""
        runner = _make_runner(dry_run=True)
        # Spy on the orchestrator so we can prove dry-run does not call execute().
        runner.execution_orchestrator.execute = AsyncMock()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()

        fake_bundle = MagicMock()
        fake_bundle.transactions = [object(), object()]
        fake_bundle.metadata = {"expected_output_human": "1.0"}
        step_result = MagicMock()
        step_result.action_bundle = fake_bundle

        result = await runner._single_chain_execute_step(state, step_result)
        assert result is not None
        assert result.status == IterationStatus.DRY_RUN
        assert result.intent is state.intent
        # Metadata should have been captured for enrichment (matches pre-refactor behaviour)
        assert state.last_bundle_metadata == {"expected_output_human": "1.0"}
        # Dry-run must short-circuit before reaching the orchestrator.
        runner.execution_orchestrator.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_dry_run_closes_clob_client(self) -> None:
        """Dry-run must still release the ClobClient httpx pool (if one was wired up)."""
        runner = _make_runner(dry_run=True)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.clob_client = MagicMock()

        fake_bundle = MagicMock()
        fake_bundle.transactions = []
        fake_bundle.metadata = None
        step_result = MagicMock()
        step_result.action_bundle = fake_bundle

        result = await runner._single_chain_execute_step(state, step_result)
        assert result is not None
        state.clob_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_dry_run_records_success_when_flag_enabled(self) -> None:
        runner = _make_runner(dry_run=True)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.record_metrics = True

        fake_bundle = MagicMock()
        fake_bundle.transactions = []
        fake_bundle.metadata = None
        step_result = MagicMock()
        step_result.action_bundle = fake_bundle

        pre_total = runner._total_iterations
        pre_success = runner._successful_iterations
        await runner._single_chain_execute_step(state, step_result)
        assert runner._total_iterations == pre_total + 1
        assert runner._successful_iterations == pre_success + 1

    @pytest.mark.asyncio
    async def test_dry_run_skips_metrics_when_flag_disabled(self) -> None:
        runner = _make_runner(dry_run=True)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.record_metrics = False

        fake_bundle = MagicMock()
        fake_bundle.transactions = []
        fake_bundle.metadata = None
        step_result = MagicMock()
        step_result.action_bundle = fake_bundle

        pre_total = runner._total_iterations
        await runner._single_chain_execute_step(state, step_result)
        assert runner._total_iterations == pre_total  # no metric recorded


# =============================================================================
# _single_chain_slippage_guard
# =============================================================================


class TestSingleChainSlippageGuard:
    @pytest.mark.asyncio
    async def test_no_execution_result_returns_none(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.last_execution_result = None

        assert await runner._single_chain_slippage_guard(state) is None

    @pytest.mark.asyncio
    async def test_no_swap_amounts_returns_none(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        # swap_amounts is None by default -- no slippage guard applies
        assert await runner._single_chain_slippage_guard(state) is None

    @pytest.mark.asyncio
    async def test_slippage_within_limit_returns_none(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.05"),  # 500 bps
        )
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(
            slippage_bps=100,  # within 500 bps limit
            token_in="USDC",
            token_out="ETH",
        )

        assert await runner._single_chain_slippage_guard(state) is None

    @pytest.mark.asyncio
    async def test_slippage_breach_returns_execution_failed(self) -> None:
        runner = _make_runner()
        # Track write_ledger / save_state side effects
        runner._write_ledger_entry = AsyncMock()
        runner._emit_execution_timeline_event = MagicMock()

        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),  # 100 bps
        )
        state = _make_state(strategy, intent=intent)
        # Issue #1780: make the metric gate explicit. The single-intent
        # flow enters the guard with record_metrics=True; spell it out so
        # the assertion on _total_iterations below does not depend on a
        # default that could drift.
        state.record_metrics = True
        state.last_execution_result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(
            slippage_bps=200,  # exceeds 100 bps limit
            token_in="USDC",
            token_out="ETH",
        )

        pre_total = runner._total_iterations
        result = await runner._single_chain_slippage_guard(state)
        assert result is not None
        assert result.status == IterationStatus.EXECUTION_FAILED
        assert "Slippage circuit breaker" in result.error
        assert "200" in result.error and "100" in result.error
        # Timeline event and ledger entry must be written with success=False
        runner._emit_execution_timeline_event.assert_called_once()
        assert runner._emit_execution_timeline_event.call_args.kwargs.get("success") is False
        runner._write_ledger_entry.assert_awaited_once()
        assert runner._write_ledger_entry.await_args.kwargs.get("success") is False
        # Strategy callback fired with success=False
        strategy.on_intent_executed.assert_called_once()
        assert strategy.on_intent_executed.call_args.kwargs.get("success") is False
        # Strategy state must be persisted on slippage breach (on-chain state already changed).
        strategy.save_state.assert_called_once()
        # Issue #1780: the slippage-breach iteration counts in the
        # lifetime total when record_metrics=True (single-intent flow),
        # mirroring the ``_record_success`` tick on the success branch.
        assert runner._total_iterations == pre_total + 1

    @pytest.mark.asyncio
    async def test_slippage_breach_multi_intent_defers_metrics(self) -> None:
        """Issue #1780: when state.record_metrics is False (multi-intent
        sequence), the slippage guard must NOT bump ``_total_iterations``.
        The caller (``_run_single_chain_intents``) records once per
        sequence to avoid double-counting.
        """
        runner = _make_runner()
        runner._write_ledger_entry = AsyncMock()
        runner._emit_execution_timeline_event = MagicMock()

        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
        )
        state = _make_state(strategy, intent=intent)
        state.record_metrics = False  # multi-intent sequence
        state.last_execution_result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(
            slippage_bps=200,
            token_in="USDC",
            token_out="ETH",
        )

        pre_total = runner._total_iterations
        result = await runner._single_chain_slippage_guard(state)
        assert result is not None
        assert result.status == IterationStatus.EXECUTION_FAILED
        # Multi-intent: the caller will record, so this helper must not.
        assert runner._total_iterations == pre_total

    @pytest.mark.asyncio
    async def test_slippage_breach_sets_error_before_emitting_timeline(self) -> None:
        """Regression for issue #1649.

        The timeline event description is built from ``result.error`` at the
        moment of emission. If the slippage-breach error is assigned AFTER
        the event fires, consumers (UI, operator cards, Slack alerts) see
        "Unknown error" instead of the real reason. Assert that at the time
        ``_emit_execution_timeline_event`` is called the ``last_execution_result.error``
        already contains the slippage-circuit-breaker message.
        """
        runner = _make_runner()
        runner._write_ledger_entry = AsyncMock()

        # Capture the error value on the result at call time (not at assertion
        # time, which is after the error is set either way).
        captured: dict[str, str | None] = {}

        def _capture(strategy, intent, *, success, result):  # noqa: ANN001
            captured["error"] = getattr(result, "error", None)
            captured["success"] = success

        runner._emit_execution_timeline_event = MagicMock(side_effect=_capture)

        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),  # 100 bps
        )
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(
            slippage_bps=200,  # exceeds 100 bps limit
            token_in="USDC",
            token_out="ETH",
        )

        result = await runner._single_chain_slippage_guard(state)
        assert result is not None
        # Timeline event fired with success=False and error already populated
        assert captured["success"] is False
        assert captured["error"] is not None
        assert "Slippage circuit breaker" in captured["error"]
        assert "200" in captured["error"] and "100" in captured["error"]
