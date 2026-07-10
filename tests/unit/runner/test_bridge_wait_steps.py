"""Tests for the ``_execute_with_bridge_waiting`` step helpers (Phase 3c).

Phase 3c split ``StrategyRunner._execute_with_bridge_waiting`` (CC=79) into:

* ``_init_bridge_wait_state`` (state provider + progress resolution)
* ``_bridge_wait_process_intent`` (per-intent loop body)
* ``_bridge_wait_cross_chain`` (delegates TX verification + polling)
* ``_bridge_wait_verify_source_tx`` (gateway-only source-TX polling)
* ``_bridge_wait_poll_completion`` (bridge completion poll + failure paths)
* ``_bridge_wait_apply_completion`` (amount normalization + chaining)
* ``_bridge_wait_finalize`` / ``_bridge_wait_build_failed_result``

These tests exercise the helpers via their exposed side effects on
``BridgeWaitState`` so regressions in the break-exit contract surface at
unit level.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.chain_executor import TransactionExecutionResult
from almanak.framework.intents.vocabulary import HoldIntent, SwapIntent
from almanak.framework.runner.runner_models import ExecutionProgress
from almanak.framework.runner.strategy_runner import (
    BridgeWaitState,
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_runner(
    *,
    state_manager: MagicMock | None = None,
    balance_provider: MagicMock | None = None,
) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    if state_manager is None:
        state_manager = MagicMock()
    if balance_provider is None:
        balance_provider = MagicMock()
        balance_provider.invalidate_cache = MagicMock()
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=balance_provider,
        execution_orchestrator=MagicMock(),
        state_manager=state_manager,
        config=config,
    )
    # VIB-5670 Stage 3: bridge-failure paths run the degraded source-REQUEST
    # persistence. Pin non-live (same convention as test_vib5670_stage1.py) so
    # MagicMock persistence backends degrade to logged errors instead of the
    # live-mode fail-closed AccountingPersistenceError.
    runner._is_live_mode = MagicMock(return_value=False)  # type: ignore[method-assign]
    return runner


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "test-strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    return strategy


def _make_orchestrator() -> MagicMock:
    orch = MagicMock()
    orch.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    orch.primary_chain = "arbitrum"
    orch._config = SimpleNamespace(rpc_urls={"arbitrum": "https://arb"})
    orch.execute = AsyncMock()
    return orch


def _make_state(
    *,
    intents: list,
    strategy: MagicMock | None = None,
    orchestrator: MagicMock | None = None,
    resume_progress: ExecutionProgress | None = None,
) -> BridgeWaitState:
    strategy = strategy or _make_strategy()
    orchestrator = orchestrator or _make_orchestrator()
    return BridgeWaitState(
        strategy=strategy,
        intents=intents,
        orchestrator=orchestrator,
        start_time=datetime.now(UTC),
        resume_progress=resume_progress,
        deployment_id=strategy.deployment_id,
        first_intent=intents[0] if intents else None,
    )


def _make_progress(
    deployment_id: str = "test-strategy",
    *,
    total_steps: int = 1,
) -> ExecutionProgress:
    return ExecutionProgress(
        execution_id="abcd",
        deployment_id=deployment_id,
        intents_hash="hash",
        total_steps=total_steps,
    )


# =============================================================================
# _init_bridge_wait_state
# =============================================================================


class TestInitBridgeWaitState:
    @pytest.mark.asyncio
    async def test_fresh_start_saves_initial_progress(self) -> None:
        runner = _make_runner()
        runner._compute_intents_hash = MagicMock(return_value="hash1")
        runner._load_execution_progress = AsyncMock(return_value=None)
        runner._save_execution_progress = AsyncMock()
        runner._clear_execution_progress = AsyncMock()
        runner._get_gateway_client = MagicMock(return_value=None)

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent])

        with patch(
            "almanak.framework.runner.strategy_runner.EnsoStateProvider",
            return_value=MagicMock(),
        ):
            await runner._init_bridge_wait_state(state)

        assert state.start_step_index == 0
        assert state.previous_amount_received is None
        assert state.progress is not None
        assert state.progress.total_steps == 1
        # successful_count starts at start_step_index (0 for fresh start)
        assert state.successful_count == 0
        runner._save_execution_progress.assert_awaited()

    @pytest.mark.asyncio
    async def test_resume_progress_parameter_short_circuits(self) -> None:
        runner = _make_runner()
        runner._compute_intents_hash = MagicMock(return_value="hash1")
        runner._load_execution_progress = AsyncMock()
        runner._save_execution_progress = AsyncMock()
        runner._get_gateway_client = MagicMock(return_value=None)

        resume = _make_progress(total_steps=3)
        resume.completed_step_index = 0  # next step to execute = 1
        resume.previous_amount_received = Decimal("42")

        intent_a = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        intent_b = SwapIntent(from_token="ETH", to_token="USDC", amount=Decimal("1"))
        intent_c = SwapIntent(from_token="USDC", to_token="DAI", amount=Decimal("1"))
        state = _make_state(intents=[intent_a, intent_b, intent_c], resume_progress=resume)

        with patch(
            "almanak.framework.runner.strategy_runner.EnsoStateProvider",
            return_value=MagicMock(),
        ):
            await runner._init_bridge_wait_state(state)

        assert state.start_step_index == 1
        assert state.successful_count == 1
        assert state.previous_amount_received == Decimal("42")
        assert state.progress is resume
        # Fresh-start path must NOT be hit when resume_progress is provided
        runner._load_execution_progress.assert_not_called()
        runner._save_execution_progress.assert_not_called()

    @pytest.mark.asyncio
    async def test_saved_progress_hash_match_resumes(self) -> None:
        runner = _make_runner()
        runner._compute_intents_hash = MagicMock(return_value="hash1")
        saved = _make_progress(total_steps=2)
        saved.intents_hash = "hash1"
        saved.completed_step_index = 0  # start at 1
        saved.previous_amount_received = Decimal("7")
        runner._load_execution_progress = AsyncMock(return_value=saved)
        runner._save_execution_progress = AsyncMock()
        runner._clear_execution_progress = AsyncMock()
        runner._get_gateway_client = MagicMock(return_value=None)

        intents = [
            SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1")),
            SwapIntent(from_token="ETH", to_token="USDC", amount=Decimal("1")),
        ]
        state = _make_state(intents=intents)

        with patch(
            "almanak.framework.runner.strategy_runner.EnsoStateProvider",
            return_value=MagicMock(),
        ):
            await runner._init_bridge_wait_state(state)

        assert state.start_step_index == 1
        assert state.successful_count == 1
        assert state.previous_amount_received == Decimal("7")
        assert state.progress is saved
        # No fresh save when resuming
        runner._save_execution_progress.assert_not_called()
        # Hash-match resume path must preserve saved progress, not clear it
        runner._clear_execution_progress.assert_not_called()

    @pytest.mark.asyncio
    async def test_saved_progress_hash_mismatch_clears_and_restarts(self) -> None:
        runner = _make_runner()
        runner._compute_intents_hash = MagicMock(return_value="new-hash")
        stale = _make_progress(total_steps=2)
        stale.intents_hash = "old-hash"
        runner._load_execution_progress = AsyncMock(return_value=stale)
        runner._clear_execution_progress = AsyncMock()
        runner._save_execution_progress = AsyncMock()
        runner._get_gateway_client = MagicMock(return_value=None)

        state = _make_state(intents=[HoldIntent(reason="x")])

        with patch(
            "almanak.framework.runner.strategy_runner.EnsoStateProvider",
            return_value=MagicMock(),
        ):
            await runner._init_bridge_wait_state(state)

        runner._clear_execution_progress.assert_awaited_once()
        # Fresh progress saved
        runner._save_execution_progress.assert_awaited()
        assert state.start_step_index == 0


# =============================================================================
# _bridge_wait_apply_completion
# =============================================================================


class TestBridgeWaitApplyCompletion:
    @pytest.mark.asyncio
    async def test_none_balance_increase_returns_false(self) -> None:
        """When bridge status omits balance_increase, no break and no state change."""
        runner = _make_runner()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent])
        state.current_intent = intent

        bridge_status = {"status": "completed"}  # no balance_increase
        result = SimpleNamespace(tx_result=SimpleNamespace(tx_hash="0xabc"))

        should_break = await runner._bridge_wait_apply_completion(
            state,
            result=result,
            bridge_status=bridge_status,
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )
        assert should_break is False
        assert state.failed_step is None
        assert state.previous_amount_received is None

    @pytest.mark.asyncio
    async def test_successful_normalization_updates_amount(self) -> None:
        runner = _make_runner()
        # Stub out the normalization helper
        runner._normalize_bridge_balance_increase = MagicMock(
            return_value=(Decimal("9.5"), {"raw_wei": 9500000, "decimals": 6, "resolved_from": "static"})
        )

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent])
        state.current_intent = intent

        bridge_status = {"status": "completed", "balance_increase": 9500000}
        result = SimpleNamespace(tx_result=SimpleNamespace(tx_hash="0xabc"))

        should_break = await runner._bridge_wait_apply_completion(
            state,
            result=result,
            bridge_status=bridge_status,
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )
        assert should_break is False
        assert state.previous_amount_received == Decimal("9.5")
        assert state.failed_step is None

    @pytest.mark.asyncio
    async def test_token_not_found_fails_step_and_fires_callback(self) -> None:
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        runner = _make_runner()
        runner._normalize_bridge_balance_increase = MagicMock(
            side_effect=TokenNotFoundError(token="USDC", chain="base", reason="token metadata missing")
        )

        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.current_intent = intent

        bridge_status = {"status": "completed", "balance_increase": 9500000}
        result = SimpleNamespace(tx_result=TransactionExecutionResult(success=True, tx_hash="0xabc"))

        should_break = await runner._bridge_wait_apply_completion(
            state,
            result=result,
            bridge_status=bridge_status,
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )
        assert should_break is True
        assert state.failed_step == "step-1-bridge"
        assert "token metadata missing" in state.error_message
        # VIB-5670 Stage 3: the callback moved to the degraded source-REQUEST
        # persistence in _bridge_wait_process_intent — not fired inline here.
        assert state.callback_fired is False
        strategy.on_intent_executed.assert_not_called()

        await runner._persist_degraded_bridge_source_leg(state, intent, "arbitrum", result)
        assert state.callback_fired is True
        strategy.on_intent_executed.assert_called_once()
        assert strategy.on_intent_executed.call_args.kwargs.get("success") is False

    @pytest.mark.asyncio
    async def test_unresolvable_decimals_preserves_previous_amount(self) -> None:
        """Normalization returns (None, meta): warn but don't break or mutate amount."""
        runner = _make_runner()
        runner._normalize_bridge_balance_increase = MagicMock(
            return_value=(None, {"raw_wei": 9500000, "decimals": None})
        )

        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent])
        state.current_intent = intent
        state.previous_amount_received = Decimal("123")

        bridge_status = {"status": "completed", "balance_increase": 9500000}
        result = SimpleNamespace(tx_result=SimpleNamespace(tx_hash="0xabc"))

        should_break = await runner._bridge_wait_apply_completion(
            state,
            result=result,
            bridge_status=bridge_status,
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )
        assert should_break is False
        # Pre-existing amount preserved
        assert state.previous_amount_received == Decimal("123")


# =============================================================================
# _bridge_wait_poll_completion -- failure paths
# =============================================================================


class TestBridgeWaitPollCompletionFailures:
    @pytest.mark.asyncio
    async def test_non_completed_status_fails_step(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.current_intent = intent
        state.state_provider = MagicMock()
        state.state_provider.register_bridge_transfer = MagicMock(return_value="deposit-1")
        state.state_provider.wait_for_bridge_completion = AsyncMock(
            return_value={"status": "failed", "error": "bridge reverted"}
        )

        result = SimpleNamespace(tx_result=TransactionExecutionResult(success=True, tx_hash="0xabc"))

        should_break = await runner._bridge_wait_poll_completion(
            state,
            result=result,
            tx_hash="0xabc",
            chain="arbitrum",
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )
        assert should_break is True
        assert state.failed_step == "step-1-bridge"
        assert "bridge reverted" in state.error_message
        # VIB-5670 Stage 3: the callback moved to the degraded source-REQUEST
        # persistence in _bridge_wait_process_intent — not fired inline here.
        assert state.callback_fired is False
        strategy.on_intent_executed.assert_not_called()

        await runner._persist_degraded_bridge_source_leg(state, intent, "arbitrum", result)
        assert state.callback_fired is True
        strategy.on_intent_executed.assert_called_once()
        assert strategy.on_intent_executed.call_args.kwargs.get("success") is False

    @pytest.mark.asyncio
    async def test_timeout_fails_step(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.current_intent = intent
        state.state_provider = MagicMock()
        state.state_provider.register_bridge_transfer = MagicMock(return_value="deposit-1")
        state.state_provider.wait_for_bridge_completion = AsyncMock(
            side_effect=TimeoutError("5 minutes passed")
        )

        result = SimpleNamespace(tx_result=TransactionExecutionResult(success=True, tx_hash="0xabc"))

        should_break = await runner._bridge_wait_poll_completion(
            state,
            result=result,
            tx_hash="0xabc",
            chain="arbitrum",
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )
        assert should_break is True
        assert state.failed_step == "step-1-bridge"
        assert "timed out after 5 minutes" in state.error_message
        # VIB-5670 Stage 3: the callback moved to the degraded source-REQUEST
        # persistence in _bridge_wait_process_intent — not fired inline here.
        assert state.callback_fired is False
        strategy.on_intent_executed.assert_not_called()

        await runner._persist_degraded_bridge_source_leg(state, intent, "arbitrum", result)
        assert state.callback_fired is True
        strategy.on_intent_executed.assert_called_once()
        assert strategy.on_intent_executed.call_args.kwargs.get("success") is False

    @pytest.mark.asyncio
    async def test_non_timeout_exception_still_fires_failure_callback(self) -> None:
        """Regression for #1648.

        Prior to the fix, ``_bridge_wait_poll_completion`` only caught
        ``TimeoutError``. Any other exception raised by
        ``wait_for_bridge_completion`` (connection errors, protocol errors,
        malformed responses, etc.) propagated up and bypassed the
        on_intent_executed callback + callback_fired + failed_step
        bookkeeping, leaving the strategy in an inconsistent state versus
        the orchestrator's view of the in-flight bridge.
        """
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.on_intent_executed = MagicMock()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.current_intent = intent
        state.state_provider = MagicMock()
        state.state_provider.register_bridge_transfer = MagicMock(return_value="deposit-1")
        state.state_provider.wait_for_bridge_completion = AsyncMock(
            side_effect=ConnectionError("gateway unreachable")
        )

        result = SimpleNamespace(tx_result=TransactionExecutionResult(success=True, tx_hash="0xabc"))

        should_break = await runner._bridge_wait_poll_completion(
            state,
            result=result,
            tx_hash="0xabc",
            chain="arbitrum",
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )

        # Must not re-raise: returns True so the outer loop breaks and
        # _bridge_wait_finalize builds a failed IterationResult.
        assert should_break is True
        # Failure bookkeeping must match the TimeoutError path.
        assert state.failed_step == "step-1-bridge"
        # Error message must identify the exception type so ops can
        # distinguish timeout from other bridge errors.
        assert "ConnectionError" in state.error_message
        assert "gateway unreachable" in state.error_message

        # VIB-5670 Stage 3: the strategy notification moved to the degraded
        # source-REQUEST persistence (fired exactly once with the ENRICHED
        # per-leg ExecutionResult, not the raw leg result — design v3 #5).
        assert state.callback_fired is False
        strategy.on_intent_executed.assert_not_called()
        await runner._persist_degraded_bridge_source_leg(state, intent, "arbitrum", result)
        assert state.callback_fired is True
        strategy.on_intent_executed.assert_called_once()
        call_args = strategy.on_intent_executed.call_args
        assert call_args.args[0] is intent
        assert call_args.kwargs.get("success") is False
        notified_result = call_args.kwargs.get("result")
        assert notified_result is not None
        assert notified_result.transaction_results[0].tx_hash == "0xabc"

    @pytest.mark.asyncio
    async def test_callback_exception_in_non_timeout_path_is_swallowed(self) -> None:
        """A failing on_intent_executed callback must not break the failure path."""
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.on_intent_executed = MagicMock(side_effect=RuntimeError("cb exploded"))
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.current_intent = intent
        state.state_provider = MagicMock()
        state.state_provider.register_bridge_transfer = MagicMock(return_value="deposit-1")
        state.state_provider.wait_for_bridge_completion = AsyncMock(
            side_effect=ValueError("malformed bridge response")
        )

        result = SimpleNamespace(tx_result=TransactionExecutionResult(success=True, tx_hash="0xabc"))

        should_break = await runner._bridge_wait_poll_completion(
            state,
            result=result,
            tx_hash="0xabc",
            chain="arbitrum",
            dest_chain="base",
            token_symbol="USDC",
            step_num=1,
        )

        assert should_break is True
        assert state.failed_step == "step-1-bridge"
        assert "ValueError" in state.error_message

        # VIB-5670 Stage 3: the callback fires from the degraded source-REQUEST
        # persistence; a callback that raises must not break the failure path.
        await runner._persist_degraded_bridge_source_leg(state, intent, "arbitrum", result)
        assert state.callback_fired is True
        call_args = strategy.on_intent_executed.call_args
        assert call_args.args[0] is intent
        assert call_args.kwargs.get("success") is False

    @pytest.mark.parametrize("exc_cls", [SystemExit, KeyboardInterrupt])
    @pytest.mark.asyncio
    async def test_base_exceptions_are_not_swallowed(self, exc_cls: type[BaseException]) -> None:
        """`except Exception` must not swallow SystemExit / KeyboardInterrupt.

        These inherit from ``BaseException`` (not ``Exception``) and must
        propagate so a shutdown signal or interpreter exit is never masked
        by the bridge-wait failure pipeline.
        """
        runner = _make_runner()
        strategy = _make_strategy()
        strategy.on_intent_executed = MagicMock()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.current_intent = intent
        state.state_provider = MagicMock()
        state.state_provider.register_bridge_transfer = MagicMock(return_value="deposit-1")
        state.state_provider.wait_for_bridge_completion = AsyncMock(side_effect=exc_cls())

        result = SimpleNamespace(tx_result=SimpleNamespace(tx_hash="0xabc"))

        with pytest.raises(exc_cls):
            await runner._bridge_wait_poll_completion(
                state,
                result=result,
                tx_hash="0xabc",
                chain="arbitrum",
                dest_chain="base",
                token_symbol="USDC",
                step_num=1,
            )

        # BaseException must propagate unchanged. The failure pipeline must
        # NOT have fired, matching the intent of the fix: route regular
        # Exceptions to the failure callback, but never catch shutdown
        # signals.
        assert state.callback_fired is False
        strategy.on_intent_executed.assert_not_called()


# =============================================================================
# _bridge_wait_cross_chain -- pre-poll tx_hash extraction failures
# =============================================================================


class TestBridgeWaitCrossChain:
    @pytest.mark.asyncio
    async def test_missing_tx_hash_fails_step(self) -> None:
        runner = _make_runner()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent])
        state.current_intent = intent

        # tx_result is None -> no tx_hash -> fail
        result = SimpleNamespace(tx_result=None)

        should_break = await runner._bridge_wait_cross_chain(
            state,
            result=result,
            step_num=1,
            chain="arbitrum",
            dest_chain="base",
            token_symbol="USDC",
        )
        assert should_break is True
        assert state.failed_step == "step-1"
        assert "No transaction hash" in state.error_message
        assert state.callback_fired is False
        state.strategy.on_intent_executed.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_0x_prefix_gets_prepended(self) -> None:
        """Tx hashes without an 0x prefix must be normalized before downstream use."""
        runner = _make_runner()
        runner._bridge_wait_verify_source_tx = AsyncMock(return_value=False)
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
        state = _make_state(intents=[intent])
        state.current_intent = intent

        result = SimpleNamespace(tx_result=SimpleNamespace(tx_hash="deadbeef"))

        # With verify returning False, cross_chain should return True (break) and
        # the verify helper must have been called with a 0x-prefixed hash.
        should_break = await runner._bridge_wait_cross_chain(
            state,
            result=result,
            step_num=1,
            chain="arbitrum",
            dest_chain="base",
            token_symbol="USDC",
        )
        assert should_break is True
        runner._bridge_wait_verify_source_tx.assert_awaited_once()
        kwargs = runner._bridge_wait_verify_source_tx.await_args.kwargs
        assert kwargs.get("tx_hash") == "0xdeadbeef"


# =============================================================================
# _bridge_wait_process_intent -- skip/resume and exec failure paths
# =============================================================================


class TestBridgeWaitProcessIntent:
    @pytest.mark.asyncio
    async def test_skip_already_completed_steps(self) -> None:
        runner = _make_runner()
        intent_a = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        intent_b = SwapIntent(from_token="ETH", to_token="USDC", amount=Decimal("1"))
        state = _make_state(intents=[intent_a, intent_b])
        state.start_step_index = 1  # skip index 0

        should_break = await runner._bridge_wait_process_intent(state, 0)
        assert should_break is False
        # No execute call was made
        state.orchestrator.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_orchestrator_exception_fails_step_and_fires_callback(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.orchestrator.execute.side_effect = RuntimeError("rpc blew up")

        should_break = await runner._bridge_wait_process_intent(state, 0)
        assert should_break is True
        assert state.failed_step == "step-1"
        assert "rpc blew up" in state.error_message
        assert state.callback_fired is True
        strategy.on_intent_executed.assert_called_once()

    @pytest.mark.asyncio
    async def test_cross_chain_missing_destination_fields_breaks(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        # Craft a cross-chain SwapIntent whose destination fields resolve to None.
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("1"),
            destination_chain="base",  # makes it cross-chain
        )
        state = _make_state(intents=[intent], strategy=strategy)
        # Gateway client must be set for cross-chain path (fix #1647);
        # otherwise the fail-fast guard raises before the VIB-3223 assertion
        # being exercised here has a chance to run.
        state.gateway_client = MagicMock()

        with (
            patch(
                "almanak.framework.runner.strategy_runner.is_cross_chain_intent",
                return_value=True,
            ),
            patch(
                "almanak.framework.runner.strategy_runner.get_intent_destination_chain",
                return_value=None,
            ),
            patch(
                "almanak.framework.runner.strategy_runner.get_intent_destination_token",
                return_value=None,
            ),
        ):
            should_break = await runner._bridge_wait_process_intent(state, 0)

        assert should_break is True
        assert state.failed_step == "step-1"
        assert "missing destination_chain/to_chain" in state.error_message
        # Orchestrator must not have been called for this defensive validation
        state.orchestrator.execute.assert_not_called()
        # Defensive validation aborts before the deferred callback fires.
        assert state.callback_fired is False
        strategy.on_intent_executed.assert_not_called()

    @pytest.mark.asyncio
    async def test_result_failure_fails_step(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        failed_result = SimpleNamespace(success=False, error="insufficient funds", tx_result=None)
        state.orchestrator.execute.return_value = failed_result

        should_break = await runner._bridge_wait_process_intent(state, 0)
        assert should_break is True
        assert state.failed_step == "step-1"
        assert state.error_message == "insufficient funds"
        assert state.callback_fired is True
        assert state.failed_result is failed_result
        strategy.on_intent_executed.assert_called_once()
        assert strategy.on_intent_executed.call_args.kwargs.get("success") is False


# =============================================================================
# _bridge_wait_finalize / _bridge_wait_build_failed_result
# =============================================================================


class TestBridgeWaitFinalize:
    @pytest.mark.asyncio
    async def test_success_path_records_and_clears_progress(self) -> None:
        runner = _make_runner()
        runner._clear_execution_progress = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()
        # successful_count + no failure = SUCCESS
        state.successful_count = 1

        pre_total = runner._total_iterations
        pre_success = runner._successful_iterations

        result = await runner._bridge_wait_finalize(state)
        assert result.status == IterationStatus.SUCCESS
        assert result.intent is intent
        runner._clear_execution_progress.assert_awaited_once()
        # balance cache invalidated on success path
        runner.balance_provider.invalidate_cache.assert_called()
        # _record_success bumped metrics
        assert runner._total_iterations == pre_total + 1
        assert runner._successful_iterations == pre_success + 1

    @pytest.mark.asyncio
    async def test_failure_path_persists_progress_and_returns_failed(self) -> None:
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()
        runner._clear_execution_progress = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()
        state.failed_step = "step-1"
        state.error_message = "boom"
        state.current_intent = intent
        # RPC map empty -> diagnostics skipped cleanly
        state.rpc_urls = {}

        result = await runner._bridge_wait_finalize(state)
        assert result.status == IterationStatus.EXECUTION_FAILED
        assert "step-1" in result.error
        assert "boom" in result.error
        # Progress saved with failure metadata
        runner._save_execution_progress.assert_awaited()
        assert state.progress.failed_at_step_index == 0
        assert state.progress.failure_error == "boom"
        # Progress NOT cleared on failure (only cleared on success)
        runner._clear_execution_progress.assert_not_called()
        runner.balance_provider.invalidate_cache.assert_called()

    @pytest.mark.asyncio
    async def test_failure_fires_callback_for_break_exits_that_missed_it(self) -> None:
        """Source-TX verification break-exits don't fire inline; finalize must catch them."""
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()
        state.failed_step = "step-1"
        state.error_message = "tx verify timed out"
        state.callback_fired = False  # verification path leaves this False
        state.current_intent = intent
        state.rpc_urls = {}

        await runner._bridge_wait_finalize(state)

        strategy.on_intent_executed.assert_called_once()
        assert strategy.on_intent_executed.call_args.kwargs.get("success") is False

    @pytest.mark.asyncio
    async def test_failure_does_not_refire_callback_when_already_fired(self) -> None:
        runner = _make_runner()
        runner._save_execution_progress = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(intents=[intent], strategy=strategy)
        state.progress = _make_progress()
        state.failed_step = "step-1"
        state.error_message = "boom"
        state.callback_fired = True  # already fired inline
        state.current_intent = intent
        state.rpc_urls = {}

        await runner._bridge_wait_finalize(state)

        strategy.on_intent_executed.assert_not_called()
