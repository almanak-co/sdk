"""Tests for mode-aware persistence on the copy-trading and vault lanes.

Contract (blueprint 27 failure-mode table):
- live: a failed durable state write raises AccountingPersistenceError
  (write_kind="copy_state" or "vault_state"); the run loop / run_iteration
  escalates to ACCOUNTING_FAILED.
- paper / dry_run: failures log ERROR and the loop continues.
- StateConflictError (CAS) logs a distinct identity-collision message in
  ALL modes (same contract as the STATE lane).

This file mirrors the structural pattern from
tests/unit/runner/test_update_state_persistence.py — same _Runner
bypass-__init__ helper, same RunnerConfig(dry_run=...) mode selection, same
structlog logger-monkeypatch idiom (runner_state uses structlog, not stdlib
logging, so stdlib caplog cannot capture its output; monkeypatch the
module-level logger and assert on call_args_list).
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.runner_models import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
)
from almanak.framework.runner.runner_state import (
    persist_copy_trading_state,
    persist_vault_state,
)
from almanak.framework.runner.strategy_runner import StrategyRunner
from almanak.framework.state.exceptions import (
    AccountingPersistenceError,
    AccountingWriteKind,
)
from almanak.framework.state.state_manager import StateConflictError, StateData


# =============================================================================
# Shared helpers
# =============================================================================


class _Runner(StrategyRunner):
    """Bypass StrategyRunner.__init__ -- only the attrs the persist helpers touch."""

    def __init__(self, *, state_manager: Any, config: RunnerConfig | None = None) -> None:
        self.state_manager = state_manager
        self.config = config or RunnerConfig()


def _state_mgr(*, load_return=None, save_side_effect: Exception | None = None) -> MagicMock:
    mgr = MagicMock()
    mgr.load_state = AsyncMock(
        return_value=load_return
        if load_return is not None
        else StateData(deployment_id="d1", version=3, state={})
    )
    mgr.save_state = AsyncMock(side_effect=save_side_effect)
    return mgr


# =============================================================================
# Unit-level: persist_copy_trading_state
# =============================================================================


class TestPersistCopyTradingState:
    """Unit tests for runner_state.persist_copy_trading_state."""

    def _activity_provider(self) -> MagicMock:
        provider = MagicMock()
        provider.get_state.return_value = {"cursor:arbitrum": {"last_processed_block": 5}}
        return provider

    @pytest.mark.asyncio
    async def test_happy_path_saves_with_cas_and_writes_key(self) -> None:
        """Test 1: saves with expected_version=3 and writes the copy_trading_state key."""
        mgr = _state_mgr()
        runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=False))
        provider = self._activity_provider()

        await persist_copy_trading_state(runner, "d1", provider)

        mgr.save_state.assert_awaited_once()
        saved = mgr.save_state.await_args.args[0]
        assert mgr.save_state.await_args.kwargs["expected_version"] == 3
        assert saved.state["copy_trading_state"] == {"cursor:arbitrum": {"last_processed_block": 5}}

    @pytest.mark.asyncio
    async def test_load_state_none_returns_without_save_even_in_live(self) -> None:
        """Test 2: load_state returns None -> returns without calling save_state, even in live mode."""
        mgr = _state_mgr(load_return=None)
        mgr.load_state = AsyncMock(return_value=None)
        runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=False))
        provider = self._activity_provider()

        await persist_copy_trading_state(runner, "d1", provider)  # must NOT raise

        mgr.save_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_live_save_failure_raises_typed_error(self) -> None:
        """Test 3: live + save_state RuntimeError -> raises AccountingPersistenceError with COPY_STATE."""
        err = RuntimeError("database is locked")
        mgr = _state_mgr(save_side_effect=err)
        runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=False))
        provider = self._activity_provider()

        with pytest.raises(AccountingPersistenceError) as exc_info:
            await persist_copy_trading_state(runner, "d1", provider)

        assert exc_info.value.write_kind == AccountingWriteKind.COPY_STATE
        assert exc_info.value.deployment_id == "d1"
        assert exc_info.value.cause is err

    @pytest.mark.asyncio
    async def test_dry_run_save_failure_logs_error_and_continues(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test 4: dry-run + same failure -> no raise; ERROR log contains 'Failed to persist copy trading state'."""
        from almanak.framework.runner import runner_state

        captured_logger = MagicMock()
        monkeypatch.setattr(runner_state, "logger", captured_logger)

        mgr = _state_mgr(save_side_effect=RuntimeError("database is locked"))
        runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=True))
        provider = self._activity_provider()

        await persist_copy_trading_state(runner, "d1", provider)  # must NOT raise

        assert captured_logger.error.called, (
            "expected ERROR log, got: " + repr(captured_logger.error.call_args_list)
        )
        error_messages = [
            str(call.args[0]) if call.args else "" for call in captured_logger.error.call_args_list
        ]
        assert any("Failed to persist copy trading state" in m for m in error_messages), (
            f"expected 'Failed to persist copy trading state' in error log, got: {error_messages}"
        )

    @pytest.mark.asyncio
    async def test_live_cas_conflict_raises_and_logs_identity_collision(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test 5: live + StateConflictError -> raises typed error + ERROR log with collision signal."""
        from almanak.framework.runner import runner_state

        captured_logger = MagicMock()
        monkeypatch.setattr(runner_state, "logger", captured_logger)

        conflict = StateConflictError(deployment_id="d1", expected_version=3, actual_version=5)
        mgr = _state_mgr(save_side_effect=conflict)
        runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=False))
        provider = self._activity_provider()

        with pytest.raises(AccountingPersistenceError) as exc_info:
            await persist_copy_trading_state(runner, "d1", provider)

        assert exc_info.value.write_kind == AccountingWriteKind.COPY_STATE
        assert exc_info.value.cause is conflict

        assert captured_logger.error.called, (
            "expected ERROR log for CAS conflict, got: " + repr(captured_logger.error.call_args_list)
        )
        error_messages = [
            str(call.args[0]) if call.args else "" for call in captured_logger.error.call_args_list
        ]
        assert any("State version conflict" in m for m in error_messages), (
            f"expected 'State version conflict' in error log, got: {error_messages}"
        )
        assert any("deployment-identity" in m for m in error_messages), (
            f"expected 'deployment-identity' in error log, got: {error_messages}"
        )

    @pytest.mark.asyncio
    async def test_non_live_cas_conflict_logs_without_raising(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test 6: non-live + CAS conflict -> no raise, ERROR log contains 'State version conflict'."""
        from almanak.framework.runner import runner_state

        captured_logger = MagicMock()
        monkeypatch.setattr(runner_state, "logger", captured_logger)

        conflict = StateConflictError(deployment_id="d1", expected_version=3, actual_version=5)
        mgr = _state_mgr(save_side_effect=conflict)
        runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=True))
        provider = self._activity_provider()

        await persist_copy_trading_state(runner, "d1", provider)  # must NOT raise

        assert captured_logger.error.called, (
            "expected ERROR log for CAS conflict, got: " + repr(captured_logger.error.call_args_list)
        )
        error_messages = [
            str(call.args[0]) if call.args else "" for call in captured_logger.error.call_args_list
        ]
        assert any("State version conflict" in m for m in error_messages), (
            f"expected 'State version conflict' in error log, got: {error_messages}"
        )


# =============================================================================
# Unit-level: persist_vault_state
# =============================================================================

_VAULT_DICT = {"settlement_phase": "proposed", "last_settlement_epoch": 2}
_VAULT_KEY = "vault_state"


class TestPersistVaultState:
    """Unit tests for runner_state.persist_vault_state."""

    @pytest.mark.asyncio
    async def test_happy_path_existing_row_saves_with_cas(self) -> None:
        """Test 7: saves with expected_version=3 and writes the vault_state key."""
        mgr = _state_mgr()
        runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=False))

        await persist_vault_state(runner, "d1", _VAULT_DICT, _VAULT_KEY)

        mgr.save_state.assert_awaited_once()
        saved = mgr.save_state.await_args.args[0]
        assert mgr.save_state.await_args.kwargs["expected_version"] == 3
        assert saved.state[_VAULT_KEY] == _VAULT_DICT

    @pytest.mark.asyncio
    async def test_load_state_none_creates_fresh_state_and_saves_with_no_version(self) -> None:
        """Test 8: load_state returns None -> creates StateData(version=1) and saves with expected_version=None."""
        mgr = MagicMock()
        mgr.load_state = AsyncMock(return_value=None)
        mgr.save_state = AsyncMock()
        runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=False))

        await persist_vault_state(runner, "d1", _VAULT_DICT, _VAULT_KEY)

        mgr.save_state.assert_awaited_once()
        saved = mgr.save_state.await_args.args[0]
        assert mgr.save_state.await_args.kwargs["expected_version"] is None
        assert isinstance(saved, StateData)
        assert saved.state[_VAULT_KEY] == _VAULT_DICT

    @pytest.mark.asyncio
    async def test_live_save_failure_raises_typed_error(self) -> None:
        """Test 9: live + save_state RuntimeError -> raises AccountingPersistenceError with VAULT_STATE."""
        err = RuntimeError("database is locked")
        mgr = _state_mgr(save_side_effect=err)
        runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=False))

        with pytest.raises(AccountingPersistenceError) as exc_info:
            await persist_vault_state(runner, "d1", _VAULT_DICT, _VAULT_KEY)

        assert exc_info.value.write_kind == AccountingWriteKind.VAULT_STATE
        assert exc_info.value.deployment_id == "d1"
        assert exc_info.value.cause is err

    @pytest.mark.asyncio
    async def test_dry_run_save_failure_logs_error_and_continues(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test 10: dry-run + same failure -> no raise; ERROR log contains 'Failed to persist vault state'."""
        from almanak.framework.runner import runner_state

        captured_logger = MagicMock()
        monkeypatch.setattr(runner_state, "logger", captured_logger)

        mgr = _state_mgr(save_side_effect=RuntimeError("database is locked"))
        runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=True))

        await persist_vault_state(runner, "d1", _VAULT_DICT, _VAULT_KEY)  # must NOT raise

        assert captured_logger.error.called, (
            "expected ERROR log, got: " + repr(captured_logger.error.call_args_list)
        )
        error_messages = [
            str(call.args[0]) if call.args else "" for call in captured_logger.error.call_args_list
        ]
        assert any("Failed to persist vault state" in m for m in error_messages), (
            f"expected 'Failed to persist vault state' in error log, got: {error_messages}"
        )


# =============================================================================
# Loop-level: copy-trading lane escalates to ACCOUNTING_FAILED
# =============================================================================


def _make_loop_runner() -> StrategyRunner:
    """Trimmed run-loop harness, adapted from test_update_state_persistence._make_loop_runner."""
    config = RunnerConfig(
        default_interval_seconds=0,
        max_consecutive_errors=10,
        enable_state_persistence=True,
        enable_alerting=False,
    )
    state_mgr = AsyncMock()
    state_mgr.get_accounting_events_sync = MagicMock(return_value=[])
    state_mgr.get_position_events_sync = MagicMock(return_value=[])
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_mgr,
        alert_manager=MagicMock(),
        config=config,
    )
    runner._register_with_gateway = MagicMock()
    runner._deregister_from_gateway = MagicMock()
    runner._gateway_heartbeat = MagicMock()
    runner._gateway_update_status = MagicMock()
    runner._get_gateway_client = MagicMock(return_value=None)
    runner._recover_incomplete_sessions = AsyncMock(return_value=0)
    runner._lifecycle_write_state = MagicMock()
    runner._lifecycle_heartbeat = MagicMock()
    runner._lifecycle_poll_command = MagicMock(return_value=None)
    runner._lifecycle_handle_stop = MagicMock()
    runner._collect_position_snapshot = MagicMock(return_value=None)
    runner._capture_portfolio_snapshot = AsyncMock(return_value=None)
    runner._alert_accounting_failure = AsyncMock()
    return runner


def _make_loop_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "test-strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.config = {}
    # _wallet_activity_provider must exist so run_loop picks it up for copy-trading persist
    strategy._wallet_activity_provider = MagicMock()
    del strategy.flush_pending_saves
    return strategy


class TestCopyTradingLaneLoopEscalation:
    """Loop-level tests: copy-lane AccountingPersistenceError escalates to ACCOUNTING_FAILED."""

    @pytest.mark.asyncio
    async def test_live_failure_escalates_accounting_failed_with_timestamp_preservation(
        self,
    ) -> None:
        """Test 11: live copy-lane failure -> ACCOUNTING_FAILED, correct write_kind in error,
        _alert_accounting_failure awaited once, timestamp preserved.

        Also verifies ordering: _update_state receives the REBUILT result
        (status=ACCOUNTING_FAILED), proving it is called AFTER the copy-trading
        persist and its rebuild handler.  A SUCCESS status here would mean
        _update_state ran before the rebuild — i.e. the pre-fix bug.
        """
        runner = _make_loop_runner()
        runner._update_state = AsyncMock()  # state lane succeeds
        runner._persist_copy_trading_state = AsyncMock(
            side_effect=AccountingPersistenceError(
                AccountingWriteKind.COPY_STATE, deployment_id="test-strategy"
            )
        )
        strategy = _make_loop_strategy()
        captured: list[IterationResult] = []
        original = IterationResult(
            status=IterationStatus.SUCCESS, deployment_id="test-strategy", duration_ms=10.0
        )
        runner.run_iteration = AsyncMock(return_value=original)

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                iteration_callback=captured.append,
                max_iterations=1,
            ),
            timeout=5,
        )

        assert len(captured) == 1
        assert captured[0].status == IterationStatus.ACCOUNTING_FAILED
        assert "copy_state" in (captured[0].error or "")
        assert runner._alert_accounting_failure.await_count == 1
        assert captured[0].timestamp == original.timestamp

        # Ordering assertion: _update_state must have been called with the
        # REBUILT result (ACCOUNTING_FAILED), not the pre-failure original
        # (SUCCESS).  _update_state is called as (deployment_id, result,
        # strategy=...) so args[1] is the result object.
        runner._update_state.assert_awaited_once()
        update_state_result_arg = runner._update_state.await_args.args[1]
        assert update_state_result_arg.status == IterationStatus.ACCOUNTING_FAILED, (
            f"_update_state received status={update_state_result_arg.status!r}; "
            "expected ACCOUNTING_FAILED — copy-trading persist must run and rebuild "
            "result BEFORE the iteration-state write (ordering regression)"
        )

    @pytest.mark.asyncio
    async def test_summary_reflects_copy_lane_rebuild(self) -> None:
        """Test 12: _emit_iteration_summary receives the ACCOUNTING_FAILED result (mirrors #1782 invariant)."""
        runner = _make_loop_runner()
        runner._update_state = AsyncMock()
        runner._persist_copy_trading_state = AsyncMock(
            side_effect=AccountingPersistenceError(
                AccountingWriteKind.COPY_STATE, deployment_id="test-strategy"
            )
        )
        strategy = _make_loop_strategy()
        captured: list[IterationResult] = []
        summary_calls: list[IterationResult] = []
        runner._emit_iteration_summary = MagicMock(
            side_effect=lambda result, chain=None: summary_calls.append(result)
        )
        runner.run_iteration = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.SUCCESS, deployment_id="test-strategy", duration_ms=10.0
            )
        )

        await asyncio.wait_for(
            runner.run_loop(
                strategy,
                interval_seconds=0,
                iteration_callback=captured.append,
                max_iterations=1,
            ),
            timeout=5,
        )

        assert len(summary_calls) == 1
        assert summary_calls[0].status == IterationStatus.ACCOUNTING_FAILED
        assert summary_calls[0] is captured[0]


# =============================================================================
# Iteration-level: vault lane escalates through run_iteration's exception handler
# =============================================================================


def _make_vault_runner(*, save_side_effect: Exception | None = None) -> StrategyRunner:
    """Vault-lifecycle iteration harness, adapted from test_strategy_runner_vault._make_runner."""
    from almanak.framework.state.state_manager import StateData

    state_manager = MagicMock()
    state_manager.load_state = AsyncMock(
        return_value=StateData(
            deployment_id="test_vault_strategy", version=1, state={"is_paused": False}
        )
    )
    state_manager.save_state = AsyncMock(side_effect=save_side_effect)

    vault_lifecycle = MagicMock()
    vault_lifecycle.pre_decide_hook.return_value = __import__(
        "almanak.framework.vault.config", fromlist=["VaultAction"]
    ).VaultAction.HOLD
    vault_lifecycle.get_vault_state_dict.return_value = {
        "settlement_phase": "proposed",
        "last_settlement_epoch": 2,
    }

    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_manager,
        vault_lifecycle=vault_lifecycle,
    )


def _make_vault_strategy() -> MagicMock:
    from almanak.framework.intents.vocabulary import HoldIntent

    strategy = MagicMock()
    strategy.deployment_id = "test_vault_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0xWALLET"
    strategy.decide.return_value = HoldIntent(reason="No action")
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.create_market_snapshot.return_value.has_critical_data_failures.return_value = False
    strategy.should_teardown.return_value = False
    return strategy


class TestVaultLaneIterationEscalation:
    """Iteration-level tests for vault state persistence failure semantics."""

    @pytest.mark.asyncio
    async def test_live_vault_persist_failure_produces_accounting_failed(self) -> None:
        """Test 13: live + save_state RuntimeError in vault path -> run_iteration returns ACCOUNTING_FAILED
        and 'vault_state' appears in result.error."""
        runner = _make_vault_runner(save_side_effect=RuntimeError("disk full"))
        strategy = _make_vault_strategy()

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.ACCOUNTING_FAILED
        assert "vault_state" in (result.error or ""), (
            f"expected 'vault_state' in result.error, got: {result.error!r}"
        )

    @pytest.mark.asyncio
    async def test_dry_run_vault_persist_failure_continues_to_hold(self) -> None:
        """Test 13 variant: dry_run + failing save_state -> run_iteration proceeds (result is HOLD, not ACCOUNTING_FAILED)."""
        from almanak.framework.state.state_manager import StateData

        state_manager = MagicMock()
        state_manager.load_state = AsyncMock(
            return_value=StateData(
                deployment_id="test_vault_strategy", version=1, state={"is_paused": False}
            )
        )
        state_manager.save_state = AsyncMock(side_effect=RuntimeError("disk full"))

        vault_lifecycle = MagicMock()
        vault_lifecycle.pre_decide_hook.return_value = __import__(
            "almanak.framework.vault.config", fromlist=["VaultAction"]
        ).VaultAction.HOLD
        vault_lifecycle.get_vault_state_dict.return_value = {
            "settlement_phase": "proposed",
            "last_settlement_epoch": 2,
        }

        runner = StrategyRunner(
            price_oracle=MagicMock(),
            balance_provider=MagicMock(),
            execution_orchestrator=MagicMock(),
            state_manager=state_manager,
            vault_lifecycle=vault_lifecycle,
            config=RunnerConfig(dry_run=True),
        )
        strategy = _make_vault_strategy()

        result = await runner.run_iteration(strategy)

        # Dry-run: persist failure logs ERROR but does not halt the iteration.
        assert result.status != IterationStatus.ACCOUNTING_FAILED, (
            f"dry-run should continue past vault persist failure, got status={result.status}"
        )

    @pytest.mark.asyncio
    async def test_unwind_safety_cancelled_error_not_suppressed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test 14: CancelledError during settlement + live-mode vault persist failure ->
        the CancelledError propagates (not replaced by AccountingPersistenceError).
        The 'unwinding' guard prevents the finally-block raise from masking the cancellation.
        The strategy_runner logger records the 'unwinding' ERROR message.
        """
        from almanak.framework.runner import strategy_runner as sr_module
        from almanak.framework.state.state_manager import StateData
        from almanak.framework.vault.config import VaultAction

        captured_logger = MagicMock()
        monkeypatch.setattr(sr_module, "logger", captured_logger)

        state_manager = MagicMock()
        state_manager.load_state = AsyncMock(
            return_value=StateData(
                deployment_id="test_vault_strategy", version=1, state={"is_paused": False}
            )
        )
        # save_state raises RuntimeError in live mode -> would raise AccountingPersistenceError
        state_manager.save_state = AsyncMock(side_effect=RuntimeError("disk full"))

        vault_lifecycle = MagicMock()
        # Return SETTLE so settlement cycle is triggered
        vault_lifecycle.pre_decide_hook.return_value = VaultAction.SETTLE
        vault_lifecycle.run_settlement_cycle = AsyncMock(
            side_effect=asyncio.CancelledError()
        )
        vault_lifecycle.get_vault_state_dict.return_value = {
            "settlement_phase": "proposed",
            "last_settlement_epoch": 2,
        }

        runner = StrategyRunner(
            price_oracle=MagicMock(),
            balance_provider=MagicMock(),
            execution_orchestrator=MagicMock(),
            state_manager=state_manager,
            vault_lifecycle=vault_lifecycle,
            # default RunnerConfig -> live mode
        )

        # Build a RunIterationState to call _step_periodic_hooks directly
        from almanak.framework.runner.strategy_runner import RunIterationState
        from datetime import datetime, timezone

        strategy = _make_vault_strategy()
        iteration_state = RunIterationState(
            strategy=strategy,
            deployment_id="test_vault_strategy",
            start_time=datetime.now(timezone.utc),
        )

        # CancelledError must propagate -- the AccountingPersistenceError must NOT replace it
        with pytest.raises(asyncio.CancelledError):
            await runner._step_periodic_hooks(iteration_state)

        # The unwinding guard must have logged the ERROR about the persistence failure
        # (not raised it, since an exception was in flight)
        error_messages = [
            str(call.args[0]) if call.args else "" for call in captured_logger.error.call_args_list
        ]
        assert any("unwinding" in m.lower() or "vault state persistence failed" in m.lower() for m in error_messages), (
            f"expected 'unwinding' ERROR log from finally-block guard, got: {error_messages}"
        )
