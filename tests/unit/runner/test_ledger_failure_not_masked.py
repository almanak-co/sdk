"""VIB-4040 / PRD-TimelineEvents §11 test #5: a successful timeline write
must NEVER mask a live-mode ledger persistence failure.

The Accounting-Timeline epic (VIB-4039) rescopes `timeline_events` as a UX
activity feed. A foreseeable abuse of that boundary is using the timeline
write as a "soft fallback" when the ledger write fails — making operators
think a trade is safely recorded when in fact the money trail is broken.

Invariant under test:

    In live mode, `AccountingPersistenceError` from the ledger writer must
    halt the iteration with `IterationStatus.ACCOUNTING_FAILED`, regardless
    of whether the timeline writer succeeded for the same iteration.

Companion to `test_critical_accounting_error_handling.py` which covers the
sibling `CriticalAccountingError` path.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.api.timeline import TimelineEvent
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.state.exceptions import AccountingPersistenceError


def _make_runner() -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


def _stub_pre_execute_steps(runner: StrategyRunner) -> None:
    runner._step_pause_gate = AsyncMock(return_value=None)  # type: ignore[method-assign]
    runner._step_teardown_and_cb_gate = AsyncMock(return_value=None)  # type: ignore[method-assign]
    runner._step_periodic_hooks = AsyncMock(return_value=None)  # type: ignore[method-assign]
    runner._step_build_snapshot = AsyncMock(return_value=None)  # type: ignore[method-assign]
    runner._step_decide = AsyncMock(return_value=None)  # type: ignore[method-assign]
    runner._step_extract_intents = MagicMock(return_value=None)  # type: ignore[method-assign]
    runner._step_log_intents = MagicMock()  # type: ignore[method-assign]
    runner._step_circuit_breaker_pre_execute = MagicMock(return_value=None)  # type: ignore[method-assign]
    runner._step_snapshot_pre_balances = AsyncMock(return_value=None)  # type: ignore[method-assign]


class TestLedgerFailureNotMaskedByTimeline:
    """A successful timeline write must NOT short-circuit ACCOUNTING_FAILED."""

    @pytest.mark.asyncio
    async def test_ledger_failure_returns_accounting_failed(self) -> None:
        """`AccountingPersistenceError` from the ledger writer surfaces as
        `IterationStatus.ACCOUNTING_FAILED`. This is the baseline contract,
        documented at strategy_runner.py:768-787 (VIB-3157)."""
        runner = _make_runner()
        strategy = MagicMock()
        strategy.deployment_id = "test-strategy"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x" + "ab" * 20

        ledger_error = AccountingPersistenceError(
            "ledger",
            deployment_id="test-strategy",
            message="Synthetic ledger write failure (test)",
        )
        runner._step_execute = AsyncMock(side_effect=ledger_error)  # type: ignore[method-assign]
        _stub_pre_execute_steps(runner)

        result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.ACCOUNTING_FAILED, (
            f"Expected ACCOUNTING_FAILED, got {result.status}. AccountingPersistenceError must halt the iteration."
        )
        assert result.success is False
        assert "ledger" in result.error.lower() or "Accounting persistence" in result.error

    @pytest.mark.asyncio
    async def test_timeline_write_success_does_not_mask_halt(self) -> None:
        """Even when the timeline writer succeeds for the failing iteration,
        the runner still halts with ACCOUNTING_FAILED.

        Concretely, this protects against a future regression where someone
        writes a "soft fallback" path:

            try:
                save_ledger_entry(...)
            except AccountingPersistenceError:
                add_event(TimelineEvent(...))  # nope
                return success_result()

        The contract: timeline events live in their own lane, and a
        successful write there does NOT signal that the books are intact.

        CodeRabbit (PR #2117): exercise the REAL timeline-emission path —
        ``_emit_execution_timeline_event`` → module-level ``add_event`` — so
        a regression in payload sanitization or persistence wiring fails
        loudly instead of slipping through. Stubbing ``_alert_accounting_failure``
        only proves a side effect; it doesn't pin the timeline contract.
        """
        runner = _make_runner()
        strategy = MagicMock()
        strategy.deployment_id = "test-strategy"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x" + "ab" * 20

        ledger_error = AccountingPersistenceError(
            "ledger",
            deployment_id="test-strategy",
            message="Synthetic ledger write failure (test)",
        )

        # Stub ``add_event`` at the runner's import site — this is the real
        # persistence boundary that ``_emit_execution_timeline_event`` calls.
        # Recording the call (instead of letting it write to a DB) keeps the
        # test self-contained while still exercising every line up to and
        # including the persistence dispatch.
        timeline_writes: list[TimelineEvent] = []

        # Simulate a "soft fallback" refactor at the strategy step level —
        # the kind of code path the test guards against. It emits a real
        # timeline event, then raises ``AccountingPersistenceError``. The
        # contract: the iteration MUST still halt with ACCOUNTING_FAILED
        # despite the successful timeline write.
        #
        # CodeRabbit on PR #2117 round 5: production ``_step_execute`` takes
        # ``RunIterationState`` (see ``StrategyRunner._step_execute(state)``
        # at strategy_runner.py:1367) and calls ``_emit_execution_timeline_event``
        # with ``state.strategy`` (NOT ``state`` itself — the emission
        # boundary reads ``strategy.deployment_id`` / ``strategy.chain``). The
        # earlier stub forwarded its first argument straight through, which
        # silently passed the run state object into a parameter typed as
        # the strategy and could mask regressions on strategy-derived
        # fields (e.g. ``strategy.chain`` flowing into the timeline event's
        # chain). Forward ``state.strategy`` explicitly to match the real
        # call shape.
        async def _step_execute_emits_then_fails(state: object) -> None:
            intent = MagicMock()
            intent.intent_type = SimpleNamespace(value="SWAP")
            result = SimpleNamespace(
                position_id=None,
                transaction_results=[],
                total_gas_used=0,
                error="ledger persistence failure (test)",
                extracted_data={},
                lp_close_data=None,
                swap_amounts=None,
            )
            runner._emit_execution_timeline_event(state.strategy, intent, success=False, result=result)
            raise ledger_error

        runner._step_execute = _step_execute_emits_then_fails  # type: ignore[method-assign]
        _stub_pre_execute_steps(runner)

        with patch(
            "almanak.framework.runner.strategy_runner.add_event",
            side_effect=timeline_writes.append,
        ) as add_event_spy:
            result = await runner.run_iteration(strategy)

        assert result.status == IterationStatus.ACCOUNTING_FAILED, (
            "Timeline-write success must not change the halt: a successful "
            "write to the UX activity feed is not evidence that the ledger is "
            f"intact. Got status={result.status}."
        )
        assert result.success is False

        # Real-path assertions: prove the timeline persistence boundary was
        # reached and that what landed there is a sanitized lifecycle marker.
        assert add_event_spy.called, (
            "Real timeline-emission path was NOT exercised — the test is "
            "vacuous. ``add_event`` is the persistence boundary inside "
            "``_emit_execution_timeline_event``."
        )
        assert len(timeline_writes) == 1
        emitted = timeline_writes[0]
        assert isinstance(emitted, TimelineEvent), (
            "Expected a real ``TimelineEvent`` at the persistence boundary; "
            "got something else, which means the emission path diverged."
        )
        # PR4 / PRD-TimelineEvents §6.1: the failure breadcrumb must be
        # money-safe. Pin both the lifecycle marker shape (intent_type +
        # success) and the absence of money-shaped keys so a regression in
        # ``_emit_execution_timeline_event`` shows up here too.
        assert emitted.details == {"intent_type": "SWAP", "success": False}
        for forbidden in ("amount", "amount_in", "amount_out", "gas_used", "slippage_bps"):
            assert forbidden not in emitted.details, (
                f"PR4 violation: ``{forbidden}`` leaked into timeline details on the failure path"
            )
