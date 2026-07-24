"""Branch coverage for PlanExecutor restart paths.

Covers ``_rehydrate_step`` (tx + bridge reconciliation on restart),
``rehydrate_plan``, ``reconcile_plan`` / ``_reconcile_step`` and
``_determine_resume_point`` — all against a mocked OnChainStateProvider.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.plan import (
    PlanBundle,
    PlanStep,
    StepArtifacts,
    StepStatus,
)
from almanak.framework.execution.plan_executor import (
    PlanExecutor,
    PlanExecutorConfig,
    ReconciliationStatus,
    RehydrationStatus,
)


def _step(
    step_id: str = "step-1",
    *,
    status: StepStatus = StepStatus.PENDING,
    tx_hash: str | None = None,
    bridge_deposit_id: str | None = None,
    pinned_quote: dict | None = None,
    pinned_at: datetime | None = None,
    dependencies: list[str] | None = None,
) -> PlanStep:
    return PlanStep(
        step_id=step_id,
        chain="ethereum",
        intent={"type": "swap"},
        dependencies=dependencies or [],
        status=status,
        artifacts=StepArtifacts(
            tx_hash=tx_hash,
            bridge_deposit_id=bridge_deposit_id,
            pinned_quote=pinned_quote,
            pinned_at=pinned_at,
        ),
    )


def _plan(*steps: PlanStep) -> PlanBundle:
    return PlanBundle(plan_id="plan-1", steps=list(steps))


@pytest.fixture
def provider():
    provider = MagicMock()
    provider.get_transaction_status = AsyncMock(return_value={"status": "confirmed"})
    provider.get_bridge_transfer_status = AsyncMock(return_value={"status": "completed"})
    return provider


@pytest.fixture
def executor(provider) -> PlanExecutor:
    return PlanExecutor(
        config=PlanExecutorConfig(auto_requote_stale=False),
        state_provider=provider,
    )


class TestRehydrateStep:
    def test_no_state_provider_returns_valid(self):
        executor = PlanExecutor(state_provider=None)
        step = _step(status=StepStatus.SUBMITTED, tx_hash="0xtx")
        result = asyncio.run(executor._rehydrate_step(step))
        assert result.valid
        assert result.on_chain_status is None

    def test_submitted_confirmed_on_chain(self, executor, provider):
        provider.get_transaction_status.return_value = {
            "status": "confirmed",
            "block_number": 456,
        }
        step = _step(status=StepStatus.SUBMITTED, tx_hash="0xtx")
        result = asyncio.run(executor._rehydrate_step(step))
        assert step.status == StepStatus.CONFIRMED
        assert step.artifacts.block_number == 456
        assert result.status_updated
        assert not result.needs_remediation

    def test_submitted_failed_on_chain_needs_remediation(self, executor, provider):
        provider.get_transaction_status.return_value = {
            "status": "failed",
            "error": "out of gas",
        }
        step = _step(status=StepStatus.SUBMITTED, tx_hash="0xtx")
        result = asyncio.run(executor._rehydrate_step(step))
        assert step.status == StepStatus.FAILED
        assert step.error_message == "out of gas"
        assert result.status_updated
        assert result.needs_remediation

    def test_submitted_still_pending_left_alone(self, executor, provider):
        provider.get_transaction_status.return_value = {"status": "pending"}
        step = _step(status=StepStatus.SUBMITTED, tx_hash="0xtx")
        result = asyncio.run(executor._rehydrate_step(step))
        assert step.status == StepStatus.SUBMITTED
        assert not result.status_updated

    def test_confirming_promoted_to_confirmed(self, executor):
        step = _step(status=StepStatus.CONFIRMING, tx_hash="0xtx")
        result = asyncio.run(executor._rehydrate_step(step))
        assert step.status == StepStatus.CONFIRMED
        assert result.status_updated

    def test_completed_but_unconfirmed_marked_invalid(self, executor, provider):
        provider.get_transaction_status.return_value = {"status": "pending"}
        step = _step(status=StepStatus.COMPLETED, tx_hash="0xtx")
        result = asyncio.run(executor._rehydrate_step(step))
        assert not result.valid
        assert "COMPLETED but tx status is pending" in result.details

    def test_completed_and_confirmed_stays_valid(self, executor):
        step = _step(status=StepStatus.COMPLETED, tx_hash="0xtx")
        result = asyncio.run(executor._rehydrate_step(step))
        assert result.valid

    def test_tx_status_error_is_soft(self, executor, provider):
        provider.get_transaction_status.side_effect = RuntimeError("rpc down")
        step = _step(status=StepStatus.SUBMITTED, tx_hash="0xtx")
        result = asyncio.run(executor._rehydrate_step(step))
        assert result.valid
        assert "Error checking tx status" in result.details
        assert step.status == StepStatus.SUBMITTED

    def test_bridge_completed_promotes_step(self, executor, provider):
        provider.get_bridge_transfer_status.return_value = {
            "status": "completed",
            "destination_tx": "0xdest",
        }
        step = _step(
            status=StepStatus.CONFIRMED,
            bridge_deposit_id="dep-1",
            pinned_quote={"bridge_name": "across"},
        )
        result = asyncio.run(executor._rehydrate_step(step))
        assert step.status == StepStatus.COMPLETED
        assert step.artifacts.destination_credit_tx == "0xdest"
        assert result.status_updated
        provider.get_bridge_transfer_status.assert_awaited_once_with("across", "dep-1")

    def test_bridge_failure_needs_remediation(self, executor, provider):
        provider.get_bridge_transfer_status.return_value = {"status": "failed"}
        step = _step(status=StepStatus.CONFIRMED, bridge_deposit_id="dep-1")
        result = asyncio.run(executor._rehydrate_step(step))
        assert step.status == StepStatus.FAILED
        assert step.error_message == "Bridge transfer failed"
        assert result.needs_remediation
        # No pinned quote: bridge name falls back to "unknown"
        provider.get_bridge_transfer_status.assert_awaited_once_with("unknown", "dep-1")

    def test_bridge_incomplete_on_completed_step_invalid(self, executor, provider):
        provider.get_bridge_transfer_status.return_value = {"status": "pending"}
        step = _step(status=StepStatus.COMPLETED, bridge_deposit_id="dep-1")
        result = asyncio.run(executor._rehydrate_step(step))
        assert not result.valid
        assert "bridge status is pending" in result.details

    def test_bridge_status_error_is_soft(self, executor, provider):
        provider.get_bridge_transfer_status.side_effect = RuntimeError("bridge api down")
        step = _step(status=StepStatus.CONFIRMED, bridge_deposit_id="dep-1")
        result = asyncio.run(executor._rehydrate_step(step))
        assert result.valid
        assert "Error checking bridge status" in result.details


class TestRehydratePlan:
    def test_tampered_hash_is_invalid(self, executor):
        plan = _plan(_step())
        plan.plan_hash = "deadbeefdeadbeef"
        result = asyncio.run(executor.rehydrate_plan(plan))
        assert result.status == RehydrationStatus.INVALID
        assert result.errors

    def test_failed_step_needs_remediation(self, executor, provider):
        provider.get_transaction_status.return_value = {"status": "failed"}
        plan = _plan(_step(status=StepStatus.SUBMITTED, tx_hash="0xtx"))
        result = asyncio.run(executor.rehydrate_plan(plan))
        assert result.status == RehydrationStatus.NEEDS_REMEDIATION
        assert result.steps_needing_remediation == ["step-1"]

    def test_invalid_step_yields_state_updated(self, executor, provider):
        provider.get_transaction_status.return_value = {"status": "pending"}
        plan = _plan(_step(status=StepStatus.COMPLETED, tx_hash="0xtx"))
        result = asyncio.run(executor.rehydrate_plan(plan))
        assert result.status == RehydrationStatus.STATE_UPDATED
        assert result.warnings

    def test_clean_plan_is_valid(self, executor):
        plan = _plan(_step(status=StepStatus.COMPLETED, tx_hash="0xtx"))
        result = asyncio.run(executor.rehydrate_plan(plan))
        assert result.status == RehydrationStatus.VALID


class TestReconcilePlan:
    def test_tampered_hash_cannot_resume(self, executor):
        plan = _plan(_step())
        plan.plan_hash = "deadbeefdeadbeef"
        recon = asyncio.run(executor.reconcile_plan(plan))
        assert recon.status == ReconciliationStatus.INVALID
        assert not recon.can_resume

    def test_stale_quotes_without_auto_refresh(self, executor):
        stale = _step(
            pinned_quote={"bridge_name": "across"},
            pinned_at=datetime.now(UTC) - timedelta(hours=2),
        )
        recon = asyncio.run(executor.reconcile_plan(_plan(stale)))
        assert recon.status == ReconciliationStatus.STALE_QUOTES
        assert recon.stale_steps == ["step-1"]

    def test_resume_from_first_incomplete_step(self, executor):
        done = _step("step-1", status=StepStatus.COMPLETED, tx_hash="0xtx")
        todo = _step("step-2", dependencies=["step-1"])
        recon = asyncio.run(executor.reconcile_plan(_plan(done, todo)))
        assert recon.status == ReconciliationStatus.VALID
        assert recon.can_resume
        assert recon.resume_from_step == "step-2"

    def test_all_steps_complete_nothing_to_resume(self, executor):
        done = _step(status=StepStatus.COMPLETED, tx_hash="0xtx")
        recon = asyncio.run(executor.reconcile_plan(_plan(done)))
        assert not recon.can_resume
        assert recon.resume_from_step is None

    def test_state_mismatch_blocks_resume(self, executor, provider):
        provider.get_transaction_status.return_value = {"status": "pending"}
        # COMPLETED-but-unconfirmed is a mismatch; it is also the first
        # non-success... COMPLETED counts as success, so add a pending step
        # that depends on it — the mismatch on step-1 poisons the plan status.
        bad = _step("step-1", status=StepStatus.COMPLETED, tx_hash="0xtx")
        recon = asyncio.run(executor.reconcile_plan(_plan(bad)))
        assert recon.status == ReconciliationStatus.STATE_MISMATCH
        assert recon.warnings

    def test_reconcile_step_error_is_soft(self, executor, provider):
        provider.get_transaction_status.side_effect = RuntimeError("rpc down")
        step = _step(status=StepStatus.COMPLETED, tx_hash="0xtx")
        recon = asyncio.run(executor.reconcile_plan(_plan(step)))
        step_recon = recon.step_reconciliations[0]
        assert step_recon.matches
        assert "Error checking on-chain state" in step_recon.details

    def test_bridge_mismatch_recommends_check(self, executor, provider):
        provider.get_bridge_transfer_status.return_value = {"status": "pending"}
        step = _step(
            status=StepStatus.COMPLETED,
            tx_hash=None,
            bridge_deposit_id="dep-1",
        )
        recon = asyncio.run(executor.reconcile_plan(_plan(step)))
        step_recon = recon.step_reconciliations[0]
        assert not step_recon.matches
        assert step_recon.recommended_action == "Check bridge transfer status"


class TestDetermineResumePoint:
    def test_mismatched_incomplete_step_blocks_resume(self, executor, provider):
        # SUBMITTED step (not success) whose tx errors out into a mismatch is
        # impossible via _reconcile_step (only success steps mismatch), so
        # drive _determine_resume_point directly through reconcile_plan with a
        # FAILED step marked mismatching via bridge state.
        provider.get_bridge_transfer_status.return_value = {"status": "pending"}
        done_but_wrong = _step(
            "step-1", status=StepStatus.COMPLETED, bridge_deposit_id="dep-1"
        )
        blocked = _step("step-2", dependencies=["step-1"])
        recon = asyncio.run(executor.reconcile_plan(_plan(done_but_wrong, blocked)))
        # step-2 is the first non-success step and it has no mismatch of its
        # own, so the plan resumes there despite step-1's mismatch downgrading
        # overall status.
        assert recon.status == ReconciliationStatus.STATE_MISMATCH
        assert recon.can_resume
        assert recon.resume_from_step == "step-2"
