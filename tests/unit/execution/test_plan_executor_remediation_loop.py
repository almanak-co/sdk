"""Branch coverage for PlanExecutor.execute_plan_with_remediation.

Covers the step-dispatch loop: empty plans, sequential success, operator
escalation (with and without a card), plain failure, stuck steps promoting
the final status to NEEDS_OPERATOR, and the outer exception path. The
per-step executor is faked; no chain access.
"""

import asyncio

import pytest

from almanak.framework.execution.plan import PlanBundle, PlanStep, StepStatus
from almanak.framework.execution.plan_executor import (
    PlanExecutionStatus,
    PlanExecutor,
    StepExecutionResult,
)


def _step(step_id="step-1", dependencies=None):
    return PlanStep(
        step_id=step_id,
        chain="ethereum",
        intent={"type": "swap"},
        dependencies=dependencies or [],
    )


def _plan(*steps):
    return PlanBundle(plan_id="plan-1", steps=list(steps))


def _wire_steps(executor, monkeypatch, outcomes):
    """Fake per-step execution: set the step's status and return a result.

    ``outcomes`` maps step_id -> (StepStatus, StepExecutionResult).
    """
    executed = []

    async def _execute(plan, step, step_executor, remediation_executor):
        executed.append(step.step_id)
        status, result = outcomes[step.step_id]
        step.status = status
        return result

    monkeypatch.setattr(executor, "_execute_step_with_remediation", _execute)
    return executed


@pytest.fixture
def executor() -> PlanExecutor:
    return PlanExecutor()


def _run(executor, plan):
    return asyncio.run(executor.execute_plan_with_remediation(plan))


class TestExecutePlanWithRemediation:
    def test_empty_plan_completes(self, executor):
        result = _run(executor, _plan())
        assert result.status == PlanExecutionStatus.COMPLETED
        assert result.step_results == []

    def test_sequential_success(self, executor, monkeypatch):
        plan = _plan(_step("step-1"), _step("step-2", dependencies=["step-1"]))
        executed = _wire_steps(
            executor,
            monkeypatch,
            {
                "step-1": (StepStatus.COMPLETED, StepExecutionResult("step-1", success=True)),
                "step-2": (StepStatus.COMPLETED, StepExecutionResult("step-2", success=True)),
            },
        )
        result = _run(executor, plan)
        assert executed == ["step-1", "step-2"]
        assert result.status == PlanExecutionStatus.COMPLETED
        assert plan.completed_at is not None

    def test_operator_escalation_with_card(self, executor, monkeypatch):
        plan = _plan(_step("step-1"), _step("step-2", dependencies=["step-1"]))
        card = {"title": "Bridge stuck", "action": "review"}
        executed = _wire_steps(
            executor,
            monkeypatch,
            {
                "step-1": (
                    StepStatus.STUCK,
                    StepExecutionResult("step-1", success=False, requires_operator=True, operator_card=card),
                ),
            },
        )
        result = _run(executor, plan)
        assert executed == ["step-1"]
        assert result.status == PlanExecutionStatus.NEEDS_OPERATOR
        assert result.operator_cards == [card]

    def test_operator_escalation_without_card(self, executor, monkeypatch):
        plan = _plan(_step("step-1"))
        _wire_steps(
            executor,
            monkeypatch,
            {
                "step-1": (
                    StepStatus.STUCK,
                    StepExecutionResult("step-1", success=False, requires_operator=True),
                ),
            },
        )
        result = _run(executor, plan)
        assert result.status == PlanExecutionStatus.NEEDS_OPERATOR
        assert result.operator_cards == []

    def test_failed_step_stops_plan(self, executor, monkeypatch):
        plan = _plan(_step("step-1"), _step("step-2", dependencies=["step-1"]))
        executed = _wire_steps(
            executor,
            monkeypatch,
            {
                "step-1": (StepStatus.FAILED, StepExecutionResult("step-1", success=False)),
            },
        )
        result = _run(executor, plan)
        assert executed == ["step-1"]
        assert result.status == PlanExecutionStatus.FAILED

    def test_unexpected_error_marks_failed(self, executor, monkeypatch):
        plan = _plan(_step("step-1"))

        async def _boom(plan_, step, step_executor, remediation_executor):
            raise RuntimeError("executor crashed")

        monkeypatch.setattr(executor, "_execute_step_with_remediation", _boom)
        result = _run(executor, plan)
        assert result.status == PlanExecutionStatus.FAILED
        assert "executor crashed" in result.error

    def test_started_at_stamped(self, executor, monkeypatch):
        plan = _plan(_step("step-1"))
        _wire_steps(
            executor,
            monkeypatch,
            {"step-1": (StepStatus.COMPLETED, StepExecutionResult("step-1", success=True))},
        )
        _run(executor, plan)
        assert plan.started_at is not None
