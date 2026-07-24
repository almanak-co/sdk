"""Branch coverage for RemediationStateMachine BRIDGE_BACK / SWAP_TO_STABLE.

Drives ``handle_failure`` on a retries-exhausted step so the real state
transitions fire, covering every branch of ``_execute_bridge_back`` and
``_execute_swap_to_stable``: missing remediation intent, risk-guard block,
executor success/failure/exception, and the executor-less simulation path.
All executors and risk guards are in-memory fakes — no chain access.
"""

import asyncio

import pytest

from almanak.framework.execution.plan import (
    PlanBundle,
    PlanStep,
    RemediationAction,
    StepStatus,
)
from almanak.framework.execution.remediation import (
    RemediationState,
    RemediationStateMachine,
    RiskValidationResult,
)

_REMEDIATION_INTENT = {"type": "bridge", "token": "USDC", "amount": "100"}

_ACTIONS = pytest.mark.parametrize(
    ("action", "active_state", "failure_phrase"),
    [
        (RemediationAction.BRIDGE_BACK, RemediationState.BRIDGING_BACK, "Bridge back"),
        (RemediationAction.SWAP_TO_STABLE, RemediationState.SWAPPING_TO_STABLE, "Swap to stable"),
    ],
)


class _Executor:
    """Async intent-executor double recording every call."""

    def __init__(self, result=None, error=None):
        self._result = result
        self._error = error
        self.calls = []

    async def execute_intent(self, intent):
        self.calls.append(intent)
        if self._error is not None:
            raise self._error
        return self._result


class _RiskGuard:
    def __init__(self, allowed=True, violations=()):
        self._result = RiskValidationResult(allowed=allowed, violations=list(violations))

    def validate_intent(self, intent):
        return self._result


def _step(action, *, remediation_intent=_REMEDIATION_INTENT):
    return PlanStep(
        step_id="step-1",
        chain="arbitrum",
        intent={"type": "bridge", "token": "USDC", "amount": "100"},
        status=StepStatus.FAILED,
        remediation=action,
        remediation_intent=remediation_intent,
        retry_count=3,
        max_retries=3,
    )


def _machine(step, *, risk_guard=None, executor=None):
    plan = PlanBundle(plan_id="plan-1", steps=[step])
    return RemediationStateMachine(plan, risk_guard=risk_guard, intent_executor=executor)


def _handle(machine, step):
    return asyncio.run(machine.handle_failure(step))


@_ACTIONS
class TestRemediationActions:
    def test_missing_remediation_intent_escalates(self, action, active_state, failure_phrase):
        step = _step(action, remediation_intent=None)
        machine = _machine(step)

        result = _handle(machine, step)

        assert result.success is False
        assert result.requires_operator is True
        assert result.state == RemediationState.ESCALATED
        assert result.action_taken == RemediationAction.OPERATOR_INTERVENTION
        assert "No remediation intent configured" in (result.error or "")
        assert step.status == StepStatus.STUCK
        card = machine.operator_cards["step-1"]
        assert "No remediation intent configured" in (card.attempted_remediation or "")

    def test_risk_guard_block_escalates(self, action, active_state, failure_phrase):
        executor = _Executor(result={"success": True, "tx_hash": "0xabc"})
        step = _step(action)
        machine = _machine(
            step,
            risk_guard=_RiskGuard(allowed=False, violations=["too big", "too fast"]),
            executor=executor,
        )

        result = _handle(machine, step)

        assert result.requires_operator is True
        assert "Risk check failed: too big, too fast" in (result.error or "")
        # The blocked intent never reaches the executor.
        assert executor.calls == []

    def test_executor_success_resolves(self, action, active_state, failure_phrase):
        executor = _Executor(result={"success": True, "tx_hash": "0xabc"})
        step = _step(action)
        machine = _machine(step, risk_guard=_RiskGuard(allowed=True), executor=executor)

        result = _handle(machine, step)

        assert result.success is True
        assert result.state == RemediationState.RESOLVED
        assert result.action_taken == action
        assert result.new_tx_hash == "0xabc"
        assert result.artifacts == {"remediation_intent": _REMEDIATION_INTENT}
        assert executor.calls == [_REMEDIATION_INTENT]
        assert machine.get_state("step-1") == RemediationState.RESOLVED
        # The full transition chain went through the active remediation state.
        states = [record.to_state for record in machine.get_state_history("step-1")]
        assert active_state in states

    def test_executor_reported_failure_escalates(self, action, active_state, failure_phrase):
        executor = _Executor(result={"success": False, "error": "reverted"})
        step = _step(action)
        machine = _machine(step, executor=executor)

        result = _handle(machine, step)

        assert result.requires_operator is True
        assert f"{failure_phrase} execution failed" in (result.error or "")
        assert "reverted" in (result.error or "")

    def test_executor_exception_escalates(self, action, active_state, failure_phrase):
        executor = _Executor(error=RuntimeError("rpc down"))
        step = _step(action)
        machine = _machine(step, executor=executor)

        result = _handle(machine, step)

        assert result.requires_operator is True
        assert f"{failure_phrase} execution failed" in (result.error or "")
        assert "rpc down" in (result.error or "")

    def test_no_executor_simulates_success(self, action, active_state, failure_phrase):
        step = _step(action)
        machine = _machine(step)

        result = _handle(machine, step)

        assert result.success is True
        assert result.state == RemediationState.RESOLVED
        assert result.action_taken == action
        assert result.new_tx_hash is None
        assert result.artifacts == {"remediation_intent": _REMEDIATION_INTENT}
        assert machine.get_state("step-1") == RemediationState.RESOLVED
