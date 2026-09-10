"""Connector admission failures remain visible to execution event consumers."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionEventType,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ValueError("hook operation is not admitted"), "Connector operation refused: hook operation is not admitted"),
        (TypeError("invalid operation evidence"), "Connector operation refused: invalid operation evidence"),
        (KeyError("v4_operation"), "Connector operation refused: 'v4_operation'"),
    ],
)
async def test_connector_refusal_emits_validation_failure(error, expected):
    orchestrator = ExecutionOrchestrator.__new__(ExecutionOrchestrator)
    orchestrator.chain = "base"
    orchestrator.signer = Mock()
    orchestrator.operation_observer_factory = None
    orchestrator.managed_fork = False
    orchestrator._emit_event = Mock()
    orchestrator._complete_session = Mock()
    context = ExecutionContext(deployment_id="deployment:test", chain="base")
    state = SimpleNamespace(
        context=context,
        action_bundle=Mock(),
        result=ExecutionResult(success=False, phase=ExecutionPhase.VALIDATION),
        session=None,
    )
    with patch(
        "almanak.framework.execution.connector_validation.validate_connector_execution",
        side_effect=error,
    ):
        result = await orchestrator._validate_connector_operation(state)
    assert result is state.result and not result.success
    assert result.error_phase is ExecutionPhase.VALIDATION
    assert result.error == expected
    orchestrator._complete_session.assert_called_once_with(state.session, success=False, error=expected)
    orchestrator._emit_event.assert_called_once_with(
        ExecutionEventType.RISK_BLOCKED,
        context,
        {
            "violations": [expected],
            "connector_validation": {"status": "refused", "phase": ExecutionPhase.VALIDATION.value, "error": str(error)},
        },
    )
