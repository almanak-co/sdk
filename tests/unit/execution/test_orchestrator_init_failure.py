"""Regression tests for ``ExecutionOrchestrator.execute`` init-phase failures (#1661).

If ``_init_pipeline_state`` raises (e.g., intent-description generation,
session creation, or session-store persistence fails), ``execute`` must
return an ``ExecutionResult`` rather than re-raising the exception. These
tests pin that behaviour.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionEventType,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.models.reproduction_bundle import ActionBundle


@pytest.fixture
def orchestrator():
    signer = MagicMock()
    signer.address = "0x1111111111111111111111111111111111111111"
    submitter = MagicMock()
    simulator = MagicMock()
    return ExecutionOrchestrator(
        signer=signer,
        submitter=submitter,
        simulator=simulator,
        chain="arbitrum",
    )


class TestExecuteInitFailure:
    @pytest.mark.asyncio
    async def test_init_pipeline_state_exception_returns_execution_result(self, orchestrator):
        """An exception from ``_init_pipeline_state`` must be normalized.

        Before the fix, the init call lived outside the ``try`` block, so any
        failure during session creation / description generation escaped
        ``execute`` entirely. The caller would see a raw exception instead of
        a structured ``ExecutionResult`` failure.
        """
        bundle = ActionBundle(intent_type="SWAP", transactions=[{"to": "0x0", "data": "0x", "value": 0}])

        orchestrator._init_pipeline_state = MagicMock(
            side_effect=RuntimeError("session store unavailable"),
        )

        # Must not re-raise.
        result = await orchestrator.execute(bundle)

        assert isinstance(result, ExecutionResult)
        assert result.success is False
        assert result.error is not None
        assert "session store unavailable" in result.error
        # VALIDATION is the earliest pipeline phase, so init failures are
        # attributed there.
        assert result.phase == ExecutionPhase.VALIDATION
        assert result.error_phase == ExecutionPhase.VALIDATION

    @pytest.mark.asyncio
    async def test_init_pipeline_state_exception_message_propagates(self, orchestrator):
        """The original exception's message must appear in ``result.error``."""
        bundle = ActionBundle(intent_type="SWAP", transactions=[])

        class _CustomInitError(Exception):
            pass

        orchestrator._init_pipeline_state = MagicMock(
            side_effect=_CustomInitError("intent description generation failed"),
        )

        result = await orchestrator.execute(bundle)

        assert isinstance(result, ExecutionResult)
        assert result.success is False
        assert "intent description generation failed" in (result.error or "")

    @pytest.mark.asyncio
    async def test_init_pipeline_state_exception_preserves_caller_correlation_id(self, orchestrator):
        """Caller-provided correlation_id must be preserved on init failure.

        Cross-component traceability relies on a stable correlation_id. The
        init-failure fallback must not mint a new UUID when the caller already
        passed a context with an explicit ID.
        """
        bundle = ActionBundle(intent_type="SWAP", transactions=[])
        caller_context = ExecutionContext(
            wallet_address="0x2222222222222222222222222222222222222222",
            chain="arbitrum",
            correlation_id="caller-provided-id-123",
        )

        orchestrator._init_pipeline_state = MagicMock(
            side_effect=RuntimeError("session store unavailable"),
        )

        result = await orchestrator.execute(bundle, context=caller_context)

        assert result.correlation_id == "caller-provided-id-123"

    @pytest.mark.asyncio
    async def test_init_pipeline_state_exception_emits_execution_failed_event(self, orchestrator):
        """Init-failure path must emit EXECUTION_FAILED for timeline consumers.

        Monitoring/retry flows rely on the EXECUTION_FAILED event; previously
        init-phase failures returned silently without emitting it.
        """
        bundle = ActionBundle(intent_type="SWAP", transactions=[])
        orchestrator._init_pipeline_state = MagicMock(
            side_effect=RuntimeError("session store unavailable"),
        )

        emitted: list[tuple] = []

        def _capture(event_type, context, details=None):
            emitted.append((event_type, context, details))

        orchestrator._emit_event = _capture  # type: ignore[method-assign]

        await orchestrator.execute(bundle)

        failed_events = [e for e in emitted if e[0] == ExecutionEventType.EXECUTION_FAILED]
        assert len(failed_events) == 1
        _, _, details = failed_events[0]
        assert details is not None
        assert details["error"] == "session store unavailable"
        assert details["error_type"] == "RuntimeError"
