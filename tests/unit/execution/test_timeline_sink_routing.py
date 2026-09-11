"""Default and injected execution telemetry routes remain mutually exclusive."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType
from almanak.framework.execution import orchestrator


@pytest.mark.parametrize("native_sink", [False, True])
def test_timeline_routes_exact_event_to_only_selected_sink(monkeypatch, native_sink):
    default = MagicMock()
    native = MagicMock()
    monkeypatch.setattr(orchestrator, "add_event", default)
    runner = orchestrator.ExecutionOrchestrator(
        signer=MagicMock(),
        submitter=MagicMock(),
        simulator=None,
        chain="bsc",
        timeline_sink=native if native_sink else None,
    )
    observed = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=TimelineEventType.TRANSACTION_CONFIRMED,
        description="canonical receipt",
        deployment_id="deployment",
        chain="bsc",
        tx_hash="0x123",
        details={"amount": "0"},
    )

    runner._persist_timeline_event(observed)

    selected, other = (native, default) if native_sink else (default, native)
    selected.assert_called_once_with(observed)
    assert selected.call_args.args[0] is observed
    other.assert_not_called()
