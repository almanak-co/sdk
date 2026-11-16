"""Retained GMX snapshots cross the composed gateway wire without losing protocol facts."""

import pytest
from pydantic import TypeAdapter

from almanak.connectors.aave_v3.state import DecisionRecord
from almanak.connectors.composed.state import Checkpoint
from tests.gmx_evidence import checkpoint
from tests.test_gmx_swap_replay import completed as completed


@pytest.mark.parametrize("field", ("baseline", "closure"))
def test_composed_checkpoint_accepts_captured_gmx_snapshots(completed, field):
    snapshots = (checkpoint().baseline[0], completed.baseline[0])
    adapter = TypeAdapter(Checkpoint.model_fields[field].annotation)
    wire = adapter.dump_json(snapshots, warnings="error")
    assert adapter.validate_json(wire) == snapshots


def test_lending_decision_retains_sibling_gmx_observations(completed):
    snapshots = (checkpoint().baseline[0], completed.baseline[0])
    adapter = TypeAdapter(DecisionRecord.model_fields["observations"].annotation)
    assert adapter.validate_json(adapter.dump_json(snapshots, warnings="error")) == snapshots
