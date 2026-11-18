"""Aave V4 financial observations survive the shared persisted wire union."""

import pytest
from pydantic import TypeAdapter

from almanak.connectors.aave_v4.models import LendingSnapshot
from almanak.connectors.composed.state import Checkpoint
from almanak.connectors.decision_observations import Observation
from tests.lending_captures import read_capture
from tests.uniswap_support import scope_only_json


@pytest.mark.parametrize("field", ["baseline", "closure"])
def test_checkpoint_observation_fields_retain_native_aave_v4_facts(recorded, field):
    snapshot = recorded.baseline[0].model_copy(
        update={
            "drawn_shares": 17,
            "premium_shares": 3,
            "premium_offset_ray": -5,
            "collateral_enabled": True,
            "health_error": "synthetic unavailable risk observation",
        }
    )
    adapter = TypeAdapter(Checkpoint.model_fields[field].annotation)
    decoded = adapter.validate_json(adapter.dump_json((snapshot,), warnings="error"))
    assert decoded == (snapshot,)
    assert isinstance(decoded[0], LendingSnapshot)
    assert TypeAdapter(Observation).validate_json(snapshot.model_dump_json()) == snapshot


@pytest.mark.parametrize("chain", ["ethereum", "avalanche"])
def test_actual_initialized_observations_survive_native_decision_and_checkpoint(chain, recorded):
    from pathlib import Path

    from almanak.connectors.composed.codec import codec
    from almanak.connectors.identifiers import ConnectorId
    from almanak.strategy_api import Memory

    path = Path(__file__).parent / "fixtures" / f"aave-v4-composed-{chain}-initialized.json.gz"
    state = Checkpoint.model_validate_json(scope_only_json(read_capture(path)))
    config = state.config.component(ConnectorId("aave_v4"))
    record = codec().decision_record(
        identity="a" * 32,
        intent=config.supply("collateral", config.allocations[0].capital),
        observations=state.baseline,
        trigger=recorded.outcomes[0].decision.trigger,
        memory=Memory(),
    )
    assert record.observations == state.baseline
    changed = state.model_copy(update={"decision": record})
    assert Checkpoint.model_validate_json(changed.model_dump_json(warnings="error")) == changed
