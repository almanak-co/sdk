"""Reusable author hooks distinguish external liquidation from admitted request outcomes."""

import gzip
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2.state import Checkpoint
from tests.gmx_retired import current_json

ROOT = Path(__file__).parents[1]


def reference(chain):
    path = ROOT / "strategies" / f"gmx_v2_{chain}_position/strategy.py"
    spec = importlib.util.spec_from_file_location("gmx_position_reference", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
def test_real_reference_waits_for_keeper_and_finishes_liquidated_allocation_once(chain):
    path = (
        ROOT
        / "docs/internal/reports/evidence/2026-09-09-gmx-liquidation"
        / f"complete-{chain}-eoa-external_commit/checkpoint.json.gz"
    )
    state = Checkpoint.model_validate_json(current_json(gzip.decompress(path.read_bytes())))
    strategy = reference(chain)
    context = SimpleNamespace(memory=strategy.State(), result=state.outcomes[1].strategy_result())
    assert strategy.on_outcome(context) == context.memory
    context.result = state.outcomes[2].strategy_result()
    context.memory = strategy.on_outcome(context)
    opened = dict(context.memory.opened_at)
    assert opened == {"Main": state.outcomes[2].after.block.timestamp}
    context.result = state.outcomes[-1].strategy_result()
    context.memory = strategy.on_outcome(context)
    assert context.memory.finished == ("Main",)
    assert context.memory.opened_at == opened
    assert strategy.on_outcome(context) == context.memory
