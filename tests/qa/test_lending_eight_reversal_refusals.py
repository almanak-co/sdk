"""Keep real fork routing faults distinct from synthetic historical-input withholding."""

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from almanak.connectors.composed.economic_state import State
from almanak.connectors.composed.state import Checkpoint
from tests.scope_only_sources import reviewed_json, reviewed_record

EVIDENCE = (
    Path(__file__).parents[2]
    / "docs/internal/reports/evidence/2026-09-09-lending-rollout/lending-eight-reversal-refusals"
)


def raw(name):
    return gzip.decompress((EVIDENCE / (name + ".gz")).read_bytes())


def row(index):
    return json.loads((EVIDENCE / "summary.json").read_text())["rows"][index]


def test_manifest_keeps_eight_separate_proof_pairs():
    manifest = json.loads((EVIDENCE / "manifest.json").read_text())
    for name, digest in manifest["files"].items():
        assert hashlib.sha256((EVIDENCE / name).read_bytes()).hexdigest() == digest
    rows = json.loads((EVIDENCE / "summary.json").read_text())["rows"]
    assert len({value["reference"] for value in rows}) == 8
    assert all(value["source_run_manifest_sha256"] and value["source_sdk_digest"] for value in rows)
    replay = list((EVIDENCE / "current-source-replay").iterdir())
    assert len(replay) == 5
    for folder in replay:
        for phase in ("execute", "resume", "reproduce", "synthetic-withholding"):
            result = json.loads(gzip.decompress((folder / (phase + ".json.gz")).read_bytes()))
            assert result["status"] == "passed" and result["network"] == "isolated"


@pytest.mark.parametrize("index", range(8))
def test_actual_router_fault_keeps_debt_until_exact_restoration(index):
    path = row(index)["native_path"]
    proof = json.loads(raw(path + "/reversal-proof.json"))
    failed = Checkpoint.model_validate_json(json.dumps(reviewed_record(proof["failed_checkpoint"])))
    closed_paths = list((EVIDENCE / path).rglob("checkpoint.json.gz"))
    assert len(closed_paths) == 1
    closed = Checkpoint.model_validate_json(
        reviewed_json(gzip.decompress(closed_paths[0].read_bytes()))
    )
    assert failed.mode == "teardown" and failed.pending is None and failed.plan is None
    assert failed.memory.values["traded"]
    held = {str(position.target.allocation): position for position in failed.owned}
    assert collateral_units(held["collateral"]) > 0
    if hasattr(held["loan"], "borrow_shares"):
        assert held["loan"].borrow_shares > 0
    elif hasattr(held["loan"], "debt_scaled"):
        assert held["loan"].debt_scaled > 0
    else:
        assert held["loan"].principal < 0
    assert not any(o.operation.kind in ("repay", "withdraw") for o in failed.outcomes)
    assert tuple(closed.outcomes[: len(failed.outcomes)]) == failed.outcomes
    assert proof["original_code_sha256"] and proof["injected_code"] == "0x60006000fd"
    assert proof["restored_router_completed_reference"]
    assert closed.mode == "closed" and all(empty(position) for position in closed.owned[:-1])
    assert not closed.closure[-1].needs_cleanup
    assert closed.delivered == len(closed.outcomes)


@pytest.mark.parametrize("index", range(8))
def test_synthetic_missing_input_has_no_financial_effects_or_false_closure(index):
    path = row(index)["synthetic_path"]
    proof = json.loads(raw(path + "/result.json"))
    before = State.model_validate_json(reviewed_json(raw(path + "/before-withheld-input.json")))
    after = State.model_validate_json(reviewed_json(raw(path + "/state.json")))
    assert after.portfolio == before.portfolio
    failed = [item for item in after.completed if not item.transition.result.success]
    assert len(failed) == 1 and failed[0].pending.intent.kind == "exit_exchange"
    assert failed[0].transition.result.unavailable is not None
    assert failed[0].transition.effects.assets == failed[0].transition.effects.positions == ()
    assert not any(item.pending.intent.kind in ("repay", "withdraw") for item in after.completed)
    assert not proof["closed"] and proof["network_probe_errno"] == 101
    assert proof["shared_inventory_and_owned_positions_unchanged"]
    assert not json.loads(raw(path + "/report.json"))["closed"]


def collateral_units(position):
    return position.supply_scaled if hasattr(position, "supply_scaled") else position.collateral


def empty(position):
    if hasattr(position, "supply_scaled"):
        return (
            position.supply_scaled == position.debt_scaled == 0 and not position.collateral_enabled
        )
    return position.empty
