"""Synthetic observer clocks over captured LP bytes; no live-duration claim."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from qa_lab.e2e_card import canonical, digest, load_json
from qa_lab.e2e_hold import HoldObserver, held_exposure, validate_hold_contract

POSITIONS = Path(__file__).resolve().parents[2] / "fixtures/accounting/harness/lp-dual-generations/positions-open.json"


@pytest.fixture
def observations(tmp_path):
    origin = datetime(2026, 9, 7, tzinfo=UTC)
    raw = POSITIONS.read_bytes()
    for index in range(3):
        (tmp_path / f"{index:06d}-positions.json").write_bytes(raw)
        sample = {
            "schema_version": 1,
            "sequence": index,
            "segment_id": "observer-epoch-one",
            "started_at": (origin + timedelta(seconds=index * 60)).isoformat(),
            "finished_at": (origin + timedelta(seconds=index * 60 + 1)).isoformat(),
            "started_monotonic_ns": index * 60 * 10**9,
            "finished_monotonic_ns": (index * 60 + 1) * 10**9,
            "positions_sha256": digest(raw),
        }
        (tmp_path / f"{index:06d}-sample.json").write_bytes(canonical(sample))
    return tmp_path


def test_hold_duration_excludes_incomplete_endpoint_capture_windows(observations):
    result = held_exposure(observations, minimum_seconds=119, maximum_gap_seconds=90)
    assert result["status"] == "PASS"
    assert result["elapsed_seconds"] == 119
    assert result["maximum_observed_gap_seconds"] == 61


def test_six_hour_target_does_not_inflate_two_minutes_of_observations(observations):
    result = held_exposure(observations, minimum_seconds=21600, maximum_gap_seconds=90)
    assert result["status"] == "FAIL"
    assert result["elapsed_seconds"] == 119
    assert "shorter" in result["reason"]


@pytest.mark.parametrize(
    "fault",
    ["sequence", "boolean_sequence", "restart", "wall_jump", "overlap", "missing_sample", "orphan", "positions"],
)
def test_hold_mutations_fail_closed(observations, fault):
    path = observations / "000001-sample.json"
    sample = load_json(path)
    if fault == "sequence":
        sample["sequence"] = 0
    elif fault == "boolean_sequence":
        sample["sequence"] = True
    elif fault == "restart":
        sample["segment_id"] = "replacement-observer"
    elif fault == "wall_jump":
        sample["started_at"] = "2026-09-07T01:01:00+00:00"
        sample["finished_at"] = "2026-09-07T01:01:01+00:00"
    elif fault == "overlap":
        sample["started_monotonic_ns"] = 0
        sample["finished_monotonic_ns"] = 10**9
    elif fault == "missing_sample":
        path.unlink()
    elif fault == "orphan":
        (observations / "uncommitted-positions.json").write_text("{}")
    else:
        (observations / "000001-positions.json").write_text("{}")
    if fault not in {"missing_sample", "orphan", "positions"}:
        path.write_bytes(canonical(sample))
    result = held_exposure(observations, minimum_seconds=119, maximum_gap_seconds=90)
    assert result["status"] == "FAIL"
    assert {
        "sequence": "sequence",
        "boolean_sequence": "sequence",
        "restart": "restart",
        "wall_jump": "clocks",
        "overlap": "clocks",
        "missing_sample": "sequence",
        "orphan": "orphan",
        "positions": "digest",
    }[fault] in result["reason"]


def test_capture_windows_count_toward_the_gap_bound(observations):
    result = held_exposure(observations, minimum_seconds=119, maximum_gap_seconds=60)
    assert result["status"] == "FAIL"
    assert "gap" in result["reason"]


def test_absent_hold_observations_are_unmeasured(tmp_path):
    assert held_exposure(tmp_path, minimum_seconds=21600, maximum_gap_seconds=180)["status"] == "UNMEASURED"


def test_closed_positions_cannot_count_as_held_exposure(observations):
    raw = POSITIONS.with_name("positions-terminal.json").read_bytes()
    (observations / "000001-positions.json").write_bytes(raw)
    path = observations / "000001-sample.json"
    sample = load_json(path)
    sample["positions_sha256"] = digest(raw)
    path.write_bytes(canonical(sample))
    result = held_exposure(observations, minimum_seconds=119, maximum_gap_seconds=90)
    assert result["status"] == "FAIL"
    assert "modified between samples" in result["reason"]


def test_observer_records_real_clock_windows_and_cannot_overwrite_a_previous_epoch(tmp_path, monkeypatch):
    import qa_lab.e2e_hold as hold

    witness = load_json(POSITIONS)
    context = SimpleNamespace(require_owned=lambda path: path, fork_block=witness["fork_identity"]["fork_block"])
    monkeypatch.setattr(hold, "capture_position_generations", lambda *args: witness)
    directory = tmp_path / "observations"
    observer = HoldObserver(context, wallet=witness["wallet"], output=directory)
    first = load_json(observer.capture())
    second = load_json(observer.capture())
    assert first["segment_id"] == second["segment_id"]
    assert first["finished_monotonic_ns"] <= second["started_monotonic_ns"]
    assert first["positions_sha256"] == digest((directory / "000000-positions.json").read_bytes())
    with pytest.raises(FileExistsError):
        HoldObserver(context, wallet=witness["wallet"], output=directory)


def test_hold_contract_binds_samples_between_position_checkpoints(observations):
    bundle = observations / "bundle"
    directory = bundle / "hold-observations"
    directory.mkdir(parents=True)
    for path in observations.glob("*.json"):
        path.rename(directory / path.name)
    (bundle / "positions-managed.json").write_bytes(POSITIONS.read_bytes())
    (bundle / "positions-terminal.json").write_bytes(POSITIONS.with_name("positions-terminal.json").read_bytes())
    contract = {"sampled_hold": {"schema_version": 1, "minimum_seconds": 119, "maximum_gap_seconds": 90}}
    with pytest.raises(ValueError, match="generation obligation"):
        validate_hold_contract(bundle, contract, generation_evidence=None)
    generation = {"rebalance": {"status": "PASS"}, "terminal_closure": {"status": "PASS"}}
    result = validate_hold_contract(bundle, contract, generation_evidence=generation)
    assert result["elapsed_seconds"] == 119
    assert len(result["source_artifacts"]) == 8
    terminal_path = bundle / "positions-terminal.json"
    terminal = load_json(terminal_path)
    terminal["fork_identity"]["instance_id"] = "another-fork"
    terminal_path.write_bytes(canonical(terminal))
    with pytest.raises(ValueError, match="different runs"):
        validate_hold_contract(bundle, contract, generation_evidence=generation)
