import json
from types import SimpleNamespace

import pytest

from qa_lab import e2e_controller as controller


@pytest.fixture
def run(tmp_path, monkeypatch):
    preparation = tmp_path / "preparation"
    preparation.mkdir()
    scenario = json.loads((controller.REPO / "qa_lab/docs/scenarios/v1/lp-dual-rebalance.json").read_text())
    (preparation / "scenario.json").write_text(json.dumps(scenario))
    for name in ("positions-open.json", "positions-managed.json"):
        (tmp_path / name).write_text("{}")
    context = SimpleNamespace(root=tmp_path, require_owned=lambda path: path)
    calls = []
    monkeypatch.setattr(controller, "verify_preparation", lambda *args: None)
    monkeypatch.setattr(controller, "load_owned_phases", lambda *args, **kwargs: ({}, {}))
    monkeypatch.setattr(controller, "generation_predicates", lambda *args: {"rebalance": {"status": "PASS"}})
    monkeypatch.setattr(controller, "HoldObserver", lambda *args, **kwargs: object())

    def monitor(observer, store, lease, schedule, *, baseline):
        assert baseline == {}
        calls.append(("monitor", schedule))
        return {"status": "PASS", "elapsed_seconds": 5400}

    def cleanup(*args, **kwargs):
        calls.append(("cleanup", kwargs["output"]))
        return {"status": "PASS"}

    monkeypatch.setattr(controller, "monitor_hold", monitor)
    monkeypatch.setattr(controller, "cleanup_subject", cleanup)
    monkeypatch.setattr(controller, "cleanup_stimulus", lambda *args, **kwargs: {"status": "UNMEASURED"})
    return context, calls


def invoke(run):
    return controller.monitor_and_cleanup(run[0], object(), object(), wallet="subject", supervised=True)


def test_frozen_schedule_reaches_monitor_and_component_success_is_not_e2e_pass(run):
    result = invoke(run)
    assert [call[0] for call in run[1]] == ["monitor", "cleanup"]
    schedule = run[1][0][1]
    assert (
        schedule.minimum_seconds,
        schedule.maximum_gap_seconds,
        schedule.interval_seconds,
        schedule.deadline_seconds,
    ) == (5400, 180, 60, 6600)
    assert result["status"] == result["e2e_admission"] == "UNMEASURED"
    assert (run[0].root / "controller-result.json").is_file()


def test_failed_rebalance_aborts_hold_but_still_cleans_up(run, monkeypatch):
    monkeypatch.setattr(
        controller,
        "generation_predicates",
        lambda *args: {"rebalance": {"status": "FAIL", "reason": "no replacement NFT"}},
    )
    with pytest.raises(ExceptionGroup) as error:
        invoke(run)
    assert "no replacement NFT" in str(error.value.exceptions[0])
    assert [call[0] for call in run[1]] == ["cleanup"]


def test_monitor_cannot_use_unowned_phase_files_and_still_attempts_cleanup(run, monkeypatch):
    def unowned(*args, **kwargs):
        raise ValueError("Position evidence has no ownership record")

    monkeypatch.setattr(controller, "load_owned_phases", unowned)
    with pytest.raises(ExceptionGroup):
        invoke(run)
    assert [call[0] for call in run[1]] == ["cleanup"]


def test_interrupt_is_preserved_after_cleanup(run, monkeypatch):
    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(controller, "monitor_hold", interrupted)
    with pytest.raises(BaseExceptionGroup) as error:
        invoke(run)
    assert isinstance(error.value.exceptions[0], KeyboardInterrupt)
    assert [call[0] for call in run[1]] == ["cleanup"]
    assert json.loads((run[0].root / "controller-result.json").read_text())["status"] == "FAIL"


def test_cleanup_failure_is_not_hidden_by_completed_hold(run, monkeypatch):
    def failed(*args, **kwargs):
        raise OSError("fork lost")

    monkeypatch.setattr(controller, "cleanup_subject", failed)
    with pytest.raises(ExceptionGroup) as error:
        invoke(run)
    assert isinstance(error.value.exceptions[0], OSError)
    result = json.loads((run[0].root / "controller-result.json").read_text())
    assert result["status"] == "FAIL"
    assert result["subject_cleanup"] is None


def test_invalid_frozen_schedule_cannot_start_monitor(run):
    path = run[0].root / "preparation/scenario.json"
    scenario = json.loads(path.read_text())
    scenario["hold_policy"]["minimum_seconds"] = True
    path.write_text(json.dumps(scenario))
    with pytest.raises(ExceptionGroup):
        invoke(run)
    assert [call[0] for call in run[1]] == ["cleanup"]


def test_unqualified_unattended_mode_is_rejected_before_operation(run):
    with pytest.raises(ValueError, match="qualified durable hosting"):
        controller.monitor_and_cleanup(run[0], object(), object(), wallet="subject", supervised=False)
    assert run[1] == []


def test_actor_cleanup_failure_preserves_completed_subject_evidence(run, monkeypatch):
    def fail(*args, **kwargs):
        raise ValueError("actor fork mismatch")

    monkeypatch.setattr(controller, "cleanup_stimulus", fail)
    with pytest.raises(ExceptionGroup, match="Controller did not complete"):
        invoke(run)
    result = json.loads((run[0].root / "controller-result.json").read_text())
    assert result["status"] == "FAIL"
    assert result["subject_cleanup"]["status"] == "PASS"
    assert result["stimulus_cleanup"] is None
    assert result["cleanup_error_type"] == "ValueError"
