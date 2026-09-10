"""Qualification retains immutable evidence before and after process release."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from qa_lab import e2e_local_qualification as qualification


def test_failed_startup_with_dead_backup_keeps_controller_as_cleanup_owner(tmp_path, monkeypatch):
    from qa_lab import e2e_preboot_watchdog

    lease = SimpleNamespace(owner="controller")
    store = SimpleNamespace(acquire=lambda *a, **kw: lease, request_cleanup=Mock())
    backup = SimpleNamespace(poll=lambda: 1)
    worker = SimpleNamespace(wait=Mock(), returncode=0)
    monkeypatch.setattr(qualification, "runtime_environment", lambda *a, **kw: {})
    monkeypatch.setattr(qualification, "OwnershipStore", lambda path: store)
    monkeypatch.setattr(qualification, "start_local_cleanup", lambda *a, **kw: backup)
    monkeypatch.setattr(qualification, "start_local_subject", lambda *a, **kw: worker)

    def startup(*args, **kwargs):
        assert kwargs["cleanup_worker"] is backup
        raise RuntimeError("Cleanup worker exited during startup")

    def recovery(root, actual_store, **kwargs):
        assert actual_store is store
        store.request_cleanup.assert_called_once_with(lease)
        assert kwargs["cleanup_lease"] is lease
        assert kwargs["owner"] == lease.owner
        assert kwargs["output"] == root / "controller-startup-cleanup"
        return {"e2e_admission": "UNMEASURED"}

    monkeypatch.setattr(qualification, "wait_for_startup", startup)
    monkeypatch.setattr(e2e_preboot_watchdog, "watch_prepared_cleanup", recovery)
    with pytest.raises(ExceptionGroup, match="incomplete"):
        qualification.qualify_local_processes(tmp_path, gateway_port=50071)
    worker.wait.assert_called_once_with(timeout=30)
    retained = json.loads((tmp_path / "local-qualification.json").read_text())
    assert retained["error_type"] == "RuntimeError"
    assert retained["startup_recovery"]["e2e_admission"] == "UNMEASURED"
    assert retained["worker_returncode"] == 0


@pytest.mark.parametrize("exited_child", ["subject", "cleanup"])
@pytest.mark.parametrize("returncode", [0, 1])
def test_opening_rejects_child_exit_during_successful_observation(monkeypatch, exited_child, returncode):
    worker = SimpleNamespace(returncode=None)
    backup = SimpleNamespace(returncode=None)
    worker.poll = lambda: worker.returncode
    backup.poll = lambda: backup.returncode

    def observe(*args, **kwargs):
        child = worker if exited_child == "subject" else backup
        child.returncode = returncode
        return {"end_block": 123}

    monkeypatch.setattr(qualification, "capture_phase", observe)
    with pytest.raises(RuntimeError, match="exited during opening"):
        qualification._opening(None, None, None, worker, backup, "wallet")


@pytest.mark.parametrize("elapsed,accepted", [(479, True), (480, False), (481, False)])
def test_opening_observation_must_finish_before_deadline(monkeypatch, elapsed, accepted):
    clock = [0]
    monkeypatch.setattr(qualification.time, "monotonic", lambda: clock[0])
    child = SimpleNamespace(poll=lambda: None)

    def observe(*args, **kwargs):
        clock[0] = elapsed
        return {"end_block": 123}

    monkeypatch.setattr(qualification, "capture_phase", observe)
    if accepted:
        assert qualification._opening(None, None, None, child, child, "wallet") == {"end_block": 123}
    else:
        with pytest.raises(TimeoutError, match="exceeded.*deadline"):
            qualification._opening(None, None, None, child, child, "wallet")


@pytest.mark.parametrize("reason", ["RELEASED", "INVALID_RELEASE"])
def test_release_completion_keeps_both_immutable_summaries(tmp_path, monkeypatch, reason):
    monkeypatch.setattr(qualification, "cleanup_subject", lambda *a, **kw: {"status": "PASS"})
    monkeypatch.setattr(qualification, "cleanup_stimulus", lambda *a, **kw: {"status": "UNMEASURED"})
    (tmp_path / "fork-shutdown.json").write_text(
        json.dumps(
            {
                "processes_stopped": True,
                "reason": reason,
                "observation_complete": reason == "RELEASED",
            }
        )
    )
    store = SimpleNamespace(request_cleanup=Mock(), release_fork=Mock())
    worker = SimpleNamespace(wait=Mock(), returncode=0)
    if reason == "RELEASED":
        qualification._close(SimpleNamespace(root=tmp_path), store, "lease", "wallet", worker)
    else:
        with pytest.raises(RuntimeError, match="observed fork release"):
            qualification._close(SimpleNamespace(root=tmp_path), store, "lease", "wallet", worker)
    before = json.loads((tmp_path / "qualification-before-release.json").read_text())
    after = json.loads((tmp_path / "qualification-cleanup.json").read_text())
    assert before["worker_returncode"] == 0
    assert after["worker_returncode"] == 0
    assert after["e2e_admission"] == "UNMEASURED"


def test_actor_exits_before_release_but_subject_release_precedes_worker_exit(tmp_path, monkeypatch):
    sequence = []
    monkeypatch.setattr(qualification, "cleanup_subject", lambda *a, **kw: {"status": "PASS"})
    monkeypatch.setattr(qualification, "cleanup_stimulus", lambda *a, **kw: {"status": "PASS"})
    (tmp_path / "fork-shutdown.json").write_text(
        json.dumps({"processes_stopped": True, "reason": "RELEASED", "observation_complete": True})
    )
    actor = SimpleNamespace(wait=lambda **kw: sequence.append("actor_stopped"), returncode=0)
    worker = SimpleNamespace(wait=lambda **kw: sequence.append("subject_stopped"), returncode=0)
    store = SimpleNamespace(request_cleanup=Mock(), release_fork=lambda lease: sequence.append("release"))
    result = qualification._close(SimpleNamespace(root=tmp_path), store, "lease", "wallet", worker, actors=[actor])
    assert sequence == ["release", "subject_stopped", "actor_stopped"]
    assert result["actor_returncodes"] == [0]
    assert result["e2e_admission"] == "UNMEASURED"


def test_unmeasured_opening_still_releases_observed_fork_but_fails_qualification(tmp_path, monkeypatch):
    monkeypatch.setattr(qualification, "cleanup_subject", lambda *a, **kw: {"status": "UNMEASURED"})
    monkeypatch.setattr(qualification, "cleanup_stimulus", lambda *a, **kw: {"status": "UNMEASURED"})
    (tmp_path / "fork-shutdown.json").write_text(
        json.dumps({"processes_stopped": True, "reason": "RELEASED", "observation_complete": True})
    )
    store = SimpleNamespace(request_cleanup=Mock(), release_fork=Mock())
    worker = SimpleNamespace(wait=Mock(), returncode=0)
    with pytest.raises(RuntimeError, match="did not complete subject cleanup"):
        qualification._close(SimpleNamespace(root=tmp_path), store, "lease", "wallet", worker)
    store.release_fork.assert_called_once_with("lease")
    retained = json.loads((tmp_path / "qualification-cleanup.json").read_text())
    assert retained["subject"]["status"] == "UNMEASURED"
    assert retained["fork_shutdown"]["processes_stopped"] is True
    assert retained["e2e_admission"] == "UNMEASURED"


def test_live_actor_prevents_fork_release(tmp_path, monkeypatch):
    import subprocess

    monkeypatch.setattr(qualification, "cleanup_subject", lambda *a, **kw: {"status": "PASS"})
    monkeypatch.setattr(qualification, "cleanup_stimulus", lambda *a, **kw: {"status": "PASS"})
    actor = SimpleNamespace(wait=Mock(side_effect=subprocess.TimeoutExpired("owned-actor", 30)))
    store = SimpleNamespace(request_cleanup=Mock(), release_fork=Mock())
    with pytest.raises(subprocess.TimeoutExpired):
        qualification._close(SimpleNamespace(root=tmp_path), store, "lease", "wallet", None, actors=[actor])
    store.release_fork.assert_called_once_with("lease")
    assert not (tmp_path / "qualification-cleanup.json").exists()


def test_bundle_is_assembled_after_process_shutdown_and_retains_controller_failure(tmp_path, monkeypatch):
    from qa_lab import e2e_bundle

    sequence = []
    monkeypatch.setattr(qualification, "cleanup_subject", lambda *a, **kw: {"status": "PASS"})
    monkeypatch.setattr(qualification, "cleanup_stimulus", lambda *a, **kw: {"status": "PASS"})
    worker = SimpleNamespace(returncode=None)

    def stopped(**kwargs):
        sequence.append("stopped")
        worker.returncode = 0
        (tmp_path / "fork-shutdown.json").write_text(
            json.dumps({"processes_stopped": True, "reason": "RELEASED", "observation_complete": True})
        )

    worker.wait = stopped
    store = SimpleNamespace(request_cleanup=Mock(), release_fork=lambda lease: sequence.append("release"))

    def assemble(*args, **kwargs):
        sequence.append("assemble")
        assert worker.returncode == 0
        controller = json.loads((tmp_path / "controller-result.json").read_text())
        assert controller["status"] == "FAIL"
        assert controller["monitor_error_type"] == "TimeoutError"
        assert controller["sampled_hold"] is None
        assert controller["fork_shutdown"]["processes_stopped"] is True
        return {"status": "INCOMPLETE"}

    monkeypatch.setattr(e2e_bundle, "assemble_bundle", assemble)
    result = qualification._close(
        SimpleNamespace(root=tmp_path, require_owned=lambda path: path),
        store,
        "lease",
        "wallet",
        worker,
        assemble=True,
        scenario_error_type="TimeoutError",
    )
    assert sequence == ["release", "stopped", "assemble"]
    assert result["bundle_assembly"]["status"] == "INCOMPLETE"
    assert result["terminal_boundary"]["status"] == "UNMEASURED"
    assert result["e2e_admission"] == "UNMEASURED"
