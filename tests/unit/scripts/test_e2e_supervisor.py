"""Supervisor preparation composes real pinned source and SQLite ownership."""

import shutil
import subprocess
from types import SimpleNamespace

import pytest

from qa_lab import e2e_supervisor as supervisor
from qa_lab.e2e_card import REPO, TEMPLATE, canonical, digest, load_json, verify_preparation
from qa_lab.e2e_ownership import OwnershipStore
from qa_lab.e2e_runtime import freeze_runtime
from tests.unit.scripts import test_e2e_runtime as runtime_tests

checkout = runtime_tests.checkout
runtime_parts = runtime_tests.runtime


@pytest.fixture
def prepared_source(runtime_parts, tmp_path, monkeypatch):
    monkeypatch.setattr(
        supervisor.shutil, "disk_usage", lambda path: SimpleNamespace(free=4 * supervisor.MINIMUM_FREE_BYTES)
    )
    repo, runtime, manifest = runtime_parts
    template = repo / TEMPLATE
    template.parent.mkdir(parents=True)
    shutil.copyfile(REPO / TEMPLATE, template)
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.name=QA", "-c", "user.email=qa@example.invalid", "commit", "-qm", "scenario"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    freeze_runtime(repo, checkout=runtime, manifest=manifest)
    monkeypatch.setattr(supervisor, "REPO", runtime)
    anchor = tmp_path / "anchor.json"
    anchor.write_bytes(canonical({"chain_id": 42161, "fork_block": 100, "fork_hash": "0x" + "ab" * 32}))
    return runtime, manifest, anchor, tmp_path / "run"


def prepare(parts):
    _, manifest, anchor, root = parts
    return supervisor.prepare_run(runtime_manifest=manifest, root=root, anchor=anchor, run_id="supervisor-test-run")


def test_prepared_supervisor_binds_inputs_but_has_no_launch_authority(prepared_source):
    runtime, _, anchor, root = prepared_source
    result = prepare(prepared_source)
    assert load_json(root / "supervisor.json") == result
    assert result["launch_status"] == "BLOCKED"
    assert result["execution_status"] == "UNMEASURED"
    assert result["fork_anchor_status"] == "SUPPLIED_UNVERIFIED"
    assert result["card_sha256"] == digest((root / "preparation/card.json").read_bytes())
    assert load_json(root / "fork-anchor.json") == load_json(anchor)
    assert verify_preparation(root / "preparation", runtime)["run_id"] == "supervisor-test-run"
    state = OwnershipStore(root / "ownership.sqlite").snapshot()
    assert state["ownership"]["card_hash"] == result["card_sha256"]
    assert state["ownership"]["generation"] == 0
    assert state["ownership"]["owner"] is None
    assert state["launches"] == []
    assert not (root / "context.json").exists()
    assert not (root / "subject/almanak_state.db").exists()
    assert result["environment"]["PYTHONPATH"] == str(runtime)
    assert result["environment"]["UV_PROJECT_ENVIRONMENT"] == str(root / "process-state/venv")
    before = (root / "supervisor.json").read_bytes()
    with pytest.raises(ValueError, match="fresh"):
        prepare(prepared_source)
    assert (root / "supervisor.json").read_bytes() == before


def test_low_disk_refuses_preparation_without_creating_run_or_deleting_history(prepared_source, monkeypatch):
    root = prepared_source[-1]
    retained = root.parent / "retained-proof.json"
    retained.write_bytes(b"existing evidence")
    monkeypatch.setattr(
        supervisor.shutil, "disk_usage", lambda path: SimpleNamespace(free=supervisor.MINIMUM_FREE_BYTES - 1)
    )
    with pytest.raises(OSError, match="2 GiB"):
        prepare(prepared_source)
    assert not root.exists()
    assert retained.read_bytes() == b"existing evidence"


def test_storage_preflight_measures_current_volume_and_does_not_reuse_old_reading(tmp_path, monkeypatch):
    available = [supervisor.MINIMUM_FREE_BYTES]
    paths = []

    def measure(path):
        paths.append(path)
        return SimpleNamespace(free=available[0])

    monkeypatch.setattr(supervisor.shutil, "disk_usage", measure)
    root = tmp_path / "new-run"
    result = supervisor.storage_preflight(root)
    assert result["free_bytes"] == supervisor.MINIMUM_FREE_BYTES
    assert paths == [tmp_path]
    root.mkdir()
    available[0] = 0
    with pytest.raises(OSError, match="retained evidence was not deleted"):
        supervisor.storage_preflight(root)
    assert paths[-1] == root
    assert root.is_dir()


@pytest.mark.parametrize("mutation", ["chain", "height", "hash", "extra"])
def test_anchor_must_be_explicit_and_bounded_before_creating_run(prepared_source, mutation):
    _, _, anchor, root = prepared_source
    value = load_json(anchor)
    if mutation == "chain":
        value["chain_id"] = 1
    elif mutation == "height":
        value["fork_block"] = True
    elif mutation == "hash":
        value["fork_hash"] = "unknown"
    else:
        value["rpc_url"] = "https://example.invalid/synthetic-secret"
    anchor.write_bytes(canonical(value))
    with pytest.raises(ValueError, match="exact Arbitrum"):
        prepare(prepared_source)
    assert not root.exists()


def test_preparer_cannot_execute_from_development_checkout(prepared_source, monkeypatch):
    monkeypatch.setattr(supervisor, "REPO", REPO)
    with pytest.raises(ValueError, match="pinned source checkout"):
        prepare(prepared_source)
    assert not prepared_source[-1].exists()


def test_materialization_change_prevents_completed_supervisor_record(prepared_source, monkeypatch):
    original = supervisor.bind_pool_input

    def altered(*args):
        result = original(*args)
        (prepared_source[-1] / "subject/strategy.py").write_text("changed after materialization\n")
        return result

    monkeypatch.setattr(supervisor, "bind_pool_input", altered)
    with pytest.raises(ValueError, match="materialization changed"):
        prepare(prepared_source)
    assert not (prepared_source[-1] / "supervisor.json").exists()
    assert OwnershipStore(prepared_source[-1] / "ownership.sqlite").snapshot()["launches"] == []


def test_local_storage_preserves_evidence_across_invocations(tmp_path, monkeypatch):
    monkeypatch.delenv("ALMANAK_QA_RUNS_ROOT", raising=False)
    monkeypatch.setenv("ALMANAK_QA_STORE", str(tmp_path / "board"))
    root = supervisor.local_runs_root()
    assert root == tmp_path / "board/runs"
    evidence = root / "retained-proof.json"
    evidence.write_text('{"status": "UNMEASURED"}')
    assert supervisor.local_runs_root() == root
    assert evidence.read_text() == '{"status": "UNMEASURED"}'
    assert root.stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize("unsafe", ["symlink", "permissions"])
def test_local_storage_refuses_unsafe_existing_directory(tmp_path, monkeypatch, unsafe):
    monkeypatch.delenv("ALMANAK_QA_RUNS_ROOT", raising=False)
    monkeypatch.setenv("ALMANAK_QA_STORE", str(tmp_path))
    root = tmp_path / "runs"
    if unsafe == "symlink":
        target = tmp_path / "other"
        target.mkdir(mode=0o700)
        root.symlink_to(target, target_is_directory=True)
    else:
        root.mkdir(mode=0o755)
        root.chmod(0o755)
    with pytest.raises(ValueError, match="symlinks|mode 0700"):
        supervisor.local_runs_root()
    assert root.is_symlink() if unsafe == "symlink" else root.stat().st_mode & 0o777 == 0o755


def test_persistent_run_environment_retains_storage_identity(prepared_source, monkeypatch):
    root = prepared_source[-1]
    monkeypatch.setenv("ALMANAK_QA_RUNS_ROOT", str(root.parent))
    result = prepare(prepared_source)
    assert result["environment"]["ALMANAK_QA_RUNS_ROOT"] == str(root.parent)


@pytest.fixture
def ready_local_subject(prepared_source, monkeypatch):
    result = prepare(prepared_source)
    root = prepared_source[-1]
    store = OwnershipStore(root / "ownership.sqlite")
    lease = store.acquire("controller")
    (root / "cleanup-ready.json").write_bytes(
        canonical(
            {
                "pid": 123,
                "run_id": result["run_id"],
                "card_sha256": result["card_sha256"],
                "owner": "backup",
            }
        )
    )
    monkeypatch.setattr(
        supervisor,
        "runtime_environment",
        lambda *a, **kw: {
            "UV_PROJECT_ENVIRONMENT": str(root / "test-environment"),
            "ALMANAK_QA_DEPENDENCY_MANIFEST": str(root / "dependency-environment.json"),
        },
    )
    return root, store, lease


def test_dead_cleanup_child_cannot_reserve_subject(ready_local_subject):
    root, store, lease = ready_local_subject
    dead = SimpleNamespace(pid=123, poll=lambda: 1)
    with pytest.raises(ValueError, match="live cleanup child"):
        supervisor.start_local_subject(root, store, lease, dead, gateway_port=50071)
    assert store.snapshot()["launches"] == []


def test_disk_exhaustion_after_preparation_prevents_reservation_but_allows_cleanup(ready_local_subject, monkeypatch):
    root, store, lease = ready_local_subject
    monkeypatch.setattr(supervisor.shutil, "disk_usage", lambda path: SimpleNamespace(free=0))
    with pytest.raises(OSError, match="2 GiB"):
        supervisor.start_local_subject(
            root, store, lease, SimpleNamespace(pid=123, poll=lambda: None), gateway_port=50071
        )
    assert store.snapshot()["launches"] == []
    store.request_cleanup(lease)
    assert store.snapshot()["ownership"]["cleanup"] == 1


def test_failed_subject_spawn_retains_nonretryable_reservation(ready_local_subject, monkeypatch):
    from qa_lab.e2e_ownership import OwnershipError

    root, store, lease = ready_local_subject
    alive = SimpleNamespace(pid=123, poll=lambda: None)

    def failed(*args, **kwargs):
        raise OSError("process creation failed")

    monkeypatch.setattr(supervisor, "subprocess", SimpleNamespace(Popen=failed, DEVNULL=-3, STDOUT=-2))
    with pytest.raises(OSError, match="process creation"):
        supervisor.start_local_subject(root, store, lease, alive, gateway_port=50071)
    launches = store.snapshot()["launches"]
    assert len(launches) == 1 and launches[0]["claimed"] == 0
    with pytest.raises(OwnershipError):
        supervisor.start_local_subject(root, store, lease, alive, gateway_port=50071)


@pytest.mark.parametrize("mutation", ["dead", "pid", "run_id", "card_sha256", "owner"])
def test_actor_rejects_unbound_cleanup_before_reservation(ready_local_subject, mutation):
    root, store, lease = ready_local_subject
    readiness = load_json(root / "cleanup-ready.json")
    if mutation != "dead":
        readiness[mutation] = lease.owner if mutation == "owner" else "another-run"
        (root / "cleanup-ready.json").write_bytes(canonical(readiness))
    backup = SimpleNamespace(pid=123, poll=lambda: 1 if mutation == "dead" else None)
    with pytest.raises(ValueError, match="cleanup child bound to this run"):
        supervisor.start_local_actor(SimpleNamespace(root=root), store, lease, backup, gateway_port=50072)
    assert store.snapshot()["launches"] == []


def test_failed_actor_spawn_retains_nonretryable_reservation(ready_local_subject, monkeypatch):
    from qa_lab import e2e_actor_worker
    from qa_lab.e2e_ownership import OwnershipError

    root, store, lease = ready_local_subject
    backup = SimpleNamespace(pid=123, poll=lambda: None)
    monkeypatch.setattr(e2e_actor_worker, "actor_command", lambda *args, **kwargs: [])

    def failed(*args, **kwargs):
        raise OSError("process creation failed")

    monkeypatch.setattr(supervisor, "subprocess", SimpleNamespace(Popen=failed, DEVNULL=-3, STDOUT=-2))
    with pytest.raises(OSError, match="process creation"):
        supervisor.start_local_actor(SimpleNamespace(root=root), store, lease, backup, gateway_port=50072)
    launches = store.snapshot()["launches"]
    assert len(launches) == 1 and launches[0]["role"] == "stimulus" and launches[0]["claimed"] == 0
    with pytest.raises(OwnershipError):
        supervisor.start_local_actor(SimpleNamespace(root=root), store, lease, backup, gateway_port=50072)


@pytest.mark.parametrize(
    "flags",
    [
        ["--run-scenario"],
        ["--run-scenario", "--use-anvil-stimulus-wallet", "--actor-gateway-port", "50071"],
        ["--use-anvil-stimulus-wallet"],
        ["--run-scenario", "--qualify-processes", "--use-anvil-stimulus-wallet"],
    ],
)
def test_scenario_cli_rejects_ambiguous_launch_before_preparation(monkeypatch, flags):
    from unittest.mock import Mock

    prepare = Mock(side_effect=AssertionError("Preflight must precede preparation"))
    monkeypatch.setattr(supervisor, "prepare_run", prepare)
    monkeypatch.setattr(
        supervisor.sys,
        "argv",
        [
            "e2e_supervisor",
            "--runtime-manifest",
            "/unused/runtime.json",
            "--anchor",
            "/unused/anchor.json",
            "--run-id",
            "preflight-test",
            "--environment",
            "/unused/venv",
            *flags,
        ],
    )
    with pytest.raises(SystemExit) as result:
        supervisor.main()
    assert result.value.code == 2
    prepare.assert_not_called()
