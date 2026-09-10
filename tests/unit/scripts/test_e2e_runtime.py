"""Pinned source tests use real Git worktrees and independent Python imports."""

import os
import stat
import subprocess
import sys

import pytest

from qa_lab.e2e_card import canonical, load_json
from qa_lab.e2e_runtime import freeze_runtime, runtime_environment, verify_runtime
from tests.unit.scripts import test_e2e_card as card_tests

checkout = card_tests.checkout


@pytest.fixture
def local_dependencies(runtime, tmp_path, monkeypatch):
    repo, output, manifest = runtime
    monkeypatch.setenv("UV_CACHE_DIR", str(tmp_path / "uv-cache"))
    (repo / "almanak/__init__.py").write_text("")
    (repo / "qa_lab").mkdir()
    (repo / "qa_lab/__init__.py").write_text("")
    (repo / "pyproject.toml").write_text(
        '[project]\nname="qa-environment-fixture"\nversion="0.0.0"\nrequires-python=">=3.12"\ndependencies=[]\n'
    )
    subprocess.run(["uv", "lock", "--offline", "--python", sys.executable], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.name=QA", "-c", "user.email=qa@example.invalid", "commit", "-qm", "dependency fixture"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    environment = tmp_path / "installed-environment"
    subprocess.run(["uv", "venv", "--python", sys.executable, str(environment)], check=True, capture_output=True)
    freeze_runtime(repo, checkout=output, manifest=manifest)
    return manifest, environment, tmp_path / "dependency-environment.json"


def test_existing_environment_is_bound_without_installation(local_dependencies, tmp_path):
    from qa_lab.e2e_runtime import bind_dependencies, verify_dependencies

    manifest, environment, binding = local_dependencies
    def inventory():
        return sorted(
            str(path.relative_to(environment))
            for path in environment.rglob("*")
            if path.name != ".lock"
        )

    before = inventory()
    value = bind_dependencies(manifest, environment, binding)
    assert verify_dependencies(manifest, binding) == value
    assert inventory() == before
    state = tmp_path / "process-state"
    state.mkdir()
    launch = runtime_environment(manifest, state=state)
    assert launch["UV_PROJECT_ENVIRONMENT"] == str(environment)
    assert launch["ALMANAK_QA_DEPENDENCY_MANIFEST"] == str(binding)
    assert not (state / "venv").exists()


def test_changed_installed_package_inventory_refuses_reuse(local_dependencies):
    from qa_lab.e2e_runtime import bind_dependencies, verify_dependencies

    manifest, environment, binding = local_dependencies
    bind_dependencies(manifest, environment, binding)
    site = next((environment / "lib").glob("python*/site-packages"))
    added = site / "unexpected_package-1.0.dist-info"
    added.mkdir()
    (added / "METADATA").write_text("Metadata-Version: 2.1\nName: unexpected-package\nVersion: 1.0\n")
    with pytest.raises(ValueError, match="environment changed"):
        verify_dependencies(manifest, binding)


def test_missing_environment_cannot_leave_successful_binding(local_dependencies):
    from qa_lab.e2e_runtime import bind_dependencies

    manifest, environment, binding = local_dependencies
    with pytest.raises(ValueError, match="existing canonical"):
        bind_dependencies(manifest, environment / "missing", binding)
    assert not binding.exists()


def test_deleted_binding_cannot_fall_back_to_unqualified_environment(local_dependencies, tmp_path):
    from qa_lab.e2e_card import digest
    from qa_lab.e2e_runtime import bind_dependencies

    manifest, environment, binding = local_dependencies
    bind_dependencies(manifest, environment, binding)
    (tmp_path / "supervisor.json").write_bytes(
        canonical({"dependency_environment_sha256": digest(binding.read_bytes())})
    )
    binding.unlink()
    state = tmp_path / "process-state"
    state.mkdir()
    with pytest.raises(ValueError, match="binding is missing"):
        runtime_environment(manifest, state=state)


@pytest.fixture
def runtime(checkout, tmp_path):
    output = tmp_path / "execution"
    manifest = tmp_path / "runtime.json"
    try:
        yield checkout, output, manifest
    finally:
        if output.exists():
            for path in (output, *output.rglob("*")):
                if not path.is_symlink():
                    path.chmod(stat.S_IMODE(path.stat().st_mode) | 0o700)
            subprocess.run(["git", "worktree", "unlock", str(output)], cwd=checkout, capture_output=True)
            subprocess.run(["git", "worktree", "remove", "--force", str(output)], cwd=checkout, capture_output=True)


def test_execution_imports_remain_pinned_after_development_changes(runtime, tmp_path):
    repo, output, manifest = runtime
    (repo / "almanak/runner.py").write_text("VALUE = 'prepared'\n")
    (repo / "almanak/__init__.py").write_text("")
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.name=QA", "-c", "user.email=qa@example.invalid", "commit", "-qm", "runtime"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    (repo / ".git/info/exclude").write_text(".env\n")
    (repo / ".env").write_text("SYNTHETIC_CREDENTIAL=not-a-real-secret\n")
    value = freeze_runtime(repo, checkout=output, manifest=manifest)
    (repo / "almanak/runner.py").write_text("VALUE = 'edited'\n")
    assert verify_runtime(manifest)["commit"] == value["commit"]
    assert not (output / ".env").exists()
    state = tmp_path / "process-state"
    state.mkdir()
    environment = {**os.environ, **runtime_environment(manifest, state=state)}
    result = subprocess.run(
        [sys.executable, "-c", "from almanak.runner import VALUE; print(VALUE)"],
        cwd=output,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == "prepared"
    assert not (output / "almanak/__pycache__").exists()
    assert list((state / "pycache").rglob("*.pyc"))


@pytest.mark.parametrize("mutation", ["bytes", "permissions", "directory", "unlocked", "manifest"])
def test_runtime_verification_refuses_source_drift(runtime, mutation):
    repo, output, manifest = runtime
    freeze_runtime(repo, checkout=output, manifest=manifest)
    target = output / "almanak/runner.py"
    if mutation == "bytes":
        target.chmod(0o600)
        target.write_text("changed\n")
        target.chmod(0o400)
    elif mutation == "permissions":
        target.chmod(0o600)
    elif mutation == "directory":
        target.parent.chmod(0o700)
    elif mutation == "unlocked":
        subprocess.run(["git", "worktree", "unlock", str(output)], cwd=repo, check=True, capture_output=True)
    else:
        value = load_json(manifest)
        value["commit"] = "a" * 40
        manifest.write_bytes(canonical(value))
    with pytest.raises(ValueError, match="changed|writable|detached and locked"):
        verify_runtime(manifest)


def test_dirty_development_checkout_cannot_be_frozen(runtime):
    repo, output, manifest = runtime
    (repo / "almanak/runner.py").write_text("uncommitted\n")
    with pytest.raises(ValueError, match="committed and clean"):
        freeze_runtime(repo, checkout=output, manifest=manifest)
    assert not output.exists()
    assert not manifest.exists()


def test_subject_worker_requires_the_pinned_execution_checkout(runtime, tmp_path, monkeypatch):
    from qa_lab.e2e_card import REPO, TEMPLATE, bind_pool_input, prepare
    from qa_lab.e2e_worker import subject_command

    repo, output, manifest = runtime
    freeze_runtime(repo, checkout=output, manifest=manifest)
    run = tmp_path / "run"
    preparation = run / "preparation"
    prepare(output, REPO / TEMPLATE, preparation, "pinned-runtime-test")
    anchor = run / "anchor.json"
    anchor.write_bytes(canonical({"chain_id": 42161, "fork_block": 100, "fork_hash": "0x" + "ab" * 32}))
    bind_pool_input(preparation, output, run / "subject", anchor, run / "pool-input.json")
    monkeypatch.setenv("ALMANAK_QA_RUNTIME_MANIFEST", str(manifest))
    (repo / "almanak/runner.py").write_text("development changed\n")
    command, environment = subject_command(preparation, output, gateway_port=51234, supervised=True)
    assert command[:6] == ("uv", "run", "--no-sync", "almanak", "strat", "run")
    assert environment["PYTHONPATH"] == str(output)
    with pytest.raises(ValueError, match="not executing from its pinned"):
        subject_command(preparation, repo, gateway_port=51234, supervised=True)
    (output / "almanak").chmod(0o700)
    with pytest.raises(ValueError, match="writable"):
        subject_command(preparation, output, gateway_port=51234, supervised=True)


def test_shared_storage_is_refused_before_checkout_creation(runtime):
    repo, output, manifest = runtime
    original = stat.S_IMODE(output.parent.stat().st_mode)
    output.parent.chmod(0o755)
    try:
        with pytest.raises(ValueError, match="owned private storage"):
            freeze_runtime(repo, checkout=output, manifest=manifest)
    finally:
        output.parent.chmod(original)
    assert not output.exists()


def test_staged_runtime_survives_loss_of_image_seed(checkout, tmp_path):
    from qa_lab.e2e_runtime import stage_runtime

    (checkout / ".git/info/exclude").write_text(".env\n")
    (checkout / ".env").write_text("SYNTHETIC_IGNORED_INPUT=not-a-secret\n")
    root = tmp_path / "persistent-runtime"
    try:
        result = stage_runtime(checkout, root=root)
        common = subprocess.check_output(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"], cwd=root / "source", text=True
        ).strip()
        assert common == str(root / "seed.git")
        assert not (root / "seed.git/almanak").exists()
        assert (
            subprocess.check_output(
                ["git", "rev-parse", "--is-bare-repository"], cwd=root / "seed.git", text=True
            ).strip()
            == "true"
        )
        assert not (root / "source/.env").exists()
        checkout.rename(tmp_path / "removed-image-seed")
        assert verify_runtime(root / "runtime.json")["commit"] == result["commit"]
        with pytest.raises(ValueError, match="fresh canonical"):
            stage_runtime(root / "source", root=root)
    finally:
        if root.exists():
            for path in (root, *root.rglob("*")):
                if not path.is_symlink():
                    path.chmod(stat.S_IMODE(path.stat().st_mode) | 0o700)


def test_dirty_image_seed_cannot_create_persistent_runtime(checkout, tmp_path):
    from qa_lab.e2e_runtime import stage_runtime

    (checkout / "almanak/runner.py").write_text("modified\n")
    root = tmp_path / "runtime"
    with pytest.raises(ValueError, match="committed and clean"):
        stage_runtime(checkout, root=root)
    assert not root.exists()
