"""Tests for ``scripts/ci/pick_sidecar_matrix.py``.

Mirrors (and replaces) ``tests/ci/test_pick_sidecar_matrix.sh``. Spins up
a throwaway git repo under ``tmp_path`` and drives BASE_SHA → HEAD_SHA via
staged commits, asserting the emitted matrix JSON.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


PICKER = Path(__file__).resolve().parents[4] / "scripts" / "ci" / "pick_sidecar_matrix.py"


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    _git(repo_dir, "init", "-q", "-b", "main")
    _git(repo_dir, "config", "user.email", "ci@example.com")
    _git(repo_dir, "config", "user.name", "ci")

    for path in (
        "almanak/framework/connectors/aerodrome/adapter.py",
        "almanak/framework/connectors/uniswap_v3/adapter.py",
        "almanak/framework/connectors/morpho_blue/adapter.py",
        "almanak/framework/connectors/aave_v3/adapter.py",
        "almanak/gateway/server.py",
        "docs/readme.md",
    ):
        full = repo_dir / path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.touch()

    # Demo dirs referenced by the registry.
    for demo in ("aerodrome_lp", "uniswap_lp", "morpho_looping", "aave_borrow"):
        d = repo_dir / "almanak" / "demo_strategies" / demo
        d.mkdir(parents=True, exist_ok=True)
        (d / "marker").write_text(demo)

    (repo_dir / ".github").mkdir()
    (repo_dir / ".github" / "sidecar-demos.yml").write_text(
        textwrap.dedent(
            """
            connectors:
              aerodrome:
                demo_dir: almanak/demo_strategies/aerodrome_lp
                chain: base
                force_action: open
                max_iterations: 1
              uniswap_v3:
                demo_dir: almanak/demo_strategies/uniswap_lp
                chain: arbitrum
                force_action: open
                max_iterations: 1
              morpho_blue:
                demo_dir: almanak/demo_strategies/morpho_looping
                chain: ethereum
                force_action: supply
                max_iterations: 1
              aave_v3:
                demo_dir: almanak/demo_strategies/aave_borrow
                chain: arbitrum
                force_action: supply
                max_iterations: 1
            """
        ).strip()
    )
    _git(repo_dir, "add", "-A")
    _git(repo_dir, "commit", "-q", "-m", "base")
    return repo_dir


def _run_picker(repo: Path, base_sha: str, head_sha: str = "HEAD") -> dict:
    output_file = repo / "_gh_output"
    output_file.write_text("")
    env = os.environ.copy()
    env.update(
        {
            "BASE_SHA": base_sha,
            "HEAD_SHA": head_sha,
            "REGISTRY": str(repo / ".github" / "sidecar-demos.yml"),
            "GITHUB_OUTPUT": str(output_file),
            "PYTHONPATH": str(PICKER.parents[2]) + os.pathsep + env.get("PYTHONPATH", ""),
            # Suppress workflow_dispatch behavior unless the test sets it.
            "GITHUB_EVENT_NAME": env.get("GITHUB_EVENT_NAME", ""),
        }
    )
    proc = subprocess.run(
        [sys.executable, str(PICKER)],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"picker failed (rc={proc.returncode})\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    line = output_file.read_text().strip()
    assert line.startswith("matrix="), f"unexpected output: {line!r}"
    return json.loads(line[len("matrix=") :])


def _connectors(matrix: dict) -> set[str]:
    return {item["connector"] for item in matrix["include"]}


@pytest.fixture
def base_sha(repo: Path) -> str:
    return _git(repo, "rev-parse", "HEAD").stdout.strip()


def _checkout_branch(repo: Path, name: str) -> None:
    _git(repo, "checkout", "-q", "-b", name)


def _commit(repo: Path, msg: str) -> None:
    _git(repo, "commit", "-q", "-am", msg)


class TestPicker:
    def test_docs_only_yields_empty_matrix(self, repo: Path, base_sha: str):
        _checkout_branch(repo, "case_docs")
        (repo / "docs" / "readme.md").write_text("change")
        _commit(repo, "docs only")
        matrix = _run_picker(repo, base_sha)
        assert matrix["include"] == []

    def test_connector_dir_selects_only_that_connector(
        self, repo: Path, base_sha: str
    ):
        _checkout_branch(repo, "case_aerodrome")
        (repo / "almanak/framework/connectors/aerodrome/adapter.py").write_text("change")
        _commit(repo, "aerodrome only")
        matrix = _run_picker(repo, base_sha)
        assert _connectors(matrix) == {"aerodrome"}

    def test_demo_dir_selects_only_that_connector(self, repo: Path, base_sha: str):
        _checkout_branch(repo, "case_aerodrome_demo")
        (repo / "almanak/demo_strategies/aerodrome_lp/strategy.py").write_text("x")
        _git(repo, "add", "-A")
        _commit(repo, "aerodrome demo only")
        matrix = _run_picker(repo, base_sha)
        assert _connectors(matrix) == {"aerodrome"}

    def test_gateway_dir_selects_all(self, repo: Path, base_sha: str):
        _checkout_branch(repo, "case_gateway")
        (repo / "almanak/gateway/server.py").write_text("change")
        _commit(repo, "gateway change")
        matrix = _run_picker(repo, base_sha)
        assert _connectors(matrix) == {
            "aerodrome",
            "uniswap_v3",
            "morpho_blue",
            "aave_v3",
        }

    def test_non_connector_framework_change_runs_all(
        self, repo: Path, base_sha: str
    ):
        # Safer-by-default semantics: any framework change outside connectors/
        # is treated as cross-cutting and forces RUN-ALL. This caught the
        # April-29 sidecar coverage gap.
        _checkout_branch(repo, "case_framework")
        target = repo / "almanak/framework/unrelated/mod.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("x")
        _git(repo, "add", "-A")
        _commit(repo, "framework non-connector change")
        matrix = _run_picker(repo, base_sha)
        assert _connectors(matrix) == {
            "aerodrome",
            "uniswap_v3",
            "morpho_blue",
            "aave_v3",
        }

    def test_quarantined_connector_dropped_from_matrix(
        self, repo: Path, base_sha: str
    ):
        # Quarantine the demo backing the aerodrome connector entry.
        quarantine_path = repo / "scripts" / "ci" / "demo-quarantine.yml"
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        quarantine_path.write_text(
            textwrap.dedent(
                """
                quarantines:
                  - demo: aerodrome_lp
                    chain: base
                    ticket: VIB-9999
                    until: 2099-01-01
                    reason: pinned for the test
                """
            ).strip()
        )
        _git(repo, "add", "-A")
        _commit(repo, "quarantine aerodrome")
        env = os.environ.copy()
        env["GITHUB_EVENT_NAME"] = "workflow_dispatch"
        try:
            os.environ.update(env)
            matrix = _run_picker(repo, base_sha)
        finally:
            os.environ.pop("GITHUB_EVENT_NAME", None)
        connectors = _connectors(matrix)
        assert "aerodrome" not in connectors
        # Other connectors still selected.
        assert {"uniswap_v3", "morpho_blue", "aave_v3"} <= connectors

    def test_expired_quarantine_fails_picker(self, repo: Path, base_sha: str):
        quarantine_path = repo / "scripts" / "ci" / "demo-quarantine.yml"
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        quarantine_path.write_text(
            textwrap.dedent(
                """
                quarantines:
                  - demo: aerodrome_lp
                    chain: base
                    ticket: VIB-9999
                    until: 2000-01-01
                    reason: stale on purpose
                """
            ).strip()
        )
        _git(repo, "add", "-A")
        _commit(repo, "expired quarantine")
        env = os.environ.copy()
        env["GITHUB_EVENT_NAME"] = "workflow_dispatch"
        try:
            os.environ.update(env)
            with pytest.raises(AssertionError):
                _run_picker(repo, base_sha)
        finally:
            os.environ.pop("GITHUB_EVENT_NAME", None)

    def test_workflow_dispatch_runs_all(self, repo: Path, base_sha: str):
        env = os.environ.copy()
        env["GITHUB_EVENT_NAME"] = "workflow_dispatch"
        try:
            os.environ.update(env)
            matrix = _run_picker(repo, base_sha)
        finally:
            os.environ.pop("GITHUB_EVENT_NAME", None)
        assert _connectors(matrix) == {
            "aerodrome",
            "uniswap_v3",
            "morpho_blue",
            "aave_v3",
        }
