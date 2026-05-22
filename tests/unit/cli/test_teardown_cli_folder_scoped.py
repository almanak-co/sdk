"""VIB-3835: teardown CLI folder-scoped resolution + --wait flag.

Pins the new contract for ``almanak strat teardown {request,status,list,cancel}``:

* ``-d / --working-dir`` is accepted on all four subcommands.
* The resolver exports ``ALMANAK_STRATEGY_FOLDER`` so the strategy-scoped
  DB path (``local_strategy_db_path``) lands in the strategy's folder.
* When neither ``-d`` nor ``ALMANAK_STRATEGY_FOLDER`` resolve and the cwd
  is not a strategy folder, the command hard-fails — no silent fallback
  to the per-user utility DB (the May 1 mainnet teardown failure mode).
* ``request --wait`` blocks until the runner reaches a terminal state,
  with progressive output, and exits with the appropriate code.
* ``request --wait --timeout N`` exits non-zero with a hint when no
  runner ever picks up the request.

Test IDs T-3835-1..T-3835-12.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak.framework.cli.teardown import teardown as teardown_cli
from almanak.framework.teardown import (
    TeardownAssetPolicy,
    TeardownMode,
    TeardownPhase,
    TeardownRequest,
    TeardownStateManager,
    TeardownStatus,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Clear every env var the resolver consults."""
    for var in (
        "ALMANAK_IS_HOSTED",
        "ALMANAK_DEPLOYMENT_ID",
        "ALMANAK_STATE_DB",
        "ALMANAK_STRATEGY_FOLDER",
        "ALMANAK_GATEWAY_DB_PATH",
        "XDG_DATA_HOME",
    ):
        monkeypatch.delenv(var, raising=False)
    # Also blow away the cached singleton so each test reads from a fresh DB.
    from almanak.framework.teardown import state_manager as sm

    sm._default_manager = None
    return monkeypatch


@pytest.fixture
def strategy_folder(tmp_path: Path) -> Path:
    """A directory that ``_looks_like_strategy_folder`` recognises."""
    folder = tmp_path / "my_strat"
    folder.mkdir()
    (folder / "config.json").write_text("{}", encoding="utf-8")
    (folder / "strategy.py").write_text("# stub\n", encoding="utf-8")
    return folder


# ---------------------------------------------------------------------------
# T-3835-1..4: -d resolution + env export across all 4 subcommands
# ---------------------------------------------------------------------------
def test_t_3835_1_request_resolves_d_flag(clean_env, strategy_folder: Path) -> None:
    """T-3835-1: ``request -d <folder>`` writes the row into <folder>/almanak_state.db."""
    runner = CliRunner()
    result = runner.invoke(
        teardown_cli,
        [
            "request",
            "-d",
            str(strategy_folder),
            "-s",
            "TestStrat:abc",
            "--mode",
            "graceful",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    db_file = strategy_folder / "almanak_state.db"
    assert db_file.exists(), "request did not create the strategy-folder DB"
    # The row should be readable from a manager opened on the same path.
    mgr = TeardownStateManager(db_path=str(db_file))
    found = mgr.get_request("TestStrat:abc")
    assert found is not None
    assert found.mode == TeardownMode.SOFT


def test_t_3835_2_status_reads_from_d_flag_folder(clean_env, strategy_folder: Path) -> None:
    """T-3835-2: ``status -d <folder>`` reads the row written into <folder>'s DB."""
    db_file = strategy_folder / "almanak_state.db"
    mgr = TeardownStateManager(db_path=str(db_file))
    mgr.create_request(
        TeardownRequest(
            deployment_id="TestStrat:def",
            mode=TeardownMode.SOFT,
            asset_policy=TeardownAssetPolicy.TARGET_TOKEN,
            target_token="USDC",
            requested_by="test",
        )
    )

    runner = CliRunner()
    result = runner.invoke(
        teardown_cli,
        ["status", "-d", str(strategy_folder), "-s", "TestStrat:def", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert payload["deployment_id"] == "TestStrat:def"


def test_t_3835_3_list_uses_d_flag_db(clean_env, strategy_folder: Path) -> None:
    """T-3835-3: ``list -d <folder> --json`` enumerates only that folder's DB."""
    db_file = strategy_folder / "almanak_state.db"
    mgr = TeardownStateManager(db_path=str(db_file))
    mgr.create_request(
        TeardownRequest(
            deployment_id="StratA:1",
            mode=TeardownMode.SOFT,
            asset_policy=TeardownAssetPolicy.TARGET_TOKEN,
            target_token="USDC",
            requested_by="test",
        )
    )

    runner = CliRunner()
    result = runner.invoke(
        teardown_cli,
        ["list", "-d", str(strategy_folder), "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert {row["deployment_id"] for row in payload} == {"StratA:1"}


def test_t_3835_4_cancel_targets_d_flag_db(clean_env, strategy_folder: Path) -> None:
    """T-3835-4: ``cancel -d <folder>`` flips cancel_requested in that folder's DB."""
    db_file = strategy_folder / "almanak_state.db"
    mgr = TeardownStateManager(db_path=str(db_file))
    mgr.create_request(
        TeardownRequest(
            deployment_id="StratC:1",
            mode=TeardownMode.SOFT,
            asset_policy=TeardownAssetPolicy.TARGET_TOKEN,
            target_token="USDC",
            requested_by="test",
        )
    )

    runner = CliRunner()
    result = runner.invoke(
        teardown_cli,
        ["cancel", "-d", str(strategy_folder), "-s", "StratC:1", "--force"],
    )

    assert result.exit_code == 0, result.output
    refreshed = TeardownStateManager(db_path=str(db_file)).get_request("StratC:1")
    assert refreshed is not None
    assert refreshed.cancel_requested is True


# ---------------------------------------------------------------------------
# T-3835-5..7: cwd auto-detect + hard-fail
# ---------------------------------------------------------------------------
def test_t_3835_5_cwd_autodetect_when_no_d_flag(
    clean_env, monkeypatch: pytest.MonkeyPatch, strategy_folder: Path
) -> None:
    """T-3835-5: when -d is omitted but cwd has config.json, cwd is the folder."""
    monkeypatch.chdir(strategy_folder)
    runner = CliRunner()
    result = runner.invoke(
        teardown_cli,
        ["request", "-s", "StratCwd:1", "--force"],
    )
    assert result.exit_code == 0, result.output
    assert (strategy_folder / "almanak_state.db").exists()


def test_t_3835_6_hard_fails_when_no_folder_resolves(
    clean_env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T-3835-6: no -d, no env var, cwd is not a strategy folder → hard-fail.

    Critically: the failure must be a non-zero CLI exit and the message must
    name the remediation. Any silent fallback here re-opens the May 1 bug.
    """
    bare = tmp_path / "not_a_strategy"
    bare.mkdir()
    monkeypatch.chdir(bare)
    runner = CliRunner()
    result = runner.invoke(
        teardown_cli,
        ["request", "-s", "Stale:1", "--force"],
    )
    assert result.exit_code != 0
    assert "no strategy folder resolved" in result.output
    assert "--working-dir" in result.output


def test_t_3835_7_hard_fails_when_d_path_lacks_strategy_files(clean_env, tmp_path: Path) -> None:
    """T-3835-7: -d points at a real dir but it has no config.json/strategy.py."""
    junk = tmp_path / "junk"
    junk.mkdir()
    runner = CliRunner()
    result = runner.invoke(
        teardown_cli,
        ["request", "-d", str(junk), "-s", "Stale:1", "--force"],
    )
    assert result.exit_code != 0
    assert "does not look like a strategy folder" in result.output


def test_t_3835_8_env_var_resolution_is_honoured(
    clean_env, monkeypatch: pytest.MonkeyPatch, strategy_folder: Path
) -> None:
    """T-3835-8: a parent process exporting ALMANAK_STRATEGY_FOLDER is honoured.

    This is the ``strat run`` → spawned-teardown path: no -d flag, cwd may be
    elsewhere, but the env var points at the strategy folder.
    """
    other_dir = strategy_folder.parent / "elsewhere"
    other_dir.mkdir()
    monkeypatch.chdir(other_dir)
    monkeypatch.setenv("ALMANAK_STRATEGY_FOLDER", str(strategy_folder))

    runner = CliRunner()
    result = runner.invoke(
        teardown_cli,
        ["request", "-s", "EnvStrat:1", "--force"],
    )
    assert result.exit_code == 0, result.output
    assert (strategy_folder / "almanak_state.db").exists()


# ---------------------------------------------------------------------------
# T-3835-9..12: --wait state progression and timeout
# ---------------------------------------------------------------------------
def _drive_runner_to_completion(
    db_file: Path,
    deployment_id: str,
    *,
    delay_per_step: float = 0.3,
    final_status: TeardownStatus = TeardownStatus.COMPLETED,
) -> None:
    """Background helper: simulate a runner driving the request through states."""
    time.sleep(delay_per_step)
    mgr = TeardownStateManager(db_path=str(db_file))
    # Acknowledge. Wait briefly for the CLI to create the request so the
    # thread isn't racing the CLI's confirmation/import overhead.
    req = None
    for _ in range(20):  # ~2s max wait
        req = mgr.get_active_request(deployment_id)
        if req is not None:
            break
        time.sleep(0.1)
    if req is None:
        return
    req.acknowledged_at = datetime.now(UTC)
    req.status = TeardownStatus.CANCEL_WINDOW
    mgr.update_request(req)
    time.sleep(delay_per_step)
    # Start.
    req.started_at = datetime.now(UTC)
    req.status = TeardownStatus.EXECUTING
    req.current_phase = TeardownPhase.POSITION_CLOSURE
    req.positions_total = 2
    mgr.update_request(req)
    time.sleep(delay_per_step)
    # Progress.
    req.positions_closed = 2
    mgr.update_request(req)
    time.sleep(delay_per_step)
    # Terminal.
    req.status = final_status
    req.completed_at = datetime.now(UTC)
    mgr.update_request(req)


def test_t_3835_9_wait_returns_zero_on_completion(clean_env, strategy_folder: Path) -> None:
    """T-3835-9: --wait blocks through the state ladder and returns 0 on COMPLETED."""
    db_file = strategy_folder / "almanak_state.db"
    deployment_id = "WaitStrat:1"

    # Pre-create the DB by writing a request synchronously, then immediately
    # spawn the simulated runner thread before the CLI starts polling. The CLI
    # writes its OWN request as it runs; the runner thread observes whichever
    # is active.
    runner_thread = threading.Thread(
        target=_drive_runner_to_completion,
        kwargs={
            "db_file": db_file,
            "deployment_id": deployment_id,
            "delay_per_step": 0.3,
        },
        daemon=True,
    )
    runner_thread.start()

    cli_runner = CliRunner()
    result = cli_runner.invoke(
        teardown_cli,
        [
            "request",
            "-d",
            str(strategy_folder),
            "-s",
            deployment_id,
            "--mode",
            "graceful",
            "--force",
            "--wait",
            "--timeout",
            "30",
        ],
    )
    runner_thread.join(timeout=10)

    assert result.exit_code == 0, result.output
    out = result.output
    assert "acknowledged" in out
    assert "started" in out
    assert "completed" in out.lower()


def test_t_3835_10_wait_returns_nonzero_on_failure(clean_env, strategy_folder: Path) -> None:
    """T-3835-10: --wait returns non-zero when the runner reports FAILED."""
    db_file = strategy_folder / "almanak_state.db"
    deployment_id = "WaitFail:1"

    runner_thread = threading.Thread(
        target=_drive_runner_to_completion,
        kwargs={
            "db_file": db_file,
            "deployment_id": deployment_id,
            "delay_per_step": 0.3,
            "final_status": TeardownStatus.FAILED,
        },
        daemon=True,
    )
    runner_thread.start()

    cli_runner = CliRunner()
    result = cli_runner.invoke(
        teardown_cli,
        [
            "request",
            "-d",
            str(strategy_folder),
            "-s",
            deployment_id,
            "--force",
            "--wait",
            "--timeout",
            "30",
        ],
    )
    runner_thread.join(timeout=10)

    assert result.exit_code != 0, result.output
    assert "FAILED" in result.output


def test_t_3835_11_wait_times_out_when_no_runner(clean_env, strategy_folder: Path) -> None:
    """T-3835-11: --wait --timeout 3 with no runner exits non-zero with the hint."""
    cli_runner = CliRunner()
    result = cli_runner.invoke(
        teardown_cli,
        [
            "request",
            "-d",
            str(strategy_folder),
            "-s",
            "Orphan:1",
            "--force",
            "--wait",
            "--timeout",
            "3",
        ],
    )
    assert result.exit_code != 0
    assert "timeout waiting for runner" in result.output.lower()
    assert "is the runner running" in result.output.lower()


def test_t_3835_12_wait_off_is_fire_and_forget(clean_env, strategy_folder: Path) -> None:
    """T-3835-12: default behaviour (no --wait) returns immediately with exit 0."""
    cli_runner = CliRunner()
    result = cli_runner.invoke(
        teardown_cli,
        [
            "request",
            "-d",
            str(strategy_folder),
            "-s",
            "FireForget:1",
            "--force",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Teardown request created" in result.output
    # Should NOT have any of the --wait progress strings.
    assert "acknowledged" not in result.output
    assert "completed" not in result.output.lower()


# ---------------------------------------------------------------------------
# VIB-3837: --wait Ctrl-C handler returns 130 with operator-friendly message
# ---------------------------------------------------------------------------
def test_t_3837_1_wait_returns_130_on_keyboard_interrupt(
    clean_env, strategy_folder: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ctrl-C during --wait exits 130 with a yellow info line, not a traceback."""
    from almanak.framework.cli import teardown as teardown_module

    deployment_id = "InterruptStrat:1"

    # Pre-create a pending row so the wait loop has something to read on the
    # first poll. Then make the SECOND poll raise KeyboardInterrupt to
    # simulate the operator hitting Ctrl-C while the runner is still working.
    db_file = strategy_folder / "almanak_state.db"
    pre_mgr = TeardownStateManager(db_path=str(db_file))
    pre_mgr.create_request(
        TeardownRequest(
            deployment_id=deployment_id,
            mode=TeardownMode.SOFT,
            asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS,
            requested_by="test",
        )
    )

    real_sleep = time.sleep

    def interrupting_sleep(_seconds: float) -> None:
        # Skip the first sleep (let the loop tick once) so the test exercises
        # the SIGINT handler from inside the loop rather than at entry.
        if not getattr(interrupting_sleep, "_fired", False):
            interrupting_sleep._fired = True  # type: ignore[attr-defined]
            real_sleep(0)
            return
        raise KeyboardInterrupt

    monkeypatch.setattr(teardown_module.time, "sleep", interrupting_sleep)

    cli_runner = CliRunner()
    result = cli_runner.invoke(
        teardown_cli,
        [
            "request",
            "-d",
            str(strategy_folder),
            "-s",
            deployment_id,
            "--force",
            "--wait",
            "--timeout",
            "30",
        ],
    )

    assert result.exit_code == 130, result.output
    assert "Interrupted" in result.output
    assert "runner will continue" in result.output
    assert "almanak strat teardown status" in result.output
    # CodeRabbit P1: resume hint must carry both -d <folder> AND -s <id> so
    # the operator can pick up status from a different cwd after the
    # ALMANAK_STRATEGY_FOLDER process-export dies with this CLI process.
    assert f"-d {strategy_folder}" in result.output
    assert f"-s {deployment_id}" in result.output
    # No traceback bled through.
    assert "Traceback" not in result.output


# ---------------------------------------------------------------------------
# VIB-3838: execute -d routes through the canonical resolver
# ---------------------------------------------------------------------------
def test_t_3838_1_execute_rejects_non_strategy_folder(clean_env, tmp_path: Path) -> None:
    """`teardown execute -d <real-but-non-strategy-dir>` hard-fails with the
    canonical "does not look like a strategy folder" message — not a noisier
    failure later in strategy loading.
    """
    junk = tmp_path / "junk"
    junk.mkdir()
    cli_runner = CliRunner()
    result = cli_runner.invoke(teardown_cli, ["execute", "-d", str(junk), "--preview"])
    assert result.exit_code != 0, result.output
    assert "does not look like a strategy folder" in result.output
