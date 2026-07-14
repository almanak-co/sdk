"""VIB-5778: CLI render honesty for FAILED teardowns.

Every CLI render site of a FAILED teardown must (a) surface the persisted
``error_message`` and (b) render unmeasured counts as ``unknown`` — never a
success-shaped ``0`` — when the row failed before enumeration ran
(``started_at IS NULL``). A FAILED row that DID start renders its real measured
counts, including a genuine measured ``0`` (Empty != Zero, both directions).

Render sites audited (all in ``almanak/framework/cli/teardown.py``):
* ``format_progress`` / ``format_count`` — the shared count formatters.
* ``status`` — single-request detail view.
* ``list`` — the multi-request table.
* ``_poll_for_terminal_state`` — the ``request --wait`` terminal print.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak.framework.cli.teardown import (
    format_count,
    format_progress,
)
from almanak.framework.cli.teardown import (
    teardown as teardown_cli,
)
from almanak.framework.teardown import (
    TeardownMode,
    TeardownRequest,
    TeardownStateManager,
    TeardownStatus,
)


# ---------------------------------------------------------------------------
# Fixtures (mirrors test_teardown_cli_folder_scoped.py)
# ---------------------------------------------------------------------------
@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for var in (
        "ALMANAK_IS_HOSTED",
        "ALMANAK_DEPLOYMENT_ID",
        "ALMANAK_STATE_DB",
        "ALMANAK_STRATEGY_FOLDER",
        "ALMANAK_GATEWAY_DB_PATH",
        "XDG_DATA_HOME",
    ):
        monkeypatch.delenv(var, raising=False)
    from almanak.framework.teardown import state_manager as sm

    sm._default_manager = None
    return monkeypatch


@pytest.fixture
def strategy_folder(tmp_path: Path) -> Path:
    folder = tmp_path / "my_strat"
    folder.mkdir()
    (folder / "config.json").write_text("{}", encoding="utf-8")
    (folder / "strategy.py").write_text("# stub\n", encoding="utf-8")
    return folder


def _mgr(folder: Path) -> TeardownStateManager:
    return TeardownStateManager(db_path=str(folder / "almanak_state.db"))


def _req(deployment_id: str) -> TeardownRequest:
    return TeardownRequest(deployment_id=deployment_id, mode=TeardownMode.SOFT)


# ---------------------------------------------------------------------------
# Pure formatters
# ---------------------------------------------------------------------------
class TestFormatters:
    def test_format_count_unknown_for_failed_unstarted(self) -> None:
        req = _req("S")
        req.status = TeardownStatus.FAILED
        req.started_at = None
        # Even a raw 0 renders as unknown — it is unmeasured, not zero.
        assert format_count(req, 0) == "unknown"
        assert format_count(req, req.positions_failed) == "unknown"

    def test_format_count_real_for_started_failure(self) -> None:
        req = _req("S")
        req.status = TeardownStatus.FAILED
        req.started_at = datetime.now(UTC)
        req.positions_closed = 0  # genuine measured zero
        assert format_count(req, req.positions_closed) == "0"
        assert format_count(req, 5) == "5"

    def test_format_progress_unknown_for_failed_unstarted(self) -> None:
        req = _req("S")
        req.status = TeardownStatus.FAILED
        req.started_at = None
        assert format_progress(req) == "unknown"

    def test_format_progress_real_for_started_failure(self) -> None:
        req = _req("S")
        req.status = TeardownStatus.FAILED
        req.started_at = datetime.now(UTC)
        req.positions_total = 3
        req.positions_closed = 1
        req.positions_failed = 2
        out = format_progress(req)
        assert "1/3" in out
        assert "2 failed" in out


# ---------------------------------------------------------------------------
# `status` command
# ---------------------------------------------------------------------------
class TestStatusRender:
    def test_generation_failure_shows_unknown_and_error(self, clean_env, strategy_folder: Path) -> None:
        mgr = _mgr(strategy_folder)
        mgr.create_request(_req("GenFail:1"))
        # Field-incident shape: mark_failed with NO mark_started, NO counts.
        mgr.mark_failed("GenFail:1", error="generate_teardown_intents raised HealthUnavailableError")

        result = CliRunner().invoke(
            teardown_cli, ["status", "-d", str(strategy_folder), "-s", "GenFail:1"]
        )
        assert result.exit_code == 0, result.output
        assert "unknown" in result.output
        # The success-shaped "0/0" or "0" counts must NOT appear as progress.
        assert "HealthUnavailableError" in result.output
        assert "Error:" in result.output

    def test_started_failure_shows_real_counts(self, clean_env, strategy_folder: Path) -> None:
        mgr = _mgr(strategy_folder)
        mgr.create_request(_req("Started:1"))
        mgr.mark_started("Started:1", total_positions=3)
        mgr.mark_failed("Started:1", error="1 reverted", positions_closed=2, positions_failed=1)

        result = CliRunner().invoke(
            teardown_cli, ["status", "-d", str(strategy_folder), "-s", "Started:1"]
        )
        assert result.exit_code == 0, result.output
        assert "2/3" in result.output
        assert "unknown" not in result.output
        assert "1 reverted" in result.output

    def test_json_output_carries_error_message(self, clean_env, strategy_folder: Path) -> None:
        mgr = _mgr(strategy_folder)
        mgr.create_request(_req("Json:1"))
        mgr.mark_failed("Json:1", error="boom")

        result = CliRunner().invoke(
            teardown_cli, ["status", "-d", str(strategy_folder), "-s", "Json:1", "--json"]
        )
        assert result.exit_code == 0, result.output
        assert '"error_message": "boom"' in result.output


# ---------------------------------------------------------------------------
# `list` command
# ---------------------------------------------------------------------------
class TestListRender:
    def test_failed_row_shows_unknown_progress_and_error(self, clean_env, strategy_folder: Path) -> None:
        mgr = _mgr(strategy_folder)
        mgr.create_request(_req("ListFail:1"))
        mgr.mark_failed("ListFail:1", error="strategy enumeration blew up")

        result = CliRunner().invoke(
            teardown_cli, ["list", "-d", str(strategy_folder), "--all"]
        )
        assert result.exit_code == 0, result.output
        assert "unknown" in result.output
        assert "strategy enumeration blew up" in result.output

    def test_started_failed_row_shows_real_counts_and_error(self, clean_env, strategy_folder: Path) -> None:
        """A FAILED row that DID start renders real measured progress (not
        ``unknown``) AND still surfaces the error — the error branch is
        status-gated, not started-gated."""
        mgr = _mgr(strategy_folder)
        mgr.create_request(_req("ListStarted:1"))
        mgr.mark_started("ListStarted:1", total_positions=3)
        mgr.mark_failed("ListStarted:1", error="2 intents reverted", positions_closed=1, positions_failed=2)

        result = CliRunner().invoke(
            teardown_cli, ["list", "-d", str(strategy_folder), "--all"]
        )
        assert result.exit_code == 0, result.output
        assert "1/3" in result.output
        assert "unknown" not in result.output
        assert "2 intents reverted" in result.output

    def test_list_json_carries_error_message(self, clean_env, strategy_folder: Path) -> None:
        """``list --json`` carries the persisted error_message (machine readers
        must see the failure reason too, not only the human table)."""
        mgr = _mgr(strategy_folder)
        mgr.create_request(_req("ListJson:1"))
        mgr.mark_failed("ListJson:1", error="list json boom")

        result = CliRunner().invoke(
            teardown_cli, ["list", "-d", str(strategy_folder), "--all", "--json"]
        )
        assert result.exit_code == 0, result.output
        assert '"error_message": "list json boom"' in result.output


# ---------------------------------------------------------------------------
# `request --wait` terminal print (_poll_for_terminal_state)
# ---------------------------------------------------------------------------
def _drive_to_failed_before_start(db_file: Path, deployment_id: str) -> None:
    """Simulate the generation-exception path: acknowledge, then mark_failed
    WITHOUT mark_started (started_at stays NULL, counts stay at defaults)."""
    time.sleep(0.3)
    mgr = TeardownStateManager(db_path=str(db_file))
    req = None
    for _ in range(20):
        req = mgr.get_active_request(deployment_id)
        if req is not None:
            break
        time.sleep(0.1)
    if req is None:
        return
    req.acknowledged_at = datetime.now(UTC)
    req.status = TeardownStatus.CANCEL_WINDOW
    mgr.update_request(req)
    time.sleep(0.3)
    mgr.mark_failed(deployment_id, error="generate_teardown_intents raised")


def _drive_to_failed_after_start(db_file: Path, deployment_id: str) -> None:
    """Simulate an execution failure AFTER enumeration: acknowledge, mark_started
    (started_at set, positions_total measured), then mark_failed WITH real counts.
    The --wait render must then show real counts, never ``unknown``."""
    time.sleep(0.3)
    mgr = TeardownStateManager(db_path=str(db_file))
    req = None
    for _ in range(20):
        req = mgr.get_active_request(deployment_id)
        if req is not None:
            break
        time.sleep(0.1)
    if req is None:
        return
    req.acknowledged_at = datetime.now(UTC)
    req.status = TeardownStatus.CANCEL_WINDOW
    mgr.update_request(req)
    mgr.mark_started(deployment_id, total_positions=3)
    time.sleep(0.3)
    mgr.mark_failed(deployment_id, error="1 intent reverted", positions_closed=2, positions_failed=1)


class TestWaitRender:
    @pytest.fixture(autouse=True)
    def _stub_target_validation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """VIB-5777 added fail-fast gateway target-validation to `request` that
        makes `--force` REFUSE an unknown deployment identity (exiting before the
        --wait render code). This test exercises the --wait FAILED render path
        (VIB-5778), not target validation, and runs with no gateway — so stub the
        validation seam so the request proceeds to the render path exactly as a
        validated RUNNING target would in production. Validation itself is covered
        by VIB-5777's own suite.

        ``raising=False``: on this standalone branch VIB-5777 is not yet merged,
        so ``_validate_teardown_target`` does not exist and `request` never
        validates — the setattr is then a harmless no-op the product code never
        calls. Once VIB-5777 lands, the same seam is stubbed for real. Either way
        the render assertions below are untouched.
        """
        import almanak.framework.cli.teardown as teardown_module

        monkeypatch.setattr(
            teardown_module,
            "_validate_teardown_target",
            lambda *args, **kwargs: None,
            raising=False,
        )

    def test_wait_failed_before_start_prints_unknown_and_error(
        self, clean_env, strategy_folder: Path
    ) -> None:
        db_file = strategy_folder / "almanak_state.db"
        deployment_id = "WaitGenFail:1"
        t = threading.Thread(
            target=_drive_to_failed_before_start,
            args=(db_file, deployment_id),
            daemon=True,
        )
        t.start()
        result = CliRunner().invoke(
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
        t.join(timeout=10)
        assert result.exit_code != 0, result.output
        assert "FAILED" in result.output
        # Honest counts, not a success-shaped 0/0.
        assert "positions_closed=unknown" in result.output
        assert "total=unknown" in result.output
        assert "generate_teardown_intents raised" in result.output

    def test_wait_failed_after_start_prints_real_counts_and_error(
        self, clean_env, strategy_folder: Path
    ) -> None:
        """A failure AFTER mark_started renders real measured counts on --wait
        (never ``unknown``) and still surfaces the error — Empty != Zero, the
        measured direction, on the terminal poll print."""
        db_file = strategy_folder / "almanak_state.db"
        deployment_id = "WaitStarted:1"
        t = threading.Thread(
            target=_drive_to_failed_after_start,
            args=(db_file, deployment_id),
            daemon=True,
        )
        t.start()
        result = CliRunner().invoke(
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
        t.join(timeout=10)
        assert result.exit_code != 0, result.output
        assert "FAILED" in result.output
        assert "positions_closed=2" in result.output
        assert "total=3" in result.output
        assert "unknown" not in result.output
        assert "1 intent reverted" in result.output
