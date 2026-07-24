"""Unit tests for `almanak agent status` and `almanak agent update` (almanak/cli/agent.py).

Covers every branch of the two commands:

status:
* local vs global scope header
* per-platform states: n/a (no global support), not installed,
  installed (version unknown), up to date, outdated
* summary line with and without the N/A count
* the "Run 'almanak agent update[ -g]'" hint only when outdated > 0

update:
* local vs global scope header
* skip of platforms with no file installed, and of platforms without
  global support in --global mode
* version transition line ("old -> new") vs plain "updated" (same
  version or missing version marker)
* file content actually rewritten to the current SDK version
* "No installed skill files found" hint with and without -g

Everything runs against tmp_path / a fake home directory (monkeypatched
Path.home); no network, no mocking of the render step.

Path helpers are shared with tests/unit/cli/test_agent_install.py, which
owns the `install` command coverage.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak._version import __version__
from almanak.cli.agent import agent
from almanak.skills.skill_renderer import PLATFORM_CONFIGS, Platform
from tests.unit.cli.test_agent_install import _global_path, _local_path

GLOBAL_PLATFORMS = [p for p in Platform if PLATFORM_CONFIGS[p].global_directory is not None]
NO_GLOBAL_PLATFORMS = [p for p in Platform if PLATFORM_CONFIGS[p].global_directory is None]

OLD_VERSION = "0.0.1"


def _write_skill_file(path: Path, version: str | None) -> None:
    """Write a minimal skill file, optionally with a version marker."""
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "# Almanak strategy builder\n"
    if version is not None:
        body += f"\n<!-- version: {version} -->\n"
    path.write_text(body, encoding="utf-8")


def _status_line(platform: Platform, state: str) -> str:
    """Reproduce the status command's per-platform output line."""
    return f"  {platform.value:<10}  {state}"


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def fake_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect Path.home() so --global never touches the real ~/."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: home)
    return home


class TestStatusLocal:
    def test_empty_directory_reports_all_missing(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(agent, ["status", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert f"Agent skill status - local (SDK v{__version__}):" in result.output
        for platform in Platform:
            assert _status_line(platform, "not installed") in result.output
        assert f"Installed: 0  Outdated: 0  Missing: {len(list(Platform))}" in result.output
        # No platform is n/a locally, and nothing is outdated.
        assert "N/A:" not in result.output
        assert "almanak agent update" not in result.output

    def test_mixed_states_and_update_hint(self, runner: CliRunner, tmp_path: Path) -> None:
        _write_skill_file(_local_path(Platform.CLAUDE, tmp_path), __version__)
        _write_skill_file(_local_path(Platform.CURSOR, tmp_path), OLD_VERSION)
        _write_skill_file(_local_path(Platform.CODEX, tmp_path), None)

        result = runner.invoke(agent, ["status", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert _status_line(Platform.CLAUDE, f"up to date (v{__version__})") in result.output
        assert _status_line(Platform.CURSOR, f"outdated (v{OLD_VERSION} -> v{__version__})") in result.output
        assert _status_line(Platform.CODEX, "installed (version unknown)") in result.output
        missing = len(list(Platform)) - 3
        assert f"Installed: 2  Outdated: 1  Missing: {missing}" in result.output
        # Local hint carries no -g flag.
        assert "Run 'almanak agent update' to update outdated files." in result.output

    def test_up_to_date_only_no_hint(self, runner: CliRunner, tmp_path: Path) -> None:
        _write_skill_file(_local_path(Platform.CLAUDE, tmp_path), __version__)

        result = runner.invoke(agent, ["status", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert "Installed: 1  Outdated: 0" in result.output
        assert "almanak agent update" not in result.output


class TestStatusGlobal:
    def test_no_global_platforms_reported_na(self, runner: CliRunner, fake_home: Path) -> None:
        result = runner.invoke(agent, ["status", "-g"])

        assert result.exit_code == 0, result.output
        assert f"Agent skill status - global (SDK v{__version__}):" in result.output
        for platform in NO_GLOBAL_PLATFORMS:
            assert _status_line(platform, "n/a (no global support)") in result.output
        for platform in GLOBAL_PLATFORMS:
            assert _status_line(platform, "not installed") in result.output
        summary = f"Installed: 0  Outdated: 0  Missing: {len(GLOBAL_PLATFORMS)}  N/A: {len(NO_GLOBAL_PLATFORMS)}"
        assert summary in result.output
        assert "almanak agent update" not in result.output

    def test_outdated_global_hint_carries_g_flag(self, runner: CliRunner, fake_home: Path) -> None:
        _write_skill_file(_global_path(Platform.CLAUDE, fake_home), OLD_VERSION)

        result = runner.invoke(agent, ["status", "-g"])

        assert result.exit_code == 0, result.output
        assert _status_line(Platform.CLAUDE, f"outdated (v{OLD_VERSION} -> v{__version__})") in result.output
        missing = len(GLOBAL_PLATFORMS) - 1
        assert f"Installed: 0  Outdated: 1  Missing: {missing}  N/A: {len(NO_GLOBAL_PLATFORMS)}" in result.output
        assert "Run 'almanak agent update -g' to update outdated files." in result.output

    def test_global_ignores_directory_option(self, runner: CliRunner, fake_home: Path, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        _write_skill_file(_local_path(Platform.CLAUDE, project), __version__)

        result = runner.invoke(agent, ["status", "-g", "-d", str(project)])

        assert result.exit_code == 0, result.output
        # The local install under -d is invisible to --global.
        assert _status_line(Platform.CLAUDE, "not installed") in result.output


class TestUpdateLocal:
    @pytest.mark.parametrize(
        ("args", "expected_hint"),
        [
            ([], "Run 'almanak agent install' first."),
            (["-g"], "Run 'almanak agent install -g' first."),
        ],
        ids=["local", "global"],
    )
    def test_nothing_installed_prints_install_hint(
        self,
        runner: CliRunner,
        tmp_path: Path,
        fake_home: Path,
        args: list[str],
        expected_hint: str,
    ) -> None:
        result = runner.invoke(agent, ["update", "-d", str(tmp_path), *args])

        assert result.exit_code == 0, result.output
        assert "No installed skill files found." in result.output
        assert expected_hint in result.output
        assert "Updated" not in result.output

    def test_updates_only_installed_files(self, runner: CliRunner, tmp_path: Path) -> None:
        _write_skill_file(_local_path(Platform.CLAUDE, tmp_path), OLD_VERSION)
        _write_skill_file(_local_path(Platform.CODEX, tmp_path), None)
        _write_skill_file(_local_path(Platform.CURSOR, tmp_path), __version__)

        result = runner.invoke(agent, ["update", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert f"Updating local agent skill files to v{__version__}..." in result.output
        # Old version present and different -> transition line.
        assert f"  claude: {OLD_VERSION} -> {__version__}" in result.output
        # Missing marker or already-current version -> plain "updated".
        assert "  codex: updated" in result.output
        assert "  cursor: updated" in result.output
        assert "Updated 3 file(s)." in result.output
        # Files are rewritten with the current version marker.
        for platform in (Platform.CLAUDE, Platform.CODEX, Platform.CURSOR):
            content = _local_path(platform, tmp_path).read_text(encoding="utf-8")
            assert f"<!-- version: {__version__} -->" in content
        # Platforms that were never installed stay absent.
        assert not _local_path(Platform.COPILOT, tmp_path).exists()
        assert "copilot" not in result.output


class TestUpdateGlobal:
    def test_updates_home_file_and_ignores_local_installs(
        self, runner: CliRunner, fake_home: Path, tmp_path: Path
    ) -> None:
        project = tmp_path / "project"
        project.mkdir()
        stale_local = _local_path(Platform.CLAUDE, project)
        _write_skill_file(stale_local, OLD_VERSION)
        _write_skill_file(_global_path(Platform.CLAUDE, fake_home), OLD_VERSION)

        result = runner.invoke(agent, ["update", "-g", "-d", str(project)])

        assert result.exit_code == 0, result.output
        assert f"Updating global agent skill files to v{__version__}..." in result.output
        assert f"  claude: {OLD_VERSION} -> {__version__}" in result.output
        assert "Updated 1 file(s)." in result.output
        content = _global_path(Platform.CLAUDE, fake_home).read_text(encoding="utf-8")
        assert f"<!-- version: {__version__} -->" in content
        # The stale local file under -d is untouched in --global mode.
        assert f"<!-- version: {OLD_VERSION} -->" in stale_local.read_text(encoding="utf-8")

    def test_platforms_without_global_support_are_skipped(self, runner: CliRunner, fake_home: Path) -> None:
        _write_skill_file(_global_path(Platform.CODEX, fake_home), __version__)

        result = runner.invoke(agent, ["update", "-g"])

        assert result.exit_code == 0, result.output
        assert "  codex: updated" in result.output
        assert "Updated 1 file(s)." in result.output
        for platform in NO_GLOBAL_PLATFORMS:
            assert platform.value not in result.output
