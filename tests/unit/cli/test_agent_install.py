"""Unit tests for `almanak agent install` (almanak/cli/agent.py:install).

Covers every branch of the install command:

* explicit platform(s) via -p, including "-p all"
* auto-detection when no -p is given and platform directories exist
* interactive-selector fallback when nothing is detected (selection,
  cancellation, and the non-interactive UsageError path)
* --global: default-to-all-platforms, skip of platforms without global
  support, and writes under ~/ (monkeypatched Path.home)
* --dry-run: no files written, in both local and global modes

Everything runs against tmp_path / a fake home directory; the renderer
only reads the SKILL.md bundled with the package, so no mocking of the
render step is needed.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak._version import __version__
from almanak.cli.agent import agent
from almanak.skills.skill_renderer import PLATFORM_CONFIGS, Platform

GLOBAL_PLATFORMS = [p for p in Platform if PLATFORM_CONFIGS[p].global_directory is not None]
NO_GLOBAL_PLATFORMS = [p for p in Platform if PLATFORM_CONFIGS[p].global_directory is None]


def _local_path(platform: Platform, base: Path) -> Path:
    config = PLATFORM_CONFIGS[platform]
    return base / config.directory / config.filename


def _global_path(platform: Platform, home: Path) -> Path:
    config = PLATFORM_CONFIGS[platform]
    assert config.global_directory is not None
    return home / config.global_directory / config.filename


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


class TestExplicitPlatforms:
    @pytest.mark.parametrize("platform", [Platform.CLAUDE, Platform.CURSOR])
    def test_single_platform_writes_file(self, runner: CliRunner, tmp_path: Path, platform: Platform) -> None:
        result = runner.invoke(agent, ["install", "-p", platform.value, "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        file_path = _local_path(platform, tmp_path)
        assert file_path.exists()
        content = file_path.read_text(encoding="utf-8")
        assert f"<!-- version: {__version__} -->" in content
        assert f"Installing almanak agent skill v{__version__} locally..." in result.output
        assert f"{platform.value}: {file_path}" in result.output
        assert "Done." in result.output
        assert "[DRY RUN MODE]" not in result.output

    def test_cursor_file_includes_frontmatter(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(agent, ["install", "-p", "cursor", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        content = _local_path(Platform.CURSOR, tmp_path).read_text(encoding="utf-8")
        assert content.startswith("---\nglobs:")

    def test_repeated_platform_flags(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(agent, ["install", "-p", "claude", "-p", "codex", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert _local_path(Platform.CLAUDE, tmp_path).exists()
        assert _local_path(Platform.CODEX, tmp_path).exists()
        assert not _local_path(Platform.CURSOR, tmp_path).exists()

    def test_all_installs_every_platform(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(agent, ["install", "-p", "all", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        for platform in Platform:
            assert _local_path(platform, tmp_path).exists(), platform.value


class TestDryRun:
    def test_dry_run_writes_nothing(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(agent, ["install", "-p", "claude", "-d", str(tmp_path), "--dry-run"])

        assert result.exit_code == 0, result.output
        assert "[DRY RUN MODE]" in result.output
        expected = _local_path(Platform.CLAUDE, tmp_path)
        assert f"[DRY RUN] Would write -> {expected}" in result.output
        assert not expected.exists()
        # The parent directory is not created either.
        assert not (tmp_path / ".claude").exists()
        assert "Dry run complete. No files written." in result.output
        assert "Done." not in result.output

    def test_global_dry_run_writes_nothing(self, runner: CliRunner, fake_home: Path) -> None:
        result = runner.invoke(agent, ["install", "-g", "-p", "claude", "--dry-run"])

        assert result.exit_code == 0, result.output
        expected = _global_path(Platform.CLAUDE, fake_home)
        assert f"[DRY RUN] Would write -> {expected}" in result.output
        assert not expected.exists()
        assert "Dry run complete. No files written." in result.output


class TestAutoDetection:
    def test_detected_platforms_are_installed(self, runner: CliRunner, tmp_path: Path) -> None:
        (tmp_path / ".claude").mkdir()
        (tmp_path / ".cursor").mkdir()

        result = runner.invoke(agent, ["install", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert "Auto-detected platforms: claude, cursor" in result.output
        assert _local_path(Platform.CLAUDE, tmp_path).exists()
        assert _local_path(Platform.CURSOR, tmp_path).exists()
        assert not _local_path(Platform.CODEX, tmp_path).exists()

    def test_github_directory_detects_copilot(self, runner: CliRunner, tmp_path: Path) -> None:
        (tmp_path / ".github").mkdir()

        result = runner.invoke(agent, ["install", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert "Auto-detected platforms: copilot" in result.output
        assert _local_path(Platform.COPILOT, tmp_path).exists()


class TestInteractiveFallback:
    def test_nothing_detected_non_interactive_errors(self, runner: CliRunner, tmp_path: Path) -> None:
        # CliRunner streams are not TTYs, so the interactive selector
        # raises click.UsageError (exit code 2) instead of showing a menu.
        result = runner.invoke(agent, ["install", "-d", str(tmp_path)])

        assert result.exit_code == 2
        assert "No agent platforms detected and terminal is non-interactive." in result.output
        assert "almanak agent install -p <platform>" in result.output

    def test_selector_cancel_returns_without_installing(
        self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("almanak.cli.agent._interactive_platform_select", lambda: None)

        result = runner.invoke(agent, ["install", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert "Installing" not in result.output
        assert list(tmp_path.iterdir()) == []

    def test_selector_choice_is_installed(
        self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("almanak.cli.agent._interactive_platform_select", lambda: [Platform.CODEX])

        result = runner.invoke(agent, ["install", "-d", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert _local_path(Platform.CODEX, tmp_path).exists()
        assert "Done." in result.output


class TestGlobalInstall:
    def test_no_platforms_defaults_to_all_with_skips(self, runner: CliRunner, fake_home: Path) -> None:
        result = runner.invoke(agent, ["install", "-g"])

        assert result.exit_code == 0, result.output
        assert f"Installing almanak agent skill v{__version__} globally..." in result.output
        for platform in GLOBAL_PLATFORMS:
            file_path = _global_path(platform, fake_home)
            assert file_path.exists(), platform.value
            assert f"<!-- version: {__version__} -->" in file_path.read_text(encoding="utf-8")
        for platform in NO_GLOBAL_PLATFORMS:
            assert f"{platform.value}: skipped (no global support)" in result.output

    def test_explicit_supported_platform_writes_under_home(self, runner: CliRunner, fake_home: Path) -> None:
        result = runner.invoke(agent, ["install", "-g", "-p", "claude"])

        assert result.exit_code == 0, result.output
        file_path = _global_path(Platform.CLAUDE, fake_home)
        assert file_path.exists()
        assert f"claude: {file_path}" in result.output
        assert "Done." in result.output

    def test_explicit_unsupported_platform_is_skipped(self, runner: CliRunner, fake_home: Path) -> None:
        result = runner.invoke(agent, ["install", "-g", "-p", "copilot"])

        assert result.exit_code == 0, result.output
        assert "copilot: skipped (no global support)" in result.output
        # Nothing written anywhere under the fake home.
        assert list(fake_home.rglob("*")) == []
        assert "Done." in result.output

    def test_global_ignores_directory_option(self, runner: CliRunner, fake_home: Path, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()

        result = runner.invoke(agent, ["install", "-g", "-p", "claude", "-d", str(project)])

        assert result.exit_code == 0, result.output
        assert _global_path(Platform.CLAUDE, fake_home).exists()
        assert list(project.iterdir()) == []
