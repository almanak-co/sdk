"""CLI commands for managing agent skill installation.

Usage:
    almanak agent install [-p platform] [-d directory] [-g] [--dry-run]
    almanak agent update [-d directory] [-g]
    almanak agent status [-d directory] [-g]
"""

from __future__ import annotations

import re
from pathlib import Path

import click

from almanak._version import __version__
from almanak.skills.skill_renderer import (
    PLATFORM_CONFIGS,
    Platform,
    PlatformConfig,
    RenderedSkill,
    SkillRenderer,
)

# Detection patterns: platform directory -> platform
_DETECTION_MAP: list[tuple[str, Platform]] = [
    (".claude", Platform.CLAUDE),
    (".codex", Platform.CODEX),
    (".cursor", Platform.CURSOR),
    (".github", Platform.COPILOT),
    (".windsurf", Platform.WINDSURF),
    (".clinerules", Platform.CLINE),
    (".roo", Platform.ROO),
    (".aider", Platform.AIDER),
    (".amazonq", Platform.AMAZONQ),
    (".openclaw", Platform.OPENCLAW),
]


def _detect_platforms(directory: Path) -> list[Platform]:
    """Auto-detect agent platforms by checking for known directories."""
    found: list[Platform] = []
    seen: set[Platform] = set()
    for indicator, platform in _DETECTION_MAP:
        if platform in seen:
            continue
        if (directory / indicator).exists():
            found.append(platform)
            seen.add(platform)
    return found


_VERSION_RE = re.compile(r"<!--\s*version:\s*([\d.]+\S*)\s*-->")


def _extract_installed_version(file_path: Path) -> str | None:
    """Extract the almanak SDK version from a skill file's version marker."""
    if not file_path.exists():
        return None
    text = file_path.read_text(encoding="utf-8")
    match = _VERSION_RE.search(text)
    return match.group(1) if match else None


def _resolve_target(directory: str, global_install: bool) -> Path:
    """Resolve the base directory for install/update/status."""
    if global_install:
        return Path.home()
    return Path(directory).resolve()


def _get_skill_path(config: PlatformConfig, target_dir: Path, global_install: bool) -> Path | None:
    """Get the file path for a platform, respecting global vs local.

    Returns None if global is requested but the platform doesn't support it.
    """
    if global_install:
        if config.global_directory is None:
            return None
        return target_dir / config.global_directory / config.filename
    return target_dir / config.directory / config.filename


def _install_file(rendered: RenderedSkill, target_dir: Path, dry_run: bool) -> Path:
    """Write a rendered skill to disk.

    Returns the path of the written file.
    """
    file_dir = target_dir / rendered.directory
    file_path = file_dir / rendered.filename

    if dry_run:
        click.echo(f"  [DRY RUN] Would write -> {file_path}")
        return file_path

    file_dir.mkdir(parents=True, exist_ok=True)
    file_path.write_text(rendered.content, encoding="utf-8")

    return file_path


def _install_file_global(
    rendered: RenderedSkill,
    config: PlatformConfig,
    target_dir: Path,
    dry_run: bool,
) -> Path:
    """Write a rendered skill to disk using the global directory path.

    Returns the path of the written file.
    """
    assert config.global_directory is not None  # caller must check before calling
    file_dir = target_dir / config.global_directory
    file_path = file_dir / rendered.filename

    if dry_run:
        click.echo(f"  [DRY RUN] Would write -> {file_path}")
        return file_path

    file_dir.mkdir(parents=True, exist_ok=True)
    file_path.write_text(rendered.content, encoding="utf-8")

    return file_path


@click.group()
def agent():
    """Manage AI agent skill files for strategy development."""
    pass


@agent.command()
@click.option(
    "--platform",
    "-p",
    "platforms",
    multiple=True,
    type=click.Choice([p.value for p in Platform] + ["all"], case_sensitive=False),
    help="Target platform(s). Use 'all' for every platform. Can be repeated.",
)
@click.option(
    "--directory",
    "-d",
    type=click.Path(exists=True, file_okay=False),
    default=".",
    help="Project directory to install into (default: current directory).",
)
@click.option(
    "--global",
    "-g",
    "global_install",
    is_flag=True,
    default=False,
    help="Install globally (~/) so the skill is available in all projects.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show what would be done without writing files.",
)
def install(platforms: tuple[str, ...], directory: str, global_install: bool, dry_run: bool) -> None:
    """Install agent skill files into the current project.

    Auto-detects platforms if none specified. Falls back to Claude Code
    if no platforms are detected. Use --global to install into ~/ for
    all projects.

    Examples:

    \b
        almanak agent install                    # auto-detect
        almanak agent install -p claude -p cursor # specific platforms
        almanak agent install -p all             # all platforms
        almanak agent install -g                 # global install (~/)
        almanak agent install --dry-run          # preview only
    """
    target_dir = _resolve_target(directory, global_install)
    renderer = SkillRenderer()

    # Resolve platform list
    if not platforms:
        if global_install:
            # Global install: default to all platforms (skip unsupported ones below)
            resolved = list(Platform)
        else:
            detected = _detect_platforms(target_dir)
            if detected:
                click.echo(f"Auto-detected platforms: {', '.join(p.value for p in detected)}")
                resolved = detected
            else:
                click.echo("No platforms detected. Installing as Claude Code skill.")
                click.echo("Tip: 'npx skills add almanak-co/almanak-sdk' works across all platforms.")
                resolved = [Platform.CLAUDE]
    elif "all" in platforms:
        resolved = list(Platform)
    else:
        resolved = [Platform(p) for p in platforms]

    scope = "globally" if global_install else "locally"
    click.echo(f"Installing almanak agent skill v{__version__} {scope}...")
    if dry_run:
        click.echo("[DRY RUN MODE]")
    click.echo()

    for platform in resolved:
        config = PLATFORM_CONFIGS[platform]

        if global_install:
            if config.global_directory is None:
                click.echo(f"  {platform.value}: skipped (no global support)")
                continue
            rendered = renderer.render(platform, version=__version__)
            file_path = _install_file_global(rendered, config, target_dir, dry_run)
        else:
            rendered = renderer.render(platform, version=__version__)
            file_path = _install_file(rendered, target_dir, dry_run)

        if not dry_run:
            click.echo(f"  {platform.value}: {file_path}")

    click.echo()
    click.echo("Done." if not dry_run else "Dry run complete. No files written.")


@agent.command()
@click.option(
    "--directory",
    "-d",
    type=click.Path(exists=True, file_okay=False),
    default=".",
    help="Project directory to update (default: current directory).",
)
@click.option(
    "--global",
    "-g",
    "global_install",
    is_flag=True,
    default=False,
    help="Update globally installed skill files (~/).",
)
def update(directory: str, global_install: bool) -> None:
    """Update all installed agent skill files to the current SDK version.

    Scans for existing platform files and updates any that are found.
    Use --global to update files installed in ~/.
    """
    target_dir = _resolve_target(directory, global_install)
    renderer = SkillRenderer()
    updated_count = 0

    scope = "global" if global_install else "local"
    click.echo(f"Updating {scope} agent skill files to v{__version__}...")
    click.echo()

    for platform in Platform:
        config = PLATFORM_CONFIGS[platform]
        file_path = _get_skill_path(config, target_dir, global_install)
        if file_path is None or not file_path.exists():
            continue

        installed_ver = _extract_installed_version(file_path)
        rendered = renderer.render(platform, version=__version__)

        if global_install:
            _install_file_global(rendered, config, target_dir, dry_run=False)
        else:
            _install_file(rendered, target_dir, dry_run=False)
        updated_count += 1

        if installed_ver and installed_ver != __version__:
            click.echo(f"  {platform.value}: {installed_ver} -> {__version__}")
        else:
            click.echo(f"  {platform.value}: updated")

    if updated_count == 0:
        flag = " -g" if global_install else ""
        click.echo(f"No installed skill files found. Run 'almanak agent install{flag}' first.")
    else:
        click.echo(f"\nUpdated {updated_count} file(s).")


@agent.command()
@click.option(
    "--directory",
    "-d",
    type=click.Path(exists=True, file_okay=False),
    default=".",
    help="Project directory to check (default: current directory).",
)
@click.option(
    "--global",
    "-g",
    "global_install",
    is_flag=True,
    default=False,
    help="Check globally installed skill files (~/).",
)
def status(directory: str, global_install: bool) -> None:
    """Show installation status of agent skill files.

    Reports installed, missing, and outdated platforms.
    Use --global to check files installed in ~/.
    """
    target_dir = _resolve_target(directory, global_install)

    scope = "global" if global_install else "local"
    click.echo(f"Agent skill status - {scope} (SDK v{__version__}):")
    click.echo()

    installed = 0
    outdated = 0
    missing = 0
    skipped = 0

    for platform in Platform:
        config = PLATFORM_CONFIGS[platform]
        file_path = _get_skill_path(config, target_dir, global_install)

        if file_path is None:
            click.echo(f"  {platform.value:<10}  n/a (no global support)")
            skipped += 1
            continue

        if not file_path.exists():
            click.echo(f"  {platform.value:<10}  not installed")
            missing += 1
            continue

        installed_ver = _extract_installed_version(file_path)
        if installed_ver is None:
            click.echo(f"  {platform.value:<10}  installed (version unknown)")
            installed += 1
        elif installed_ver == __version__:
            click.echo(f"  {platform.value:<10}  up to date (v{installed_ver})")
            installed += 1
        else:
            click.echo(f"  {platform.value:<10}  outdated (v{installed_ver} -> v{__version__})")
            outdated += 1

    click.echo()
    summary = f"Installed: {installed}  Outdated: {outdated}  Missing: {missing}"
    if skipped > 0:
        summary += f"  N/A: {skipped}"
    click.echo(summary)
    if outdated > 0:
        flag = " -g" if global_install else ""
        click.echo(f"Run 'almanak agent update{flag}' to update outdated files.")
