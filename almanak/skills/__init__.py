"""Bundled agent skill for teaching AI coding agents how to build DeFi strategies.

The strategy-builder skill ships with `pip install almanak` and can be installed
into user projects via `almanak agent install`, producing platform-specific files
for 9 agent platforms (Claude Code, Codex, Cursor, Copilot, Windsurf, Cline,
Roo Code, Aider, Amazon Q).

Usage:
    from almanak.skills import get_skill_path, get_skill_content, Platform
    from almanak.skills import SkillRenderer, render_all_platforms

    # Get the path to the canonical SKILL.md
    path = get_skill_path()

    # Get the raw SKILL.md content
    content = get_skill_content()

    # Render for a specific platform
    renderer = SkillRenderer()
    rendered = renderer.render(Platform.CURSOR, version="2.0.0")

    # Render for all platforms
    results = render_all_platforms()
"""

from __future__ import annotations

from pathlib import Path

from almanak._version import __version__
from almanak.skills.skill_renderer import _SKILL_PATH, Platform, RenderedSkill, SkillRenderer


def get_skill_path() -> Path:
    """Return the absolute path to the bundled SKILL.md."""
    if not _SKILL_PATH.exists():
        raise FileNotFoundError(
            f"SKILL.md not found at {_SKILL_PATH}. Ensure the almanak package was installed correctly."
        )
    return _SKILL_PATH


def get_skill_content() -> str:
    """Load and return the full SKILL.md content as a string."""
    return get_skill_path().read_text(encoding="utf-8")


def render_all_platforms(version: str | None = None) -> list[RenderedSkill]:
    """Render the skill for all supported platforms.

    Returns a list of RenderedSkill objects.
    """
    ver = version or __version__
    renderer = SkillRenderer()
    return [renderer.render(p, version=ver) for p in Platform]
