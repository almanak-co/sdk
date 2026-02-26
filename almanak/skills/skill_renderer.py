"""Skill renderer that transforms canonical SKILL.md into platform-specific files.

Supports 10 agent platforms. Each platform gets a standalone file written to its
native skills/rules directory - never appended to user-authored root files.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

_SKILL_PATH = Path(__file__).parent / "almanak-strategy-builder" / "SKILL.md"

VERSION_MARKER_TEMPLATE = "<!-- version: {} -->"


class Platform(StrEnum):
    """Supported agent platforms."""

    CLAUDE = "claude"
    CODEX = "codex"
    CURSOR = "cursor"
    COPILOT = "copilot"
    WINDSURF = "windsurf"
    CLINE = "cline"
    ROO = "roo"
    AIDER = "aider"
    AMAZONQ = "amazonq"
    OPENCLAW = "openclaw"


@dataclass(frozen=True)
class PlatformConfig:
    """Configuration for a specific agent platform."""

    directory: str
    filename: str
    frontmatter: str | None = None
    global_directory: str | None = None  # Path under ~/, None = no global support


PLATFORM_CONFIGS: dict[Platform, PlatformConfig] = {
    Platform.CLAUDE: PlatformConfig(
        directory=".claude/skills/almanak-strategy-builder",
        filename="SKILL.md",
        global_directory=".claude/skills/almanak-strategy-builder",
    ),
    Platform.CODEX: PlatformConfig(
        directory=".codex/skills/almanak-strategy-builder",
        filename="SKILL.md",
        global_directory=".codex/skills/almanak-strategy-builder",
    ),
    Platform.CURSOR: PlatformConfig(
        directory=".cursor/rules",
        filename="almanak-strategy-builder.mdc",
        frontmatter='---\nglobs:\n  - "**/strategy.py"\n  - "**/config.json"\n---\n',
        global_directory=".cursor/rules",
    ),
    Platform.COPILOT: PlatformConfig(
        directory=".github/instructions",
        filename="almanak-strategy-builder.instructions.md",
        frontmatter='---\napplyTo: "**/strategy.py"\n---\n',
    ),
    Platform.WINDSURF: PlatformConfig(
        directory=".windsurf/rules",
        filename="almanak-strategy-builder.md",
        global_directory=".windsurf/rules",
    ),
    Platform.CLINE: PlatformConfig(
        directory=".clinerules",
        filename="almanak-strategy-builder.md",
    ),
    Platform.ROO: PlatformConfig(
        directory=".roo/rules",
        filename="almanak-strategy-builder.md",
    ),
    Platform.AIDER: PlatformConfig(
        directory=".aider/skills/almanak-strategy-builder",
        filename="SKILL.md",
        global_directory=".aider/skills/almanak-strategy-builder",
    ),
    Platform.AMAZONQ: PlatformConfig(
        directory=".amazonq/rules",
        filename="almanak-strategy-builder.md",
    ),
    Platform.OPENCLAW: PlatformConfig(
        directory=".openclaw/skills/almanak-strategy-builder",
        filename="SKILL.md",
        global_directory=".openclaw/skills/almanak-strategy-builder",
    ),
}


@dataclass(frozen=True)
class RenderedSkill:
    """Result of rendering a skill for a specific platform."""

    platform: Platform
    filename: str
    directory: str
    content: str


class SkillRenderer:
    """Renders the canonical SKILL.md into platform-specific formats."""

    def __init__(self, skill_path: Path | None = None):
        self._skill_path = skill_path or _SKILL_PATH

    def _load_skill(self) -> str:
        """Load the canonical SKILL.md content."""
        if not self._skill_path.exists():
            raise FileNotFoundError(f"SKILL.md not found at {self._skill_path}")
        return self._skill_path.read_text(encoding="utf-8")

    def render(self, platform: Platform, version: str = "0.0.0") -> RenderedSkill:
        """Render the skill for a specific platform.

        Args:
            platform: Target agent platform.
            version: SDK version string for staleness detection.

        Returns:
            RenderedSkill with the formatted content.
        """
        config = PLATFORM_CONFIGS[platform]
        skill_content = self._load_skill()

        parts = []
        if config.frontmatter:
            parts.append(config.frontmatter)
        parts.append(VERSION_MARKER_TEMPLATE.format(version))
        parts.append(skill_content)
        content = "\n".join(parts)

        return RenderedSkill(
            platform=platform,
            filename=config.filename,
            directory=config.directory,
            content=content,
        )
