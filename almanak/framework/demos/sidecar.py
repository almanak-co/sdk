"""Reader for ``.github/sidecar-demos.yml``.

Wraps the connector → demo registry as a typed ``SidecarRegistry``. The
DemoSpec loader uses this so connector regression and the demo catalog
share one source of truth.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


_DEFAULT_PATH_MARKERS = (".github/sidecar-demos.yml",)


def _repo_root() -> Path:
    """Walk upward from this file to locate the repo root.

    Looks for ``.github/sidecar-demos.yml`` (the file we want to read) or a
    ``pyproject.toml`` sibling. Falls back to four-levels-up for installed
    layouts where ``.github`` does not exist.
    """
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        if (parent / ".github" / "sidecar-demos.yml").is_file():
            return parent
        if (parent / "pyproject.toml").is_file() and (parent / ".github").is_dir():
            return parent
    return here.parents[3]


@dataclass(frozen=True)
class SidecarEntry:
    """One row in ``.github/sidecar-demos.yml`` under ``connectors:``."""

    connector: str
    demo_dir: Path
    chain: str
    force_action: str
    max_iterations: int

    @property
    def demo_name(self) -> str:
        return self.demo_dir.name


@dataclass
class SidecarRegistry:
    """In-memory view of the sidecar registry, keyed by connector name."""

    connectors: dict[str, SidecarEntry]
    source_path: Path | None = None

    def entries(self) -> list[SidecarEntry]:
        return list(self.connectors.values())

    def for_demo(self, demo_dir: Path) -> SidecarEntry | None:
        target = demo_dir.resolve()
        for entry in self.connectors.values():
            if entry.demo_dir.resolve() == target:
                return entry
        return None

    @classmethod
    def load(cls, path: Path) -> SidecarRegistry:
        """Parse ``path`` (a YAML file). Raises ``FileNotFoundError`` if missing."""
        if not path.is_file():
            raise FileNotFoundError(f"Sidecar registry not found: {path}")

        try:
            import yaml
        except ImportError as exc:  # pragma: no cover - PyYAML is a hard dep
            raise RuntimeError("PyYAML is required to parse sidecar-demos.yml") from exc

        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}

        connectors_raw = data.get("connectors") or {}
        if not isinstance(connectors_raw, dict):
            raise ValueError(f"{path}: top-level 'connectors:' must be a mapping, got {type(connectors_raw).__name__}")

        repo_root = path.resolve().parents[1]  # .github/<file>.yml -> repo root
        entries: dict[str, SidecarEntry] = {}
        for connector, raw in connectors_raw.items():
            if not isinstance(raw, dict):
                raise ValueError(f"{path}: connector '{connector}' must be a mapping")
            demo_dir_str = raw.get("demo_dir")
            if not isinstance(demo_dir_str, str) or not demo_dir_str:
                raise ValueError(f"{path}: connector '{connector}' is missing demo_dir")
            chain = str(raw.get("chain", "")).strip()
            if not chain:
                raise ValueError(f"{path}: connector '{connector}' is missing chain")
            force_action = str(raw.get("force_action", "") or "")
            max_iterations_raw = raw.get("max_iterations", 1)
            try:
                max_iterations = int(max_iterations_raw)
            except (TypeError, ValueError):
                raise ValueError(
                    f"{path}: connector '{connector}' has non-integer max_iterations: {max_iterations_raw!r}"
                ) from None
            demo_dir = (repo_root / demo_dir_str).resolve()
            entries[connector] = SidecarEntry(
                connector=connector,
                demo_dir=demo_dir,
                chain=chain,
                force_action=force_action,
                max_iterations=max_iterations,
            )

        return cls(connectors=entries, source_path=path)

    @classmethod
    def load_default(cls) -> SidecarRegistry:
        """Load from the canonical ``.github/sidecar-demos.yml`` path."""
        return cls.load(_repo_root() / ".github" / "sidecar-demos.yml")
