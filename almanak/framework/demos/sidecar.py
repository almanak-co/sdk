"""Reader for ``.github/sidecar-demos.yml``.

Wraps the connector → demo registry as a typed ``SidecarRegistry``. The
DemoSpec loader uses this so connector regression and the demo catalog
share one source of truth.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_CONNECTOR_NAME_RE = re.compile(r"[a-z0-9_]+")


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
    """One row in ``.github/sidecar-demos.yml`` under ``connectors:``.

    ``key`` is the registry key and the matrix cell name; ``connector`` is the
    connector directory the cell covers. They coincide unless the row sets an
    explicit ``connector:``, which is how one connector gets several cells.
    """

    connector: str
    demo_dir: Path
    chain: str
    force_action: str
    max_iterations: int
    key: str = ""

    def __post_init__(self) -> None:
        if not self.key:
            object.__setattr__(self, "key", self.connector)

    @property
    def demo_name(self) -> str:
        return self.demo_dir.name


@dataclass
class SidecarRegistry:
    """In-memory view of the sidecar registry, keyed by registry key (matrix cell)."""

    connectors: dict[str, SidecarEntry]
    source_path: Path | None = None

    def entries(self) -> list[SidecarEntry]:
        return list(self.connectors.values())

    def covered_connectors(self) -> set[str]:
        return {entry.connector for entry in self.connectors.values()}

    def for_connector(self, connector: str) -> list[SidecarEntry]:
        return [entry for entry in self.connectors.values() if entry.connector == connector]

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
        for key, raw in connectors_raw.items():
            if not isinstance(raw, dict):
                raise ValueError(f"{path}: connector '{key}' must be a mapping")
            demo_dir_str = raw.get("demo_dir")
            if not isinstance(demo_dir_str, str) or not demo_dir_str:
                raise ValueError(f"{path}: connector '{key}' is missing demo_dir")
            chain = str(raw.get("chain", "")).strip()
            if not chain:
                raise ValueError(f"{path}: connector '{key}' is missing chain")
            # The shell picker reads this file with awk and only sees keys matching
            # `^  [a-z0-9_]+:`. A key outside that charset is invisible to it, so the
            # cell would vanish from the matrix CI actually runs while passing every
            # check here. Keep both parsers on the same schema. The isinstance guard
            # comes first because an unquoted key such as `123:` deserialises to int,
            # which the regex rejects with TypeError rather than the ValueError
            # callers catch.
            if not isinstance(key, str) or not _CONNECTOR_NAME_RE.fullmatch(key):
                raise ValueError(f"{path}: cell key {key!r} must match [a-z0-9_]+ so both matrix pickers can read it")
            connector = str(raw.get("connector") or key).strip()
            if not _CONNECTOR_NAME_RE.fullmatch(connector):
                raise ValueError(f"{path}: entry '{key}' has an invalid connector name: {connector!r}")
            force_action = str(raw.get("force_action", "") or "")
            max_iterations_raw = raw.get("max_iterations", 1)
            try:
                max_iterations = int(max_iterations_raw)
            except (TypeError, ValueError):
                raise ValueError(
                    f"{path}: connector '{key}' has non-integer max_iterations: {max_iterations_raw!r}"
                ) from None
            demo_dir = (repo_root / demo_dir_str).resolve()
            entries[key] = SidecarEntry(
                connector=connector,
                demo_dir=demo_dir,
                chain=chain,
                force_action=force_action,
                max_iterations=max_iterations,
                key=key,
            )

        return cls(connectors=entries, source_path=path)

    @classmethod
    def load_default(cls) -> SidecarRegistry:
        """Load from the canonical ``.github/sidecar-demos.yml`` path."""
        return cls.load(_repo_root() / ".github" / "sidecar-demos.yml")
