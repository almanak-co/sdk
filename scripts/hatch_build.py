"""Wheel build hook: bundle reference strategy author files from ``strategies/``.

``strategies/`` stays the single source in Git. The wheel carries a read-only
copy under ``almanak/references/files/`` for ``almanak support --references``;
``almanak.references.is_published`` decides which references are copied and
``almanak.references.is_author_file`` which of their files.
"""

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

DESTINATION = "almanak/references/files"


def selection_rule(root: Path) -> ModuleType:
    # Loaded by path: the build environment has hatchling but not the SDK's
    # dependencies, and the module's rule needs only the standard library.
    path = root / "src" / "almanak" / "references" / "__init__.py"
    spec = importlib.util.spec_from_file_location("almanak_reference_rule", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load the reference file rule from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def reference_files(root: Path) -> dict[str, str]:
    """Map each distributed source file to its path inside the wheel."""
    rule = selection_rule(root)
    return {
        str(path): f"{DESTINATION}/{path.parent.name}/{path.name}"
        for path in sorted((root / "strategies").glob("*/*"))
        if path.is_file() and rule.is_published(path.parent.name) and rule.is_author_file(path.name)
    }


class ReferenceStrategies(BuildHookInterface):
    PLUGIN_NAME = "custom"

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if self.target_name == "wheel":
            build_data["force_include"].update(reference_files(Path(self.root)))
