"""Single source of truth for demo strategy metadata.

This package consolidates discovery and metadata access for the demo
strategies that ship under ``almanak/demo_strategies/``. It does **not**
introduce a new metadata file — every field is read from one of three
existing sources:

* ``@almanak_strategy`` decorator on the strategy class
  (``StrategyMetadata`` in ``almanak.framework.strategies.metadata``)
* per-demo ``config.json`` (runtime config + ``anvil_funding`` +
  optional ``qa`` sub-key)
* ``.github/sidecar-demos.yml`` (connector → demo registry)

All callers — the CLI ``almanak strat demo`` command, sidecar matrix
picker, smoke runner, CI gates, mkdocs demo pages — should import
``DemoSpec`` / ``DemoCatalog`` from this package rather than re-walk the
demo tree or hand-maintain parallel registries. See
``docs/internal/DemoFixing.md`` for the rationale (v2, 2026-05-02).
"""

from __future__ import annotations

from .quarantine import Quarantine, QuarantineEntry, QuarantineExpiredError
from .sidecar import SidecarEntry, SidecarRegistry
from .spec import (
    DemoCatalog,
    DemoLoadError,
    DemoSpec,
    QaConfig,
    default_demos_root,
)

__all__ = [
    "DemoCatalog",
    "DemoLoadError",
    "DemoSpec",
    "QaConfig",
    "Quarantine",
    "QuarantineEntry",
    "QuarantineExpiredError",
    "SidecarEntry",
    "SidecarRegistry",
    "default_demos_root",
]
