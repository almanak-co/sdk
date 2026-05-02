"""Packaged demo strategies for `almanak strat demo`.

This package contains the demo strategies that ship with the almanak
package. The `demo` CLI command copies a strategy directory from here
into the user's working directory so they can immediately run and modify
it.

The metadata helpers in this module delegate to
``almanak.framework.demos.DemoCatalog`` — the single source of truth
for demo discovery (see ``docs/internal/DemoFixing.md``).
"""

from __future__ import annotations

import functools
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_PACKAGE_DIR = Path(__file__).parent


def _discover_strategy_names() -> tuple[str, ...]:
    """Cheap, framework-free probe for demo directory slugs.

    Mirrors :class:`DemoCatalog.discover`'s gate: a directory is a demo iff
    it contains a ``strategy.py`` (config.json is optional). Kept aligned
    with the catalog so ``DEMO_STRATEGY_NAMES`` and
    ``DemoCatalog.specs[*].directory.name`` cannot drift on a fresh
    checkout.
    """
    return tuple(
        sorted(p.parent.name for p in _PACKAGE_DIR.glob("*/strategy.py") if not p.parent.name.startswith(("_", ".")))
    )


# Strategy names are the subdirectory names that contain a strategy.py.
# Immutable to prevent mutation-based bypass of the path-traversal guard.
DEMO_STRATEGY_NAMES: tuple[str, ...] = _discover_strategy_names()


@functools.lru_cache(maxsize=1)
def _catalog():
    """Lazy DemoCatalog singleton scoped to this package directory.

    Imported lazily so importing ``almanak.demo_strategies`` does not
    transitively import the framework package.
    """
    from almanak.framework.demos import DemoCatalog

    return DemoCatalog.discover(_PACKAGE_DIR)


def get_demo_strategy_path(name: str) -> Path:
    """Return the absolute path to a demo strategy directory.

    Raises:
        KeyError: If no demo strategy with the given name exists.
    """
    if name not in DEMO_STRATEGY_NAMES:
        raise KeyError(f"Unknown demo strategy '{name}'. Available: {', '.join(DEMO_STRATEGY_NAMES)}")
    return _PACKAGE_DIR / name


def get_demo_strategy_metadata(name: str) -> dict:
    """Return metadata (description, chain) for a demo strategy.

    Sources the data from ``DemoSpec`` so the chain reflects the
    decorator's ``default_chain``. Loads the requested demo on its own
    rather than walking the whole tree, so a single demo lookup is cheap.
    """
    from almanak.framework.demos import DemoSpec

    path = get_demo_strategy_path(name)
    try:
        spec = DemoSpec.load(path)
    except Exception as exc:  # noqa: BLE001 - exec_module can raise anything
        # ``DemoSpec.load`` execs ``strategy.py``; the imported code can
        # raise any exception (ImportError, SyntaxError, OSError on a
        # missing config file, custom exceptions, …). The caller asks
        # only for description + chain — fall back rather than crash.
        logger.warning("Failed to load demo metadata for %s: %s", name, exc)
        return {"description": "", "chain": "unknown"}
    return {
        "description": spec.description,
        "chain": spec.default_chain or "unknown",
    }


def list_demo_strategies() -> list[dict]:
    """Return a sorted list of dicts with name, description, chain.

    Uses the cached ``DemoCatalog`` so a CLI invocation walks the demo
    tree exactly once even when listing every demo. ``name`` is the
    directory slug — the same key accepted by ``get_demo_strategy_path``
    and ``almanak strat demo --name``.
    """
    catalog = _catalog()
    return [
        {
            "name": spec.directory.name,
            "description": spec.description,
            "chain": spec.default_chain or "unknown",
        }
        for spec in catalog.specs
    ]
