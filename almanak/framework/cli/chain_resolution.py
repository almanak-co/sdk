"""Lightweight chain-resolution helpers with no heavy CLI dependencies.

Issue #1703: `_run_sweep_task_worker` previously did a lazy
``from ..run import get_default_chain`` inside the worker function.

The lazy import existed because importing ``almanak/framework/cli/run.py``
pulls in gateway modules, every technical-indicator calculator, the full
CLI runtime, and transitively almost the entire framework. That's tolerable
in the main process (the imports happen once at startup), but in a
``ProcessPoolExecutor`` worker we re-import the world for every subprocess.
Worse, the ``from`` inside a worker function means any import failure (e.g.
a missing indicator at install time) turns into a per-task exception
rather than a single fail-fast at CLI launch.

This module hosts the minimal functions that sweep workers need. It has
*no* heavy deps — just stdlib — so sweep.py can import it at module level
and workers pay a near-zero import cost.

``run.py`` continues to re-export ``get_default_chain`` for back-compat;
there is a single source of truth here.
"""

from __future__ import annotations

from typing import Any


def get_default_chain(strategy_class: type[Any]) -> str:
    """Return the default chain for ``strategy_class``.

    Reads ``STRATEGY_METADATA.default_chain``, falling back to
    ``supported_chains[0]``, then the legacy ``SUPPORTED_CHAINS[0]``,
    then ``"arbitrum"`` as a last resort.

    Kept free of framework-internal imports so that the lazy-import
    anti-pattern in sweep workers can be replaced with a cheap
    module-level import (#1703).
    """
    metadata = getattr(strategy_class, "STRATEGY_METADATA", None)
    if metadata is not None:
        default_chain = getattr(metadata, "default_chain", None)
        if default_chain:
            return default_chain
        supported_chains = getattr(metadata, "supported_chains", None)
        if supported_chains:
            return supported_chains[0]
    # Legacy fallback
    supported = getattr(strategy_class, "SUPPORTED_CHAINS", None)
    if supported:
        return supported[0]
    return "arbitrum"


__all__ = ["get_default_chain"]
