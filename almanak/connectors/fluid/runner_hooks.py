"""Back-compat shim: ``fluid.runner_hooks`` re-exports the shared implementation from
``almanak.connectors._fluid_core.runner_hooks`` (single source of truth).
"""

from almanak.connectors._fluid_core import runner_hooks as _impl
from almanak.connectors._fluid_core.runner_hooks import *  # noqa: F401,F403


def __getattr__(name: str):  # noqa: ANN202
    """Re-export private / non-__all__ names for back-compat (PEP 562)."""
    return getattr(_impl, name)
