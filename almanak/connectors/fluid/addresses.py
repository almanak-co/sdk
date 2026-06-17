"""Back-compat shim: ``fluid.addresses`` re-exports the shared implementation from
``almanak.connectors._fluid_core.addresses`` (single source of truth).
"""

from almanak.connectors._fluid_core import addresses as _impl
from almanak.connectors._fluid_core.addresses import *  # noqa: F401,F403


def __getattr__(name: str):  # noqa: ANN202
    """Re-export private / non-__all__ names for back-compat (PEP 562)."""
    return getattr(_impl, name)
