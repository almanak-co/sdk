"""Back-compat shim: ``fluid.rates_provider`` re-exports the shared implementation from
``almanak.connectors._fluid_core.rates_provider`` (single source of truth).
"""

from almanak.connectors._fluid_core import rates_provider as _impl
from almanak.connectors._fluid_core.rates_provider import *  # noqa: F401,F403


def __getattr__(name: str):  # noqa: ANN202
    """Re-export private / non-__all__ names for back-compat (PEP 562)."""
    return getattr(_impl, name)
