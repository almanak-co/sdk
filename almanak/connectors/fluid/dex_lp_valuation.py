"""Back-compat shim: ``fluid.dex_lp_valuation`` re-exports the shared implementation from
``almanak.connectors._fluid_core.dex_lp_valuation`` (single source of truth).
"""

from almanak.connectors._fluid_core import dex_lp_valuation as _impl
from almanak.connectors._fluid_core.dex_lp_valuation import *  # noqa: F401,F403


def __getattr__(name: str):  # noqa: ANN202
    """Re-export private / non-__all__ names for back-compat (PEP 562)."""
    return getattr(_impl, name)
