"""Back-compat shim: ``fluid.receipt_parser`` re-exports the shared implementation from
``almanak.connectors._fluid_core.receipt_parser`` (single source of truth).
"""

from almanak.connectors._fluid_core import receipt_parser as _impl
from almanak.connectors._fluid_core.receipt_parser import *  # noqa: F401,F403


def __getattr__(name: str):  # noqa: ANN202
    """Re-export private / non-__all__ names for back-compat (PEP 562)."""
    return getattr(_impl, name)
