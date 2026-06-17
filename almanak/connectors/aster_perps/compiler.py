"""Back-compat shim: ``aster_perps.compiler`` re-exports the shared implementation
from ``almanak.connectors._aster_perps_core.compiler`` (single source of truth).
"""

from almanak.connectors._aster_perps_core.compiler import *  # noqa: F401,F403
