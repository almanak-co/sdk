"""Back-compat shim: ``aster_perps.adapter`` re-exports the shared implementation
from ``almanak.connectors._aster_perps_core.adapter`` (single source of truth).
"""

from almanak.connectors._aster_perps_core.adapter import *  # noqa: F401,F403
