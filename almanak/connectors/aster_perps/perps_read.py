"""Back-compat shim: ``aster_perps.perps_read`` re-exports the shared implementation
from ``almanak.connectors._aster_perps_core.perps_read`` (single source of truth).
"""

from almanak.connectors._aster_perps_core.perps_read import *  # noqa: F401,F403
