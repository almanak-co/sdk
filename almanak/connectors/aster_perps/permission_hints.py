"""Back-compat shim: ``aster_perps.permission_hints`` re-exports the shared implementation
from ``almanak.connectors._aster_perps_core.permission_hints`` (single source of truth).
"""

from almanak.connectors._aster_perps_core.permission_hints import *  # noqa: F401,F403
