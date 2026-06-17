"""Back-compat shim: ``aster_perps.addresses`` re-exports the shared implementation
from ``almanak.connectors._aster_perps_core.addresses`` (single source of truth).
"""

from almanak.connectors._aster_perps_core.addresses import *  # noqa: F401,F403
