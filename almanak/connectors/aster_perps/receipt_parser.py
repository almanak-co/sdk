"""Back-compat shim: ``aster_perps.receipt_parser`` re-exports the shared implementation
from ``almanak.connectors._aster_perps_core.receipt_parser`` (single source of truth).
"""

from almanak.connectors._aster_perps_core.receipt_parser import *  # noqa: F401,F403
