"""Back-compat shim: ``aster_perps.sdk`` re-exports the shared implementation
from ``almanak.connectors._aster_perps_core.sdk`` (single source of truth).
"""

from almanak.connectors._aster_perps_core.sdk import *  # noqa: F401,F403
from almanak.connectors._aster_perps_core.sdk import (
    _check_address,  # noqa: F401 — private helper used by tests/intents/bnb/conftest.py
)
