"""Compatibility alias for the Pyth integration price source."""

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.integrations.pyth.gateway.price_source import PythPriceSource as PythPriceSource
else:
    from almanak.integrations.pyth.gateway import price_source as _implementation

    sys.modules[__name__] = _implementation
