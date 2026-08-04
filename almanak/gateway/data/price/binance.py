"""Compatibility alias for the Binance integration price source."""

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.integrations.binance.gateway.price_source import BinancePriceSource as BinancePriceSource
else:
    from almanak.integrations.binance.gateway import price_source as _implementation

    sys.modules[__name__] = _implementation
