"""Compatibility alias for the CoinGecko integration price source."""

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.integrations.coingecko.gateway.price_source import CoinGeckoPriceSource as CoinGeckoPriceSource
else:
    from almanak.integrations.coingecko.gateway import price_source as _implementation

    sys.modules[__name__] = _implementation
