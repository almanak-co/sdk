"""Compatibility alias for the DexScreener integration price source."""

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.integrations.dexscreener.gateway.price_source import (
        CHAIN_TO_DEXSCREENER_PLATFORM as CHAIN_TO_DEXSCREENER_PLATFORM,
    )
    from almanak.integrations.dexscreener.gateway.price_source import (
        DexScreenerPriceSource as DexScreenerPriceSource,
    )
else:
    from almanak.integrations.dexscreener.gateway import price_source as _implementation

    sys.modules[__name__] = _implementation
