"""Compatibility alias for the provider-owned Chainlink price source."""

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.integrations.chainlink.gateway.live import ChainlinkPriceSource as ChainlinkPriceSource
    from almanak.integrations.chainlink.gateway.live import OnChainPriceSource as OnChainPriceSource
else:
    from almanak.integrations.chainlink.gateway import live as _implementation

    sys.modules[__name__] = _implementation
