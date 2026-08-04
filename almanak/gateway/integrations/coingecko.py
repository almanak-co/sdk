"""Compatibility alias for the provider-owned CoinGecko client."""

import sys

from almanak.integrations.coingecko.gateway import client as _implementation

sys.modules[__name__] = _implementation
