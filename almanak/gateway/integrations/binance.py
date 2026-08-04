"""Compatibility alias for the provider-owned Binance client."""

import sys

from almanak.integrations.binance.gateway import client as _implementation

sys.modules[__name__] = _implementation
