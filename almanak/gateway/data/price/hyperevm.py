"""Compatibility alias for the HyperCore integration price source."""

import sys

from almanak.integrations.hypercore.gateway import price_source as _implementation

sys.modules[__name__] = _implementation
