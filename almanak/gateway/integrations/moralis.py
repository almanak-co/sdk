"""Compatibility alias for the provider-owned Moralis client."""

import sys

from almanak.integrations.moralis.gateway import client as _implementation

sys.modules[__name__] = _implementation
