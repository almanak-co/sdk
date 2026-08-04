"""Compatibility alias for the provider-owned The Graph client."""

import sys

from almanak.integrations.thegraph.gateway import client as _implementation

sys.modules[__name__] = _implementation
