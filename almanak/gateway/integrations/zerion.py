"""Compatibility alias for the provider-owned Zerion client."""

import sys

from almanak.integrations.zerion.gateway import client as _implementation

sys.modules[__name__] = _implementation
