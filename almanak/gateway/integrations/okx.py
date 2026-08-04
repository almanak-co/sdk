"""Compatibility alias for the provider-owned OKX client."""

import sys

from almanak.integrations.okx.gateway import client as _implementation

sys.modules[__name__] = _implementation
