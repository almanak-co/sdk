"""Compatibility alias for the integration circuit breaker."""

import sys

from almanak.integrations._base.gateway import circuit_breaker as _implementation

sys.modules[__name__] = _implementation
