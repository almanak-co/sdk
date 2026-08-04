"""Compatibility alias for portfolio-provider orchestration."""

import sys

from almanak.integrations._base.gateway import portfolio_chain as _implementation

sys.modules[__name__] = _implementation
