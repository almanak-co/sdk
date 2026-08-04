"""Compatibility alias for the provider-neutral integration runtime."""

import sys

from almanak.integrations._base.gateway import base as _implementation

sys.modules[__name__] = _implementation
