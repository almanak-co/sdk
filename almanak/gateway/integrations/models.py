"""Compatibility alias for provider-neutral integration models."""

import sys

from almanak.integrations._base.gateway import models as _implementation

sys.modules[__name__] = _implementation
