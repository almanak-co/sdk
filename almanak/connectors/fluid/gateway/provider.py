"""Back-compat shim: ``fluid.gateway.provider`` re-exports
``almanak.connectors._fluid_core.gateway.provider``.
"""

from almanak.connectors._fluid_core.gateway import provider as _impl
from almanak.connectors._fluid_core.gateway.provider import *  # noqa: F401,F403


def __getattr__(name: str):  # noqa: ANN202
    return getattr(_impl, name)
