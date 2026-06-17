"""Back-compat shim: ``fluid.gateway.market_lookup`` re-exports
``almanak.connectors._fluid_core.gateway.market_lookup``.
"""

from almanak.connectors._fluid_core.gateway import market_lookup as _impl
from almanak.connectors._fluid_core.gateway.market_lookup import *  # noqa: F401,F403


def __getattr__(name: str):  # noqa: ANN202
    return getattr(_impl, name)
