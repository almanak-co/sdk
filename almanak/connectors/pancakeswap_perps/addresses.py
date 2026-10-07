"""PancakeSwap Perps address-table declaration.

Re-exports the legacy Aster Diamond router table from the shared
``_aster_perps_core.addresses`` foundation.
"""

from __future__ import annotations

from almanak.connectors._aster_perps_core.addresses import PANCAKESWAP_PERPS

__all__ = ["PANCAKESWAP_PERPS"]
