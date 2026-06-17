"""PancakeSwap Perps address-table declaration.

PancakeSwap Perps is a deprecated shim over Aster Perps. The address dict is
re-exported from the shared ``_aster_perps_core.addresses`` foundation (same
Diamond router), not from the sibling ``aster_perps`` leaf, so deleting either
leaf does not strand the other. This connector still owns the
``pancakeswap_perps`` registry declaration, so deleting the shim folder removes
the shim protocol key.
"""

from __future__ import annotations

from almanak.connectors._aster_perps_core.addresses import PANCAKESWAP_PERPS

__all__ = ["PANCAKESWAP_PERPS"]
