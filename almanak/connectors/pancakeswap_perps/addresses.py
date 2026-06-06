"""PancakeSwap Perps address-table declaration.

PancakeSwap Perps is a deprecated shim over Aster Perps. The address dict
remains re-exported from ``aster_perps.addresses`` for backward-compatible
imports, but this connector owns the ``pancakeswap_perps`` registry declaration
so deleting the shim folder removes the shim protocol key.
"""

from __future__ import annotations

from almanak.connectors.aster_perps.addresses import PANCAKESWAP_PERPS

__all__ = ["PANCAKESWAP_PERPS"]
