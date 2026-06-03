"""Back-compat shim for GMX V2 perpetual position valuation.

The GMX mark-to-market formula and the ``30``-decimal USD constant relocated
into the connector (:mod:`almanak.connectors.gmx_v2.perps_read`, as
``value_perps_position`` / ``_GMX_USD_DECIMALS``) when perp read+value became a
connector-published capability (VIB-4930). The framework now reaches the math
through :class:`~almanak.connectors._strategy_base.perps_read_registry.PerpsReadRegistry`
rather than importing it.

Only the shared :class:`PerpsPositionValue` result type is re-exported here so
external importers of ``perps_valuer.PerpsPositionValue`` keep working; its
canonical home is :mod:`almanak.connectors._strategy_base.perps_read_base`.
"""

from almanak.connectors._strategy_base.perps_read_base import PerpsPositionValue

__all__ = ["PerpsPositionValue"]
