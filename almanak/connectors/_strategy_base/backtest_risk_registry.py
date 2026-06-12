"""Strategy-side backtest risk-parameter registry (plan 022).

Connectors that declare per-protocol/asset liquidation defaults for backtesting
publish a ``BacktestRiskDecl`` on their ``CONNECTOR`` manifest. The population
step in ``almanak.connectors._strategy_backtest_risk_registry`` iterates the
connector registry and registers each connector's decl into this registry.

``LiquidationParamRegistry._initialize_defaults`` and
``_initialize_asset_defaults`` derive their tables from this registry instead of
carrying hardcoded literals.
"""

from __future__ import annotations

from almanak.connectors._connector_descriptor import BacktestRiskDecl

__all__ = [
    "BACKTEST_RISK_REGISTRY",
    "BacktestRiskRegistry",
]


class BacktestRiskRegistry:
    """Registry mapping connector name -> BacktestRiskDecl.

    Populated lazily by ``_strategy_backtest_risk_registry`` on first access
    via ``liquidation_params`` / ``liquidation_params``.

    Keys are connector names (lowercase, hyphen-free folder names), NOT
    historical protocol aliases.  The rewire layer in ``liquidation_params.py``
    is responsible for translating connector names to the legacy protocol keys
    that ``LiquidationParamRegistry.protocol_defaults`` uses.
    """

    def __init__(self) -> None:
        self._map: dict[str, BacktestRiskDecl] = {}

    def register(self, connector_name: str, decl: BacktestRiskDecl) -> None:
        """Register a BacktestRiskDecl for ``connector_name`` (idempotent)."""
        self._map[connector_name] = decl

    def liquidation_params(self) -> dict[str, BacktestRiskDecl]:
        """Return a snapshot of the full registry keyed by connector name."""
        return dict(self._map)

    def get(self, connector_name: str) -> BacktestRiskDecl | None:
        """Return the decl for ``connector_name``, or None if not registered."""
        return self._map.get(connector_name)


BACKTEST_RISK_REGISTRY = BacktestRiskRegistry()
