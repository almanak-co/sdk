"""Hyperliquid backtest liquidation risk parameters (plan 022).

Values moved verbatim from
``almanak.framework.backtesting.pnl.calculators.liquidation_params``.
No asset-specific rows exist for hyperliquid in the baseline; only a protocol default.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._connector_descriptor import BacktestRiskDecl, LiquidationDefault

BACKTEST_RISK = BacktestRiskDecl(
    liquidation_default=LiquidationDefault(
        liquidation_threshold=Decimal("0"),
        maintenance_margin=Decimal("0.005"),  # 0.5% maintenance margin
        liquidation_penalty=Decimal("0.05"),
    ),
    liquidation_asset_params=None,
)

__all__ = ["BACKTEST_RISK"]
