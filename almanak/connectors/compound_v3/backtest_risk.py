"""Compound V3 backtest liquidation risk parameters (plan 022).

Values moved verbatim from
``almanak.framework.backtesting.pnl.calculators.liquidation_params``.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._connector_descriptor import BacktestRiskDecl, LiquidationDefault

BACKTEST_RISK = BacktestRiskDecl(
    liquidation_default=LiquidationDefault(
        liquidation_threshold=Decimal("0.85"),
        maintenance_margin=Decimal("0"),
        liquidation_penalty=Decimal("0.05"),
    ),
    liquidation_asset_params={
        # (liquidation_threshold, maintenance_margin, liquidation_penalty)
        "ETH": (Decimal("0.90"), Decimal("0"), Decimal("0.05")),
        "WETH": (Decimal("0.90"), Decimal("0"), Decimal("0.05")),
        "WBTC": (Decimal("0.80"), Decimal("0"), Decimal("0.05")),
        "WSTETH": (Decimal("0.90"), Decimal("0"), Decimal("0.05")),
        "CBETH": (Decimal("0.90"), Decimal("0"), Decimal("0.05")),
    },
)

__all__ = ["BACKTEST_RISK"]
