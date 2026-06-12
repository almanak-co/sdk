"""Aave V3 backtest liquidation risk parameters (plan 022).

Values moved verbatim from
``almanak.framework.backtesting.pnl.calculators.liquidation_params``.
The connector manifest wires ``BACKTEST_RISK`` into ``Connector.backtest_risk``
so that ``LiquidationParamRegistry`` can derive its tables from the manifest
instead of carrying duplicate literals.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._connector_descriptor import BacktestRiskDecl, LiquidationDefault

BACKTEST_RISK = BacktestRiskDecl(
    liquidation_default=LiquidationDefault(
        liquidation_threshold=Decimal("0.825"),  # Average across assets
        maintenance_margin=Decimal("0"),  # N/A for lending
        liquidation_penalty=Decimal("0.05"),  # 5% penalty
    ),
    liquidation_asset_params={
        # (liquidation_threshold, maintenance_margin, liquidation_penalty)
        "ETH": (Decimal("0.86"), Decimal("0"), Decimal("0.05")),
        "WETH": (Decimal("0.86"), Decimal("0"), Decimal("0.05")),
        "WBTC": (Decimal("0.80"), Decimal("0"), Decimal("0.065")),
        "USDC": (Decimal("0.88"), Decimal("0"), Decimal("0.045")),
        "USDT": (Decimal("0.80"), Decimal("0"), Decimal("0.05")),
        "DAI": (Decimal("0.80"), Decimal("0"), Decimal("0.05")),
        "LINK": (Decimal("0.75"), Decimal("0"), Decimal("0.075")),
        "AAVE": (Decimal("0.73"), Decimal("0"), Decimal("0.075")),
        "UNI": (Decimal("0.77"), Decimal("0"), Decimal("0.10")),
        "WSTETH": (Decimal("0.84"), Decimal("0"), Decimal("0.05")),
        "CBETH": (Decimal("0.80"), Decimal("0"), Decimal("0.075")),
        "RETH": (Decimal("0.79"), Decimal("0"), Decimal("0.075")),
    },
)

__all__ = ["BACKTEST_RISK"]
