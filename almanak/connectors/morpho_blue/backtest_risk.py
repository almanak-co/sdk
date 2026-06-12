"""Morpho Blue backtest liquidation risk parameters (plan 022).

The historical key for this connector in ``LiquidationParamRegistry`` is ``"morpho"``
(not ``"morpho_blue"``). The rewire layer in ``liquidation_params.py`` handles the
alias mapping; the values here are verbatim from the pre-rewire literal.

No asset-specific rows exist for morpho in the baseline; only a protocol default.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._connector_descriptor import BacktestRiskDecl, LiquidationDefault

BACKTEST_RISK = BacktestRiskDecl(
    liquidation_default=LiquidationDefault(
        liquidation_threshold=Decimal("0.825"),  # Uses Aave thresholds
        maintenance_margin=Decimal("0"),
        liquidation_penalty=Decimal("0.05"),
    ),
    liquidation_asset_params=None,
    legacy_param_keys=("morpho",),
)

__all__ = ["BACKTEST_RISK"]
