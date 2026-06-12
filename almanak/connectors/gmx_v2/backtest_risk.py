"""GMX V2 backtest liquidation risk parameters (plan 022).

The historical registry carries BOTH ``"gmx"`` and ``"gmx_v2"`` keys — both
pointing to the same defaults. The rewire layer in ``liquidation_params.py``
handles that alias duplication; the values here are verbatim from the baseline.

Asset-specific rows carry only ``maintenance_margin`` (threshold=0, penalty=0.05
constant across all assets).
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._connector_descriptor import BacktestRiskDecl, LiquidationDefault

BACKTEST_RISK = BacktestRiskDecl(
    liquidation_default=LiquidationDefault(
        liquidation_threshold=Decimal("0"),
        maintenance_margin=Decimal("0.01"),  # 1% maintenance margin
        liquidation_penalty=Decimal("0.05"),
    ),
    legacy_param_keys=("gmx", "gmx_v2"),
    liquidation_asset_params={
        # (liquidation_threshold, maintenance_margin, liquidation_penalty)
        "ETH": (Decimal("0"), Decimal("0.01"), Decimal("0.05")),  # 1%
        "BTC": (Decimal("0"), Decimal("0.01"), Decimal("0.05")),  # 1%
        "LINK": (Decimal("0"), Decimal("0.015"), Decimal("0.05")),  # 1.5% (more volatile)
        "ARB": (Decimal("0"), Decimal("0.02"), Decimal("0.05")),  # 2% (more volatile)
        "UNI": (Decimal("0"), Decimal("0.02"), Decimal("0.05")),
        "SOL": (Decimal("0"), Decimal("0.015"), Decimal("0.05")),
    },
)

__all__ = ["BACKTEST_RISK"]
