"""Explicit Uniswap execution costs over shared hypothetical account inventory."""

from typing import Literal

from pydantic import Field

from almanak.backtest.inventory import Fund as Fund
from almanak.backtest.inventory import InventoryConfig as InventoryConfig
from almanak.backtest.strategy_prices import StrategyPriceData
from almanak.models import Hash32, Model, UInt

OperationKind = Literal[
    "approve_erc20",
    "approve_permit2",
    "revoke_erc20",
    "revoke_permit2",
    "swap",
    "mint",
    "collect",
    "burn",
]


class FixedCosts(Model):
    kind: Literal["fixed_native"]
    calibration_sha256: Hash32
    per_operation: dict[OperationKind, UInt]


class BacktestConfig(InventoryConfig):
    market_model: Literal["historical_path"]
    swap_model: Literal["historical_quoter"]
    liquidity_model: Literal["fixed_historical_fee_growth"] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    approval_model: Literal["at_intent_eligibility"]
    costs: FixedCosts
    # The strategy's declared series, folded in at resolution; omitted when none.
    data: StrategyPriceData = Field(
        default_factory=StrategyPriceData, exclude_if=lambda d: not d.prices
    )
