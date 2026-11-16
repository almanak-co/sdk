"""Economic exchange and liquidity intents with concrete connector targets."""

from abc import ABC, abstractmethod
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.asset_amounts import AssetAmounts
from almanak.caip import AssetType
from almanak.models import Account, AssetAmount, Model, UInt

Name = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]
Tick = Annotated[int, Field(strict=True, ge=-887272, le=887272)]


class TickBounds(Model):
    """One concrete concentrated-liquidity range, chosen inside an authorized band."""

    lower: Tick
    upper: Tick

    @model_validator(mode="after")
    def ordered(self):
        if self.lower >= self.upper:
            raise ValueError("liquidity range must have increasing tick bounds")
        return self


class DexTarget(Model, ABC):
    allocation: Name
    account: Name
    fund: Account

    @property
    @abstractmethod
    def assets(self) -> tuple[AssetType, ...]:
        """The connector's explicitly selected market currencies."""


class DexIntent[Target: DexTarget](Model):
    target: Target

    @model_validator(mode="after")
    def concrete_target(self):
        if type(self.target) is not type(self).model_fields["target"].annotation:
            raise ValueError("exchange intents require their exact concrete target type")
        return self


class SwapExactInput[Target: DexTarget](DexIntent[Target]):
    kind: Literal["swap_exact_input"] = "swap_exact_input"
    amount: AssetAmount
    minimum_output: AssetAmount
    deadline: UInt

    @model_validator(mode="after")
    def units(self):
        pair = {self.amount.asset, self.minimum_output.asset}
        if len(pair) != 2 or not pair.issubset(self.target.assets):
            raise ValueError("swap amounts must identify two different target assets")
        if min(self.amount.amount, self.minimum_output.amount) <= 0:
            raise ValueError("swap requires positive input and minimum output")
        return self


class LiquidityBudget[Target: DexTarget](DexIntent[Target]):
    kind: str
    maximum0: AssetAmount
    maximum1: AssetAmount
    deadline: UInt

    @model_validator(mode="after")
    def units(self):
        amounts = AssetAmounts((self.maximum0, self.maximum1))
        amounts.in_order(self.target.assets)
        amounts.require_inventory()
        return self


class OpenLiquidity[Target: DexTarget](LiquidityBudget[Target]):
    kind: Literal["open_liquidity"] = "open_liquidity"
    # Only a banded target chooses its range at decision time; a fixed range stays on the target.
    range: TickBounds | None = Field(default=None, exclude_if=lambda v: v is None)


class IncreaseLiquidity[Target: DexTarget](LiquidityBudget[Target]):
    kind: Literal["increase_liquidity"] = "increase_liquidity"


class CollectLiquidityFees[Target: DexTarget](DexIntent[Target]):
    kind: Literal["collect_liquidity_fees"] = "collect_liquidity_fees"
    deadline: UInt


class LiquidityMinimum[Target: DexTarget](DexIntent[Target]):
    kind: str
    minimum0: AssetAmount
    minimum1: AssetAmount
    deadline: UInt

    @model_validator(mode="after")
    def units(self):
        AssetAmounts((self.minimum0, self.minimum1)).in_order(self.target.assets)
        return self


class CloseLiquidity[Target: DexTarget](LiquidityMinimum[Target]):
    kind: Literal["close_liquidity"] = "close_liquidity"


class DecreaseLiquidity[Target: DexTarget](LiquidityMinimum[Target]):
    kind: Literal["decrease_liquidity"] = "decrease_liquidity"
    liquidity: Annotated[UInt, Field(gt=0)]


class PoolLiquidityBudget[Target: DexTarget](DexIntent[Target]):
    """Pool-wide quantities for markets whose coin count is part of the target."""

    kind: str
    maximums: AssetAmounts
    deadline: UInt

    @model_validator(mode="after")
    def units(self):
        self.maximums.in_order(self.target.assets)
        self.maximums.require_inventory()
        return self


class OpenPoolLiquidity[Target: DexTarget](PoolLiquidityBudget[Target]):
    kind: Literal["open_liquidity"] = "open_liquidity"


class IncreasePoolLiquidity[Target: DexTarget](PoolLiquidityBudget[Target]):
    kind: Literal["increase_liquidity"] = "increase_liquidity"


class PoolLiquidityMinimum[Target: DexTarget](DexIntent[Target]):
    kind: str
    minimums: AssetAmounts
    output_asset: AssetType | None = None
    deadline: UInt

    @model_validator(mode="after")
    def units(self):
        self.minimums.in_order(self.target.assets)
        if self.output_asset is not None:
            if self.output_asset not in self.target.assets:
                raise ValueError("single-coin removal must select a configured pool asset")
            if any(
                value.amount for value in self.minimums.root if value.asset != self.output_asset
            ):
                raise ValueError("single-coin removal cannot require output in other assets")
        return self


class ClosePoolLiquidity[Target: DexTarget](PoolLiquidityMinimum[Target]):
    kind: Literal["close_liquidity"] = "close_liquidity"


class DecreasePoolLiquidity[Target: DexTarget](PoolLiquidityMinimum[Target]):
    kind: Literal["decrease_liquidity"] = "decrease_liquidity"
    liquidity: Annotated[UInt, Field(gt=0)]


class RevokeExchange[Target: DexTarget](DexIntent[Target]):
    """Request economic closure of spending authority; the connector chooses calls."""

    kind: Literal["revoke_exchange"] = "revoke_exchange"


class ExitExchange[Target: DexTarget](DexIntent[Target]):
    """Close owned liquidity, return to configured funding currency and clear authority."""

    kind: Literal["exit_exchange"] = "exit_exchange"
