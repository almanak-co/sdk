"""Unresolved Morpho economic choices, with exact named market/account references.

One allocation is one fund's bounded purpose on one resolved market leg. Author
quantities stay unconverted until the leg's verified precision is known.
"""

from typing import Literal

from pydantic import Field

from almanak.configuration.input_values import Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.connectors.lending_health import (
    MINIMUM_HEALTH_FACTOR_DESCRIPTION,
    health_factor_input,
)
from almanak.connectors.morpho_blue.models import Allocation
from almanak.connectors.morpho_blue.resolution import ResolvedMarket
from almanak.gateway.evm import native_metadata


class AllocationInput(PositionInput):
    connector: Literal["morpho_blue"] = Field(default="morpho_blue", exclude=True)
    # The reserve asset the allocation starts with; omitted means none.
    capital: Quantity | None = Field(default=None, exclude_if=lambda value: value is None)
    borrow: bool = False
    minimum_health_factor: Quantity | None = Field(
        default=None, description=MINIMUM_HEALTH_FACTOR_DESCRIPTION
    )
    minimum_native_balance: Quantity

    def resolve(self, name: str, market: ResolvedMarket) -> Allocation:
        identity, native = native_metadata(market.selection.chain)
        result = self.model_dump(exclude={"market"})
        result.update(
            name=name,
            reserve=market.reserve,
            native_asset=identity,
            minimum_health_factor=health_factor_input(self.minimum_health_factor),
            minimum_native_balance=converted(
                self.minimum_native_balance, native.decimals, "minimum_native_balance"
            ),
        )
        result["capital"] = (
            0 if self.capital is None else converted(self.capital, market.asset.decimals, "capital")
        )
        return Allocation.model_validate(result)
