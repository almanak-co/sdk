"""Unresolved Comet economic choices, with exact named market/account references.

The connector's risk and lifecycle modules own the strategy-facing helpers, so
these public author inputs live beside them under their own name.
"""

from typing import Literal

from pydantic import Field

from almanak.configuration.input_values import Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.connectors.compound_v3.models import Allocation
from almanak.connectors.compound_v3.resolution import ResolvedMarket
from almanak.connectors.lending_health import (
    MINIMUM_HEALTH_FACTOR_DESCRIPTION,
    health_factor_input,
)
from almanak.gateway.evm import native_metadata


class AllocationInput(PositionInput):
    connector: Literal["compound_v3"] = Field(default="compound_v3", exclude=True)
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
