"""Unresolved Aave economic choices, with exact named market/account references."""

from typing import Annotated, Literal

from pydantic import Field

from almanak.configuration.input_values import Duration, Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.connectors.aave_v4.models import Allocation, DebtAllocation
from almanak.connectors.aave_v4.resolution import ResolvedReserve
from almanak.gateway.evm import native_metadata
from almanak.gateway.operator_config import Name
from almanak.models import Address
from almanak.triggers import HOLD_SECONDS_DESCRIPTION


class PositionValues(PositionInput):
    connector: Literal["aave_v4"] = Field(default="aave_v4", exclude=True)
    minimum_native_balance: Quantity
    hold_seconds: Duration | None = Field(default=None, description=HOLD_SECONDS_DESCRIPTION)

    def values(self, name: str, market: ResolvedReserve):
        identity, native = native_metadata(market.selection.chain)
        result = self.model_dump(exclude={"market"})
        result.update(name=name, reserve=market.reserve, native_asset=identity)
        result["minimum_native_balance"] = converted(
            self.minimum_native_balance, native.decimals, "minimum_native_balance"
        )
        return result


class SupplyInput(PositionValues):
    # The reserve asset the allocation starts with; omitted means none.
    capital: Quantity | None = Field(default=None, exclude_if=lambda value: value is None)

    def resolve(self, name: str, market: ResolvedReserve) -> Allocation:
        result = self.values(name, market)
        result["capital"] = (
            0 if self.capital is None else converted(self.capital, market.asset.decimals, "capital")
        )
        return Allocation.model_validate(result)


class DebtInput(PositionValues):
    """A debt names its reserve; how much to borrow is the strategy's decision."""

    collateral: Annotated[tuple[Name, ...], Field(min_length=1)]
    minimum_health_factor: Quantity
    # These remain deliberately pinned source choices, not protocol-relative policies.
    oracle: Address
    collateral_price_sources: tuple[Address, ...]
    debt_price_source: Address

    def resolve(self, name: str, market: ResolvedReserve) -> DebtAllocation:
        result = self.values(name, market)
        result["minimum_health_factor"] = converted(
            self.minimum_health_factor, 18, "minimum_health_factor"
        )
        return DebtAllocation.model_validate(result)
