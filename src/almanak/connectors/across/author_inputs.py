"""Unresolved bridge choices, with exact named market and account references."""

from typing import Literal

from pydantic import Field

from almanak.configuration.input_values import Quantity
from almanak.configuration.positions import PositionInput, converted
from almanak.connectors.across.models import Allocation
from almanak.connectors.across.resolution import ResolvedRoute
from almanak.gateway.evm import native_metadata
from almanak.gateway.operator_config import Name


class AllocationInput(PositionInput):
    connector: Literal["across"] = Field(default="across", exclude=True)
    # The named account on the market's destination chain that receives the bridged asset.
    destination_account: Name
    # The sent asset the allocation starts with; omitted means none.
    capital: Quantity | None = Field(default=None, exclude_if=lambda value: value is None)
    minimum_native_balance: Quantity

    def resolve(self, name: str, market: ResolvedRoute) -> Allocation:
        identity, native = native_metadata(market.selection.chain)
        capital = (
            0 if self.capital is None else converted(self.capital, market.asset.decimals, "capital")
        )
        return Allocation(
            name=name,
            account=self.account,
            destination_account=self.destination_account,
            route=market.route,
            capital=capital,
            native_asset=identity,
            minimum_native_balance=converted(
                self.minimum_native_balance, native.decimals, "minimum_native_balance"
            ),
        )
