"""Unresolved Aave V3 reserve economics, with exact named market/account references."""

from typing import Literal

from pydantic import Field

from almanak.connectors.aave_v3 import reader as aave_v3_reader
from almanak.connectors.aave_v3.models import Allocation
from almanak.connectors.aave_v3.resolution import ResolvedReserve
from almanak.connectors.pool_lending_authoring import (
    ReserveAllocationInput,
    allocation_values,
    require_supply_authority,
)


class AllocationInput(ReserveAllocationInput):
    connector: Literal["aave_v3"] = Field(default="aave_v3", exclude=True)

    def resolve(self, name: str, market: ResolvedReserve) -> Allocation:
        return Allocation.model_validate(allocation_values(self, name, market))

    def check_resolved(self, reader, market: ResolvedReserve):
        require_supply_authority(self, aave_v3_reader, reader, market)
