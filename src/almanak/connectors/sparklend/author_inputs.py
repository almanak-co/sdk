"""Unresolved SparkLend reserve economics, with exact named market/account references."""

from typing import Literal

from pydantic import Field

from almanak.connectors.pool_lending_authoring import (
    ReserveAllocationInput,
    allocation_values,
    require_supply_authority,
)
from almanak.connectors.sparklend import reader as sparklend_reader
from almanak.connectors.sparklend.models import Allocation
from almanak.connectors.sparklend.resolution import ResolvedReserve


class AllocationInput(ReserveAllocationInput):
    connector: Literal["sparklend"] = Field(default="sparklend", exclude=True)

    def resolve(self, name: str, market: ResolvedReserve) -> Allocation:
        return Allocation.model_validate(allocation_values(self, name, market))

    def check_resolved(self, reader, market: ResolvedReserve):
        require_supply_authority(self, sparklend_reader, reader, market)
