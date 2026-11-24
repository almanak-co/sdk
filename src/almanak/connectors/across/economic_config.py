"""What a modeled bridge assumes: how long a transfer takes and what its gas costs.

The model reads no history. Both figures are statements of the backtest
configuration, fixed for the run and reported as assumptions, never measurements.
"""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.config import ExcludedCosts
from almanak.backtest.inventory import InventoryConfig
from almanak.models import Model, UInt
from almanak.triggers import PositiveSeconds

BridgeModel = Literal["full_fee_ceiling_fixed_latency"]


class BridgePolicy(Model):
    # Seconds from the modeled deposit to the modeled arrival. Stated, never defaulted.
    arrival_latency_seconds: PositiveSeconds


class Costs(Model):
    kind: Literal["fixed_native"]
    # Native cost of one bridge on its source chain: its approval and its deposit.
    bridge: UInt


class EconomicConfig(InventoryConfig):
    market_model: Literal["economic"]
    data: BridgePolicy
    costs: Annotated[ExcludedCosts | Costs, Field(discriminator="kind")]
    bridge_model: BridgeModel

    @model_validator(mode="after")
    def payer_costs(self):
        if not self.payers and any(f.profile.submitter != f.fund for f in self.funds):
            raise ValueError("a modeled bridge requires explicit external payer inventory")
        return self
