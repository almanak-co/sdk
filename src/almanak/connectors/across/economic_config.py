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
# A modeled transfer's fill deadline is its deposit's second plus this window, as the
# fork's constructed quote states it. It is a deadline, never a forecast of the arrival.
FILL_WINDOW_SECONDS = 3600


class BridgePolicy(Model):
    # Seconds from the modeled deposit to the modeled arrival. Stated, never defaulted.
    arrival_latency_seconds: PositiveSeconds

    @model_validator(mode="after")
    def within_fill_window(self):
        # The model has no refund, so an arrival at or past the deadline has no meaning.
        if self.arrival_latency_seconds >= FILL_WINDOW_SECONDS:
            raise ValueError(
                f"arrival_latency_seconds must be below the modeled fill window of "
                f"{FILL_WINDOW_SECONDS} seconds: the model fills every transfer and has no refund"
            )
        return self


def stated_policy(values):
    """An absent data block is an empty one, so the refusal names the missing latency."""
    if isinstance(values, dict) and "data" not in values:
        return {**values, "data": {}}
    return values


class Costs(Model):
    kind: Literal["fixed_native"]
    # Native cost of one bridge on its source chain: its approval and its deposit.
    bridge: UInt


class EconomicConfig(InventoryConfig):
    market_model: Literal["economic"]
    data: BridgePolicy
    costs: Annotated[ExcludedCosts | Costs, Field(discriminator="kind")]
    bridge_model: BridgeModel

    @model_validator(mode="before")
    @classmethod
    def stated(cls, values):
        return stated_policy(values)

    @model_validator(mode="after")
    def payer_costs(self):
        if not self.payers and any(f.profile.submitter != f.fund for f in self.funds):
            raise ValueError("a modeled bridge requires explicit external payer inventory")
        return self
