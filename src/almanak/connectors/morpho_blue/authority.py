"""Morpho economic authority scopes each allocation to one exact market and asset."""

from typing import Literal

from pydantic import TypeAdapter, model_validator

from almanak.capabilities import CapabilityCodec, CapabilitySet
from almanak.connectors.morpho_blue.domain import Target
from almanak.connectors.morpho_blue.models import Config, Parameters
from almanak.lending_intents import (
    BorrowIntent,
    RepayAmountIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawAmountIntent,
    WithdrawIntent,
)
from almanak.models import Model


class ReserveCapability(Model):
    kind: Literal["morpho_blue_reserve"] = "morpho_blue_reserve"
    target: Target
    borrow: bool

    @model_validator(mode="after")
    def scope(self):
        if not self.target.reserve.is_loan and self.borrow:
            raise ValueError("Morpho collateral cannot authorize borrowing or repayment")
        return self


Capabilities = CapabilitySet[ReserveCapability]


def declared(config: Parameters) -> Capabilities:
    return Capabilities(
        requirements=tuple(
            ReserveCapability(
                target=config.target(allocation.name),
                borrow=allocation.borrow,
            )
            for allocation in config.allocations
        )
    )


def validate(config: Parameters, capabilities: Capabilities) -> None:
    if capabilities != declared(config):
        raise ValueError("Morpho capabilities differ from the authoritative allocations")


def admit(capabilities: Capabilities, intent) -> None:
    matches = [
        capability for capability in capabilities.requirements if capability.target == intent.target
    ]
    if len(matches) != 1:
        raise ValueError("Morpho intent requires exactly one retained target capability")
    capability = matches[0]
    if isinstance(intent, (BorrowIntent, RepayIntent, RepayAmountIntent)):
        # Repayment is scoped to the fund's own debt, not to an amount. Planning
        # refuses a partial repayment above the observed debt.
        if not capability.borrow:
            raise ValueError("Morpho borrowing and repayment require a borrowing allocation")
        return
    if not isinstance(intent, (SupplyIntent, WithdrawIntent, WithdrawAmountIntent)):
        raise ValueError("action is outside Morpho economic authority")


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate,
        admit=admit,
        worker_factory="almanak.connectors.morpho_blue.authority:capability_codec",
        declare=declared,
    )
