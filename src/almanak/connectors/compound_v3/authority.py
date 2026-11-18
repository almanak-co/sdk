"""Scoped Comet economic authority; ABI permission compilation remains separate."""

from typing import Literal

from pydantic import TypeAdapter, model_validator

from almanak.capabilities import CapabilityCodec, CapabilitySet
from almanak.connectors.compound_v3.domain import Target
from almanak.connectors.compound_v3.models import Config, Parameters
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
    kind: Literal["compound_v3_reserve"] = "compound_v3_reserve"
    target: Target
    borrow: bool

    @model_validator(mode="after")
    def scope(self):
        if not self.target.reserve.is_base and self.borrow:
            raise ValueError("Compound collateral cannot authorize base borrowing or repayment")
        return self


Capabilities = CapabilitySet[ReserveCapability]


def declared(config: Parameters) -> Capabilities:
    return Capabilities(
        requirements=tuple(
            ReserveCapability(
                target=config.target(a.name),
                borrow=a.borrow,
            )
            for a in config.allocations
        )
    )


def validate(config: Parameters, capabilities: Capabilities) -> None:
    if capabilities != declared(config):
        raise ValueError("Compound capabilities differ from the authoritative allocations")


def admit(capabilities: Capabilities, intent) -> None:
    matches = [c for c in capabilities.requirements if c.target == intent.target]
    if len(matches) != 1:
        raise ValueError("Compound intent requires exactly one retained reserve capability")
    capability = matches[0]
    if isinstance(intent, (BorrowIntent, RepayIntent, RepayAmountIntent)):
        # Repayment is scoped to the fund's own debt, not to an amount. Planning
        # refuses a partial repayment above the observed debt.
        if not capability.borrow:
            raise ValueError("Compound borrowing and repayment require a borrowing allocation")
        return
    # Supply and withdrawal stay within the allocation's own reserve; execution
    # separately verifies a withdrawal against owned principal.
    if not isinstance(intent, (SupplyIntent, WithdrawIntent, WithdrawAmountIntent)):
        raise ValueError("action is outside the Compound economic capability")


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate,
        admit=admit,
        worker_factory="almanak.connectors.compound_v3.authority:capability_codec",
        declare=declared,
    )
