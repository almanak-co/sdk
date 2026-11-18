"""Scoped reserve capabilities for trusted admission and Roles compilation."""

from typing import Literal

from pydantic import TypeAdapter

from almanak.capabilities import CapabilityCodec, CapabilitySet
from almanak.connectors.pool_lending_authority import admit as admit
from almanak.connectors.sparklend.domain import Target
from almanak.connectors.sparklend.models import Config, Parameters
from almanak.models import Model


class ReserveCapability(Model):
    kind: Literal["sparklend_reserve"] = "sparklend_reserve"
    target: Target
    collateral: bool
    borrow: bool


Capabilities = CapabilitySet[ReserveCapability]


def declared(config: Parameters) -> Capabilities:
    return Capabilities(
        requirements=tuple(
            ReserveCapability(
                target=config.target(a.name),
                collateral=a.collateral,
                borrow=a.borrow,
            )
            for a in config.allocations
        )
    )


def validate(config: Parameters, capabilities: Capabilities) -> None:
    if capabilities != declared(config):
        raise ValueError("SparkLend capabilities differ from the authoritative allocations")


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate,
        admit=admit,
        worker_factory="almanak.connectors.sparklend.authority:capability_codec",
        declare=declared,
    )
