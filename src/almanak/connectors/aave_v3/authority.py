"""Scoped reserve capabilities for trusted admission and Roles compilation."""

from typing import Literal

from pydantic import TypeAdapter

from almanak.capabilities import CapabilityCodec, CapabilitySet
from almanak.connectors.aave_v3.domain import Target
from almanak.connectors.aave_v3.models import Config, Parameters
from almanak.connectors.pool_lending_authority import admit as admit
from almanak.models import Model


class ReserveCapability(Model):
    kind: Literal["aave_v3_reserve"] = "aave_v3_reserve"
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
        raise ValueError("Aave V3 capabilities differ from the authoritative allocations")


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate,
        admit=admit,
        worker_factory="almanak.connectors.aave_v3.authority:capability_codec",
        declare=declared,
    )
