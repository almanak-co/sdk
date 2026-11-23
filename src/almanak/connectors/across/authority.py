"""Scoped bridge authority: one route between two named funds per allocation."""

from typing import Literal

from pydantic import TypeAdapter

from almanak.bridge_intents import BridgeIntent
from almanak.capabilities import CapabilityCodec, CapabilitySet
from almanak.connectors.across.domain import Target
from almanak.connectors.across.models import Config, Parameters
from almanak.models import Model


class RouteCapability(Model):
    kind: Literal["across_route"] = "across_route"
    target: Target


Capabilities = CapabilitySet[RouteCapability]


def declared(config: Parameters) -> Capabilities:
    return Capabilities(
        requirements=tuple(
            RouteCapability(target=config.target(a.name)) for a in config.allocations
        )
    )


def validate(config: Parameters, capabilities: Capabilities) -> None:
    if capabilities != declared(config):
        raise ValueError("Across capabilities differ from the authoritative allocations")


def admit(capabilities: Capabilities, intent) -> None:
    matches = [c for c in capabilities.requirements if c.target == intent.target]
    if len(matches) != 1:
        raise ValueError("Across intent requires exactly one retained route capability")
    if not isinstance(intent, BridgeIntent):
        raise ValueError("action is outside the Across economic capability")


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate,
        admit=admit,
        worker_factory="almanak.connectors.across.authority:capability_codec",
        declare=declared,
    )
