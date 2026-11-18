"""Scope-only V3 economic authority, independent of protocol approval sequences."""

from pydantic import TypeAdapter

from almanak.capabilities import CapabilityCodec
from almanak.connectors.uniswap_v3.models import Capabilities, Capability, Config, Parameters
from almanak.connectors.v3_pool_authority import admit as admit


def declared(config):
    return Capabilities(
        requirements=tuple(Capability(target=config.target(a.name)) for a in config.allocations)
    )


def validate_capabilities(config, capabilities):
    if capabilities != declared(config):
        raise ValueError("V3 capabilities differ from the validated economic configuration")


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate_capabilities,
        admit=admit,
        worker_factory="almanak.connectors.uniswap_v3.authority:capability_codec",
        declare=declared,
    )
