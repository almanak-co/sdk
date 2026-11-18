"""One sandboxed capability hook yields correlated requirements for all configured targets."""

from pydantic import TypeAdapter

from almanak.capabilities import CapabilityCodec, CapabilitySet
from almanak.configuration.composed import Parameters
from almanak.connectors.aave_v3.authority import ReserveCapability
from almanak.connectors.aave_v4.models import AaveCapability
from almanak.connectors.aerodrome.models import Capability as AerodromeCapability
from almanak.connectors.aerodrome_slipstream.models import Capability as SlipstreamCapability
from almanak.connectors.composed.models import Config, OwnedParameters
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.compound_v3.authority import ReserveCapability as CompoundCapability
from almanak.connectors.curve.models import Capability as CurveCapability
from almanak.connectors.gmx_v2.exchange_models import ExchangeCapability as GmxExchangeCapability
from almanak.connectors.gmx_v2.liquidity_models import (
    LiquidityCapability as GmxLiquidityCapability,
)
from almanak.connectors.gmx_v2.models import Capability as GmxCapability
from almanak.connectors.morpho_blue.authority import ReserveCapability as MorphoCapability
from almanak.connectors.pancakeswap_v3.models import Capability as PancakeCapability
from almanak.connectors.sparklend.authority import ReserveCapability as SparkCapability
from almanak.connectors.target_routing import connector_for_target, target_connector
from almanak.connectors.uniswap_v3.models import Capability as ExchangeCapability
from almanak.connectors.uniswap_v4.models import Capability as V4Capability

Capabilities = CapabilitySet[
    ReserveCapability
    | AaveCapability
    | SparkCapability
    | V4Capability
    | ExchangeCapability
    | PancakeCapability
    | AerodromeCapability
    | SlipstreamCapability
    | MorphoCapability
    | CompoundCapability
    | CurveCapability
    | GmxCapability
    | GmxExchangeCapability
    | GmxLiquidityCapability
]


def declared(config):
    requirements = []
    for identity, view in config.parameter_components.items():
        codec = connector_from_wire({"connector": str(identity)}).codec.capability
        requirements.extend(codec.declared(view).requirements)
    by_target = {r.target: r for r in requirements}
    return Capabilities(
        requirements=tuple(by_target[config.target(a.name)] for a in config.positions)
    )


def validate(config, capabilities):
    if capabilities != declared(config):
        raise ValueError("composed capabilities differ from the authoritative target allocations")


def selected(capabilities, identity):
    codec = connector_from_wire({"connector": str(identity)}).codec.capability
    return codec.capabilities.validate_python(
        {
            "requirements": tuple(
                c for c in capabilities.requirements if target_connector(c.target) == identity
            )
        }
    )


def admit(capabilities, intent):
    identity = target_connector(intent.target)
    connector_for_target(intent.target).codec.capability.admit(
        selected(capabilities, identity), intent
    )


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | OwnedParameters | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate,
        admit=admit,
        worker_factory="almanak.connectors.composed.authority:capability_codec",
        declare=declared,
    )
