"""Scope-only authority binds both invariant families to exact configured pools.

Intents are not compared with any author amount (blueprint 27); inventory, quotes,
slippage and deadlines are gateway checks against observations.
"""

from pydantic import TypeAdapter

from almanak.capabilities import CapabilityCodec
from almanak.connectors.aerodrome.models import Capabilities, Capability, Config, Parameters
from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    DecreaseLiquidity,
    ExitExchange,
    IncreaseLiquidity,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)


def admit(capabilities, intent):
    if capabilities is None:
        raise ValueError("Aerodrome execution requires retained capabilities")
    matches = [value for value in capabilities.requirements if value.target == intent.target]
    if len(matches) != 1:
        raise ValueError("Aerodrome intent requires one exact retained target capability")
    if isinstance(intent, (SwapExactInput, RevokeExchange, ExitExchange)):
        return
    if not intent.target.liquidity_enabled:
        raise ValueError("liquidity action requires explicit pool liquidity authority")
    if not isinstance(
        intent,
        (
            OpenLiquidity,
            IncreaseLiquidity,
            DecreaseLiquidity,
            CloseLiquidity,
            CollectLiquidityFees,
        ),
    ):
        raise ValueError("unsupported Aerodrome economic action")


def declared(config):
    return Capabilities(
        requirements=tuple(Capability(target=config.target(a.name)) for a in config.allocations)
    )


def validate_capabilities(config, capabilities):
    if capabilities != declared(config):
        raise ValueError("Aerodrome capabilities differ from the validated economic configuration")


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate_capabilities,
        admit=admit,
        worker_factory="almanak.connectors.aerodrome.authority:capability_codec",
        declare=declared,
    )
