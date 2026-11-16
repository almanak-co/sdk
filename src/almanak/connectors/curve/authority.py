"""Economic authority binds every Curve action to its retained target and liquidity scope."""

from pydantic import TypeAdapter

from almanak.capabilities import CapabilityCodec
from almanak.connectors.curve.models import Capabilities, Capability, Config, Parameters
from almanak.dex_intents import (
    ClosePoolLiquidity,
    DecreasePoolLiquidity,
    ExitExchange,
    IncreasePoolLiquidity,
    OpenPoolLiquidity,
    RevokeExchange,
    SwapExactInput,
)


def admit(capabilities, intent):
    if capabilities is None:
        raise ValueError("Curve execution requires retained capabilities")
    matches = [value for value in capabilities.requirements if value.target == intent.target]
    if len(matches) != 1:
        raise ValueError("Curve intent requires one exact retained target capability")
    if isinstance(intent, (SwapExactInput, RevokeExchange, ExitExchange)):
        return
    if not intent.target.liquidity_enabled:
        raise ValueError("Curve liquidity action requires explicit pool liquidity authority")
    admit_liquidity(intent)


def admit_liquidity(intent):
    if isinstance(intent, (DecreasePoolLiquidity, ClosePoolLiquidity)):
        if (
            intent.output_asset is not None
            and intent.output_asset != intent.target.single_coin_exit
        ):
            raise ValueError("Curve single-coin removal lacks configured output authority")
    elif not isinstance(intent, (OpenPoolLiquidity, IncreasePoolLiquidity)):
        raise ValueError("unsupported Curve economic action")


def declared(config):
    return Capabilities(
        requirements=tuple(
            Capability(target=config.target(allocation.name)) for allocation in config.allocations
        )
    )


def validate_capabilities(config, capabilities):
    if capabilities != declared(config):
        raise ValueError("Curve capabilities differ from the validated economic configuration")


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate_capabilities,
        admit=admit,
        worker_factory="almanak.connectors.curve.authority:capability_codec",
        declare=declared,
    )
