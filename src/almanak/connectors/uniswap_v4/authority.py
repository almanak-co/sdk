"""Capabilities are evaluated in the strategy sandbox and independently admitted."""

from pydantic import TypeAdapter

from almanak.capabilities import CapabilityCodec
from almanak.connectors.uniswap_v4.models import (
    Capabilities,
    Capability,
    Config,
    Parameters,
)
from almanak.connectors.v3_pool_authority import admit_selected_range
from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    ExitExchange,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)


def requirement(config, name):
    return Capability(target=config.target(name))


def declared(config):
    return Capabilities(requirements=tuple(requirement(config, a.name) for a in config.allocations))


def validate_capabilities(config, capabilities):
    if capabilities != declared(config):
        raise ValueError("Uniswap capabilities differ from the validated economic parameters")


def admit_intent(capabilities, intent):
    matches = [c for c in capabilities.requirements if c.target == intent.target]
    if len(matches) != 1:
        raise ValueError("intent has no retained Uniswap capability")
    capability = matches[0]
    if isinstance(intent, OpenLiquidity):
        require_lp(capability)
        admit_selected_range(intent)
    elif isinstance(intent, (CollectLiquidityFees, CloseLiquidity)):
        require_lp(capability)
    elif not isinstance(intent, (SwapExactInput, RevokeExchange, ExitExchange)):
        raise ValueError("unsupported Uniswap economic action")


def require_lp(capability):
    if not capability.target.liquidity_enabled:
        raise ValueError("liquidity action has no declared range or band capability")


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate_capabilities,
        admit=admit_intent,
        worker_factory="almanak.connectors.uniswap_v4.authority:capability_codec",
        declare=declared,
    )
