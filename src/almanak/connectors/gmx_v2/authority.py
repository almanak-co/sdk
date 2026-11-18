"""Economic authority binds exact GMX targets before trusted protocol planning."""

from pydantic import TypeAdapter

from almanak.capabilities import CapabilityCodec
from almanak.connectors.gmx_v2.exchange_models import ExchangeCapability, SwapAllocation
from almanak.connectors.gmx_v2.intents import (
    ClosePosition,
    DecreasePosition,
    IncreasePosition,
    RevokePosition,
)
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation, LiquidityCapability
from almanak.connectors.gmx_v2.models import Capabilities, Capability, Config, Parameters


def declared(config):
    return Capabilities(
        requirements=tuple(declared_capability(config, a) for a in config.allocations)
    )


def declared_capability(config, a):
    if isinstance(a, LiquidityAllocation):
        return LiquidityCapability(
            target=config.target(a.name),
            minimum_market_tokens=a.minimum_market_tokens,
            minimum_outputs=a.withdrawal_minimums,
            maximum_execution_fee=a.execution_fee.amount,
        )
    if isinstance(a, SwapAllocation):
        return ExchangeCapability(
            target=config.target(a.name), maximum_execution_fee=a.execution_fee.amount
        )
    return Capability(target=config.target(a.name), maximum_execution_fee=a.execution_fee.amount)


def validate(config, capabilities):
    if capabilities != declared(config):
        raise ValueError(
            "GMX capabilities differ from the authoritative configured economic targets"
        )


def admit(capabilities, intent):
    values = [c for c in capabilities.requirements if c.target == intent.target]
    if len(values) != 1:
        raise ValueError("GMX intent requires exactly its retained target capability")
    capability = values[0]
    if isinstance(capability, LiquidityCapability):
        from almanak.connectors.gmx_v2.liquidity_planning import admit as admit_liquidity

        admit_liquidity(capability, intent)
        return
    if isinstance(capability, ExchangeCapability):
        from almanak.connectors.gmx_v2.exchange_planning import admit as admit_exchange

        admit_exchange(capability, intent)
        return
    # Scope-only authority: size and collateral are bounded by owned inventory, the
    # retained position and GMX's own leverage limits, never by an author budget.
    if not isinstance(intent, (IncreasePosition, DecreasePosition, ClosePosition, RevokePosition)):
        raise ValueError("unsupported GMX economic position intent")


def capability_codec():
    return CapabilityCodec(
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate,
        admit=admit,
        declare=declared,
        worker_factory="almanak.connectors.gmx_v2.authority:capability_codec",
    )
