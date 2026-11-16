"""Scoped HyperCore authority: each allocation's own market and actions.

On HyperCore the agent can trade every market of the account, so the gateway is the
only enforcement (section 5.3, D2). A backtest admits an intent only for a retained
allocation's exact target, as the gateway does before planning a fork run's steps.
"""

from typing import Literal

from pydantic import TypeAdapter

from almanak.capabilities import CapabilityCodec, CapabilitySet
from almanak.connectors.hyperliquid.models import Config, Parameters, Target
from almanak.models import Model


class PerpCapability(Model):
    kind: Literal["hypercore_perp"] = "hypercore_perp"
    target: Target
    leverage: int
    slippage_bps: int


Capabilities = CapabilitySet[PerpCapability]


def declared(config: Parameters) -> Capabilities:
    """One capability per allocation: its target, cross leverage and slippage."""
    return Capabilities(
        requirements=tuple(
            PerpCapability(
                target=config.target(a.name), leverage=a.leverage, slippage_bps=a.slippage_bps
            )
            for a in config.allocations
        )
    )


def validate(config: Parameters, capabilities: Capabilities) -> None:
    if capabilities != declared(config):
        raise ValueError("HyperCore capabilities differ from the authoritative allocations")


def admit(capabilities: Capabilities, intent) -> None:
    """Refuse an intent for another target, or leverage other than the configured."""
    matches = [c for c in capabilities.requirements if c.target == intent.target]
    if len(matches) != 1:
        raise ValueError("HyperCore intent requires exactly one retained perp capability")
    if intent.kind == "set_leverage" and intent.leverage != matches[0].leverage:
        raise ValueError("HyperCore leverage differs from the configured cross leverage")


def capability_codec():
    return CapabilityCodec(
        # A backtest's parameters, or a fork run's prepared configuration.
        parameters=TypeAdapter(Config | Parameters),
        capabilities=TypeAdapter(Capabilities),
        validate=validate,
        admit=admit,
        worker_factory="almanak.connectors.hyperliquid.authority:capability_codec",
        declare=declared,
    )
