"""Bridge USDC from Arbitrum to Base once, then lend exactly what arrived on Base."""

from typing import cast

from almanak.api import (
    PositiveDecimal,
    StrategyState,
    TypedParameters,
    declared,
    parameter_base_units,
)
from almanak.connectors.across.api import (
    ArrivalResult,
    BridgeIntent,
    RefundResult,
    RevokeBridgeIntent,
)
from almanak.connectors.compound_v3.observations import owned

BRIDGE = "move"
EARN = "earn"


class StrategyParams(TypedParameters):
    # Sent once, in the bridged asset's units.
    bridge_quantity: PositiveDecimal


class State(StrategyState):
    # Base units that left the source account, once the deposit is mined.
    sent: int | None = None
    # Base units measured on the destination account when the transfer arrived.
    received: int | None = None
    # Base units the venue paid back to the source account when nobody filled it.
    returned: int | None = None
    # Base units supplied on the destination chain, once that supply is mined.
    supplied: int | None = None
    # A step failed; nothing is sent or supplied again.
    failed: bool = False
    # The revoke of a standing approval failed. A running strategy does not ask again;
    # teardown does.
    revoke_failed: bool = False


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def bridge_amount(config) -> int:
    """`bridge_quantity` in base units of the sent asset."""
    asset = config.allocation(BRIDGE).route.asset
    decimals = config.asset_metadata[asset].decimals
    return parameter_base_units(config.parameters, "bridge_quantity", decimals)


def validate_config(config) -> None:
    if {a.name for a in config.positions} != {BRIDGE, EARN}:
        raise ValueError(f"this reference requires the allocations {BRIDGE} and {EARN}")
    bridge, earn = config.allocation(BRIDGE), config.allocation(EARN)
    if bridge_amount(config) > bridge.capital:
        raise ValueError("bridge_quantity exceeds the bridge allocation's capital")
    if earn.account != bridge.destination_account:
        raise ValueError("the lending allocation belongs to the account the bridge sends to")
    if earn.reserve.asset != bridge.route.destination_asset or not earn.reserve.is_base:
        raise ValueError("the lending allocation supplies the asset the bridge delivers")
    if earn.borrow:
        raise ValueError("this reference only supplies and withdraws")


def decide(context):
    memory = state(context)
    if context.observation(BRIDGE) is not None:
        return source_tick(context, memory)
    if context.observation(EARN) is not None:
        return destination_tick(context, memory)
    return context.decision(reason=progress(memory))


def source_tick(context, memory: State):
    """The chain the bridge sends from: deposit once, and clear a standing approval."""
    observed = context.require_observation(BRIDGE)
    if context.teardown or memory.failed:
        # A bridge clears its own approval. One whose deposit never was mined is left:
        # a failed bridge asks to clear it once, and teardown asks until it is cleared.
        if observed.allowance and (context.teardown or not memory.revoke_failed):
            return context.decision(intent=context.actions.revoke(BRIDGE))
        return context.decision(reason=progress(memory))
    if memory.sent is None:
        return context.decision(
            intent=context.actions.bridge(BRIDGE, bridge_amount(context.config))
        )
    return context.decision(reason=progress(memory))


def destination_tick(context, memory: State):
    """The chain the bridge delivers to: lend what arrived, and withdraw it at teardown.

    Nothing is supplied from the bridge's offer. The amount is the one the arrival
    measured, which is owned cash of this account only from then on.
    """
    observed = context.require_observation(EARN)
    if context.teardown or memory.failed:
        if not owned(observed).empty or observed.allowance:
            return context.decision(intent=context.actions.withdraw(EARN))
        return context.decision(reason=progress(memory))
    if memory.received is not None and memory.supplied is None:
        return context.decision(intent=context.actions.supply(EARN, memory.received))
    return context.decision(reason=progress(memory))


def progress(memory: State) -> str:
    if memory.supplied is not None:
        return "Arrived and supplied on the destination account; holding"
    if memory.received is not None:
        return "Arrived on the destination account"
    if memory.returned is not None:
        return "Refunded to the source account; nothing left to do"
    if memory.failed:
        return "A step failed; nothing is sent or supplied again"
    if memory.sent is not None:
        return "In flight; waiting for the arrival or the refund"
    return "Waiting for the source account's bridge data"


def on_outcome(context):
    memory, result = state(context), context.result
    if isinstance(result, ArrivalResult):
        return memory.updated(received=result.received.amount)
    if isinstance(result, RefundResult):
        return memory.updated(returned=result.returned.amount)
    intent = result.intent
    if isinstance(intent, (BridgeIntent, RevokeBridgeIntent)):
        return on_bridge_action(memory, result)
    if result.status == "failed":
        return memory.updated(failed=True)
    if result.status == "completed" and intent.kind == "supply":
        return memory.updated(supplied=result.assets.amount)
    return memory


def on_bridge_action(memory: State, result):
    revoke = isinstance(result.intent, RevokeBridgeIntent)
    if result.status == "failed":
        return memory.updated(failed=True, revoke_failed=revoke or memory.revoke_failed)
    if revoke:
        return memory.updated(revoke_failed=False)
    if result.transfer is not None:
        return memory.updated(sent=result.sent.amount)
    return memory
