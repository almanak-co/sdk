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


def broken_rules(config):
    """Each rule of this reference, with whether the configuration breaks it."""
    bridge, earn = config.allocation(BRIDGE), config.allocation(EARN)
    delivered = earn.reserve.is_base and earn.reserve.asset == bridge.route.destination_asset
    return (
        (
            bridge_amount(config) > bridge.capital,
            "bridge_quantity exceeds the bridge allocation's capital",
        ),
        (
            earn.account != bridge.destination_account,
            "the lending allocation belongs to the account the bridge sends to",
        ),
        (not delivered, "the lending allocation supplies the asset the bridge delivers"),
        (bool(earn.borrow), "this reference only supplies and withdraws"),
    )


def validate_config(config) -> None:
    if {a.name for a in config.positions} != {BRIDGE, EARN}:
        raise ValueError(f"this reference requires the allocations {BRIDGE} and {EARN}")
    for broken, message in broken_rules(config):
        if broken:
            raise ValueError(message)


def decide(context):
    """A fork or live run evaluates each chain on its own turn and shows one of the two
    allocations. A backtest shows both at once, so the destination's step follows when
    the source's turn asks for nothing."""
    memory = state(context)
    decision = None
    if context.observation(BRIDGE) is not None:
        decision = source_tick(context, memory)
    if (decision is None or decision.intent is None) and context.observation(EARN) is not None:
        decision = destination_tick(context, memory)
    return decision or context.decision(reason=progress(memory))


def source_tick(context, memory: State):
    """The chain the bridge sends from: deposit once, and clear a standing approval."""
    if context.teardown or memory.failed:
        return unwind_source(context, memory)
    if memory.sent is None:
        return context.decision(
            intent=context.actions.bridge(BRIDGE, bridge_amount(context.config))
        )
    return context.decision(reason=progress(memory))


def unwind_source(context, memory: State):
    """A bridge clears its own approval. One whose deposit never was mined is left:
    a failed bridge asks to clear it once, and teardown asks until it is cleared."""
    asking = context.teardown or not memory.revoke_failed
    if context.require_observation(BRIDGE).allowance and asking:
        return context.decision(intent=context.actions.revoke(BRIDGE))
    return context.decision(reason=progress(memory))


def destination_tick(context, memory: State):
    """The chain the bridge delivers to: lend what arrived, and withdraw it at teardown.

    Nothing is supplied from the bridge's offer. The amount is the one the arrival
    measured, which is owned cash of this account only from then on.
    """
    if context.teardown or memory.failed:
        return unwind_destination(context, memory)
    if memory.received is not None and memory.supplied is None:
        return context.decision(intent=context.actions.supply(EARN, memory.received))
    return context.decision(reason=progress(memory))


def unwind_destination(context, memory: State):
    """Withdraw what is lent, and through the same intent clear a standing approval."""
    observed = context.require_observation(EARN)
    if not owned(observed).empty or observed.allowance:
        return context.decision(intent=context.actions.withdraw(EARN))
    return context.decision(reason=progress(memory))


def progress(memory: State) -> str:
    """The furthest stage the strategy has recorded, most advanced first."""
    stages = (
        (memory.supplied is not None, "Arrived and supplied on the destination account; holding"),
        (memory.received is not None, "Arrived on the destination account"),
        (memory.returned is not None, "Refunded to the source account; nothing left to do"),
        (memory.failed, "A step failed; nothing is sent or supplied again"),
        (memory.sent is not None, "In flight; waiting for the arrival or the refund"),
    )
    reached = (text for done, text in stages if done)
    return next(reached, "Waiting for the source account's bridge data")


def on_outcome(context):
    memory, result = state(context), context.result
    if isinstance(result, ArrivalResult):
        return memory.updated(received=result.received.amount)
    if isinstance(result, RefundResult):
        return memory.updated(returned=result.returned.amount)
    if isinstance(result.intent, (BridgeIntent, RevokeBridgeIntent)):
        return on_bridge_action(memory, result)
    return on_lending_action(memory, result)


def on_lending_action(memory: State, result):
    if result.status == "failed":
        return memory.updated(failed=True)
    if result.status == "completed" and result.intent.kind == "supply":
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
