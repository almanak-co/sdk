"""Bridge a stated amount of USDC from Arbitrum to Base once, and record how it ended."""

from typing import cast

from almanak.api import (
    PositiveDecimal,
    StrategyState,
    TypedParameters,
    declared,
    parameter_base_units,
)
from almanak.connectors.across.api import (
    ActionResult,
    ArrivalResult,
    BridgeIntent,
    Parameters,
    RefundResult,
)

ALLOCATION = "move"


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
    # A step of the bridge failed; nothing is sent again.
    failed: bool = False


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def bridge_amount(config) -> int:
    """`bridge_quantity` in base units of the sent asset."""
    asset = config.allocation(ALLOCATION).route.asset
    decimals = config.asset_metadata[asset].decimals
    return parameter_base_units(config.parameters, "bridge_quantity", decimals)


def validate_config(config: Parameters) -> None:
    if [a.name for a in config.positions] != [ALLOCATION]:
        raise ValueError(f"this reference bridges through one allocation named {ALLOCATION}")
    if bridge_amount(config) > config.allocation(ALLOCATION).capital:
        raise ValueError("bridge_quantity exceeds the allocation's capital")


def decide(context):
    memory = state(context)
    observed = context.observation(ALLOCATION)
    if observed is None:
        # The destination chain's turn: it observes no bridge allocation and sends nothing.
        return context.decision(reason=progress(memory))
    if context.teardown or memory.failed:
        # A bridge clears its own approval. One whose deposit never was mined is left.
        if observed.allowance:
            return context.decision(intent=context.actions.revoke(ALLOCATION))
        return context.decision(reason=progress(memory))
    if memory.sent is None:
        return context.decision(
            intent=context.actions.bridge(ALLOCATION, bridge_amount(context.config))
        )
    return context.decision(reason=progress(memory))


def progress(memory: State) -> str:
    if memory.received is not None:
        return "Arrived on the destination account; nothing left to do"
    if memory.returned is not None:
        return "Refunded to the source account; nothing left to do"
    if memory.failed:
        return "A bridge step failed; nothing is sent again"
    if memory.sent is not None:
        return "In flight; waiting for the arrival or the refund"
    return "Waiting for the source account's bridge data"


def on_outcome(context):
    memory, result = state(context), context.result
    if isinstance(result, ArrivalResult):
        return memory.updated(received=result.received.amount)
    if isinstance(result, RefundResult):
        return memory.updated(returned=result.returned.amount)
    return on_action(memory, result)


def on_action(memory: State, result: ActionResult):
    if result.status == "failed":
        return memory.updated(failed=True)
    if isinstance(result.intent, BridgeIntent) and result.transfer is not None:
        return memory.updated(sent=result.sent.amount)
    return memory
