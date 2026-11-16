"""Supply named WETH positions once, with optional timed withdrawal."""

from decimal import Decimal
from typing import cast

from pydantic import Field

from almanak.api import (
    PositiveDecimal,
    StrategyState,
    TypedParameters,
    declared,
    parameter_base_units,
)
from almanak.caip import AssetType
from almanak.connectors.aave_v4.api import (
    Decision,
    DecisionInput,
    LendingParameters,
    Memory,
    OutcomeInput,
    obligations,
)
from almanak.lending_intents import BorrowIntent, RepayIntent, SupplyIntent


class StrategyParams(TypedParameters):
    # Borrowed per action by each debt, in its asset's units.
    borrow_quantity: PositiveDecimal
    # The debt unwinds below minimum_health_factor plus this margin. The floor refuses
    # any action that would leave the health factor under it, the unwind's own
    # withdrawals included, so a trigger at the floor fires only once the unwind can be
    # refused.
    unwind_margin: PositiveDecimal


class State(StrategyState):
    supplied_at: dict[str, int] = Field(default_factory=dict)
    borrowed_at: dict[str, int] = Field(default_factory=dict)
    repaid_at: dict[str, int] = Field(default_factory=dict)


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def borrow_amount(config, name: str) -> int:
    """`borrow_quantity` in base units of the named allocation's reserve asset."""
    asset = config.allocation(name).reserve.asset
    decimals = config.asset_metadata[asset].decimals
    return parameter_base_units(config.parameters, "borrow_quantity", decimals)


def validate_config(config: LendingParameters) -> None:
    for allocation in config.allocations:
        chain = allocation.reserve.chain.chain_id
        weth = AssetType(f"{chain}/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
        if allocation.reserve.asset != weth:
            raise ValueError(f"{allocation.name}: this strategy requires the WETH reserve")
        if allocation.capital <= 0:
            raise ValueError(
                f"{allocation.name}: this strategy supplies its capital, set it above 0"
            )
    for debt in config.debts:
        usdc = AssetType(
            f"{debt.reserve.chain.chain_id}/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        )
        if debt.reserve.asset != usdc:
            raise ValueError(f"{debt.name}: this strategy borrows only configured USDC")
        borrow_amount(config, debt.name)  # refuses a borrow_quantity finer than USDC


def decide(context: DecisionInput) -> Decision:
    observations = {
        item.allocation: context.require_observation(item.allocation)
        for item in context.observations
    }
    owned = {item.target.allocation: item.shares for item in context.owned}
    supplied_at = state(context).supplied_at
    independent = independent_teardown(context, observations, owned)
    if independent is not None:
        return context.decision(intent=independent)
    for debt in context.config.debts:
        if debt.name not in observations:
            continue
        observed = observations[debt.name]
        if debt_exit_due(context, debt, observed, observations, supplied_at) and debt_obligation(
            observed
        ):
            return context.decision(
                intent=context.actions.repay(debt.name),
                reason=repay_reason(context, debt, observed),
            )
    for allocation in context.config.allocations:
        if allocation.name not in observations:
            continue
        observed = observations[allocation.name]
        intent = decide_allocation(context, allocation, observed, owned, supplied_at)
        if intent is not None:
            return context.decision(intent=intent)
    for debt in context.config.debts:
        if debt.name in observations and borrowing_due(context, debt, observations, supplied_at):
            return context.decision(
                intent=context.actions.borrow(debt.name, borrow_amount(context.config, debt.name))
            )
    return context.decision(reason=hold_reason(context, observations, owned))


def independent_teardown(context, observations, owned):
    if not context.teardown:
        return None
    collateral = {name for debt in context.config.debts for name in debt.collateral}
    for allocation in context.config.allocations:
        if allocation.name not in collateral and allocation.name in observations:
            intent = close_allocation(
                context, allocation, observations[allocation.name], owned[allocation.name]
            )
            if intent is not None:
                return intent
    return None


def debt_exit_due(context, debt, observed, observations, supplied_at):
    if context.teardown or (
        observed.health_factor is not None
        and observed.health_factor < unwind_trigger(context.config, debt)
    ):
        return True
    return any(
        withdrawal_due(context.config.allocation(name), observations[name], supplied_at.get(name))
        for name in debt.collateral
    )


def unwind_trigger(config, debt) -> int:
    """`minimum_health_factor` plus `unwind_margin`, both in 18-decimal fixed point."""
    return debt.minimum_health_factor + int(config.parameters.unwind_margin.scaleb(18))


def health_text(context, debt, observed):
    """The debt's health factor against its unwind level, or None without debt."""
    if observed.health_factor is None or not observed.position.total_debt:
        return None
    health = Decimal(observed.health_factor).scaleb(-18)
    trigger = Decimal(unwind_trigger(context.config, debt)).scaleb(-18)
    side = "below" if observed.health_factor < unwind_trigger(context.config, debt) else "above"
    return f"Health {health:.2f}, {side} the {trigger:.2f} unwind level"


def hold_reason(context, observations, owned):
    if state(context).supplied_at and not any(owned.values()):
        return "Withdrawn; nothing left to do"
    for debt in context.config.debts:
        if debt.name in observations:
            health = health_text(context, debt, observations[debt.name])
            if health is not None:
                return f"{health}; holding"
    return "Supplied; holding until the hold time ends or the strategy stops"


def repay_reason(context, debt, observed):
    """Only a health-triggered repay says why; teardown and hold-time repays are routine."""
    if context.teardown:
        return None
    health = health_text(context, debt, observed)
    if health is None or observed.health_factor >= unwind_trigger(context.config, debt):
        return None
    return f"{health}; repaying"


def debt_obligation(observed):
    return obligations(observed) == "remaining"


def borrowing_due(context, debt, observations, supplied_at):
    borrowed = state(context).borrowed_at
    if debt.name in borrowed or debt_exit_due(
        context, debt, observations[debt.name], observations, supplied_at
    ):
        return False
    return all(observations[name].position.supplied_shares for name in debt.collateral)


def decide_allocation(context, allocation, observed, owned, supplied_at):
    name = allocation.name
    started = supplied_at.get(name)
    if (
        context.teardown
        or withdrawal_due(allocation, observed, started)
        or collateral_repaid(context, name)
    ):
        return close_allocation(context, allocation, observed, owned[name])
    if started is None:
        return context.actions.supply(name, allocation.capital)
    return None


def collateral_repaid(context, allocation):
    repaid = state(context).repaid_at
    return any(
        debt.name in repaid and allocation in debt.collateral for debt in context.config.debts
    )


def withdrawal_due(allocation, observed, started):
    return (
        started is not None
        and allocation.hold_seconds is not None
        and observed.effective_at.timestamp() >= started + allocation.hold_seconds
    )


def close_allocation(context, allocation, observed, shares):
    if shares or observed.allowance:
        return context.actions.withdraw(allocation.name)
    return None


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    if not (result.status == "completed"):
        return context.memory
    if isinstance(result.intent, SupplyIntent):
        field = "supplied_at"
    elif isinstance(result.intent, BorrowIntent):
        field = "borrowed_at"
    elif isinstance(result.intent, RepayIntent):
        field = "repaid_at"
    else:
        return context.memory
    timestamps = {
        **getattr(state(context), field),
        result.intent.target.allocation: int(result.effective_at.timestamp()),
    }
    return state(context).updated(**{field: timestamps})
