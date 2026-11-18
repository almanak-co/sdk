"""Economic admission uses simulation time and modeled quote provenance explicitly."""

from almanak.connectors.aerodrome.economic_simulation import principal
from almanak.connectors.aerodrome.ownership import validate_owned
from almanak.connectors.aerodrome.validation import addition_limits
from almanak.connectors.v3_pool_lifecycle import increases_exposure
from almanak.dex_intents import (
    CloseLiquidity,
    DecreaseLiquidity,
    ExitExchange,
    IncreaseLiquidity,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)


def validate(config, state, pending, decision, current):
    intent = pending.intent
    allocation = config.allocation(intent.target.allocation)
    if not decision.simulated or not current.simulated:
        raise ValueError("economic admission requires hypothetical observations")
    if decision.target != intent.target or current.target != intent.target:
        raise ValueError("economic admission requires its configured target")
    if (
        int(decision.effective_at.timestamp()) != pending.decision_time
        or int(current.effective_at.timestamp()) != pending.eligible_at
    ):
        raise ValueError("economic admission requires exact decision and eligibility times")
    validate_owned(current.owned, current)
    if not isinstance(intent, (ExitExchange, RevokeExchange)) and not (
        pending.eligible_at < intent.deadline <= pending.decision_time + allocation.deadline_seconds
    ):
        raise ValueError("economic intent deadline expired or exceeds its decision policy")
    if isinstance(intent, SwapExactInput):
        swap_limits(intent, decision, allocation)
    elif isinstance(intent, (OpenLiquidity, IncreaseLiquidity)):
        addition_limits(intent, decision, current, allocation)
    elif isinstance(intent, (CloseLiquidity, DecreaseLiquidity)):
        removal_limits(intent, decision, allocation)
    if increases_exposure(intent):
        validate_entry(state, current, allocation)


def validate_entry(state, current, allocation):
    if state.mode != "running":
        raise ValueError("economic entry requires running authority")
    if (
        current.native_balance is None
        or current.native_balance.amount < allocation.minimum_native_balance
    ):
        raise ValueError("economic payer is below the configured native minimum")


def swap_limits(intent, decision, allocation):
    quotes = tuple(
        q
        for q in (decision.entry_quote, decision.exit_quote)
        if q is not None and q.amount_in == intent.amount
    )
    if not quotes or any(q.calculation != "prepared_integer_pool" for q in quotes):
        raise ValueError("economic swap requires its retained exact-size modeled quote")
    floor = max(
        max(1, q.amount_out.amount * (10000 - allocation.slippage_bps) // 10000) for q in quotes
    )
    if intent.minimum_output.amount < floor:
        raise ValueError("economic swap minimum widens configured slippage")


def removal_limits(intent, decision, allocation):
    position = decision.owned.position
    if position is None:
        raise ValueError("economic removal requires retained hypothetical shares or claims")
    shares = intent.liquidity if isinstance(intent, DecreaseLiquidity) else position.shares.amount
    expected = principal(decision.pool, shares)
    if any(
        minimum.amount < value * (10000 - allocation.slippage_bps) // 10000
        for minimum, value in zip((intent.minimum0, intent.minimum1), expected, strict=True)
    ):
        raise ValueError("economic removal minima widen configured slippage")
