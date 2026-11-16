"""Economic admission uses simulation time and modeled quote provenance explicitly."""

from almanak.connectors.curve import economic_pool
from almanak.connectors.curve.economic_simulation import minimums, virtual_pool
from almanak.connectors.curve.lifecycle import increases_exposure
from almanak.connectors.curve.ownership import validate_owned
from almanak.dex_intents import (
    ClosePoolLiquidity,
    DecreasePoolLiquidity,
    ExitExchange,
    IncreasePoolLiquidity,
    OpenPoolLiquidity,
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
    elif isinstance(intent, (OpenPoolLiquidity, IncreasePoolLiquidity)):
        addition_limits(intent, decision, current, allocation)
    elif isinstance(intent, (ClosePoolLiquidity, DecreasePoolLiquidity)):
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
        for q in (decision.entry_quote, *decision.exit_quotes)
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
    shares = (
        intent.liquidity if isinstance(intent, DecreasePoolLiquidity) else position.shares.amount
    )
    expected = minimums(decision, shares, allocation.slippage_bps, intent.output_asset)
    if any(
        minimum.amount < value.amount
        for minimum, value in zip(intent.minimums.root, expected.root, strict=True)
    ):
        raise ValueError("economic Curve removal minima widen configured slippage")


def addition_limits(intent, decision, current, allocation):
    amounts = tuple(value.amount for value in intent.maximums.root)
    before = virtual_pool(decision.pool, decision.owned, int(decision.effective_at.timestamp()))
    after = virtual_pool(current.pool, current.owned, int(current.effective_at.timestamp()))
    floor = max(
        1, economic_pool.minted(before, amounts) * (10000 - allocation.slippage_bps) // 10000
    )
    if economic_pool.minted(after, amounts) < floor:
        raise ValueError("economic Curve pool movement violates its retained share minimum")
