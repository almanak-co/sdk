"""Candidate TwoCrypto transitions retain fees, profit gates and price updates."""

from dataclasses import dataclass, replace

from almanak.connectors.curve.crypto_integer import unsigned
from almanak.connectors.curve.twocrypto_liquidity import (
    AdminState,
    LiquidityState,
    addition,
    admin_transition,
    proportional,
    single_coin,
)
from almanak.connectors.curve.twocrypto_math import invariant, solve_balance
from almanak.connectors.curve.twocrypto_quotes import swap_details
from almanak.connectors.curve.twocrypto_rebalancing import PriceState, tweak


@dataclass(frozen=True)
class Transition:
    state: LiquidityState
    admin: AdminState
    prices: PriceState
    amounts: tuple[int, int]
    admin_amounts: tuple[int, int]


def withdraw_proportional(observation, shares, *, timestamp):
    if timestamp < observation.block.timestamp:
        raise ValueError("TwoCrypto transition cannot use future observation state")
    state = observation.math_state(timestamp=timestamp)
    amounts = proportional(state, shares)
    if shares == state.total_supply:
        raise ValueError("TwoCrypto candidate transition does not model an emptied global pool")
    changed = replace(
        state,
        total_supply=state.total_supply - shares,
        balances=tuple(
            value - amount for value, amount in zip(state.balances, amounts, strict=True)
        ),
        invariant=state.invariant - state.invariant * (shares - 1) // state.total_supply,
    )
    return Transition(
        changed, observation.accounting.math_state(), price_state(observation), amounts, (0, 0)
    )


def initial_state(observation, timestamp):
    if timestamp < observation.block.timestamp:
        raise ValueError("TwoCrypto transition cannot use future observation state")
    state = observation.math_state(timestamp=timestamp)
    if observation.ramp.future_time > timestamp:
        state = replace(
            state,
            invariant=invariant(state.amplification, state.gamma, state.normalized(state.balances)),
        )
    return state


def price_state(observation):
    return PriceState(
        observation.cached_price_oracle,
        observation.last_prices,
        observation.last_timestamp,
        observation.packed_rebalancing,
    )


def exchange(observation, input_index, output_index, amount, *, timestamp):
    state = initial_state(observation, timestamp)
    output, ratio, balances = swap_details(state, input_index, output_index, amount)
    changed = replace(state, balances=balances)
    changed, admin, prices = tweak(
        changed,
        observation.accounting.math_state(),
        price_state(observation),
        changed.normalized(changed.balances),
        timestamp=timestamp,
        ramp_end=observation.ramp.future_time,
        previous_ratio=ratio,
    )
    amounts = (-amount, output) if input_index == 0 else (output, -amount)
    return Transition(changed, admin, prices, amounts, (0, 0))


def add_liquidity(observation, amounts, *, timestamp):
    state = initial_state(observation, timestamp)
    minted, fee, new_invariant = addition(state, amounts)
    changed = replace(
        state,
        total_supply=unsigned(state.total_supply + minted),
        balances=tuple(
            unsigned(value + amount) for value, amount in zip(state.balances, amounts, strict=True)
        ),
    )
    admin = observation.accounting.math_state()
    admin = replace(
        admin, virtual_shares=unsigned(admin.virtual_shares + fee * 5 * 10**9 // 10**10)
    )
    changed, admin, prices = tweak(
        changed,
        admin,
        price_state(observation),
        changed.normalized(changed.balances),
        timestamp=timestamp,
        ramp_end=observation.ramp.future_time,
        new_invariant=new_invariant,
    )
    return Transition(changed, admin, prices, (-amounts[0], -amounts[1]), (0, 0))


def withdraw_one(observation, index, shares, *, timestamp):
    if timestamp < observation.block.timestamp:
        raise ValueError("TwoCrypto transition cannot use future observation state")
    state = observation.math_state(timestamp=timestamp)
    state, admin, admin_amounts = admin_transition(
        state,
        observation.accounting.math_state(),
        timestamp=timestamp,
        ramp_end=observation.ramp.future_time,
    )
    normalized = list(state.normalized(state.balances))
    if observation.ramp.future_time > timestamp:
        state = replace(state, invariant=invariant(state.amplification, state.gamma, normalized))
    output, _, reduced = single_coin(state, index, shares)
    normalized[index] = solve_balance(state.amplification, state.gamma, normalized, reduced, index)[
        0
    ]
    amounts = (output, 0) if index == 0 else (0, output)
    changed = replace(
        state,
        total_supply=state.total_supply - shares,
        balances=tuple(
            value - amount for value, amount in zip(state.balances, amounts, strict=True)
        ),
    )
    prices = PriceState(
        observation.cached_price_oracle,
        observation.last_prices,
        observation.last_timestamp,
        observation.packed_rebalancing,
    )
    changed, admin, prices = tweak(
        changed,
        admin,
        prices,
        normalized,
        timestamp=timestamp,
        ramp_end=observation.ramp.future_time,
        new_invariant=reduced,
    )
    return Transition(changed, admin, prices, amounts, admin_amounts)
