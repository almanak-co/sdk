"""Candidate TriCrypto transitions retain fees, profit gates and price updates."""

from dataclasses import dataclass, replace

from almanak.connectors.curve.crypto_integer import unsigned
from almanak.connectors.curve.tricrypto_liquidity import (
    AdminState,
    LiquidityState,
    addition,
    admin_transition,
    proportional,
    single_coin,
)
from almanak.connectors.curve.tricrypto_math import invariant, solve_balance
from almanak.connectors.curve.tricrypto_quotes import swap_details
from almanak.connectors.curve.tricrypto_rebalancing import PriceState, tweak


@dataclass(frozen=True)
class Transition:
    state: LiquidityState
    admin: AdminState
    prices: PriceState
    amounts: tuple[int, int, int]
    admin_shares: int


def withdraw_proportional(observation, shares, *, timestamp):
    if observation.token_accounting is not None:
        from almanak.connectors.curve import tricrypto_token_transitions

        return tricrypto_token_transitions.withdraw_proportional(
            observation, shares, timestamp=timestamp
        )
    if timestamp < observation.block.timestamp:
        raise ValueError("TriCrypto transition cannot use future observation state")
    state = observation.math_state(timestamp=timestamp)
    amounts = proportional(state, shares)
    if shares == state.total_supply:
        raise ValueError("TriCrypto candidate transition does not model an emptied global pool")
    changed = replace(
        state,
        total_supply=state.total_supply - shares,
        balances=tuple(
            value - amount for value, amount in zip(state.balances, amounts, strict=True)
        ),
        invariant=state.invariant - state.invariant * (shares - 1) // state.total_supply,
    )
    return Transition(
        changed, observation.accounting.math_state(), price_state(observation), amounts, 0
    )


def initial_state(observation, timestamp):
    if timestamp < observation.block.timestamp:
        raise ValueError("TriCrypto transition cannot use future observation state")
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
    if observation.token_accounting is not None:
        from almanak.connectors.curve import tricrypto_token_transitions

        return tricrypto_token_transitions.exchange(
            observation, input_index, output_index, amount, timestamp=timestamp
        )
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
    amounts = tuple(
        -amount if index == input_index else output if index == output_index else 0
        for index in range(3)
    )
    return Transition(changed, admin, prices, (amounts[0], amounts[1], amounts[2]), 0)


def add_liquidity(observation, amounts, *, timestamp):
    if observation.token_accounting is not None:
        from almanak.connectors.curve import tricrypto_token_transitions

        return tricrypto_token_transitions.add_liquidity(observation, amounts, timestamp=timestamp)
    state = initial_state(observation, timestamp)
    minted, _, new_invariant = addition(state, amounts)
    changed = replace(
        state,
        total_supply=unsigned(state.total_supply + minted),
        balances=tuple(
            unsigned(value + amount) for value, amount in zip(state.balances, amounts, strict=True)
        ),
    )
    admin = observation.accounting.math_state()
    changed, admin, prices = tweak(
        changed,
        admin,
        price_state(observation),
        changed.normalized(changed.balances),
        timestamp=timestamp,
        ramp_end=observation.ramp.future_time,
        new_invariant=new_invariant,
    )
    actual = tuple(
        unsigned(balance + amount)
        for balance, amount in zip(observation.actual_balances, amounts, strict=True)
    )
    changed, admin, claimed = admin_transition(changed, admin, actual)
    return Transition(changed, admin, prices, tuple(-value for value in amounts), claimed)


def withdraw_one(observation, index, shares, *, timestamp):
    if observation.token_accounting is not None:
        from almanak.connectors.curve import tricrypto_token_transitions

        return tricrypto_token_transitions.withdraw_one(
            observation, index, shares, timestamp=timestamp
        )
    state, admin, claimed, normalized = single_withdrawal_state(observation, timestamp)
    output, _, reduced = single_coin(state, index, shares)
    normalized[index] = solve_balance(state.amplification, state.gamma, normalized, reduced, index)[
        0
    ]
    amounts = tuple(output if offset == index else 0 for offset in range(3))
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
    return Transition(changed, admin, prices, (amounts[0], amounts[1], amounts[2]), claimed)


def single_withdrawal_state(observation, timestamp):
    if timestamp < observation.block.timestamp:
        raise ValueError("TriCrypto transition cannot use future observation state")
    state = observation.math_state(timestamp=timestamp)
    state, admin, claimed = admin_transition(
        state,
        observation.accounting.math_state(),
        observation.actual_balances,
    )
    normalized = list(state.normalized(state.balances))
    if observation.ramp.future_time > timestamp:
        state = replace(state, invariant=invariant(state.amplification, state.gamma, normalized))
    return state, admin, claimed, normalized
