"""January 2024 TriCrypto transfers admin fees as underlying tokens, never LP mints."""

from dataclasses import dataclass, replace

from almanak.connectors.curve.crypto_integer import PRECISION, unsigned
from almanak.connectors.curve.tricrypto_liquidity import addition, proportional, single_coin, xcp
from almanak.connectors.curve.tricrypto_math import geometric_mean, invariant, solve_balance
from almanak.connectors.curve.tricrypto_quotes import swap_details
from almanak.connectors.curve.tricrypto_rebalancing import tweak
from almanak.connectors.curve.tricrypto_token_accounting import TokenAccounting
from almanak.connectors.curve.tricrypto_transitions import Transition as LegacyTransition
from almanak.connectors.curve.tricrypto_transitions import initial_state, price_state


@dataclass(frozen=True)
class Transition(LegacyTransition):
    token_accounting: TokenAccounting
    admin_amounts: tuple[int, int, int] = (0, 0, 0)


def claim(state, admin, tokens, *, timestamp, ramp_end):
    if timestamp < tokens.last_claim_time:
        raise ValueError("TriCrypto fee claim cannot consume future accounting")
    if timestamp - tokens.last_claim_time < 86400 or ramp_end > timestamp:
        return state, admin, tokens, (0, 0, 0)
    if admin.profit <= admin.claimed_profit or state.total_supply < PRECISION:
        return state, admin, tokens, (0, 0, 0)
    fees = (admin.profit - admin.claimed_profit) * (5 * 10**9) // (2 * 10**10)
    shares, profit = tokens.virtual_admin_shares, admin.profit
    if admin.receiver_enabled and fees > 0:
        fraction = (
            unsigned(admin.virtual_price * PRECISION) // unsigned(admin.virtual_price - fees)
            - PRECISION
        )
        shares = unsigned(shares + state.total_supply * fraction // PRECISION)
        profit = unsigned(profit - 2 * fees)
    supply = unsigned(state.total_supply + shares)
    virtual_price = unsigned(PRECISION * xcp(state.invariant, state.price_scales)) // supply
    if virtual_price < PRECISION:
        return state, admin, tokens, (0, 0, 0)
    amounts = tuple(unsigned(value * shares) // supply for value in state.balances)
    changed = replace(
        state,
        balances=tuple(
            value - amount for value, amount in zip(state.balances, amounts, strict=True)
        ),
        invariant=state.invariant - state.invariant * shares // supply,
    )
    admin = replace(
        admin,
        virtual_price=virtual_price,
        profit=profit,
        claimed_profit=max(profit, admin.claimed_profit),
    )
    tokens = tokens.model_copy(update={"virtual_admin_shares": 0, "last_claim_time": timestamp})
    return changed, admin, tokens, amounts


def rebalanced(
    observation, state, admin, tokens, normalized, *, timestamp, new_invariant=0, previous_ratio=0
):
    # XCP is retained before price-scale adjustment; the EMA consumes the previous
    # XCP except for balanced removals, whose source deliberately uses the new one.
    unadjusted = new_invariant or invariant(
        state.amplification, state.gamma, normalized, previous_ratio
    )
    tokens = tokens.advance(timestamp, xcp(unadjusted, state.price_scales))
    state, admin, prices = tweak(
        state,
        admin,
        price_state(observation),
        normalized,
        timestamp=timestamp,
        ramp_end=observation.ramp.future_time,
        new_invariant=unadjusted,
    )
    return state, admin, prices, tokens


def exchange(observation, input_index, output_index, amount, *, timestamp):
    state = initial_state(observation, timestamp)
    output, ratio, balances = swap_details(state, input_index, output_index, amount)
    changed = replace(state, balances=balances)
    changed, admin, prices, tokens = rebalanced(
        observation,
        changed,
        observation.accounting.math_state(),
        observation.token_accounting,
        changed.normalized(changed.balances),
        timestamp=timestamp,
        previous_ratio=ratio,
    )
    amounts = tuple(
        -amount if index == input_index else output if index == output_index else 0
        for index in range(3)
    )
    return Transition(changed, admin, prices, (amounts[0], amounts[1], amounts[2]), 0, tokens)


def add_liquidity(observation, amounts, *, timestamp):
    state = initial_state(observation, timestamp)
    minted, fee, updated = addition(state, amounts)
    tokens = observation.token_accounting
    tokens = tokens.model_copy(
        update={"virtual_admin_shares": unsigned(tokens.virtual_admin_shares + fee // 2)}
    )
    changed = replace(
        state,
        total_supply=unsigned(state.total_supply + minted),
        balances=tuple(
            unsigned(value + amount) for value, amount in zip(state.balances, amounts, strict=True)
        ),
    )
    changed, admin, prices, tokens = rebalanced(
        observation,
        changed,
        observation.accounting.math_state(),
        tokens,
        changed.normalized(changed.balances),
        timestamp=timestamp,
        new_invariant=updated,
    )
    return Transition(changed, admin, prices, tuple(-value for value in amounts), 0, tokens)


def withdraw_one(observation, index, shares, *, timestamp):
    if timestamp < observation.block.timestamp:
        raise ValueError("TriCrypto withdrawal cannot consume future state")
    state, admin, tokens, claims = claim(
        observation.math_state(timestamp=timestamp),
        observation.accounting.math_state(),
        observation.token_accounting,
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
    amounts = tuple(output if offset == index else 0 for offset in range(3))
    changed = replace(
        state,
        total_supply=state.total_supply - shares,
        balances=tuple(
            value - amount for value, amount in zip(state.balances, amounts, strict=True)
        ),
    )
    changed, admin, prices, tokens = rebalanced(
        observation,
        changed,
        admin,
        tokens,
        normalized,
        timestamp=timestamp,
        new_invariant=reduced,
    )
    return Transition(
        changed, admin, prices, (amounts[0], amounts[1], amounts[2]), 0, tokens, claims
    )


def withdraw_proportional(observation, shares, *, timestamp):
    if timestamp < observation.block.timestamp:
        raise ValueError("TriCrypto withdrawal cannot consume future state")
    state = observation.math_state(timestamp=timestamp)
    amounts = proportional(state, shares)
    if shares == state.total_supply:
        raise ValueError("TriCrypto transition does not model an emptied global pool")
    changed = replace(
        state,
        total_supply=state.total_supply - shares,
        balances=tuple(
            value - amount for value, amount in zip(state.balances, amounts, strict=True)
        ),
        invariant=state.invariant - state.invariant * (shares - 1) // state.total_supply,
    )
    tokens = observation.token_accounting.advance(
        timestamp, geometric_mean(changed.normalized(changed.balances)), proportional=True
    )
    return Transition(
        changed, observation.accounting.math_state(), price_state(observation), amounts, 0, tokens
    )
