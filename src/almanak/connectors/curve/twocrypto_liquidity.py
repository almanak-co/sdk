"""TwoCrypto NG LP amounts preserve the distinct view and execution fee rules."""

from dataclasses import dataclass, replace
from math import isqrt

from almanak.connectors.curve.crypto_integer import PRECISION, unsigned
from almanak.connectors.curve.twocrypto_math import dynamic_fee, invariant, solve_balance
from almanak.connectors.curve.twocrypto_quotes import QuoteState


@dataclass(frozen=True)
class LiquidityState(QuoteState):
    total_supply: int

    def __post_init__(self):
        super().__post_init__()
        if unsigned(self.total_supply) == 0:
            raise ValueError("TwoCrypto model requires an existing funded LP supply")


def token_fee(state, amounts, normalized):
    if len(amounts) != 2 or sum(amounts) <= 0:
        raise ValueError("TwoCrypto liquidity addition requires positive ordered inputs")
    for value in amounts:
        unsigned(value)
    total = unsigned(sum(amounts))
    average = total // 2
    difference = sum(abs(value - average) for value in amounts)
    fee = dynamic_fee(normalized, state.packed_fees) * 2 // 4
    return unsigned(fee * difference) // total + 10**5


def addition(state, amounts, *, contract_view=False):
    if len(amounts) != 2 or sum(amounts) <= 0:
        raise ValueError("TwoCrypto liquidity addition requires positive ordered inputs")
    for value in amounts:
        unsigned(value)
    previous = state.normalized(state.balances)
    expanded = state.normalized(
        tuple(
            unsigned(balance + amount)
            for balance, amount in zip(state.balances, amounts, strict=True)
        )
    )
    normalized_amounts = (
        state.normalized(amounts)
        if contract_view
        else tuple(new - old for new, old in zip(expanded, previous, strict=True))
    )
    new_invariant = invariant(state.amplification, state.gamma, expanded)
    gross = unsigned(state.total_supply * new_invariant // state.invariant - state.total_supply)
    fee = token_fee(state, normalized_amounts, expanded) * gross // 10**10 + 1
    minted = unsigned(gross - fee)
    if minted == 0:
        raise ValueError("TwoCrypto addition mints no LP shares")
    return minted, fee, new_invariant


def burn_amount(state, shares):
    if not 0 < unsigned(shares) <= state.total_supply:
        raise ValueError("TwoCrypto removal requires positive shares within LP supply")


def proportional(state, shares):
    burn_amount(state, shares)
    if shares == state.total_supply:
        return state.balances
    # The deployed revision subtracts one share before computing partial payouts.
    return tuple(unsigned(value * (shares - 1)) // state.total_supply for value in state.balances)


def withdrawal_fee(state, normalized, index, shares):
    correction = normalized[index] * 2 * shares // state.total_supply
    if correction >= normalized[index]:
        return (state.packed_fees >> 64) & (2**64 - 1)
    adjusted = list(normalized)
    adjusted[index] -= correction
    return dynamic_fee(adjusted, state.packed_fees)


def single_coin(state, index, shares):
    burn_amount(state, shares)
    if type(index) is not int or index not in (0, 1):
        raise ValueError("TwoCrypto withdrawal coin index is out of range")
    normalized = state.normalized(state.balances)
    fee = withdrawal_fee(state, normalized, index, shares)
    decrease = unsigned(shares * state.invariant) // state.total_supply
    invariant_fee = fee * decrease // (2 * 10**10) + 1
    reduced = unsigned(state.invariant - unsigned(decrease - invariant_fee))
    remaining, _ = solve_balance(state.amplification, state.gamma, normalized, reduced, index)
    scale = (
        PRECISION * state.precisions[0] if index == 0 else state.price_scale * state.precisions[1]
    )
    output = unsigned(unsigned(normalized[index] - remaining) * PRECISION) // scale
    approximate_fee = unsigned(2 * invariant_fee * state.balances[index]) // state.invariant
    return output, approximate_fee, reduced


@dataclass(frozen=True)
class AdminState:
    virtual_price: int
    profit: int
    claimed_profit: int
    virtual_shares: int
    last_claim_time: int
    receiver_enabled: bool

    def __post_init__(self):
        for value in (
            self.virtual_price,
            self.profit,
            self.claimed_profit,
            self.virtual_shares,
            self.last_claim_time,
        ):
            unsigned(value)
        if type(self.receiver_enabled) is not bool:
            raise ValueError("TwoCrypto admin receiver availability must be explicit")


def xcp(invariant_value, price_scale):
    first = invariant_value // 2
    second = unsigned(invariant_value * PRECISION) // unsigned(price_scale * 2)
    return isqrt(unsigned(first * second))


def admin_claim(state, admin, *, timestamp, ramp_end):
    changed, _, amounts = admin_transition(state, admin, timestamp=timestamp, ramp_end=ramp_end)
    return changed, amounts


def admin_transition(state, admin, *, timestamp, ramp_end):
    unsigned(timestamp)
    unsigned(ramp_end)
    if timestamp < admin.last_claim_time:
        raise ValueError("TwoCrypto admin state cannot come from the future")
    if timestamp - admin.last_claim_time < 86400 or ramp_end > timestamp:
        return state, admin, (0, 0)
    if admin.profit <= admin.claimed_profit or state.total_supply < PRECISION:
        return state, admin, (0, 0)
    fees = (admin.profit - admin.claimed_profit) * (5 * 10**9) // (2 * 10**10)
    shares = admin.virtual_shares
    profit = admin.profit
    if admin.receiver_enabled and fees > 0:
        denominator = unsigned(admin.virtual_price - fees)
        fraction = unsigned(admin.virtual_price * PRECISION) // denominator - PRECISION
        shares = unsigned(shares + state.total_supply * fraction // PRECISION)
        profit = unsigned(profit - 2 * fees)
    supply = unsigned(state.total_supply + shares)
    virtual_price = unsigned(PRECISION * xcp(state.invariant, state.price_scale)) // supply
    if virtual_price < PRECISION:
        return state, admin, (0, 0)
    amounts = tuple(unsigned(value * shares) // supply for value in state.balances)
    changed = replace(
        state,
        balances=tuple(
            value - amount for value, amount in zip(state.balances, amounts, strict=True)
        ),
        invariant=state.invariant - state.invariant * shares // supply,
    )
    retained = replace(
        admin,
        virtual_price=virtual_price,
        profit=profit,
        claimed_profit=max(profit, admin.claimed_profit),
        virtual_shares=0,
        last_claim_time=timestamp,
    )
    return changed, retained, amounts
