"""TriCrypto NG LP amounts preserve the distinct view and execution fee rules."""

from dataclasses import dataclass, replace

from almanak.connectors.curve.crypto_integer import PRECISION, unsigned
from almanak.connectors.curve.tricrypto_math import (
    dynamic_fee,
    geometric_mean,
    invariant,
    solve_balance,
)
from almanak.connectors.curve.tricrypto_quotes import QuoteState


@dataclass(frozen=True)
class LiquidityState(QuoteState):
    total_supply: int

    def __post_init__(self):
        super().__post_init__()
        if unsigned(self.total_supply) == 0:
            raise ValueError("TriCrypto model requires an existing funded LP supply")


def token_fee(state, amounts, normalized):
    if len(amounts) != 3 or sum(amounts) <= 0:
        raise ValueError("TriCrypto liquidity addition requires positive ordered inputs")
    for value in amounts:
        unsigned(value)
    total = unsigned(sum(amounts))
    average = total // 3
    difference = sum(abs(value - average) for value in amounts)
    fee = dynamic_fee(normalized, state.packed_fees) * 3 // 8
    return unsigned(fee * difference) // total + 10**5


def addition(state, amounts, *, contract_view=False):
    if len(amounts) != 3 or sum(amounts) <= 0:
        raise ValueError("TriCrypto liquidity addition requires positive ordered inputs")
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
        raise ValueError("TriCrypto addition mints no LP shares")
    return minted, fee, new_invariant


def burn_amount(state, shares):
    if not 0 < unsigned(shares) <= state.total_supply:
        raise ValueError("TriCrypto removal requires positive shares within LP supply")


def proportional(state, shares):
    burn_amount(state, shares)
    if shares == state.total_supply:
        return state.balances
    # The deployed revision subtracts one share before computing partial payouts.
    return tuple(unsigned(value * (shares - 1)) // state.total_supply for value in state.balances)


def withdrawal_fee(state, normalized, index, shares):
    correction = normalized[index] * 3 * shares // state.total_supply
    if correction >= normalized[index]:
        return (state.packed_fees >> 64) & (2**64 - 1)
    adjusted = list(normalized)
    adjusted[index] -= correction
    return dynamic_fee(adjusted, state.packed_fees)


def single_coin(state, index, shares):
    burn_amount(state, shares)
    if type(index) is not int or index not in (0, 1, 2):
        raise ValueError("TriCrypto withdrawal coin index is out of range")
    normalized = state.normalized(state.balances)
    fee = withdrawal_fee(state, normalized, index, shares)
    decrease = unsigned(shares * state.invariant) // state.total_supply
    invariant_fee = fee * decrease // (2 * 10**10) + 1
    reduced = unsigned(state.invariant - unsigned(decrease - invariant_fee))
    remaining, _ = solve_balance(state.amplification, state.gamma, normalized, reduced, index)
    scale = (
        PRECISION * state.precisions[0]
        if index == 0
        else state.price_scales[index - 1] * state.precisions[index]
    )
    output = unsigned(unsigned(normalized[index] - remaining) * PRECISION) // scale
    approximate_fee = unsigned(3 * invariant_fee * state.balances[index]) // state.invariant
    return output, approximate_fee, reduced


@dataclass(frozen=True)
class AdminState:
    virtual_price: int
    profit: int
    claimed_profit: int
    receiver_enabled: bool

    def __post_init__(self):
        for value in (self.virtual_price, self.profit, self.claimed_profit):
            if unsigned(value) == 0:
                raise ValueError("TriCrypto admin prices and profit must be positive")
        if type(self.receiver_enabled) is not bool:
            raise ValueError("TriCrypto admin receiver availability must be explicit")


def xcp(invariant_value, price_scales):
    if len(price_scales) != 2 or any(unsigned(scale) == 0 for scale in price_scales):
        raise ValueError("TriCrypto xcp requires two positive price scales")
    values = (
        invariant_value // 3,
        *(unsigned(invariant_value * PRECISION) // unsigned(scale * 3) for scale in price_scales),
    )
    return geometric_mean(values)


def admin_transition(state, admin, actual_balances):
    if len(actual_balances) != 3:
        raise ValueError("TriCrypto admin claim requires measured pool custody balances")
    for value in actual_balances:
        unsigned(value)
    if admin.profit <= admin.claimed_profit or state.total_supply < PRECISION:
        return state, admin, 0
    fees = (admin.profit - admin.claimed_profit) * (5 * 10**9) // (2 * 10**10)
    minted = 0
    profit = admin.profit
    if admin.receiver_enabled and fees > 0:
        denominator = unsigned(admin.virtual_price - fees)
        fraction = unsigned(admin.virtual_price * PRECISION) // denominator - PRECISION
        minted = unsigned(state.total_supply * fraction) // PRECISION
        profit = unsigned(profit - 2 * fees)
    supply = unsigned(state.total_supply + minted)
    updated = invariant(state.amplification, state.gamma, state.normalized(actual_balances))
    virtual_price = unsigned(PRECISION * xcp(updated, state.price_scales)) // supply
    changed = replace(
        state, total_supply=supply, balances=tuple(actual_balances), invariant=updated
    )
    retained = replace(admin, virtual_price=virtual_price, profit=profit, claimed_profit=profit)
    return changed, retained, minted
