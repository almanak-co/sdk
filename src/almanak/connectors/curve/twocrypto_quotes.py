"""Pinned TwoCrypto NG v2.1.0 view calculations, distinct from state transitions."""

from dataclasses import dataclass

from almanak.connectors.curve.crypto_integer import PRECISION, unsigned
from almanak.connectors.curve.twocrypto_math import (
    balances,
    dynamic_fee,
    parameters,
    solve_balance,
)


@dataclass(frozen=True)
class QuoteState:
    balances: tuple[int, int]
    precisions: tuple[int, int]
    price_scale: int
    amplification: int
    gamma: int
    invariant: int
    packed_fees: int

    def __post_init__(self):
        parameters(self.amplification, self.gamma)
        balances(self.balances)
        if len(self.precisions) != 2:
            raise ValueError("TwoCrypto requires two ordered decimal precisions")
        for value in (*self.precisions, self.price_scale, self.invariant):
            if unsigned(value) == 0:
                raise ValueError("TwoCrypto quote precision, scale and invariant must be positive")
        dynamic_fee(self.normalized(self.balances), self.packed_fees)

    def normalized(self, values):
        return (
            unsigned(values[0] * self.precisions[0]),
            unsigned(unsigned(values[1] * self.price_scale) * self.precisions[1]) // PRECISION,
        )


def swap(state, input_index, output_index, amount):
    return swap_details(state, input_index, output_index, amount)[0]


def swap_details(state, input_index, output_index, amount):
    if type(input_index) is not int or type(output_index) is not int:
        raise ValueError("TwoCrypto swap requires integer coin indices")
    if {input_index, output_index} != {0, 1}:
        raise ValueError("TwoCrypto swap requires two different configured coins")
    if unsigned(amount) == 0:
        raise ValueError("TwoCrypto swap requires positive input")
    values = list(state.balances)
    values[input_index] = unsigned(values[input_index] + amount)
    normalized = list(state.normalized(values))
    remaining, ratio = solve_balance(
        state.amplification, state.gamma, normalized, state.invariant, output_index
    )
    output = unsigned(normalized[output_index] - remaining - 1)
    normalized[output_index] = remaining
    if output_index == 1:
        output = unsigned(output * PRECISION) // state.price_scale
    output //= state.precisions[output_index]
    output = unsigned(output - dynamic_fee(normalized, state.packed_fees) * output // 10**10)
    values[output_index] = unsigned(values[output_index] - output)
    return output, ratio, tuple(values)
