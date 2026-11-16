"""Pinned TriCrypto NG v2.0.0 view calculations, distinct from state transitions."""

from dataclasses import dataclass

from almanak.connectors.curve.crypto_integer import PRECISION, unsigned
from almanak.connectors.curve.tricrypto_math import (
    balances,
    dynamic_fee,
    parameters,
    solve_balance,
)


@dataclass(frozen=True)
class QuoteState:
    balances: tuple[int, int, int]
    precisions: tuple[int, int, int]
    price_scales: tuple[int, int]
    amplification: int
    gamma: int
    invariant: int
    packed_fees: int

    def __post_init__(self):
        parameters(self.amplification, self.gamma)
        balances(self.balances)
        if len(self.precisions) != 3 or len(self.price_scales) != 2:
            raise ValueError("TriCrypto requires three precisions and two price scales")
        for value in (*self.precisions, *self.price_scales, self.invariant):
            if unsigned(value) == 0:
                raise ValueError("TriCrypto quote precision, scale and invariant must be positive")
        dynamic_fee(self.normalized(self.balances), self.packed_fees)

    def normalized(self, values):
        return (
            unsigned(values[0] * self.precisions[0]),
            *(
                unsigned(
                    unsigned(values[index] * self.price_scales[index - 1]) * self.precisions[index]
                )
                // PRECISION
                for index in (1, 2)
            ),
        )


def swap(state, input_index, output_index, amount):
    return swap_details(state, input_index, output_index, amount)[0]


def swap_details(state, input_index, output_index, amount):
    if type(input_index) is not int or type(output_index) is not int:
        raise ValueError("TriCrypto swap requires integer coin indices")
    if input_index == output_index or not {input_index, output_index} <= {0, 1, 2}:
        raise ValueError("TriCrypto swap requires two different configured coins")
    if unsigned(amount) == 0:
        raise ValueError("TriCrypto swap requires positive input")
    values = list(state.balances)
    values[input_index] = unsigned(values[input_index] + amount)
    normalized = list(state.normalized(values))
    remaining, ratio = solve_balance(
        state.amplification, state.gamma, normalized, state.invariant, output_index
    )
    output = unsigned(normalized[output_index] - remaining - 1)
    normalized[output_index] = remaining
    if output_index > 0:
        output = unsigned(output * PRECISION) // state.price_scales[output_index - 1]
    output //= state.precisions[output_index]
    output = unsigned(output - dynamic_fee(normalized, state.packed_fees) * output // 10**10)
    values[output_index] = unsigned(values[output_index] - output)
    return output, ratio, tuple(values)
