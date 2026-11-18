"""V3 exact-input steps retain rounded actual input separately from fees.

Compared against v3-core v1.0.0 SwapMath through an isolated Solidity harness.
V3 recomputes partial-step input from the rounded resulting price, including
rounding residue in fees even when the configured pool fee is zero.
"""

from almanak.connectors.concentrated_swap import v3_step as swap_step


def protocol_fee_divisor(encoded, zero_for_one):
    if type(encoded) is not int or not 0 <= encoded <= 255:
        raise ValueError("V3 protocol fees require a uint8 encoding")
    divisors = encoded & 15, encoded >> 4
    if any(value and not 4 <= value <= 10 for value in divisors):
        raise ValueError("V3 protocol fee divisor is outside the core contract bounds")
    return divisors[0 if zero_for_one else 1]


def exact_input(sample, amount, zero_for_one):
    from almanak.connectors.concentrated_swap import exact_input as calculate

    return calculate(sample, amount, zero_for_one, sample.pool.source.fee, swap_step)
