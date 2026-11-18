"""Verified V3 swap steps and Pancake's distinct directional protocol-fee fractions."""

from almanak.connectors.concentrated_swap import v3_step as swap_step


def protocol_fee_bps(encoded, zero_for_one):
    if type(encoded) is not int or not 0 <= encoded < 2**32:
        raise ValueError("Pancake V3 protocol fees require a uint32 encoding")
    fractions = encoded & 65535, encoded >> 16
    if any(value and not 1000 <= value <= 4000 for value in fractions):
        raise ValueError("Pancake V3 protocol fee fraction is outside the core bounds")
    return fractions[0 if zero_for_one else 1]


def protocol_fee_amount(fee_paid, encoded, zero_for_one):
    if type(fee_paid) is not int or not 0 <= fee_paid < 2**256:
        raise ValueError("Pancake V3 paid fee requires a uint256 amount")
    product = fee_paid * protocol_fee_bps(encoded, zero_for_one)
    if product >= 2**256:
        raise ValueError("Pancake V3 protocol fee multiplication overflows uint256")
    return product // 10_000


def exact_input(sample, amount, zero_for_one):
    from almanak.connectors.concentrated_swap import exact_input as calculate

    return calculate(sample, amount, zero_for_one, sample.pool.source.fee, swap_step)
