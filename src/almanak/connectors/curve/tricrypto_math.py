"""Revision-bound TriCrypto NG v2.0.0 integer invariant and output balances.

Compared against deployed Math at 6bfe070cca8788ce438dcdf15d9fe1bcea74cba6.
This module does not establish execution or historical-model support.
"""

from almanak.connectors.curve.crypto_integer import PRECISION, cube_root, divide, unsigned
from almanak.connectors.curve.tricrypto_cubic import coefficients, root


def parameters(amplification, gamma):
    unsigned(amplification)
    unsigned(gamma)
    if not 2700 <= amplification <= 270000000 or not 10**10 <= gamma <= 5 * 10**16:
        raise ValueError("TriCrypto amplification or gamma is outside the revision's domain")


def balances(values):
    if len(values) != 3:
        raise ValueError("TriCrypto requires exactly three normalized balances")
    for value in values:
        unsigned(value)
    if min(values) == 0:
        raise ValueError("TriCrypto requires funded reserves")


def balance_ratio(value, invariant, *, strict_lower=False):
    ratio = unsigned(value * PRECISION) // invariant
    if not 10**16 - 1 <= ratio <= 10**20 or (strict_lower and ratio == 10**16 - 1):
        raise ValueError("TriCrypto balance is outside the safe invariant ratio")
    return value


def balance_domain(amplification, gamma, values, invariant, index):
    parameters(amplification, gamma)
    balances(values)
    unsigned(invariant)
    if type(index) is not int or index not in (0, 1, 2):
        raise ValueError("TriCrypto coin index is out of range")
    if not 10**17 <= invariant <= 10**33:
        raise ValueError("TriCrypto invariant is outside the revision's domain")
    for offset, value in enumerate(values):
        if offset != index:
            balance_ratio(value, invariant, strict_lower=True)


def newton_balance_step(amplification, gamma, known_sum, known_ratio, invariant, current):
    ratio = known_ratio * current * 3 // invariant
    total = known_sum + current
    gap = abs(gamma + PRECISION - ratio) + 1
    first = PRECISION * invariant // gamma * gap // gamma * gap * 10000 // amplification
    second = PRECISION + 2 * PRECISION * ratio // gap
    derivative = PRECISION * current + total * second + first
    decrement = invariant * second
    if derivative < decrement:
        return current // 2
    derivative -= decrement
    slope = derivative // current
    negative = first // slope
    positive = (derivative + PRECISION * invariant) // slope + negative * PRECISION // ratio
    negative += PRECISION * total // slope
    return current // 2 if positive < negative else unsigned(positive - negative)


def newton_balance(amplification, gamma, values, invariant, index):
    balance_domain(amplification, gamma, values, invariant, index)
    known = sorted((value for offset, value in enumerate(values) if offset != index), reverse=True)
    current = invariant // 3
    known_ratio = PRECISION
    for value in reversed(known):
        current = unsigned(current * invariant) // unsigned(value * 3)
    for value in known:
        known_ratio = unsigned(known_ratio * value * 3) // invariant
    tolerance = max(known[0] // 10**14, invariant // 10**14, 100)
    for _ in range(255):
        previous = current
        current = newton_balance_step(
            amplification, gamma, sum(known), known_ratio, invariant, current
        )
        if abs(current - previous) < max(tolerance, current // 10**14):
            return balance_ratio(current, invariant, strict_lower=True)
    raise ValueError("TriCrypto output balance did not converge")


def solve_balance(amplification, gamma, values, invariant, index):
    balance_domain(amplification, gamma, values, invariant, index)
    first, second = (value for offset, value in enumerate(values) if offset != index)
    result = root(*coefficients(amplification, gamma, first, second, invariant))
    if result is None:
        return newton_balance(amplification, gamma, values, invariant, index), 0
    ratio, leading = result
    value = unsigned(divide(divide(divide(invariant * invariant, 27), second) * invariant, first))
    value = unsigned(divide(value * ratio, leading))
    return balance_ratio(value, invariant), unsigned(divide(PRECISION * ratio, leading))


def geometric_mean(values):
    if len(values) != 3:
        raise ValueError("TriCrypto geometric mean requires three balances")
    for value in values:
        unsigned(value)
    product = unsigned(unsigned(values[0] * values[1]) // PRECISION * values[2]) // PRECISION
    return cube_root(product) if product else 0


def initial_invariant(values, previous_ratio):
    if previous_ratio == 0:
        return unsigned(3 * geometric_mean(values))
    total = sum(values)
    divisor, factor = (
        (10**36, 10**12)
        if total > 10**36
        else ((10**24, 10**6) if total > 10**24 else (PRECISION, 1))
    )
    product = unsigned(unsigned(values[0] * values[1]) // divisor * values[2])
    return cube_root(unsigned(product // previous_ratio * 27 * factor))


def invariant_step(amplification, gamma, values, current):
    total = sum(values)
    ratio = PRECISION
    for value in values:
        ratio = unsigned(ratio * value * 3) // current
    gap = abs(gamma + PRECISION - ratio) + 1
    first = PRECISION * current // gamma * gap // gamma * gap * 10000 // amplification
    second = 6 * PRECISION * ratio // gap
    slope = unsigned(
        total + total * second // PRECISION + first * 3 // ratio - second * current // PRECISION
    )
    positive = unsigned(current * (slope + total)) // slope
    negative = unsigned(current * current) // slope
    correction = current * (first // slope) // PRECISION * abs(PRECISION - ratio) // ratio
    negative = unsigned(negative + correction if PRECISION > ratio else negative - correction)
    return unsigned(positive - negative if positive > negative else (negative - positive) // 2)


def invariant(amplification, gamma, values, previous_ratio=0):
    parameters(amplification, gamma)
    balances(values)
    unsigned(previous_ratio)
    ordered = tuple(sorted(values, reverse=True))
    if ordered[0] >= (2**256 - 1) // PRECISION * 27:
        raise ValueError("TriCrypto input balance is outside the revision's domain")
    current = initial_invariant(ordered, previous_ratio)
    for _ in range(255):
        previous = current
        current = invariant_step(amplification, gamma, ordered, current)
        if abs(current - previous) * 10**14 < max(10**16, current):
            for value in ordered:
                balance_ratio(value, current)
            return current
    raise ValueError("TriCrypto invariant did not converge")


def reduction_coefficient(values, gamma):
    balances(values)
    unsigned(gamma)
    total = unsigned(sum(values))
    ratio = PRECISION
    for value in values:
        ratio = unsigned(ratio * 3 * value) // total
    return unsigned(gamma * PRECISION) // unsigned(gamma + PRECISION - ratio) if gamma else ratio


def dynamic_fee(values, packed):
    unsigned(packed)
    mid, out, gamma = ((packed >> shift) & (2**64 - 1) for shift in (128, 64, 0))
    if not 0 < gamma <= PRECISION or not 0 <= mid <= out <= 10**10:
        raise ValueError("TriCrypto packed fees are outside the supported domain")
    weight = reduction_coefficient(values, gamma)
    return (mid * weight + out * (PRECISION - weight)) // PRECISION


def marginal_prices(values, invariant, amplification, gamma):
    balance_domain(amplification, gamma, values, invariant, 0)
    ratio = 27 * values[0] * values[1] // invariant * values[2] // invariant * 10**36 // invariant
    gradient = unsigned(
        2 * ratio * ratio // 10**36 * ratio // 10**36
        + (gamma + PRECISION) ** 2
        - ratio * ratio // 10**36 * (2 * gamma + 3 * PRECISION) // PRECISION
    )
    amplified = amplification * gamma * gamma // 10000
    denominator = gradient + amplified * values[0] // invariant * ratio // 10**36
    return tuple(
        unsigned(values[0] * (gradient + amplified * value // invariant * ratio // 10**36))
        // value
        * PRECISION
        // denominator
        for value in values[1:]
    )
